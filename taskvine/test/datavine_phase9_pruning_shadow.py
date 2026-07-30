#!/usr/bin/env python3

import json
import random

from ndcctools.taskvine.datavine.recovery import (
    IncrementalPruner,
    LineageGraph,
)


def assert_equivalent(pruner):
    plan = pruner.assert_matches_reference()
    assert len(plan.records) == len(pruner.graph.data_ids)
    assert plan.nodes_examined <= len(pruner.graph.data_ids)
    assert {
        record.decision for record in plan.records
    } <= {"absent", "keep", "prune", "cancel-persistence"}
    assert_safe_after_pruning(pruner, plan)
    return plan


def assert_safe_after_pruning(pruner, plan):
    """Independent worst-case lineage replay check after proposed deletions."""

    deleted = set(plan.prunable)
    memo = {}

    def recoverable(data_id):
        if data_id in memo:
            return memo[data_id]
        state = pruner.data_states[data_id]
        if state.available and state.durable and data_id not in deleted:
            memo[data_id] = True
            return True
        # Stable task code and EData make root producers replayable.
        result = all(
            recoverable(parent_id)
            for parent_id in pruner.graph.producer_inputs(data_id)
        )
        memo[data_id] = result
        return result

    obligations = set()
    for task_id in pruner.graph.task_ids:
        if pruner.task_states[task_id] in ("pending", "running"):
            obligations.update(pruner.graph.inputs_by_task[task_id])
    obligations.update(
        data_id
        for data_id, state in pruner.data_states.items()
        if state.required_output
    )
    assert all(recoverable(data_id) for data_id in obligations)


def build_diverse_graph():
    graph = LineageGraph()
    graph.add_task(1, (), (1,))
    graph.add_task(2, (), (2,))
    graph.add_task(3, (1,), (3,))
    graph.add_task(4, (1,), (4,))
    graph.add_task(5, (3, 4), (5,))
    graph.add_task(6, (2,), (6,))
    graph.add_task(7, (5, 6), (7, 8))
    graph.add_task(8, (7,), (9,))
    return graph


def deterministic_frontier_case():
    pruner = IncrementalPruner(build_diverse_graph())
    for data_id in pruner.graph.data_ids:
        pruner.set_data_state(data_id, available=True)
    for task_id in range(1, 8):
        pruner.set_task_state(task_id, "completed")
    pruner.set_data_state(3, durable=True)
    pruner.set_data_state(6, durable=True)
    pruner.set_data_state(9, required_output=True)
    plan = assert_equivalent(pruner)
    assert 3 in plan.protected
    assert 6 in plan.protected
    assert 1 in plan.prunable
    assert 2 in plan.prunable
    assert dict(plan.recovery_depths)[9] > 0

    pruner.set_task_state(8, "completed")
    pruner.set_data_state(9, durable=True)
    plan = assert_equivalent(pruner)
    assert dict(plan.recovery_depths)[9] == 0
    assert 9 in plan.protected
    assert 3 in plan.prunable
    assert 6 in plan.prunable

    pruner.set_data_state(5, persistence="queued")
    plan = assert_equivalent(pruner)
    assert 5 in plan.cancel_persistence
    record_by_id = {
        record.data_id: record for record in plan.records
    }
    assert "obsolete-persistence" in record_by_id[5].reasons

    pruner.set_data_state(5, persistence="none")
    pruner.set_data_state(4, persistence="writing")
    plan = assert_equivalent(pruner)
    assert 4 in plan.protected
    pruner.set_data_state(4, persistence="none", durable=True)
    assert_equivalent(pruner)

    # First loss/recovery cycle.
    pruner.set_data_state(9, required_output=False)
    pruner.set_data_state(7, available=False)
    pruner.set_task_state(7, "pending")
    plan = assert_equivalent(pruner)
    assert 3 in plan.protected
    assert 6 in plan.protected
    pruner.set_task_state(7, "completed")
    pruner.set_data_state(7, available=True)

    # A second loss on the same lineage must produce the same proof.
    pruner.set_data_state(7, available=False)
    pruner.set_task_state(7, "pending")
    second = assert_equivalent(pruner)
    assert 3 in second.protected
    assert 6 in second.protected

    # Dynamic growth invalidates the old proof and adds a direct consumer.
    pruner.add_task(9, (8,), (10,))
    pruner.set_data_state(10, required_output=True)
    dynamic = assert_equivalent(pruner)
    record_by_id = {
        record.data_id: record for record in dynamic.records
    }
    assert "active-consumer:T9" in record_by_id[8].reasons
    return dynamic.to_dict()


def random_graph(seed, task_count):
    rng = random.Random(seed)
    graph = LineageGraph()
    data_ids = []
    next_data_id = 1
    for task_id in range(1, task_count + 1):
        maximum_inputs = min(4, len(data_ids))
        input_count = rng.randint(0, maximum_inputs)
        inputs = (
            tuple(sorted(rng.sample(data_ids, input_count)))
            if input_count
            else ()
        )
        output_count = 2 if task_id % 17 == 0 else 1
        outputs = tuple(
            range(next_data_id, next_data_id + output_count)
        )
        next_data_id += output_count
        graph.add_task(task_id, inputs, outputs)
        data_ids.extend(outputs)
    return graph


def random_equivalence_cases():
    rng = random.Random(90210)
    events = 0
    maximum_incremental_scan = 0
    for graph_seed in range(40):
        graph = random_graph(graph_seed, 80)
        pruner = IncrementalPruner(graph)
        assert_equivalent(pruner)
        for _ in range(160):
            events += 1
            action = rng.randrange(5)
            if action == 0:
                task_id = rng.choice(graph.task_ids)
                old = pruner.task_states[task_id]
                choices = {
                    "pending": ("running", "completed", "cancelled"),
                    "running": ("pending", "completed", "cancelled"),
                    "completed": ("pending",),
                    "cancelled": (),
                }[old]
                if choices:
                    pruner.set_task_state(
                        task_id, rng.choice(choices)
                    )
            else:
                data_id = rng.choice(graph.data_ids)
                old = pruner.data_states[data_id]
                if action == 1:
                    pruner.set_data_state(
                        data_id,
                        available=not old.available,
                        durable=False,
                    )
                elif action == 2 and old.available:
                    pruner.set_data_state(
                        data_id, durable=not old.durable
                    )
                elif action == 3:
                    pruner.set_data_state(
                        data_id,
                        required_output=not old.required_output,
                    )
                elif action == 4:
                    value = rng.choice(("none", "queued", "writing"))
                    pruner.set_data_state(data_id, persistence=value)
            plan = assert_equivalent(pruner)
            maximum_incremental_scan = max(
                maximum_incremental_scan, plan.nodes_examined
            )
    return {
        "graphs": 40,
        "tasks_per_graph": 80,
        "events": events,
        "maximum_incremental_nodes_examined": maximum_incremental_scan,
    }


def invalid_transitions_fail_closed():
    graph = LineageGraph()
    graph.add_task(1, (), (1,))
    pruner = IncrementalPruner(graph)
    try:
        pruner.set_data_state(1, durable=True)
    except ValueError as exc:
        assert "durable data must be available" in str(exc)
    else:
        raise AssertionError("accepted durable unavailable data")
    pruner.set_task_state(1, "cancelled")
    try:
        pruner.set_task_state(1, "pending")
    except ValueError as exc:
        assert "invalid task transition" in str(exc)
    else:
        raise AssertionError("resurrected a cancelled task")


def main():
    deterministic = deterministic_frontier_case()
    random_report = random_equivalence_cases()
    invalid_transitions_fail_closed()
    print(
        json.dumps(
            {
                "deterministic": deterministic,
                "random": random_report,
            },
            sort_keys=True,
        )
    )
    print("DataVine Phase 9 shadow pruning proof PASS")


if __name__ == "__main__":
    main()
