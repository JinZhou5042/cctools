#!/usr/bin/env python3

import argparse
import json

from ndcctools.taskvine.datavine import Workflow

from datavine_phase4_demand_pull import run_case


def seed(value):
    return value


def advance(value, increment):
    return value + increment


def combine(left, right):
    return left * 1000 + right


def build_workflow():
    workflow = Workflow()

    left_root = workflow.add_task(seed, 10)
    left_a = workflow.add_task(advance, left_root.output(), 1)
    left_b = workflow.add_task(advance, left_root.output(), 2)
    left_frontier = workflow.add_task(
        combine, left_a.output(), left_b.output()
    )

    right_root = workflow.add_task(seed, 20)
    right_frontier = workflow.add_task(
        advance, right_root.output(), 3
    )
    right_tail = workflow.add_task(
        advance, right_frontier.output(), 4
    )

    join = workflow.add_task(
        combine, left_frontier.output(), right_tail.output()
    )
    target = workflow.add_task(advance, join.output(), 5)
    oracle = advance(combine(combine(11, 12), 27), 5)
    return {
        "workflow": workflow,
        "left": (
            left_root,
            left_a,
            left_b,
            left_frontier,
        ),
        "right": (right_root, right_frontier, right_tail),
        "join": join,
        "target": target,
        "oracle": oracle,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()

    graph = build_workflow()
    workflow = graph["workflow"]
    left = graph["left"]
    right = graph["right"]
    left_frontier = left[-1]
    right_frontier = right[1]
    right_tail = right[-1]
    join = graph["join"]
    target = graph["target"]

    snapshot = run_case(
        "branched-minimum-cut",
        workflow,
        target.task_id,
        graph["oracle"],
        factory_manager=args.factory_manager,
        worker_count=3,
        worker_cores=1,
        prefetch=False,
        persistence=True,
        persistence_parent=(
            "/groups/dthain/users/jzhou24/factory-scratch"
            if args.factory_manager
            else None
        ),
        persistence_attempts_by_task={
            left_frontier.task_id: 1,
            right_frontier.task_id: 1,
        },
        prune_after_persistence_by_task={
            left_frontier.task_id: tuple(
                task.task_id for task in left[:-1]
            ),
            right_frontier.task_id: (right[0].task_id,),
        },
        inject_worker_loss_schedule=(join.task_id,),
        inject_worker_loss_data_by_task={
            join.task_id: (
                right_tail.task_id,
                join.task_id,
            )
        },
        worker_loss_process_shutdown=True,
    )
    report = snapshot["scheduler_report"]
    assert report["logical_tasks"] == 9
    assert report["physical_attempts"] == 11
    assert report["recovery_reexecutions"] == 2
    assert report["legacy_recovery_tasks"] == 0
    assert report["persistence_tasks_completed"] == 0
    assert report["persistence_controller_tasks_completed"] == 2
    assert report["persistence_controller_bytes"] > 0
    assert report["persistence_worker_bytes"] == 0
    assert report["persistence_required_data_ids"] == [
        left_frontier.task_id,
        right_frontier.task_id,
    ]
    assert report["runtime_pruned_data_ids"] == [
        left[0].task_id,
        left[1].task_id,
        left[2].task_id,
        right[0].task_id,
    ]
    assert len(report["frontier_pruning"]) == 2
    assert report["recovery_waves"][0]["tasks"] == [
        join.task_id,
        right_tail.task_id,
    ]
    assert report["recovery_waves"][0]["rollback_depth"] == 2
    attempts = report["attempts_by_task"]
    assert all(
        attempts[str(task.task_id)] == 1 for task in left
    )
    assert attempts[str(right[0].task_id)] == 1
    assert attempts[str(right_frontier.task_id)] == 1
    assert attempts[str(right_tail.task_id)] == 2
    assert attempts[str(join.task_id)] == 2
    assert attempts[str(target.task_id)] == 1
    event = report["worker_loss_events"][0]
    assert event["process_shutdown"]
    assert event["workers_before"] and len(event["workers_before"]) == 3
    assert len(event["workers_after"]) == 2
    assert event["released_worker_id"] not in event["workers_after"]
    assert (
        event["released_worker_id"]
        in event["target_replica_worker_ids"]
    )
    assert snapshot["durable_hashes_valid"]
    assert snapshot["persistence_temporary_files"] == []
    print(
        json.dumps(
            {
                "attempts_by_task": attempts,
                "durability_frontiers": [
                    left_frontier.task_id,
                    right_frontier.task_id,
                ],
                "pruned_data_ids": report[
                    "runtime_pruned_data_ids"
                ],
                "recovery_wave": report["recovery_waves"][0],
                "status": "PASS",
                "unaffected_left_branch": True,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
