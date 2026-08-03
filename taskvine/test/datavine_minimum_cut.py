#!/usr/bin/env python3

import argparse
import hashlib
import json
import time

from ndcctools.taskvine.datavine import Workflow

from datavine_phase4_demand_pull import run_case


PAYLOAD_SIZE = 512 * 1024


def make_payload(size):
    return bytes(index % 251 for index in range(size))


def advance(payload, increment, delay=0):
    if delay:
        time.sleep(delay)
    return bytes(
        (value + increment) % 251 for value in payload
    )


def payload_digest(payload):
    return hashlib.sha256(payload).hexdigest()


def build_workflow():
    workflow = Workflow()
    stages = [workflow.add_task(make_payload, PAYLOAD_SIZE)]
    for increment in range(1, 8):
        stages.append(
            workflow.add_task(
                advance,
                stages[-1].output(),
                increment,
                0.25 if increment == 5 else 0,
            )
        )
    target = workflow.add_task(payload_digest, stages[-1].output())
    payload = make_payload(PAYLOAD_SIZE)
    for increment in range(1, 8):
        payload = advance(payload, increment)
    return workflow, stages, target, payload_digest(payload)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()

    workflow, stages, target, oracle = build_workflow()
    first_frontier = stages[0].task_id
    second_frontier = stages[4].task_id
    first_loss = stages[4].task_id
    second_loss = stages[7].task_id
    snapshot = run_case(
        "minimum-recoverable-cut",
        workflow,
        target.task_id,
        oracle,
        factory_manager=args.factory_manager,
        worker_count=3,
        worker_cores=1,
        prefetch=False,
        persistence=True,
        validate_durable_recovery=True,
        max_idata_bytes=128 * 1024,
        max_inline_idata_bytes=64 * 1024,
        persistence_parent=(
            "/groups/dthain/users/jzhou24/factory-scratch"
            if args.factory_manager
            else None
        ),
        persistence_attempts_by_task={
            first_frontier: 1,
            second_frontier: 2,
        },
        inject_worker_loss_schedule=(
            first_loss,
            second_loss,
        ),
        inject_worker_loss_data_by_task={
            first_loss: tuple(
                stage.task_id for stage in stages[1:5]
            ),
            second_loss: tuple(
                stage.task_id for stage in stages[5:8]
            ),
        },
        prune_after_persistence_by_task={
            second_frontier: tuple(
                stage.task_id for stage in stages[0:4]
            )
        },
        worker_loss_process_shutdown=True,
    )
    report = snapshot["scheduler_report"]
    assert report["logical_tasks"] == 9
    assert report["worker_loss_injections"] == 2
    assert len(report["worker_loss_events"]) == 2
    assert report["worker_loss_process_shutdown"]
    assert all(
        event["process_shutdown"]
        for event in report["worker_loss_events"]
    )
    assert [
        len(event["workers_before"])
        for event in report["worker_loss_events"]
    ] == [3, 2], report["worker_loss_events"]
    assert all(
        event["released_worker_id"] not in event["workers_after"]
        and event["released_worker_id"]
        in event["target_replica_worker_ids"]
        for event in report["worker_loss_events"]
    ), report["worker_loss_events"]
    assert report["recovery_reexecutions"] == 7, report
    assert report["physical_attempts"] == 16, report
    assert report["persistence_required_data_ids"] == [
        first_frontier,
        second_frontier,
    ]
    assert report["persistence_outstanding_data_ids"] == [
        second_frontier
    ]
    assert len(report["recovery_waves"]) == 2, report
    assert report["runtime_pruned_data_ids"] == [1, 2, 3, 4]
    assert len(report["frontier_pruning"]) == 1
    pruning = report["frontier_pruning"][0]
    assert pruning["frontier_task_id"] == second_frontier
    assert pruning["data_ids"] == [1, 2, 3, 4]
    pruning_actions = {
        record["action"]
        for record in pruning["result"]["controller"]["applied"]
    }
    assert "quarantine-sharedfs" in pruning_actions
    assert "invalidate-worker-pending-delete" in pruning_actions
    assert all(
        worker_prune["requested"] == worker_prune["confirmed"]
        and worker_prune["tracker_released"]
        for worker_prune in pruning["result"]["worker_prunes"]
    )
    recovered_tasks = [
        wave["tasks"] for wave in report["recovery_waves"]
    ]
    assert recovered_tasks == [
        [stage.task_id for stage in reversed(stages[1:5])],
        [stage.task_id for stage in reversed(stages[5:8])],
    ], recovered_tasks
    recovery_depths = [
        wave["rollback_depth"]
        for wave in report["recovery_waves"]
    ]
    assert recovery_depths == [4, 3], recovery_depths
    assert snapshot["durable_hashes_valid"]
    assert snapshot["persistence_temporary_files"] == []
    assert len(snapshot["durable_files"]) == 1
    assert snapshot["superseded_persistence_data_ids"] == [
        first_frontier
    ]
    assert snapshot["durable_recovery_actions"] == {
        str(second_frontier): "validated-durable",
    }
    assert snapshot["idata_bytes_high_water"] <= 128 * 1024
    print(
        json.dumps(
            {
                "durability_frontiers": [
                    first_frontier,
                    second_frontier,
                ],
                "recovery_depths": recovery_depths,
                "recovery_waves": report["recovery_waves"],
                "scheduler_report": report,
                "status": "PASS",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
