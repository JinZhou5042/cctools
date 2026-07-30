#!/usr/bin/env python3

import argparse
import json
import time

from datavine_phase4_demand_pull import run_case
from ndcctools.taskvine.datavine import Workflow


def add(left, right):
    return left + right


def sleepy_add(left, right, delay):
    time.sleep(delay)
    return left + right


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()
    workflow = Workflow()
    root = workflow.add_task(add, 8, 5)
    leaves = [
        workflow.add_task(sleepy_add, root.output(), value, 0.2)
        for value in (1, 2, 3, 4)
    ]
    left = workflow.add_task(
        add, leaves[0].output(), leaves[1].output()
    )
    right = workflow.add_task(
        add, leaves[2].output(), leaves[3].output()
    )
    final = workflow.add_task(add, left.output(), right.output())

    snapshot = run_case(
        "local-pruning",
        workflow,
        final.task_id,
        62,
        factory_manager=args.factory_manager,
        worker_count=2,
        prefetch=False,
        apply_pruning=True,
    )
    result = snapshot["pruning_result"]
    worker_prunes = result["worker_prunes"]
    assert len(worker_prunes) == len(workflow.tasks), worker_prunes
    assert all(item["requested"] >= 1 for item in worker_prunes)
    assert any(item["requested"] >= 2 for item in worker_prunes)
    assert all(
        item["confirmed"] == item["requested"]
        for item in worker_prunes
    )
    assert all(item["failed"] == 0 for item in worker_prunes)
    assert all(item["tracker_released"] for item in worker_prunes)
    total_requests = sum(
        item["requested"] for item in worker_prunes
    )
    if args.factory_manager is None:
        assert (
            len(snapshot["worker_cache_before_pruning"])
            - len(snapshot["worker_cache_after_pruning"])
            == total_requests
        )

    audit = snapshot["pruning"]["audits"]
    invalidations = [
        item for item in audit
        if item["action"] == "invalidate-worker-pending-delete"
    ]
    confirmations = [
        item for item in audit
        if item["action"] == "confirm-worker-pruned"
    ]
    assert len(invalidations) == total_requests
    assert len(confirmations) == total_requests
    replica_states = snapshot["replica_directory"]["replica_states"]
    assert replica_states["pruned"] == total_requests

    print(
        json.dumps(
            {
                "workflow_tasks": len(workflow.tasks),
                "physical_prune_requests": total_requests,
                "worker_prunes": worker_prunes,
                "cache_entries_before": len(
                    snapshot["worker_cache_before_pruning"]
                ),
                "cache_entries_after": len(
                    snapshot["worker_cache_after_pruning"]
                ),
                "cache_filesystem_observed": (
                    args.factory_manager is None
                ),
                "controller_pruned_replicas": (
                    replica_states["pruned"]
                ),
                "status": "PASS",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
