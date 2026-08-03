#!/usr/bin/env python3

import argparse
import json
import time

from ndcctools.taskvine.datavine import Workflow

from datavine_phase4_demand_pull import run_case


def seed(value):
    return value


def advance(value, increment):
    return value + increment


def slow_independent(value, seconds):
    time.sleep(seconds)
    return value


def build():
    workflow = Workflow()
    root = workflow.add_task(seed, 10)
    middle = workflow.add_task(advance, root.output(), 1)
    frontier = workflow.add_task(advance, middle.output(), 2)
    target = workflow.add_task(advance, frontier.output(), 3)
    independent = workflow.add_task(slow_independent, 99, 8)
    return workflow, root, middle, frontier, target, independent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()

    (
        workflow,
        root,
        middle,
        frontier,
        target,
        independent,
    ) = build()
    snapshot = run_case(
        "asynchronous-frontier-pruning",
        workflow,
        target.task_id,
        16,
        factory_manager=args.factory_manager,
        worker_count=2,
        worker_cores=1,
        persistence=True,
        persistence_parent=(
            "/groups/dthain/users/jzhou24/factory-scratch"
            if args.factory_manager
            else None
        ),
        persistence_attempts_by_task={frontier.task_id: 1},
        prune_after_persistence_by_task={
            frontier.task_id: (root.task_id, middle.task_id)
        },
        additional_result_task_ids=(independent.task_id,),
        frontier_pruning_ack_delay=12,
    )
    report = snapshot["scheduler_report"]
    assert report["logical_tasks"] == 5
    assert report["physical_attempts"] == 5
    assert report["attempts_by_task"][str(independent.task_id)] == 1
    assert report["runtime_pruned_data_ids"] == [
        root.task_id,
        middle.task_id,
    ]
    assert report["compute_completions_while_frontier_pruning"] >= 1
    assert len(report["frontier_pruning"]) == 1
    pruning = report["frontier_pruning"][0]
    assert pruning["frontier_task_id"] == frontier.task_id
    assert pruning["data_ids"] == [root.task_id, middle.task_id]
    assert all(
        entry["confirmed"] == entry["requested"]
        for entry in pruning["result"]["worker_prunes"]
    )
    assert snapshot["durable_hashes_valid"]
    assert snapshot["persistence_temporary_files"] == []
    print(
        json.dumps(
            {
                "compute_completions_while_frontier_pruning": (
                    report[
                        "compute_completions_while_frontier_pruning"
                    ]
                ),
                "frontier_task_id": frontier.task_id,
                "pruned_data_ids": report["runtime_pruned_data_ids"],
                "status": "PASS",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
