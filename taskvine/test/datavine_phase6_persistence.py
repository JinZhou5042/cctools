#!/usr/bin/env python3

import argparse
import json

from ndcctools.taskvine.datavine import Workflow
from datavine_phase4_demand_pull import run_case


def produce(value):
    return value * 2


def combine(*values):
    return sum(values)


def build():
    workflow = Workflow()
    parts = [workflow.add_task(produce, index) for index in range(6)]
    target = workflow.add_task(
        combine, *(task.output() for task in parts)
    )
    return workflow, target, 30


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()
    workflow, target, expected = build()
    enabled = run_case(
        "phase6-persistence",
        workflow,
        target.task_id,
        expected,
        worker_count=2,
        persistence=True,
        factory_manager=args.factory_manager,
    )
    assert enabled["durability"]["durable"] == len(workflow.tasks)
    assert enabled["persistence_max_active"] == 1
    assert len(enabled["durable_files"]) == len(workflow.tasks)

    workflow, target, expected = build()
    recovered = run_case(
        "phase6-write-retry",
        workflow,
        target.task_id,
        expected,
        worker_count=2,
        persistence=True,
        persistence_fail_first=True,
        factory_manager=args.factory_manager,
    )
    assert recovered["durability"]["durable"] == len(workflow.tasks)
    assert recovered["durability"]["failed"] == 0
    assert recovered["persistence_requests"] == len(workflow.tasks) + 1

    workflow, target, expected = build()
    disabled = run_case(
        "phase6-disabled",
        workflow,
        target.task_id,
        expected,
        worker_count=2,
        persistence=False,
        factory_manager=args.factory_manager,
    )
    assert disabled["durability"]["volatile"] == len(workflow.tasks)
    assert disabled["durable_files"] == []
    print(
        json.dumps(
            {
                "enabled": enabled,
                "write_retry": recovered,
                "disabled": disabled,
            },
            sort_keys=True,
        )
    )
    print("DataVine Phase 6 controlled persistence E2E PASS")


if __name__ == "__main__":
    main()
