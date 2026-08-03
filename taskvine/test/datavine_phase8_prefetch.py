#!/usr/bin/env python3

import argparse
import json
import time

from ndcctools.taskvine.datavine import Workflow
from datavine_phase4_demand_pull import run_case


SHARED = b"datavine-phase8-prefetch\n" * 65536


def slow_root(delay):
    time.sleep(delay)
    return 10


def consume(shared, root, ordinal):
    return len(shared) + root + ordinal


def total(*values):
    return sum(values)


def build_prefetch():
    workflow = Workflow()
    root = workflow.add_task(slow_root, 3)
    parts = [
        workflow.add_task(
            consume, SHARED, root.output(), ordinal
        )
        for ordinal in range(6)
    ]
    target = workflow.add_task(
        total, *(part.output() for part in parts)
    )
    expected = 6 * (len(SHARED) + 10) + sum(range(6))
    return workflow, target, expected


def produce_list():
    return [1, 2, 3]


def consume_nested(value):
    assert value["left"][0] is value["right"]
    return sum(value["left"][0]) + sum(value["right"])


def build_nested():
    workflow = Workflow()
    source = workflow.add_task(produce_list)
    shared_reference = source.output()
    target = workflow.add_task(
        consume_nested,
        {
            "left": [shared_reference],
            "right": shared_reference,
        },
    )
    return workflow, target, 12


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()

    workflow, target, expected = build_prefetch()
    enabled = run_case(
        "phase8-prefetch",
        workflow,
        target.task_id,
        expected,
        worker_count=2,
        prefetch=True,
        prefetch_byte_budget=8 * 1024 * 1024,
        factory_manager=args.factory_manager,
    )
    report = enabled["scheduler_report"]
    assert report["prefetch_selected"] >= 2
    assert report["prefetch_completed"] == report["prefetch_selected"]
    assert report["prefetch_failed"] == 0
    assert report["prefetch_overlapped"]
    assert report["prefetch_bytes"] <= 8 * 1024 * 1024
    assert (
        enabled["taskvine_running_order"][0]
        not in report["prefetch_task_ids"]
    ), "prefetch traffic ran ahead of ready demand work"
    transfer_metrics = enabled["replica_directory"]
    assert transfer_metrics["source_selection_requests"] > 0
    assert transfer_metrics["source_selection_requests"] >= (
        transfer_metrics["peer_transfer_acquires"]
    )
    assert (
        transfer_metrics["peer_transfer_acquires"]
        == transfer_metrics["peer_transfer_releases"]
    )
    assert transfer_metrics["active_leases"] == 0

    workflow, target, expected = build_prefetch()
    failed = run_case(
        "phase8-prefetch-failure",
        workflow,
        target.task_id,
        expected,
        worker_count=2,
        prefetch=True,
        inject_prefetch_failure=True,
        factory_manager=args.factory_manager,
    )
    assert failed["scheduler_report"]["prefetch_failed"] > 0

    workflow, target, expected = build_prefetch()
    disabled = run_case(
        "phase8-prefetch-disabled",
        workflow,
        target.task_id,
        expected,
        worker_count=2,
        prefetch=False,
        factory_manager=args.factory_manager,
    )
    assert disabled["scheduler_report"]["prefetch_selected"] == 0

    workflow, target, expected = build_nested()
    nested = run_case(
        "phase8-nested-binding",
        workflow,
        target.task_id,
        expected,
        worker_count=2,
        factory_manager=args.factory_manager,
    )
    assert nested["scheduler_report"]["local_idata_hits"] >= 1

    print(
        json.dumps(
            {
                "enabled": enabled,
                "prefetch_failure": failed,
                "disabled": disabled,
                "nested": nested,
            },
            sort_keys=True,
        )
    )
    print("DataVine Phase 8 prefetch/adaptive placement E2E PASS")


if __name__ == "__main__":
    main()
