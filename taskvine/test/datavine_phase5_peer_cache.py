#!/usr/bin/env python3

import argparse
import json
import time

from ndcctools.taskvine.datavine import Workflow

from datavine_phase4_demand_pull import run_case


SHARED = b"datavine-phase5-peer-cache\n" * 65536


def warm(value, delay):
    time.sleep(delay)
    return len(value)


def consume(value, warm_size, ordinal):
    assert len(value) == warm_size
    time.sleep(0.5)
    return warm_size + ordinal


def total(*values):
    return sum(values)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()
    workflow = Workflow()
    warmed = workflow.add_task(
        warm, SHARED, 20 if args.factory_manager else 0
    )
    consumers = [
        workflow.add_task(
            consume, SHARED, warmed.output(), ordinal
        )
        for ordinal in range(6)
    ]
    target = workflow.add_task(
        total, *(task.output() for task in consumers)
    )
    expected = 6 * len(SHARED) + sum(range(6))
    snapshot = run_case(
        "phase5-peer-cache",
        workflow,
        target.task_id,
        expected,
        worker_count=2,
        worker_cores=1,
        factory_manager=args.factory_manager,
        prefetch=False,
    )
    sizes = snapshot["edata_sizes_by_id"]
    shared_id = max(sizes, key=sizes.get)
    fetches = snapshot["edata_fetches_by_id"].get(shared_id, 0)
    assert fetches == 1, (
        f"shared EDataID {shared_id} fetched from Controller {fetches} times; "
        "worker cache/peer reuse did not occur"
    )
    assert snapshot["taskvine_workers_used"] == 2, snapshot
    transfer_metrics = snapshot["replica_directory"]
    assert transfer_metrics["observed_transfer_acquires"] >= 1, snapshot
    assert (
        transfer_metrics["observed_transfer_acquires"]
        == transfer_metrics["observed_transfer_releases"]
    ), snapshot
    assert transfer_metrics["active_leases"] == 0, snapshot
    assert snapshot["edata_bytes"] <= snapshot["edata_capacity_bytes"]
    rollback = run_case(
        "phase5-peer-disabled",
        workflow,
        target.task_id,
        expected,
        worker_count=2,
        worker_cores=1,
        peer_transfers=False,
        factory_manager=args.factory_manager,
        prefetch=False,
    )
    assert rollback["available_idata"] == len(workflow.tasks)
    assert (
        rollback["replica_directory"]["observed_transfer_acquires"] == 0
    ), rollback
    print(json.dumps({"peer_on": snapshot, "peer_off": rollback}, sort_keys=True))
    print(
        f"DataVine Phase 5 peer cache E2E PASS shared=e{shared_id} "
        f"controller_fetches={fetches} rollback=PASS"
    )


if __name__ == "__main__":
    main()
