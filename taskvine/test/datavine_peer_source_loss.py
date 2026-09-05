#!/usr/bin/env python3

import argparse
import json
import signal
import time

from ndcctools.taskvine.datavine import Workflow

from datavine_phase4_demand_pull import run_case


# Large enough that the source shutdown races a real transfer, while remaining
# small enough for deterministic local and factory regression runs.
SHARED = b"datavine-peer-source-loss\n" * 524288


def warm(value, delay):
    time.sleep(delay)
    return len(value)


def consume(value, warm_size, ordinal):
    assert len(value) == warm_size
    return warm_size + ordinal


def total(*values):
    return sum(values)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()

    workflow = Workflow()
    warmed = workflow.add_task(
        warm,
        SHARED,
        20 if args.factory_manager else 2,
    )
    consumers = [
        workflow.add_task(consume, SHARED, warmed.output(), ordinal)
        for ordinal in range(8)
    ]
    target = workflow.add_task(
        total, *(task.output() for task in consumers)
    )
    expected = 8 * len(SHARED) + sum(range(8))

    snapshot = run_case(
        "peer-source-loss",
        workflow,
        target.task_id,
        expected,
        worker_count=2,
        worker_cores=1,
        factory_manager=args.factory_manager,
        prefetch=False,
        inject_peer_source_losses=1,
    )
    replicas = snapshot["replica_directory"]
    report = snapshot["scheduler_report"]
    assert report["peer_source_losses_requested"] == 1, report
    assert report["peer_transfer_faults"][
        "peer_source_losses_injected"
    ] == 1, report
    assert report["peer_transfer_faults"][
        "peer_transfer_starts"
    ] >= 1, report
    assert snapshot["taskvine_worker_disconnections"] >= 1, snapshot
    if not args.factory_manager:
        returncodes = snapshot["taskvine_worker_process_returncodes"]
        group_alive = snapshot["taskvine_worker_process_groups_alive"]
        killed = [
            index
            for index, returncode in enumerate(returncodes)
            if returncode == -signal.SIGKILL
        ]
        assert len(killed) == 1, snapshot
        assert not group_alive[killed[0]], snapshot
    assert replicas["peer_transfer_acquires"] >= 1, replicas
    assert replicas["peer_transfer_failures"] >= 1, replicas
    assert (
        replicas["peer_transfer_acquires"]
        == replicas["peer_transfer_releases"]
    ), replicas
    assert replicas["active_leases"] == 0, replicas
    assert snapshot["available_idata"] == len(workflow.tasks), snapshot

    sizes = snapshot["edata_sizes_by_id"]
    shared_id = max(sizes, key=sizes.get)
    fetches = snapshot["edata_fetches_by_id"].get(shared_id, 0)
    assert 1 <= fetches <= 2, snapshot

    print(json.dumps(snapshot, sort_keys=True))
    print(
        "DataVine peer source process-loss E2E PASS "
        f"shared=e{shared_id} controller_fetches={fetches} "
        f"failed_leases={replicas['peer_transfer_failures']}"
    )


if __name__ == "__main__":
    main()
