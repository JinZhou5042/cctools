#!/usr/bin/env python3

import argparse
import json
import time

from ndcctools.taskvine.datavine import Workflow

from datavine_phase4_demand_pull import run_case


SHARED = b"datavine-peer-corruption\n" * 320_000


def warm(value, ordinal):
    return len(value) + ordinal


def hold(value, warmed, ordinal, delay):
    assert warmed == len(value) + ordinal
    time.sleep(delay)
    return warmed


def consume(value, first, second):
    assert first == len(value)
    assert second == len(value) + 1
    return len(value) + first + second


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()

    workflow = Workflow()
    first = workflow.add_task(warm, SHARED, 0)
    second = workflow.add_task(warm, SHARED, 1)
    delay = 60 if args.factory_manager else 20
    workflow.add_task(hold, SHARED, first.output(), 0, delay)
    workflow.add_task(hold, SHARED, second.output(), 1, delay)
    target = workflow.add_task(
        consume, SHARED, first.output(), second.output()
    )
    expected = 3 * len(SHARED) + 1

    snapshot = run_case(
        "peer-corruption",
        workflow,
        target.task_id,
        expected,
        worker_count=3,
        worker_cores=1,
        factory_manager=args.factory_manager,
        prefetch=False,
        inject_peer_corruptions=1,
    )
    report = snapshot["scheduler_report"]
    faults = report["peer_transfer_faults"]
    replicas = snapshot["replica_directory"]
    assert report["peer_corruptions_requested"] == 1, report
    assert faults["peer_corruptions_injected"] == 1, report
    assert faults["peer_corruptions_rejected"] == 1, report
    assert faults["peer_alternate_source_fallbacks"] == 1, report
    assert faults["peer_corrupt_fallback_pending"] == 0, report
    assert faults["peer_source_losses_injected"] == 0, report
    assert snapshot["taskvine_worker_disconnections"] == 0, snapshot
    assert snapshot["taskvine_workers_used"] == 3, snapshot
    assert replicas["peer_transfer_acquires"] >= 2, replicas
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
        "DataVine corrupt peer alternate-source E2E PASS "
        f"shared=e{shared_id} controller_fetches={fetches} "
        f"peer_fallbacks={faults['peer_alternate_source_fallbacks']}"
    )


if __name__ == "__main__":
    main()
