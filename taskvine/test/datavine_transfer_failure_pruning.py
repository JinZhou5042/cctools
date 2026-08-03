#!/usr/bin/env python3

import argparse
import json

from ndcctools.taskvine.datavine import Workflow

from datavine_phase4_demand_pull import run_case


PAYLOAD_BYTES = 48_600_009
TRANSFER_CUT_BYTES = 4096


def produce(size):
    return b"p" * size


def hold_local(value, delay):
    import time

    time.sleep(delay)
    return len(value)


def consume_peer(value):
    return len(value)


def durable_frontier(local_size, peer_size):
    assert local_size == peer_size
    return local_size + peer_size


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()

    workflow = Workflow()
    source = workflow.add_task(produce, PAYLOAD_BYTES)
    holder = workflow.add_task(
        hold_local,
        source.output(),
        30 if args.factory_manager else 5,
    )
    peer_consumer = workflow.add_task(
        consume_peer, source.output()
    )
    frontier = workflow.add_task(
        durable_frontier,
        holder.output(),
        peer_consumer.output(),
    )

    snapshot = run_case(
        "transfer-failure-pruning",
        workflow,
        frontier.task_id,
        2 * PAYLOAD_BYTES,
        factory_manager=args.factory_manager,
        worker_count=2,
        worker_cores=1,
        prefetch=False,
        persistence=True,
        persistence_parent=(
            "/groups/dthain/users/jzhou24/factory-scratch"
            if args.factory_manager
            else None
        ),
        max_inline_idata_bytes=1024,
        persistence_attempts_by_task={frontier.task_id: 1},
        prune_after_persistence_by_task={
            frontier.task_id: (source.task_id,)
        },
        inject_peer_source_loss_after_bytes=TRANSFER_CUT_BYTES,
        defer_peer_source_loss_after_bytes=True,
        peer_transfer_pruning_probe_task_ids=(source.task_id,),
        replacement_worker_delay=(
            None if args.factory_manager else 3
        ),
    )

    report = snapshot["scheduler_report"]
    faults = report["peer_transfer_faults"]
    probes = report["peer_transfer_pruning_probes"]
    replicas = snapshot["replica_directory"]
    assert report["deferred_peer_source_loss_after_bytes"], report
    assert report["peer_transfer_pruning_probe_task_ids"] == [
        source.task_id
    ], report
    assert len(probes) == 1, report
    probe = probes[0]
    assert len(probe["records"]) == 1, probe
    probe_record = probe["records"][0]
    assert probe_record["data_id"] == source.task_id, probe
    assert probe_record["decision"] != "prune", probe
    observed = probe["partial_bytes"]
    assert TRANSFER_CUT_BYTES <= observed < PAYLOAD_BYTES, probe
    assert faults["deferred_peer_source_loss_pauses"] == 1, faults
    assert faults["deferred_peer_source_loss_triggers"] == 1, faults
    assert faults["deferred_peer_source_loss_expirations"] == 0, faults
    assert faults["deferred_peer_source_loss_pending"] == 0, faults
    assert faults["peer_source_losses_injected"] == 1, faults
    assert faults["peer_transfer_cleanup_reports"] >= 1, faults
    assert (
        faults["peer_transfer_cleanup_reports"]
        == faults["peer_transfer_cleanup_absent"]
    ), faults
    assert faults["peer_transfer_cleanup_pending"] == 0, faults
    assert snapshot["taskvine_worker_disconnections"] >= 1, snapshot
    assert report["attempts_by_task"][str(source.task_id)] >= 2, report
    assert report["recovery_reexecutions"] >= 1, report
    assert report["runtime_pruned_data_ids"] == [
        source.task_id
    ], report
    event = report["frontier_pruning"][0]
    worker_prune = next(
        item
        for item in event["result"]["worker_prunes"]
        if item["data_id"] == source.task_id
    )
    assert worker_prune["confirmed"] == worker_prune["requested"]
    assert (
        worker_prune["confirmed"] + worker_prune.get("reconciled", 0)
        >= 1
    )
    assert worker_prune["failed"] == 0
    assert worker_prune["tracker_released"]
    assert replicas["active_leases"] == 0, replicas
    assert (
        replicas["peer_transfer_acquires"]
        == replicas["peer_transfer_releases"]
    ), replicas
    assert snapshot["durable_hashes_valid"], snapshot
    assert snapshot["persistence_temporary_files"] == [], snapshot

    result = {
        "cleanup_reports": faults["peer_transfer_cleanup_reports"],
        "partial_bytes": observed,
        "probe_decision": probe_record["decision"],
        "recovery_reexecutions": report["recovery_reexecutions"],
        "source_attempts": report["attempts_by_task"][
            str(source.task_id)
        ],
        "worker_prunes": worker_prune["confirmed"],
    }
    print(json.dumps(result, sort_keys=True))
    print(
        "DataVine positive-byte transfer/pruning E2E PASS "
        f"partial_bytes={result['partial_bytes']} "
        f"probe={result['probe_decision']} "
        f"source_attempts={result['source_attempts']} "
        f"worker_prunes={result['worker_prunes']}"
    )


if __name__ == "__main__":
    main()
