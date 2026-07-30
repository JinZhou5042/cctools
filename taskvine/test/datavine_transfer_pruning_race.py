#!/usr/bin/env python3

import argparse
import json
import time

from ndcctools.taskvine.datavine import Workflow

from datavine_phase4_demand_pull import run_case


PAYLOAD_BYTES = 8_000_123


def produce(size):
    return b"d" * size


def hold_local(value, delay):
    time.sleep(delay)
    return len(value)


def consume_peer(value):
    return len(value)


def durable_frontier(local_size, peer_size):
    assert local_size == peer_size
    return local_size + peer_size


def run_pruning_mode(factory_manager):
    workflow = Workflow()
    source = workflow.add_task(produce, PAYLOAD_BYTES)
    holder = workflow.add_task(
        hold_local,
        source.output(),
        1.5 if factory_manager else 0.75,
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
        "transfer-pruning-race",
        workflow,
        frontier.task_id,
        2 * PAYLOAD_BYTES,
        factory_manager=factory_manager,
        worker_count=2,
        worker_cores=1,
        prefetch=False,
        persistence=True,
        persistence_parent=(
            "/groups/dthain/users/jzhou24/factory-scratch"
            if factory_manager
            else None
        ),
        max_inline_idata_bytes=1024,
        persistence_attempts_by_task={frontier.task_id: 1},
        prune_after_persistence_by_task={
            frontier.task_id: (source.task_id,)
        },
        inject_idata_release_failures=1,
        peer_release_retry_seconds=(
            8 if factory_manager else 4
        ),
    )

    report = snapshot["scheduler_report"]
    faults = report["peer_transfer_faults"]
    replicas = snapshot["replica_directory"]
    events = report["frontier_pruning"]
    assert report["idata_release_failures_requested"] == 1, report
    assert faults["peer_release_failures_injected"] == 1, faults
    assert faults["peer_release_retries_succeeded"] == 1, faults
    assert faults["peer_release_pending_high_water"] == 1, faults
    assert faults["peer_release_pending"] == 0, faults
    assert (
        faults["peer_release_pending_high_water"]
        <= faults["peer_release_pending_capacity"]
    ), faults
    assert len(events) == 1, report
    event = events[0]
    initial_deferred = event["result"]["controller"]["deferred"]
    assert [item["data_id"] for item in initial_deferred] == [
        source.task_id
    ], event
    assert initial_deferred[0]["active_leases"] == 1, event
    continuations = event["result"]["controller_continuations"]
    assert continuations, event
    assert any(
        item["data_id"] == source.task_id
        for continuation in continuations
        for item in continuation["applied"]
    ), event
    worker_prune = next(
        item
        for item in event["result"]["worker_prunes"]
        if item["data_id"] == source.task_id
    )
    assert worker_prune["requested"] >= 1, worker_prune
    assert (
        worker_prune["confirmed"] == worker_prune["requested"]
    ), worker_prune
    assert worker_prune["failed"] == 0, worker_prune
    assert worker_prune["tracker_released"], worker_prune
    assert report["runtime_pruned_data_ids"] == [
        source.task_id
    ], report
    assert replicas["observed_transfer_acquires"] >= 1, replicas
    assert (
        replicas["observed_transfer_acquires"]
        == replicas["observed_transfer_releases"]
    ), replicas
    assert replicas["active_leases"] == 0, replicas
    assert snapshot["durable_hashes_valid"], snapshot
    assert snapshot["persistence_temporary_files"] == [], snapshot
    assert snapshot["taskvine_worker_disconnections"] == 0, snapshot
    assert snapshot["taskvine_workers_used"] == 2, snapshot
    workers = snapshot["taskvine_worker_by_task"]
    assert workers[str(source.task_id)] != workers[
        str(peer_consumer.task_id)
    ], workers

    return {
        "active_leases_at_initial_prune": (
            initial_deferred[0]["active_leases"]
        ),
        "idata": source.task_id,
        "release_retries": (
            faults["peer_release_retries_succeeded"]
        ),
        "worker_prunes": worker_prune["confirmed"],
    }


def run_capacity_mode(factory_manager):
    workflow = Workflow()
    source = workflow.add_task(produce, PAYLOAD_BYTES)
    holder = workflow.add_task(
        hold_local,
        source.output(),
        0.5 if factory_manager else 0.2,
    )
    peer_consumer = workflow.add_task(
        consume_peer, source.output()
    )
    target = workflow.add_task(
        durable_frontier,
        holder.output(),
        peer_consumer.output(),
    )
    snapshot = run_case(
        "transfer-release-capacity",
        workflow,
        target.task_id,
        2 * PAYLOAD_BYTES,
        factory_manager=factory_manager,
        worker_count=2,
        worker_cores=1,
        prefetch=False,
        max_inline_idata_bytes=1024,
        inject_idata_release_failures=1,
        peer_release_retry_seconds=(
            15 if factory_manager else 10
        ),
        peer_release_capacity=1,
    )
    report = snapshot["scheduler_report"]
    faults = report["peer_transfer_faults"]
    replicas = snapshot["replica_directory"]
    assert report["peer_release_capacity"] == 1, report
    assert faults["peer_release_pending_capacity"] == 1, faults
    assert faults["peer_release_pending_high_water"] == 1, faults
    assert faults["peer_release_capacity_backpressure"] > 0, faults
    assert faults["peer_release_failures_injected"] == 1, faults
    assert faults["peer_release_retries_succeeded"] == 1, faults
    assert faults["peer_release_pending"] == 0, faults
    assert replicas["observed_transfer_acquires"] >= 2, replicas
    assert (
        replicas["observed_transfer_acquires"]
        == replicas["observed_transfer_releases"]
    ), replicas
    assert replicas["active_leases"] == 0, replicas
    assert snapshot["taskvine_worker_disconnections"] == 0, snapshot
    assert snapshot["taskvine_workers_used"] == 2, snapshot
    workers = snapshot["taskvine_worker_by_task"]
    assert workers[str(source.task_id)] != workers[
        str(peer_consumer.task_id)
    ], workers
    return {
        "backpressure_observations": (
            faults["peer_release_capacity_backpressure"]
        ),
        "capacity": faults["peer_release_pending_capacity"],
        "high_water": faults["peer_release_pending_high_water"],
        "release_retries": (
            faults["peer_release_retries_succeeded"]
        ),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()
    result = {
        "bounded_release_capacity": run_capacity_mode(
            args.factory_manager
        ),
        "transfer_pruning": run_pruning_mode(
            args.factory_manager
        ),
    }
    print(json.dumps(result, sort_keys=True))
    print(
        "DataVine real-transfer pruning continuation E2E PASS "
        f"idata=i:{result['transfer_pruning']['idata']} "
        f"initial_active_leases="
        f"{result['transfer_pruning']['active_leases_at_initial_prune']} "
        f"release_retries="
        f"{result['transfer_pruning']['release_retries']} "
        f"worker_prunes="
        f"{result['transfer_pruning']['worker_prunes']} "
        f"capacity_backpressure="
        f"{result['bounded_release_capacity']['backpressure_observations']}"
    )


if __name__ == "__main__":
    main()
