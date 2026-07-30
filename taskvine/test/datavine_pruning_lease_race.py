#!/usr/bin/env python3

import argparse
import json
import threading
import time

from ndcctools.taskvine.datavine import Workflow

from datavine_phase4_demand_pull import run_case


def seed(value):
    return value


def delayed_advance(value, increment, seconds):
    time.sleep(seconds)
    return value + increment


def advance(value, increment):
    return value + increment


def build():
    workflow = Workflow()
    root = workflow.add_task(seed, 10)
    middle = workflow.add_task(
        delayed_advance, root.output(), 1, 1.5
    )
    frontier = workflow.add_task(advance, middle.output(), 2)
    target = workflow.add_task(advance, frontier.output(), 3)
    return workflow, root, middle, frontier, target


def lease_hook(data_id, invalidate_proof, observations):
    def install(client):
        deadline = time.monotonic() + 20
        source = None
        while time.monotonic() < deadline:
            records = client.replica_records(f"i:{data_id}")[
                "records"
            ]
            source = next(
                (
                    record
                    for record in records
                    if record["state"] == "available"
                    and record["tier"] in (
                        "worker-dram",
                        "worker-disk",
                    )
                ),
                None,
            )
            if source is not None:
                break
            time.sleep(0.02)
        if source is None:
            raise TimeoutError("worker source did not become available")
        client.join_worker("lease-race-destination", 1)
        lease = client.acquire_replica(
            f"i:{data_id}",
            source["replica_id"],
            source["generation"],
            "lease-race-destination",
            1,
        )
        observations["lease_id"] = lease["lease_id"]

        def finish():
            try:
                deadline_value = time.monotonic() + 20
                while time.monotonic() < deadline_value:
                    current = next(
                        record
                        for record in client.replica_records(
                            f"i:{data_id}"
                        )["records"]
                        if record["replica_id"]
                        == source["replica_id"]
                    )
                    if current["state"] == "retiring":
                        observations["retiring_observed"] = True
                        break
                    time.sleep(0.02)
                else:
                    raise TimeoutError(
                        "pruning did not retire the leased source"
                    )
                if invalidate_proof:
                    client.set_required_output(data_id, True)
                    observations["proof_invalidated"] = True
                client.release_replica(lease["lease_id"], True)
                deadline_value = time.monotonic() + 20
                expected = (
                    "available" if invalidate_proof else "pruned"
                )
                while time.monotonic() < deadline_value:
                    current = next(
                        record
                        for record in client.replica_records(
                            f"i:{data_id}"
                        )["records"]
                        if record["replica_id"]
                        == source["replica_id"]
                    )
                    if current["state"] == expected:
                        observations["final_state"] = expected
                        return
                    time.sleep(0.02)
                raise TimeoutError(
                    f"leased source did not reach {expected}"
                )
            except Exception as exc:
                observations["error"] = repr(exc)

        thread = threading.Thread(
            target=finish,
            name=f"datavine-lease-race-{data_id}",
        )
        thread.start()
        return thread

    return install


def run_mode(factory_manager, invalidate_proof):
    workflow, root, middle, frontier, target = build()
    observations = {}
    snapshot = run_case(
        (
            "pruning-lease-proof-invalidation"
            if invalidate_proof
            else "pruning-lease-release"
        ),
        workflow,
        target.task_id,
        16,
        factory_manager=factory_manager,
        worker_count=2,
        worker_cores=1,
        persistence=True,
        persistence_parent=(
            "/groups/dthain/users/jzhou24/factory-scratch"
            if factory_manager
            else None
        ),
        persistence_attempts_by_task={frontier.task_id: 1},
        prune_after_persistence_by_task={
            frontier.task_id: (root.task_id, middle.task_id)
        },
        runtime_controller_hook=lease_hook(
            root.task_id, invalidate_proof, observations
        ),
    )
    assert "error" not in observations, observations
    assert observations["retiring_observed"]
    report = snapshot["scheduler_report"]
    event = report["frontier_pruning"][0]
    assert event["result"]["controller"]["deferred"]
    assert event["result"]["controller_continuations"]
    assert snapshot["durable_hashes_valid"]
    assert snapshot["persistence_temporary_files"] == []
    if invalidate_proof:
        assert observations["proof_invalidated"]
        assert observations["final_state"] == "available"
        assert event["cancelled_data_ids"] == [root.task_id]
        assert report["runtime_pruned_data_ids"] == [middle.task_id]
    else:
        assert observations["final_state"] == "pruned"
        assert event["cancelled_data_ids"] == []
        assert report["runtime_pruned_data_ids"] == [
            root.task_id,
            middle.task_id,
        ]
    return {
        "cancelled_data_ids": event["cancelled_data_ids"],
        "final_state": observations["final_state"],
        "runtime_pruned_data_ids": report["runtime_pruned_data_ids"],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()
    released = run_mode(args.factory_manager, False)
    invalidated = run_mode(args.factory_manager, True)
    print(
        json.dumps(
            {
                "lease_release": released,
                "proof_invalidation": invalidated,
                "status": "PASS",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
