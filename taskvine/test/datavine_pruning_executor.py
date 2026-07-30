#!/usr/bin/env python3

import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time

from ndcctools.taskvine.datavine.controller.service import ControllerService
from ndcctools.taskvine.datavine.controller.state import ControllerState
from ndcctools.taskvine.datavine.models import TaskRecord
from ndcctools.taskvine.datavine.protocol import DataVineRemoteError
from ndcctools.taskvine.datavine.scheduler.client import ControllerClient
from ndcctools.taskvine.datavine.serialization import serialize


def expect_remote_error(fragment, function, *args, **kwargs):
    try:
        function(*args, **kwargs)
    except DataVineRemoteError as exc:
        assert fragment in str(exc), (fragment, str(exc))
    else:
        raise AssertionError(
            f"expected remote failure containing {fragment!r}"
        )


def wait_for(function, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = function()
        if value:
            return value
        time.sleep(0.01)
    raise TimeoutError("condition did not become true")


def main():
    writing_four = threading.Event()
    release_four = threading.Event()

    def persistence_hook(request, transition):
        if request.data_id == 4 and transition == "before-commit":
            writing_four.set()
            if not release_four.wait(10):
                raise TimeoutError("test did not release persistence")

    with tempfile.TemporaryDirectory(
        prefix="datavine-pruning-executor-"
    ) as temp_dir:
        durable_root = Path(temp_dir) / "durable"
        state = ControllerState(
            completed_pruning_operation_capacity=2
        )
        state.configure_persistence(
            durable_root,
            workers=1,
            queue_capacity=8,
            transition_hook=persistence_hook,
        )
        service = ControllerService(
            "127.0.0.1", 0, "pruning-token", state
        )
        host, port = service.start()
        client = ControllerClient(
            f"http://{host}:{port}", "pruning-token"
        )
        try:
            metadata, function_payload = serialize(sum)
            function_id = client.register_edata(
                metadata, function_payload
            )["data_id"]
            metadata, constant_payload = serialize(1)
            constant_id = client.register_edata(
                metadata, constant_payload
            )["data_id"]

            outputs = [
                client.allocate_idata(task_id)
                for task_id in range(1, 5)
            ]
            records = (
                TaskRecord(
                    1,
                    function_id,
                    (("e", constant_id),),
                    (),
                    outputs[0],
                    (),
                ),
                TaskRecord(
                    2,
                    function_id,
                    (("i", outputs[0]),),
                    (),
                    outputs[1],
                    (outputs[0],),
                ),
                TaskRecord(
                    3,
                    function_id,
                    (("e", constant_id),),
                    (),
                    outputs[2],
                    (),
                ),
                TaskRecord(
                    4,
                    function_id,
                    (("e", constant_id),),
                    (),
                    outputs[3],
                    (),
                ),
            )
            for record in records:
                client.register_task(record)
            payloads = {}
            for task_id, data_id in enumerate(outputs, 1):
                payload = f"idata-{task_id}".encode()
                payloads[data_id] = payload
                client.publish_idata(data_id, 1, payload)
                client.set_task_state(task_id, "completed")

            client.set_required_output(outputs[1], True)
            client.persist_idata(outputs[0])
            wait_for(
                lambda: client.idata_status(outputs[0])[
                    "durability"
                ] == "durable"
            )
            client.persist_idata(outputs[1])
            wait_for(
                lambda: client.idata_status(outputs[1])[
                    "durability"
                ] == "durable"
            )
            first_path = Path(
                client.idata_status(outputs[0])["durable_path"]
            )
            assert first_path.is_file()

            client.persist_idata(outputs[3])
            assert writing_four.wait(10)
            client.persist_idata(outputs[2])
            wait_for(
                lambda: client.idata_status(outputs[2])[
                    "durability"
                ] == "queued"
            )

            client.join_worker("source", 1)
            client.join_worker("destination", 1)
            digest = hashlib.sha256(payloads[outputs[0]]).hexdigest()
            worker_replica = client.report_replica(
                f"i:{outputs[0]}",
                "source-local-i1",
                1,
                "worker-disk",
                digest,
                len(payloads[outputs[0]]),
                "source",
                1,
            )
            lease = client.acquire_replica(
                f"i:{outputs[0]}",
                worker_replica["replica_id"],
                worker_replica["generation"],
                "destination",
                1,
            )

            stale = client.pruning_plan()
            client.set_required_output(outputs[0], True)
            expect_remote_error(
                "proof revision changed",
                client.apply_pruning,
                stale["records"][0]["graph_revision"],
                stale["records"][0]["state_revision"],
                10,
                None,
                100,
            )
            client.set_required_output(outputs[0], False)

            plan = client.pruning_plan()
            result = client.apply_pruning(
                plan["records"][0]["graph_revision"],
                plan["records"][0]["state_revision"],
                grace_seconds=10,
                now=100,
            )
            assert outputs[2] in result["cancelled_persistence"]
            assert any(
                item["action"] == "retiring-active-read"
                and item["data_id"] == outputs[0]
                for item in result["deferred"]
            )
            assert not any(
                item["data_id"] == outputs[0]
                for item in result["applied"]
            )
            assert first_path.exists()
            assert client.fetch_idata(outputs[0]) == payloads[outputs[0]]
            duplicate = client.apply_pruning(
                result["plan"]["records"][0]["graph_revision"],
                result["plan"]["records"][0]["state_revision"],
                grace_seconds=10,
                data_ids=[outputs[0]],
                now=100,
            )
            assert duplicate["deferred"] == result["deferred"]
            assert not duplicate["applied"]
            client.release_replica(lease["lease_id"], True)
            continued = state.continue_deferred_pruning(
                "pruning:test-release",
                [outputs[0]]
            )
            recovered_response = client.continue_deferred_pruning(
                "pruning:test-release", [outputs[0]]
            )
            assert recovered_response == continued
            assert (
                client.continue_deferred_pruning(
                    "pruning:test-release", [outputs[0]]
                )
                == continued
            )
            expect_remote_error(
                "conflicting pruning continuation identity",
                client.continue_deferred_pruning,
                "pruning:test-release",
                [outputs[1]],
            )
            client.continue_deferred_pruning(
                "pruning:bounded-two", []
            )
            bounded_three = client.continue_deferred_pruning(
                "pruning:bounded-three", []
            )
            assert (
                client.continue_deferred_pruning(
                    "pruning:bounded-three", []
                )
                == bounded_three
            )
            bounded_snapshot = client.snapshot()
            assert (
                bounded_snapshot[
                    "completed_pruning_operation_tombstones"
                ]
                == 2
            )
            assert bounded_snapshot[
                "completed_pruning_operation_capacity"
            ] == 2
            assert (
                bounded_snapshot[
                    "completed_pruning_operation_bytes"
                ]
                <= bounded_snapshot[
                    "completed_pruning_operation_byte_capacity"
                ]
            )
            assert (
                bounded_snapshot[
                    "completed_pruning_operation_bytes_high_water"
                ]
                <= bounded_snapshot[
                    "completed_pruning_operation_byte_capacity"
                ]
            )
            assert bounded_snapshot[
                "pruning_continuation_idempotent"
            ] == 3
            assert bounded_snapshot[
                "pruning_continuation_evictions"
            ] == 1
            assert not continued["deferred"]
            assert any(
                item["action"] == "quarantine-sharedfs"
                and item["data_id"] == outputs[0]
                for item in continued["applied"]
            )
            assert any(
                item["action"]
                == "invalidate-worker-pending-delete"
                and item["data_id"] == outputs[0]
                for item in continued["applied"]
            )
            assert not first_path.exists()

            restored = client.restore_quarantined(outputs[0])
            assert restored["restored"][0]["action"] == (
                "restore-quarantine"
            )
            assert first_path.is_file()
            assert client.fetch_idata(outputs[0]) == payloads[outputs[0]]

            plan = client.pruning_plan()
            client.apply_pruning(
                plan["records"][0]["graph_revision"],
                plan["records"][0]["state_revision"],
                grace_seconds=10,
                data_ids=[outputs[0]],
                now=200,
            )
            quarantine_path = Path(
                next(
                    record["path"]
                    for record in client.snapshot()["pruning"]["audits"]
                    if record["action"] == "quarantine-sharedfs"
                    and record["data_id"] == outputs[0]
                )
            )
            original_quarantine_payload = quarantine_path.read_bytes()
            quarantine_path.write_bytes(b"corrupt")
            expect_remote_error(
                "checksum mismatch",
                client.restore_quarantined,
                outputs[0],
            )
            assert not client.replica_sources(
                f"i:{outputs[0]}"
            )["sources"]
            quarantine_path.write_bytes(original_quarantine_payload)
            new_output = client.allocate_idata(5)
            client.register_task(
                TaskRecord(
                    5,
                    function_id,
                    (("i", outputs[0]),),
                    (),
                    new_output,
                    (outputs[0],),
                )
            )
            changed = client.pruning_plan()
            expect_remote_error(
                "no longer proven prunable",
                client.hard_delete_quarantined,
                changed["records"][0]["graph_revision"],
                changed["records"][0]["state_revision"],
                211,
            )
            client.restore_quarantined(outputs[0])
            client.set_task_state(5, "cancelled")

            plan = client.pruning_plan()
            client.apply_pruning(
                plan["records"][0]["graph_revision"],
                plan["records"][0]["state_revision"],
                grace_seconds=10,
                data_ids=[outputs[0]],
                now=300,
            )
            current = client.pruning_plan()
            expect_remote_error(
                "cannot be hard deleted",
                client.hard_delete_quarantined,
                current["records"][0]["graph_revision"],
                current["records"][0]["state_revision"],
                309,
            )
            deleted = client.hard_delete_quarantined(
                current["records"][0]["graph_revision"],
                current["records"][0]["state_revision"],
                311,
            )
            assert any(
                item["action"] == "hard-delete-sharedfs"
                and item["data_id"] == outputs[0]
                for item in deleted["deleted"]
            )
            assert not first_path.exists()
            assert client.fetch_idata(outputs[1]) == payloads[outputs[1]]

            release_four.set()
            wait_for(
                lambda: client.idata_status(outputs[2])[
                    "durability"
                ] == "cancelled"
            )
            wait_for(
                lambda: client.idata_status(outputs[3])[
                    "durability"
                ] in ("durable", "failed")
            )
            snapshot = client.snapshot()
            actions = {
                record["action"]
                for record in snapshot["pruning"]["audits"]
            }
            assert {
                "cancel-persistence",
                "quarantine-sharedfs",
                "restore-quarantine",
                "hard-delete-sharedfs",
            } <= actions
            assert snapshot["pruning"]["quarantined_paths"] == 0
            assert snapshot["pruning"]["audit_records"] <= (
                snapshot["pruning"]["audit_capacity"]
            )
            print(
                json.dumps(
                    {
                        "actions": sorted(actions),
                        "deleted": len(deleted["deleted"]),
                        "pruning": snapshot["pruning"],
                        "replicas": snapshot["replica_directory"],
                    },
                    sort_keys=True,
                )
            )
            print("DataVine physical pruning executor E2E PASS")
        finally:
            release_four.set()
            service.stop()


if __name__ == "__main__":
    main()
