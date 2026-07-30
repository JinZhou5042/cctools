#!/usr/bin/env python3

import argparse
import hashlib
import json
from pathlib import Path
import tempfile

from datavine_phase4_demand_pull import run_case
from ndcctools.taskvine.datavine import TaskRecord, Workflow
from ndcctools.taskvine.datavine.controller.state import ControllerState
from ndcctools.taskvine.datavine.serialization import serialize


LARGE_SIZE = 2 * 1024 * 1024
INLINE_LIMIT = 64 * 1024
CONTROLLER_IDATA_LIMIT = 128 * 1024


def state_capacity_contract():
    state = ControllerState(
        max_idata_bytes=8,
        max_inline_idata_bytes=6,
    )
    metadata, function_payload = serialize(bytes)
    function = state.register_edata(metadata, function_payload)
    first = state.allocate_idata(1)
    second = state.allocate_idata(2)
    state.register_task(
        TaskRecord(1, function.data_id, (), (), first.data_id, ())
    )
    state.register_task(
        TaskRecord(2, function.data_id, (), (), second.data_id, ())
    )
    state.publish_idata(first.data_id, 1, b"123456")
    try:
        state.publish_idata(second.data_id, 1, b"abc")
    except MemoryError:
        pass
    else:
        raise AssertionError("Controller accepted IData beyond total capacity")
    try:
        state.publish_idata(second.data_id, 1, b"1234567")
    except MemoryError:
        pass
    else:
        raise AssertionError("Controller accepted oversized inline IData")
    metadata = state.publish_idata_metadata(
        second.data_id,
        1,
        hashlib.sha256(b"1234567").hexdigest(),
        7,
    )
    assert metadata.serialized_bytes is None
    snapshot = state.snapshot()
    assert snapshot["idata_bytes"] == 6
    assert snapshot["idata_bytes_high_water"] == 6
    assert snapshot["idata_metadata_records"] == 1
    return snapshot


def external_persistence_idempotency_contract():
    with tempfile.TemporaryDirectory(
        prefix="datavine-external-persistence-contract-"
    ) as root:
        state = ControllerState(
            max_idata_bytes=8,
            max_inline_idata_bytes=6,
        )
        state.configure_persistence(root)
        try:
            metadata, function_payload = serialize(bytes)
            function = state.register_edata(
                metadata, function_payload
            )
            record = state.allocate_idata(1)
            state.register_task(
                TaskRecord(
                    1,
                    function.data_id,
                    (),
                    (),
                    record.data_id,
                    (),
                )
            )
            payload = b"1234567"
            state.publish_idata_metadata(
                record.data_id,
                1,
                hashlib.sha256(payload).hexdigest(),
                len(payload),
            )
            state.join_worker("persistence-source", 1)
            state.report_worker_replica(
                f"i:{record.data_id}",
                "persistence-source-replica",
                1,
                "worker-disk",
                hashlib.sha256(payload).hexdigest(),
                len(payload),
                "persistence-source",
                1,
            )
            state.request_persistence(record.data_id)
            request = state.idata_status(record.data_id)[
                "persistence_request"
            ]
            state.begin_external_persistence(
                record.data_id, request["request_id"]
            )
            Path(request["target_path"]).write_bytes(payload)
            first = state.complete_external_persistence(
                record.data_id, request["request_id"]
            )
            second = state.complete_external_persistence(
                record.data_id, request["request_id"]
            )
            repeated_begin = state.begin_external_persistence(
                record.data_id, request["request_id"]
            )
            assert first == second
            assert repeated_begin["state"] == "durable"
            return state.snapshot()
        finally:
            state.stop()


def make_large_payload(size):
    return bytes(index % 251 for index in range(size))


def payload_digest(payload):
    return hashlib.sha256(payload).hexdigest()


def run_bounded_case(factory_manager=None):
    workflow = Workflow()
    large = workflow.add_task(make_large_payload, LARGE_SIZE)
    target = workflow.add_task(payload_digest, large.output())
    oracle = hashlib.sha256(
        bytes(index % 251 for index in range(LARGE_SIZE))
    ).hexdigest()
    snapshot = run_case(
        "idata-capacity",
        workflow,
        target.task_id,
        oracle,
        factory_manager=factory_manager,
        worker_count=1,
        worker_cores=1,
        prefetch=False,
        persistence=True,
        validate_durable_recovery=True,
        max_idata_bytes=CONTROLLER_IDATA_LIMIT,
        max_inline_idata_bytes=INLINE_LIMIT,
        inject_worker_loss_after=large.task_id,
        replacement_worker_delay=None if factory_manager else 1,
        persistence_parent=(
            "/groups/dthain/users/jzhou24/factory-scratch"
            if factory_manager
            else None
        ),
        additional_result_task_ids=(large.task_id,),
    )
    report = snapshot["scheduler_report"]
    assert snapshot["idata_capacity_bytes"] == CONTROLLER_IDATA_LIMIT
    assert (
        snapshot["idata_inline_object_capacity_bytes"] == INLINE_LIMIT
    )
    assert snapshot["idata_bytes"] <= CONTROLLER_IDATA_LIMIT
    assert snapshot["idata_bytes_high_water"] <= CONTROLLER_IDATA_LIMIT
    assert snapshot["idata_bytes_after_durable_recovery"] == (
        snapshot["idata_bytes"]
    )
    assert snapshot["idata_metadata_records"] >= 1
    assert snapshot["idata_metadata_publications"] >= 1
    assert snapshot["idata_inline_records"] >= 1
    assert snapshot["external_persistence_requests"] == 1
    assert snapshot["external_persistence_durable"] == 1
    assert snapshot["durable_hashes_valid"]
    assert (
        snapshot["durable_recovery_actions"]["1"]
        == "validated-durable"
    )
    assert len(snapshot["durable_files"]) == 2
    assert report["persistence_tasks_completed"] == 1
    assert report["persistence_worker_bytes"] > LARGE_SIZE
    assert (
        snapshot["result_summaries"][str(large.task_id)][
            "serialized_size"
        ]
        > LARGE_SIZE
    )
    assert report["local_idata_hits"] >= 1, report
    assert report["legacy_recovery_tasks"] == 0, report
    if not factory_manager:
        assert report["worker_loss_injected"], report
        assert report["recovery_reexecutions"] >= 1, report
        assert report["physical_attempts"] > report["logical_tasks"], report
    return snapshot


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()
    contract = state_capacity_contract()
    idempotency = external_persistence_idempotency_contract()
    snapshot = run_bounded_case(args.factory_manager)
    print(
        json.dumps(
            {
                "controller_idata": {
                    key: snapshot[key]
                    for key in (
                        "idata_bytes",
                        "idata_bytes_high_water",
                        "idata_capacity_bytes",
                        "idata_inline_object_capacity_bytes",
                        "idata_inline_records",
                        "idata_metadata_records",
                        "idata_metadata_publications",
                    )
                },
                "state_capacity_contract": {
                    key: contract[key]
                    for key in (
                        "idata_bytes",
                        "idata_bytes_high_water",
                        "idata_capacity_bytes",
                        "idata_inline_object_capacity_bytes",
                        "idata_metadata_records",
                    )
                },
                "external_persistence_idempotency": {
                    "durable": idempotency[
                        "external_persistence_durable"
                    ],
                    "stale_completions": idempotency[
                        "persistence_stale_completions"
                    ],
                },
                "scheduler_report": snapshot["scheduler_report"],
                "status": "PASS",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
