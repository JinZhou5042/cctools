#!/usr/bin/env python3

import argparse
import hashlib
import json

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
        max_idata_bytes=CONTROLLER_IDATA_LIMIT,
        max_inline_idata_bytes=INLINE_LIMIT,
        inject_worker_loss_after=(
            None if factory_manager else large.task_id
        ),
        replacement_worker_delay=None if factory_manager else 1,
    )
    report = snapshot["scheduler_report"]
    assert snapshot["idata_capacity_bytes"] == CONTROLLER_IDATA_LIMIT
    assert (
        snapshot["idata_inline_object_capacity_bytes"] == INLINE_LIMIT
    )
    assert snapshot["idata_bytes"] <= CONTROLLER_IDATA_LIMIT
    assert snapshot["idata_bytes_high_water"] <= CONTROLLER_IDATA_LIMIT
    assert snapshot["idata_metadata_records"] >= 1
    assert snapshot["idata_metadata_publications"] >= 1
    assert snapshot["idata_inline_records"] >= 1
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
                "scheduler_report": snapshot["scheduler_report"],
                "status": "PASS",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
