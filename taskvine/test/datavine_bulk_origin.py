#!/usr/bin/env python3

import argparse
import dataclasses
import hashlib
import json
from pathlib import Path
import tempfile

from datavine_phase4_demand_pull import run_case
from ndcctools.taskvine.datavine import Workflow
from ndcctools.taskvine.datavine.controller.state import ControllerState
from ndcctools.taskvine.datavine.models import EDataRecord
from ndcctools.taskvine.datavine.serialization import serialize


BULK_SIZE = 4 * 1024 * 1024
BULK = bytes(index % 251 for index in range(BULK_SIZE))


def inspect_bulk(first, second, ordinal):
    assert first is second
    return (hashlib.sha256(first).hexdigest(), len(first), ordinal)


def collect(*values):
    return tuple(values)


def make_workflow():
    workflow = Workflow()
    branches = [
        workflow.add_task(inspect_bulk, BULK, BULK, ordinal)
        for ordinal in range(4)
    ]
    final = workflow.add_task(
        collect, *(branch.output() for branch in branches)
    )
    oracle = tuple(
        (hashlib.sha256(BULK).hexdigest(), BULK_SIZE, ordinal)
        for ordinal in range(4)
    )
    return workflow, final.task_id, oracle


def origin_contract():
    with tempfile.TemporaryDirectory(
        prefix="datavine-bulk-contract-"
    ) as temporary:
        root = Path(temporary)
        allowed = root / "allowed"
        allowed.mkdir()
        metadata, payload = serialize(BULK)
        digest = EDataRecord.digest(metadata, payload)
        origin = allowed / f"edata-{digest}.pkl"
        origin.write_bytes(payload)
        state = ControllerState(
            max_edata_bytes=1024, bulk_origin_root=allowed
        )
        first = state.register_edata_origin(
            metadata, origin, digest, len(payload)
        )
        second = state.register_edata_origin(
            metadata, origin, digest, len(payload)
        )
        assert first.data_id == second.data_id == 1
        inline_duplicate = state.register_edata(metadata, payload)
        assert inline_duplicate.data_id == first.data_id
        assert inline_duplicate.serialized_bytes is None
        different_metadata, different_payload = serialize(BULK + b"x")
        try:
            state.register_edata(different_metadata, different_payload)
        except MemoryError:
            pass
        else:
            raise AssertionError("bulk payload entered Controller memory")
        try:
            state.register_edata_origin(
                metadata, origin, "0" * 64, len(payload)
            )
        except ValueError:
            pass
        else:
            raise AssertionError("incorrect bulk hash was accepted")
        outside = root / f"edata-{digest}.pkl"
        outside.write_bytes(payload)
        try:
            state.register_edata_origin(
                metadata, outside, digest, len(payload)
            )
        except ValueError:
            pass
        else:
            raise AssertionError("out-of-root bulk path was accepted")
        symlink = allowed / f"edata-{'1' * 64}.pkl"
        symlink.symlink_to(origin)
        try:
            state.register_edata_origin(
                metadata, symlink, "1" * 64, len(payload)
            )
        except ValueError:
            pass
        else:
            raise AssertionError("symbolic-link bulk path was accepted")
        snapshot = state.snapshot()
        assert snapshot["edata_bulk_records"] == 1
        assert snapshot["edata_bytes"] == 0
        assert snapshot["edata_bulk_bytes"] == len(payload)
        domain_state = ControllerState(max_edata_bytes=1024)
        small_metadata, small_payload = serialize(42)
        value_record = domain_state.register_edata(
            dataclasses.replace(small_metadata, domain="value"),
            small_payload,
        )
        function_record = domain_state.register_edata(
            dataclasses.replace(small_metadata, domain="function"),
            small_payload,
        )
        assert value_record.data_id != function_record.data_id
        return {
            "deduplicated_origin_registration": True,
            "inline_capacity_rejected_bulk": True,
            "bad_hash_rejected": True,
            "out_of_root_rejected": True,
            "symlink_rejected": True,
            "serialization_domains_isolated": True,
        }


def validate(snapshot):
    bulk_ids = [
        data_id
        for data_id, size in snapshot["edata_sizes_by_id"].items()
        if size >= BULK_SIZE
    ]
    assert len(bulk_ids) == 1, bulk_ids
    bulk_id = bulk_ids[0]
    assert snapshot["edata_bulk_records"] == 1
    assert snapshot["edata_bulk_bytes"] >= BULK_SIZE
    assert snapshot["edata_bytes"] < 1024 * 1024
    assert len(snapshot["bulk_origin_files"]) == 1
    assert bulk_id not in snapshot["edata_fetches_by_id"]
    assert snapshot["byte_serving"]["bytes_served"] < BULK_SIZE
    assert snapshot["byte_serving"]["rejected"] == 0
    assert snapshot["taskvine_workers_used"] == 2
    assert snapshot["registrations"] == 7
    assert snapshot["deduplicated_registrations"] == 0
    report = snapshot["scheduler_report"]
    assert report["edata_serializations"] == 7
    assert report["bulk_edata_serializations"] == 1
    return {
        "bulk_data_id": int(bulk_id),
        "bulk_bytes": snapshot["edata_bulk_bytes"],
        "controller_inline_bytes": snapshot["edata_bytes"],
        "controller_bytes_served": snapshot["byte_serving"]["bytes_served"],
        "deduplicated_registrations": (
            snapshot["deduplicated_registrations"]
        ),
        "edata_serializations": report["edata_serializations"],
        "bulk_edata_serializations": (
            report["bulk_edata_serializations"]
        ),
        "workers_used": snapshot["taskvine_workers_used"],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    parser.add_argument(
        "--bulk-origin-parent",
        default="/groups/dthain/users/jzhou24/factory-scratch",
    )
    args = parser.parse_args()
    workflow, target, oracle = make_workflow()
    snapshot = run_case(
        "bulk-origin",
        workflow,
        target,
        oracle,
        factory_manager=args.factory_manager,
        worker_count=2,
        bulk_threshold=1024 * 1024,
        bulk_origin_parent=(
            args.bulk_origin_parent if args.factory_manager else None
        ),
        max_edata_bytes=1024 * 1024,
        max_serving_bytes=1024 * 1024,
        prefetch=False,
    )
    result = validate(snapshot)
    result["origin_contract"] = origin_contract()
    result["status"] = "PASS"
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
