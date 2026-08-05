#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time

from datavine_phase4_demand_pull import run_case
from ndcctools.taskvine.datavine import Workflow
from ndcctools.taskvine.datavine.cache import WorkerCacheAdmission
from ndcctools.taskvine.datavine.worker.cache import WorkerDiskStore
from ndcctools.taskvine.datavine.worker.cache import WorkerMemoryStore


HOT = b"datavine-hot-cache-value\n" * 4096


def worker_index_contract():
    cache = WorkerMemoryStore()
    started = time.monotonic()
    for index in range(10000):
        data_key = f"e:{index}"
        payload = index.to_bytes(8, "big")
        assert cache.put(
            "scope", data_key, str(index), payload, 1024 * 1024, 1, False
        )[0]
        assert cache.local("scope", data_key) == payload
    dram_seconds = time.monotonic() - started
    assert dram_seconds < 2, dram_seconds
    assert cache.put(
        "scope", "e:1", "new", b"new", 1024 * 1024, 1, False
    )[0]
    assert cache.local("scope", "e:1") == b"new"

    with tempfile.TemporaryDirectory(
        prefix="datavine-worker-index-"
    ) as root:
        previous = os.environ.get("WORKER_TMPDIR")
        os.environ["WORKER_TMPDIR"] = root
        try:
            disk = WorkerDiskStore()
            disk.configure("worker")
            first_hash = hashlib.sha256(b"one").hexdigest()
            second_hash = hashlib.sha256(b"two").hexdigest()
            disk.put_data(
                "controller", "token", "e:1", first_hash, b"one"
            )
            recovered = WorkerDiskStore()
            recovered.configure("worker")
            assert recovered.get_local_data(
                "controller", "token", "e:1"
            ) == b"one"
            recovered.put_data(
                "controller", "token", "e:1", second_hash, b"two"
            )
            assert recovered.get_local_data(
                "controller", "token", "e:1"
            ) == b"two"
            assert len(tuple(Path(recovered.root).glob("*-e-1-*"))) == 1
        finally:
            if previous is None:
                os.environ.pop("WORKER_TMPDIR", None)
            else:
                os.environ["WORKER_TMPDIR"] = previous
    return {"dram_records": 10000, "dram_seconds": dram_seconds}


def cache_accounting_scale():
    policy = WorkerCacheAdmission(None)
    started = time.monotonic()
    for index in range(10000):
        policy.observe(
            {
                "worker_id": f"worker-{index % 64}",
                "data_id": f"e:{index}",
                "size": 64,
            }
        )
    observe_seconds = time.monotonic() - started
    assert observe_seconds < 2, observe_seconds
    usage = policy.usage()
    assert sum(item["items"] for item in usage.values()) == 10000
    assert sum(item["bytes"] for item in usage.values()) == 640000

    policy.enforce(20000, 200, {})
    policy.observe(
        {"worker_id": "worker-0", "data_id": "e:0", "size": 128}
    )
    usage_after_update = policy.usage()
    assert usage_after_update["worker-0"]["items"] == 157
    assert usage_after_update["worker-0"]["bytes"] == 10112
    return {
        "records": len(policy.records),
        "workers": len(usage_after_update),
        "observe_seconds": observe_seconds,
        "under_capacity_evictions": policy.eviction_count,
    }


def consume(hot, unique, previous, ordinal):
    assert hot is HOT or hot == HOT
    return previous + len(hot) + len(unique) + ordinal


def add(left, right):
    return left + right


def build_workflow(count=12):
    workflow = Workflow()
    previous = [None, None]
    expected = 0
    for ordinal in range(count):
        branch = ordinal % 2
        unique = bytes([ordinal + 1]) * (32768 + ordinal * 97)
        if previous[branch] is None:
            previous_value = 0
        else:
            previous_value = previous[branch].output()
        previous[branch] = workflow.add_task(
            consume, HOT, unique, previous_value, ordinal
        )
        expected += len(HOT) + len(unique) + ordinal
    final = workflow.add_task(
        add, previous[0].output(), previous[1].output()
    )
    return workflow, final.task_id, expected


def worker_loss_recovery_case(factory_manager=None):
    workflow, target, oracle = build_workflow(6)
    combined = run_case(
        "cache-capacity-worker-loss-recovery",
        workflow,
        target,
        oracle,
        factory_manager=factory_manager,
        worker_count=1,
        worker_cores=2,
        worker_dram_cache_bytes=1,
        inject_worker_loss_after=1,
        worker_loss_process_shutdown=True,
        replacement_worker_delay=None if factory_manager else 1,
        replacement_worker_delays=() if factory_manager else (10,),
        worker_disk_cache_bytes=400000,
        worker_disk_cache_items=12,
        worker_disk_cache_admission_items=12,
        worker_disk_cache_admission_bytes=400000,
    )
    report = combined["scheduler_report"]
    assert report["worker_loss_injected"], report
    assert report["recovery_reexecutions"] >= 1, report
    assert report["worker_disk_cache_observed_items_high_water"] <= 24
    assert report["worker_disk_cache_observed_bytes_high_water"] <= 800000
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    parser.add_argument(
        "--factory-recovery-only",
        action="store_true",
        help=(
            "run one recovery Manager for an unambiguous factory test; "
            "requires --factory-manager"
        ),
    )
    args = parser.parse_args()
    worker_index = worker_index_contract()
    accounting_scale = cache_accounting_scale()

    if args.factory_recovery_only:
        if not args.factory_manager:
            parser.error("--factory-recovery-only requires --factory-manager")
        report = worker_loss_recovery_case(args.factory_manager)
        print(
            json.dumps(
                {"worker_loss_recovery": report, "status": "PASS"},
                indent=2,
                sort_keys=True,
            )
        )
        return

    bounded_workflow, bounded_target, bounded_oracle = build_workflow()
    bounded = run_case(
        "cache-capacity-bounded",
        bounded_workflow,
        bounded_target,
        bounded_oracle,
        factory_manager=args.factory_manager,
        worker_count=2,
        worker_cores=1,
        worker_dram_cache_bytes=1,
        worker_disk_cache_bytes=239308,
        worker_disk_cache_items=6,
        worker_disk_cache_admission_items=6,
        worker_disk_cache_admission_bytes=239308,
    )
    bounded_report = bounded["scheduler_report"]
    assert bounded["taskvine_workers_used"] == 2, bounded
    assert bounded_report["worker_disk_cache_evictions"] > 0
    assert bounded_report["worker_disk_cache_admission_items"] == 6
    assert bounded_report["worker_disk_cache_admission_bytes"] == 239308
    assert all(
        usage["items"] <= 6
        for usage in bounded_report["worker_disk_cache_usage"].values()
    ), bounded_report
    future_idata_evictions = [
        record
        for record in bounded_report[
            "worker_disk_cache_eviction_records"
        ]
        if record["data_id"].startswith("i:")
        and record["remaining_uses"] > 0
    ]
    assert not future_idata_evictions, bounded_report
    assert bounded_report[
        "worker_disk_cache_effective_retention_items"
    ] == 0, bounded_report
    assert bounded_report["worker_disk_cache_max_task_items"] == 6, (
        bounded_report
    )

    undersized_workflow, undersized_target, undersized_oracle = (
        build_workflow(2)
    )
    try:
        run_case(
            "cache-capacity-undersized",
            undersized_workflow,
            undersized_target,
            undersized_oracle,
            worker_count=1,
            worker_cores=1,
            worker_disk_cache_items=3,
            worker_disk_cache_admission_items=3,
        )
    except ValueError as error:
        assert "largest task working set of 6 items" in str(error)
        undersized = {"status": "REJECTED", "error": str(error)}
    else:
        raise AssertionError("undersized cache admission did not fail closed")

    zero_workflow, zero_target, zero_oracle = build_workflow(6)
    zero = run_case(
        "cache-capacity-zero",
        zero_workflow,
        zero_target,
        zero_oracle,
        factory_manager=args.factory_manager,
        worker_count=1,
        worker_cores=1,
        worker_dram_cache_bytes=1,
        worker_disk_cache_items=0,
    )
    zero_report = zero["scheduler_report"]
    assert zero_report["worker_disk_cache_evictions"] > 0
    assert zero_report["worker_timing_seconds"]["output_disk_store"] > 0
    assert zero_report["worker_dram_cache"]["bytes"] <= 1
    assert not any(
        record["data_id"].startswith("i:")
        and record["remaining_uses"] > 0
        for record in zero_report[
            "worker_disk_cache_eviction_records"
        ]
    )
    assert all(
        usage["items"] == 0
        for usage in zero_report["worker_disk_cache_usage"].values()
    ), zero_report
    assert zero["replica_directory"]["active_leases"] == 0

    combined_report = worker_loss_recovery_case(args.factory_manager)

    print(
        json.dumps(
            {
                "accounting_scale": accounting_scale,
                "worker_index": worker_index,
                "bounded": bounded_report,
                "undersized": undersized,
                "zero": zero_report,
                "worker_loss_recovery": combined_report,
                "status": "PASS",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
