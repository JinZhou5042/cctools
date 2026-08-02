#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import tempfile
import time

from datavine_phase4_demand_pull import run_case, start_worker
from ndcctools.taskvine import Manager, Task, cvine
from ndcctools.taskvine.datavine import Workflow
from ndcctools.taskvine.datavine.cache import WorkerCacheAdmission


HOT = b"datavine-hot-cache-value\n" * 4096


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

    resolver_calls = 0

    def unexpected_resolver(_):
        nonlocal resolver_calls
        resolver_calls += 1
        raise AssertionError("under-capacity enforcement scanned candidates")

    policy.enforce(object(), unexpected_resolver, 20000, 200, {})
    assert resolver_calls == 0
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
        "under_capacity_resolver_calls": resolver_calls,
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


def pending_unlink_worker_loss():
    with tempfile.TemporaryDirectory(
        prefix="datavine-cache-unlink-loss-"
    ) as root:
        manager = Manager(
            port=0, run_info_path=str(Path(root) / "run-info")
        )
        worker = start_worker(manager.port, cores=1)
        try:
            while not manager.status("workers"):
                manager.wait(1)
            cached = manager.declare_buffer(
                b"pending-unlink-worker-loss", cache="worker"
            )
            task = Task("/bin/true")
            task.add_input(cached, "input")
            manager.submit(task)
            completed = manager.wait(10)
            assert completed is not None and completed.successful()
            worker_id = manager.status("workers")[0]["workerid"]
            before = manager.prune_file_status(cached)
            assert manager.prune_file_on_worker(cached, worker_id) == 1
            assert cvine.vine_manager_release_random_worker(
                manager._taskvine
            )
            status = manager.prune_file_status(cached)
            assert status["requested"] - before["requested"] == 1
            assert status["confirmed"] - before["confirmed"] == 0
            assert status["failed"] - before["failed"] == 1
            assert manager.forget_prune_file_status(cached)
            return status
        finally:
            if worker.poll() is None:
                worker.terminate()
                worker.wait(timeout=10)
            manager._free()


def worker_byte_rejection():
    with tempfile.TemporaryDirectory(
        prefix="datavine-cache-byte-rejection-"
    ) as root:
        manager = Manager(
            port=0, run_info_path=str(Path(root) / "run-info")
        )
        assert manager.tune("datavine-cache-capacity-items", -1) == 0
        assert manager.tune("datavine-cache-capacity-bytes", 1024) == 0
        worker = start_worker(manager.port, cores=1)
        try:
            output = manager.declare_temp()
            task = Task(
                "/bin/dd if=/dev/zero of=oversized bs=2048 count=1"
            )
            task.add_output(output, "oversized")
            manager.submit(task)
            completed = manager.wait(20)
            assert completed is not None
            assert not completed.successful(), completed
            manager.wait(1)
            workers = manager.status("workers")
            assert len(workers) == 1, workers
            status = workers[0]
            assert status["cache_capacity_configured"], status
            assert status["cache_capacity_bytes"] == 1024, status
            assert status["worker_cache_bytes_high_water"] <= 1024, status
            assert (
                status["worker_cache_admission_rejections"] >= 1
            ), status
            assert status["worker_cache_bytes"] == 0, status
            return {
                "task_result": completed.result,
                "cache_capacity_bytes": status[
                    "cache_capacity_bytes"
                ],
                "cache_bytes_high_water": status[
                    "worker_cache_bytes_high_water"
                ],
                "cache_admission_rejections": status[
                    "worker_cache_admission_rejections"
                ],
                "worker_cache_bytes": status["worker_cache_bytes"],
            }
        finally:
            if worker.poll() is None:
                worker.terminate()
                worker.wait(timeout=10)
            manager._free()


def prefetch_recovery_case(factory_manager=None):
    workflow, target, oracle = build_workflow(6)
    combined = run_case(
        "cache-capacity-prefetch-recovery",
        workflow,
        target,
        oracle,
        factory_manager=factory_manager,
        worker_count=1,
        worker_cores=2,
        prefetch=True,
        inject_worker_loss_after=1,
        replacement_worker_delay=None if factory_manager else 1,
        worker_disk_cache_bytes=238743,
        worker_disk_cache_items=6,
        worker_disk_cache_admission_items=6,
        worker_disk_cache_admission_bytes=238743,
    )
    report = combined["scheduler_report"]
    assert report["worker_loss_injected"], report
    assert report["recovery_reexecutions"] >= 1, report
    assert report["legacy_recovery_tasks"] == 0, report
    assert report["prefetch_selected"] > 0, report
    assert all(
        worker["cache_items_high_water"] <= 6
        and worker["cache_bytes_high_water"] <= 238743
        and worker["worker_cache_items_high_water"] <= 6
        and worker["worker_cache_bytes_high_water"] <= 238743
        and worker["cache_capacity_configured"]
        for worker in report["worker_physical_cache"]
    ), report
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
    accounting_scale = cache_accounting_scale()

    if args.factory_recovery_only:
        if not args.factory_manager:
            parser.error("--factory-recovery-only requires --factory-manager")
        report = prefetch_recovery_case(args.factory_manager)
        print(
            json.dumps(
                {"prefetch_recovery": report, "status": "PASS"},
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
        prefetch=False,
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
    assert bounded_report["worker_physical_cache"]
    assert all(
        worker["cache_items_high_water"] <= 6
        for worker in bounded_report["worker_physical_cache"]
    ), bounded_report
    assert all(
        worker["cache_bytes_high_water"] <= 239308
        and worker["worker_cache_bytes_high_water"] <= 239308
        and worker["worker_cache_items_high_water"] <= 6
        and worker["cache_capacity_configured"]
        and worker["cache_capacity_bytes"] == 239308
        and worker["worker_cache_bytes"] <= 239308
        for worker in bounded_report["worker_physical_cache"]
    ), bounded_report
    assert all(
        worker["cache_prune_pending_items"] == 0
        for worker in bounded_report["worker_physical_cache"]
    ), bounded_report
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
    assert future_idata_evictions, bounded_report
    assert bounded_report["legacy_recovery_tasks"] == 0, bounded_report
    assert bounded_report[
        "worker_disk_cache_effective_retention_items"
    ] == 0
    assert bounded_report["worker_disk_cache_max_task_items"] == 6

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
            prefetch=False,
            worker_disk_cache_items=4,
            worker_disk_cache_admission_items=5,
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
        prefetch=False,
        worker_disk_cache_items=0,
    )
    zero_report = zero["scheduler_report"]
    assert zero_report["worker_disk_cache_evictions"] > 0
    assert any(
        record["data_id"].startswith("i:")
        and record["remaining_uses"] > 0
        for record in zero_report[
            "worker_disk_cache_eviction_records"
        ]
    )
    assert zero_report["legacy_recovery_tasks"] == 0, zero_report
    assert all(
        usage["items"] == 0
        for usage in zero_report["worker_disk_cache_usage"].values()
    ), zero_report
    assert zero["replica_directory"]["active_leases"] == 0

    combined_report = prefetch_recovery_case(args.factory_manager)

    unlink_loss = pending_unlink_worker_loss()
    byte_rejection = worker_byte_rejection()

    print(
        json.dumps(
            {
                "accounting_scale": accounting_scale,
                "bounded": bounded_report,
                "undersized": undersized,
                "zero": zero_report,
                "prefetch_recovery": combined_report,
                "pending_unlink_worker_loss": unlink_loss,
                "worker_byte_rejection": byte_rejection,
                "status": "PASS",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
