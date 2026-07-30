#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import tempfile

from datavine_phase4_demand_pull import run_case, start_worker
from ndcctools.taskvine import Manager, Task, cvine
from ndcctools.taskvine.datavine import Workflow


HOT = b"datavine-hot-cache-value\n" * 4096


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()

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
        worker_disk_cache_items=6,
        worker_disk_cache_admission_items=6,
    )
    bounded_report = bounded["scheduler_report"]
    assert bounded["taskvine_workers_used"] == 2, bounded
    assert bounded_report["worker_disk_cache_evictions"] > 0
    assert bounded_report["worker_disk_cache_admission_items"] == 6
    assert bounded_report["worker_physical_cache"]
    assert all(
        worker["cache_items_high_water"] <= 6
        for worker in bounded_report["worker_physical_cache"]
    ), bounded_report
    assert sum(
        worker["cache_admission_rejections"]
        for worker in bounded_report["worker_physical_cache"]
    ) > 0, bounded_report
    assert all(
        worker["cache_prune_pending_items"] == 0
        for worker in bounded_report["worker_physical_cache"]
    ), bounded_report
    assert all(
        usage["items"] <= 6
        for usage in bounded_report["worker_disk_cache_usage"].values()
    ), bounded_report
    assert all(
        record["remaining_uses"] == 0
        or record["data_id"].startswith("e:")
        for record in bounded_report[
            "worker_disk_cache_eviction_records"
        ]
    )
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
    assert all(
        record["remaining_uses"] == 0
        for record in zero_report[
            "worker_disk_cache_eviction_records"
        ]
    )
    assert all(
        usage["items"] == 0
        for usage in zero_report["worker_disk_cache_usage"].values()
    ), zero_report
    assert zero["replica_directory"]["active_leases"] == 0
    unlink_loss = pending_unlink_worker_loss()

    print(
        json.dumps(
            {
                "bounded": bounded_report,
                "undersized": undersized,
                "zero": zero_report,
                "pending_unlink_worker_loss": unlink_loss,
                "status": "PASS",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
