#!/usr/bin/env python3

from types import SimpleNamespace

from ndcctools.taskvine.datavine.scheduler.reporting import (
    MANAGER_TIMING_FIELDS,
    WORKER_CACHE_FIELDS,
    format_logical_outputs,
    format_manager_metrics,
    format_worker_caches,
    select_report_scope,
)


def main():
    slots = {1: (11,), 2: (12, 13), 3: (14,)}
    assert select_report_scope(
        slots, {11, 12, 13, 14}, {3, 2}, slots, False
    ) == ((2, 3), (12, 13, 14))
    assert select_report_scope(
        slots, {11, 12, 13, 14}, {3}, slots, True
    ) == ((1, 2, 3), (11, 12, 13, 14))

    statuses = {
        data_id: {
            "data_id": data_id,
            "producer_task_id": 1,
            "producer_output_index": 0,
            "attempt": 2,
            "content_hash": f"hash-{data_id}",
            "size": data_id,
            "available": True,
            "durability": "volatile",
            "unreported": "excluded",
        }
        for data_id in (11, 12, 13, 14)
    }
    logical = format_logical_outputs(
        slots, {1: 1, 2: 2, 3: 3}, statuses, (2,), (12, 13), False
    )
    assert logical["logical_output_slots"] == {"2": [12, 13]}
    assert logical["attempts_by_task"] == {"2": 2}
    assert not logical["logical_output_status_complete"]
    assert "unreported" not in logical["logical_output_status"]["12"]

    stats = SimpleNamespace(
        **{name: index for index, name in enumerate(MANAGER_TIMING_FIELDS)},
        bytes_sent=101,
        bytes_received=202,
    )
    manager = format_manager_metrics(stats)
    assert manager["manager_bytes"] == {"sent": 101, "received": 202}
    assert set(manager["manager_timing_us"]) == set(MANAGER_TIMING_FIELDS)

    worker = {"workerid": "worker-1", "cache_items": 3, "extra": 9}
    caches = format_worker_caches([worker])
    assert set(caches[0]) == set(WORKER_CACHE_FIELDS)
    assert caches[0]["workerid"] == "worker-1"
    assert caches[0]["cache_items"] == 3
    assert "extra" not in caches[0]

    print("DataVine Scheduler reporting contract PASS")


if __name__ == "__main__":
    main()
