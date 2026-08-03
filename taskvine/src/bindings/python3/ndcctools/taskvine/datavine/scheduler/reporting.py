"""Deterministic Scheduler report formatting."""


MANAGER_TIMING_FIELDS = (
    "time_send",
    "time_receive",
    "time_status_msgs",
    "time_internal",
    "time_polling",
    "time_application",
    "time_scheduling",
    "time_workers_execute",
)

WORKER_CACHE_FIELDS = (
    "workerid",
    "cache_items",
    "cache_bytes",
    "cache_items_high_water",
    "cache_bytes_high_water",
    "cache_prune_pending_items",
    "cache_prune_pending_bytes",
    "cache_admission_rejections",
    "cache_capacity_configured",
    "cache_capacity_items",
    "cache_capacity_bytes",
    "worker_cache_items",
    "worker_cache_bytes",
    "worker_cache_items_high_water",
    "worker_cache_bytes_high_water",
    "worker_cache_admission_rejections",
)


def select_report_scope(
    all_task_ids,
    all_data_ids,
    result_task_ids,
    logical_output_slots,
    detailed,
):
    """Select bounded default or complete detailed report identifiers."""

    if detailed:
        return tuple(sorted(all_task_ids)), tuple(sorted(all_data_ids))
    task_ids = tuple(sorted(set(result_task_ids)))
    data_ids = tuple(
        sorted(
            data_id
            for task_id in task_ids
            for data_id in logical_output_slots[task_id]
        )
    )
    return task_ids, data_ids


def format_logical_outputs(
    logical_output_slots,
    attempts,
    output_status,
    task_ids,
    data_ids,
    detailed,
):
    """Format logical output slots, statuses, and attempt counters."""

    return {
        "logical_output_slots": {
            str(task_id): list(logical_output_slots[task_id])
            for task_id in task_ids
        },
        "logical_output_slots_complete": bool(detailed),
        "logical_output_status": {
            str(data_id): {
                key: output_status[data_id][key]
                for key in (
                    "producer_task_id",
                    "producer_output_index",
                    "attempt",
                    "content_hash",
                    "size",
                    "available",
                    "durability",
                )
            }
            for data_id in data_ids
        },
        "logical_output_status_complete": bool(detailed),
        "attempts_by_task": {
            str(task_id): attempts[task_id] for task_id in task_ids
        },
        "attempts_by_task_complete": bool(detailed),
    }


def format_manager_metrics(stats):
    """Copy the stable Manager timing and byte counters."""

    return {
        "manager_timing_us": {
            name: int(getattr(stats, name))
            for name in MANAGER_TIMING_FIELDS
        },
        "manager_bytes": {
            "sent": int(stats.bytes_sent),
            "received": int(stats.bytes_received),
        },
    }


def format_worker_caches(workers):
    """Return the bounded physical worker-cache view used in reports."""

    return [
        {key: worker.get(key) for key in WORKER_CACHE_FIELDS}
        for worker in workers
    ]
