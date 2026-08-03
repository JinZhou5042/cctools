"""Pure logical-task readiness and physical-batch planning."""

from dataclasses import dataclass


DEFAULT_BATCH_INPUT_BYTE_LIMIT = 16 * 1024 * 1024


@dataclass(frozen=True)
class CachePlan:
    """Cache demand and safe retention limits for one workflow run."""

    task_inputs: dict
    remaining_uses: dict
    known_sizes: dict
    max_task_items: int
    max_known_input_bytes: int
    retention_items: int | None
    retention_bytes: int | None


def build_cache_plan(
    task_ids,
    task_record,
    nested_idata_by_task,
    logical_output_slots,
    size_for_key,
    retention_items=None,
    retention_bytes=None,
    admission_items=None,
    admission_bytes=None,
):
    """Build cache-use accounting and validate admission capacity."""

    task_inputs = {}
    remaining_uses = {}
    for task_id in sorted(task_ids):
        record = task_record(task_id)
        keys = {f"e:{record.function_data_id}"}
        keys.update(
            f"{'e' if kind == 'c' else kind}:{data_id}"
            for kind, data_id in record.positional
            if kind in ("e", "c", "i")
        )
        keys.update(
            f"{'e' if kind == 'c' else kind}:{data_id}"
            for _, (kind, data_id) in record.keyword
            if kind in ("e", "c", "i")
        )
        keys.update(
            f"i:{data_id}"
            for data_id in nested_idata_by_task.get(task_id, ())
        )
        task_inputs[task_id] = keys
        for key in keys:
            remaining_uses[key] = remaining_uses.get(key, 0) + 1

    max_task_items = max(
        (
            len(keys) + len(logical_output_slots[task_id])
            for task_id, keys in task_inputs.items()
        ),
        default=0,
    )
    if admission_items is not None and int(admission_items) < max_task_items:
        raise ValueError(
            "worker disk cache admission capacity "
            f"{admission_items} cannot fit the largest task working set "
            f"of {max_task_items} items"
        )

    known_sizes = {
        key: max(0, int(size_for_key(key) or 0))
        for key in remaining_uses
    }
    max_known_input_bytes = max(
        (
            sum(known_sizes[key] for key in keys)
            for keys in task_inputs.values()
        ),
        default=0,
    )
    if (
        admission_bytes is not None
        and int(admission_bytes) < max_known_input_bytes
    ):
        raise ValueError(
            "worker disk cache admission capacity "
            f"{admission_bytes} bytes cannot fit the largest known task "
            f"input working set of {max_known_input_bytes} bytes"
        )

    if admission_items is not None:
        headroom = max(0, int(admission_items) - max_task_items)
        if retention_items is None or int(retention_items) > headroom:
            retention_items = headroom
    if admission_bytes is not None:
        headroom = max(0, int(admission_bytes) - max_known_input_bytes)
        if retention_bytes is None or int(retention_bytes) > headroom:
            retention_bytes = headroom

    return CachePlan(
        task_inputs=task_inputs,
        remaining_uses=remaining_uses,
        known_sizes=known_sizes,
        max_task_items=max_task_items,
        max_known_input_bytes=max_known_input_bytes,
        retention_items=retention_items,
        retention_bytes=retention_bytes,
    )


def select_ready_tasks(
    pending,
    dependencies,
    done,
    task_cache_inputs,
    pruning_data_keys,
    persistence_ready,
):
    """Return deterministic logical TaskIDs that may be submitted now."""

    return tuple(
        sorted(
            task_id
            for task_id in pending
            if dependencies[task_id] <= done
            and persistence_ready(task_id)
            and not (task_cache_inputs[task_id] & pruning_data_keys)
        )
    )


def plan_ready_batches(
    ready_task_ids,
    unbatchable_tasks,
    task_cache_inputs,
    cache_known_sizes,
    maximum_batch_size,
    connected_slots,
    input_byte_limit=DEFAULT_BATCH_INPUT_BYTE_LIMIT,
):
    """Group ready tasks while bounding batch size and unique input bytes."""

    maximum_batch_size = int(maximum_batch_size)
    connected_slots = int(connected_slots)
    input_byte_limit = int(input_byte_limit)
    if maximum_batch_size < 1:
        raise ValueError("maximum batch size must be positive")
    if connected_slots < 1:
        raise ValueError("connected slots must be positive")
    if input_byte_limit < 1:
        raise ValueError("input byte limit must be positive")

    ready_task_ids = tuple(ready_task_ids)
    target_size = min(
        maximum_batch_size,
        max(
            1,
            (len(ready_task_ids) + connected_slots - 1)
            // connected_slots,
        ),
    )
    batches = []
    batch = []
    batch_data_keys = set()
    batch_input_bytes = 0
    for task_id in ready_task_ids:
        if task_id in unbatchable_tasks:
            if batch:
                batches.append(tuple(batch))
                batch = []
                batch_data_keys.clear()
                batch_input_bytes = 0
            batches.append((task_id,))
            continue

        new_keys = task_cache_inputs[task_id] - batch_data_keys
        new_bytes = sum(cache_known_sizes[key] for key in new_keys)
        if batch and (
            len(batch) == target_size
            or batch_input_bytes + new_bytes > input_byte_limit
        ):
            batches.append(tuple(batch))
            batch = []
            batch_data_keys.clear()
            batch_input_bytes = 0
            new_keys = set(task_cache_inputs[task_id])
            new_bytes = sum(cache_known_sizes[key] for key in new_keys)
        batch.append(task_id)
        batch_data_keys.update(new_keys)
        batch_input_bytes += new_bytes
    if batch:
        batches.append(tuple(batch))
    return tuple(batches)
