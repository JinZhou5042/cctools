"""Pure logical-task readiness and physical-batch planning."""


DEFAULT_BATCH_INPUT_BYTE_LIMIT = 16 * 1024 * 1024


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
