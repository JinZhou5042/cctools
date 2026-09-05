"""Logical output normalization."""


def normalize_output_values(task, result):
    output_count = len(task.output_data_ids)
    if output_count == 1:
        return (result,)
    if not isinstance(result, (tuple, list)):
        raise TypeError(
            f"TaskID {task.task_id} declared {output_count} outputs "
            f"but returned {type(result).__name__}"
        )
    if len(result) != output_count:
        raise ValueError(
            f"TaskID {task.task_id} declared {output_count} outputs "
            f"but returned {len(result)} values"
        )
    return tuple(result)
