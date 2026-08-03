"""Worker output staging, publication, and replica preparation."""

import cloudpickle
import hashlib

from .outputs import normalize_output_values


def publish_task_outputs(
    task,
    result,
    attempt,
    reporter,
    worker_id,
    worker_epoch,
    emit,
    capture_output=None,
):
    output_values = normalize_output_values(task, result)
    total_bytes = 0
    for output_index, (output_data_id, output_value) in enumerate(
        zip(task.output_data_ids, output_values)
    ):
        payload = cloudpickle.dumps(output_value)
        total_bytes += len(payload)
        content_hash = hashlib.sha256(payload).hexdigest()
        with reporter.process_cache.lock:
            reporter.process_cache.data.put_data(
                reporter.controller,
                reporter.token,
                f"i:{output_data_id}",
                content_hash,
                payload,
            )
        capture_output(
            {
                "task_id": task.task_id,
                "output_index": output_index,
                "data_id": output_data_id,
                "attempt": attempt,
                "content_hash": content_hash,
                "size": len(payload),
                "payload": payload,
                "replica_id": reporter.replica_id(f"i:{output_data_id}"),
                "worker_id": worker_id,
                "worker_epoch": worker_epoch,
            }
        )
        emit(
            "DATAVINE "
            f"task={task.task_id} slot={output_index} "
            f"output=i{output_data_id} bytes={len(payload)}"
        )
    emit(
        f"DATAVINE_OUTPUTS task={task.task_id} "
        f"count={len(task.output_data_ids)} bytes={total_bytes}"
    )
    return total_bytes
