"""Worker output staging, publication, and replica preparation."""

import cloudpickle
import hashlib
import time

from .outputs import normalize_output_values


def publish_task_outputs(
    task,
    result,
    attempt,
    reporter,
    worker_id,
    worker_epoch,
    capture_output=None,
    timings=None,
):
    output_values = normalize_output_values(task, result)
    total_bytes = 0
    for output_index, (output_data_id, output_value) in enumerate(
        zip(task.output_data_ids, output_values)
    ):
        started = time.monotonic()
        payload = cloudpickle.dumps(output_value)
        total_bytes += len(payload)
        content_hash = hashlib.sha256(payload).hexdigest()
        if timings is not None:
            timings["output_serialize"] = timings.get(
                "output_serialize", 0.0
            ) + (time.monotonic() - started)
        started = time.monotonic()
        data_key = f"i:{output_data_id}"
        admitted = reporter.process_cache.data_service.put_data(
            reporter.controller,
            reporter.token,
            data_key,
            content_hash,
            payload,
            reporter.process_cache.dram_capacity,
            protected=True,
        )
        tier = "worker-dram" if admitted else "worker-disk"
        if not admitted:
            reporter.process_cache.disk.put_data(
                reporter.controller,
                reporter.token,
                data_key,
                content_hash,
                payload,
            )
        if timings is not None:
            name = (
                "output_shared_dram_store"
                if admitted
                else "output_disk_store"
            )
            timings[name] = timings.get(
                name, 0.0
            ) + (time.monotonic() - started)
        capture_output(
            {
                "task_id": task.task_id,
                "output_index": output_index,
                "data_id": f"i:{output_data_id}",
                "attempt": attempt,
                "content_hash": content_hash,
                "size": len(payload),
                "payload": payload,
                "replica_id": reporter.replica_id(f"i:{output_data_id}"),
                "worker_id": worker_id,
                "worker_epoch": worker_epoch,
                "tier": tier,
                "source_endpoint": reporter.endpoint,
            }
        )
    return total_bytes
