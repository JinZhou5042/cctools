"""Worker output staging, publication, and replica preparation."""

import cloudpickle
import hashlib
import json
import os
from pathlib import Path
import time

from .outputs import normalize_output_values


def publish_task_outputs(
    task,
    result,
    args,
    client,
    reporter,
    worker_id,
    worker_epoch,
    emit,
    capture_inline=None,
):
    output_values = normalize_output_values(task, result)
    if args.output_file and len(args.output_file) != len(
        task.output_data_ids
    ):
        raise ValueError(
            "output file count does not match logical output count"
        )
    total_bytes = 0
    for output_index, (output_data_id, output_value) in enumerate(
        zip(task.output_data_ids, output_values)
    ):
        payload = cloudpickle.dumps(output_value)
        total_bytes += len(payload)
        if capture_inline is not None:
            if len(payload) > args.idata_inline_threshold:
                raise RuntimeError(
                    "DATAVINE_INLINE_TOO_LARGE "
                    f"task={task.task_id} slot={output_index} "
                    f"bytes={len(payload)}"
                )
            capture_inline(output_index, output_data_id, payload)
            emit(
                "DATAVINE_INLINE_CAPTURE "
                f"task={task.task_id} slot={output_index} "
                f"output=i{output_data_id} bytes={len(payload)}"
            )
            continue
        stage = Path(
            args.output_file[output_index]
            if args.output_file
            else f".datavine-idata-{output_data_id}.stage"
        )
        with stage.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        staged_payload = stage.read_bytes()
        if len(staged_payload) <= args.idata_inline_threshold:
            publication = client.publish_idata(
                output_data_id, args.attempt, staged_payload
            )
        else:
            publication = client.publish_idata_metadata(
                output_data_id,
                args.attempt,
                hashlib.sha256(staged_payload).hexdigest(),
                len(staged_payload),
            )
        prepared = client.prepare_replica(
            f"i:{output_data_id}",
            reporter.replica_id(f"i:{output_data_id}"),
            args.attempt,
            "worker-disk",
            publication["content_hash"],
            publication["size"],
            worker_id,
            worker_epoch,
        )
        emit(
            "DATAVINE_REPLICA_PREPARED "
            + json.dumps(
                {
                    "data_id": prepared["data_id"],
                    "output_index": output_index,
                    "replica_id": prepared["replica_id"],
                    "generation": prepared["generation"],
                    "attempt": prepared["attempt"],
                    "content_hash": prepared["content_hash"],
                    "size": prepared["size"],
                    "worker_id": worker_id,
                    "worker_epoch": worker_epoch,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        if not args.output_file:
            stage.unlink(missing_ok=True)
        emit(
            "DATAVINE "
            f"task={task.task_id} slot={output_index} "
            f"output=i{output_data_id} bytes={len(payload)}"
        )
        if output_index == args.pause_after_output_index:
            time.sleep(30)
    emit(
        f"DATAVINE_OUTPUTS task={task.task_id} "
        f"count={len(task.output_data_ids)} bytes={total_bytes}"
    )
    return total_bytes
