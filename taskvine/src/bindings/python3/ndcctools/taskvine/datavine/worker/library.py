"""Persistent TaskVine library entry points for DataVine execution."""

import json
import os
import time
import traceback


def warm_datavine_worker():
    return True


def persist_datavine_idata(
    controller,
    token,
    data_id,
    request_id,
    delay_before_complete=False,
    inject_failure_during_write=False,
    delay_before_failure=0,
):
    from .persist import main

    argv = [
        "--controller",
        str(controller),
        "--token",
        str(token),
        "--data-id",
        str(int(data_id)),
        "--request-id",
        str(request_id),
    ]
    if delay_before_complete:
        argv.extend(("--delay-before-complete", "3"))
    if inject_failure_during_write:
        argv.extend(
            (
                "--inject-failure-during-write",
                "--delay-before-failure",
                str(float(delay_before_failure)),
            )
        )
    try:
        return {
            "protocol": "datavine-persist-v1",
            "exit_code": int(main(argv)),
            "error": None,
        }
    except Exception:
        return {
            "protocol": "datavine-persist-v1",
            "exit_code": 1,
            "error": traceback.format_exc(),
        }


def _cache_event(process_cache):
    with process_cache.lock:
        snapshot = process_cache.data.snapshot()
    snapshot["worker_id"] = os.environ.get("VINE_WORKER_ID", "")
    return "DATAVINE_DRAM_CACHE " + json.dumps(
        snapshot, sort_keys=True, separators=(",", ":")
    )


def execute_datavine_task(
    controller,
    token,
    task_id,
    attempt,
    output_files,
    worker_dram_cache_bytes,
    task_record,
    controller_inline_idata_bytes,
    allow_peer_transfer,
):
    """Execute one logical task through the worker-owned data path."""
    from .runner import execute_task
    from .cache import PROCESS_CACHE
    from ..models import TaskRecord
    from ..scheduler.client import ControllerClient

    controller_key = (controller, token)
    with PROCESS_CACHE.lock:
        PROCESS_CACHE.data.configure(worker_dram_cache_bytes)
        client = PROCESS_CACHE.clients.get(controller_key)
        if client is None:
            client = ControllerClient(
                controller, token, transient_retries=8
            )
            PROCESS_CACHE.clients[controller_key] = client
    with PROCESS_CACHE.lock:
        PROCESS_CACHE.task_records[
            (controller, token, int(task_id))
        ] = TaskRecord.from_dict(task_record)

    events = []
    outputs = []
    timings = {}
    started = time.monotonic()
    error = None
    try:
        result = execute_task(
            controller,
            token,
            task_id,
            attempt,
            output_files,
            emit=events.append,
            capture_output=outputs.append,
            timings=timings,
            allow_peer_transfer=allow_peer_transfer,
        )
        if result:
            raise RuntimeError(
                f"DataVine TaskID {task_id} runner returned {result}"
            )
        publish_started = time.monotonic()
        if outputs:
            published = client.publish_outputs(
                outputs[0]["worker_id"],
                outputs[0]["worker_epoch"],
                outputs,
                controller_inline_idata_bytes,
            )
            if len(published) != len(outputs):
                raise RuntimeError(
                    "Controller returned incomplete publications"
                )
            for output, replica in zip(outputs, published):
                output.update(replica)
        timings["controller_publication"] = (
            time.monotonic() - publish_started
        )
    except Exception:
        error = traceback.format_exc()
    finally:
        for output in outputs:
            output.pop("payload", None)
    events.append(_cache_event(PROCESS_CACHE))
    return {
        "protocol": "datavine-task-v1",
        "task_id": int(task_id),
        "events": events,
        "outputs": outputs,
        "error": error,
        "worker_seconds": time.monotonic() - started,
        "timing_seconds": timings,
        "outputs_committed": error is None,
    }
