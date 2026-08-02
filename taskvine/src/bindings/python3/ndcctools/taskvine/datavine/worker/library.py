"""Persistent TaskVine library entry points for DataVine execution."""

import base64
import json
import time


def execute_datavine_task(
    controller,
    token,
    task_id,
    attempt,
    output_files,
    idata_inline_threshold,
):
    """Execute the existing DataVine worker protocol without a new process."""
    from .runner import main

    argv = [
        "--controller",
        str(controller),
        "--token",
        str(token),
        "--task-id",
        str(int(task_id)),
        "--attempt",
        str(int(attempt)),
        "--idata-inline-threshold",
        str(int(idata_inline_threshold)),
    ]
    for output_file in output_files:
        argv.extend(("--output-file", str(output_file)))
    events = []
    result = main(argv, emit=events.append)
    if result:
        raise RuntimeError(
            f"DataVine TaskID {task_id} runner returned {result}"
        )
    return "\n".join(events) + ("\n" if events else "")


def execute_datavine_tasks(
    controller,
    token,
    calls,
    inline_edata,
    inline_tasks,
    idata_inline_threshold,
):
    """Execute independent ready tasks in one physical library call."""
    from .runner import main

    task_results = []
    started = time.monotonic()
    for task_id, attempt, output_files in calls:
        events = []
        publications = []
        argv = [
            "--controller",
            str(controller),
            "--token",
            str(token),
            "--task-id",
            str(int(task_id)),
            "--attempt",
            str(int(attempt)),
            "--idata-inline-threshold",
            str(int(idata_inline_threshold)),
        ]
        for output_file in output_files:
            argv.extend(("--output-file", str(output_file)))
        error = None
        try:
            def capture(output_index, data_id, payload):
                publications.append(
                    (
                        int(output_index),
                        int(data_id),
                        int(attempt),
                        payload,
                    )
                )

            result = main(
                argv,
                emit=events.append,
                capture_inline=capture,
                trust_taskvine_inputs=True,
                inline_edata=inline_edata,
                inline_tasks=inline_tasks,
            )
            if result:
                raise RuntimeError(
                    f"DataVine TaskID {task_id} runner returned {result}"
                )
        except Exception as exc:
            error = repr(exc)
        task_results.append(
            {
                "task_id": int(task_id),
                "events": events,
                "publications": publications,
                "error": error,
            }
        )
    worker_seconds = time.monotonic() - started
    if any(result["error"] is not None for result in task_results):
        output = []
        for result in task_results:
            output.append(
                f"DATAVINE_TASK_BEGIN {result['task_id']}"
            )
            output.extend(result["events"])
            for output_index, data_id, attempt, payload in result[
                "publications"
            ]:
                output.append(
                    "DATAVINE_INLINE_RESULT "
                    + json.dumps(
                        {
                            "task_id": result["task_id"],
                            "output_index": output_index,
                            "data_id": data_id,
                            "attempt": attempt,
                            "payload": base64.b64encode(payload).decode(
                                "ascii"
                            ),
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
            if result["error"] is not None:
                output.append(
                    "DATAVINE_TASK_FAILURE "
                    + json.dumps(
                        {
                            "task_id": result["task_id"],
                            "error": result["error"],
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
            output.append(f"DATAVINE_TASK_END {result['task_id']}")
        output.append(
            "DATAVINE_BATCH_TIMING "
            f"tasks={len(calls)} seconds={worker_seconds:.9f}"
        )
        return "\n".join(output) + "\n"
    return {
        "protocol": "datavine-batch-v1",
        "tasks": task_results,
        "worker_seconds": worker_seconds,
    }
