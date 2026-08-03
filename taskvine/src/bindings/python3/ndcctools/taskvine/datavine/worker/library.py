"""Persistent TaskVine library entry points for DataVine execution."""

import time


def execute_datavine_task(
    controller,
    token,
    task_id,
    attempt,
    output_files,
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
):
    """Execute independent ready tasks in one physical library call."""
    from .runner import main
    from .cache import PROCESS_CACHE
    from ..scheduler.client import ControllerClient

    controller_key = (controller, token)
    with PROCESS_CACHE.lock:
        client = PROCESS_CACHE.clients.get(controller_key)
        if client is None:
            client = ControllerClient(
                controller, token, transient_retries=8
            )
            PROCESS_CACHE.clients[controller_key] = client
    records = client.get_tasks(task_id for task_id, _, _ in calls)
    with PROCESS_CACHE.lock:
        for record in records:
            PROCESS_CACHE.task_records[
                (controller, token, record.task_id)
            ] = record

    task_results = []
    started = time.monotonic()
    for task_id, attempt, output_files in calls:
        events = []
        outputs = []
        argv = [
            "--controller",
            str(controller),
            "--token",
            str(token),
            "--task-id",
            str(int(task_id)),
            "--attempt",
            str(int(attempt)),
        ]
        for output_file in output_files:
            argv.extend(("--output-file", str(output_file)))
        error = None
        try:
            result = main(
                argv,
                emit=events.append,
                capture_output=outputs.append,
                trust_taskvine_inputs=True,
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
                "outputs": outputs,
                "error": error,
            }
        )
    worker_seconds = time.monotonic() - started
    if any(result["error"] is not None for result in task_results):
        raise RuntimeError(
            "; ".join(
                f"TaskID {result['task_id']}: {result['error']}"
                for result in task_results
                if result["error"] is not None
            )
        )
    outputs = [
        output
        for result in task_results
        for output in result["outputs"]
    ]
    if outputs:
        worker_id = outputs[0]["worker_id"]
        worker_epoch = outputs[0]["worker_epoch"]
        if any(
            output["worker_id"] != worker_id
            or output["worker_epoch"] != worker_epoch
            for output in outputs
        ):
            raise RuntimeError("batch outputs span worker incarnations")
        prepared = client.prepare_outputs(
            worker_id, worker_epoch, outputs
        )
        if len(prepared) != len(outputs):
            raise RuntimeError("Controller returned incomplete preparations")
        for output, replica in zip(outputs, prepared):
            output.update(replica)
    return {
        "protocol": "datavine-batch-v2",
        "tasks": task_results,
        "worker_seconds": worker_seconds,
    }
