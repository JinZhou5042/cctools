"""Persistent TaskVine library entry points for DataVine execution."""


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
