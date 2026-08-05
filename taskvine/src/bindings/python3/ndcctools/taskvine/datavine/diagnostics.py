"""Bounded performance bottleneck diagnostics."""


def rank_bottlenecks(
    workflow_timing,
    registration_timing,
    controller_requests,
    limit=12,
    worker_timing=None,
    manager_timing_us=None,
    task_count=None,
):
    """Rank existing aggregate timings without adding hot-loop tracing."""

    limit = int(limit)
    if limit < 1:
        raise ValueError("bottleneck limit must be positive")
    entries = [
        {
            "category": "workflow",
            "name": str(name),
            "seconds": float(seconds),
        }
        for name, seconds in workflow_timing.items()
    ]
    entries.extend(
        {
            "category": "registration",
            "name": str(name),
            "seconds": float(seconds),
        }
        for name, seconds in registration_timing.items()
    )
    if controller_requests:
        entries.extend(
            {
                "category": "controller-request",
                "name": str(name),
                "seconds": float(metrics["seconds"]),
                "count": int(metrics["count"]),
            }
            for name, metrics in controller_requests.items()
        )
    entries.extend(
        {
            "category": "worker",
            "name": str(name),
            "seconds": float(seconds),
            "scope": "cumulative-task-time",
        }
        for name, seconds in (worker_timing or {}).items()
    )
    entries.extend(
        {
            "category": "manager",
            "name": str(name),
            "seconds": int(microseconds) / 1_000_000,
            "scope": "cumulative-manager-time",
        }
        for name, microseconds in (manager_timing_us or {}).items()
    )
    if task_count:
        for entry in entries:
            entry["microseconds_per_task"] = (
                entry["seconds"] * 1_000_000 / int(task_count)
            )
    entries.sort(
        key=lambda entry: (
            -entry["seconds"],
            entry["category"],
            entry["name"],
        )
    )
    return entries[:limit]
