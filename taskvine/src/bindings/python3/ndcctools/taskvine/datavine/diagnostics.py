"""Bounded performance bottleneck diagnostics."""


def rank_bottlenecks(
    workflow_timing,
    registration_timing,
    controller_requests,
    limit=12,
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
    entries.sort(
        key=lambda entry: (
            -entry["seconds"],
            entry["category"],
            entry["name"],
        )
    )
    return entries[:limit]
