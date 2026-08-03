#!/usr/bin/env python3

from ndcctools.taskvine.datavine.diagnostics import rank_bottlenecks


def main():
    ranked = rank_bottlenecks(
        {
            "registration": 0.8,
            "execution_loop": 1.5,
            "reporting_and_cleanup": 0.2,
        },
        {"edata": 0.4, "task_registration": 0.3},
        {
            "POST /v1/tasks/register-batch": {
                "count": 3,
                "seconds": 0.35,
            },
            "GET /v1/snapshot": {"count": 1, "seconds": 0.01},
        },
        limit=4,
    )
    assert [(entry["category"], entry["name"]) for entry in ranked] == [
        ("workflow", "execution_loop"),
        ("workflow", "registration"),
        ("registration", "edata"),
        ("controller-request", "POST /v1/tasks/register-batch"),
    ]
    assert ranked[-1]["count"] == 3
    assert len(ranked) == 4
    try:
        rank_bottlenecks({}, {}, {}, 0)
    except ValueError:
        pass
    else:
        raise AssertionError("accepted an unbounded empty bottleneck limit")

    print("DataVine bounded diagnostics contract PASS")


if __name__ == "__main__":
    main()
