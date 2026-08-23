#!/usr/bin/env python3
"""Measure large repeated-eData construction without starting a runtime."""

import argparse
import json
import resource
import time

from ndcctools.taskvine.datavine import Workflow


def identity(value):
    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=1_000_000)
    args = parser.parse_args()
    workflow = Workflow(
        "edata-scale", workflow_id="edata-scale",
        maximum_tasks=args.tasks, maximum_edges=0,
    )
    started = time.perf_counter()
    for _ in range(args.tasks):
        workflow.python_callable(identity, 7)
    elapsed = time.perf_counter() - started
    profile = workflow.profile()
    result = {
        "tasks": args.tasks,
        "data_records": len(workflow._data),
        "elapsed_seconds": elapsed,
        "tasks_per_second": args.tasks / elapsed,
        "maximum_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "profile": profile,
    }
    assert result["data_records"] == args.tasks + 2
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
