#!/usr/bin/env python3
"""Compare DataVine with TaskVine FunctionCall on identical identity work."""

import argparse
import gc
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import time

from ndcctools.taskvine import FunctionCall, Manager
from ndcctools.taskvine.datavine import Workflow


TEST_DIR = Path(__file__).resolve().parents[2] / "taskvine" / "test"
sys.path.insert(0, str(TEST_DIR))
from datavine_phase4_demand_pull import run_case  # noqa: E402


def identity(value):
    return value


def start_worker(port, cores):
    return subprocess.Popen(
        [
            os.environ.get("VINE_WORKER", "vine_worker"),
            "127.0.0.1",
            str(port),
            "--cores",
            str(cores),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def stop_workers(workers):
    for worker in workers:
        if worker.poll() is not None:
            continue
        try:
            os.killpg(worker.pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
    for worker in workers:
        try:
            worker.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(worker.pid, signal.SIGKILL)
            worker.wait(timeout=10)


def wait_for_workers(manager, expected, timeout=30):
    deadline = time.monotonic() + timeout
    while len(manager.status("workers")) < expected:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"expected {expected} workers")
        manager.wait(1)


def run_functioncall(tasks, workers, cores):
    manager = Manager(port=0)
    library = manager.create_library_from_functions(
        "datavine-performance-baseline",
        identity,
        add_env=False,
        exec_mode="direct",
    )
    library.set_function_slots(cores)
    manager.install_library(library)
    processes = [start_worker(manager.port, cores) for _ in range(workers)]
    try:
        wait_for_workers(manager, workers)
        warmup = FunctionCall(
            "datavine-performance-baseline", "identity", -1
        )
        warmup.set_exec_method("direct")
        manager.submit(warmup)
        while True:
            completed = manager.wait(1)
            if completed and completed.id == warmup.id:
                if completed.output != -1:
                    raise RuntimeError("FunctionCall warmup returned wrong output")
                break

        started = time.monotonic()
        for value in range(tasks):
            task = FunctionCall(
                "datavine-performance-baseline", "identity", value
            )
            task.set_exec_method("direct")
            manager.submit(task)
        completed_count = 0
        while completed_count < tasks:
            completed = manager.wait(1)
            if completed is None:
                continue
            if not completed.successful():
                raise RuntimeError(
                    f"FunctionCall {completed.id} failed: {completed.result}"
                )
            completed_count += 1
        elapsed = time.monotonic() - started
        return {
            "mode": "functioncall-direct",
            "logical_tasks": tasks,
            "physical_tasks": tasks,
            "elapsed_seconds": elapsed,
            "tasks_per_second": tasks / elapsed,
        }
    finally:
        stop_workers(processes)
        manager._free()
        gc.collect()


def run_datavine(tasks, workers, cores, library_batch_size=4096):
    workflow = Workflow()
    logical_tasks = [
        workflow.add_task(identity, value) for value in range(tasks)
    ]
    snapshot = run_case(
        "performance-identity",
        workflow,
        logical_tasks[-1].task_id,
        tasks - 1,
        worker_count=workers,
        worker_cores=cores,
        prefetch=False,
        use_worker_library=True,
        library_batch_size=library_batch_size,
        workflow_timeout=max(180, tasks * 2),
        detailed_report=False,
    )
    elapsed = snapshot["workflow_elapsed_seconds"]
    report = snapshot["scheduler_report"]
    request_count = sum(
        value["count"]
        for value in report["scheduler_controller_requests"].values()
    )
    return {
        "mode": "datavine-library",
        "logical_tasks": tasks,
        "physical_tasks": report["physical_compute_submissions"],
        "elapsed_seconds": elapsed,
        "tasks_per_second": tasks / elapsed,
        "controller_requests": request_count,
        "controller_requests_per_task": request_count / tasks,
        "controller_request_counts": {
            route: value["count"]
            for route, value in sorted(
                report["scheduler_controller_requests"].items()
            )
        },
        "controller_request_metrics": report[
            "scheduler_controller_requests"
        ],
        "workflow_timing_seconds": report["workflow_timing_seconds"],
        "batch_worker_seconds": report["batch_worker_seconds"],
        "inline_task_values": report["inline_task_values"],
        "physical_batch_metrics": report["physical_batch_metrics"],
        "manager_timing_us": report["manager_timing_us"],
        "registration_timing_seconds": report[
            "registration_timing_seconds"
        ],
    }


def summarize(samples):
    values = [sample["tasks_per_second"] for sample in samples]
    return {
        "median_tasks_per_second": statistics.median(values),
        "min_tasks_per_second": min(values),
        "max_tasks_per_second": max(values),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=128)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--cores", type=int, default=4)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--minimum-ratio", type=float, default=1.0)
    parser.add_argument("--library-batch-size", type=int, default=4096)
    parser.add_argument("--datavine-only", action="store_true")
    args = parser.parse_args()
    if min(
        args.tasks,
        args.workers,
        args.cores,
        args.repetitions,
        args.library_batch_size,
    ) < 1:
        parser.error("tasks, workers, cores, and repetitions must be positive")

    if args.datavine_only:
        results = [
            run_datavine(
                args.tasks,
                args.workers,
                args.cores,
                args.library_batch_size,
            )
            for _ in range(args.repetitions)
        ]
        print(
            json.dumps(
                {
                    "benchmark": "datavine-identity-v1",
                    "samples": results,
                    "summary": summarize(results),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    samples = {"functioncall-direct": [], "datavine-library": []}
    for repetition in range(args.repetitions):
        runners = (
            (run_functioncall, run_datavine)
            if repetition % 2 == 0
            else (run_datavine, run_functioncall)
        )
        for runner in runners:
            result = (
                runner(
                    args.tasks,
                    args.workers,
                    args.cores,
                    args.library_batch_size,
                )
                if runner is run_datavine
                else runner(args.tasks, args.workers, args.cores)
            )
            samples[result["mode"]].append(result)

    baseline = summarize(samples["functioncall-direct"])
    datavine = summarize(samples["datavine-library"])
    ratio = (
        datavine["median_tasks_per_second"]
        / baseline["median_tasks_per_second"]
    )
    report = {
        "benchmark": "datavine-functioncall-identity-v1",
        "configuration": {
            "tasks": args.tasks,
            "workers": args.workers,
            "cores_per_worker": args.cores,
            "repetitions": args.repetitions,
            "library_batch_size": args.library_batch_size,
        },
        "samples": samples,
        "summary": {
            "functioncall-direct": baseline,
            "datavine-library": datavine,
            "datavine_to_functioncall_ratio": ratio,
            "minimum_ratio": args.minimum_ratio,
            "status": "PASS" if ratio >= args.minimum_ratio else "FAIL",
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["summary"]["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
