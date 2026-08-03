#!/usr/bin/env python3
"""Compare DataVine with TaskVine Python execution architectures."""

import argparse
import gc
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from ndcctools.taskvine import FunctionCall, Manager, PythonTask
from ndcctools.taskvine.datavine import Workflow

from benchmark_support import (
    BoundedLatencySampler,
    ProcessTreeSampler,
    latency_summary,
    throughput_summary,
)


TEST_DIR = Path(__file__).resolve().parents[2] / "taskvine" / "test"
sys.path.insert(0, str(TEST_DIR))
from datavine_phase4_demand_pull import run_case  # noqa: E402


FUNCTIONCALL_MODES = frozenset(("functioncall-direct", "functioncall-fork"))
ALL_MODES = ("datavine", "functioncall-direct", "functioncall-fork", "pythontask")


def benchmark_work(value, payload, compute_steps):
    """Execute deterministic real work with a reusable serialized payload."""
    state = int(value) & 0xFFFFFFFF
    for step in range(int(compute_steps)):
        state = (state * 1664525 + 1013904223 + step) & 0xFFFFFFFF
    return int(value), len(payload), state


def expected_result(value, payload_bytes, compute_steps):
    state = int(value) & 0xFFFFFFFF
    for step in range(int(compute_steps)):
        state = (state * 1664525 + 1013904223 + step) & 0xFFFFFFFF
    return int(value), int(payload_bytes), state


def make_payload(size):
    size = int(size)
    if size < 0:
        raise ValueError("payload size cannot be negative")
    block = bytes(range(251))
    return (block * ((size + len(block) - 1) // len(block)))[:size]


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


def _wait_for_task(manager, task_id):
    while True:
        completed = manager.wait(1)
        if completed is not None and completed.id == task_id:
            return completed


def run_taskvine(
    mode,
    tasks,
    workers,
    cores,
    payload,
    compute_steps,
    latency_sample_capacity,
    process_sample_interval,
):
    manager = Manager(port=0)
    library_name = f"datavine-architecture-{mode}"
    if mode in FUNCTIONCALL_MODES:
        exec_method = mode.removeprefix("functioncall-")
        library = manager.create_library_from_functions(
            library_name,
            benchmark_work,
            add_env=False,
            exec_mode=exec_method,
        )
        library.set_function_slots(cores)
        manager.install_library(library)
    processes = [start_worker(manager.port, cores) for _ in range(workers)]
    sampler = None
    try:
        wait_for_workers(manager, workers)
        if mode in FUNCTIONCALL_MODES:
            warmup = FunctionCall(
                library_name,
                "benchmark_work",
                -1,
                payload,
                compute_steps,
            )
            warmup.set_exec_method(mode.removeprefix("functioncall-"))
        else:
            warmup = PythonTask(benchmark_work, -1, payload, compute_steps)
        manager.submit(warmup)
        completed = _wait_for_task(manager, warmup.id)
        if completed.output != expected_result(-1, len(payload), compute_steps):
            raise RuntimeError(f"{mode} warmup returned wrong output")

        expected_outputs = [
            expected_result(value, len(payload), compute_steps)
            for value in range(tasks)
        ]
        expected_by_task_id = {}
        latencies = BoundedLatencySampler(tasks, latency_sample_capacity)
        sampler = ProcessTreeSampler(
            interval_seconds=process_sample_interval
        ).start()
        started = time.monotonic()
        submit_started = started
        for value in range(tasks):
            if mode in FUNCTIONCALL_MODES:
                task = FunctionCall(
                    library_name,
                    "benchmark_work",
                    value,
                    payload,
                    compute_steps,
                )
                task.set_exec_method(mode.removeprefix("functioncall-"))
            else:
                task = PythonTask(
                    benchmark_work, value, payload, compute_steps
                )
            task_id = manager.submit(task)
            expected_by_task_id[task_id] = expected_outputs[value]
            latencies.submitted(value, task_id)
        submit_seconds = time.monotonic() - submit_started
        completed_count = 0
        while completed_count < tasks:
            completed = manager.wait(1)
            if completed is None:
                continue
            if not completed.successful():
                raise RuntimeError(
                    f"{mode} task {completed.id} failed: {completed.result}"
                )
            expected = expected_by_task_id.pop(completed.id)
            if completed.output != expected:
                raise RuntimeError(
                    f"{mode} task {completed.id} returned wrong output"
                )
            latencies.completed(completed.id)
            completed_count += 1
        elapsed = time.monotonic() - started
        process_metrics = sampler.stop()
        sampler = None
        return {
            "mode": mode,
            "logical_tasks": tasks,
            "physical_tasks": tasks,
            "task_submission_seconds": submit_seconds,
            "workflow_elapsed_seconds": elapsed,
            "application_elapsed_seconds": elapsed,
            "workflow_tasks_per_second": tasks / elapsed,
            "application_tasks_per_second": tasks / elapsed,
            "completion_latency": latencies.summary(),
            "process_tree": process_metrics,
        }
    finally:
        if sampler is not None:
            sampler.stop()
        stop_workers(processes)
        manager._free()
        gc.collect()


def run_datavine(
    tasks,
    workers,
    cores,
    payload,
    compute_steps,
    library_batch_size,
    process_sample_interval,
    compact_task_records=True,
):
    sampler = ProcessTreeSampler(
        interval_seconds=process_sample_interval
    ).start()
    try:
        oracle = expected_result(tasks - 1, len(payload), compute_steps)
        application_started = time.monotonic()
        build_started = application_started
        workflow = Workflow()
        logical_tasks = [
            workflow.add_task(benchmark_work, value, payload, compute_steps)
            for value in range(tasks)
        ]
        build_seconds = time.monotonic() - build_started
        snapshot = run_case(
            f"architecture-{tasks}-{len(payload)}-{compute_steps}",
            workflow,
            logical_tasks[-1].task_id,
            oracle,
            worker_count=workers,
            worker_cores=cores,
            prefetch=False,
            use_worker_library=True,
            library_batch_size=library_batch_size,
            compact_task_records=compact_task_records,
            workflow_timeout=max(180, tasks * 2),
            detailed_report=False,
        )
        application_wall_seconds = time.monotonic() - application_started
        process_metrics = sampler.stop()
        sampler = None
    finally:
        if sampler is not None:
            sampler.stop()
    workflow_elapsed = snapshot["workflow_elapsed_seconds"]
    application_elapsed = build_seconds + workflow_elapsed
    report = snapshot["scheduler_report"]
    request_count = sum(
        value["count"]
        for value in report["scheduler_controller_requests"].values()
    )
    batch_latencies = [
        (
            batch["time_when_done"] - batch["time_when_submitted"]
        )
        / 1_000_000
        for batch in report["physical_batch_metrics"]
    ]
    request_metrics = report["scheduler_controller_requests"]
    worker_cache_rejections = sum(
        worker["worker_cache_admission_rejections"]
        for worker in report["worker_physical_cache"]
    )
    return {
        "mode": "datavine",
        "task_record_wire_format": (
            "task-record-row-v1" if compact_task_records else "legacy-object"
        ),
        "logical_tasks": tasks,
        "physical_tasks": report["physical_compute_submissions"],
        "workflow_build_seconds": build_seconds,
        "workflow_elapsed_seconds": workflow_elapsed,
        "application_elapsed_seconds": application_elapsed,
        "application_wall_seconds_including_cleanup": application_wall_seconds,
        "workflow_tasks_per_second": tasks / workflow_elapsed,
        "application_tasks_per_second": tasks / application_elapsed,
        "controller_requests": request_count,
        "controller_requests_per_task": request_count / tasks,
        "controller_request_metrics": request_metrics,
        "workflow_timing_seconds": report["workflow_timing_seconds"],
        "registration_timing_seconds": report["registration_timing_seconds"],
        "manager_timing_us": report["manager_timing_us"],
        "manager_bytes": report["manager_bytes"],
        "physical_batch_latency": latency_summary(batch_latencies),
        "data_path_metrics": {
            "local_idata_hits": report["local_idata_hits"],
            "controller_idata_fetches": request_metrics.get(
                "GET /v1/idata/{id}", {}
            ).get("count", 0),
            "peer_transfer_starts": report["peer_transfer_faults"][
                "peer_transfer_starts"
            ],
            "peer_alternate_source_fallbacks": report[
                "peer_transfer_faults"
            ]["peer_alternate_source_fallbacks"],
            "worker_cache_evictions": report[
                "worker_disk_cache_evictions"
            ],
            "worker_cache_admission_rejections": worker_cache_rejections,
        },
        "peer_transfer_faults": report["peer_transfer_faults"],
        "performance_bottlenecks": report["performance_bottlenecks"],
        "process_tree": process_metrics,
    }


def parse_modes(value):
    modes = tuple(item.strip() for item in value.split(",") if item.strip())
    unknown = sorted(set(modes) - set(ALL_MODES))
    if not modes or unknown:
        raise argparse.ArgumentTypeError(
            f"modes must be selected from {','.join(ALL_MODES)}; unknown={unknown}"
        )
    return modes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=128)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--cores", type=int, default=4)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--compute-steps", type=int, default=0)
    parser.add_argument("--library-batch-size", type=int, default=4096)
    parser.add_argument("--latency-sample-capacity", type=int, default=10_000)
    parser.add_argument("--process-sample-interval", type=float, default=0.1)
    parser.add_argument(
        "--modes",
        type=parse_modes,
        default=parse_modes(
            "datavine,functioncall-direct,functioncall-fork,pythontask"
        ),
    )
    parser.add_argument("--minimum-datavine-ratio", type=float, default=1.0)
    parser.add_argument(
        "--legacy-datavine-task-records",
        action="store_true",
        help="use the rollback-compatible legacy TaskRecord JSON objects",
    )
    args = parser.parse_args()
    if min(
        args.tasks,
        args.workers,
        args.cores,
        args.repetitions,
        args.library_batch_size,
        args.latency_sample_capacity,
    ) < 1:
        parser.error(
            "task, worker, core, repetition, batch, and sample counts "
            "must be positive"
        )
    if min(
        args.payload_bytes,
        args.compute_steps,
        args.process_sample_interval,
    ) < 0:
        parser.error("payload, compute, and sampling values cannot be negative")
    if args.process_sample_interval == 0:
        parser.error("process sampling interval must be positive")

    payload = make_payload(args.payload_bytes)
    samples = {mode: [] for mode in args.modes}
    for repetition in range(args.repetitions):
        rotation = repetition % len(args.modes)
        ordered_modes = args.modes[rotation:] + args.modes[:rotation]
        for mode in ordered_modes:
            if mode == "datavine":
                sample = run_datavine(
                    args.tasks,
                    args.workers,
                    args.cores,
                    payload,
                    args.compute_steps,
                    args.library_batch_size,
                    args.process_sample_interval,
                    not args.legacy_datavine_task_records,
                )
            else:
                sample = run_taskvine(
                    mode,
                    args.tasks,
                    args.workers,
                    args.cores,
                    payload,
                    args.compute_steps,
                    args.latency_sample_capacity,
                    args.process_sample_interval,
                )
            samples[mode].append(sample)

    summaries = {
        mode: throughput_summary(values) for mode, values in samples.items()
    }
    baselines = [
        value["median_tasks_per_second"]
        for mode, value in summaries.items()
        if mode != "datavine"
    ]
    ratio = None
    status = "PASS"
    if "datavine" in summaries and baselines:
        ratio = (
            summaries["datavine"]["median_tasks_per_second"]
            / max(baselines)
        )
        status = (
            "PASS"
            if ratio >= args.minimum_datavine_ratio
            else "FAIL"
        )
    report = {
        "benchmark": "datavine-architecture-matrix-v1",
        "configuration": {
            "tasks": args.tasks,
            "workers": args.workers,
            "cores_per_worker": args.cores,
            "repetitions": args.repetitions,
            "payload_bytes": args.payload_bytes,
            "compute_steps": args.compute_steps,
            "library_batch_size": args.library_batch_size,
            "datavine_task_record_format": (
                "legacy-object"
                if args.legacy_datavine_task_records
                else "task-record-row-v1"
            ),
            "modes": list(args.modes),
        },
        "samples": samples,
        "summary": {
            "architectures": summaries,
            "datavine_to_fastest_baseline_ratio": ratio,
            "minimum_datavine_ratio": args.minimum_datavine_ratio,
            "status": status,
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
