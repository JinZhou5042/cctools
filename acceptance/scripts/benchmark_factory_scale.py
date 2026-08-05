#!/usr/bin/env python3
"""Run independent empty DataVine tasks on an external worker pool."""

import argparse
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

from ndcctools.taskvine.datavine import Workflow
from ndcctools.taskvine.datavine.client import ControllerClient
from ndcctools.taskvine.datavine.scheduler import WorkflowDriver

from benchmark_support import ProcessTreeSampler


def empty_task():
    return None


def emit(event, **fields):
    print(
        json.dumps({"event": event, **fields}, sort_keys=True),
        file=sys.stderr,
        flush=True,
    )


def wait_ready(path, process, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"Data Controller exited with status {process.returncode}"
            )
        try:
            return json.loads(path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.05)
    raise TimeoutError("Data Controller did not become ready")


def wait_workers(scheduler, expected, timeout):
    started = time.monotonic()
    deadline = started + timeout
    last = -1
    while True:
        observed = scheduler.call("worker_count")
        if observed != last:
            emit(
                "worker-count",
                expected=expected,
                observed=observed,
                elapsed_seconds=time.monotonic() - started,
            )
            last = observed
        if observed == expected:
            return time.monotonic() - started
        if observed > expected:
            raise RuntimeError(
                f"observed {observed} workers, expected exactly {expected}"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"observed {observed} of {expected} workers before timeout"
            )


def controller_command(root, host, token, limits):
    return [
        sys.executable,
        "-m",
        "ndcctools.taskvine.datavine.controller.cli",
        "--host",
        "0.0.0.0",
        "--advertise-host",
        host,
        "--token",
        token,
        "--ready-file",
        str(root / "controller-ready.json"),
        "--persistence-dir",
        str(root / "durable"),
        "--max-edata-bytes",
        str(limits["edata"]),
        "--max-idata-bytes",
        str(limits["idata"]),
        "--max-inline-idata-bytes",
        str(limits["inline"]),
        "--max-serving-bytes",
        str(limits["serving"]),
        "--max-replicas",
        str(limits["replicas"]),
    ]


def compact_controller_snapshot(snapshot):
    keys = (
        "tasks",
        "edata",
        "idata",
        "publications",
        "idata_metadata_publications",
        "idata_bytes",
        "idata_bytes_high_water",
        "registrations",
        "deduplicated_registrations",
        "metadata",
        "native_journal",
        "replica_directory",
    )
    return {key: snapshot[key] for key in keys if key in snapshot}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=1_000_000)
    parser.add_argument("--workers", type=int, default=100)
    parser.add_argument("--cores", type=int, default=16)
    parser.add_argument("--manager-name", required=True)
    parser.add_argument("--worker-timeout", type=float, default=3600)
    parser.add_argument("--workflow-timeout", type=float, default=10800)
    args = parser.parse_args()
    if min(args.tasks, args.workers, args.cores) < 1:
        parser.error("tasks, workers, and cores must be positive")

    sampler = ProcessTreeSampler(interval_seconds=0.25).start()
    sampler_running = True
    workflow_started = time.monotonic()
    workflow = Workflow()
    for _ in range(args.tasks):
        workflow.add_task(empty_task)
    workflow_build_seconds = time.monotonic() - workflow_started
    emit(
        "workflow-built",
        tasks=args.tasks,
        elapsed_seconds=workflow_build_seconds,
    )

    limits = {
        "edata": 64 * 1024 * 1024,
        "idata": 64 * 1024 * 1024,
        "inline": 8 * 1024 * 1024,
        "serving": 256 * 1024 * 1024,
        "replicas": max(10_000_000, args.tasks * 2),
    }
    host = socket.getfqdn()
    token = f"token-{args.manager_name}"
    scheduler = None
    controller = None
    controller_stderr = None
    with tempfile.TemporaryDirectory(
        prefix="datavine-factory-scale-"
    ) as temporary:
        root = Path(temporary)
        controller_stderr = (root / "controller.stderr").open("w")
        controller = subprocess.Popen(
            controller_command(root, host, token, limits),
            stdout=subprocess.DEVNULL,
            stderr=controller_stderr,
            text=True,
        )
        try:
            ready = wait_ready(root / "controller-ready.json", controller)
            client = ControllerClient(
                f"http://{host}:{ready['port']}",
                token,
                native_endpoint=f"tcp://{host}:{ready['native_port']}",
            )
            scheduler = WorkflowDriver(client).start()
            port = scheduler.call(
                "create_manager",
                0,
                args.manager_name,
                str(root / "manager"),
                True,
            )
            emit(
                "manager-ready",
                host=host,
                manager_name=args.manager_name,
                port=port,
            )
            worker_wait_seconds = wait_workers(
                scheduler, args.workers, args.worker_timeout
            )
            warmup_started = time.monotonic()
            scheduler.call("warm_worker_library")
            warmup_seconds = time.monotonic() - warmup_started
            workers_at_dispatch = scheduler.call("worker_count")
            if workers_at_dispatch != args.workers:
                raise RuntimeError(
                    f"worker count changed to {workers_at_dispatch} before dispatch"
                )
            emit(
                "dispatch-start",
                cores_per_worker=args.cores,
                tasks=args.tasks,
                warmup_seconds=warmup_seconds,
                workers=workers_at_dispatch,
            )
            dispatch_started = time.monotonic()
            future = scheduler.submit(
                "run_workflow",
                workflow,
                environment=None,
                wait_timeout=1,
                worker_dram_cache_bytes=256 * 1024 * 1024,
                result_task_ids=[args.tasks],
                detailed_report=False,
            )
            deadline = dispatch_started + args.workflow_timeout
            next_progress = dispatch_started + 60
            while not future.done():
                now = time.monotonic()
                if now >= deadline:
                    raise TimeoutError("million-task workflow timed out")
                if now >= next_progress:
                    emit(
                        "workflow-running",
                        elapsed_seconds=now - dispatch_started,
                    )
                    next_progress = now + 60
                time.sleep(1)
            results = future.result()
            elapsed = time.monotonic() - dispatch_started
            if results != {args.tasks: None}:
                raise RuntimeError("empty-task workflow returned wrong result")
            report = scheduler.call("last_run_report")
            snapshot = compact_controller_snapshot(client.snapshot())
            resources = sampler.stop()
            sampler_running = False
            output = {
                "benchmark": "datavine-factory-independent-empty-v1",
                "configuration": {
                    "tasks": args.tasks,
                    "workers": args.workers,
                    "cores_per_worker": args.cores,
                    "total_cores": args.workers * args.cores,
                    "manager_name": args.manager_name,
                    "task_batching": False,
                    "application_compute": "empty function returning None",
                    "application_io": False,
                },
                "status": "PASS",
                "workflow_build_seconds": workflow_build_seconds,
                "worker_wait_seconds": worker_wait_seconds,
                "library_warmup_seconds": warmup_seconds,
                "workers_at_dispatch": workers_at_dispatch,
                "elapsed_seconds": elapsed,
                "tasks_per_second": args.tasks / elapsed,
                "physical_compute_submissions": report[
                    "physical_compute_submissions"
                ],
                "physical_attempts": report["physical_attempts"],
                "recovery_reexecutions": report["recovery_reexecutions"],
                "workflow_timing_seconds": report[
                    "workflow_timing_seconds"
                ],
                "registration_timing_seconds": report[
                    "registration_timing_seconds"
                ],
                "manager_timing_us": report["manager_timing_us"],
                "worker_seconds": report["worker_seconds"],
                "worker_timing_seconds": report[
                    "worker_timing_seconds"
                ],
                "performance_bottlenecks": report[
                    "performance_bottlenecks"
                ],
                "controller_request_metrics": report[
                    "scheduler_controller_requests"
                ],
                "controller": snapshot,
                "driver_resources": resources,
            }
            print(json.dumps(output, indent=2, sort_keys=True))
        finally:
            if sampler_running:
                sampler.stop()
            if scheduler is not None:
                scheduler.stop()
            if controller is not None and controller.poll() is None:
                controller.terminate()
                try:
                    controller.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    controller.kill()
                    controller.wait(timeout=10)
            if controller_stderr is not None:
                controller_stderr.close()


if __name__ == "__main__":
    main()
