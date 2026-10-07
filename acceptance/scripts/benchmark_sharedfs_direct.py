#!/usr/bin/env python3
"""Measure unthrottled Worker-direct durable writes to a shared filesystem."""

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import time

import ndcctools.taskvine as vine

from evidence_layout import log_path


def stop_group(process, timeout=60):
    if process is None or process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=timeout)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=30)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--files", type=int, required=True)
    parser.add_argument("--bytes-per-file", type=int, required=True)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--cores", type=int, default=16)
    parser.add_argument("--memory", type=int, default=3072)
    parser.add_argument("--disk", type=int, default=10240)
    parser.add_argument("--batch-type", choices=("local", "condor"), default="condor")
    parser.add_argument("--shared-root", type=Path, required=True)
    parser.add_argument("--worker-timeout", type=int, default=1200)
    parser.add_argument("--workflow-timeout", type=int, default=1800)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.files, args.workers, args.cores, args.memory, args.disk) < 1:
        parser.error("counts and resources must be positive")
    if args.bytes_per_file < 0:
        parser.error("bytes-per-file cannot be negative")
    return args


def main():
    args = parse_args()
    repository = Path(__file__).resolve().parents[2]
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    writer_source = repository / "acceptance/helpers/storage_file_writer.c"
    args.output = args.output.resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    shared_parent = args.shared_root.resolve()
    shared_parent.mkdir(parents=True, exist_ok=True)
    run_root = shared_parent / (
        f"run-{int(time.time())}-{os.getpid()}-{args.bytes_per_file}"
    )
    run_root.mkdir()

    factory = None
    factory_log = log_path(args.output, "factory").open("w")
    with tempfile.TemporaryDirectory(prefix="datavine-sharedfs-direct-") as state_name:
        state = Path(state_name)
        writer = state / "storage_file_writer"
        subprocess.run(
            ("cc", "-O2", "-Wall", "-Wextra", "-Werror", "-o", str(writer),
             str(writer_source)),
            check=True,
        )
        previous_directory = Path.cwd()
        os.chdir(state)
        try:
            manager = vine.Manager(port=0)
        finally:
            os.chdir(previous_directory)
        manager.set_name(f"datavine-sharedfs-direct-{os.getpid()}")
        if manager.tune("attempt-schedule-depth", args.workers * args.cores) != 0:
            raise RuntimeError("TaskVine scheduling-depth tune is unavailable")
        if manager.tune("wait-for-workers", args.workers + 1) != 0:
            raise RuntimeError("TaskVine admission gate is unavailable")
        writer_file = manager.declare_file(str(writer), cache=True)
        for item in range(args.files):
            task = vine.Task(
                f"./storage_file_writer {run_root} {args.bytes_per_file} {item}"
            )
            task.set_cores(1)
            task.set_tag(str(item))
            task.add_input(writer_file, "storage_file_writer")
            manager.submit(task)

        manager_host = "localhost" if args.batch_type == "local" else socket.getfqdn()
        factory_command = (
            str(repository / "batch_job/src/vine_factory"),
            "--batch-type", args.batch_type,
            "--min-workers", str(args.workers),
            "--max-workers", str(args.workers),
            "--workers-per-cycle", str(args.workers),
            "--factory-period", "1",
            "--factory-timeout", str(args.workflow_timeout + 120),
            "--timeout", str(args.workflow_timeout + 60),
            "--cores", str(args.cores),
            "--memory", str(args.memory),
            "--disk", str(args.disk),
            "--gpus", "0",
            "--worker-binary", str(worker_binary),
            "--scratch-dir", str(state / "factory"),
            "--parent-death", manager_host, str(manager.port),
        )
        try:
            factory = subprocess.Popen(
                factory_command,
                stdout=factory_log,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            admission_started = time.monotonic()
            deadline = admission_started + args.worker_timeout
            while time.monotonic() < deadline:
                if factory.poll() is not None:
                    raise RuntimeError(f"vine_factory exited with {factory.returncode}")
                unexpected = manager.wait(1)
                if unexpected is not None:
                    raise RuntimeError("task completed while admission gate was closed")
                if (
                    int(manager.stats.workers_connected) == args.workers
                    and int(manager.stats.total_cores) == args.workers * args.cores
                ):
                    break
            else:
                raise TimeoutError("exact Worker topology was not admitted")
            admission_seconds = time.monotonic() - admission_started
            if manager.tune("wait-for-workers", 0) != 0:
                raise RuntimeError("could not open TaskVine scheduling gate")

            started = time.monotonic()
            completed = failed = 0
            peak_running = peak_cores = 0
            deadline = started + args.workflow_timeout
            while completed < args.files:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"{args.files - completed} tasks remain")
                task = manager.wait(1)
                peak_running = max(peak_running, int(manager.stats.tasks_running))
                peak_cores = max(peak_cores, int(manager.stats.committed_cores))
                if task is None:
                    continue
                completed += 1
                if not task.successful():
                    failed += 1
            seconds = time.monotonic() - started
            output_files = list(run_root.glob("data.*"))
            output_bytes = sum(path.stat().st_size for path in output_files)
            expected_bytes = args.files * args.bytes_per_file
            if failed or len(output_files) != args.files or output_bytes != expected_bytes:
                raise RuntimeError({
                    "failed_tasks": failed,
                    "files": len(output_files),
                    "bytes": output_bytes,
                    "expected_files": args.files,
                    "expected_bytes": expected_bytes,
                })
            result = {
                "status": "PASS",
                "mode": "worker_direct_sharedfs",
                "durability": "per-file fsync then atomic rename; no directory fsync",
                "software_concurrency_limit": None,
                "files": args.files,
                "bytes_per_file": args.bytes_per_file,
                "total_bytes": output_bytes,
                "workers": args.workers,
                "cores_per_worker": args.cores,
                "available_cores": args.workers * args.cores,
                "admission_seconds": admission_seconds,
                "execution_seconds": seconds,
                "files_per_second": args.files / seconds,
                "mib_per_second": output_bytes / seconds / 1048576,
                "peak_running_tasks": peak_running,
                "peak_committed_cores": peak_cores,
                "completed_tasks": completed,
                "failed_tasks": failed,
                "verified_files": len(output_files),
                "verified_bytes": output_bytes,
                "shared_parent": str(shared_parent),
                "factory_command": list(factory_command),
            }
            args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
            print(json.dumps(result, sort_keys=True))
        finally:
            stop_group(factory)
            factory_log.close()
            shutil.rmtree(run_root, ignore_errors=True)


if __name__ == "__main__":
    main()
