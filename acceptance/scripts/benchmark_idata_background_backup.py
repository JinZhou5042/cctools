#!/usr/bin/env python3
"""Measure logical progress separately from Controller iData backup drain."""

import argparse
import base64
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time

from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


def stop(process):
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGTERM)
    if process is not None:
        try:
            process.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=20)


def wait_for_log(path, pattern, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        text = path.read_text() if path.exists() else ""
        matches = list(pattern.finditer(text))
        if matches:
            return matches[-1]
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {pattern.pattern}")


def data_file_count(root):
    return sum(1 for _ in root.glob("*/*.data")) if root.exists() else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--cores", type=int, default=16)
    parser.add_argument("--chunk-tasks", type=int, default=10_000)
    parser.add_argument(
        "--executor", choices=("command", "builtin"), default="command"
    )
    parser.add_argument(
        "--mode",
        choices=("worker-local", "controller-background"),
        required=True,
    )
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.tasks, args.workers, args.cores, args.chunk_tasks) < 1:
        parser.error("tasks, workers, cores, and chunk-tasks must be positive")

    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    workflow_id = f"idata-backup-{args.executor}-{args.mode}-{args.tasks}"
    token = "idata-background-benchmark-token"
    args.output = args.output.resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    log_path = Path(f"{args.output}.service.log")

    with tempfile.TemporaryDirectory(prefix="datavine-idata-backup-") as root_name:
        root = Path(root_name)
        journal = root / "journal"
        with log_path.open("w") as service_log:
            service = subprocess.Popen(
                (str(service_binary), "serve", str(journal), token),
                stdout=subprocess.PIPE,
                stderr=service_log,
                text=True,
                env=dict(
                    os.environ,
                    DATAVINE_WORKFLOW_METRICS="1",
                    DATAVINE_PERSISTENCE_DIAGNOSTICS="1",
                ),
            )
            workers = []
            client = None
            try:
                contact = json.loads(service.stdout.readline())
                for _ in range(args.workers):
                    workers.append(subprocess.Popen(
                        (
                            str(worker_binary),
                            f"--cores={args.cores}",
                            "--memory=2048",
                            "--disk=4096",
                            "--idle-timeout=120",
                            "localhost",
                            str(contact["manager_port"]),
                        ),
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        text=True,
                    ))
                client = WorkflowClient(contact["endpoint"], token)
                initial = {
                    "schema": "datavine.workflow/v1",
                    "workflow_id": workflow_id,
                    "idempotency_key": f"{workflow_id}-initial",
                    "mode": "streaming",
                    "tasks": [],
                    "data": (
                        [{
                            "data_id": 1,
                            "codec": {
                                "name": "datavine/builtin",
                                "version": "1",
                            },
                            "origin": {
                                "kind": "inline",
                                "base64": base64.b64encode(b"DVB1\x01").decode(),
                            },
                        }]
                        if args.executor == "builtin" else []
                    ),
                    "requested_outputs": [],
                    "policy": {
                        "maximum_tasks": args.tasks,
                        "maximum_edges": 0,
                        "idata_backup": args.mode,
                    },
                }
                generation = client.submit_workflow(initial)["generation"]
                started = time.monotonic()
                for start in range(1, args.tasks + 1, args.chunk_tasks):
                    stop_id = min(args.tasks + 1, start + args.chunk_tasks)
                    executor = (
                        {
                            "kind": "taskvine",
                            "version": "builtin-v1",
                            "payload_ref": 1,
                        }
                        if args.executor == "builtin"
                        else {
                            "kind": "command",
                            "version": "1",
                            "argv": ["/bin/true"],
                        }
                    )
                    delta = {
                        "schema": "datavine.workflow-delta/v1",
                        "workflow_id": workflow_id,
                        "idempotency_key": f"{workflow_id}-{start}-{stop_id - 1}",
                        "task_defaults": {"executor": executor},
                        "data_defaults": {
                            "codec": {"name": "bytes", "version": "1"}
                        },
                        "tasks": [
                            [task_id, [], [
                                task_id + 1
                                if args.executor == "builtin" else task_id
                            ]]
                            for task_id in range(start, stop_id)
                        ],
                        "data": [
                            [
                                data_id + 1
                                if args.executor == "builtin" else data_id,
                                data_id,
                                0,
                            ]
                            for data_id in range(start, stop_id)
                        ],
                        "requested_outputs": [],
                    }
                    generation = client.append_workflow(
                        workflow_id, generation, delta
                    )["generation"]

                completed_pattern = re.compile(
                    rf"datavine workflow {re.escape(workflow_id)} .*"
                    r"run_seconds=([0-9.]+).*"
                    rf"physical_completions={args.tasks}(?: |$)"
                )
                completed = wait_for_log(log_path, completed_pattern, args.timeout)
                logical_seconds = time.monotonic() - started
                service_run_seconds = float(completed.group(1))

                backup_seconds = None
                backup_files = 0
                if args.mode == "controller-background":
                    deadline = time.monotonic() + args.timeout
                    data_root = Path(f"{journal}.data")
                    while time.monotonic() < deadline:
                        backup_files = data_file_count(data_root)
                        if backup_files == args.tasks:
                            break
                        time.sleep(0.05)
                    if backup_files != args.tasks:
                        raise TimeoutError(
                            f"background backup stopped at {backup_files}/{args.tasks}"
                        )
                    backup_seconds = time.monotonic() - started

                terminal = client.seal_workflow(workflow_id, generation)
                terminal = client.wait_workflow(workflow_id, timeout=args.timeout)
                if terminal["state"] != "completed":
                    raise RuntimeError(terminal)
                data_root = Path(f"{journal}.data")
                gc_deadline = time.monotonic() + min(args.timeout, 10)
                post_gc_files = data_file_count(data_root)
                while post_gc_files and time.monotonic() < gc_deadline:
                    time.sleep(0.01)
                    post_gc_files = data_file_count(data_root)
                if post_gc_files:
                    raise RuntimeError(
                        f"logical GC retained {post_gc_files} unrequested backups"
                    )

                service_log.flush()
                persistence_lines = [
                    line for line in log_path.read_text().splitlines()
                    if f"datavine controller_persistence {workflow_id} " in line
                ]
                persistence = {}
                if persistence_lines:
                    persistence = dict(re.findall(
                        r"([a-z_]+)=([^ ]+)", persistence_lines[-1]
                    ))
                evidence = {
                    "status": "PASS",
                    "tasks": args.tasks,
                    "workers": args.workers,
                    "cores": args.cores,
                    "mode": args.mode,
                    "executor": args.executor,
                    "logical_seconds": logical_seconds,
                    "service_run_seconds": service_run_seconds,
                    "logical_tasks_per_second": args.tasks / logical_seconds,
                    "service_tasks_per_second": args.tasks / service_run_seconds,
                    "backup_drain_seconds": backup_seconds,
                    "backup_files": backup_files,
                    "post_gc_files": post_gc_files,
                    "backup_files_per_second": (
                        args.tasks / backup_seconds if backup_seconds else None
                    ),
                    "background_jobs": int(
                        persistence.get("agent_background_backup_jobs", 0)
                    ),
                    "background_bytes": int(
                        persistence.get("agent_background_backup_bytes", 0)
                    ),
                    "background_peak_backlog": int(
                        persistence.get("agent_background_backup_peak_backlog", 0)
                    ),
                    "terminal_state": terminal["state"],
                    "workload": (
                        "independent builtin-v1 tasks with one empty live iData"
                        if args.executor == "builtin"
                        else "independent /bin/true tasks with one empty live iData"
                    ),
                }
                args.output.write_text(
                    json.dumps(evidence, indent=2, sort_keys=True) + "\n"
                )
                print(json.dumps(evidence, sort_keys=True))
            finally:
                if client is not None:
                    client.close()
                for worker in workers:
                    stop(worker)
                stop(service)


if __name__ == "__main__":
    main()
