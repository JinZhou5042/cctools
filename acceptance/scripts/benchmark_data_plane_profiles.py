#!/usr/bin/env python3
"""Reproducible local stage-profile experiment for the production data plane."""

import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time

from ndcctools.taskvine.datavine import Workflow, WorkflowClient


METRIC = re.compile(r"(?P<name>[a-z_]+)=(?P<value>[^ ]+)")


def measured_work(payload, cpu_milliseconds):
    deadline = time.thread_time() + cpu_milliseconds / 1000
    value = 0
    while time.thread_time() < deadline:
        value = (value * 1664525 + 1013904223) & 0xFFFFFFFF
    return len(payload) + (value & 0)


def wait_terminal(client, workflow_id, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = client.describe_workflow(workflow_id)
        if info["state"] in {"completed", "failed", "cancelled"}:
            return info
        time.sleep(0.02)
    raise TimeoutError(client.describe_workflow(workflow_id))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, required=True)
    parser.add_argument("--input-bytes", type=int, required=True)
    parser.add_argument("--cpu-ms", type=int, required=True)
    parser.add_argument("--unique-inputs", action="store_true")
    parser.add_argument("--cores", type=int, default=8)
    return parser.parse_args()


def main():
    args = parse_args()
    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    workflow_id = "profile-experiment"
    token = "profile-experiment-token"
    with tempfile.TemporaryDirectory(prefix="datavine-profile-") as root:
        root = Path(root)
        service_log_path = root / "service.log"
        worker_log_path = root / "worker.log"
        with service_log_path.open("w") as service_log:
            service = subprocess.Popen(
                (str(service_binary), "serve", str(root / "journal"), token),
                stdout=subprocess.PIPE, stderr=service_log, text=True,
                env=dict(os.environ, DATAVINE_WORKFLOW_METRICS="1"),
            )
            worker = None
            try:
                contact = json.loads(service.stdout.readline())
                worker = subprocess.Popen((
                    str(worker_binary), "-d", "vine", "-o", str(worker_log_path),
                    f"--cores={args.cores}", "--memory=4096", "--disk=8192",
                    "--idle-timeout=30", "localhost", str(contact["manager_port"]),
                ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                client = WorkflowClient(contact["endpoint"], token)
                workflow = Workflow(
                    "profile-experiment-submit", workflow_id=workflow_id,
                    maximum_tasks=args.tasks,
                    maximum_edges=args.tasks,
                )
                shared = None
                if not args.unique_inputs:
                    shared = workflow.python_value(b"x" * args.input_bytes)
                outputs = []
                for index in range(args.tasks):
                    source = shared
                    if source is None:
                        prefix = index.to_bytes(8, "big")
                        source = workflow.python_value(
                            (prefix + b"x" * args.input_bytes)[:args.input_bytes]
                        )
                    outputs.append(workflow.python_callable(
                        measured_work, source, args.cpu_ms
                    ))
                workflow.request(*outputs)
                started = time.monotonic()
                workflow.submit(client)
                info = wait_terminal(client, workflow_id)
                wall_seconds = time.monotonic() - started
                if info["state"] != "completed":
                    raise RuntimeError({
                        "workflow": info,
                        "service_log_tail": service_log_path.read_text().splitlines()[-40:],
                        "worker_log_tail": worker_log_path.read_text().splitlines()[-40:],
                    })
                client.close()
                client_profile = workflow.profile()
            finally:
                if worker is not None and worker.poll() is None:
                    worker.send_signal(signal.SIGTERM)
                    worker.communicate(timeout=20)
                if service.poll() is None:
                    service.send_signal(signal.SIGTERM)
                    service.communicate(timeout=20)
        metric_lines = [
            line for line in service_log_path.read_text().splitlines()
            if f"datavine workflow {workflow_id} " in line
            and "dominant_stage=" in line
        ]
        if len(metric_lines) != 1:
            raise RuntimeError(metric_lines)
        runtime_profile = {
            match.group("name"): match.group("value")
            for match in METRIC.finditer(metric_lines[0])
        }
        result = {
            "tasks": args.tasks,
            "cores": args.cores,
            "input_bytes_each": args.input_bytes,
            "unique_inputs": args.unique_inputs,
            "cpu_milliseconds_each": args.cpu_ms,
            "wall_seconds": wall_seconds,
            "worker_direct_pulls": worker_log_path.read_text().count(
                "cache: transferring datavine-file://"
            ),
            "client_profile": client_profile,
            "runtime_profile": runtime_profile,
        }
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
