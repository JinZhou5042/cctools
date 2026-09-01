#!/usr/bin/env python3

import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


def wait_state(client, workflow_id, states, generation=None, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = client.describe_workflow(workflow_id)
        if info["state"] in states and (
            generation is None or info["generation"] == generation
        ):
            return info
        time.sleep(0.05)
    raise AssertionError(client.describe_workflow(workflow_id))


def output(data_id, task_id):
    return {
        "data_id": data_id,
        "codec": {"name": "text/utf-8", "version": "1"},
        "origin": {"kind": "output", "task_id": task_id, "output_index": 0},
    }


def task(task_id, argv, inputs, output_id):
    return {
        "task_id": task_id,
        "executor": {"kind": "command", "version": "1", "argv": argv},
        "inputs": [
            {"position": position, "data_id": data_id}
            for position, data_id in enumerate(inputs)
        ],
        "output_data_ids": [output_id],
    }


def main():
    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    token = "dynamic-workflow-test-token"
    workflow_id = "result-driven-dynamic"
    initial = {
        "schema": "datavine.workflow/v1",
        "workflow_id": workflow_id,
        "idempotency_key": "dynamic-initial-v1",
        "mode": "streaming",
        "tasks": [task(1, ["/usr/bin/printf", "7"], [], 1)],
        "data": [output(1, 1)],
        "requested_outputs": [1],
        "policy": {"maximum_tasks": 3, "maximum_edges": 2},
    }
    delta2 = {
        "schema": "datavine.workflow-delta/v1",
        "workflow_id": workflow_id,
        "idempotency_key": "dynamic-square-v1",
        "tasks": [
            task(
                2,
                [
                    "/bin/sh",
                    "-c",
                    'value=$(cat "$1"); printf "%s" "$((value * value))"',
                    "datavine",
                    "{{data:1}}",
                ],
                [1],
                2,
            )
        ],
        "data": [output(2, 2)],
        "requested_outputs": [2],
    }
    delta3 = {
        "schema": "datavine.workflow-delta/v1",
        "workflow_id": workflow_id,
        "idempotency_key": "dynamic-increment-v1",
        "tasks": [
            task(
                3,
                [
                    "/bin/sh",
                    "-c",
                    'value=$(cat "$1"); printf "%s" "$((value + 1))"',
                    "datavine",
                    "{{data:2}}",
                ],
                [2],
                3,
            )
        ],
        "data": [output(3, 3)],
        "requested_outputs": [3],
    }

    with tempfile.TemporaryDirectory(prefix="datavine-dynamic-") as root:
        journal = Path(root) / "journal"
        service = subprocess.Popen(
            (str(service_binary), "serve", str(journal), token),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            errors="backslashreplace",
        )
        worker = None
        try:
            contact = json.loads(service.stdout.readline())
            worker = subprocess.Popen(
                (
                    str(worker_binary),
                    "--cores=1",
                    "--memory=512",
                    "--disk=512",
                    "--idle-timeout=20",
                    "localhost",
                    str(contact["manager_port"]),
                ),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            client = WorkflowClient(contact["endpoint"], token)
            client.submit_workflow(initial)
            wait_state(client, workflow_id, {"open_quiescent"}, 1)
            assert client.fetch_workflow_result(workflow_id, 1) == b"7"

            # Crash the native owner at an open/quiescent boundary. A fresh
            # Manager and Worker must recover the result and accept new graph.
            service.kill()
            service.communicate(timeout=20)
            if worker.poll() is None:
                worker.send_signal(signal.SIGTERM)
                worker.communicate(timeout=20)
            service = subprocess.Popen(
                (str(service_binary), "serve", str(journal), token),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                errors="backslashreplace",
            )
            contact = json.loads(service.stdout.readline())
            worker = subprocess.Popen(
                (
                    str(worker_binary), "--cores=1", "--memory=512",
                    "--disk=512", "--idle-timeout=20", "localhost",
                    str(contact["manager_port"]),
                ),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            client = WorkflowClient(contact["endpoint"], token)
            wait_state(client, workflow_id, {"open_quiescent"}, 1)
            assert client.fetch_workflow_result(workflow_id, 1) == b"7"

            info = client.append_workflow(workflow_id, 1, delta2)
            assert info["state"] == "running_open" and info["generation"] == 2
            wait_state(client, workflow_id, {"open_quiescent"}, 2)
            assert client.fetch_workflow_result(workflow_id, 2) == b"49"

            client.append_workflow(workflow_id, 2, delta3)
            wait_state(client, workflow_id, {"open_quiescent"}, 3)
            assert client.fetch_workflow_result(workflow_id, 3) == b"50"
            client.seal_workflow(workflow_id, 3)
            wait_state(client, workflow_id, {"completed"}, 3)
            assert client.fetch_workflow_result(workflow_id, 3) == b"50"
            events = client.watch_workflow(workflow_id)
            event_types = [event["type"] for event in events]
            assert event_types.count("quiescent") >= 3, event_types
            assert event_types.count("resumed") == 2, event_types
            assert "recovered" in event_types, event_types
            submitted = [
                event.get("task_id")
                for event in events
                if event["type"] == "task_submitted"
            ]
            assert submitted.count(1) == 1, submitted
            assert submitted.count(2) == 1, submitted
            assert submitted.count(3) == 1, submitted
        finally:
            if worker is not None and worker.poll() is None:
                worker.send_signal(signal.SIGTERM)
                worker.communicate(timeout=20)
            if service.poll() is None:
                service.send_signal(signal.SIGTERM)
            stdout, stderr = service.communicate(timeout=20)
            if stderr:
                print(stderr, end="", file=sys.stderr)
            if service.returncode != 0:
                raise AssertionError((service.returncode, stdout, stderr))

    print(
        "DataVine dynamic workflow PASS result-driven=7->49->50 "
        "runtime-delta=2 quiescent=3 resumed=2 runtime-crash-recovery=1 "
        "durable-rehydrate=1 producer-replay=0 seal=completed"
    )


if __name__ == "__main__":
    main()
