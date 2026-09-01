#!/usr/bin/env python3
"""Controller-background iData survives loss of its only Worker replica."""

import hashlib
import json
from pathlib import Path
import signal
import subprocess
import tempfile
import time

from ndcctools.taskvine.datavine import Workflow
from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


def wait_for_task(client, workflow_id, task_id, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(
            event["type"] == "task_completed" and event.get("task_id") == task_id
            for event in client.watch_workflow(workflow_id)
        ):
            return
        time.sleep(0.05)
    raise AssertionError(f"task {task_id} did not complete")


def wait_terminal(client, workflow_id, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = client.describe_workflow(workflow_id)
        if info["state"] in {"completed", "failed", "cancelled"}:
            return info
        time.sleep(0.05)
    raise AssertionError(
        (client.describe_workflow(workflow_id), client.watch_workflow(workflow_id))
    )


def start_worker(binary, manager_port):
    return subprocess.Popen(
        (
            str(binary),
            "--cores=1",
            "--memory=512",
            "--disk=512",
            "--idle-timeout=30",
            "localhost",
            str(manager_port),
        ),
        stdout=subprocess.DEVNULL,
        stderr=None,
        text=True,
    )


def stop(process):
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGTERM)
    if process is not None:
        process.communicate(timeout=20)


def main():
    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    token = "background-backup-test-token"
    workflow_id = "background-backup-worker-loss"
    payload = b"\0" * (1024 * 1024 + 17)

    # Resilience is the production default; worker-local remains an explicit
    # opt-out for controlled baseline comparisons.
    assert (
        Workflow("default").document()["policy"]["idata_backup"]
        == "controller-background"
    )
    assert (
        Workflow("backup", idata_backup="controller-background")
        .document()["policy"]["idata_backup"]
        == "controller-background"
    )
    assert (
        Workflow("volatile", idata_backup="worker-local")
        .document()["policy"]["idata_backup"]
        == "worker-local"
    )
    try:
        Workflow("invalid", idata_backup="best-effort")
    except ValueError:
        pass
    else:
        raise AssertionError("invalid iData backup policy was accepted")

    with tempfile.TemporaryDirectory(prefix="datavine-background-backup-") as root:
        root = Path(root)
        journal = root / "journal"
        marker = root / "producer-runs"
        service = subprocess.Popen(
            (str(service_binary), "serve", str(journal), token),
            stdout=subprocess.PIPE,
            stderr=None,
            text=True,
        )
        worker = None
        client = None
        try:
            contact = json.loads(service.stdout.readline())
            worker = start_worker(worker_binary, contact["manager_port"])
            client = WorkflowClient(contact["endpoint"], token)
            assert client.workflow_capabilities()["idata_backup_modes"] == [
                "worker-local",
                "controller-background",
            ]
            assert (
                client.workflow_capabilities()["idata_backup_default"]
                == "controller-background"
            )
            initial = {
                "schema": "datavine.workflow/v1",
                "workflow_id": workflow_id,
                "idempotency_key": "background-backup-initial-v1",
                "mode": "streaming",
                "tasks": [
                    {
                        "task_id": 1,
                        "executor": {
                            "kind": "command",
                            "version": "1",
                            "argv": [
                                "/bin/sh",
                                "-c",
                                f"printf 'run\\n' >> {marker}; "
                                f"head -c {len(payload)} /dev/zero",
                            ],
                        },
                        "inputs": [],
                        "output_data_ids": [1],
                    }
                ],
                "data": [
                    {
                        "data_id": 1,
                        "codec": {"name": "bytes", "version": "1"},
                        "origin": {"kind": "output", "task_id": 1, "output_index": 0},
                    }
                ],
                "requested_outputs": [],
                "policy": {
                    "maximum_tasks": 2,
                    "maximum_edges": 1,
                },
            }
            client.submit_workflow(initial)
            wait_for_task(client, workflow_id, 1)

            deadline = time.monotonic() + 30
            backup = None
            while time.monotonic() < deadline:
                matches = list(Path(f"{journal}.data").glob("*/1.1.data"))
                if matches and matches[0].read_bytes() == payload:
                    backup = matches[0]
                    break
                time.sleep(0.05)
            assert backup is not None, "background backup was not admitted"

            # Remove the only Worker replica, then use a fresh Worker. The
            # producer must remain logically complete and must not be replayed.
            stop(worker)
            worker = None
            time.sleep(0.25)
            worker = start_worker(worker_binary, contact["manager_port"])

            generation = client.describe_workflow(workflow_id)["generation"]
            delta = {
                "schema": "datavine.workflow-delta/v1",
                "workflow_id": workflow_id,
                "idempotency_key": "background-backup-consumer-v1",
                "tasks": [
                    {
                        "task_id": 2,
                        "executor": {
                            "kind": "command",
                            "version": "1",
                            "argv": ["/bin/cat", "{{data:1}}"],
                        },
                        "inputs": [{"position": 0, "data_id": 1}],
                        "output_data_ids": [2],
                    }
                ],
                "data": [
                    {
                        "data_id": 2,
                        "codec": {"name": "bytes", "version": "1"},
                        "origin": {"kind": "output", "task_id": 2, "output_index": 0},
                    }
                ],
                "requested_outputs": [1, 2],
            }
            appended = client.append_workflow(workflow_id, generation, delta)
            assert client.fetch_workflow_result(workflow_id, 1) == payload
            client.seal_workflow(workflow_id, appended["generation"])
            assert wait_terminal(client, workflow_id)["state"] == "completed"
            result = client.fetch_workflow_result(workflow_id, 2)
            assert result == payload
            assert marker.read_text().splitlines() == ["run"]
            assert hashlib.sha256(result).hexdigest() == hashlib.sha256(payload).hexdigest()
        finally:
            if client is not None:
                client.close()
            stop(worker)
            if service.poll() is None:
                service.send_signal(signal.SIGTERM)
            service.communicate(timeout=20)

    print(
        "DataVine background backup PASS default=controller-background "
        "worker-local=explicit-opt-out "
        "backup=async worker-loss=controller-fetch producer-replay=0"
    )


if __name__ == "__main__":
    main()
