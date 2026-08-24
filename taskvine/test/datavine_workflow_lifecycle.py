#!/usr/bin/env python3

import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time

from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


def start_service(binary, journal, token):
    process = subprocess.Popen(
        (str(binary), "serve", str(journal), token),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    line = process.stdout.readline()
    if not line:
        raise AssertionError(process.stderr.read())
    return process, json.loads(line)


def start_worker(binary, port):
    return subprocess.Popen(
        (
            str(binary),
            "--cores=1",
            "--memory=256",
            "--disk=256",
            "--idle-timeout=15",
            "localhost",
            str(port),
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )


def stop(process):
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGTERM)
    if process is not None:
        process.communicate(timeout=20)


def process_tree(root_pid):
    descendants = {int(root_pid)}
    changed = True
    while changed:
        changed = False
        for stat_path in Path("/proc").glob("[0-9]*/stat"):
            try:
                fields = stat_path.read_text().split(") ", 1)[1].split()
                pid = int(stat_path.parent.name)
                parent = int(fields[1])
            except (FileNotFoundError, IndexError, ValueError):
                continue
            if parent in descendants and pid not in descendants:
                descendants.add(pid)
                changed = True
    return descendants


def kill_process_tree(root_pid):
    descendants = process_tree(root_pid)
    for pid in sorted(descendants, reverse=True):
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def wait_state(client, workflow_id, states, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = client.describe_workflow(workflow_id)
        if info["state"] in states:
            return info
        time.sleep(0.05)
    raise AssertionError(client.describe_workflow(workflow_id))


def command_workflow(workflow_id, argv, attempts=1):
    return {
        "schema": "datavine.workflow/v1",
        "workflow_id": workflow_id,
        "idempotency_key": f"{workflow_id}-v1",
        "mode": "sealed",
        "tasks": [
            {
                "task_id": 1,
                "executor": {"kind": "command", "version": "1", "argv": argv},
                "inputs": [],
                "output_data_ids": [1],
                "retry": {"maximum_attempts": attempts},
            }
        ],
        "data": [
            {
                "data_id": 1,
                "codec": {"name": "bytes", "version": "1"},
                "origin": {"kind": "output", "task_id": 1, "output_index": 0},
            }
        ],
        "requested_outputs": [1],
        "policy": {"maximum_tasks": 1, "maximum_edges": 0},
    }


def event_types(client, workflow_id):
    return [item["type"] for item in client.watch_workflow(workflow_id)]


def restart_chain(workflow_id):
    return {
        "schema": "datavine.workflow/v1",
        "workflow_id": workflow_id,
        "idempotency_key": f"{workflow_id}-v1",
        "mode": "sealed",
        "tasks": [
            {
                "task_id": 1,
                "executor": {
                    "kind": "command",
                    "version": "1",
                    "argv": ["/usr/bin/printf", "checkpoint"],
                },
                "inputs": [],
                "output_data_ids": [1],
            },
            {
                "task_id": 2,
                "executor": {
                    "kind": "command",
                    "version": "1",
                    "argv": [
                        "/bin/sh",
                        "-c",
                        "sleep 3; cat \"$1\"",
                        "datavine-restart",
                        "{{data:1}}",
                    ],
                },
                "inputs": [{"position": 0, "data_id": 1}],
                "output_data_ids": [2],
            },
        ],
        "data": [
            {
                "data_id": 1,
                "codec": {"name": "bytes", "version": "1"},
                "origin": {"kind": "output", "task_id": 1, "output_index": 0},
            },
            {
                "data_id": 2,
                "codec": {"name": "bytes", "version": "1"},
                "origin": {"kind": "output", "task_id": 2, "output_index": 0},
            },
        ],
        "requested_outputs": [2],
        "policy": {"maximum_tasks": 2, "maximum_edges": 1},
    }


def main():
    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    token = "workflow-lifecycle-token"
    with tempfile.TemporaryDirectory(prefix="datavine-workflow-lifecycle-") as root:
        journal = Path(root) / "native.journal"
        service, contact = start_service(service_binary, journal, token)
        worker = start_worker(worker_binary, contact["manager_port"])
        client = WorkflowClient(contact["endpoint"], token)
        try:
            retry_id = "native-retry-failure"
            client.submit_workflow(
                command_workflow(retry_id, ["/bin/false"], attempts=2)
            )
            assert wait_state(client, retry_id, {"failed"})["state"] == "failed"
            assert event_types(client, retry_id) == [
                "accepted",
                "started",
                "task_submitted",
                "task_retry",
                "task_submitted",
                "task_failed",
                "failed",
            ]

            worker_loss_id = "native-worker-loss"
            loss_marker = Path(root) / "worker-loss-first-attempt"
            client.submit_workflow(
                command_workflow(
                    worker_loss_id,
                    [
                        "/bin/sh",
                        "-c",
                        f"if [ ! -e '{loss_marker}' ]; then "
                        f": > '{loss_marker}'; sleep 60; "
                        "else printf recovered-after-worker-loss; fi",
                    ],
                    attempts=2,
                )
            )
            deadline = time.monotonic() + 10
            while "task_submitted" not in event_types(client, worker_loss_id):
                assert time.monotonic() < deadline
                time.sleep(0.05)
            deadline = time.monotonic() + 15
            while not loss_marker.exists():
                assert time.monotonic() < deadline, "loss task never reached Worker"
                time.sleep(0.05)
            kill_process_tree(worker.pid)
            worker.wait(timeout=20)
            worker.stderr.close()
            worker = start_worker(worker_binary, contact["manager_port"])
            assert wait_state(client, worker_loss_id, {"completed", "failed"})[
                "state"
            ] == "completed"
            loss_events = event_types(client, worker_loss_id)
            # The Manager reports FORSAKEN once; the DataVine scheduler owns
            # the single logical retry and records it explicitly.
            assert loss_events == [
                "accepted",
                "started",
                "task_submitted",
                "task_retry",
                "task_submitted",
                "task_completed",
                "completed",
            ], loss_events
            assert client.fetch_workflow_result(worker_loss_id, 1) == (
                b"recovered-after-worker-loss"
            )

            cancel_id = "native-running-cancel"
            client.submit_workflow(
                command_workflow(cancel_id, ["/bin/sleep", "30"])
            )
            deadline = time.monotonic() + 10
            while "task_submitted" not in event_types(client, cancel_id):
                assert time.monotonic() < deadline
                time.sleep(0.05)
            assert client.cancel_workflow(cancel_id)["state"] == "cancelled"
            assert wait_state(client, cancel_id, {"cancelled"})["state"] == (
                "cancelled"
            )

            restart_id = "native-owner-restart"
            client.submit_workflow(restart_chain(restart_id))
            deadline = time.monotonic() + 10
            while not any(
                event["type"] == "task_submitted" and event["task_id"] == 2
                for event in client.watch_workflow(restart_id)
            ):
                assert time.monotonic() < deadline
                time.sleep(0.05)
        finally:
            stop(service)
            stop(worker)

        service, contact = start_service(service_binary, journal, token)
        worker = start_worker(worker_binary, contact["manager_port"])
        client = WorkflowClient(contact["endpoint"], token)
        try:
            assert wait_state(client, restart_id, {"completed", "failed"})[
                "state"
            ] == "completed"
            restart_events = event_types(client, restart_id)
            assert restart_events == [
                "accepted",
                "started",
                "task_submitted",
                "task_completed",
                "task_submitted",
                "recovered",
                "started",
                "task_retry",
                "task_submitted",
                "task_completed",
                "task_submitted",
                "task_completed",
                "completed",
            ], restart_events
            events = client.watch_workflow(restart_id)
            submitted_ids = [
                event["task_id"]
                for event in events
                if event["type"] == "task_submitted"
            ]
            # Data 1 was intentionally non-durable.  Killing both owner and
            # worker removes its final replica, so recovery replays producer 1
            # and then its in-flight descendant 2.  Requested data 2 remains
            # durable after the replay.
            assert submitted_ids == [1, 2, 1, 2], submitted_ids
            assert client.fetch_workflow_result(restart_id, 2) == b"checkpoint"
        finally:
            stop(worker)
            stop(service)

    print(
        "DataVine native lifecycle PASS retries=2 cancel-running=1 "
        "worker-loss=scheduler-retry owner-restart=1 checkpoint-resume=1 "
        "resumable-events=1"
    )


if __name__ == "__main__":
    main()
