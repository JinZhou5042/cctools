#!/usr/bin/env python3
"""Fail-closed and restart recovery gates for requested-output fsync errors."""

import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time

from ndcctools.taskvine.datavine.workflow_client import (
    WorkflowClient,
    WorkflowClientError,
)


def start_service(binary, journal, token, environment, log_path):
    log = log_path.open("w")
    process = subprocess.Popen(
        (str(binary), "serve", str(journal), token),
        stdout=subprocess.PIPE,
        stderr=log,
        text=True,
        env=environment,
    )
    line = process.stdout.readline()
    if not line:
        process.wait(timeout=10)
        log.close()
        raise AssertionError(log_path.read_text())
    return process, log, json.loads(line)


def start_worker(binary, port, log_path):
    return subprocess.Popen(
        (
            str(binary), "-d", "vine", "-o", str(log_path),
            "--cores=1", "--memory=512", "--disk=1024",
            "--idle-timeout=30", "localhost", str(port),
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        start_new_session=True,
    )


def stop(process, log=None):
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGTERM)
    if process is not None:
        try:
            process.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=10)
    if log is not None:
        log.close()


def wait_for(predicate, message, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError(message)


def workflow_document(workflow_id, payload):
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
                    "argv": ["/usr/bin/printf", payload],
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
        "requested_outputs": [1],
        "policy": {"maximum_tasks": 1, "maximum_edges": 0},
    }


def result_absent(client, workflow_id):
    try:
        client.workflow_result_descriptors(workflow_id, (1,))
    except WorkflowClientError:
        return True
    return False


def run_case(root, service_binary, worker_binary, preload, fault):
    case_root = root / fault.lower()
    case_root.mkdir()
    journal = case_root / "workflow.journal"
    data_root = Path(f"{journal}.data")
    gate = case_root / "fault.enabled"
    fault_log = case_root / "fault.log"
    gate.touch()
    token = f"persistence-fault-{fault.lower()}-token"
    workflow_id = f"persistence-fault-{fault.lower()}"
    payload = f"durable-after-{fault.lower()}"
    environment = dict(
        os.environ,
        LD_PRELOAD=str(preload),
        DATAVINE_TEST_FSYNC_FAULT=fault,
        DATAVINE_TEST_FSYNC_FAULT_GATE=str(gate),
        DATAVINE_TEST_FSYNC_FAULT_LOG=str(fault_log),
        DATAVINE_TEST_FSYNC_TARGET_ROOT=str(data_root),
        DATAVINE_WORKFLOW_METRICS="1",
        DATAVINE_PERSISTENCE_DIAGNOSTICS="1",
    )

    service = service_log = worker = client = None
    first_log = case_root / "service-fault.log"
    try:
        service, service_log, contact = start_service(
            service_binary, journal, token, environment, first_log
        )
        worker = start_worker(
            worker_binary, contact["manager_port"], case_root / "worker-fault.log"
        )
        client = WorkflowClient(contact["endpoint"], token)
        client.submit_workflow(workflow_document(workflow_id, payload))
        wait_for(
            lambda: fault_log.exists() and len(fault_log.read_text().splitlines()) >= 8,
            f"{fault} was not injected into all persistence attempts",
        )
        assert result_absent(client, workflow_id)
        assert client.describe_workflow(workflow_id)["state"] not in {
            "completed", "failed", "cancelled",
        }
        assert not list(data_root.rglob("*.data"))
    finally:
        if client is not None:
            client.close()
        stop(worker)
        stop(service, service_log)

    injected = fault_log.read_text().splitlines()
    assert len(injected) >= 8, injected
    assert all(line.startswith(f"{fault} ") and ".part." in line for line in injected)
    assert not list(data_root.rglob("*.part.*"))
    assert not list(data_root.rglob("*.data"))

    # A restart before any Worker joins proves the failed fsync did not leave a
    # phantom journal/catalog result.  Removing the gate then permits recovery.
    gate.unlink()
    recovery_log_path = case_root / "service-recovery.log"
    service = service_log = worker = client = None
    try:
        service, service_log, contact = start_service(
            service_binary, journal, token, environment, recovery_log_path
        )
        client = WorkflowClient(contact["endpoint"], token)
        assert result_absent(client, workflow_id)
        worker = start_worker(
            worker_binary, contact["manager_port"], case_root / "worker-recovery.log"
        )
        info = client.wait_workflow(workflow_id, timeout=60)
        assert info["state"] == "completed", info
        assert client.fetch_workflow_result(workflow_id, 1) == payload.encode()
        descriptors = client.workflow_result_descriptors(workflow_id, (1,))
        assert len(descriptors) == 1 and descriptors[0]["data_id"] == 1
    finally:
        if client is not None:
            client.close()
        stop(worker)
        stop(service, service_log)

    recovery_log = recovery_log_path.read_text()
    assert f"datavine controller_persistence {workflow_id} " in recovery_log
    assert "agent_persistence_commit_groups=1" in recovery_log
    final_files = list(data_root.rglob("*.data"))
    assert len(final_files) == 1, final_files
    assert not list(data_root.rglob("*.part.*"))

    # A second replay must expose the same single committed result without a
    # Worker, proving the successful recovery was durably journaled once.
    replay_log_path = case_root / "service-replay.log"
    service = service_log = client = None
    try:
        service, service_log, contact = start_service(
            service_binary, journal, token, environment, replay_log_path
        )
        client = WorkflowClient(contact["endpoint"], token)
        assert client.fetch_workflow_result(workflow_id, 1) == payload.encode()
        assert len(client.workflow_result_descriptors(workflow_id, (1,))) == 1
    finally:
        if client is not None:
            client.close()
        stop(service, service_log)

    assert len(list(data_root.rglob("*.data"))) == 1
    assert not list(data_root.rglob("*.part.*"))
    return len(injected)


def main():
    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    preload = Path(os.environ["DATAVINE_TEST_FSYNC_PRELOAD"])
    assert preload.is_file()
    with tempfile.TemporaryDirectory(prefix="datavine-persistence-fault-test-") as tmp:
        root = Path(tmp)
        counts = {
            fault: run_case(
                root, service_binary, worker_binary, preload, fault
            )
            for fault in ("ENOSPC", "EIO")
        }
    print(
        "DataVine persistence faults PASS "
        f"ENOSPC-injections={counts['ENOSPC']} "
        f"EIO-injections={counts['EIO']} "
        "phantom-result=0 temp-leak=0 recovery=2 commit-groups=2 replay=2"
    )


if __name__ == "__main__":
    main()
