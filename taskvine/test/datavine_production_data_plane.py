#!/usr/bin/env python3
"""End-to-end contract for the decoupled DataVine content plane."""

import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time

import cloudpickle

from ndcctools.taskvine.datavine import Workflow, WorkflowClient


def payload_size(value):
    return len(value)


def wait_terminal(client, workflow_id, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = client.describe_workflow(workflow_id)
        if info["state"] in {"completed", "failed", "cancelled"}:
            return info
        time.sleep(0.02)
    raise TimeoutError(client.describe_workflow(workflow_id))


def main():
    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    token = "production-data-plane-token-must-not-be-persisted"
    workflow_id = "production-data-plane-dedup"
    with tempfile.TemporaryDirectory(prefix="datavine-production-data-plane-") as root:
        root = Path(root)
        journal = root / "journal"
        service_log_path = root / "service.log"
        worker_log_path = root / "worker.log"
        with service_log_path.open("w") as service_log:
            service = subprocess.Popen(
                (str(service_binary), "serve", str(journal), token),
                stdout=subprocess.PIPE,
                stderr=service_log,
                text=True,
                env=dict(os.environ, DATAVINE_WORKFLOW_METRICS="1"),
            )
            worker = None
            try:
                contact = json.loads(service.stdout.readline())
                worker = subprocess.Popen(
                    (
                        str(worker_binary), "-d", "vine", "-o",
                        str(worker_log_path), "--cores=4", "--memory=1024",
                        "--disk=2048", "--idle-timeout=30", "localhost",
                        str(contact["manager_port"]),
                    ),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
                client = WorkflowClient(contact["endpoint"], token)
                capabilities = client.workflow_capabilities()
                assert capabilities["object_store"] == "sharedfs-single-file-sha256-v1"
                assert capabilities["object_max_bytes"] == 64 * 1024 * 1024 - 64
                assert capabilities["inline_invocation_max_bytes"] == 64 * 1024
                assert capabilities["result_stream"] == (
                    "controller-admitted-sequence-v1"
                )
                assert capabilities["physical_submission_window"] == 4096
                assert capabilities["physical_recovery_reserve"] == 2048
                workflow = Workflow(
                    "production-data-plane-submit", workflow_id=workflow_id,
                    maximum_tasks=64, maximum_edges=64,
                )
                shared = workflow.python_value(b"x" * (4 * 1024 * 1024))
                workflow.python_value(b"x" * (4 * 1024 * 1024))
                outputs = [
                    workflow.python_callable(payload_size, shared)
                    for _ in range(64)
                ]
                workflow.request(*outputs)
                workflow.submit(client)
                document = workflow.document()
                external = [
                    record for record in document["data"]
                    if record["origin"]["kind"] != "output"
                    and record["codec"]["name"] != "python/invocation"
                ]
                inline_control = [
                    record for record in document["data"]
                    if record["codec"]["name"] == "python/invocation"
                ]
                assert external and all(
                    record["origin"]["kind"] == "object" for record in external
                )
                assert all("base64" not in record["origin"] for record in external)
                assert len(inline_control) == 1
                assert inline_control[0]["origin"]["kind"] == "inline"
                info = wait_terminal(client, workflow_id)
                assert info["state"] == "completed", info
                fetched = client.fetch_workflow_results(
                    workflow_id, (output.data_id for output in outputs)
                )
                assert len(fetched) == len(outputs)
                assert all(
                    cloudpickle.loads(payload) == 4 * 1024 * 1024
                    for payload in fetched
                )
                profile = workflow.profile()
                assert profile["object_records"] == 3, profile
                assert profile["object_put_requests"] == 2, profile
                assert profile["object_local_deduplicated"] == 1, profile
                assert profile["object_put_deduplicated"] == 0, profile
                assert profile["object_put_bytes"] > 4 * 1024 * 1024
                assert profile["object_put_parallelism"] == 2, profile
                assert profile["inline_invocation_records"] == 1, profile
                assert profile["object_put_wall_nanoseconds"] > 0, profile
                assert profile["python_value_serialize_nanoseconds"] > 0
                client.close()
            finally:
                if worker is not None and worker.poll() is None:
                    worker.send_signal(signal.SIGTERM)
                    worker.communicate(timeout=20)
                if service.poll() is None:
                    service.send_signal(signal.SIGTERM)
                    service.communicate(timeout=20)

        journal_bytes = journal.read_bytes()
        assert token.encode() not in journal_bytes
        assert journal_bytes.count(b'"base64"') == 1
        worker_log = worker_log_path.read_text()
        direct_pulls = worker_log.count("cache: transferring datavine-file://")
        # The ordinary shared value uses Worker cache transfer. Function and
        # invocation eData bypass scheduler mounts and are pulled by the
        # persistent worker-local Python executor.
        assert direct_pulls == 1, direct_pulls
        service_metrics = [
            line for line in service_log_path.read_text().splitlines()
            if f"datavine workflow {workflow_id} " in line
            and "dominant_stage=" in line
        ]
        assert len(service_metrics) == 1, service_metrics
        assert "stage_in_seconds=" in service_metrics[0]
        assert "worker_execute_seconds=" in service_metrics[0]
        assert "python_object_pull_seconds=" in service_metrics[0]

    print(
        "DataVine production data plane PASS tasks=64 input=4MiB "
        "object-records=3 unique-put=2 local-dedup=1 worker-cache-pulls=1 "
        "executor-object-pull=1 inline-invocation=1 "
        "IR-control-base64=1 token-leak=0 "
        "profile=dominant-stage"
    )


if __name__ == "__main__":
    main()
