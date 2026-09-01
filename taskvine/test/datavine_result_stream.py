#!/usr/bin/env python3
"""End-to-end admission stream, legacy fallback, and replay acceptance."""

import json
from pathlib import Path
import subprocess
import tempfile

import cloudpickle

from ndcctools.taskvine.datavine.workflow import Workflow
from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


def identity(value):
    return value


def start_service(binary, journal, token):
    process = subprocess.Popen(
        (str(binary), "serve", str(journal), token),
        stdout=subprocess.PIPE,
        text=True,
    )
    contact = json.loads(process.stdout.readline())
    return process, contact


def stop(process):
    if process is not None and process.poll() is None:
        process.terminate()
        process.wait(timeout=20)


def main():
    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    token = "datavine-result-stream-test"
    with tempfile.TemporaryDirectory(prefix="datavine-result-stream-") as root:
        journal = Path(root) / "workflow.journal"
        service, contact = start_service(service_binary, journal, token)
        worker = None
        try:
            client = WorkflowClient(contact["endpoint"], token)
            workflow = Workflow(
                "result-stream-v1",
                workflow_id="result-stream",
                maximum_tasks=3,
                maximum_edges=0,
            )
            values = (b"alpha", b"", b"x" * 70_000)
            outputs = tuple(
                workflow.python_callable(identity, value) for value in values
            )
            workflow.request(*outputs).submit(client)
            profile = workflow.profile()
            assert profile["inline_invocation_records"] == 2, profile
            assert profile["object_put_requests"] == 2, profile
            worker = subprocess.Popen(
                (
                    str(worker_binary), "--cores=3", "--memory=512",
                    "--disk=512", "--idle-timeout=120", "localhost",
                    str(contact["manager_port"]),
                ),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            terminal = client.wait_workflow("result-stream", timeout=30)
            assert terminal["state"] == "completed", terminal

            stream = client.workflow_result_stream("result-stream")
            first = next(stream)
            stream.close()
            cursor = first["sequence"]
            stop(worker)
            worker = None
            stop(service)

            service, contact = start_service(service_binary, journal, token)
            client = WorkflowClient(contact["endpoint"], token)
            resumed = client.workflow_result_stream(
                "result-stream", after_sequence=cursor
            )
            records = [first, *list(resumed)]
            assert resumed.terminal_state == "completed", resumed.terminal_state
            assert [item["sequence"] for item in records] == [1, 2, 3]
            decoded = []
            descriptor_records = 0
            for item in records:
                payload = item["payload"]
                if payload is None:
                    descriptor_records += 1
                    payload = client.fetch_workflow_result(
                        "result-stream", item["data_id"]
                    )
                decoded.append(cloudpickle.loads(payload))
            assert sorted(decoded, key=len) == sorted(values, key=len), decoded
            assert descriptor_records == 1, descriptor_records
        finally:
            stop(worker)
            stop(service)
    print(
        "DataVine result stream PASS inline=2 legacy-fallback=1 "
        "descriptor=1 restart-resume=2 terminal=completed"
    )


if __name__ == "__main__":
    main()
