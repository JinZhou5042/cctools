#!/usr/bin/env python3

import concurrent.futures
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.parse

from ndcctools.taskvine.datavine.workflow_client import (
    WorkflowClient,
    WorkflowClientError,
    WorkflowEventCursorExpired,
)


def start_service(executable, journal, token):
    process = subprocess.Popen(
        (str(executable), "serve", str(journal), token),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    line = process.stdout.readline()
    if not line:
        raise AssertionError(process.stderr.read())
    endpoint = json.loads(line)["endpoint"]
    return process, endpoint


def stop_service(process):
    process.send_signal(signal.SIGTERM)
    stdout, stderr = process.communicate(timeout=20)
    assert process.returncode == 0, (process.returncode, stdout, stderr)


def assert_corrupt_journal_rejected(executable, source, token, root):
    corrupt = root / "corrupt.journal"
    shutil.copyfile(source, corrupt)
    with corrupt.open("r+b") as stream:
        stream.seek(-1, os.SEEK_END)
        value = stream.read(1)[0]
        stream.seek(-1, os.SEEK_END)
        stream.write(bytes((value ^ 0xFF,)))
    process = subprocess.run(
        (str(executable), "serve", str(corrupt), token),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=20,
    )
    assert process.returncode != 0
    assert not process.stdout


def assert_resource_bounds(process, endpoint):
    parsed = urllib.parse.urlsplit(endpoint)
    address = (parsed.hostname, parsed.port)
    oversized = socket.create_connection(address, timeout=2)
    oversized.sendall(
        struct.pack("!IHHIQ", 0x44564331, 1, 1, 64 * 1024 * 1024 + 1, 1)
    )
    oversized.settimeout(2)
    assert oversized.recv(1) == b""
    oversized.close()

    slow = []
    for _ in range(1100):
        try:
            connection = socket.create_connection(address, timeout=0.2)
        except OSError:
            continue
        slow.append(connection)
    time.sleep(0.5)
    service_fds = len(list(Path(f"/proc/{process.pid}/fd").iterdir()))
    assert service_fds <= 1060, service_fds
    for connection in slow:
        connection.close()
    time.sleep(0.2)


def main():
    repository = Path(__file__).resolve().parents[2]
    executable = repository / "taskvine/src/tools/datavine_workflow"
    token = "workflow-service-test-token"

    expired_body = bytearray(80)
    struct.pack_into("!I", expired_body, 0, 1)
    struct.pack_into("!IQQ", expired_body, 4, 1, 100, 1)
    expired_body[24:64] = b"0" * 40
    cursor_client = object.__new__(WorkflowClient)
    cursor_client.request = lambda opcode, payload=b"": bytes(expired_body)
    try:
        cursor_client.watch_workflow("cursor-test", after_event_id=0)
    except WorkflowEventCursorExpired as error:
        assert error.oldest == 100
    else:
        raise AssertionError("expired event cursor was silently truncated")
    initial = {
        "schema": "datavine.workflow/v1",
        "workflow_id": "detached-workflow",
        "idempotency_key": "detached-submit-1",
        "mode": "streaming",
        "tasks": [],
        "data": [],
        "requested_outputs": [],
        "policy": {"maximum_tasks": 10, "maximum_edges": 10},
    }
    extended = {
        "schema": "datavine.workflow-delta/v1",
        "workflow_id": initial["workflow_id"],
        "idempotency_key": "detached-append-1",
        "data": [
            {
                "data_id": 1,
                "codec": {"name": "bytes", "version": "1"},
                "origin": {
                    "kind": "output",
                    "task_id": 1,
                    "output_index": 0,
                },
            }
        ],
        "tasks": [
            {
                "task_id": 1,
                "executor": {
                    "kind": "command",
                    "version": "1",
                    "argv": ["/bin/true"],
                },
                "inputs": [],
                "output_data_ids": [1],
            }
        ],
        "requested_outputs": [1],
    }
    with tempfile.TemporaryDirectory(prefix="datavine-workflow-service-") as root:
        root = Path(root)
        journal = root / "native.journal"
        document = root / "workflow.json"
        document.write_text(json.dumps(initial))
        service, endpoint = start_service(executable, journal, token)
        try:
            submit_code = "\n".join(
                (
                    "import json,sys",
                    "from ndcctools.taskvine.datavine.workflow_client import WorkflowClient",
                    "client=WorkflowClient(sys.argv[1],sys.argv[2])",
                    "print(json.dumps(client.submit_workflow(json.load(open(sys.argv[3])))))",
                )
            )
            submitted = subprocess.run(
                (sys.executable, "-c", submit_code, endpoint, token, str(document)),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
                env=dict(os.environ),
            )
            assert json.loads(submitted.stdout)["generation"] == 1
            client = WorkflowClient(endpoint, token)
            capabilities = client.workflow_capabilities()
            assert capabilities["schema_versions"] == [
                "datavine.workflow/v1",
                "datavine.workflow-delta/v1",
            ]
            assert "taskvine" in capabilities["executor_kinds"]
            assert capabilities["object_store"] == "sharedfs-single-file-sha256-v1"

            object_payload = b"shared immutable python argument"

            def put_shared_object(_):
                contender = WorkflowClient(endpoint, token)
                return contender.put_object(object_payload)

            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                object_results = list(pool.map(put_shared_object, range(64)))
            object_digest = object_results[0]["sha256"]
            assert all(result["sha256"] == object_digest for result in object_results)
            assert sum(not result["deduplicated"] for result in object_results) == 1
            assert client.get_object(object_digest) == object_payload
            try:
                client.request(30, b"0" * 64 + object_payload)
            except WorkflowClientError:
                pass
            else:
                raise AssertionError("object store accepted a false content identity")
            info = client.describe_workflow("detached-workflow")
            assert info["state"] in {"open", "running_open", "open_quiescent"}
            assert info["generation"] == 1
            info = client.append_workflow("detached-workflow", 1, extended)
            assert info["generation"] == 2 and info["tasks"] == 1
            events = client.watch_workflow("detached-workflow")
            event_types = [event["type"] for event in events]
            assert event_types[0] == "accepted" and "appended" in event_types
            assert_resource_bounds(service, endpoint)
            assert client.describe_workflow("detached-workflow")["generation"] == 2

            race_initial = {
                **initial,
                "workflow_id": "append-cas-race",
                "idempotency_key": "append-cas-race-v1",
            }
            client.submit_workflow(race_initial)
            race_documents = []
            for ordinal, command in enumerate(("/bin/true", "/bin/false"), 1):
                race_documents.append(
                    {
                        "schema": "datavine.workflow-delta/v1",
                        "workflow_id": race_initial["workflow_id"],
                        "idempotency_key": f"append-cas-race-{ordinal}",
                        "tasks": [
                            {
                                "task_id": 1,
                                "executor": {
                                    "kind": "command",
                                    "version": "1",
                                    "argv": [command],
                                },
                                "inputs": [],
                                "output_data_ids": [1],
                            }
                        ],
                        "data": [
                            {
                                "data_id": 1,
                                "codec": {"name": "bytes", "version": "1"},
                                "origin": {
                                    "kind": "output",
                                    "task_id": 1,
                                    "output_index": 0,
                                },
                            }
                        ],
                        "requested_outputs": [1],
                    }
                )

            def race_append(document):
                contender = WorkflowClient(endpoint, token)
                try:
                    contender.append_workflow("append-cas-race", 1, document)
                    return "accepted"
                except WorkflowClientError:
                    return "rejected"

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(race_append, race_documents))
            assert sorted(outcomes) == ["accepted", "rejected"], outcomes
            race_info = client.describe_workflow("append-cas-race")
            assert race_info["generation"] == 2 and race_info["tasks"] == 1
            client.cancel_workflow("append-cas-race")
        finally:
            stop_service(service)

        assert_corrupt_journal_rejected(executable, journal, token, root)

        with journal.open("ab") as stream:
            stream.write(b"truncated-tail")

        service, endpoint = start_service(executable, journal, token)
        try:
            client = WorkflowClient(endpoint, token)
            assert client.get_object(object_digest) == object_payload
            info = client.describe_workflow("detached-workflow")
            assert info["generation"] == 2 and info["tasks"] == 1
            info = client.seal_workflow("detached-workflow", 2)
            assert info["state"] in {"sealed", "running"}
            assert any(
                event["type"] == "sealed"
                for event in client.watch_workflow("detached-workflow")
            )
        finally:
            stop_service(service)

    print(
        "DataVine native workflow service PASS "
        "submitter-exit=1 restart=1 generation-cas-race=single-winner "
        "truncated-tail=recovered corrupt-checksum=reject events=3 "
        "expired-cursor=fail-closed oversized-frame=reject connections<=1024"
        " object-store=atomic-deduplicated-persistent"
    )


if __name__ == "__main__":
    main()
