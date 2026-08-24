#!/usr/bin/env python3
"""Direct Worker Data Agent / Controller metadata protocol contract."""

import hashlib
import hmac
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import time
import urllib.parse

from ndcctools.taskvine.datavine import Workflow, WorkflowClient


MAGIC = 0x44564331
VERSION = 1
AUTH = 1
HELLO = 40
DATA_READY = 41
RESOLVE = 42
DATA_FAULT = 43
HEADER = struct.Struct("!IHHIQ")
RESPONSE = struct.Struct("!IHHIIQ")
BATCH = struct.Struct("!QIIQQII")
PUBLISH = struct.Struct("!QIIQQ32s")
RESOLVE_ITEM = struct.Struct("!QII")
RESOLVE_REPLY = struct.Struct("!IIQQ32sIIQQHH64sI")
FAULT = struct.Struct("!QIIQ")


def read_exact(stream, size):
    value = bytearray()
    while len(value) < size:
        block = stream.recv(size - len(value))
        if not block:
            raise ConnectionError("unexpected EOF")
        value.extend(block)
    return bytes(value)


def request(stream, opcode, request_id, payload=b""):
    stream.sendall(HEADER.pack(MAGIC, VERSION, opcode, len(payload), request_id))
    stream.sendall(payload)
    response = RESPONSE.unpack(read_exact(stream, RESPONSE.size))
    magic, version, returned_opcode, status, size, returned_id = response
    assert (magic, version, returned_opcode, returned_id) == (
        MAGIC, VERSION, opcode, request_id
    )
    return status, read_exact(stream, size)


def batch_header(slot, worker, epoch, sequence, count):
    return BATCH.pack(slot, worker, 0, epoch, sequence, count, 0)


def main():
    repository = Path(__file__).resolve().parents[2]
    executable = repository / "taskvine/src/tools/datavine_workflow"
    token = "direct-agent-protocol-token"
    workflow_id = "direct-agent-protocol"
    with tempfile.TemporaryDirectory(prefix="datavine-agent-protocol-") as root:
        journal = Path(root) / "workflow.journal"
        service = subprocess.Popen(
            (str(executable), "serve", str(journal), token),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=dict(os.environ),
        )
        try:
            contact = json.loads(service.stdout.readline())
            client = WorkflowClient(contact["endpoint"], token)
            workflow = Workflow(
                "direct-agent-submit", workflow_id=workflow_id,
                maximum_tasks=4, maximum_edges=4,
            )
            output = workflow.command(("/bin/true",))
            workflow.request(output)
            workflow.submit(client)

            parsed = urllib.parse.urlsplit(contact["endpoint"])
            stream = socket.create_connection((parsed.hostname, parsed.port), timeout=5)
            try:
                assert request(stream, AUTH, 1, token.encode())[0] == 0
                key = hmac.digest(token.encode(), workflow_id.encode(), "sha256")
                host = b"127.0.0.1"
                hello = (
                    b"DVA1" + key + struct.pack("!IQHH", 7, 123456, 23456, len(host))
                    + host.ljust(64, b"\0") + struct.pack("!I", 0)
                )
                deadline = time.monotonic() + 5
                while True:
                    status, body = request(stream, HELLO, 2, hello)
                    if status == 0:
                        break
                    if time.monotonic() >= deadline:
                        raise AssertionError((status, body, service.stderr.read()))
                    stream.close()
                    time.sleep(0.01)
                    stream = socket.create_connection(
                        (parsed.hostname, parsed.port), timeout=5
                    )
                    assert request(stream, AUTH, 1, token.encode())[0] == 0
                assert len(body) == 16
                slot, assigned_worker, reserved = struct.unpack("!QII", body)
                assert slot
                assert assigned_worker == 7 and reserved == 0

                digest = hashlib.sha256(b"agent-output").digest()
                ready = batch_header(slot, 7, 123456, 1, 1) + PUBLISH.pack(
                    output.data_id, 1, 1, len(b"agent-output"), 9001, digest
                )
                assert request(stream, DATA_READY, 3, ready)[0] == 0
                # An exact retransmission is idempotent.
                assert request(stream, DATA_READY, 4, ready)[0] == 0

                resolve = batch_header(slot, 7, 123456, 0, 1) + RESOLVE_ITEM.pack(
                    output.data_id, 1, 0
                )
                status, body = request(stream, RESOLVE, 5, resolve)
                assert status == 0 and len(body) == RESOLVE_REPLY.size
                fields = RESOLVE_REPLY.unpack(body)
                assert fields[0] == 2  # AVAILABLE
                assert fields[1:4] == (1, output.data_id, len(b"agent-output"))
                assert fields[4] == digest
                assert fields[5:9] == (7, 0, 123456, 9001)
                assert fields[9:11] == (23456, len(host))
                assert fields[11][: len(host)] == host and fields[12] == 0

                conflicting = batch_header(slot, 7, 123456, 2, 1) + PUBLISH.pack(
                    output.data_id, 1, 1, len(b"agent-output"), 9002,
                    hashlib.sha256(b"different").digest(),
                )
                assert request(stream, DATA_READY, 6, conflicting)[0] == 3

                fault = batch_header(slot, 7, 123456, 2, 1) + FAULT.pack(
                    output.data_id, 1, 0, 9001
                )
                assert request(stream, DATA_FAULT, 7, fault)[0] == 0
                status, body = request(stream, RESOLVE, 8, resolve)
                assert status == 0
                assert RESOLVE_REPLY.unpack(body)[0] == 1  # PENDING
            finally:
                stream.close()
                client.close()
        finally:
            if service.poll() is None:
                service.send_signal(signal.SIGTERM)
            stdout, stderr = service.communicate(timeout=20)
            assert service.returncode == 0, (service.returncode, stdout, stderr)
    print("DataVine direct agent protocol PASS")


if __name__ == "__main__":
    main()
