#!/usr/bin/env python3
"""Measure one-record Data Controller RPCs without executing physical Tasks."""

import argparse
import concurrent.futures
import hashlib
import hmac
import json
import multiprocessing
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
HEADER = struct.Struct("!IHHIQ")
RESPONSE = struct.Struct("!IHHIIQ")
BATCH = struct.Struct("!QIIQQII")
PUBLISH = struct.Struct("!QIIQQ32s")
RESOLVE_ITEM = struct.Struct("!QII")
RESOLVE_REPLY = struct.Struct("!IIQQ32sIIQQHH64sI")


def read_exact(stream, size):
    value = bytearray()
    while len(value) < size:
        block = stream.recv(size - len(value))
        if not block:
            raise ConnectionError("unexpected EOF")
        value.extend(block)
    return bytes(value)


def request(stream, opcode, request_id, payload=b""):
    header = HEADER.pack(MAGIC, VERSION, opcode, len(payload), request_id)
    stream.sendall(header + payload)
    response = RESPONSE.unpack(read_exact(stream, RESPONSE.size))
    magic, version, returned_opcode, status, size, returned_id = response
    if (magic, version, returned_opcode, returned_id) != (
        MAGIC, VERSION, opcode, request_id
    ):
        raise RuntimeError(response)
    return status, read_exact(stream, size)


def batch_header(slot, worker, epoch, sequence):
    return BATCH.pack(slot, worker, 0, epoch, sequence, 1, 0)


def process_cpu_seconds(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().split()
    return (int(fields[13]) + int(fields[14])) / os.sysconf("SC_CLK_TCK")


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(fraction * len(ordered))))
    return ordered[index]


def phase_result(operation, completed, args, seconds, cpu_seconds, latencies):
    expected = args.records * args.iterations
    if completed != expected:
        raise RuntimeError((operation, completed, expected))
    result = {
        "records": completed,
        "seconds": seconds,
        "records_per_second": completed / seconds,
        "service_cpu_seconds": cpu_seconds,
        "service_cpu_nanoseconds_per_record": cpu_seconds * 1e9 / completed,
    }
    if latencies:
        result["latency_samples"] = len(latencies)
        result["latency_microseconds"] = {
            "p50": percentile(latencies, 0.50) / 1000,
            "p95": percentile(latencies, 0.95) / 1000,
            "p99": percentile(latencies, 0.99) / 1000,
            "maximum": max(latencies) / 1000,
        }
    return result


def connect_agent(host, port, token, workflow_id, worker):
    stream = socket.create_connection((host, port), timeout=30)
    stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if request(stream, AUTH, 1, token.encode())[0] != 0:
        raise RuntimeError("AUTH failed")
    key = hmac.digest(token.encode(), workflow_id.encode(), "sha256")
    endpoint = b"127.0.0.1"
    epoch = 1000000 + worker
    hello = (
        b"DVA1" + key + struct.pack("!IQHH", worker, epoch, 20000 + worker,
                                     len(endpoint))
        + endpoint.ljust(64, b"\0") + struct.pack("!I", 0)
    )
    deadline = time.monotonic() + 30
    while True:
        status, body = request(stream, HELLO, 2, hello)
        if status == 0:
            slot, assigned, reserved = struct.unpack("!QII", body)
            if assigned != worker or reserved:
                raise RuntimeError((assigned, reserved))
            return stream, slot, epoch
        if time.monotonic() >= deadline:
            raise RuntimeError(("HELLO failed", status, body))
        time.sleep(0.01)


def run_partition(operation, connection, records, iterations, sample_stride):
    stream, slot, worker, epoch = connection
    request_id = 10
    completed = 0
    latencies = []
    for _ in range(iterations):
        for data_id in records:
            request_id += 1
            sampled = sample_stride and completed % sample_stride == 0
            if operation == "publish":
                digest = hashlib.sha256(struct.pack("!Q", data_id)).digest()
                payload = batch_header(slot, worker, epoch, request_id)
                payload += PUBLISH.pack(data_id, 1, 0, 4096, data_id, digest)
                before = time.perf_counter_ns() if sampled else 0
                status, body = request(stream, DATA_READY, request_id, payload)
                if status or body:
                    raise RuntimeError((operation, data_id, status, body))
            else:
                payload = batch_header(slot, worker, epoch, 0)
                payload += RESOLVE_ITEM.pack(data_id, 1, 0)
                before = time.perf_counter_ns() if sampled else 0
                status, body = request(stream, RESOLVE, request_id, payload)
                if status or len(body) != RESOLVE_REPLY.size:
                    raise RuntimeError((operation, data_id, status, len(body)))
                fields = RESOLVE_REPLY.unpack(body)
                if fields[0] != 2 or fields[2] != data_id or fields[5] < 1:
                    raise RuntimeError((operation, data_id, fields[:6]))
            if sampled:
                latencies.append(time.perf_counter_ns() - before)
            completed += 1
    return completed, latencies


def process_client(host, port, token, workflow_id, worker, records, iterations,
                   sample_stride, barrier, results):
    stream = None
    try:
        stream, slot, epoch = connect_agent(
            host, port, token, workflow_id, worker
        )
        connection = (stream, slot, worker, epoch)
        barrier.wait(timeout=30)
        barrier.wait(timeout=30)
        completed, latencies = run_partition(
            "publish", connection, records, iterations, sample_stride
        )
        results.put(("publish", completed, latencies, None))
        barrier.wait(timeout=300)
        barrier.wait(timeout=30)
        completed, latencies = run_partition(
            "resolve", connection, records, iterations, sample_stride
        )
        results.put(("resolve", completed, latencies, None))
        barrier.wait(timeout=300)
    except BaseException as error:
        results.put(("error", 0, [], repr(error)))
        barrier.abort()
    finally:
        if stream is not None:
            stream.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, required=True)
    parser.add_argument("--connections", type=int, required=True)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument(
        "--latency-sample-stride", type=int, default=0,
        help="sample every Nth RPC latency; zero disables sampling",
    )
    parser.add_argument(
        "--client-mode", choices=("threads", "processes"), default="processes",
        help="processes isolate Controller throughput from the Python GIL",
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if (args.records < 1 or args.records > 4096 or args.connections < 1 or
            args.connections > 1000 or args.iterations < 1):
        parser.error("invalid benchmark dimensions")
    if args.latency_sample_stride < 0:
        parser.error("latency sample stride must be nonnegative")

    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    token = "controller-rpc-benchmark-token"
    workflow_id = f"controller-rpc-{args.records}-{args.connections}"
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="datavine-controller-rpc-") as root:
        root = Path(root)
        with (Path(f"{output}.service.log")).open("w") as service_log:
            service = subprocess.Popen(
                (str(service_binary), "serve", str(root / "journal"), token),
                stdout=subprocess.PIPE,
                stderr=service_log,
                text=True,
                env=dict(os.environ, DATAVINE_WORKFLOW_METRICS="1"),
            )
            connections = []
            client = None
            try:
                contact = json.loads(service.stdout.readline())
                parsed = urllib.parse.urlsplit(contact["endpoint"])
                client = WorkflowClient(contact["endpoint"], token, timeout=300)
                workflow = Workflow(
                    "controller-rpc-benchmark",
                    workflow_id=workflow_id,
                    maximum_tasks=args.records,
                    maximum_edges=0,
                )
                build_started = time.monotonic()
                outputs = [
                    workflow.command(("/bin/true",)) for _ in range(args.records)
                ]
                build_seconds = time.monotonic() - build_started
                submit_started = time.monotonic()
                workflow.submit(client)
                submit_seconds = time.monotonic() - submit_started
                data_ids = [item.data_id for item in outputs]
                partitions = [data_ids[index::args.connections]
                              for index in range(args.connections)]
                phases = {}
                if args.client_mode == "threads":
                    for worker in range(1, args.connections + 1):
                        stream, slot, epoch = connect_agent(
                            parsed.hostname, parsed.port, token, workflow_id,
                            worker
                        )
                        connections.append((stream, slot, worker, epoch))
                    for operation in ("publish", "resolve"):
                        cpu_before = process_cpu_seconds(service.pid)
                        started = time.monotonic()
                        with concurrent.futures.ThreadPoolExecutor(
                            max_workers=args.connections
                        ) as executor:
                            futures = [
                                executor.submit(
                                    run_partition, operation,
                                    connections[index], partitions[index],
                                    args.iterations, args.latency_sample_stride
                                )
                                for index in range(args.connections)
                            ]
                            partitions_result = [item.result() for item in futures]
                            completed = sum(item[0] for item in partitions_result)
                            latencies = [
                                latency for item in partitions_result
                                for latency in item[1]
                            ]
                        seconds = time.monotonic() - started
                        cpu_seconds = (process_cpu_seconds(service.pid) -
                                       cpu_before)
                        phases[operation] = phase_result(
                            operation, completed, args, seconds, cpu_seconds,
                            latencies
                        )
                else:
                    context = multiprocessing.get_context("spawn")
                    barrier = context.Barrier(args.connections + 1)
                    results = context.Queue()
                    clients = [
                        context.Process(
                            target=process_client,
                            args=(parsed.hostname, parsed.port, token,
                                  workflow_id, index + 1, partitions[index],
                                  args.iterations, args.latency_sample_stride,
                                  barrier, results),
                        )
                        for index in range(args.connections)
                    ]
                    for process in clients:
                        process.start()
                    barrier.wait(timeout=30)
                    for operation in ("publish", "resolve"):
                        cpu_before = process_cpu_seconds(service.pid)
                        started = time.monotonic()
                        barrier.wait(timeout=30)
                        barrier.wait(timeout=300)
                        seconds = time.monotonic() - started
                        cpu_seconds = (process_cpu_seconds(service.pid) -
                                       cpu_before)
                        completed = 0
                        latencies = []
                        for _ in clients:
                            result_operation, count, samples, error = results.get(
                                timeout=30
                            )
                            if error or result_operation != operation:
                                raise RuntimeError(
                                    (result_operation, error)
                                )
                            completed += count
                            latencies.extend(samples)
                        phases[operation] = phase_result(
                            operation, completed, args, seconds, cpu_seconds,
                            latencies
                        )
                    for process in clients:
                        process.join(timeout=30)
                        if process.exitcode != 0:
                            raise RuntimeError(
                                ("client exit", process.pid, process.exitcode)
                            )
                result = {
                    "status": "PASS",
                    "records": args.records,
                    "connections": args.connections,
                    "client_mode": args.client_mode,
                    "iterations": args.iterations,
                    "records_per_request": 1,
                    "batching": False,
                    "latency_sample_stride": args.latency_sample_stride,
                    "workflow_build_seconds": build_seconds,
                    "workflow_submit_seconds": submit_seconds,
                    "phases": phases,
                }
                output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
                print(json.dumps(result, sort_keys=True))
            finally:
                for connection in connections:
                    connection[0].close()
                if client is not None:
                    client.close()
                if service.poll() is None:
                    service.send_signal(signal.SIGTERM)
                stdout, _ = service.communicate(timeout=30)
                if service.returncode != 0:
                    raise RuntimeError((service.returncode, stdout))


if __name__ == "__main__":
    main()
