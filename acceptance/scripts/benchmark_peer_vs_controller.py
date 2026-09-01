#!/usr/bin/env python3
"""Compare peer-first and Controller-backup input delivery at scale."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import time


def load_workflow_client(repository):
    source = (
        repository / "taskvine/src/bindings/python3/ndcctools/taskvine/datavine"
        / "workflow_client.py"
    )
    spec = importlib.util.spec_from_file_location("datavine_workflow_client", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.WorkflowClient


def stop_group(process, timeout=300):
    if process is None or process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=timeout)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=30)


def worker_inventory(vine_status, port):
    try:
        result = subprocess.run(
            (str(vine_status), "-W", "localhost", str(port)),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
    except subprocess.TimeoutExpired:
        return []
    if result.returncode:
        return []
    workers = []
    for line in result.stdout.splitlines()[1:]:
        fields = line.split()
        if len(fields) >= 6:
            workers.append({"host": fields[0], "address": fields[1]})
    return workers


def wait_workers(vine_status, port, expected, factories, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for factory in factories:
            if factory is not None and factory.poll() is not None:
                raise RuntimeError(f"vine_factory exited with {factory.returncode}")
        inventory = worker_inventory(vine_status, port)
        if len(inventory) == expected:
            return inventory
        time.sleep(1)
    raise TimeoutError(
        f"expected {expected} Workers, observed {len(worker_inventory(vine_status, port))}"
    )


def start_factory(
    repository, root, name, batch_type, manager_host, manager_port,
    workers, memory, disk, timeout,
):
    log_path = root / f"{name}-factory.log"
    log = log_path.open("w")
    command = (
        str(repository / "batch_job/src/vine_factory"),
        "--batch-type", batch_type,
        "--min-workers", str(workers),
        "--max-workers", str(workers),
        "--workers-per-cycle", str(workers),
        "--factory-period", "1",
        "--factory-timeout", str(timeout + 120),
        "--cores", "1",
        "--memory", str(memory),
        "--disk", str(disk),
        "--gpus", "0",
        "--timeout", str(timeout + 60),
        "--worker-binary", str(repository / "taskvine/src/worker/vine_worker"),
        "--scratch-dir", str(root / f"{name}-factory"),
        "--parent-death",
        manager_host,
        str(manager_port),
    )
    process = subprocess.Popen(
        command,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    return process, log, command


def wait_state(client, workflow_id, state, generation, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = client.describe_workflow(workflow_id)
        if info["state"] == state and info["generation"] == generation:
            return info
        if info["state"] in {"failed", "cancelled"}:
            raise RuntimeError(info)
        time.sleep(0.1)
    raise TimeoutError(client.describe_workflow(workflow_id))


def data_file_count(root):
    return sum(1 for _ in root.glob("*/*.data")) if root.exists() else 0


def wait_backups(root, expected, timeout):
    deadline = time.monotonic() + timeout
    observed = 0
    while time.monotonic() < deadline:
        observed = data_file_count(root)
        if observed == expected:
            return observed
        time.sleep(0.25)
    raise TimeoutError(f"Controller backup count {observed}/{expected}")


def default_interface():
    for line in Path("/proc/net/route").read_text().splitlines()[1:]:
        fields = line.split()
        if len(fields) > 1 and fields[1] == "00000000":
            return fields[0]
    return None


def interface_counters(interface):
    if not interface:
        return {"receive_bytes": 0, "transmit_bytes": 0}
    for line in Path("/proc/net/dev").read_text().splitlines():
        name, separator, counters = line.partition(":")
        if separator and name.strip() == interface:
            values = counters.split()
            return {
                "receive_bytes": int(values[0]),
                "transmit_bytes": int(values[8]),
            }
    raise RuntimeError(f"network interface {interface!r} does not exist")


def process_counters(pid):
    counters = {"cpu_seconds": 0.0, "read_bytes": 0, "write_bytes": 0}
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()
        counters["cpu_seconds"] = (
            int(fields[11]) + int(fields[12])
        ) / os.sysconf("SC_CLK_TCK")
        values = dict(
            line.split(":", 1)
            for line in Path(f"/proc/{pid}/io").read_text().splitlines()
        )
        counters["read_bytes"] = int(values.get("read_bytes", 0))
        counters["write_bytes"] = int(values.get("write_bytes", 0))
    except (OSError, IndexError, ValueError):
        pass
    return counters


def delta(before, after):
    return {key: after[key] - before[key] for key in before}


def physical_counts(log_path, workflow_id):
    pattern = re.compile(
        r"physical_submissions=(?P<submitted>[0-9]+) "
        r"physical_completions=(?P<completed>[0-9]+)"
    )
    for line in reversed(log_path.read_text().splitlines()):
        if f"datavine workflow {workflow_id} " in line:
            match = pattern.search(line)
            if match:
                return {key: int(value) for key, value in match.groupdict().items()}
    raise RuntimeError("physical task counters were not reported")


def wait_physical_counts(log_path, workflow_id, expected, timeout=10):
    deadline = time.monotonic() + timeout
    observed = None
    while time.monotonic() < deadline:
        try:
            observed = physical_counts(log_path, workflow_id)
        except RuntimeError:
            observed = None
        if observed == expected:
            return observed
        time.sleep(0.05)
    raise RuntimeError(observed)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("peer", "controller"), required=True)
    parser.add_argument("--files", type=int, default=10_000)
    parser.add_argument("--bytes-per-file", type=int, default=1_048_576)
    parser.add_argument("--producer-workers", type=int, default=32)
    parser.add_argument("--consumer-workers", type=int, default=64)
    parser.add_argument("--producer-memory", type=int, default=3072)
    parser.add_argument("--consumer-memory", type=int, default=6144)
    parser.add_argument("--worker-disk", type=int, default=10240)
    parser.add_argument("--batch-type", choices=("local", "condor"), default="condor")
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--network-interface")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(
        args.files, args.bytes_per_file, args.producer_workers,
        args.consumer_workers, args.producer_memory, args.consumer_memory,
        args.worker_disk,
    ) < 1:
        parser.error("counts and resource values must be positive")
    if args.consumer_memory <= args.producer_memory + 512:
        parser.error("consumer-memory must exceed producer-memory by more than 512 MiB")
    return args


def main():
    args = parse_args()
    repository = Path(__file__).resolve().parents[2]
    WorkflowClient = load_workflow_client(repository)
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    vine_status = repository / "taskvine/src/tools/vine_status"
    workflow_id = f"peer-controller-{args.mode}"
    token = "peer-controller-benchmark-token"
    args.output = args.output.resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    service_log_path = Path(f"{args.output}.service.log")
    interface = args.network_interface or default_interface()
    consumer_task_memory = args.producer_memory + 512
    with tempfile.TemporaryDirectory(prefix="datavine-peer-controller-") as root_name:
        root = Path(root_name)
        journal = root / "journal"
        service_log = service_log_path.open("w")
        service = subprocess.Popen(
            (str(service_binary), "serve", str(journal), token),
            stdout=subprocess.PIPE,
            stderr=service_log,
            text=True,
            start_new_session=True,
            env=dict(os.environ, DATAVINE_WORKFLOW_METRICS="1"),
        )
        producer_factory = consumer_factory = None
        producer_log = consumer_log = None
        client = None
        try:
            contact = json.loads(service.stdout.readline())
            manager_host = "localhost" if args.batch_type == "local" else socket.getfqdn()
            producer_factory, producer_log, producer_command = start_factory(
                repository, root, "producer", args.batch_type, manager_host,
                contact["manager_port"], args.producer_workers,
                args.producer_memory, args.worker_disk, args.timeout,
            )
            producer_inventory = wait_workers(
                vine_status, contact["manager_port"], args.producer_workers,
                (producer_factory,), args.timeout,
            )
            client = WorkflowClient(contact["endpoint"], token)
            producer_tasks = []
            producer_data = []
            for item in range(1, args.files + 1):
                producer_tasks.append({
                    "task_id": item,
                    "executor": {
                        "kind": "command",
                        "version": "1",
                        "argv": [
                            "/usr/bin/head", "-c", str(args.bytes_per_file),
                            "/dev/zero",
                        ],
                    },
                    "inputs": [],
                    "output_data_ids": [item],
                    "resources": {"cores": 1, "memory_mb": 128},
                })
                producer_data.append({
                    "data_id": item,
                    "codec": {"name": "bytes", "version": "1"},
                    "origin": {"kind": "output", "task_id": item, "output_index": 0},
                })
            initial = {
                "schema": "datavine.workflow/v1",
                "workflow_id": workflow_id,
                "idempotency_key": f"{workflow_id}-producers",
                "mode": "streaming",
                "tasks": producer_tasks,
                "data": producer_data,
                "requested_outputs": [],
                "policy": {
                    "maximum_tasks": args.files * 2,
                    "maximum_edges": args.files,
                },
            }
            submitted = client.submit_workflow(initial)
            wait_state(client, workflow_id, "open_quiescent", 1, args.timeout)
            backup_started = time.monotonic()
            wait_backups(Path(f"{journal}.data"), args.files, args.timeout)
            backup_ready_seconds = time.monotonic() - backup_started

            if args.mode == "controller":
                stop_group(producer_factory)
                producer_factory = None
                producer_log.close()
                producer_log = None
                wait_workers(
                    vine_status, contact["manager_port"], 0, (), args.timeout
                )

            expected_workers = args.consumer_workers + (
                args.producer_workers if args.mode == "peer" else 0
            )
            # A factory targets the Manager's total connected population, not
            # just jobs submitted by that factory. In peer mode the producer
            # Workers remain connected as read-only sources, so the second
            # factory targets producers + consumers and contributes only the
            # missing consumer population.
            consumer_factory, consumer_log, consumer_command = start_factory(
                repository, root, "consumer", args.batch_type, manager_host,
                contact["manager_port"], expected_workers,
                args.consumer_memory, args.worker_disk, args.timeout,
            )
            consumer_inventory = wait_workers(
                vine_status, contact["manager_port"], expected_workers,
                tuple(
                    process for process in (producer_factory, consumer_factory)
                    if process is not None
                ),
                args.timeout,
            )

            consumer_tasks = []
            consumer_data = []
            for item in range(1, args.files + 1):
                task_id = args.files + item
                output_id = args.files + item
                consumer_tasks.append({
                    "task_id": task_id,
                    "executor": {
                        "kind": "command",
                        "version": "1",
                        "argv": [
                            "/bin/sh", "-c",
                            f'test "$(wc -c < "$1")" -eq {args.bytes_per_file}',
                            "datavine", f"{{{{data:{item}}}}}",
                        ],
                    },
                    "inputs": [{"position": 0, "data_id": item}],
                    "output_data_ids": [output_id],
                    "resources": {
                        "cores": 1,
                        "memory_mb": consumer_task_memory,
                    },
                })
                consumer_data.append({
                    "data_id": output_id,
                    "codec": {"name": "bytes", "version": "1"},
                    "origin": {
                        "kind": "output",
                        "task_id": task_id,
                        "output_index": 0,
                    },
                })
            consumer_delta = {
                "schema": "datavine.workflow-delta/v1",
                "workflow_id": workflow_id,
                "idempotency_key": f"{workflow_id}-consumers",
                "tasks": consumer_tasks,
                "data": consumer_data,
                "requested_outputs": [],
            }
            network_before = interface_counters(interface)
            process_before = process_counters(service.pid)
            consumer_started = time.monotonic()
            appended = client.append_workflow(
                workflow_id, submitted["generation"], consumer_delta
            )
            client.seal_workflow(workflow_id, appended["generation"])
            final = wait_state(
                client, workflow_id, "completed", appended["generation"],
                args.timeout,
            )
            consumer_seconds = time.monotonic() - consumer_started
            process_after = process_counters(service.pid)
            network_after = interface_counters(interface)
            service_log.flush()
            expected_counts = {
                "submitted": args.files,
                "completed": args.files,
            }
            counts = wait_physical_counts(
                service_log_path, workflow_id, expected_counts
            )
            data_root = Path(f"{journal}.data")
            deadline = time.monotonic() + 30
            post_gc_files = data_file_count(data_root)
            while post_gc_files and time.monotonic() < deadline:
                time.sleep(0.1)
                post_gc_files = data_file_count(data_root)
            evidence = {
                "status": "PASS",
                "mode": args.mode,
                "route_proof": (
                    "producer Workers remained connected but were ineligible for consumer tasks; live Worker replicas therefore forced peer sources"
                    if args.mode == "peer" else
                    "all producer Workers disconnected after Controller backup admission; no Worker replica remained, forcing Controller sources"
                ),
                "files": args.files,
                "bytes_per_file": args.bytes_per_file,
                "total_input_bytes": args.files * args.bytes_per_file,
                "producer_workers": args.producer_workers,
                "consumer_workers": args.consumer_workers,
                "producer_memory_mb": args.producer_memory,
                "consumer_memory_mb": args.consumer_memory,
                "consumer_task_memory_mb": consumer_task_memory,
                "batch_type": args.batch_type,
                "default_background_backup": True,
                "backup_files_before_consumers": args.files,
                "backup_ready_wait_after_producer_quiescence_seconds": backup_ready_seconds,
                "consumer_seconds": consumer_seconds,
                "consumer_files_per_second": args.files / consumer_seconds,
                "consumer_mib_per_second": (
                    args.files * args.bytes_per_file / consumer_seconds / 1048576
                ),
                "network_interface": interface,
                "frontend_network": delta(network_before, network_after),
                "controller_process": delta(process_before, process_after),
                "producer_inventory": len(producer_inventory),
                "consumer_phase_inventory": len(consumer_inventory),
                "consumer_epoch_physical_tasks": counts,
                "workflow": final,
                "post_gc_files": post_gc_files,
                "producer_factory_command": list(producer_command),
                "consumer_factory_command": list(consumer_command),
            }
            args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
            print(json.dumps(evidence, sort_keys=True))
        finally:
            if client is not None:
                client.close()
            stop_group(consumer_factory)
            stop_group(producer_factory)
            stop_group(service, timeout=30)
            for log in (consumer_log, producer_log, service_log):
                if log is not None and not log.closed:
                    log.close()


if __name__ == "__main__":
    main()
