#!/usr/bin/env python3
"""Scale gate for the C-owned Workflow IR runtime (no Python workflow owner)."""

import argparse
import base64
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import hashlib
import re

from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


PHYSICAL_COUNTS = re.compile(
    r"physical_submissions=(?P<submitted>[0-9]+) "
    r"physical_completions=(?P<completed>[0-9]+)"
)


def physical_task_counts(service_log_path, workflow_id):
    for line in reversed(service_log_path.read_text().splitlines()):
        if f"datavine workflow {workflow_id} " not in line:
            continue
        match = PHYSICAL_COUNTS.search(line)
        if match:
            return {
                "submissions": int(match.group("submitted")),
                "completions": int(match.group("completed")),
            }
    raise RuntimeError("native runtime did not report physical task counts")


def process_tree(root_pids):
    pids = {int(pid) for pid in root_pids if pid}
    changed = True
    while changed:
        changed = False
        for stat in Path("/proc").glob("[0-9]*/stat"):
            try:
                fields = stat.read_text().split(") ", 1)[1].split()
                pid, parent = int(stat.parent.name), int(fields[1])
            except (FileNotFoundError, IndexError, ValueError):
                continue
            if parent in pids and pid not in pids:
                pids.add(pid)
                changed = True
    return pids


def sample(root_pids):
    rss = fds = 0
    pids = process_tree(root_pids)
    for pid in pids:
        try:
            status = Path(f"/proc/{pid}/status").read_text().splitlines()
            rss += int(next(line for line in status if line.startswith("VmRSS:" )).split()[1]) * 1024
            fds += len(list(Path(f"/proc/{pid}/fd").iterdir()))
        except (FileNotFoundError, StopIteration):
            continue
    return {"processes": len(pids), "rss_bytes": rss, "fds": fds}


def worker_inventory(vine_status, port):
    try:
        completed = subprocess.run(
            (str(vine_status), "-W", "localhost", str(port)),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
    except subprocess.TimeoutExpired:
        return []
    if completed.returncode:
        return []
    workers = []
    for line in completed.stdout.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 6:
            continue
        try:
            cores = int(float(fields[4]) + float(fields[5]))
        except ValueError:
            continue
        workers.append({"host": fields[0], "address": fields[1], "cores": cores})
    return workers


def wait_workers(vine_status, port, workers, cores, factory, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if factory.poll() is not None:
            raise RuntimeError(f"vine_factory exited with {factory.returncode}")
        inventory = worker_inventory(vine_status, port)
        if len(inventory) == workers and all(
            int(item.get("cores", item.get("cores_total", -1))) == cores
            for item in inventory
        ):
            return inventory
        time.sleep(1)
    raise TimeoutError(f"did not observe exactly {workers} x {cores} Workers")


def workflow_document(tasks, executor, command_argv, request_result):
    records = []
    data = []
    if executor == "builtin":
        payload_id = tasks + 1
    for ordinal in range(1, tasks + 1):
        executor_record = (
            {"kind": "taskvine", "version": "builtin-v1", "payload_ref": payload_id}
            if executor == "builtin"
            else {"kind": "command", "version": "1", "argv": command_argv}
        )
        records.append([ordinal, [], [ordinal]])
        data.append([ordinal, ordinal, 0])
    if executor == "builtin":
        data.append({
            "data_id": payload_id,
            "codec": {"name": "datavine/builtin", "version": "1"},
            "origin": {
                "kind": "inline",
                "base64": base64.b64encode(b"DVB1\x01").decode(),
            },
        })
    return {
        "schema": "datavine.workflow/v1",
        "workflow_id": f"native-scale-{executor}-{tasks}",
        "idempotency_key": (
            f"native-scale-{executor}-{tasks}-"
            f"{hashlib.sha256(json.dumps(command_argv).encode()).hexdigest()[:12]}"
        ),
        "mode": "sealed",
        "task_defaults": {"executor": executor_record},
        "data_defaults": {"codec": {"name": "bytes", "version": "1"}},
        "tasks": records,
        "data": data,
        "requested_outputs": [tasks] if request_result else [],
        "policy": {"maximum_tasks": tasks, "maximum_edges": 0},
    }


def streaming_initial(tasks, executor, command_argv):
    data = []
    if executor == "builtin":
        data.append({
            "data_id": 1,
            "codec": {"name": "datavine/builtin", "version": "1"},
            "origin": {
                "kind": "inline",
                "base64": base64.b64encode(b"DVB1\x01").decode(),
            },
        })
    return {
        "schema": "datavine.workflow/v1",
        "workflow_id": f"native-scale-{executor}-{tasks}",
        "idempotency_key": (
            f"native-scale-{executor}-{tasks}-streaming-"
            f"{hashlib.sha256(json.dumps(command_argv).encode()).hexdigest()[:12]}"
        ),
        "mode": "streaming",
        "tasks": [],
        "data": data,
        "requested_outputs": [],
        "policy": {"maximum_tasks": tasks, "maximum_edges": 0},
    }


def workflow_delta(
    initial, start, stop, tasks, executor, command_argv, request_result
):
    records = []
    data = []
    for ordinal in range(start, stop):
        output_id = ordinal + 1 if executor == "builtin" else ordinal
        executor_record = (
            {"kind": "taskvine", "version": "builtin-v1", "payload_ref": 1}
            if executor == "builtin"
            else {"kind": "command", "version": "1", "argv": command_argv}
        )
        records.append([ordinal, [], [output_id]])
        data.append([output_id, ordinal, 0])
    final_output = tasks + 1 if executor == "builtin" else tasks
    return {
        "schema": "datavine.workflow-delta/v1",
        "workflow_id": initial["workflow_id"],
        "idempotency_key": f"{initial['workflow_id']}-tasks-{start}-{stop - 1}",
        "task_defaults": {"executor": executor_record},
        "data_defaults": {"codec": {"name": "bytes", "version": "1"}},
        "tasks": records,
        "data": data,
        "requested_outputs": (
            [final_output] if request_result and stop > tasks else []
        ),
    }


def terminate_group(process, timeout=30):
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--cores", type=int, required=True)
    parser.add_argument("--memory", type=int, default=2048,
                        help="memory in MiB requested per worker")
    parser.add_argument("--disk", type=int, default=4096,
                        help="disk in MiB requested per worker")
    parser.add_argument("--gpus", type=int, default=0,
                        help="GPUs requested per worker")
    parser.add_argument("--batch-type", choices=("local", "condor"),
                        default="local")
    parser.add_argument("--registration", choices=("streaming", "sealed"),
                        default="streaming")
    parser.add_argument("--executor", choices=("builtin", "command"), default="builtin")
    parser.add_argument(
        "--command-argv-json",
        default='["/bin/true"]',
        help="JSON argv used by every command task",
    )
    parser.add_argument(
        "--expected-result-base64",
        default="",
        help="exact requested output expected from the command",
    )
    parser.add_argument(
        "--no-requested-output",
        action="store_true",
        help=(
            "run a pure task-throughput gate without fetching a workflow "
            "result; physical task identity and terminal state are still checked"
        ),
    )
    parser.add_argument(
        "--scheduling-mode",
        choices=("baseline", "worker-first"),
        default="baseline",
        help="physical TaskVine scheduling path used by the native runtime",
    )
    parser.add_argument("--minimum-runtime-tasks-per-second", type=float, default=0)
    parser.add_argument("--worker-timeout", type=float, default=300)
    parser.add_argument("--workflow-timeout", type=float, default=1800)
    parser.add_argument(
        "--chunk-tasks",
        type=int,
        default=10_000,
        help="maximum tasks per bounded Workflow Delta transaction",
    )
    parser.add_argument(
        "--poncho-env",
        help="run factory workers inside this packed environment",
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if min(args.tasks, args.workers, args.cores, args.memory, args.disk,
           args.chunk_tasks) < 1:
        parser.error("task and worker resource arguments must be positive")
    if args.gpus < 0:
        parser.error("GPUs per worker cannot be negative")
    try:
        command_argv = json.loads(args.command_argv_json)
    except json.JSONDecodeError as error:
        parser.error(f"invalid --command-argv-json: {error}")
    if not isinstance(command_argv, list) or not command_argv:
        parser.error("--command-argv-json must be a non-empty JSON array")
    expected_result = base64.b64decode(args.expected_result_base64, validate=True)

    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    vine_status = repository / "taskvine/src/tools/vine_status"
    token = "native-scale-token"
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="datavine-native-scale-") as root_name:
        root = Path(root_name)
        journal = root / "journal"
        service_log_path = Path(f"{output}.service.log")
        service_log = service_log_path.open("w")
        factory_log = (Path(f"{output}.factory.log")).open("w")
        service_environment = dict(
            os.environ,
            DATAVINE_WORKFLOW_METRICS="1",
            DATAVINE_RUNTIME_INFO_PATH=str(root / "run-info"),
        )
        if args.scheduling_mode == "worker-first":
            service_environment["DATAVINE_WORKER_FIRST_SCHEDULING"] = "1"
        else:
            service_environment.pop("DATAVINE_WORKER_FIRST_SCHEDULING", None)
        service = subprocess.Popen(
            (str(service_binary), "serve", str(journal), token),
            stdout=subprocess.PIPE,
            stderr=service_log,
            text=True,
            start_new_session=True,
            env=service_environment,
        )
        factory = None
        try:
            contact = json.loads(service.stdout.readline())
            manager_host = (
                "localhost" if args.batch_type == "local" else socket.getfqdn()
            )
            factory_command = (
                "vine_factory", "--batch-type", args.batch_type,
                "--min-workers", str(args.workers),
                "--max-workers", str(args.workers), "--workers-per-cycle", str(args.workers),
                "--factory-period", "1", "--factory-timeout", str(round(args.workflow_timeout + 120)),
                "--cores", str(args.cores), "--timeout", str(round(args.workflow_timeout + 60)),
                "--memory", str(args.memory), "--disk", str(args.disk),
                "--gpus", str(args.gpus),
                "--worker-binary", str(repository / "taskvine/src/worker/vine_worker"),
                "--scratch-dir", str(root / "factory"), "--parent-death",
                manager_host, str(contact["manager_port"]),
            )
            if args.poncho_env:
                factory_command = (
                    *factory_command[:-2],
                    "--poncho-env",
                    str(Path(args.poncho_env).resolve()),
                    *factory_command[-2:],
                )
            factory = subprocess.Popen(
                factory_command,
                stdout=factory_log,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            gate_started = time.monotonic()
            inventory = wait_workers(
                vine_status, contact["manager_port"], args.workers, args.cores,
                factory, args.worker_timeout,
            )
            worker_wait = time.monotonic() - gate_started
            build_started = time.monotonic()
            document = (
                workflow_document(
                    args.tasks,
                    args.executor,
                    command_argv,
                    not args.no_requested_output,
                )
                if args.registration == "sealed"
                else streaming_initial(args.tasks, args.executor, command_argv)
            )
            build_seconds = time.monotonic() - build_started
            initial_payload_bytes = len(json.dumps(
                document,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode())
            client = WorkflowClient(contact["endpoint"], token)
            started = time.monotonic()
            initial_submit_started = time.monotonic()
            submitted = client.submit_workflow(document)
            initial_submit_seconds = time.monotonic() - initial_submit_started
            generation = submitted["generation"]
            transaction_count = 0
            maximum_delta_bytes = 0
            total_delta_bytes = 0
            delta_build_seconds = 0
            delta_encode_seconds = 0
            append_rpc_seconds = 0
            if args.registration == "streaming":
                for start in range(1, args.tasks + 1, args.chunk_tasks):
                    stop = min(args.tasks + 1, start + args.chunk_tasks)
                    delta_started = time.monotonic()
                    delta = workflow_delta(
                        document,
                        start,
                        stop,
                        args.tasks,
                        args.executor,
                        command_argv,
                        not args.no_requested_output,
                    )
                    delta_build_seconds += time.monotonic() - delta_started
                    encode_started = time.monotonic()
                    encoded_delta = json.dumps(
                        delta,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode()
                    delta_encode_seconds += time.monotonic() - encode_started
                    maximum_delta_bytes = max(
                        maximum_delta_bytes,
                        len(encoded_delta),
                    )
                    total_delta_bytes += len(encoded_delta)
                    append_started = time.monotonic()
                    submitted = client.append_workflow(
                        document["workflow_id"], generation, encoded_delta
                    )
                    append_rpc_seconds += time.monotonic() - append_started
                    generation = submitted["generation"]
                    transaction_count += 1
                seal_started = time.monotonic()
                submitted = client.seal_workflow(document["workflow_id"], generation)
                seal_seconds = time.monotonic() - seal_started
            else:
                seal_seconds = 0.0
            submitted_at = time.monotonic()
            peak = sample((service.pid, factory.pid))
            deadline = started + args.workflow_timeout
            while True:
                state = client.describe_workflow(document["workflow_id"])
                current = sample((service.pid, factory.pid))
                peak = {key: max(peak[key], current[key]) for key in peak}
                if state["state"] in ("completed", "failed"):
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError(state)
                time.sleep(0.25)
            elapsed = time.monotonic() - started
            run_seconds = time.monotonic() - submitted_at
            result = b""
            if not args.no_requested_output:
                result_data_id = (
                    args.tasks + 1
                    if args.executor == "builtin" and args.registration == "streaming"
                    else args.tasks
                )
                result = client.fetch_workflow_result(
                    document["workflow_id"], result_data_id
                )
            if state["state"] != "completed" or (
                not args.no_requested_output and result != expected_result
            ):
                raise RuntimeError({"state": state, "result": base64.b64encode(result).decode()})
            service_log.flush()
            physical_tasks = physical_task_counts(
                service_log_path, document["workflow_id"]
            )
            expected_physical_tasks = {
                "submissions": args.tasks,
                "completions": args.tasks,
            }
            if physical_tasks != expected_physical_tasks:
                raise RuntimeError(
                    "task identity violation: expected one physical TaskVine "
                    f"execution per logical task, got {physical_tasks}"
                )
            runtime_rate = args.tasks / elapsed
            if runtime_rate < args.minimum_runtime_tasks_per_second:
                raise RuntimeError(
                    f"runtime throughput {runtime_rate:.3f} below "
                    f"{args.minimum_runtime_tasks_per_second:.3f} tasks/s"
                )
            evidence = {
                "status": "PASS",
                "architecture": "native-c-workflow-owner",
                "executor": args.executor,
                "command_argv": command_argv if args.executor == "command" else None,
                "tasks": args.tasks,
                "physical_tasks": physical_tasks,
                "workers": args.workers,
                "cores_per_worker": args.cores,
                "memory_mib_per_worker": args.memory,
                "disk_mib_per_worker": args.disk,
                "gpus_per_worker": args.gpus,
                "batch_type": args.batch_type,
                "registration": args.registration,
                "scheduling_mode": args.scheduling_mode,
                "requested_output": not args.no_requested_output,
                "worker_inventory": len(inventory),
                "worker_wait_seconds": worker_wait,
                "workflow_build_seconds": build_seconds,
                "execution_seconds": elapsed,
                "submission_seconds": submitted_at - started,
                "initial_submit_seconds": initial_submit_seconds,
                "delta_build_seconds": delta_build_seconds,
                "delta_encode_seconds": delta_encode_seconds,
                "append_rpc_seconds": append_rpc_seconds,
                "seal_seconds": seal_seconds,
                "registration_transactions": transaction_count,
                "chunk_tasks": args.chunk_tasks,
                "initial_payload_bytes": initial_payload_bytes,
                "maximum_delta_bytes": maximum_delta_bytes,
                "total_delta_payload_bytes": total_delta_bytes,
                "total_workflow_payload_bytes": (
                    initial_payload_bytes + total_delta_bytes
                ),
                "run_seconds": run_seconds,
                "runtime_tasks_per_second": runtime_rate,
                "tasks_per_second": args.tasks / elapsed,
                "post_registration_tasks_per_second": (
                    args.tasks / run_seconds if run_seconds > 0 else None
                ),
                "workflow": state,
                "submitted": submitted,
                "exact_result_bytes": len(result),
                "peak": peak,
                "journal_bytes": sum(
                    path.stat().st_size for path in root.glob("journal*")
                ),
                "factory_command": list(factory_command),
            }
            output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
            print(json.dumps(evidence, sort_keys=True))
        finally:
            # Condor factory removes a large pool serially during shutdown.
            terminate_group(factory, timeout=300)
            terminate_group(service)
            factory_log.close()
            service_log.close()


if __name__ == "__main__":
    main()
