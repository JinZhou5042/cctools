#!/usr/bin/env python3
"""Scale gate for the C-owned Workflow IR runtime (no Python workflow owner)."""

import argparse
import base64
import importlib.util
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
import shutil

from evidence_layout import log_path


def load_workflow_client():
    """Load the client that belongs to this checkout, without an install step."""
    repository = Path(__file__).resolve().parents[2]
    source = (
        repository / "taskvine/src/bindings/python3/ndcctools/taskvine/datavine"
        / "workflow_client.py"
    )
    spec = importlib.util.spec_from_file_location("datavine_workflow_client", source)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load DataVine workflow client from {source}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.WorkflowClient


WorkflowClient = load_workflow_client()


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


def native_runtime_metrics(service_log_path, workflow_id):
    lines = service_log_path.read_text().splitlines()
    persistence_fields = {}
    for line in reversed(lines):
        if f"datavine controller_persistence {workflow_id} " in line:
            persistence_fields = dict(re.findall(r"([a-z_]+)=([^ ]+)", line))
            break
    for line in reversed(lines):
        if f"datavine workflow {workflow_id} " not in line:
            continue
        fields = dict(re.findall(r"([a-z_]+)=([^ ]+)", line))
        required = (
            "run_seconds",
            "manager_lock_seconds",
            "manager_owner_queue_seconds",
            "manager_owner_execute_seconds",
            "manager_submission_frontier",
            "manager_time_send_us",
            "manager_time_receive_us",
            "manager_time_send_good_us",
            "manager_time_receive_good_us",
            "manager_time_internal_us",
            "manager_time_polling_us",
            "manager_time_status_us",
            "manager_time_application_us",
            "manager_time_scheduling_us",
            "completion_processing_seconds",
            "logical_transition_seconds",
            "completion_event_seconds",
            "checkpoint_seconds",
            "scheduler_delay_seconds",
        )
        if all(key in fields for key in required):
            metrics = {
                key: int(fields[key]) if key == "manager_submission_frontier"
                else float(fields[key])
                for key in required
            }
            persistence_required = (
                "agent_persistence_jobs",
                "agent_persistence_bytes",
                "agent_persistence_failures",
                "agent_persistence_retries",
                "agent_persistence_peak_queue_depth",
                "agent_persistence_enqueue_block_seconds",
                "agent_persistence_queue_wait_seconds",
                "agent_persistence_connection_wait_seconds",
                "agent_persistence_request_seconds",
                "agent_persistence_stream_seconds",
                "agent_persistence_fsync_seconds",
                "agent_persistence_close_seconds",
                "agent_persistence_rename_seconds",
                "agent_persistence_verify_seconds",
                "agent_persistence_commit_wait_seconds",
                "agent_persistence_commit_seconds",
                "agent_persistence_commit_groups",
            )
            if not all(key in persistence_fields for key in persistence_required):
                raise RuntimeError(
                    "native runtime did not report Controller persistence metrics"
                )
            integer_metrics = {
                "agent_persistence_jobs",
                "agent_persistence_bytes",
                "agent_persistence_failures",
                "agent_persistence_retries",
                "agent_persistence_peak_queue_depth",
                "agent_persistence_commit_groups",
            }
            metrics.update({
                key: int(persistence_fields[key]) if key in integer_metrics
                else float(persistence_fields[key])
                for key in persistence_required
            })
            return metrics
    raise RuntimeError("native runtime did not report service metrics")


def process_tree(root_pids):
    pids = {int(pid) for pid in root_pids if pid}
    changed = True
    while changed:
        changed = False
        for stat in Path("/proc").glob("[0-9]*/stat"):
            try:
                fields = stat.read_text().split(") ", 1)[1].split()
                pid, parent = int(stat.parent.name), int(fields[1])
            except (OSError, IndexError, ValueError):
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
        except (OSError, StopIteration):
            continue
    return {"processes": len(pids), "rss_bytes": rss, "fds": fds}


def process_counters(pid):
    result = {"cpu_seconds": 0.0, "read_bytes": 0, "write_bytes": 0}
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()
        ticks = os.sysconf("SC_CLK_TCK")
        result["cpu_seconds"] = (int(fields[11]) + int(fields[12])) / ticks
    except (OSError, IndexError, ValueError):
        pass
    try:
        values = dict(
            line.split(":", 1) for line in
            Path(f"/proc/{pid}/io").read_text().splitlines()
        )
        result["read_bytes"] = int(values.get("read_bytes", 0))
        result["write_bytes"] = int(values.get("write_bytes", 0))
    except (OSError, ValueError):
        pass
    return result


def interface_counters(interface):
    if not interface:
        return {"receive_bytes": 0, "transmit_bytes": 0}
    fields = Path("/proc/net/dev").read_text().splitlines()
    for line in fields:
        name, separator, counters = line.partition(":")
        if separator and name.strip() == interface:
            values = counters.split()
            return {
                "receive_bytes": int(values[0]),
                "transmit_bytes": int(values[8]),
            }
    raise RuntimeError(f"network interface not found: {interface}")


def counter_delta(before, after):
    return {key: after[key] - before[key] for key in before}


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


def layered_inputs(task_id, tasks, width):
    """Two deterministic parents in the previous wide layer."""
    layer, offset = divmod(task_id - 1, width)
    if layer == 0:
        return []
    previous_first = (layer - 1) * width + 1
    previous_count = min(width, tasks - previous_first + 1)
    first = previous_first + offset % previous_count
    second_offset = (offset * 8191 + layer * 131) % previous_count
    second = previous_first + second_offset
    if previous_count > 1 and second == first:
        second = previous_first + (second_offset + 1) % previous_count
    return [first] if second == first else [first, second]


def dependency_inputs(task_id, tasks, mode, width):
    return [] if mode == "independent" else layered_inputs(task_id, tasks, width)


def dependency_edges(tasks, mode, width):
    if mode == "independent" or tasks <= width:
        return 0
    return 2 * (tasks - width)


def command_executor(command_argv, output_name):
    record = {"kind": "command", "version": "1", "argv": command_argv}
    if output_name:
        record["output_files"] = [output_name]
    return record


def workflow_document(tasks, executor, command_argv, command_output_name,
                      request_result,
                      request_all_outputs=False, dependency_mode="independent",
                      dag_width=16384,
                      idata_backup="controller-background",
                      workflow_recovery="journal"):
    records = []
    data = []
    if executor == "builtin":
        payload_id = tasks + 1
    for ordinal in range(1, tasks + 1):
        executor_record = (
            {"kind": "taskvine", "version": "builtin-v1", "payload_ref": payload_id}
            if executor == "builtin"
            else command_executor(command_argv, command_output_name)
        )
        records.append([ordinal, dependency_inputs(
            ordinal, tasks, dependency_mode, dag_width), [ordinal]])
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
        "workflow_id": f"native-scale-{executor}-{dependency_mode}-{tasks}",
        "idempotency_key": (
            f"native-scale-{executor}-{tasks}-"
            f"{hashlib.sha256(json.dumps(command_argv).encode()).hexdigest()[:12]}"
        ),
        "mode": "sealed",
        "task_defaults": {"executor": executor_record},
        "data_defaults": {"codec": {"name": "bytes", "version": "1"}},
        "tasks": records,
        "data": data,
        "requested_outputs": (
            list(range(1, tasks + 1)) if request_all_outputs
            else [tasks] if request_result else []
        ),
        "policy": {
            "maximum_tasks": tasks,
            "maximum_edges": dependency_edges(
                tasks, dependency_mode, dag_width),
            "idata_backup": idata_backup,
            "recovery": workflow_recovery,
        },
    }


def appendable_initial(tasks, executor, command_argv, command_output_name,
                       registration="streaming",
                       dependency_mode="independent", dag_width=16384,
                       idata_backup="controller-background",
                       workflow_recovery="journal"):
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
        "workflow_id": f"native-scale-{executor}-{dependency_mode}-{tasks}",
        "idempotency_key": (
            f"native-scale-{executor}-{tasks}-{registration}-"
            f"{hashlib.sha256(json.dumps(command_argv).encode()).hexdigest()[:12]}"
        ),
        "mode": registration,
        "tasks": [],
        "data": data,
        "requested_outputs": [],
        "policy": {
            "maximum_tasks": tasks,
            "maximum_edges": dependency_edges(
                tasks, dependency_mode, dag_width),
            "idata_backup": idata_backup,
            "recovery": workflow_recovery,
        },
    }


def workflow_delta(
    initial, start, stop, tasks, executor, command_argv, command_output_name,
    request_result,
    request_all_outputs=False, dependency_mode="independent", dag_width=16384,
):
    records = []
    data = []
    requested_ids = []
    for ordinal in range(start, stop):
        output_id = ordinal + 1 if executor == "builtin" else ordinal
        executor_record = (
            {"kind": "taskvine", "version": "builtin-v1", "payload_ref": 1}
            if executor == "builtin"
            else command_executor(command_argv, command_output_name)
        )
        records.append([ordinal, dependency_inputs(
            ordinal, tasks, dependency_mode, dag_width), [output_id]])
        data.append([output_id, ordinal, 0])
        requested_ids.append(output_id)
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
            requested_ids if request_all_outputs
            else [final_output] if request_result and stop > tasks else []
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
    parser.add_argument("--registration", choices=("streaming", "staged", "sealed"),
                        default="streaming")
    parser.add_argument(
        "--idata-backup",
        choices=("controller-background", "worker-local"),
        default="controller-background",
        help="Controller backup policy for intermediate data",
    )
    parser.add_argument(
        "--workflow-recovery",
        choices=("journal", "none"),
        default="journal",
        help="workflow metadata recovery policy; none is process-lifetime only",
    )
    parser.add_argument("--executor", choices=("builtin", "command"), default="builtin")
    parser.add_argument(
        "--command-argv-json",
        default='["/bin/true"]',
        help="JSON argv used by every command task",
    )
    parser.add_argument(
        "--command-output-name",
        help="declare the single sandbox file produced by every command task",
    )
    parser.add_argument(
        "--expected-result-base64",
        default="",
        help="exact requested output expected from the command",
    )
    parser.add_argument(
        "--expected-result-file",
        type=Path,
        help="file containing the exact expected command output",
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
        "--request-all-outputs",
        action="store_true",
        help="retain every task output as a durable Controller result",
    )
    parser.add_argument(
        "--dependency-mode", choices=("independent", "layered"),
        default="independent",
    )
    parser.add_argument(
        "--dag-width", type=int, default=16384,
        help="ready-task width for the deterministic layered DAG",
    )
    parser.add_argument(
        "--journal-parent", type=Path,
        help="create the temporary Controller journal/data tree under this filesystem",
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
    parser.add_argument(
        "--metrics-interface",
        help="record host network-byte deltas for this interface",
    )
    parser.add_argument(
        "--persistence-diagnostics",
        action="store_true",
        help="enable detailed requested-output timing counters",
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if min(args.tasks, args.workers, args.cores, args.memory, args.disk,
           args.chunk_tasks, args.dag_width) < 1:
        parser.error("task and worker resource arguments must be positive")
    if args.gpus < 0:
        parser.error("GPUs per worker cannot be negative")
    if args.no_requested_output and args.request_all_outputs:
        parser.error("requested-output options are mutually exclusive")
    try:
        command_argv = json.loads(args.command_argv_json)
    except json.JSONDecodeError as error:
        parser.error(f"invalid --command-argv-json: {error}")
    if not isinstance(command_argv, list) or not command_argv:
        parser.error("--command-argv-json must be a non-empty JSON array")
    if args.expected_result_file and args.expected_result_base64:
        parser.error("expected-result options are mutually exclusive")
    try:
        expected_result = (
            args.expected_result_file.read_bytes()
            if args.expected_result_file
            else base64.b64decode(args.expected_result_base64, validate=True)
        )
    except OSError as error:
        parser.error(f"cannot read expected result: {error}")

    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    vine_status = repository / "taskvine/src/tools/vine_status"
    token = "native-scale-token"
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="datavine-native-scale-") as root_name:
        root = Path(root_name)
        journal_state = None
        if args.journal_parent:
            args.journal_parent.resolve().mkdir(parents=True, exist_ok=True)
            journal_state = Path(tempfile.mkdtemp(
                prefix="datavine-shared-state-",
                dir=args.journal_parent.resolve(),
            ))
        journal = (journal_state or root) / "journal"
        service_log_path = log_path(output, "service")
        service_log = service_log_path.open("w")
        factory_log = log_path(output, "factory").open("w")
        service_environment = dict(
            os.environ,
            DATAVINE_WORKFLOW_METRICS="1",
            DATAVINE_RUNTIME_INFO_PATH=str(root / "run-info"),
        )
        if args.persistence_diagnostics:
            service_environment["DATAVINE_PERSISTENCE_DIAGNOSTICS"] = "1"
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
                str(repository / "batch_job/src/vine_factory"),
                "--batch-type", args.batch_type,
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
                    args.command_output_name,
                    not args.no_requested_output,
                    args.request_all_outputs,
                    args.dependency_mode,
                    args.dag_width,
                    args.idata_backup,
                    args.workflow_recovery,
                )
                if args.registration == "sealed"
                else appendable_initial(
                    args.tasks, args.executor, command_argv,
                    args.command_output_name,
                    args.registration, args.dependency_mode, args.dag_width,
                    args.idata_backup, args.workflow_recovery,
                )
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
            process_before = process_counters(service.pid)
            network_before = interface_counters(args.metrics_interface)
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
            if args.registration != "sealed":
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
                        args.command_output_name,
                        not args.no_requested_output,
                        args.request_all_outputs,
                        args.dependency_mode,
                        args.dag_width,
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
            process_after = process_counters(service.pid)
            network_after = interface_counters(args.metrics_interface)
            result = b""
            if not args.no_requested_output:
                result_data_id = (
                    args.tasks + 1
                    if args.executor == "builtin" and args.registration != "sealed"
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
            service_metrics = native_runtime_metrics(
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
            durable_root = Path(f"{journal}.data")
            durable_file_count = 0
            durable_file_bytes = 0
            if durable_root.exists():
                for directory, _, names in os.walk(durable_root):
                    for name in names:
                        path = Path(directory) / name
                        durable_file_count += 1
                        durable_file_bytes += path.stat().st_size
            expected_durable_files = (
                args.tasks if args.request_all_outputs
                else 0 if args.no_requested_output else 1
            )
            if (durable_file_count != expected_durable_files or
                    durable_file_bytes != len(expected_result) * expected_durable_files):
                raise RuntimeError({
                    "durable_file_count": durable_file_count,
                    "expected_durable_files": expected_durable_files,
                    "durable_file_bytes": durable_file_bytes,
                })
            runtime_rate = args.tasks / elapsed
            if runtime_rate < args.minimum_runtime_tasks_per_second:
                raise RuntimeError(
                    f"runtime throughput {runtime_rate:.3f} below "
                    f"{args.minimum_runtime_tasks_per_second:.3f} tasks/s"
                )
            evidence = {
                "status": "PASS",
                "architecture": "native-c-single-workflow-reactor",
                "executor": args.executor,
                "command_argv": command_argv if args.executor == "command" else None,
                "command_output_name": (
                    args.command_output_name if args.executor == "command" else None
                ),
                "tasks": args.tasks,
                "physical_tasks": physical_tasks,
                "workers": args.workers,
                "cores_per_worker": args.cores,
                "memory_mib_per_worker": args.memory,
                "disk_mib_per_worker": args.disk,
                "gpus_per_worker": args.gpus,
                "batch_type": args.batch_type,
                "registration": args.registration,
                "idata_backup": args.idata_backup,
                "workflow_recovery": args.workflow_recovery,
                "scheduling_mode": "worker-first",
                "dependency_mode": args.dependency_mode,
                "dag_width": args.dag_width,
                "dependency_edges": dependency_edges(
                    args.tasks, args.dependency_mode, args.dag_width),
                "requested_output": not args.no_requested_output,
                "requested_outputs": (
                    args.tasks if args.request_all_outputs
                    else 0 if args.no_requested_output else 1
                ),
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
                "service_execution_seconds": service_metrics["run_seconds"],
                "service_runtime_tasks_per_second": (
                    args.tasks / service_metrics["run_seconds"]
                ),
                "service_metrics": service_metrics,
                "tasks_per_second": args.tasks / elapsed,
                "post_registration_tasks_per_second": (
                    args.tasks / run_seconds if run_seconds > 0 else None
                ),
                "workflow": state,
                "submitted": submitted,
                "exact_result_bytes": len(result),
                "journal_filesystem_root": str(journal_state or root),
                "durable_file_count": durable_file_count,
                "durable_file_bytes": durable_file_bytes,
                "peak": peak,
                "system_metrics": {
                    "service": counter_delta(process_before, process_after),
                    "network_interface": args.metrics_interface,
                    "network": counter_delta(network_before, network_after),
                },
                "persistence_diagnostics": args.persistence_diagnostics,
                "journal_bytes": journal.stat().st_size,
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
            if journal_state:
                shutil.rmtree(journal_state)


if __name__ == "__main__":
    main()
