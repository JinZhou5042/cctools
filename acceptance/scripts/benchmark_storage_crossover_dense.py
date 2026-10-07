#!/usr/bin/env python3
"""Dense multi-phase Controller or SharedFS file-size crossover benchmark."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import tempfile
import time

import ndcctools.taskvine as vine

from evidence_layout import log_path


def load_helper(repository, name, relative):
    spec = importlib.util.spec_from_file_location(name, repository / relative)
    if spec is None or spec.loader is None:
        raise ImportError(relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_sizes(value):
    sizes = []
    for item in value.split(","):
        size = int(item.strip())
        if size < 0:
            raise argparse.ArgumentTypeError("sizes cannot be negative")
        sizes.append(size)
    if not sizes:
        raise argparse.ArgumentTypeError("at least one size is required")
    return sizes


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("controller", "sharedfs"), required=True)
    parser.add_argument("--sizes-kib", type=parse_sizes, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--files-per-phase", type=int, default=8192)
    parser.add_argument("--seed", type=int, default=20260902)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--cores", type=int, default=16)
    parser.add_argument("--memory", type=int, default=3072)
    parser.add_argument("--disk", type=int, default=10240)
    parser.add_argument("--batch-type", choices=("local", "condor"), default="condor")
    parser.add_argument("--shared-root", type=Path)
    parser.add_argument("--network-interface", default="enp11s0f0")
    parser.add_argument("--worker-timeout", type=int, default=1800)
    parser.add_argument("--phase-timeout", type=int, default=900)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(
        args.rounds, args.files_per_phase, args.workers, args.cores,
        args.memory, args.disk, args.worker_timeout, args.phase_timeout,
    ) < 1:
        parser.error("counts, resources and timeouts must be positive")
    if args.mode == "sharedfs" and args.shared_root is None:
        parser.error("--shared-root is required in sharedfs mode")
    return args


def phase_plan(sizes, rounds, seed):
    rng = random.Random(seed)
    plan = []
    for round_index in range(1, rounds + 1):
        order = list(sizes)
        rng.shuffle(order)
        for size_kib in order:
            plan.append({"round": round_index, "size_kib": size_kib})
    return plan


def scan_durable_phase(root, start_id, stop_id):
    count = total = 0
    if not root.exists():
        return count, total
    for directory, _, names in os.walk(root):
        for name in names:
            if ".part." in name:
                continue
            try:
                data_id = int(name.split(".", 1)[0])
            except ValueError:
                continue
            if data_id < start_id or data_id >= stop_id:
                continue
            path = Path(directory) / name
            count += 1
            total += path.stat().st_size
    return count, total


def aggregate_physical_counts(native, service_log_path, workflow_id):
    submissions = completions = 0
    epochs = 0
    for line in service_log_path.read_text().splitlines():
        if f"datavine workflow {workflow_id} " not in line:
            continue
        match = native.PHYSICAL_COUNTS.search(line)
        if not match:
            continue
        submitted = int(match.group("submitted"))
        completed = int(match.group("completed"))
        submissions += submitted
        completions += completed
        if submitted or completed:
            epochs += 1
    return {
        "submissions": submissions,
        "completions": completions,
        "nonempty_epochs": epochs,
    }


def wait_controller_phase(client, workflow_id, generation, durable_root,
                          start_id, stop_id, expected_bytes, timeout):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        state = client.describe_workflow(workflow_id)
        files, payload_bytes = scan_durable_phase(
            durable_root, start_id, stop_id
        )
        last = {"workflow": state, "files": files, "bytes": payload_bytes}
        if state["state"] == "failed":
            raise RuntimeError(last)
        if (
            state["generation"] == generation
            and state["state"] == "open_quiescent"
            and files == stop_id - start_id
            and payload_bytes == expected_bytes
        ):
            return state
        time.sleep(0.1)
    raise TimeoutError(last)


def run_controller(args, repository, native, plan):
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    vine_status = repository / "taskvine/src/tools/vine_status"
    output = args.output.resolve()
    service_log_path = log_path(output, "service")
    factory_log_path = log_path(output, "factory")
    token = "dense-crossover-token"
    total_tasks = len(plan) * args.files_per_phase
    with tempfile.TemporaryDirectory(prefix="datavine-dense-controller-") as name:
        root = Path(name)
        journal = root / "journal"
        service_log = service_log_path.open("w")
        factory_log = factory_log_path.open("w")
        service = subprocess.Popen(
            (str(service_binary), "serve", str(journal), token),
            stdout=subprocess.PIPE,
            stderr=service_log,
            text=True,
            start_new_session=True,
            env=dict(
                os.environ,
                DATAVINE_WORKFLOW_METRICS="1",
                DATAVINE_RUNTIME_INFO_PATH=str(root / "run-info"),
            ),
        )
        factory = None
        client = None
        try:
            contact = json.loads(service.stdout.readline())
            manager_host = "localhost" if args.batch_type == "local" else socket.getfqdn()
            factory_command = (
                str(repository / "batch_job/src/vine_factory"),
                "--batch-type", args.batch_type,
                "--min-workers", str(args.workers),
                "--max-workers", str(args.workers),
                "--workers-per-cycle", str(args.workers),
                "--factory-period", "1",
                "--factory-timeout", str(args.phase_timeout * len(plan) + 300),
                "--cores", str(args.cores),
                "--timeout", str(args.phase_timeout * len(plan) + 240),
                "--memory", str(args.memory),
                "--disk", str(args.disk),
                "--gpus", "0",
                "--worker-binary", str(repository / "taskvine/src/worker/vine_worker"),
                "--scratch-dir", str(root / "factory"),
                "--parent-death", manager_host, str(contact["manager_port"]),
            )
            factory = subprocess.Popen(
                factory_command,
                stdout=factory_log,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            admission_started = time.monotonic()
            inventory = native.wait_workers(
                vine_status, contact["manager_port"], args.workers, args.cores,
                factory, args.worker_timeout,
            )
            admission_seconds = time.monotonic() - admission_started
            workflow_id = f"dense-crossover-{os.getpid()}"
            initial = {
                "schema": "datavine.workflow/v1",
                "workflow_id": workflow_id,
                "idempotency_key": f"{workflow_id}-initial",
                "mode": "streaming",
                "tasks": [],
                "data": [],
                "requested_outputs": [],
                "policy": {"maximum_tasks": total_tasks, "maximum_edges": 0},
            }
            client = native.WorkflowClient(contact["endpoint"], token)
            submitted = client.submit_workflow(initial)
            generation = submitted["generation"]
            durable_root = Path(f"{journal}.data")
            next_id = 1
            expected_files = expected_bytes = 0
            phases = []
            for phase_index, phase in enumerate(plan, 1):
                size_bytes = phase["size_kib"] * 1024
                start_id = next_id
                stop_id = start_id + args.files_per_phase
                command = [
                    "/bin/sh", "-c",
                    f"head -c {size_bytes} /dev/zero > output",
                ]
                records = [
                    [task_id, [], [task_id]]
                    for task_id in range(start_id, stop_id)
                ]
                data = [
                    [task_id, task_id, 0]
                    for task_id in range(start_id, stop_id)
                ]
                delta = {
                    "schema": "datavine.workflow-delta/v1",
                    "workflow_id": workflow_id,
                    "idempotency_key": f"{workflow_id}-phase-{phase_index}",
                    "task_defaults": {
                        "executor": {
                            "kind": "command",
                            "version": "1",
                            "argv": command,
                            "output_files": ["output"],
                        }
                    },
                    "data_defaults": {"codec": {"name": "bytes", "version": "1"}},
                    "tasks": records,
                    "data": data,
                    "requested_outputs": list(range(start_id, stop_id)),
                }
                encoded = json.dumps(
                    delta, sort_keys=True, separators=(",", ":")
                ).encode()
                network_before = native.interface_counters(args.network_interface)
                process_before = native.process_counters(service.pid)
                started = time.monotonic()
                appended = client.append_workflow(workflow_id, generation, encoded)
                append_seconds = time.monotonic() - started
                generation = appended["generation"]
                expected_files += args.files_per_phase
                expected_bytes += args.files_per_phase * size_bytes
                wait_controller_phase(
                    client, workflow_id, generation, durable_root,
                    start_id, stop_id,
                    args.files_per_phase * size_bytes, args.phase_timeout,
                )
                elapsed = time.monotonic() - started
                process_after = native.process_counters(service.pid)
                network_after = native.interface_counters(args.network_interface)
                sample = client.fetch_workflow_result(workflow_id, stop_id - 1)
                if len(sample) != size_bytes:
                    raise RuntimeError("Controller result length mismatch")
                phases.append({
                    **phase,
                    "phase_index": phase_index,
                    "files": args.files_per_phase,
                    "bytes_per_file": size_bytes,
                    "total_bytes": args.files_per_phase * size_bytes,
                    "append_seconds": append_seconds,
                    "execution_seconds": elapsed,
                    "files_per_second": args.files_per_phase / elapsed,
                    "mib_per_second": (
                        args.files_per_phase * size_bytes / elapsed / 1048576
                    ),
                    "frontend_network": native.counter_delta(
                        network_before, network_after
                    ),
                    "controller_process": native.counter_delta(
                        process_before, process_after
                    ),
                    "cumulative_durable_files": expected_files,
                    "cumulative_durable_bytes": expected_bytes,
                })
                print(json.dumps(phases[-1], sort_keys=True), flush=True)
                next_id = stop_id
            sealed = client.seal_workflow(workflow_id, generation)
            deadline = time.monotonic() + args.phase_timeout
            while time.monotonic() < deadline:
                final = client.describe_workflow(workflow_id)
                if final["state"] in ("completed", "failed"):
                    break
                time.sleep(0.1)
            else:
                raise TimeoutError("workflow did not seal")
            if final["state"] != "completed":
                raise RuntimeError(final)
            service_log.flush()
            physical = aggregate_physical_counts(
                native, service_log_path, workflow_id
            )
            expected_physical = {
                "submissions": total_tasks,
                "completions": total_tasks,
                "nonempty_epochs": len(plan),
            }
            if physical != expected_physical:
                raise RuntimeError({"physical": physical, "expected": expected_physical})
            result = {
                "status": "PASS",
                "mode": "controller",
                "plan_seed": args.seed,
                "workers": args.workers,
                "cores_per_worker": args.cores,
                "available_cores": args.workers * args.cores,
                "controller_data_threads": 16,
                "files_per_phase": args.files_per_phase,
                "rounds": args.rounds,
                "sizes_kib": args.sizes_kib,
                "phase_count": len(phases),
                "admission_seconds": admission_seconds,
                "worker_inventory": len(inventory),
                "network_interface": args.network_interface,
                "physical_tasks": physical,
                "durable_files": expected_files,
                "durable_bytes": expected_bytes,
                "factory_command": list(factory_command),
                "phases": phases,
                "workflow": final,
                "seal_response": sealed,
            }
            output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
            return result
        finally:
            if client is not None:
                client.close()
            native.terminate_group(factory)
            native.terminate_group(service)
            factory_log.close()
            service_log.close()


def run_sharedfs(args, repository, direct, plan):
    output = args.output.resolve()
    shared_parent = args.shared_root.resolve()
    shared_parent.mkdir(parents=True, exist_ok=True)
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    writer_source = repository / "acceptance/helpers/storage_file_writer.c"
    factory_log = log_path(output, "factory").open("w")
    factory = None
    manager = None
    with tempfile.TemporaryDirectory(prefix="datavine-dense-sharedfs-") as name:
        state = Path(name)
        writer = state / "storage_file_writer"
        subprocess.run(
            ("cc", "-O2", "-Wall", "-Wextra", "-Werror", "-o", str(writer),
             str(writer_source)),
            check=True,
        )
        previous = Path.cwd()
        os.chdir(state)
        try:
            manager = vine.Manager(port=0)
        finally:
            os.chdir(previous)
        manager.set_name(f"datavine-dense-sharedfs-{os.getpid()}")
        manager.tune("attempt-schedule-depth", args.workers * args.cores)
        writer_file = manager.declare_file(str(writer), cache=True)
        manager_host = "localhost" if args.batch_type == "local" else socket.getfqdn()
        factory_command = (
            str(repository / "batch_job/src/vine_factory"),
            "--batch-type", args.batch_type,
            "--min-workers", str(args.workers),
            "--max-workers", str(args.workers),
            "--workers-per-cycle", str(args.workers),
            "--factory-period", "1",
            "--factory-timeout", str(args.phase_timeout * len(plan) + 300),
            "--timeout", str(args.phase_timeout * len(plan) + 240),
            "--cores", str(args.cores),
            "--memory", str(args.memory),
            "--disk", str(args.disk),
            "--gpus", "0",
            "--worker-binary", str(worker_binary),
            "--scratch-dir", str(state / "factory"),
            "--parent-death", manager_host, str(manager.port),
        )
        try:
            factory = subprocess.Popen(
                factory_command,
                stdout=factory_log,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            admission_started = time.monotonic()
            deadline = admission_started + args.worker_timeout
            while time.monotonic() < deadline:
                if factory.poll() is not None:
                    raise RuntimeError(f"vine_factory exited with {factory.returncode}")
                manager.wait(1)
                if (
                    int(manager.stats.workers_connected) == args.workers
                    and int(manager.stats.total_cores) == args.workers * args.cores
                ):
                    break
            else:
                raise TimeoutError("exact Worker topology was not admitted")
            admission_seconds = time.monotonic() - admission_started
            phases = []
            for phase_index, phase in enumerate(plan, 1):
                size_bytes = phase["size_kib"] * 1024
                run_root = shared_parent / (
                    f"run-{os.getpid()}-{phase_index}-{size_bytes}"
                )
                run_root.mkdir()
                submit_started = time.monotonic()
                for item in range(args.files_per_phase):
                    task = vine.Task(
                        f"./storage_file_writer {run_root} {size_bytes} {item}"
                    )
                    task.set_cores(1)
                    task.set_tag(f"{phase_index}:{item}")
                    task.add_input(writer_file, "storage_file_writer")
                    manager.submit(task)
                submit_seconds = time.monotonic() - submit_started
                started = time.monotonic()
                completed = failed = 0
                peak_running = peak_cores = 0
                deadline = started + args.phase_timeout
                while completed < args.files_per_phase:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"phase {phase_index} incomplete")
                    task = manager.wait(1)
                    peak_running = max(peak_running, int(manager.stats.tasks_running))
                    peak_cores = max(peak_cores, int(manager.stats.committed_cores))
                    if task is None:
                        continue
                    completed += 1
                    if not task.successful():
                        failed += 1
                elapsed = time.monotonic() - started
                files = list(run_root.glob("data.*"))
                payload_bytes = sum(path.stat().st_size for path in files)
                expected_bytes = args.files_per_phase * size_bytes
                if failed or len(files) != args.files_per_phase or payload_bytes != expected_bytes:
                    raise RuntimeError({
                        "phase": phase_index,
                        "failed": failed,
                        "files": len(files),
                        "bytes": payload_bytes,
                        "expected_bytes": expected_bytes,
                    })
                phases.append({
                    **phase,
                    "phase_index": phase_index,
                    "files": args.files_per_phase,
                    "bytes_per_file": size_bytes,
                    "total_bytes": payload_bytes,
                    "submission_seconds": submit_seconds,
                    "execution_seconds": elapsed,
                    "files_per_second": args.files_per_phase / elapsed,
                    "mib_per_second": payload_bytes / elapsed / 1048576,
                    "peak_running_tasks": peak_running,
                    "peak_committed_cores": peak_cores,
                    "failed_tasks": failed,
                })
                print(json.dumps(phases[-1], sort_keys=True), flush=True)
                shutil.rmtree(run_root)
            result = {
                "status": "PASS",
                "mode": "sharedfs",
                "plan_seed": args.seed,
                "workers": args.workers,
                "cores_per_worker": args.cores,
                "available_cores": args.workers * args.cores,
                "files_per_phase": args.files_per_phase,
                "rounds": args.rounds,
                "sizes_kib": args.sizes_kib,
                "phase_count": len(phases),
                "admission_seconds": admission_seconds,
                "software_concurrency_limit": None,
                "durability": "per-file fsync then atomic rename; no directory fsync",
                "factory_command": list(factory_command),
                "phases": phases,
            }
            output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
            return result
        finally:
            direct.stop_group(factory)
            factory_log.close()
            # Finalize the Manager while its temporary monitor directory still
            # exists; relying on interpreter teardown emits a false ENOENT
            # warning after TemporaryDirectory cleanup.
            if manager is not None:
                manager._free()
            manager = None


def main():
    args = parse_args()
    repository = Path(__file__).resolve().parents[2]
    args.output = args.output.resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    plan = phase_plan(args.sizes_kib, args.rounds, args.seed)
    native = load_helper(
        repository, "dense_native_helper",
        "acceptance/scripts/benchmark_native_workflow.py",
    )
    direct = load_helper(
        repository, "dense_sharedfs_helper",
        "acceptance/scripts/benchmark_sharedfs_direct.py",
    )
    result = (
        run_controller(args, repository, native, plan)
        if args.mode == "controller"
        else run_sharedfs(args, repository, direct, plan)
    )
    print(json.dumps({
        "status": result["status"],
        "mode": result["mode"],
        "phase_count": result["phase_count"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
