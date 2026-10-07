#!/usr/bin/env python3
"""Local A/B benchmark for DataVine adaptive FunctionCall concurrency."""

import argparse
import cloudpickle
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import socket
import signal
import subprocess
import tempfile
import time

from ndcctools.taskvine.datavine import Workflow
from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


METRIC_NAMES = (
    "function_rebalance_rounds",
    "function_recalls_sent",
    "function_recalls_succeeded",
    "function_recalls_missed",
    "function_window_updates",
    "function_window_min",
    "function_window_max",
    "manager_workers_removed",
)


def exercise(kind, duration, ordinal, padding=None):
    """One fork-executor work unit with a compact provenance result."""

    import os as worker_os
    import time as worker_time

    started = worker_time.monotonic()
    if kind in ("noop", "large"):
        # ``padding`` makes the serialized invocation exceed the inline-ticket
        # boundary in the large-reference transport gate.
        del padding
        pass
    elif kind == "io":
        worker_time.sleep(duration)
    elif kind == "cpu":
        deadline = worker_time.process_time() + duration
        value = ordinal + 1
        while worker_time.process_time() < deadline:
            value = (value * 6364136223846793005 + 1) & ((1 << 64) - 1)
    elif kind == "mixed":
        if ordinal % 3:
            worker_time.sleep(duration)
        else:
            deadline = worker_time.process_time() + duration
            value = ordinal + 1
            while worker_time.process_time() < deadline:
                value = (value * 2862933555777941757 + 3037000493) & ((1 << 64) - 1)
    elif kind == "short":
        worker_time.sleep(duration)
    elif kind == "disk":
        path = f"adaptive-random-io-{worker_os.getpid()}-{ordinal}.bin"
        descriptor = worker_os.open(
            path, worker_os.O_CREAT | worker_os.O_TRUNC | worker_os.O_RDWR,
            0o600,
        )
        try:
            block = bytes([(ordinal * 17 + 3) & 0xFF]) * 4096
            for index in range(64):
                offset = ((index * 37 + ordinal * 11) % 256) * 4096
                worker_os.pwrite(descriptor, block, offset)
            worker_os.fsync(descriptor)
            checksum = 0
            for index in range(64):
                offset = ((index * 53 + ordinal * 7) % 256) * 4096
                checksum ^= sum(worker_os.pread(descriptor, 4096, offset))
        finally:
            worker_os.close(descriptor)
            worker_os.unlink(path)
    elif kind == "memory":
        payload = bytearray(128 * 1024 * 1024)
        for index in range(0, len(payload), 4096):
            payload[index] = (ordinal + index) & 0xFF
        worker_time.sleep(duration)
    else:
        raise ValueError(kind)
    finished = worker_time.monotonic()
    return {
        "executor_pid": worker_os.getppid(),
        "ordinal": ordinal,
        "started": started,
        "finished": finished,
        "elapsed": finished - started,
    }


def exercise_source(kind, duration, ordinal):
    """Equivalent source executor payload for remote fork-window tests."""

    if kind == "noop":
        body = "pass"
    elif kind in ("short", "io"):
        body = f"import time\ntime.sleep({duration!r})"
    elif kind == "cpu":
        body = (
            "import time\n"
            f"deadline = time.process_time() + {duration!r}\n"
            f"value = {ordinal + 1}\n"
            "while time.process_time() < deadline:\n"
            "    value = (value * 6364136223846793005 + 1) & ((1 << 64) - 1)"
        )
    elif kind == "mixed":
        body = (
            f"import time\nkind = {ordinal % 3}\n"
            f"duration = {duration!r}\n"
            "if kind:\n"
            "    time.sleep(duration)\n"
            "else:\n"
            "    deadline = time.process_time() + duration\n"
            f"    value = {ordinal + 1}\n"
            "    while time.process_time() < deadline:\n"
            "        value = (value * 2862933555777941757 + 3037000493) & ((1 << 64) - 1)"
        )
    else:
        raise ValueError(f"source executor does not implement {kind}")
    return body


def stop(process):
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def worker_count(vine_status, port):
    completed = subprocess.run(
        (str(vine_status), "-W", "localhost", str(port)),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        timeout=5,
    )
    if completed.returncode:
        return 0
    return max(0, len(completed.stdout.splitlines()) - 1)


def wait_workers(vine_status, port, expected, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if worker_count(vine_status, port) == expected:
            return
        time.sleep(0.1)
    raise TimeoutError(f"expected {expected} workers")


def wait_terminal(client, workflow_id, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = client.describe_workflow(workflow_id)
        if info["state"] in ("completed", "failed", "cancelled"):
            return info
        time.sleep(0.05)
    raise TimeoutError(client.describe_workflow(workflow_id))


def parse_metrics(path, workflow_id):
    for line in reversed(path.read_text().splitlines()):
        if f"datavine workflow {workflow_id} " not in line:
            continue
        fields = dict(re.findall(r"([a-z_]+)=([^ ]+)", line))
        if "run_seconds" not in fields:
            continue
        result = {"run_seconds": float(fields["run_seconds"])}
        result.update({name: int(fields.get(name, 0)) for name in METRIC_NAMES})
        return result
    raise RuntimeError("workflow metrics missing")


def parse_window_trace(paths):
    trace = []
    pattern = re.compile(
        r"adaptive-window policy=(\w+) sample_ms=(\d+) previous=(\d+) "
        r"next=(\d+) running=(\d+) waiting=(\d+) runnable=(\d+) "
        r"cpu=([-0-9.]+) memory=([-0-9.]+) action=([\w-]+)"
    )
    for worker_index, path in enumerate(paths):
        for line in path.read_text(errors="replace").splitlines():
            match = pattern.search(line)
            if not match:
                continue
            trace.append({
                "worker": worker_index,
                "policy": match.group(1),
                "sample_ms": int(match.group(2)),
                "previous": int(match.group(3)),
                "window": int(match.group(4)),
                "running": int(match.group(5)),
                "waiting": int(match.group(6)),
                "runnable": int(match.group(7)),
                "cpu_fraction": float(match.group(8)),
                "memory_fraction": float(match.group(9)),
                "action": match.group(10),
            })
    return trace


def run_case(repository, case, queue_multiplier, tasks, cores, duration,
             late_worker_delay, timeout, artifacts=None, cpu_affinity="",
             batch_type="local", worker_total=1, worker_memory=2048,
             admission_timeout=600, executor_mode="callable"):
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    vine_status = repository / "taskvine/src/tools/vine_status"
    with tempfile.TemporaryDirectory(prefix="datavine-adaptive-") as root_name:
        root = Path(root_name)
        service_log_path = root / "service.log"
        service_log = service_log_path.open("w")
        environment = dict(
            os.environ,
            DATAVINE_WORKFLOW_METRICS="1",
            DATAVINE_FUNCTION_QUEUE_MULTIPLIER=str(queue_multiplier),
        )
        service = subprocess.Popen(
            (str(service_binary), "serve", str(root / "journal"), "adaptive-token"),
            stdout=subprocess.PIPE,
            stderr=service_log,
            text=True,
            env=environment,
        )
        workers = []
        factory = None
        factory_log = None
        admission_seconds = 0.0
        client = None
        try:
            contact = json.loads(service.stdout.readline())

            def launch_worker():
                index = len(workers)
                log = (root / f"worker-{index}.log").open("w")
                debug_path = root / f"worker-{index}.debug"
                worker_command = [
                    str(worker_binary), f"--cores={cores}",
                        f"--memory={worker_memory}", "--disk=4096",
                        "--idle-timeout=60",
                        "-d", "vine", "-o", str(debug_path),
                        "localhost", str(contact["manager_port"]),
                ]
                if cpu_affinity:
                    worker_command[:0] = ["taskset", "-c", cpu_affinity]
                process = subprocess.Popen(
                    worker_command,
                    stdout=subprocess.DEVNULL,
                    stderr=log,
                    text=True,
                    env=environment,
                )
                workers.append((process, log, debug_path))

            admission_started = time.monotonic()
            if batch_type == "local":
                for _ in range(worker_total):
                    launch_worker()
            else:
                factory_log = (root / "factory.log").open("w")
                factory_command = [
                    "vine_factory", "--batch-type", "condor",
                    "--min-workers", str(worker_total),
                    "--max-workers", str(worker_total),
                    "--workers-per-cycle", str(worker_total),
                    "--factory-period", "1", "--timeout", "60",
                    "--factory-timeout", str(int(admission_timeout)),
                    "--cores", str(cores), "--memory", str(worker_memory),
                    "--disk", "4096", "--gpus", "0", "--debug-workers",
                    "--scratch-dir", str(root / "factory"),
                    "--worker-binary", str(worker_binary),
                    "--env", f"PATH={os.environ['PATH']}",
                    socket.getfqdn(), str(contact["manager_port"]),
                ]
                factory = subprocess.Popen(
                    factory_command, stdout=factory_log, stderr=subprocess.STDOUT,
                    text=True, env=environment,
                )
            wait_workers(
                vine_status, contact["manager_port"], worker_total,
                admission_timeout,
            )
            admission_seconds = time.monotonic() - admission_started
            workflow_id = f"adaptive-{case}-{queue_multiplier}-{time.time_ns()}"
            workflow = Workflow(
                workflow_id, workflow_id=workflow_id, maximum_tasks=tasks,
                maximum_edges=0, idata_backup="worker-local",
            )
            resources = {"cores": 1}
            if case == "memory":
                resources["memory_mb"] = 128
            effective_case = case if case not in ("rebalance", "eviction") else "io"
            if executor_mode == "source":
                outputs = [
                    workflow.python_source(
                        exercise_source(effective_case, duration, ordinal),
                        resources=resources,
                    )
                    for ordinal in range(tasks)
                ]
            elif case == "large":
                padding = b"D" * (128 * 1024)
                outputs = [
                    workflow.python_callable(
                        exercise, effective_case, duration, ordinal, padding,
                        resources=resources,
                    )
                    for ordinal in range(tasks)
                ]
            else:
                outputs = [
                    workflow.python_callable(
                        exercise, effective_case, duration, ordinal,
                        resources=resources,
                    )
                    for ordinal in range(tasks)
                ]
            if case in ("rebalance", "eviction", "memory"):
                workflow.request(*outputs)
            client = WorkflowClient(contact["endpoint"], "adaptive-token")
            started = time.monotonic()
            submitted = workflow.submit(client)
            if case in ("rebalance", "eviction"):
                if batch_type != "local" or worker_total != 1:
                    raise ValueError(
                        "rebalance and eviction require one initial local Worker"
                    )
                time.sleep(late_worker_delay)
                launch_worker()
                wait_workers(vine_status, contact["manager_port"], 2)
                if case == "eviction":
                    time.sleep(0.5)
                    stop(workers[-1][0])
            state = wait_terminal(client, workflow_id, timeout)
            elapsed = time.monotonic() - started
            if state["state"] != "completed":
                raise RuntimeError(state)
            executor_counts = {}
            intervals = []
            if case in ("rebalance", "eviction", "memory"):
                for ordinal, output in enumerate(outputs):
                    value = cloudpickle.loads(client.fetch_workflow_result(
                        workflow_id, output.data_id
                    ))
                    if value["ordinal"] != ordinal:
                        raise RuntimeError(
                            f"result mismatch {value['ordinal']} != {ordinal}"
                        )
                    pid = str(value["executor_pid"])
                    executor_counts[pid] = executor_counts.get(pid, 0) + 1
                    intervals.append((value["started"], value["finished"]))
            active = 0
            peak_concurrency = None
            if intervals:
                peak_concurrency = 0
                events = sorted(
                    [(start, 1) for start, _ in intervals] +
                    [(finish, -1) for _, finish in intervals],
                    key=lambda item: (item[0], item[1]),
                )
                for _, delta in events:
                    active += delta
                    peak_concurrency = max(peak_concurrency, active)
            service_log.flush()
            metrics = parse_metrics(service_log_path, workflow_id)
            if factory is not None:
                stop(factory)
                factory = None
                if factory_log is not None:
                    factory_log.flush()
            window_trace = parse_window_trace(
                [debug_path for _, _, debug_path in workers] +
                list((root / "factory").glob("**/*debug"))
            )
            result = {
                "case": case,
                "queue_multiplier": queue_multiplier,
                "window_policy": "elastic",
                "sample_ms": 250,
                "cpu_affinity": cpu_affinity or None,
                "tasks": tasks,
                "cores_per_worker": cores,
                "workers": (
                    2 if case in ("rebalance", "eviction") else worker_total
                ),
                "batch_type": batch_type,
                "executor_mode": executor_mode,
                "worker_admission_seconds": admission_seconds,
                "duration_seconds": duration,
                "elapsed_seconds": elapsed,
                "tasks_per_second": tasks / elapsed,
                "executor_task_counts": executor_counts,
                "peak_concurrency": peak_concurrency,
                "window_trace": window_trace,
                "workflow_generation": submitted["generation"],
                "metrics": metrics,
            }
            if artifacts is not None:
                destination = artifacts / (
                    f"{case}-q{queue_multiplier}-{time.time_ns()}"
                )
                shutil.copytree(root, destination)
                result["artifacts"] = str(destination)
            return result
        except BaseException:
            stop(factory)
            factory = None
            if factory_log is not None:
                factory_log.flush()
            if artifacts is not None:
                destination = artifacts / f"failed-{case}-{time.time_ns()}"
                shutil.copytree(root, destination)
            raise
        finally:
            if client is not None:
                client.close()
            for process, log, _ in reversed(workers):
                stop(process)
                log.close()
            stop(factory)
            if factory_log is not None:
                factory_log.close()
            stop(service)
            service_log.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cases", default="noop,short,io,disk,memory,cpu,mixed,rebalance,eviction",
        help="comma-separated noop,short,io,disk,memory,cpu,mixed,rebalance,eviction",
    )
    parser.add_argument("--tasks", type=int, default=64)
    parser.add_argument("--cores", type=int, default=4)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--batch-type", choices=("local", "condor"), default="local")
    parser.add_argument(
        "--executor", choices=("callable", "source"), default="callable",
    )
    parser.add_argument("--worker-memory", type=int, default=2048)
    parser.add_argument("--admission-timeout", type=float, default=600)
    parser.add_argument("--duration", type=float, default=0.25)
    parser.add_argument("--rebalance-tasks", type=int, default=16)
    parser.add_argument("--rebalance-duration", type=float, default=1.0)
    parser.add_argument("--late-worker-delay", type=float, default=0.5)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--multipliers", default="1,8")
    parser.add_argument(
        "--cpu-affinity", default="",
        help="optional taskset CPU list for controlled local experiments",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path)
    args = parser.parse_args()
    cases = [item.strip() for item in args.cases.split(",") if item.strip()]
    if not cases or set(cases) - {
        "noop", "large", "short", "io", "disk", "memory", "cpu", "mixed",
        "rebalance", "eviction"
    }:
        parser.error("invalid cases")
    repository = Path(__file__).resolve().parents[2]
    if args.artifacts is not None:
        args.artifacts.mkdir(parents=True, exist_ok=True)
    results = []
    multipliers = [int(item) for item in args.multipliers.split(",") if item]
    if not multipliers or any(item < 1 or item > 64 for item in multipliers):
        parser.error("invalid multipliers")
    random.seed(1)
    if args.workers < 1 or args.worker_memory < 256:
        parser.error("invalid Worker resources")
    if args.batch_type == "condor" and args.cpu_affinity:
        parser.error("cpu-affinity is only valid for local Workers")
    if args.executor == "source" and set(cases) - {
        "noop", "short", "io", "cpu", "mixed",
    }:
        parser.error("source executor supports noop, short, io, cpu, mixed")
    order = [(case, multiplier) for case in cases for multiplier in multipliers]
    for repetition in range(args.repetitions):
        random.shuffle(order)
        for case, multiplier in order:
            tasks = args.rebalance_tasks if case in ("rebalance", "eviction") else args.tasks
            duration = (
                args.rebalance_duration if case in ("rebalance", "eviction") else
                0.005 if case == "short" else args.duration
            )
            result = run_case(
                repository, case, multiplier, tasks, args.cores, duration,
                args.late_worker_delay, args.timeout, args.artifacts,
                args.cpu_affinity,
                args.batch_type, args.workers, args.worker_memory,
                args.admission_timeout, args.executor,
            )
            result["repetition"] = repetition
            results.append(result)
            print(json.dumps(result, sort_keys=True), flush=True)
            # Condor admission can be substantially longer than the measured
            # run.  Preserve every completed trial so an interruption never
            # discards valid measurements from earlier trials.
            args.output.parent.mkdir(parents=True, exist_ok=True)
            checkpoint = args.output.with_suffix(args.output.suffix + ".part")
            checkpoint.write_text(json.dumps({
                "status": "RUNNING",
                "scope": f"{args.batch_type}-functioncall-adaptive-concurrency",
                "results": results,
            }, indent=2, sort_keys=True) + "\n")
            checkpoint.replace(args.output)
    source_diff = subprocess.run(
        ("git", "diff", "--binary", "HEAD"), cwd=repository,
        check=True, stdout=subprocess.PIPE,
    ).stdout
    git_head = subprocess.run(
        ("git", "rev-parse", "HEAD"), cwd=repository, check=True,
        text=True, stdout=subprocess.PIPE,
    ).stdout.strip()
    implementation_files = (
        "taskvine/src/datavine/vine_datavine_workflow_runtime.c",
        "taskvine/src/manager/vine_function_call.c",
        "taskvine/src/manager/vine_function_call.h",
        "taskvine/src/manager/vine_manager.c",
        "taskvine/src/manager/vine_manager.h",
        "taskvine/src/manager/vine_schedule.c",
        "taskvine/src/manager/vine_task.c",
        "taskvine/src/manager/vine_task.h",
        "taskvine/src/manager/vine_worker_pool.c",
        "taskvine/src/manager/vine_worker_pool.h",
        "taskvine/src/worker/vine_process.h",
        "taskvine/src/worker/vine_worker.c",
        "acceptance/scripts/benchmark_adaptive_concurrency.py",
    )
    implementation_digest = hashlib.sha256()
    for name in implementation_files:
        implementation_digest.update(name.encode() + b"\0")
        implementation_digest.update((repository / name).read_bytes())
    evidence = {
        "status": "PASS",
        "scope": f"{args.batch_type}-functioncall-adaptive-concurrency",
        "provenance": {
            "hostname": socket.gethostname(),
            "git_head": git_head,
            "source_diff_sha256": hashlib.sha256(source_diff).hexdigest(),
            "implementation_sha256": implementation_digest.hexdigest(),
            "datavine_workflow_sha256": hashlib.sha256(
                (repository / "taskvine/src/tools/datavine_workflow").read_bytes()
            ).hexdigest(),
            "vine_worker_sha256": hashlib.sha256(
                (repository / "taskvine/src/worker/vine_worker").read_bytes()
            ).hexdigest(),
        },
        "results": results,
    }
    for result in results:
        metrics = result["metrics"]
        if metrics["function_recalls_sent"] != (
            metrics["function_recalls_succeeded"] +
            metrics["function_recalls_missed"]
        ):
            raise RuntimeError(f"recall accounting mismatch: {result}")
        if result["case"] in ("rebalance", "eviction"):
            if sum(result["executor_task_counts"].values()) != result["tasks"]:
                raise RuntimeError(f"result count mismatch: {result}")
            if result["queue_multiplier"] > 1 and (
                metrics["function_recalls_succeeded"] < 1 or
                (result["case"] == "rebalance" and
                 len(result["executor_task_counts"]) < 2)
            ):
                raise RuntimeError(f"adaptive rebalance did not occur: {result}")
            if result["queue_multiplier"] == 1 and metrics[
                "function_recalls_sent"
            ]:
                raise RuntimeError(f"baseline unexpectedly recalled: {result}")
            if result["case"] == "eviction" and metrics[
                "manager_workers_removed"
            ] < 1:
                raise RuntimeError(f"Worker eviction was not observed: {result}")
        if result["case"] == "memory" and (
            sum(result["executor_task_counts"].values()) != result["tasks"] or
            result["peak_concurrency"] is None or
            result["peak_concurrency"] > 12
        ):
            raise RuntimeError(f"memory admission boundary failed: {result}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
