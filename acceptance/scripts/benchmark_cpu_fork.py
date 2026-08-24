#!/usr/bin/env python3
"""Fair CPU-intensive comparison of DataVine and TaskVine FunctionCall-fork."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time

import cloudpickle
import ndcctools.taskvine as vine
from ndcctools.taskvine.datavine.workflow import Workflow
from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


MASK64 = (1 << 64) - 1


def cgroup_cpu_quota():
    """Return the tightest cgroup v2 CPU quota inherited by this process."""

    relative = None
    for line in Path("/proc/self/cgroup").read_text().splitlines():
        fields = line.split(":", 2)
        if len(fields) == 3 and fields[0] == "0":
            relative = fields[2].lstrip("/")
            break
    if relative is None:
        return None
    current = Path("/sys/fs/cgroup") / relative
    quotas = []
    while current != current.parent:
        control = current / "cpu.max"
        if control.is_file():
            quota, period = control.read_text().split()
            if quota != "max":
                quotas.append(int(quota) / int(period))
        if current == Path("/sys/fs/cgroup"):
            break
        current = current.parent
    return min(quotas) if quotas else None


def cpu_kernel(iterations, seed):
    """Pure-Python integer workload with a deterministic result and CPU timer."""

    started = time.process_time_ns()
    value = int(seed) & MASK64
    for ordinal in range(int(iterations)):
        value ^= (value << 13) & MASK64
        value ^= value >> 7
        value ^= (value << 17) & MASK64
        value = (value + ordinal) & MASK64
    return value, time.process_time_ns() - started


def calibrate(target_ms):
    target_ns = int(float(target_ms) * 1_000_000)
    iterations = 10_000
    for _ in range(5):
        _, elapsed = cpu_kernel(iterations, 1)
        if elapsed <= 0:
            iterations *= 10
            continue
        adjusted = max(1, round(iterations * target_ns / elapsed))
        if abs(adjusted - iterations) <= max(1, iterations // 20):
            break
        iterations = adjusted
    return iterations


def terminate_group(process, timeout=30):
    """Ask a process group to clean up, then enforce a bounded shutdown.

    A Condor vine_factory removes one submitted job at a time.  Large resident
    pools can legitimately need longer than the default service-process grace
    period, so callers that own such a factory may provide a larger timeout.
    """
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


def start_factory(root, manager_port, workers, cores, worker_binary, log_path):
    root.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log = log_path.open("w")
    command = (
        "vine_factory",
        "--batch-type", "local",
        "--min-workers", str(workers),
        "--max-workers", str(workers),
        "--workers-per-cycle", str(workers),
        "--factory-period", "1",
        "--factory-timeout", "900",
        "--timeout", "840",
        "--cores", str(cores),
        "--memory", "1024",
        "--disk", "1024",
        "--worker-binary", str(worker_binary),
        "--scratch-dir", str(root / "factory"),
        "--parent-death",
        "localhost", str(manager_port),
    )
    process = subprocess.Popen(
        command,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    return process, log, command


def worker_inventory(vine_status, port):
    completed = subprocess.run(
        (str(vine_status), "-W", "localhost", str(port)),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=10,
    )
    if completed.returncode:
        return []
    result = []
    for line in completed.stdout.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 6:
            continue
        try:
            result.append(int(float(fields[4]) + float(fields[5])))
        except ValueError:
            pass
    return result


def wait_workers(vine_status, port, workers, cores, factory, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if factory.poll() is not None:
            raise RuntimeError(f"vine_factory exited with {factory.returncode}")
        inventory = worker_inventory(vine_status, port)
        if len(inventory) == workers and all(value == cores for value in inventory):
            return inventory
        time.sleep(0.5)
    raise TimeoutError(f"did not observe exactly {workers}x{cores} workers")


def verify(values, workload):
    expected = {}
    useful_cpu_ns = 0
    for item in values:
        task_key = item["task_key"]
        state, cpu_ns = item["result"]
        if task_key not in expected:
            iterations, seed = workload[task_key]
            expected[task_key] = cpu_kernel(iterations, seed)[0]
        if state != expected[task_key] or cpu_ns <= 0:
            raise AssertionError((task_key, state, expected[task_key], cpu_ns))
        useful_cpu_ns += cpu_ns
    if len(values) != len(workload):
        raise AssertionError((len(values), len(workload)))
    return useful_cpu_ns


def run_datavine(repository, root, workload, workers, cores):
    root.mkdir(parents=True, exist_ok=True)
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    vine_status = repository / "taskvine/src/tools/vine_status"
    token = "datavine-cpu-fork-benchmark"
    service_log_path = root / "datavine.service.log"
    service_log = service_log_path.open("w")
    service = subprocess.Popen(
        (str(service_binary), "serve", str(root / "datavine.journal"), token),
        stdout=subprocess.PIPE,
        stderr=service_log,
        text=True,
        start_new_session=True,
        env=dict(
            os.environ,
            DATAVINE_WORKFLOW_METRICS="1",
            DATAVINE_RUNTIME_INFO_PATH=str(root / "datavine-run-info"),
        ),
    )
    factory = None
    factory_log = None
    try:
        line = service.stdout.readline()
        if not line:
            raise RuntimeError("DataVine service exited before contact")
        contact = json.loads(line)
        factory, factory_log, factory_command = start_factory(
            root / "datavine-factory",
            contact["manager_port"], workers, cores, worker_binary,
            root / "datavine.factory.log",
        )
        wait_workers(vine_status, contact["manager_port"], workers, cores, factory)
        client = WorkflowClient(contact["endpoint"], token)
        warmup_builder = Workflow(
            "cpu-fork-datavine-warmup-v1",
            workflow_id="cpu-fork-datavine-warmup",
            maximum_tasks=1,
            maximum_edges=0,
        )
        warmup_result = warmup_builder.python_callable(cpu_kernel, 1000, 0)
        warmup_builder.request(warmup_result)
        client.submit_workflow(warmup_builder.document())
        warmup_deadline = time.monotonic() + 180
        while True:
            warmup_state = client.describe_workflow(warmup_builder.workflow_id)
            if warmup_state["state"] in {"completed", "failed", "cancelled"}:
                break
            if time.monotonic() >= warmup_deadline:
                raise TimeoutError(warmup_state)
            time.sleep(0.05)
        if warmup_state["state"] != "completed":
            raise RuntimeError(warmup_state)
        cloudpickle.loads(client.fetch_workflow_result(
            warmup_builder.workflow_id, warmup_result.data_id
        ))
        builder = Workflow(
            "cpu-fork-datavine-v1",
            workflow_id="cpu-fork-datavine",
            maximum_tasks=len(workload),
            maximum_edges=0,
        )
        references = []
        for task_key, (iterations, seed) in workload.items():
            reference = builder.python_callable(cpu_kernel, iterations, seed)
            references.append((task_key, reference))
            builder.request(reference)
        started = time.monotonic()
        client.submit_workflow(builder.document())
        submitted = time.monotonic()
        deadline = started + 900
        while True:
            state = client.describe_workflow(builder.workflow_id)
            if state["state"] in {"completed", "failed", "cancelled"}:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(state)
            time.sleep(0.05)
        completed = time.monotonic()
        if state["state"] != "completed":
            service_log.flush()
            raise RuntimeError({
                "workflow": state,
                "service_log": service_log_path.read_text(),
            })
        values = []
        for task_key, reference in references:
            payload = client.fetch_workflow_result(builder.workflow_id, reference.data_id)
            values.append({"task_key": task_key, "result": cloudpickle.loads(payload)})
        fetched = time.monotonic()
        useful_cpu_ns = verify(values, workload)
        client.close()
        service_log.flush()
        metric_lines = [
            line for line in service_log_path.read_text().splitlines()
            if line.startswith("datavine workflow ")
        ]
        return {
            "status": "PASS",
            "architecture": "datavine-c-runtime-preloaded-python-fork",
            "task_count": len(workload),
            "submission_seconds": submitted - started,
            "execution_seconds": completed - submitted,
            "fetch_seconds": fetched - completed,
            "total_seconds": fetched - started,
            "useful_cpu_seconds": useful_cpu_ns / 1e9,
            "execution_useful_cpu_percent": 100 * useful_cpu_ns /
                (1e9 * max(1, workers * cores) * (completed - submitted)),
            "tasks_per_second": len(workload) / (completed - submitted),
            "factory_command": list(factory_command),
            "service_metrics": metric_lines,
        }
    finally:
        terminate_group(factory)
        terminate_group(service)
        if factory_log:
            factory_log.close()
        service_log.close()


def run_function_call(repository, root, workload, workers, cores):
    root.mkdir(parents=True, exist_ok=True)
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    run_info = root / "function-call-run-info"
    staging = root / "function-call-staging"
    with vine.Manager(
        port=0, shutdown=True, run_info_path=str(run_info),
        staging_path=str(staging),
    ) as manager:
        library_name = "datavine-cpu-reference"
        library = manager.create_library_from_functions(
            library_name, cpu_kernel, add_env=False, exec_mode="fork"
        )
        library.set_cores(cores)
        manager.install_library(library)
        factory = vine.Factory(
            batch_type="local", manager=manager,
            worker_binary=str(worker_binary),
            log_file=str(root / "function-call.factory.log"),
        )
        factory.min_workers = workers
        factory.max_workers = workers
        factory.workers_per_cycle = workers
        factory.cores = cores
        factory.memory = 1024
        factory.disk = 1024
        factory.timeout = 840
        factory.factory_timeout = 900
        with factory:
            warmup = vine.FunctionCall(library_name, "cpu_kernel", 1000, 0)
            warmup.set_cores(1)
            manager.submit(warmup)
            warmup = manager.wait("wait_forever")
            if not warmup or not warmup.successful():
                raise RuntimeError("FunctionCall-fork library warmup failed")
            tasks = {}
            started = time.monotonic()
            for task_key, (iterations, seed) in workload.items():
                task = vine.FunctionCall(library_name, "cpu_kernel", iterations, seed)
                task.set_cores(1)
                task.set_tag(task_key)
                tasks[manager.submit(task)] = task_key
            submitted = time.monotonic()
            values = []
            deadline = started + 900
            while tasks:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"{len(tasks)} FunctionCalls remain")
                task = manager.wait(5)
                if not task:
                    continue
                task_key = tasks.pop(task.id)
                if not task.successful():
                    raise RuntimeError((task_key, task.result, task.output))
                values.append({"task_key": task_key, "result": task.output})
            completed = time.monotonic()
            useful_cpu_ns = verify(values, workload)
            return {
                "status": "PASS",
                "architecture": "taskvine-functioncall-fork",
                "task_count": len(workload),
                "submission_seconds": submitted - started,
                "execution_seconds": completed - submitted,
                "fetch_seconds": 0,
                "total_seconds": completed - started,
                "useful_cpu_seconds": useful_cpu_ns / 1e9,
                "execution_useful_cpu_percent": 100 * useful_cpu_ns /
                    (1e9 * max(1, workers * cores) * (completed - submitted)),
                "tasks_per_second": len(workload) / (completed - submitted),
            }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--durations-ms", default="20,100,500")
    parser.add_argument("--tasks-per-duration", type=int, default=8)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--cores", type=int, default=1)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    durations = [float(value) for value in args.durations_ms.split(",")]
    if (not durations or min(durations) <= 0 or args.tasks_per_duration < 1 or
            args.workers < 1 or args.cores < 1):
        parser.error("durations, task count, workers, and cores must be positive")
    repository = Path(__file__).resolve().parents[2]
    affinity_cpu_count = len(os.sched_getaffinity(0))
    quota = cgroup_cpu_quota()
    effective_capacity = min(affinity_cpu_count, quota or affinity_cpu_count)
    if args.workers * args.cores > effective_capacity:
        parser.error(
            f"requested {args.workers * args.cores} worker cores exceed "
            f"effective local CPU capacity {effective_capacity:g}"
        )
    calibrated = {str(value): calibrate(value) for value in durations}
    workload = {}
    for duration in durations:
        for ordinal in range(args.tasks_per_duration):
            key = f"{duration:g}ms-{ordinal}"
            workload[key] = (calibrated[str(duration)], ordinal + 1)
    with tempfile.TemporaryDirectory(prefix="datavine-cpu-fork-") as root_name:
        root = Path(root_name)
        datavine = run_datavine(
            repository, root / "datavine", workload, args.workers, args.cores
        )
        function_call = run_function_call(
            repository, root / "function-call", workload, args.workers, args.cores
        )
        report = {
            "artifact_type": "datavine-cpu-fork-comparison",
            "status": "PASS",
            "host_cpu_count": os.cpu_count(),
            "affinity_cpu_count": affinity_cpu_count,
            "cgroup_cpu_quota": quota,
            "effective_cpu_capacity": effective_capacity,
            "workers": args.workers,
            "cores_per_worker": args.cores,
            "durations_ms": durations,
            "tasks_per_duration": args.tasks_per_duration,
            "iterations": calibrated,
            "datavine": datavine,
            "function_call_fork": function_call,
            "datavine_to_function_call_rate": (
                datavine["tasks_per_second"] / function_call["tasks_per_second"]
            ),
        }
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
