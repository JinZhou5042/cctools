#!/usr/bin/env python3
"""Fair workflow-shape comparison of DataVine and TaskVine FunctionCall-fork."""

import argparse
import contextlib
import ctypes
import gc
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import platform
import re
import socket
import statistics
import subprocess
import sys
import time

import cloudpickle


POOL_LIFETIME_SECONDS = 24 * 60 * 60
WORKER_IDLE_SECONDS = POOL_LIFETIME_SECONDS - 60
import ndcctools.taskvine as vine
from ndcctools.taskvine.datavine.workflow import Workflow, WorkflowSession
from ndcctools.taskvine.datavine.workflow_client import WorkflowClient

from benchmark_cpu_fork import (
    calibrate,
    cgroup_cpu_quota,
    terminate_group,
    wait_workers,
)


MASK64 = (1 << 64) - 1
PHYSICAL_COUNTS = re.compile(
    r"physical_submissions=(?P<submitted>[0-9]+) "
    r"physical_completions=(?P<completed>[0-9]+)"
)
RUNTIME_METRIC = re.compile(
    r"\b(?P<name>[a-z_]+)=(?P<value>[0-9]+(?:\.[0-9]+)?)"
)


def work_unit(task_key, iterations, seed, output_bytes, *parents):
    """Deterministic CPU and data kernel shared verbatim by both backends."""

    started = time.process_time_ns()
    state = int(seed) & MASK64
    cpu_by_task = {}
    parent_digests = []
    for parent in parents:
        if not isinstance(parent, dict):
            raise TypeError(f"{task_key}: parent is not a workflow value")
        parent_digests.append(parent["digest"])
        state ^= int(parent["digest"][:16], 16)
        cpu_by_task.update(parent["cpu_by_task"])
    for ordinal in range(int(iterations)):
        state ^= (state << 13) & MASK64
        state ^= state >> 7
        state ^= (state << 17) & MASK64
        state = (state + ordinal) & MASK64
    digest = hashlib.sha256(
        ("|".join(parent_digests) + f"|{task_key}|{state}").encode()
    ).hexdigest()
    size = int(output_bytes)
    payload = (bytes.fromhex(digest) * ((size + 31) // 32))[:size]
    cpu_by_task[str(task_key)] = time.process_time_ns() - started
    return {
        "digest": digest,
        "payload": payload,
        "cpu_by_task": cpu_by_task,
    }


def timed_work_unit(task_key, target_cpu_ms, seed, output_bytes, *parents):
    """Deterministic result with an in-worker, process-CPU-time workload.

    Unlike the historical iteration calibration, this makes the requested CPU
    duration independent of submit-host and worker clock speed.  A zero target
    does not enter the CPU loop and is the campaign's immediate-return control.
    The result digest deliberately excludes the realized loop count so equal
    semantics remain byte-verifiable across different worker processors.
    """

    target_ns = max(0, round(float(target_cpu_ms) * 1_000_000))
    state = int(seed) & MASK64
    cpu_by_task = {}
    parent_digests = []
    for parent in parents:
        if not isinstance(parent, dict):
            raise TypeError(f"{task_key}: parent is not a workflow value")
        parent_digests.append(parent["digest"])
        state ^= int(parent["digest"][:16], 16)
        cpu_by_task.update(parent["cpu_by_task"])
    loop_started = time.process_time_ns()
    ordinal = 0
    while time.process_time_ns() - loop_started < target_ns:
        state ^= (state << 13) & MASK64
        state ^= state >> 7
        state ^= (state << 17) & MASK64
        state = (state + ordinal) & MASK64
        ordinal += 1
    kernel_cpu_ns = time.process_time_ns() - loop_started
    # The requested duration changes cost, not scientific content.
    digest = hashlib.sha256(
        ("|".join(parent_digests) + f"|{task_key}|{int(seed) & MASK64}").encode()
    ).hexdigest()
    size = int(output_bytes)
    payload = (bytes.fromhex(digest) * ((size + 31) // 32))[:size]
    # This field is the controlled useful-work interval only.  Parent decode,
    # digest construction and payload allocation are backend overhead and must
    # remain visible in wall time, not contaminate the CPU-equality gate.
    cpu_by_task[str(task_key)] = kernel_cpu_ns
    return {
        "digest": digest,
        "payload": payload,
        "cpu_by_task": cpu_by_task,
        "timed_iterations": ordinal,
        "target_cpu_ms": float(target_cpu_ms),
    }


def split_unit(task_key, iterations, seed, output_bytes, *parents):
    return tuple(
        work_unit(
            f"{task_key}:{index}", iterations, int(seed) + index,
            output_bytes, *parents
        )
        for index in range(3)
    )


def select_unit(index, task_key, iterations, seed, output_bytes, value):
    selected = value[int(index)] if isinstance(value, tuple) else value
    return work_unit(task_key, iterations, seed, output_bytes, selected)


def canonical(values):
    cpu_by_task = {}
    result = []
    for value in values:
        cpu_by_task.update(value["cpu_by_task"])
        result.append({
            "digest": value["digest"],
            "payload_bytes": len(value["payload"]),
            "payload_sha256": hashlib.sha256(value["payload"]).hexdigest(),
        })
    return result, cpu_by_task


def release_process_heap():
    """Collect unreachable objects and return free glibc arenas to the OS."""
    page_size = os.sysconf("SC_PAGE_SIZE")
    rss_before = int(Path("/proc/self/statm").read_text().split()[1]) * page_size
    collected = gc.collect()
    trimmed = False
    try:
        malloc_trim = ctypes.CDLL(None).malloc_trim
        malloc_trim.argtypes = [ctypes.c_size_t]
        malloc_trim.restype = ctypes.c_int
        trimmed = bool(malloc_trim(0))
    except (AttributeError, OSError):
        pass
    rss_after = int(Path("/proc/self/statm").read_text().split()[1]) * page_size
    return {
        "objects_collected": collected,
        "malloc_trim": trimmed,
        "rss_before_bytes": rss_before,
        "rss_after_bytes": rss_after,
    }


def node(key, duration_ms, *, parents=(), output_bytes=64, seed=1,
         kind="work", index=None):
    value = {
        "key": str(key),
        "duration_ms": float(duration_ms),
        "parents": list(parents),
        "output_bytes": int(output_bytes),
        "seed": int(seed),
        "kind": kind,
    }
    if index is not None:
        value["index"] = int(index)
    return value


def workflow_specs(parallelism=1):
    parallelism = max(1, int(parallelism))
    specs = {}

    map_width = max(16, 4 * parallelism)
    nodes = [node(f"map-{i}", 20, seed=i + 1) for i in range(map_width)]
    specs["map"] = {"nodes": nodes, "sinks": [item["key"] for item in nodes]}

    medium_width = max(16, 2 * parallelism)
    for duration, name in ((100, "map_100ms"), (500, "map_500ms")):
        nodes = [node(f"{name}-{i}", duration, seed=i + 1001)
                 for i in range(medium_width)]
        specs[name] = {
            "nodes": nodes,
            "sinks": [item["key"] for item in nodes],
            "cross_backend_only": True,
        }

    nodes = []
    for i in range(8):
        nodes.append(node(f"chain-{i}", 20,
                          parents=(() if i == 0 else (f"chain-{i - 1}",)),
                          seed=i + 11))
    specs["chain"] = {"nodes": nodes, "sinks": ["chain-7"]}

    nodes = [node("fanout-root", 20, output_bytes=4096)]
    fan_width = max(12, 2 * parallelism)
    nodes.extend(node(f"fanout-{i}", 20, parents=("fanout-root",), seed=i + 21)
                 for i in range(fan_width))
    specs["fanout"] = {
        "nodes": nodes,
        "sinks": [f"fanout-{i}" for i in range(fan_width)],
    }

    nodes = [node(f"fanin-{i}", 20, seed=i + 31) for i in range(fan_width)]
    nodes.append(node("fanin-merge", 20,
                      parents=tuple(f"fanin-{i}" for i in range(fan_width)), seed=99))
    specs["fanin"] = {"nodes": nodes, "sinks": ["fanin-merge"]}

    nodes = [node("diamond-root", 20, output_bytes=4096)]
    diamond_width = max(8, parallelism)
    nodes.extend(node(f"diamond-{i}", 20, parents=("diamond-root",), seed=i + 41)
                 for i in range(diamond_width))
    nodes.append(node("diamond-merge", 20,
                      parents=tuple(f"diamond-{i}" for i in range(diamond_width)),
                      seed=199))
    specs["diamond"] = {"nodes": nodes, "sinks": ["diamond-merge"]}

    nodes = []
    pipeline_width = max(4, parallelism)
    for stage in range(3):
        for lane in range(pipeline_width):
            parents = () if stage == 0 else (
                f"pipeline-{stage - 1}-{lane}",
                f"pipeline-{stage - 1}-{(lane + 1) % pipeline_width}",
            )
            nodes.append(node(f"pipeline-{stage}-{lane}", 20,
                              parents=parents, seed=stage * 10 + lane + 51))
    specs["pipeline"] = {
        "nodes": nodes,
        "sinks": [f"pipeline-2-{lane}" for lane in range(pipeline_width)],
    }

    duration_pattern = (5, 5, 10, 10, 20, 20, 20, 20, 40, 40, 80, 160)
    heavy_width = max(12, 2 * parallelism)
    durations = tuple(
        duration_pattern[index % len(duration_pattern)]
        for index in range(heavy_width)
    )
    nodes = [node(f"heavy-{i}", duration, seed=i + 61)
             for i, duration in enumerate(durations)]
    specs["heavy_tail"] = {
        "nodes": nodes,
        "sinks": [item["key"] for item in nodes],
    }

    nodes = [node("reuse-root", 10, output_bytes=2 * 1024 * 1024, seed=71)]
    reuse_width = max(8, 2 * parallelism)
    nodes.extend(node(f"reuse-{i}", 20, parents=("reuse-root",), seed=i + 72)
                 for i in range(reuse_width))
    specs["large_reuse"] = {
        "nodes": nodes,
        "sinks": [f"reuse-{i}" for i in range(reuse_width)],
    }

    nodes = [node("reuse32-root", 20, output_bytes=32 * 1024 * 1024, seed=701)]
    reuse32_width = max(16, parallelism)
    nodes.extend(node(f"reuse32-{i}", 100, parents=("reuse32-root",),
                      seed=i + 702) for i in range(reuse32_width))
    specs["large_reuse_32mb"] = {
        "nodes": nodes,
        "sinks": [f"reuse32-{i}" for i in range(reuse32_width)],
        "cross_backend_only": True,
    }

    nodes = [node("split", 20, output_bytes=512 * 1024, seed=81, kind="split")]
    nodes.extend((
        node("select-0", 20, parents=("split",), seed=82,
             kind="select", index=0),
        node("select-2", 20, parents=("split",), seed=83,
             kind="select", index=2),
    ))
    specs["selective_multi_output"] = {
        "nodes": nodes,
        "sinks": ["select-0", "select-2"],
    }

    nodes = []
    sinks = []
    for group in range(parallelism):
        split_key = f"wide-split-{group}"
        nodes.append(node(split_key, 20, output_bytes=512 * 1024,
                          seed=group + 801, kind="split"))
        for index in (0, 2):
            key = f"wide-select-{group}-{index}"
            nodes.append(node(key, 20, parents=(split_key,),
                              seed=group * 3 + index + 802,
                              kind="select", index=index))
            sinks.append(key)
    specs["selective_multi_output_wide"] = {
        "nodes": nodes,
        "sinks": sinks,
        "cross_backend_only": True,
    }

    specs["dynamic"] = {
        "mode": "dynamic",
        "steps": 8,
        "duration_ms": 20,
        "output_bytes": 64,
    }
    return specs


def materialize_iterations(spec, calibrated):
    result = json.loads(json.dumps(spec))
    for item in result.get("nodes", ()):
        item["iterations"] = calibrated[str(float(item.pop("duration_ms")))]
    if result.get("mode") == "dynamic":
        result["iterations"] = calibrated[
            str(float(result.pop("duration_ms")))
        ]
    return result


def execute_locally(spec):
    values = {}
    for item in spec["nodes"]:
        parents = [values[key] for key in item["parents"]]
        if item["kind"] == "split":
            value = split_unit(item["key"], item["iterations"], item["seed"],
                               item["output_bytes"], *parents)
        elif item["kind"] == "select":
            value = select_unit(item["index"], item["key"], item["iterations"],
                                item["seed"], item["output_bytes"], parents[0])
        elif item["kind"] == "timed":
            value = timed_work_unit(
                item["key"], item["iterations"], item["seed"],
                item["output_bytes"], *parents
            )
        else:
            value = work_unit(item["key"], item["iterations"], item["seed"],
                              item["output_bytes"], *parents)
        values[item["key"]] = value
    selected = [values[key] for key in spec["sinks"]]
    return canonical(selected)[0]


def process_tree(root_pids):
    """Read only the measured roots' kernel-maintained child lists."""

    pids = set()
    pending = [int(pid) for pid in root_pids if pid]
    while pending:
        pid = pending.pop()
        if pid in pids:
            continue
        pids.add(pid)
        try:
            task_ids = os.listdir(f"/proc/{pid}/task")
        except OSError:
            continue
        for task_id in task_ids:
            try:
                children = Path(
                    f"/proc/{pid}/task/{task_id}/children"
                ).read_text().split()
            except OSError:
                continue
            pending.extend(int(child) for child in children)
    return pids


def sample_processes(root_pids):
    rss = cpu_ticks = fds = 0
    pids = process_tree(root_pids)
    for pid in pids:
        try:
            status = Path(f"/proc/{pid}/status").read_text().splitlines()
            rss += int(next(line for line in status if line.startswith("VmRSS:"))
                       .split()[1]) * 1024
            fields = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()
            cpu_ticks += int(fields[11]) + int(fields[12])
            fds += len(list(Path(f"/proc/{pid}/fd").iterdir()))
        except (OSError, StopIteration, IndexError, ValueError):
            pass
    return {"processes": len(pids), "rss_bytes": rss, "fds": fds,
            "cpu_ticks": cpu_ticks}


def _peak_sampler_process(root_pids, interval, stop, connection):
    peak = sample_processes(root_pids)
    samples = 1
    sample_seconds = 0.0
    max_sample_seconds = 0.0
    connection.send(("ready", None))
    while not stop.wait(interval):
        started = time.monotonic()
        current = sample_processes(root_pids)
        elapsed = time.monotonic() - started
        samples += 1
        sample_seconds += elapsed
        max_sample_seconds = max(max_sample_seconds, elapsed)
        peak = {key: max(peak[key], current[key]) for key in peak}
    current = sample_processes(root_pids)
    peak = {key: max(peak[key], current[key]) for key in peak}
    peak.update({
        "sampler_samples": samples,
        "sampler_seconds": sample_seconds,
        "sampler_max_seconds": max_sample_seconds,
    })
    connection.send(("result", peak))
    connection.close()


class PeakSampler:
    """Sample resource peaks out of process so client GIL timing stays clean."""

    def __init__(self, root_pids, interval=0.2):
        self.root_pids = tuple(root_pids)
        self.interval = float(interval)
        context = multiprocessing.get_context("spawn")
        self._stop = context.Event()
        self._parent, child = context.Pipe(duplex=False)
        self._process = context.Process(
            target=_peak_sampler_process,
            args=(self.root_pids, self.interval, self._stop, child),
            daemon=True,
        )
        self._result = None

    def start(self):
        self._process.start()
        if not self._parent.poll(30):
            self._process.terminate()
            self._process.join(5)
            raise RuntimeError("peak sampler failed to start")
        kind, _ = self._parent.recv()
        if kind != "ready":
            raise RuntimeError("peak sampler sent an invalid ready message")
        return self

    def stop(self):
        if self._result is not None:
            return dict(self._result)
        self._stop.set()
        if not self._parent.poll(30):
            self._process.terminate()
            self._process.join(5)
            raise RuntimeError("peak sampler failed to stop")
        kind, peak = self._parent.recv()
        self._process.join(5)
        self._parent.close()
        if kind != "result" or self._process.exitcode:
            raise RuntimeError("peak sampler exited without a valid result")
        self._result = peak
        return dict(self._result)


def taskvine_stats(manager):
    manager._refresh_stats()
    stats = manager.stats
    names = (
        "tasks_submitted", "tasks_done", "tasks_successful", "tasks_failed",
        "tasks_exhausted_attempts",
        "bytes_sent", "bytes_received", "time_workers_execute_good",
        "time_send_good", "time_receive_good", "time_scheduling",
        "workers_joined", "workers_removed", "workers_lost",
    )
    return {name: int(getattr(stats, name)) for name in names}


def difference(after, before):
    return {key: after[key] - before[key] for key in before}


def wait_taskvine_pool(manager, factory, workers, cores, timeout=3600):
    """Drive the Python Manager until the exact resident pool is connected."""

    deadline = time.monotonic() + timeout
    expected_cores = workers * cores
    while time.monotonic() < deadline:
        if factory._factory_proc.poll() is not None:
            raise RuntimeError(
                f"vine_factory exited with {factory._factory_proc.returncode}"
            )
        manager.wait(1)
        manager._refresh_stats()
        stats = manager.stats
        if (int(stats.workers_connected) == workers and
                int(stats.total_cores) == expected_cores):
            return [cores] * workers
    raise TimeoutError(
        f"did not observe exactly {workers} workers and {expected_cores} cores"
    )


def start_resident_factory(root, manager_port, workers, cores, worker_binary,
                           log_path, batch_type, memory_mib=2048):
    root.mkdir(parents=True, exist_ok=True)
    log = log_path.open("w")
    manager_host = "localhost" if batch_type == "local" else socket.getfqdn()
    command = (
        "vine_factory",
        "--batch-type", batch_type,
        "--min-workers", str(workers),
        "--max-workers", str(workers),
        "--workers-per-cycle", str(workers),
        "--factory-period", "1",
        "--factory-timeout", str(POOL_LIFETIME_SECONDS),
        "--timeout", str(WORKER_IDLE_SECONDS),
        "--cores", str(cores),
        "--memory", str(memory_mib),
        "--disk", "4096",
        "--worker-binary", str(worker_binary),
        "--scratch-dir", str(root / "factory"),
        "--parent-death",
        manager_host, str(manager_port),
    )
    process = subprocess.Popen(
        command, stdout=log, stderr=subprocess.STDOUT, text=True,
        start_new_session=True,
    )
    return process, log, command


def physical_counts(log_path, workflow_id):
    submitted = completed = 0
    matched = 0
    for line in log_path.read_text().splitlines():
        if f"datavine workflow {workflow_id} " not in line:
            continue
        match = PHYSICAL_COUNTS.search(line)
        if match:
            submitted += int(match.group("submitted"))
            completed += int(match.group("completed"))
            matched += 1
    if not matched:
        raise RuntimeError(f"missing physical counts for {workflow_id}")
    return {"submissions": submitted, "completions": completed}


def runtime_stages(log_path, workflow_id):
    totals = {}
    matched = 0
    for line in log_path.read_text().splitlines():
        if f"datavine workflow {workflow_id} " not in line:
            continue
        values = {
            match.group("name"): match.group("value")
            for match in RUNTIME_METRIC.finditer(line)
        }
        if "setup_seconds" not in values:
            continue
        matched += 1
        for name, value in values.items():
            parsed = float(value) if name.endswith("_seconds") else int(value)
            totals[name] = totals.get(name, 0) + parsed
    if not matched:
        raise RuntimeError(f"missing runtime stage metrics for {workflow_id}")
    totals["runtime_invocations"] = matched
    return totals


def wait_datavine(client, workflow_id, roots, timeout=900):
    del roots
    return client.wait_workflow(workflow_id, timeout=timeout), None


def start_datavine(repository, root, workers, cores, batch_type):
    service_log_path = root / "service.log"
    service_log = service_log_path.open("w")
    service = subprocess.Popen(
        (str(repository / "taskvine/src/tools/datavine_workflow"), "serve",
         str(root / "journal"), "sc-workflow-comparison"),
        stdout=subprocess.PIPE, stderr=service_log, text=True,
        start_new_session=True,
        env=dict(os.environ, DATAVINE_WORKFLOW_METRICS="1",
                 DATAVINE_RUNTIME_INFO_PATH=str(root / "run-info")),
    )
    contact_line = service.stdout.readline()
    if not contact_line:
        raise RuntimeError("DataVine service exited before publishing contact")
    contact = json.loads(contact_line)
    factory, factory_log, command = start_resident_factory(
        root / "factory-state", contact["manager_port"], workers, cores,
        repository / "taskvine/src/worker/vine_worker", root / "factory.log",
        batch_type,
    )
    inventory = wait_workers(repository / "taskvine/src/tools/vine_status",
                             contact["manager_port"], workers, cores, factory,
                             timeout=3600)
    client = WorkflowClient(contact["endpoint"], "sc-workflow-comparison")
    warmup = Workflow("sc-datavine-warmup-v1", workflow_id="sc-datavine-warmup",
                      maximum_tasks=1, maximum_edges=0)
    output = warmup.python_callable(work_unit, "warmup", 1000, 1, 0)
    warmup.request(output).submit(client)
    state, _ = wait_datavine(client, warmup.workflow_id, (service.pid, factory.pid))
    if state["state"] != "completed":
        raise RuntimeError(state)
    cloudpickle.loads(client.fetch_workflow_result(warmup.workflow_id, output.data_id))
    return (service, service_log, service_log_path, factory, factory_log,
            command, client, inventory, int(contact["manager_port"]))


class DataVinePool:
    def __init__(self, repository, root, workers, cores, batch_type):
        root.mkdir(parents=True)
        values = start_datavine(
            repository, root, workers, cores, batch_type
        )
        (self.service, self.service_log, self.service_log_path, self.factory,
         self.factory_log, self.factory_command, self.client,
         self.inventory, self.manager_port) = values
        self.workers = workers
        self.cores = cores
        self.batch_type = batch_type

    def close(self):
        self.client.close()
        terminate_group(self.factory)
        terminate_group(self.service)
        self.factory_log.close()
        self.service_log.close()


def run_datavine(pool, root, name, spec, repetition):
    root.mkdir(parents=True)
    service = pool.service
    factory = pool.factory
    client = pool.client
    service_log = pool.service_log
    service_log_path = pool.service_log_path
    factory_command = pool.factory_command
    try:
        client.rpc_profile(reset=True)
        workflow_id = f"sc-{name}-dv-r{repetition}"
        sampler = PeakSampler((os.getpid(), service.pid, factory.pid)).start()
        started = time.monotonic()
        built = started
        ingest_profile = None
        if spec.get("mode") == "dynamic":
            session = WorkflowSession.create(
                client, workflow_id, maximum_tasks=spec["steps"], maximum_edges=0,
                idempotency_key=f"{workflow_id}-open",
            )
            submitted = time.monotonic()
            values = []
            seed = 1
            dynamic_function = (
                timed_work_unit if spec.get("kind") == "timed" else work_unit
            )
            for index in range(spec["steps"]):
                future = session.submit(
                    dynamic_function, f"dynamic-{index}", spec["iterations"], seed,
                    spec["output_bytes"], idempotency_key=f"{workflow_id}-{index}",
                )
                value = future.result()
                values.append(value)
                seed = int(value["digest"][:16], 16)
            session.seal()
            completed = time.monotonic()
            terminal = completed
            canonical_values, cpu_by_task = canonical((values[-1],))
            logical_tasks = spec["steps"]
        else:
            builder = Workflow(
                f"{workflow_id}-v1", workflow_id=workflow_id,
                maximum_tasks=len(spec["nodes"]),
                maximum_edges=sum(len(item["parents"]) for item in spec["nodes"]),
            )
            references = {}
            for item in spec["nodes"]:
                parents = []
                for parent in item["parents"]:
                    value = references[parent]
                    if item["kind"] == "select" and isinstance(value, tuple):
                        value = value[item["index"]]
                    parents.append(value)
                if item["kind"] == "split":
                    value = builder.python_callable(
                        split_unit, item["key"], item["iterations"], item["seed"],
                        item["output_bytes"], *parents, output_count=3,
                    )
                elif item["kind"] == "select":
                    value = builder.python_callable(
                        select_unit, item["index"], item["key"], item["iterations"],
                        item["seed"], item["output_bytes"], *parents,
                    )
                elif item["kind"] == "timed":
                    value = builder.python_callable(
                        timed_work_unit, item["key"], item["iterations"],
                        item["seed"], item["output_bytes"], *parents,
                    )
                else:
                    value = builder.python_callable(
                        work_unit, item["key"], item["iterations"], item["seed"],
                        item["output_bytes"], *parents,
                    )
                references[item["key"]] = value
            sink_refs = [references[key] for key in spec["sinks"]]
            builder.request(*sink_refs)
            built = time.monotonic()
            builder.submit(client)
            submitted = time.monotonic()
            ingest_profile = builder.profile()
            state, _ = wait_datavine(
                client, workflow_id, (service.pid, factory.pid)
            )
            if state["state"] != "completed":
                raise RuntimeError(state)
            terminal = time.monotonic()
            serialized = client.fetch_workflow_results(
                workflow_id, (reference.data_id for reference in sink_refs)
            )
            values = [cloudpickle.loads(value) for value in serialized]
            completed = time.monotonic()
            canonical_values, cpu_by_task = canonical(values)
            logical_tasks = len(spec["nodes"])
        peak = sampler.stop()
        service_log.flush()
        counts = physical_counts(service_log_path, workflow_id)
        stages = runtime_stages(service_log_path, workflow_id)
        expected_counts = {"submissions": logical_tasks, "completions": logical_tasks}
        # A removed worker can legitimately create additional physical attempts.
        # The campaign driver invalidates that entire measured pair, so preserve
        # the diagnostic result instead of aborting before it can do so.
        if (counts != expected_counts and
                not stages.get("manager_workers_removed", 0)):
            raise RuntimeError({"expected": expected_counts, "physical": counts})
        result = {
            "backend": "datavine",
            "status": "PASS",
            "workflow": name,
            "repetition": repetition,
            "logical_tasks": logical_tasks,
            "physical_tasks": counts,
            "runtime_stages": stages,
            "build_submit_seconds": submitted - started,
            "workflow_build_seconds": built - started,
            "workflow_submit_seconds": submitted - built,
            "ingest_profile": ingest_profile,
            "post_submit_seconds": completed - submitted,
            "terminal_wait_seconds": terminal - submitted,
            "fetch_seconds": completed - terminal,
            "total_seconds": completed - started,
            "tasks_per_second": logical_tasks / (completed - started),
            "useful_cpu_seconds": sum(cpu_by_task.values()) / 1e9,
            "results": canonical_values,
            "peak": peak,
            "factory_command": list(factory_command),
            "resident_worker_gate": {
                "workers": len(pool.inventory),
                "cores_per_worker": pool.cores,
            },
            "rpc_profile": client.rpc_profile(reset=True),
        }
        if "values" in locals():
            values.clear()
        cleanup_started = time.monotonic()
        if "sink_refs" in locals():
            sink_refs.clear()
        if "references" in locals():
            references.clear()
        if "builder" in locals():
            del builder
        heap = release_process_heap()
        result["client_cleanup"] = {
            "seconds": time.monotonic() - cleanup_started,
            **heap,
        }
        return result
    except Exception:
        if "sampler" in locals():
            sampler.stop()
        service_log.flush()
        raise


@contextlib.contextmanager
def taskvine_directory(root):
    previous = Path.cwd()
    os.chdir(root)
    try:
        yield
    finally:
        os.chdir(previous)


def start_taskvine(repository, root, workers, cores, batch_type):
    executor = vine.FuturesExecutor(port=0, factory=False)
    library_name = "sc-workflow-functions"
    library = executor.manager.create_library_from_functions(
        library_name, work_unit, timed_work_unit, split_unit, select_unit,
        add_env=False, exec_mode="fork",
    )
    library.set_cores(cores)
    executor.install_library(library)
    manager_host_port = (
        None if batch_type == "local"
        else f"{socket.getfqdn()}:{executor.manager.port}"
    )
    factory = vine.Factory(
        batch_type=batch_type, manager=executor.manager,
        manager_host_port=manager_host_port,
        worker_binary=str(repository / "taskvine/src/worker/vine_worker"),
        log_file=str(root / "factory.log"),
    )
    factory.min_workers = workers
    factory.max_workers = workers
    factory.workers_per_cycle = workers
    factory.cores = cores
    factory.memory = 2048
    factory.disk = 4096
    factory.timeout = WORKER_IDLE_SECONDS
    factory.factory_timeout = POOL_LIFETIME_SECONDS
    factory.start()
    inventory = wait_taskvine_pool(executor.manager, factory, workers, cores)
    warmup = executor.future_funcall(library_name, "work_unit", "warmup", 1000, 1, 0)
    warmup.set_cores(1)
    if executor.submit(warmup).result()["digest"] == "":
        raise RuntimeError("unreachable warmup result")
    return executor, factory, library_name, inventory


class TaskVinePool:
    def __init__(self, repository, root, workers, cores, batch_type):
        root.mkdir(parents=True)
        with taskvine_directory(root):
            (self.executor, self.factory, self.library,
             self.inventory) = start_taskvine(
                repository, root, workers, cores, batch_type
            )
        self.workers = workers
        self.cores = cores
        self.batch_type = batch_type
        self.manager_port = int(self.executor.manager.port)

    def close(self):
        self.factory.stop()
        self.executor.manager.__del__()


def cleanup_taskvine_futures(executor, run_futures):
    """Release one measured DAG without adding cleanup to its wall time."""
    started = time.monotonic()
    manager = executor.manager
    table_before = len(manager._task_table)
    executor_table_before = len(executor.task_table)
    drained = 0
    empty_polls = 0
    drain_started = time.monotonic()
    while not manager.empty():
        task = manager.wait(1)
        if task is None:
            empty_polls += 1
            if empty_polls >= 5:
                raise RuntimeError({
                    "taskvine_cleanup_stalled": True,
                    "task_table_entries": len(manager._task_table),
                })
            continue
        empty_polls = 0
        drained += 1
    drain_seconds = time.monotonic() - drain_started
    files_undeclared = 0
    run_tasks = []
    run_files = []
    detach_started = time.monotonic()
    for future in run_futures:
        task = getattr(future, "_task", None)
        if task is not None:
            run_tasks.append(task)
            for attribute in ("_input_file", "_output_file"):
                file = getattr(task, attribute, None)
                if file is not None:
                    run_files.append(file)
                    setattr(task, attribute, None)
            task._saved_output = None
            task._future = None
        future._task = None
        future._result = None
    detach_seconds = time.monotonic() - detach_started

    # FuturesExecutor keeps every submitted task in a lifetime registry.  A
    # completed task still owns C-side mount references to its declared files,
    # so merely undeclaring those files leaves fetched buffers (potentially
    # gigabytes) alive.  Remove exactly this run's tasks and release their C
    # task objects after the manager has returned all of them.
    run_task_ids = {id(task) for task in run_tasks}
    executor.task_table[:] = [
        task for task in executor.task_table if id(task) not in run_task_ids
    ]
    tasks_released = 0
    release_started = time.monotonic()
    # Consumers own mounts of producer outputs.  Release in reverse submission
    # order so high-indegree consumer mounts disappear before producer files.
    for task in reversed(run_tasks):
        task.__del__()
        if task._task is not None:
            raise RuntimeError("TaskVine task release left a live C task")
        tasks_released += 1
    task_release_seconds = time.monotonic() - release_started
    run_tasks.clear()
    undeclare_started = time.monotonic()
    seen_files = set()
    for file in run_files:
        identity = id(file)
        if identity in seen_files:
            continue
        seen_files.add(identity)
        manager.undeclare_file(file)
        files_undeclared += 1
    run_files.clear()
    undeclare_seconds = time.monotonic() - undeclare_started
    heap = release_process_heap()
    table_after = len(manager._task_table)
    if table_after:
        raise RuntimeError({
            "taskvine_cleanup_retained_tasks": table_after,
            "task_table_before": table_before,
            "tasks_drained": drained,
        })
    return {
        "seconds": time.monotonic() - started,
        "drain_seconds": drain_seconds,
        "detach_seconds": detach_seconds,
        "task_release_seconds": task_release_seconds,
        "undeclare_seconds": undeclare_seconds,
        "task_table_before": table_before,
        "task_table_after": table_after,
        "tasks_drained": drained,
        "files_undeclared": files_undeclared,
        "tasks_released": tasks_released,
        "executor_task_table_before": executor_table_before,
        "executor_task_table_after": len(executor.task_table),
        **heap,
    }


def run_taskvine(pool, root, name, spec, repetition):
    root.mkdir(parents=True)
    executor = pool.executor
    factory = pool.factory
    library = pool.library
    run_futures = []
    try:
        baseline = taskvine_stats(executor.manager)
        sampler = PeakSampler((os.getpid(), factory._factory_proc.pid)).start()
        started = time.monotonic()
        if spec.get("mode") == "dynamic":
            values = []
            seed = 1
            submitted = started
            dynamic_function = (
                "timed_work_unit" if spec.get("kind") == "timed" else "work_unit"
            )
            for index in range(spec["steps"]):
                task = executor.future_funcall(
                    library, dynamic_function, f"dynamic-{index}",
                    spec["iterations"], seed, spec["output_bytes"],
                )
                task.set_cores(1)
                future = executor.submit(task)
                run_futures.append(future)
                submitted = time.monotonic()
                value = future.result()
                values.append(value)
                seed = int(value["digest"][:16], 16)
            completed = time.monotonic()
            terminal = completed
            canonical_values, cpu_by_task = canonical((values[-1],))
            logical_tasks = spec["steps"]
        else:
            futures = {}
            for item in spec["nodes"]:
                parents = [futures[key] for key in item["parents"]]
                function = {
                    "work": "work_unit",
                    "timed": "timed_work_unit",
                    "split": "split_unit",
                    "select": "select_unit",
                }[item["kind"]]
                arguments = [item["key"], item["iterations"], item["seed"],
                             item["output_bytes"], *parents]
                if item["kind"] == "select":
                    arguments.insert(0, item["index"])
                task = executor.future_funcall(library, function, *arguments)
                task.set_cores(1)
                futures[item["key"]] = executor.submit(task)
                run_futures.append(futures[item["key"]])
            submitted = time.monotonic()
            sinks = [futures[key] for key in spec["sinks"]]
            outcome = vine.futures.wait(sinks, timeout=900)
            if outcome.not_done:
                raise TimeoutError(f"{len(outcome.not_done)} sinks remain")
            terminal = time.monotonic()
            values = []
            for sink_key, future in zip(spec["sinks"], sinks):
                try:
                    values.append(future.result())
                except Exception as error:
                    output_file = future._task._output_file
                    raw = b""
                    raw_error = None
                    try:
                        raw = output_file.contents()
                    except Exception as inspect_error:
                        raw_error = repr(inspect_error)
                    diagnostic = {
                        "sink_key": sink_key,
                        "task_id": future._task.id,
                        "task_result": future._task.result,
                        "task_exit_code": future._task.exit_code,
                        "output_type": output_file.type(),
                        "output_cached_name": vine.cvine.vine_file_cached_name(
                            output_file._file
                        ),
                        "output_size": len(output_file),
                        "raw_size": len(raw),
                        "raw_sha256": hashlib.sha256(raw).hexdigest(),
                        "raw_prefix_hex": raw[:64].hex(),
                        "raw_error": raw_error,
                        "exception": repr(error),
                    }
                    (root / "taskvine-result-corruption.json").write_text(
                        json.dumps(diagnostic, indent=2, sort_keys=True) + "\n"
                    )
                    raise RuntimeError({
                        "taskvine_result_corruption": diagnostic,
                    }) from error
            completed = time.monotonic()
            canonical_values, cpu_by_task = canonical(values)
            logical_tasks = len(spec["nodes"])
        peak = sampler.stop()
        counts = difference(taskvine_stats(executor.manager), baseline)
        if ((counts["tasks_submitted"] != logical_tasks or
                counts["tasks_done"] != logical_tasks or
                counts["tasks_successful"] != logical_tasks or
                counts["tasks_failed"] != 0) and
                not counts["workers_removed"]):
            raise RuntimeError({"logical_tasks": logical_tasks, "stats": counts})
        result = {
            "backend": "taskvine",
            "status": "PASS",
            "workflow": name,
            "repetition": repetition,
            "logical_tasks": logical_tasks,
            "physical_tasks": {
                "submissions": counts["tasks_submitted"],
                "completions": counts["tasks_done"],
            },
            "build_submit_seconds": submitted - started,
            "post_submit_seconds": completed - submitted,
            "terminal_wait_seconds": terminal - submitted,
            "fetch_seconds": completed - terminal,
            "total_seconds": completed - started,
            "tasks_per_second": logical_tasks / (completed - started),
            "useful_cpu_seconds": sum(cpu_by_task.values()) / 1e9,
            "results": canonical_values,
            "peak": peak,
            "taskvine_stats": counts,
            "resident_worker_gate": {
                "workers": len(pool.inventory),
                "cores_per_worker": pool.cores,
            },
        }
        if "values" in locals():
            values.clear()
        result["client_cleanup"] = cleanup_taskvine_futures(
            executor, run_futures
        )
        return result
    except Exception:
        if "sampler" in locals():
            sampler.stop()
        raise


def summarize(runs):
    grouped = {}
    for run in runs:
        grouped.setdefault((run["workflow"], run["backend"]), []).append(run)
    rows = []
    workflows = sorted({key[0] for key in grouped})
    for name in workflows:
        row = {"workflow": name}
        for backend in ("taskvine", "datavine"):
            values = grouped[(name, backend)]
            totals = [item["total_seconds"] for item in values]
            rates = [item["tasks_per_second"] for item in values]
            row[backend] = {
                "repetitions": len(values),
                "median_total_seconds": statistics.median(totals),
                "min_total_seconds": min(totals),
                "max_total_seconds": max(totals),
                "median_tasks_per_second": statistics.median(rates),
            }
            if backend == "datavine":
                stage_names = sorted(values[0]["runtime_stages"])
                row[backend]["median_runtime_stages"] = {
                    stage: statistics.median(
                        item["runtime_stages"][stage] for item in values
                    )
                    for stage in stage_names
                }
        row["datavine_to_taskvine_rate"] = (
            row["datavine"]["median_tasks_per_second"] /
            row["taskvine"]["median_tasks_per_second"]
        )
        rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflows", default=",".join(workflow_specs()))
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--cores", type=int, default=16)
    parser.add_argument("--batch-type", choices=("local", "condor"),
                        default="condor")
    parser.add_argument(
        "--backend-order",
        choices=("taskvine-first", "datavine-first"),
        default="taskvine-first",
    )
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    if min(args.repetitions, args.workers, args.cores) < 1:
        parser.error("repetitions, workers, and cores must be positive")
    available = workflow_specs(args.workers * args.cores)
    names = [value.strip() for value in args.workflows.split(",") if value.strip()]
    unknown = set(names) - set(available)
    if not names or unknown:
        parser.error(f"unknown workflows: {sorted(unknown)}")
    affinity = len(os.sched_getaffinity(0))
    quota = cgroup_cpu_quota()
    capacity = min(affinity, quota or affinity)
    if args.batch_type == "local" and args.workers * args.cores > capacity:
        parser.error(
            f"requested {args.workers * args.cores} cores exceed capacity {capacity:g}"
        )
    output = Path(args.output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        parser.error(f"output directory is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    repository = Path(__file__).resolve().parents[2]
    durations = sorted({
        item["duration_ms"]
        for spec in available.values() for item in spec.get("nodes", ())
    } | {available["dynamic"]["duration_ms"]})
    base_duration = 20.0
    base_iterations = calibrate(base_duration)
    calibrated = {
        str(float(duration)): max(1, round(base_iterations * duration / base_duration))
        for duration in durations
    }
    specs = {name: materialize_iterations(available[name], calibrated)
             for name in names}
    expected = {
        name: (
            None if spec.get("mode") == "dynamic" or
            spec.get("cross_backend_only") else execute_locally(spec)
        )
        for name, spec in specs.items()
    }
    runs = []
    jobs = [(repetition, name)
            for repetition in range(1, args.repetitions + 1) for name in names]
    backends = [
        ("taskvine", TaskVinePool, run_taskvine),
        ("datavine", DataVinePool, run_datavine),
    ]
    if args.backend_order == "datavine-first":
        backends.reverse()
    for backend, pool_type, runner in backends:
        pool = pool_type(repository, output / "pools" / backend,
                         args.workers, args.cores, args.batch_type)
        try:
            for repetition, name in jobs:
                run_root = output / "runs" / f"r{repetition}-{name}-{backend}"
                result = runner(pool, run_root, name, specs[name], repetition)
                if expected[name] is not None and result["results"] != expected[name]:
                    raise RuntimeError(f"{name}/{backend}: result mismatch")
                repetition_peer = next((item for item in runs
                    if item["workflow"] == name and item["backend"] == backend), None)
                if (repetition_peer and
                        repetition_peer["results"] != result["results"]):
                    raise RuntimeError(
                        f"{name}/{backend}: result mismatch across repetitions"
                    )
                comparison_peer = next((item for item in runs
                    if item["workflow"] == name and item["repetition"] == repetition
                    and item["backend"] != backend), None)
                if comparison_peer and comparison_peer["results"] != result["results"]:
                    raise RuntimeError(f"{name}: backend result mismatch")
                (run_root / "result.json").write_text(
                    json.dumps(result, indent=2, sort_keys=True) + "\n"
                )
                runs.append(result)
                print(json.dumps({
                    "backend": backend, "workflow": name,
                    "repetition": repetition,
                    "seconds": result["total_seconds"],
                    "tasks_per_second": result["tasks_per_second"],
                }, sort_keys=True), flush=True)
        finally:
            pool.close()
    commit = subprocess.run(
        ("git", "rev-parse", "HEAD"), cwd=repository,
        text=True, stdout=subprocess.PIPE, check=True,
    ).stdout.strip()
    report = {
        "artifact_type": "datavine-sc-workflow-comparison-pilot",
        "status": "PASS",
        "scope": (
            "cluster-scale" if args.batch_type != "local"
            else "local-pilot" if args.workers * args.cores == 1
            else "local-scale"
        ),
        "fairness": {
            "one_logical_task_per_physical_task": True,
            "semantic_batching": False,
            "taskvine_executor": "FunctionCall-fork",
            "datavine_executor": "preloaded-python-fork",
            "warmup_excluded": True,
            "result_content_validated": True,
        },
        "environment": {
            "commit": commit,
            "python": sys.version,
            "platform": platform.platform(),
            "hostname": platform.node(),
            "affinity_cpu_count": affinity,
            "cgroup_cpu_quota": quota,
            "effective_cpu_capacity": capacity,
            "workers": args.workers,
            "cores_per_worker": args.cores,
            "batch_type": args.batch_type,
            "backend_order": args.backend_order,
            "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
        },
        "calibration": calibrated,
        "workflow_specs": specs,
        "summary": summarize(runs),
        "runs": runs,
    }
    (output / "summary.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps({"status": "PASS", "output": str(output),
                      "runs": len(runs)}, sort_keys=True))


if __name__ == "__main__":
    main()
