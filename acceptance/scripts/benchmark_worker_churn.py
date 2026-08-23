#!/usr/bin/env python3
"""Measure static DataVine recovery while randomly removing Worker jobs."""

import argparse
import concurrent.futures
import gc
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import resource
import socket
import struct
import subprocess
import sys
import time
from collections import Counter

import cloudpickle

from benchmark_cpu_fork import terminate_group
from benchmark_static_scale import (
    CODEC,
    DELTA_SCHEMA,
    SCHEMA,
    encoded,
    object_record,
    output_record,
    tree_bytes,
    wait_scale_workers,
)
from compare_workflows import (
    PeakSampler,
    physical_counts,
    runtime_stages,
    start_resident_factory,
)
from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


JOURNAL_MAGIC = 0x44564A31
JOURNAL_HEADER = struct.Struct("!IHHIIQ")
STORE_RECOVER = 108
STORE_TASK_EVENT = 109
STORE_TASK_EVENT_BATCH = 112
TASK_SUBMITTED = 9
TASK_COMPLETED = 10
TASK_RETRY = 11
TASK_FAILED = 12
WORKER_EVENT = re.compile(
    r"^(?P<time>\d+) \d+ WORKER \S+ "
    r"(?P<kind>CONNECTION|DISCONNECTION)(?:\s|$)"
)
WORKER_CONNECTION = re.compile(
    r"^\d+ \d+ WORKER (?P<worker>\S+) CONNECTION "
    r"(?P<host>[^: ]+):\d+$"
)
WORKER_DISCONNECTION = re.compile(
    r"^\d+ \d+ WORKER (?P<worker>\S+) DISCONNECTION(?:\s|$)"
)
HOST_ADDRESS_CACHE = {}


def spin_increment(value, spin_nanoseconds):
    """Deterministic, CPU-intensive one-input FunctionCall kernel."""

    deadline = time.process_time_ns() + int(spin_nanoseconds)
    state = int(value) ^ 0x9E3779B97F4A7C15
    while time.process_time_ns() < deadline:
        state ^= (state << 13) & ((1 << 64) - 1)
        state ^= state >> 7
        state ^= (state << 17) & ((1 << 64) - 1)
    return int(value) + 1 + (state & 0)


class JournalTail:
    """Read complete local journal records without adding runtime messages."""

    def __init__(self, path):
        self.path = Path(path)
        self.offset = 0
        self.unique_completed = set()
        self.task_events = {
            "submitted": 0,
            "completed": 0,
            "retry": 0,
            "failed": 0,
        }
        self.maximum_attempt = 0
        self.recoveries = 0
        self.submissions_by_task = Counter()
        self.completions_by_task = Counter()

    def _task_event(self, event_type, task_id, attempt):
        names = {
            TASK_SUBMITTED: "submitted",
            TASK_COMPLETED: "completed",
            TASK_RETRY: "retry",
            TASK_FAILED: "failed",
        }
        name = names.get(event_type)
        if name:
            self.task_events[name] += 1
        if event_type == TASK_COMPLETED:
            self.unique_completed.add(task_id)
            self.completions_by_task[task_id] += 1
        elif event_type == TASK_SUBMITTED:
            self.submissions_by_task[task_id] += 1
        self.maximum_attempt = max(self.maximum_attempt, attempt)

    def _record(self, opcode, payload):
        if opcode == STORE_RECOVER:
            self.recoveries += 1
        elif opcode == STORE_TASK_EVENT and len(payload) >= 20:
            event_type = struct.unpack_from("!I", payload, 0)[0]
            task_id = struct.unpack_from("!Q", payload, 4)[0]
            attempt = struct.unpack_from("!I", payload, 12)[0]
            self._task_event(event_type, task_id, attempt)
        elif opcode == STORE_TASK_EVENT_BATCH and len(payload) >= 8:
            id_size, count = struct.unpack_from("!II", payload, 0)
            offset = 8 + id_size
            if offset + count * 20 != len(payload):
                raise RuntimeError("invalid task event batch in journal")
            for index in range(count):
                record = offset + index * 20
                event_type = struct.unpack_from("!I", payload, record)[0]
                task_id = struct.unpack_from("!Q", payload, record + 4)[0]
                attempt = struct.unpack_from("!I", payload, record + 12)[0]
                self._task_event(event_type, task_id, attempt)

    def scan(self):
        try:
            size = self.path.stat().st_size
        except FileNotFoundError:
            return
        with self.path.open("rb") as stream:
            stream.seek(self.offset)
            while self.offset + JOURNAL_HEADER.size <= size:
                header = stream.read(JOURNAL_HEADER.size)
                magic, version, opcode, payload_size, _, _ = (
                    JOURNAL_HEADER.unpack(header)
                )
                if magic != JOURNAL_MAGIC or version != 1:
                    raise RuntimeError("invalid DataVine journal header")
                if self.offset + JOURNAL_HEADER.size + payload_size > size:
                    break
                payload = stream.read(payload_size)
                self._record(opcode, payload)
                self.offset += JOURNAL_HEADER.size + payload_size

    def summary(self):
        self.scan()
        repeated = {
            task_id: count
            for task_id, count in self.completions_by_task.items()
            if count > 1
        }
        return {
            "recoveries": self.recoveries,
            "unique_completed_tasks": len(self.unique_completed),
            "task_events": dict(self.task_events),
            "maximum_attempt": self.maximum_attempt,
            "tasks_recomputed": len(repeated),
            "repeated_completion_events": sum(repeated.values()) - len(repeated),
            "maximum_completions_per_task": max(
                self.completions_by_task.values(), default=0
            ),
            "tasks_resubmitted": sum(
                count > 1 for count in self.submissions_by_task.values()
            ),
        }


def connected_worker_hosts(transaction_path):
    workers = {}
    for line in transaction_path.read_text(errors="replace").splitlines():
        connection = WORKER_CONNECTION.match(line)
        if connection:
            workers[connection.group("worker")] = connection.group("host")
            continue
        disconnection = WORKER_DISCONNECTION.match(line)
        if disconnection:
            workers.pop(disconnection.group("worker"), None)
    return Counter(workers.values())


def condor_worker_jobs(factory_directory):
    constraint = (
        f'Iwd == "{factory_directory}" && JobStatus == 2'
    )
    completed = subprocess.run(
        (
            "condor_q",
            "-constraint",
            constraint,
            "-af",
            "ClusterId",
            "ProcId",
            "JobCurrentStartDate",
            "Iwd",
            "RemoteHost",
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    if completed.returncode:
        raise RuntimeError(f"condor_q failed: {completed.stderr.strip()}")
    now = time.time()
    jobs = []
    for line in completed.stdout.splitlines():
        fields = line.split(maxsplit=4)
        if len(fields) != 5:
            continue
        cluster, process, started, iwd, remote_host = fields
        if Path(iwd) != factory_directory:
            continue
        try:
            age = now - int(started)
        except ValueError:
            continue
        remote_name = remote_host.rsplit("@", 1)[-1]
        try:
            remote_address = HOST_ADDRESS_CACHE.get(remote_name)
            if remote_address is None:
                remote_address = socket.gethostbyname(remote_name)
                HOST_ADDRESS_CACHE[remote_name] = remote_address
        except socket.gaierror:
            continue
        jobs.append({
            "job_id": f"{cluster}.{process}",
            "age_seconds": age,
            "worker_host": remote_address,
        })
    return jobs


def remove_random_worker(factory_directory, transaction_path, removed,
                         generator, timeout, minimum_age_seconds):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        jobs = condor_worker_jobs(factory_directory)
        running_by_host = Counter(job["worker_host"] for job in jobs)
        connected_by_host = connected_worker_hosts(transaction_path)
        candidates = []
        for job in jobs:
            host = job["worker_host"]
            if (
                job["job_id"] not in removed
                and job["age_seconds"] >= minimum_age_seconds
                and running_by_host[host] == connected_by_host[host]
            ):
                candidates.append(job)
        if candidates:
            chosen = generator.choice(candidates)
            host = chosen["worker_host"]
            started = time.monotonic()
            completed = subprocess.run(
                ("condor_rm", chosen["job_id"]),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=30,
            )
            elapsed = time.monotonic() - started
            if completed.returncode:
                time.sleep(0.1)
                continue
            removed.add(chosen["job_id"])
            return {
                **chosen,
                "candidate_count": len(candidates),
                "connected_workers_on_host": connected_by_host[host],
                "running_jobs_on_host": running_by_host[host],
                "connected_at_selection": True,
                "command_seconds": elapsed,
                "condor_output": completed.stdout.strip(),
            }
        time.sleep(0.2)
    raise TimeoutError("no mature running factory Worker job to remove")


def worker_utilization(transaction_path, start_us, finish_us, target_workers):
    events = []
    for line in transaction_path.read_text(errors="replace").splitlines():
        match = WORKER_EVENT.match(line)
        if match:
            events.append((
                int(match.group("time")),
                1 if match.group("kind") == "CONNECTION" else -1,
            ))
    events.sort()
    connected = 0
    for timestamp, delta in events:
        if timestamp >= start_us:
            break
        connected = max(0, connected + delta)
    cursor = start_us
    worker_microseconds = 0
    zero_microseconds = 0
    deficit_microseconds = 0
    minimum = connected
    connections = disconnections = 0
    for timestamp, delta in events:
        if timestamp < start_us:
            continue
        if timestamp > finish_us:
            break
        duration = max(0, timestamp - cursor)
        worker_microseconds += connected * duration
        if connected == 0:
            zero_microseconds += duration
        deficit_microseconds += max(0, target_workers - connected) * duration
        connected = max(0, connected + delta)
        minimum = min(minimum, connected)
        connections += delta > 0
        disconnections += delta < 0
        cursor = timestamp
    duration = max(0, finish_us - cursor)
    worker_microseconds += connected * duration
    if connected == 0:
        zero_microseconds += duration
    deficit_microseconds += max(0, target_workers - connected) * duration
    elapsed = max(1, finish_us - start_us)
    return {
        "connections": connections,
        "disconnections": disconnections,
        "minimum_connected_workers": minimum,
        "mean_connected_workers": worker_microseconds / elapsed,
        "zero_worker_seconds": zero_microseconds / 1e6,
        "worker_deficit_seconds": deficit_microseconds / 1e6,
        "target_worker_utilization": (
            worker_microseconds / (elapsed * target_workers)
        ),
    }


def find_transactions(root):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        matches = sorted(Path(root).glob("*/vine-logs/transactions"))
        if matches:
            return matches[-1]
        time.sleep(0.1)
    raise TimeoutError("TaskVine transaction log was not created")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lanes", type=int, default=128)
    parser.add_argument("--stages", type=int, default=80)
    parser.add_argument("--spin-seconds", type=float, default=0.5)
    parser.add_argument("--chunk-tasks", type=int, default=2_000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--cores", type=int, default=16)
    parser.add_argument("--removals", type=int, choices=range(0, 101), default=100)
    parser.add_argument("--seed", type=int, default=20260822)
    parser.add_argument("--minimum-job-age", type=float, default=2.0)
    parser.add_argument("--maximum-progress-lag", type=float, default=0.01)
    parser.add_argument("--pre-admit", action="store_true")
    parser.add_argument("--admit-before-seal", action="store_true")
    parser.add_argument("--batch-type", choices=("condor", "local"), default="condor")
    parser.add_argument("--timeout", type=float, default=7_200)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    if min(args.lanes, args.stages, args.chunk_tasks, args.workers, args.cores) < 1:
        parser.error("workflow and pool sizes must be positive")
    if args.spin_seconds <= 0:
        parser.error("spin-seconds must be positive")
    if args.maximum_progress_lag < 0:
        parser.error("maximum-progress-lag cannot be negative")
    if args.removals and args.batch_type != "condor":
        parser.error("worker removal currently requires --batch-type condor")
    if args.pre_admit and args.admit_before_seal:
        parser.error("--pre-admit and --admit-before-seal are mutually exclusive")
    return args


def main():
    args = parse_args()
    output = Path(args.output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        raise RuntimeError(f"output directory is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    repository = Path(__file__).resolve().parents[2]
    python_directory = str(Path(sys.executable).resolve().parent)
    os.environ["PATH"] = os.pathsep.join((
        python_directory,
        os.environ.get("PATH", ""),
    ))
    workflow_tasks = args.lanes * args.stages
    workflow_id = f"static-churn-{workflow_tasks}-{args.removals}"
    token = "datavine-worker-churn"
    service_log_path = output / "service.log"
    factory_log_path = output / "factory.log"
    service_log = service_log_path.open("w")
    service = subprocess.Popen(
        (
            str(repository / "taskvine/src/tools/datavine_workflow"),
            "serve",
            str(output / "journal"),
            token,
        ),
        stdout=subprocess.PIPE,
        stderr=service_log,
        text=True,
        start_new_session=True,
        env=dict(
            os.environ,
            DATAVINE_WORKFLOW_METRICS="1",
            DATAVINE_RUNTIME_INFO_PATH=str(output / "run-info"),
        ),
    )
    client = factory = factory_log = sampler = None
    result = None
    started = time.monotonic()
    try:
        contact_line = service.stdout.readline()
        if not contact_line:
            raise RuntimeError("DataVine service exited before publishing contact")
        contact = json.loads(contact_line)
        client = WorkflowClient(contact["endpoint"], token, timeout=300)
        client.rpc_profile(reset=True)

        function_payload = cloudpickle.dumps(spin_increment)
        invocation_payload = cloudpickle.dumps({
            "args": (
                ("pickle", cloudpickle.dumps(0)),
                ("pickle", cloudpickle.dumps(int(args.spin_seconds * 1e9))),
            ),
            "kwargs": {},
            "output_count": 1,
            "output_files": ["datavine-python-output-0"],
        })
        seed_payload = cloudpickle.dumps(0)
        payloads = (seed_payload, function_payload, invocation_payload)
        digests = [hashlib.sha256(payload).hexdigest() for payload in payloads]
        client.put_objects(zip(payloads, digests))

        if args.pre_admit:
            factory, factory_log, factory_command = start_resident_factory(
                output / "factory-state",
                contact["manager_port"],
                args.workers,
                args.cores,
                repository / "taskvine/src/worker/vine_worker",
                factory_log_path,
                args.batch_type,
            )
            inventory = wait_scale_workers(
                repository / "taskvine/src/tools/vine_status",
                contact["manager_port"],
                args.workers,
                args.cores,
                factory,
                timeout=3600,
            )
            admitted = time.monotonic()
            admitted_us = time.time_ns() // 1000
            transaction_path = find_transactions(output / "run-info")
            sampler = PeakSampler(
                (os.getpid(), service.pid, factory.pid)
            ).start()

        initial = {
            "schema": SCHEMA,
            "workflow_id": workflow_id,
            "idempotency_key": f"{workflow_id}-initial",
            "mode": (
                "sealed" if args.pre_admit else
                "staged" if args.admit_before_seal else "streaming"
            ),
            "tasks": [],
            "data": [
                object_record(1, CODEC, digests[0]),
                object_record(2, {"name": "python/callable", "version": "1"}, digests[1]),
                object_record(3, {"name": "bytes", "version": "1"}, digests[2]),
            ],
            "requested_outputs": [],
            "policy": {
                "maximum_tasks": workflow_tasks,
                "maximum_edges": workflow_tasks - args.lanes,
            },
            "metadata": {
                "benchmark": "random-worker-churn",
                "semantic_task_batching": False,
                "intermediate_policy": "worker-local-peer-first",
            },
        }
        load_sampler = PeakSampler((os.getpid(), service.pid)).start()
        load_started = time.monotonic()
        load_peak = None
        chunk_sizes = []
        requested_ids = []
        common_executor = {
            "kind": "python",
            "version": "callable-v1",
            "payload_ref": 3,
            "function_ref": 2,
            "function_digest": digests[1],
            "output_files": ["datavine-python-output-0"],
        }
        task_defaults = {
            "executor": common_executor,
            "retry": {"maximum_attempts": 1},
            "resources": {"cores": 1},
        }
        initial["task_defaults"] = task_defaults
        data_defaults = {"codec": CODEC}
        initial["data_defaults"] = data_defaults
        generation = None
        for chunk_start in range(1, workflow_tasks + 1, args.chunk_tasks):
            chunk_end = min(workflow_tasks, chunk_start + args.chunk_tasks - 1)
            tasks = []
            data = []
            requested = []
            for task_id in range(chunk_start, chunk_end + 1):
                stage = (task_id - 1) // args.lanes
                parent_data_id = 1 if stage == 0 else 3 + task_id - args.lanes
                output_data_id = 3 + task_id
                tasks.append([task_id, [parent_data_id], [output_data_id]])
                data.append([output_data_id, task_id, 0])
                if stage == args.stages - 1:
                    requested.append(output_data_id)
            requested_ids.extend(requested)
            if args.pre_admit:
                initial["tasks"].extend(tasks)
                initial["data"].extend(data)
                initial["requested_outputs"].extend(requested)
                continue
            if generation is None:
                initial["tasks"] = tasks
                initial["data"].extend(data)
                initial["requested_outputs"] = requested
                initial_body = encoded(initial)
                info = client.submit_workflow(initial_body)
                generation = int(info["generation"])
                del tasks, data, requested
                gc.collect()
                continue
            delta = {
                "schema": DELTA_SCHEMA,
                "workflow_id": workflow_id,
                "idempotency_key": f"{workflow_id}-{chunk_start}-{chunk_end}",
                "task_defaults": task_defaults,
                "data_defaults": data_defaults,
                "tasks": tasks,
                "data": data,
                "requested_outputs": requested,
            }
            body = encoded(delta)
            chunk_sizes.append(len(body))
            info = client.append_workflow(workflow_id, generation, body)
            generation = int(info["generation"])
            del body, delta, tasks, data, requested
            gc.collect()
        graph_loaded = time.monotonic()
        if args.pre_admit:
            initial_body = encoded(initial)
            info = client.submit_workflow(initial_body)
        elif args.admit_before_seal:
            load_peak = load_sampler.stop()
            load_sampler = None
            factory, factory_log, factory_command = start_resident_factory(
                output / "factory-state",
                contact["manager_port"],
                args.workers,
                args.cores,
                repository / "taskvine/src/worker/vine_worker",
                factory_log_path,
                args.batch_type,
            )
            inventory = wait_scale_workers(
                repository / "taskvine/src/tools/vine_status",
                contact["manager_port"],
                args.workers,
                args.cores,
                factory,
                timeout=3600,
            )
            admitted = time.monotonic()
            admitted_us = time.time_ns() // 1000
            transaction_path = find_transactions(output / "run-info")
            sampler = PeakSampler(
                (os.getpid(), service.pid, factory.pid)
            ).start()
            info = client.seal_workflow(workflow_id, generation)
        else:
            info = client.seal_workflow(workflow_id, generation)
        sealed = time.monotonic()
        if load_sampler is not None:
            load_peak = load_sampler.stop()
            load_sampler = None

        if not args.pre_admit and not args.admit_before_seal:
            factory, factory_log, factory_command = start_resident_factory(
                output / "factory-state",
                contact["manager_port"],
                args.workers,
                args.cores,
                repository / "taskvine/src/worker/vine_worker",
                factory_log_path,
                args.batch_type,
            )
            sampler = PeakSampler((os.getpid(), service.pid, factory.pid)).start()
            inventory = wait_scale_workers(
                repository / "taskvine/src/tools/vine_status",
                contact["manager_port"],
                args.workers,
                args.cores,
                factory,
                timeout=3600,
            )
            admitted = time.monotonic()
            admitted_us = time.time_ns() // 1000
            transaction_path = find_transactions(output / "run-info")
        execution_started = (
            sealed if args.pre_admit or args.admit_before_seal else admitted
        )
        journal = JournalTail(output / "journal")
        journal.scan()
        removed = set()
        removal_records = []
        generator = random.Random(args.seed)
        thresholds = [
            max(1, math.ceil(workflow_tasks * (index + 0.5) / args.removals))
            for index in range(args.removals)
        ] if args.removals else []
        next_removal = 0
        factory_directory = output / "factory-state/factory"
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            terminal_future = executor.submit(
                client.wait_workflow, workflow_id, args.timeout
            )
            while next_removal < args.removals:
                journal.scan()
                completed = len(journal.unique_completed)
                if completed < thresholds[next_removal]:
                    if terminal_future.done():
                        break
                    time.sleep(0.05)
                    continue
                fault = remove_random_worker(
                    factory_directory,
                    transaction_path,
                    removed,
                    generator,
                    timeout=300,
                    minimum_age_seconds=args.minimum_job_age,
                )
                record = {
                    "removal": next_removal + 1,
                    "threshold_tasks": thresholds[next_removal],
                    "observed_unique_completions": completed,
                    "observed_progress": completed / workflow_tasks,
                    "target_progress": (next_removal + 0.5) / args.removals,
                    "progress_lag": (
                        completed / workflow_tasks
                        - (next_removal + 0.5) / args.removals
                    ),
                    "seconds_after_admission": time.monotonic() - admitted,
                    "wall_time": time.time(),
                    **fault,
                }
                removal_records.append(record)
                print(json.dumps({"phase": "fault", **record}, sort_keys=True), flush=True)
                if record["progress_lag"] > args.maximum_progress_lag:
                    raise AssertionError((
                        "fault_progress_lag",
                        record["removal"],
                        record["progress_lag"],
                        args.maximum_progress_lag,
                    ))
                next_removal += 1
            info = terminal_future.result()
        terminal = time.monotonic()
        terminal_us = time.time_ns() // 1000
        if len(removal_records) != args.removals:
            raise AssertionError(("worker_removals", len(removal_records), args.removals))
        if info["state"] != "completed":
            raise RuntimeError(info)
        if info["tasks"] != workflow_tasks:
            raise AssertionError(("tasks", info["tasks"], workflow_tasks))
        if info["data"] != workflow_tasks + 3:
            raise AssertionError(("data", info["data"], workflow_tasks + 3))
        if info["edges"] != workflow_tasks - args.lanes:
            raise AssertionError(("edges", info["edges"], workflow_tasks - args.lanes))
        if info["requested_outputs"] != args.lanes:
            raise AssertionError(("requested_outputs", info["requested_outputs"], args.lanes))

        serialized = client.fetch_workflow_results(workflow_id, requested_ids)
        values = [cloudpickle.loads(value) for value in serialized]
        if values != [1] * args.lanes:
            raise AssertionError(("sink_values", values[:10], 1))
        validated = time.monotonic()
        execute_peak = sampler.stop()
        sampler = None
        service_log.flush()
        journal_summary = journal.summary()
        if journal_summary["unique_completed_tasks"] != workflow_tasks:
            raise AssertionError(("journal_completed", journal_summary))
        if journal_summary["task_events"]["failed"]:
            raise AssertionError(("journal_failures", journal_summary))
        counts = physical_counts(service_log_path, workflow_id)
        stages = runtime_stages(service_log_path, workflow_id)
        retry_events = journal_summary["task_events"]["retry"]
        extra_submissions = counts["submissions"] - workflow_tasks
        extra_completions = counts["completions"] - workflow_tasks
        if int(stages.get("runtime_invocations", 0)) != 1:
            raise AssertionError(("runtime_invocations", stages))
        if counts["submissions"] != counts["completions"]:
            raise AssertionError(("abandoned_submissions", counts))
        if extra_submissions != retry_events or extra_completions != retry_events:
            raise AssertionError((
                "nonminimal_physical_attempts",
                counts,
                retry_events,
            ))
        invalidated = int(stages.get("recovery_invalidated_tasks", 0))
        if invalidated != journal_summary["repeated_completion_events"]:
            raise AssertionError((
                "recovery_frontier_mismatch",
                invalidated,
                journal_summary,
            ))
        utilization = worker_utilization(
            transaction_path, admitted_us, terminal_us, args.workers
        )
        result = {
            "artifact_type": "datavine-worker-churn-v1",
            "status": "PASS",
            "workflow_id": workflow_id,
            "execution_semantics": (
                "worker-pool-before-sealed-submit"
                if args.pre_admit else "sealed-before-worker-admission"
            ) if not args.admit_before_seal else "streamed-graph-workers-before-seal",
            "semantic_task_batching": False,
            "graph": {
                "logical_tasks": workflow_tasks,
                "lanes": args.lanes,
                "stages": args.stages,
                "edges": workflow_tasks - args.lanes,
                "data_records": workflow_tasks + 3,
                "requested_outputs": args.lanes,
                "spin_seconds_per_task": args.spin_seconds,
            },
            "faults": {
                "requested_removals": args.removals,
                "completed_removals": len(removal_records),
                "selection_seed": args.seed,
                "schedule": "midpoint of each equal progress bucket",
                "records": removal_records,
            },
            "correctness": {
                "terminal_state": info["state"],
                "sink_count": len(values),
                "expected_sink_value": 1,
                "all_sinks_match": True,
                "journal": journal_summary,
            },
            "physical_tasks": counts,
            "recomputation": {
                "extra_submissions": extra_submissions,
                "extra_completions": extra_completions,
                "journal_recoveries": journal_summary["recoveries"],
                "journal_retry_events": retry_events,
                "in_place_recovery_epochs": int(
                    stages.get("recovery_epochs", 0)
                ),
                "lost_data_ids": int(stages.get("recovery_lost_data", 0)),
                "invalidated_task_completions": invalidated,
                "abandoned_submissions": (
                    counts["submissions"] - counts["completions"]
                ),
            },
            "admission": {
                "workers": len(inventory),
                "cores_per_worker": inventory,
                "order": (
                    "before_submit" if args.pre_admit else
                    "before_seal" if args.admit_before_seal else "after_seal"
                ),
            },
            "worker_availability": utilization,
            "timing": {
                "graph_load_seconds": graph_loaded - load_started,
                "worker_admission_seconds": (
                    admitted - started if args.pre_admit else
                    admitted - graph_loaded if args.admit_before_seal else
                    admitted - sealed
                ),
                "sealed_to_terminal_seconds": terminal - sealed,
                "post_admission_seconds": terminal - admitted,
                "execution_seconds": terminal - execution_started,
                "result_validation_seconds": validated - terminal,
                "total_seconds": validated - started,
                "logical_tasks_per_second": workflow_tasks / (
                    terminal - execution_started
                ),
            },
            "transport": {
                "initial_payload_bytes": len(initial_body),
                "delta_count": len(chunk_sizes),
                "maximum_delta_payload_bytes": max(chunk_sizes, default=0),
                "total_delta_payload_bytes": sum(chunk_sizes),
                "rpc_profile": client.rpc_profile(reset=True),
            },
            "runtime_stages": stages,
            "peak": {"load": load_peak, "execute": execute_peak},
            "storage": tree_bytes(
                output / "journal",
                output / "journal.data",
                output / "journal.objects",
            ),
            "factory_command": list(factory_command),
            "environment": {
                "workers": args.workers,
                "cores_per_worker": args.cores,
                "batch_type": args.batch_type,
                "hostname": platform.node(),
                "python": sys.version,
                "maximum_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "commit": subprocess.run(
                    ("git", "rev-parse", "HEAD"),
                    cwd=repository,
                    text=True,
                    stdout=subprocess.PIPE,
                    check=True,
                ).stdout.strip(),
            },
        }
        (output / "summary.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n"
        )
        print(json.dumps({
            "status": "PASS",
            "output": str(output),
            "removals": len(removal_records),
            "recoveries": journal_summary["recoveries"],
            "logical_tasks_per_second": result["timing"]["logical_tasks_per_second"],
        }, sort_keys=True), flush=True)
    except BaseException as error:
        (output / "failure.json").write_text(json.dumps({
            "status": "FAIL",
            "type": type(error).__name__,
            "detail": str(error),
            "elapsed_seconds": time.monotonic() - started,
        }, indent=2, sort_keys=True) + "\n")
        raise
    finally:
        if sampler is not None:
            try:
                sampler.stop()
            except (EOFError, RuntimeError):
                pass
        if client is not None:
            client.close()
        terminate_group(factory)
        terminate_group(service)
        if factory_log is not None:
            factory_log.close()
        service_log.close()


if __name__ == "__main__":
    main()
