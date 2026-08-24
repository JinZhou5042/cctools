#!/usr/bin/env python3
"""Run the sealed DataVine side of the large data-intensive benchmark."""

import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time

from benchmark_cpu_fork import terminate_group
from benchmark_static_scale import tree_bytes, wait_scale_workers
from compare_workflows import PeakSampler, physical_counts, runtime_stages, start_resident_factory
from data_intensive_workload import SIZE_PROFILES, Workload, assert_full_contract, canonical_json, stage_source
from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


SCHEMA = "datavine.workflow/v1"
DELTA_SCHEMA = "datavine.workflow-delta/v1"
CODEC = {"name": "bytes", "version": "1"}
ARTIFACT_SCHEMA = "datavine.data-intensive-run/v1"
MAX_RPC_BYTES = 64 << 20


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def inline_record(data_id, payload):
    return {
        "data_id": data_id,
        "codec": CODEC,
        "origin": {
            "kind": "inline",
            "base64": base64.b64encode(payload).decode("ascii"),
        },
    }


def source_record(data_id, path):
    return {
        "data_id": data_id,
        "codec": CODEC,
        "origin": {"kind": "uri", "uri": path.resolve().as_uri()},
    }


def atomic_json(path, value):
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def load_dataset(root, workload):
    path = root / "dataset-manifest.json"
    manifest = json.loads(path.read_text())
    errors = []
    if manifest.get("status") != "PASS" or not all(manifest.get("gates", {}).values()):
        errors.append("dataset manifest is not PASS")
    if manifest.get("contract", {}).get("contract_sha256") != workload.contract()["contract_sha256"]:
        errors.append("dataset contract digest mismatch")
    if manifest.get("source_files") != workload.source_files:
        errors.append("dataset source count mismatch")
    if manifest.get("logical_bytes") != workload.source_bytes:
        errors.append("dataset source bytes mismatch")
    copy = dict(manifest)
    claimed = copy.pop("manifest_sha256", None)
    if hashlib.sha256(canonical_json(copy)).hexdigest() != claimed:
        errors.append("dataset manifest digest mismatch")
    if errors:
        raise RuntimeError({"dataset": str(path), "errors": errors})
    return manifest


def parse_queue_status(text):
    lines = [line.split() for line in text.splitlines() if line.strip()]
    for fields in reversed(lines):
        if len(fields) >= 7:
            try:
                waiting, running, complete, workers = map(int, fields[-4:])
            except ValueError:
                continue
            return {
                "tasks_waiting": waiting,
                "tasks_running": running,
                "tasks_complete": complete,
                "workers": workers,
            }
    return None


def parse_worker_cores(text):
    active = total = workers = 0
    for line in text.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 6:
            continue
        try:
            active += int(float(fields[4]))
            total += int(float(fields[5]))
        except ValueError:
            continue
        workers += 1
    return {"active_cores": active, "total_cores": total, "worker_rows": workers}


class ParallelismSampler:
    def __init__(self, vine_status, port, output, interval=1.0):
        self.vine_status = str(vine_status)
        self.port = int(port)
        self.output = output
        self.interval = float(interval)
        self.stop_event = threading.Event()
        self.thread = None
        self.samples = []

    def start(self):
        self.thread = threading.Thread(target=self._run, name="parallelism-sampler", daemon=True)
        self.thread.start()
        return self

    def _command(self, option):
        try:
            completed = subprocess.run(
                (self.vine_status, option, "localhost", str(self.port)),
                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20,
            )
        except subprocess.TimeoutExpired:
            return ""
        return completed.stdout if completed.returncode == 0 else ""

    def _run(self):
        with self.output.open("w") as stream:
            while not self.stop_event.is_set():
                queue = parse_queue_status(self._command("-Q"))
                cores = parse_worker_cores(self._command("-W"))
                sample = {"monotonic_seconds": time.monotonic(), **cores}
                if queue:
                    sample.update(queue)
                self.samples.append(sample)
                stream.write(json.dumps(sample, sort_keys=True) + "\n")
                stream.flush()
                self.stop_event.wait(self.interval)

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(30)
        return list(self.samples)


def parallelism_summary(samples, tasks, workers, cores):
    central = [
        item for item in samples
        if 0.05 * tasks <= item.get("tasks_complete", -1) <= 0.90 * tasks
    ]
    ready_running = [item.get("tasks_waiting", 0) + item.get("tasks_running", 0) for item in central]
    active = [item.get("active_cores", 0) for item in central]
    # At the inclusive 90%-complete edge, at most 10% of a workflow can still
    # be ready/running.  Cap the small-pilot threshold at that mathematical
    # maximum; the full contract still requires the intended 32,768 tasks.
    required_ready = min(32_768, max(1, tasks // 10))
    required_active = math.ceil(workers * cores * 0.90)
    return {
        "samples": len(samples),
        "central_samples": len(central),
        "minimum_ready_plus_running": min(ready_running) if ready_running else None,
        "maximum_ready_plus_running": max(ready_running) if ready_running else None,
        "minimum_active_cores": min(active) if active else None,
        "maximum_active_cores": max(active) if active else None,
        "required_ready_plus_running": required_ready,
        "required_active_cores": required_active,
        "gates": {
            "central_window_observed": bool(central),
            "ready_parallelism": bool(ready_running) and min(ready_running) >= required_ready,
            "active_parallelism": bool(active) and min(active) >= required_active,
        },
    }


def dominant_stage(log_path, workflow_id):
    pattern = re.compile(
        r"\bdominant_stage=(?P<stage>[a-z_]+) "
        r"dominant_stage_seconds=(?P<seconds>[0-9]+(?:\.[0-9]+)?)"
    )
    selected = None
    for line in log_path.read_text().splitlines():
        if f"datavine workflow {workflow_id} " not in line:
            continue
        match = pattern.search(line)
        if match:
            item = {
                "stage": match.group("stage"),
                "seconds": float(match.group("seconds")),
            }
            if selected is None or item["seconds"] > selected["seconds"]:
                selected = item
    if selected is None:
        raise RuntimeError(f"missing dominant stage for {workflow_id}")
    return selected


def append(client, workflow_id, generation, document, payload_sizes):
    body = encoded(document)
    if len(body) > MAX_RPC_BYTES:
        raise RuntimeError(f"delta payload exceeds RPC limit: {len(body)}")
    payload_sizes.append(len(body))
    info = client.append_workflow(workflow_id, generation, body)
    return int(info["generation"]), info


def stage_defaults(payload_ref):
    return {
        "executor": {
            "kind": "python",
            "version": "source-v1",
            "payload_ref": payload_ref,
            "output_files": ["datavine-python-output-0"],
        },
        "retry": {"maximum_attempts": 1},
        "resources": {"cores": 1},
    }


def load_workflow(client, workflow_id, dataset_root, workload, task_chunk):
    profile = SIZE_PROFILES[workload.size_profile]
    payloads = [
        stage_source("A", profile["a_output"]).encode(),
        stage_source("B", profile["b_output"]).encode(),
        stage_source("C", profile["c_output"]).encode(),
    ]
    digests = [hashlib.sha256(payload).hexdigest() for payload in payloads]
    initial = {
        "schema": SCHEMA,
        "workflow_id": workflow_id,
        "idempotency_key": f"{workflow_id}-initial",
        "mode": "streaming",
        "tasks": [],
        "data": [inline_record(index + 1, payload) for index, payload in enumerate(payloads)],
        "requested_outputs": [],
        "policy": {"maximum_tasks": workload.tasks, "maximum_edges": workload.scheduler_edges},
        "metadata": {
            "benchmark_contract_sha256": workload.contract()["contract_sha256"],
            "execution_semantics": "sealed-before-worker-admission",
            "semantic_task_batching": False,
        },
    }
    initial_body = encoded(initial)
    info = client.submit_workflow(initial_body)
    generation = int(info["generation"])
    payload_sizes = [len(initial_body)]

    stages = (
        ("A", workload.a_tasks, 0, 1, None, workload.a_input_data_ids),
        ("B", workload.b_tasks, workload.a_tasks, 2, workload.b_data_first, workload.b_input_data_ids),
        ("C", workload.c_tasks, workload.a_tasks + workload.b_tasks, 3, workload.c_data_first, workload.c_input_data_ids),
    )
    for stage, count, task_offset, payload_ref, data_first, inputs_for in stages:
        for first in range(0, count, task_chunk):
            last = min(count, first + task_chunk)
            tasks = []
            data = []
            requested = []
            for local in range(first, last):
                task_id = task_offset + local + 1
                if stage == "A":
                    source_first = local * 36
                    for source_index in range(source_first, source_first + 36):
                        data.append(source_record(
                            workload.source_data_id(source_index),
                            dataset_root / workload.source_path(source_index),
                        ))
                    output_id = workload.a_data_id(local)
                else:
                    output_id = data_first + local
                tasks.append([task_id, list(inputs_for(local)), [output_id]])
                data.append([output_id, task_id, 0])
                if stage == "C":
                    requested.append(output_id)
            delta = {
                "schema": DELTA_SCHEMA,
                "workflow_id": workflow_id,
                "idempotency_key": f"{workflow_id}-{stage}-{first}-{last}",
                "task_defaults": stage_defaults(payload_ref),
                "data_defaults": {"codec": CODEC},
                "tasks": tasks, "data": data, "requested_outputs": requested,
            }
            generation, info = append(client, workflow_id, generation, delta, payload_sizes)
            print(json.dumps({"phase": f"load-{stage}", "loaded": last, "total": count}), flush=True)
    info = client.seal_workflow(workflow_id, generation)
    return info, payload_sizes, digests


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--cohorts", type=int, default=64)
    parser.add_argument("--scale", type=int, default=64)
    parser.add_argument("--size-profile", choices=("tiny", "full"), default="full")
    parser.add_argument("--workers", type=int, default=128)
    parser.add_argument("--cores", type=int, default=16)
    parser.add_argument("--batch-type", choices=("local", "condor"), default="condor")
    parser.add_argument("--task-chunk", type=int, default=2_000)
    parser.add_argument("--timeout", type=float, default=24 * 60 * 60)
    parser.add_argument("--acceptance", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()
    if min(args.cohorts, args.scale, args.workers, args.cores, args.task_chunk) < 1:
        parser.error("numeric workload arguments must be positive")
    return args


def main():
    args = parse_args()
    workload = Workload(args.cohorts, args.scale, args.size_profile)
    contract = workload.contract()
    if args.acceptance:
        assert_full_contract(workload)
        if (args.workers, args.cores, args.batch_type) != (128, 16, "condor"):
            raise ValueError("acceptance requires exactly 128 Condor workers x 16 cores")
    if args.plan_only:
        print(json.dumps({"status": "PASS", "contract": contract, "execution": vars(args)}, indent=2, sort_keys=True, default=str))
        return 0
    dataset_root = args.dataset_root.resolve()
    dataset = load_dataset(dataset_root, workload)
    output = args.output_dir.resolve()
    if output.exists() and any(output.iterdir()):
        raise RuntimeError(f"output directory is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    repository = Path(__file__).resolve().parents[2]
    service_log_path = output / "service.log"
    factory_log_path = output / "factory.log"
    service_log = service_log_path.open("w")
    runtime_info_root = Path(tempfile.mkdtemp(
        prefix="datavine-data-intensive-run-info-", dir="/tmp"
    ))
    run_succeeded = False
    service = subprocess.Popen(
        (str(repository / "taskvine/src/tools/datavine_workflow"), "serve", str(output / "journal"), "data-intensive-benchmark"),
        stdout=subprocess.PIPE, stderr=service_log, text=True, start_new_session=True,
        env=dict(
            os.environ,
            DATAVINE_WORKFLOW_METRICS="1",
            DATAVINE_RUNTIME_INFO_PATH=str(runtime_info_root),
        ),
    )
    factory = factory_log = client = sampler = peak = None
    workflow_id = f"data-intensive-c{args.cohorts}-s{args.scale}-{int(time.time())}"
    started = time.monotonic()
    try:
        contact_line = service.stdout.readline()
        if not contact_line:
            raise RuntimeError("DataVine service exited before publishing contact")
        contact = json.loads(contact_line)
        client = WorkflowClient(contact["endpoint"], "data-intensive-benchmark", timeout=300)
        client.rpc_profile(reset=True)
        load_started = time.monotonic()
        info, payload_sizes, code_digests = load_workflow(
            client, workflow_id, dataset_root, workload, args.task_chunk
        )
        sealed = time.monotonic()
        expected_info = {
            "tasks": workload.tasks,
            "data": workload.data_records,
            "edges": workload.scheduler_edges,
            "requested_outputs": workload.c_tasks,
        }
        for name, expected in expected_info.items():
            if info.get(name) != expected:
                raise AssertionError((name, info.get(name), expected))
        factory, factory_log, factory_command = start_resident_factory(
            output / "factory-state", contact["manager_port"], args.workers, args.cores,
            repository / "taskvine/src/worker/vine_worker", factory_log_path, args.batch_type,
        )
        inventory = wait_scale_workers(
            repository / "taskvine/src/tools/vine_status", contact["manager_port"],
            args.workers, args.cores, factory, timeout=3600,
        )
        admitted = time.monotonic()
        peak = PeakSampler((os.getpid(), service.pid, factory.pid)).start()
        sampler = ParallelismSampler(
            repository / "taskvine/src/tools/vine_status", contact["manager_port"],
            output / "parallelism.jsonl",
        ).start()
        info = client.wait_workflow(workflow_id, timeout=args.timeout)
        terminal = time.monotonic()
        samples = sampler.stop()
        sampler = None
        peak_result = peak.stop()
        peak = None
        if info["state"] != "completed":
            raise RuntimeError(info)
        sample_count = min(16, workload.c_tasks)
        stride = max(1, workload.c_tasks // sample_count)
        result_ids = [workload.c_data_first + index for index in range(0, workload.c_tasks, stride)][:sample_count]
        results = client.fetch_workflow_results(workflow_id, result_ids)
        expected_result_bytes = SIZE_PROFILES[workload.size_profile]["c_output"]
        if any(len(value) != expected_result_bytes for value in results):
            raise AssertionError("sampled result size mismatch")
        fetched = time.monotonic()
        final_inventory = wait_scale_workers(
            repository / "taskvine/src/tools/vine_status", contact["manager_port"],
            args.workers, args.cores, factory, timeout=min(args.timeout, 300),
        )
        recovered = time.monotonic()
        service_log.flush()
        counts = physical_counts(service_log_path, workflow_id)
        stages = runtime_stages(service_log_path, workflow_id)
        dominant = dominant_stage(service_log_path, workflow_id)
        exact_physical = counts == {"submissions": workload.tasks, "completions": workload.tasks}
        parallelism = parallelism_summary(samples, workload.tasks, args.workers, args.cores)
        intermediate_files = workload.a_tasks + workload.b_tasks
        recovery_limit = stages.get("recovery_cache_limit", 0)
        if intermediate_files <= recovery_limit:
            gc_pressure = (
                stages.get("recovery_cache_peak") == intermediate_files
                and stages.get("recovery_cache_evictions") == 0
            )
        else:
            gc_pressure = (
                stages.get("recovery_cache_peak") == recovery_limit
                and stages.get("recovery_cache_evictions", 0) >= intermediate_files - recovery_limit
            )
        gates = {
            "dataset_manifest": dataset["status"] == "PASS",
            "exact_logical_counts": all(info[name] == expected for name, expected in expected_info.items()),
            "exact_physical_counts": exact_physical,
            "worker_churn_recovered": (
                len(final_inventory) == args.workers
                and set(final_inventory) == {args.cores}
            ),
            "exact_worker_pool": len(inventory) == args.workers and set(inventory) == {args.cores},
            "sampled_outputs": len(results) == sample_count,
            "all_outputs_worker_local": stages.get("publication_remote_outputs") == workload.tasks,
            "durable_outputs_only_requested": stages.get("publication_durable_outputs") == workload.c_tasks,
            "manager_output_payload_bypass": (
                stages.get("manager_bytes_received") == 0
                and stages.get("task_bytes_received") == 0
            ),
            "exact_sharedfs_source_bytes": (
                stages.get("url_stage_in_bytes") == workload.source_bytes
            ),
            "data_path_is_runtime_bottleneck": dominant["stage"] in {
                "data_stage_in", "data_object_pull", "data_publish",
            },
            "gc_pressure_accounted": gc_pressure,
            **parallelism["gates"],
        }
        result = {
            "schema": ARTIFACT_SCHEMA,
            "status": "PASS" if all(gates.values()) else "FAIL",
            "workflow_id": workflow_id,
            "contract": contract,
            "dataset_manifest_sha256": dataset["manifest_sha256"],
            "gates": gates,
            "workflow_info": info,
            "physical_tasks": counts,
            "runtime_stages": stages,
            "dominant_stage": dominant,
            "parallelism": parallelism,
            "admission": {"workers": len(inventory), "cores_per_worker": inventory},
            "final_worker_pool": {
                "workers": len(final_inventory),
                "cores_per_worker": final_inventory,
                "manager_workers_removed": stages.get("manager_workers_removed", 0),
            },
            "transport": {
                "delta_payloads": len(payload_sizes) - 1,
                "maximum_payload_bytes": max(payload_sizes),
                "total_payload_bytes": sum(payload_sizes),
                "rpc_profile": client.rpc_profile(reset=True),
            },
            "timing": {
                "graph_load_seconds": sealed - load_started,
                "worker_admission_seconds": admitted - sealed,
                "execution_seconds": terminal - admitted,
                "result_validation_seconds": fetched - terminal,
                "worker_pool_recovery_seconds": recovered - fetched,
                "total_seconds": fetched - started,
                "tasks_per_second": workload.tasks / (terminal - admitted),
            },
            "sampled_results": {
                "count": len(results),
                "bytes_each": expected_result_bytes,
                "sha256": [hashlib.sha256(value).hexdigest() for value in results],
            },
            "peak": peak_result,
            "storage": tree_bytes(output / "journal", output / "journal.data", output / "journal.objects"),
            "factory_command": list(factory_command),
            "environment": {
                "hostname": platform.node(),
                "python": sys.version,
                "commit": subprocess.run(("git", "rev-parse", "HEAD"), cwd=repository, text=True, stdout=subprocess.PIPE, check=True).stdout.strip(),
                "code_digests": code_digests,
                "runtime_info_storage": "node-local-temporary",
            },
        }
        atomic_json(output / "summary.json", result)
        run_succeeded = result["status"] == "PASS"
        print(json.dumps({"status": result["status"], "output": str(output), "gates": gates}, sort_keys=True), flush=True)
        return 0 if result["status"] == "PASS" else 1
    except BaseException as error:
        atomic_json(output / "failure.json", {
            "schema": ARTIFACT_SCHEMA,
            "status": "FAIL",
            "workflow_id": workflow_id,
            "error_type": type(error).__name__,
            "detail": str(error),
            "elapsed_seconds": time.monotonic() - started,
            "runtime_info_root": str(runtime_info_root),
        })
        raise
    finally:
        if sampler is not None:
            sampler.stop()
        if peak is not None:
            try:
                peak.stop()
            except (EOFError, RuntimeError):
                pass
        if client is not None:
            client.close()
        # vine_factory removes 128 Condor jobs serially during SIGTERM cleanup;
        # allow that graceful path to finish instead of killing it after the
        # generic 30-second service timeout and leaking the tail of the pool.
        terminate_group(factory, timeout=300)
        terminate_group(service)
        if run_succeeded:
            shutil.rmtree(runtime_info_root, ignore_errors=True)
        if factory_log is not None:
            factory_log.close()
        service_log.close()


if __name__ == "__main__":
    sys.exit(main())
