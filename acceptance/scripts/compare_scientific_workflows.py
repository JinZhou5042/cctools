#!/usr/bin/env python3
"""Run the HEP-S model under explicit TaskVine and DataVine contracts."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import site
import statistics
import subprocess
import sys
import time

import cloudpickle
import ndcctools.taskvine as vine
from ndcctools.taskvine.datavine.workflow import Workflow
from ndcctools.taskvine.datavine.workflow_client import WorkflowClient

from compare_workflows import (
    PeakSampler,
    difference,
    physical_counts,
    runtime_stages,
    taskvine_directory,
    taskvine_stats,
    start_resident_factory,
    terminate_group,
    wait_datavine,
    wait_workers,
    wait_taskvine_pool,
)
from generate_scientific_data import verify as verify_manifest
from sample_remote_resources import CALIBRATION_SCHEMA


ARTIFACT_TYPE = "datavine-scientific-workflow-benchmark"
SCHEMA_VERSION = 1
CONTRACTS = ("TV-native", "DV-native", "TV-durable-sink")


def load_shard(value):
    if isinstance(value, (str, os.PathLike)):
        with open(value, "rb") as stream:
            value = cloudpickle.load(stream)
    if not isinstance(value, bytes):
        raise TypeError("HEP shard must decode to bytes")
    return value


def scan_shard(value, shard, histogram_bins=64, chunk_bytes=1024 * 1024):
    """Stream all shard bytes and emit a deterministic partial histogram."""

    started = time.process_time_ns()
    payload = load_shard(value)
    bins = [0] * int(histogram_bins)
    digest = hashlib.sha256()
    for offset in range(0, len(payload), int(chunk_bytes)):
        chunk = memoryview(payload)[offset:offset + int(chunk_bytes)]
        digest.update(chunk)
        summary = hashlib.blake2b(chunk, digest_size=64).digest()
        for index, item in enumerate(summary):
            bins[index % len(bins)] += item
    cpu_ns = time.process_time_ns() - started
    key = f"scan-{int(shard)}"
    return {
        "histogram": bins,
        "records": len(payload) // 32,
        "input_bytes": len(payload),
        "shard_digests": [digest.hexdigest()],
        "cpu_by_task": {key: cpu_ns},
    }


def transform_histogram(value, shard):
    """Apply two deterministic integer calibration transforms."""

    started = time.process_time_ns()
    bins = []
    for index, count in enumerate(value["histogram"]):
        pedestal_removed = max(0, int(count) - ((int(shard) + index) % 11))
        bins.append((pedestal_removed * (1000 + index % 7) + 500) // 1000)
    cpu_by_task = dict(value["cpu_by_task"])
    cpu_by_task[f"transform-{int(shard)}"] = time.process_time_ns() - started
    return {
        "histogram": bins,
        "records": int(value["records"]),
        "input_bytes": int(value["input_bytes"]),
        "shard_digests": list(value["shard_digests"]),
        "cpu_by_task": cpu_by_task,
    }


def reduce_histograms(key, *values):
    """Merge a fixed tree node without changing input ordering."""

    started = time.process_time_ns()
    if not values:
        raise ValueError("reduction requires inputs")
    width = len(values[0]["histogram"])
    bins = [0] * width
    records = input_bytes = 0
    shard_digests = []
    cpu_by_task = {}
    for value in values:
        if len(value["histogram"]) != width:
            raise ValueError("histogram width mismatch")
        for index, count in enumerate(value["histogram"]):
            bins[index] += int(count)
        records += int(value["records"])
        input_bytes += int(value["input_bytes"])
        shard_digests.extend(value["shard_digests"])
        cpu_by_task.update(value["cpu_by_task"])
    cpu_by_task[str(key)] = time.process_time_ns() - started
    return {
        "histogram": bins,
        "records": records,
        "input_bytes": input_bytes,
        "shard_digests": shard_digests,
        "cpu_by_task": cpu_by_task,
    }


def result_summary(value):
    canonical = {
        "histogram": [int(item) for item in value["histogram"]],
        "records": int(value["records"]),
        "input_bytes": int(value["input_bytes"]),
        "shard_digests": list(value["shard_digests"]),
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    return {
        "digest": hashlib.sha256(payload).hexdigest(),
        "record_count": canonical["records"],
        "input_bytes": canonical["input_bytes"],
        "histogram": canonical["histogram"],
        "shard_digests": canonical["shard_digests"],
        "useful_cpu_seconds": sum(value["cpu_by_task"].values()) / 1e9,
    }


def reduction_levels(values, fan_in, reducer):
    level = 0
    tasks = 0
    current = list(values)
    while len(current) > 1:
        following = []
        for group, start in enumerate(range(0, len(current), fan_in)):
            key = f"reduce-{level}-{group}"
            following.append(reducer(key, current[start:start + fan_in]))
            tasks += 1
        current = following
        level += 1
    return current[0], tasks, level


def reference_result(manifest, fan_in, histogram_bins):
    root = Path(manifest["_path"]).parent
    transformed = []
    for record in manifest["files"]:
        value = scan_shard(
            root / record["path"], record["shard"], histogram_bins
        )
        transformed.append(transform_histogram(value, record["shard"]))
    final, reduction_tasks, levels = reduction_levels(
        transformed, fan_in,
        lambda key, group: reduce_histograms(key, *group),
    )
    return result_summary(final), reduction_tasks, levels


def load_input_manifest(path):
    verification = verify_manifest(path, full_hash=True)
    if verification["status"] != "PASS":
        raise ValueError(verification)
    manifest_path = path if path.is_file() else path / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("format") != "cloudpickle-bytes":
        raise ValueError("HEP-S requires generator format cloudpickle-bytes")
    manifest["_path"] = str(manifest_path.resolve())
    return manifest, verification


def build_datavine(pool, manifest, fan_in, histogram_bins, workflow_id):
    root = Path(manifest["_path"]).parent
    task_count = 2 * len(manifest["files"])
    edges = len(manifest["files"])
    builder = Workflow(
        f"{workflow_id}-v1", workflow_id=workflow_id,
        maximum_tasks=4 * len(manifest["files"]) + 1,
        maximum_edges=8 * len(manifest["files"]),
        metadata={"workload": "HEP-S-local", "contract": "DV-native"},
    )
    transformed = []
    for record in manifest["files"]:
        source = builder.uri(
            (root / record["path"]).resolve().as_uri(),
            codec=("python/cloudpickle", "3"),
            content_sha256=record["stored_sha256"],
        )
        scanned = builder.python_callable(
            scan_shard, source, record["shard"], histogram_bins
        )
        transformed.append(builder.python_callable(
            transform_histogram, scanned, record["shard"]
        ))
    def reduce_group(key, group):
        return builder.python_callable(reduce_histograms, key, *group)
    final, reduction_tasks, _ = reduction_levels(transformed, fan_in, reduce_group)
    task_count += reduction_tasks
    edges = 2 * len(manifest["files"]) + reduction_tasks - 1
    builder.request(final)
    return builder, final, task_count, edges


def run_datavine(pool, root, manifest, fan_in, histogram_bins, repetition):
    root.mkdir(parents=True)
    workflow_id = f"scientific-hep-s-dv-r{repetition}"
    sampler = PeakSampler((os.getpid(), pool.service.pid, pool.factory.pid)).start()
    started = time.monotonic()
    builder, final, logical_tasks, _logical_edges = build_datavine(
        pool, manifest, fan_in, histogram_bins, workflow_id
    )
    built = time.monotonic()
    builder.submit(pool.client)
    submitted = time.monotonic()
    state, _ = wait_datavine(
        pool.client, workflow_id, (pool.service.pid, pool.factory.pid)
    )
    executed = time.monotonic()
    if state["state"] != "completed":
        raise RuntimeError(state)
    serialized = pool.client.fetch_workflow_result(workflow_id, final.data_id)
    value = cloudpickle.loads(serialized)
    fetched = time.monotonic()
    peak = sampler.stop()
    pool.service_log.flush()
    counts = physical_counts(pool.service_log_path, workflow_id)
    expected = {"submissions": logical_tasks, "completions": logical_tasks}
    if counts != expected:
        raise RuntimeError({"expected": expected, "actual": counts})
    summary = result_summary(value)
    return {
        "contract": "DV-native",
        "repetition": repetition,
        "status": "PASS",
        "counts": {
            "logical": logical_tasks,
            "submitted": counts["submissions"],
            "completed": counts["completions"],
            "failed": 0,
            "retried": 0,
        },
        "timings": {
            "build_seconds": built - started,
            "submit_seconds": submitted - built,
            "execute_seconds": executed - submitted,
            "fetch_seconds": fetched - executed,
            "total_seconds": fetched - started,
        },
        "results": {
            "digest": summary["digest"],
            "requested_bytes": len(serialized),
            "record_count": summary["record_count"],
        },
        "metrics": {
            "useful_cpu_seconds": summary["useful_cpu_seconds"],
            "peak_rss_bytes": peak["rss_bytes"],
            "manager_bytes_sent": None,
            "manager_bytes_received": None,
            "network_bytes": None,
            "process_read_bytes": None,
            "process_write_bytes": None,
            "runtime_stages": runtime_stages(pool.service_log_path, workflow_id),
        },
        "error": None,
    }


class ScientificTaskVinePool:
    """One resident TaskVine pool with only the HEP-S fork library."""

    def __init__(self, repository, root, workers, cores, batch_type):
        root.mkdir(parents=True)
        self.executor = vine.FuturesExecutor(port=0, factory=False)
        self.library = "datavine-scientific-hep-s"
        library = self.executor.manager.create_library_from_functions(
            self.library, scan_shard, transform_histogram, reduce_histograms,
            add_env=False, exec_mode="fork",
        )
        library.set_cores(cores)
        self.executor.install_library(library)
        manager_host_port = (
            None if batch_type == "local" else
            f"{socket.getfqdn()}:{self.executor.manager.port}"
        )
        self.factory = vine.Factory(
            batch_type=batch_type,
            manager=self.executor.manager,
            manager_host_port=manager_host_port,
            worker_binary=str(repository / "taskvine/src/worker/vine_worker"),
            log_file=str(root / "factory.log"),
        )
        self.factory.min_workers = workers
        self.factory.max_workers = workers
        self.factory.workers_per_cycle = workers
        self.factory.cores = cores
        self.factory.memory = 2048
        self.factory.disk = 4096
        self.factory.timeout = 840
        self.factory.factory_timeout = 900
        self.factory.start()
        self.inventory = wait_taskvine_pool(
            self.executor.manager, self.factory, workers, cores
        )
        warmup = self.executor.future_funcall(
            self.library, "reduce_histograms", "warmup", {
                "histogram": [1], "records": 1, "input_bytes": 1,
                "shard_digests": ["0" * 64], "cpu_by_task": {},
            }
        )
        warmup.set_cores(1)
        self.executor.submit(warmup).result()

    def close(self):
        self.factory.stop()
        self.executor.manager.__del__()


class ScientificDataVinePool:
    """Resident DataVine pool dedicated to one scientific workflow."""

    def __init__(self, repository, root, workers, cores, batch_type):
        root.mkdir(parents=True)
        self.service_log_path = root / "service.log"
        self.service_log = self.service_log_path.open("w")
        self.service = None
        self.factory = None
        self.factory_log = None
        self.client = None
        try:
            self.service = subprocess.Popen(
                (str(repository / "taskvine/src/tools/datavine_workflow"),
                 "serve", str(root / "journal"), "scientific-workflow"),
                stdout=subprocess.PIPE,
                stderr=self.service_log,
                text=True,
                start_new_session=True,
                env=dict(
                    os.environ,
                    DATAVINE_WORKFLOW_METRICS="1",
                    DATAVINE_RUNTIME_INFO_PATH=str(root / "run-info"),
                ),
            )
            contact_line = self.service.stdout.readline()
            if not contact_line:
                raise RuntimeError("DataVine service exited before contact")
            contact = json.loads(contact_line)
            (self.factory, self.factory_log,
             self.factory_command) = start_resident_factory(
                root / "factory-state", contact["manager_port"], workers, cores,
                repository / "taskvine/src/worker/vine_worker",
                root / "factory.log", batch_type,
            )
            self.inventory = wait_workers(
                repository / "taskvine/src/tools/vine_status",
                contact["manager_port"], workers, cores, self.factory,
                timeout=900,
            )
            self.client = WorkflowClient(
                contact["endpoint"], "scientific-workflow"
            )
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None
        terminate_group(self.factory)
        terminate_group(self.service)
        if self.factory_log is not None:
            self.factory_log.close()
            self.factory_log = None
        if self.service_log is not None:
            self.service_log.close()
            self.service_log = None


def build_taskvine(pool, manifest, fan_in, histogram_bins, library):
    manager = pool.executor.manager
    root = Path(manifest["_path"]).parent
    transformed = []
    tasks = []
    for record in manifest["files"]:
        remote_name = f"scientific-{record['path']}"
        declared = manager.declare_file(
            str(root / record["path"]), cache=True, peer_transfer=True
        )
        scan = pool.executor.future_funcall(
            library, "scan_shard", remote_name, record["shard"], histogram_bins
        )
        scan.add_input(declared, remote_name)
        scan.set_cores(1)
        scanned = pool.executor.submit(scan)
        transform = pool.executor.future_funcall(
            library, "transform_histogram", scanned, record["shard"]
        )
        transform.set_cores(1)
        transformed.append(pool.executor.submit(transform))
        tasks.extend((scan, transform))
    def reduce_group(key, group):
        task = pool.executor.future_funcall(
            library, "reduce_histograms", key, *group
        )
        task.set_cores(1)
        tasks.append(task)
        return pool.executor.submit(task)
    final, reduction_tasks, _ = reduction_levels(
        transformed, fan_in, reduce_group
    )
    return final, len(tasks), reduction_tasks


class HashingWriter:
    def __init__(self, stream):
        self.stream = stream
        self.digest = hashlib.sha256()
        self.size = 0

    def write(self, value):
        written = self.stream.write(value)
        self.digest.update(memoryview(value)[:written])
        self.size += written
        return written

    def flush(self):
        return self.stream.flush()

    def fileno(self):
        return self.stream.fileno()


def durable_sink(root, contract, repetition, value):
    root.mkdir(parents=True, exist_ok=True)
    target = root / f"hep-s-{contract.lower()}-r{repetition}.pkl"
    temporary = target.with_name(target.name + ".part")
    started = time.monotonic()
    with temporary.open("xb") as stream:
        writer = HashingWriter(stream)
        cloudpickle.dump(value, writer)
        writer.flush()
        os.fsync(writer.fileno())
    os.replace(temporary, target)
    metadata = {
        "path": target.name,
        "bytes": writer.size,
        "sha256": writer.digest.hexdigest(),
    }
    metadata_path = target.with_suffix(".json")
    with metadata_path.open("x") as stream:
        json.dump(metadata, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return metadata, time.monotonic() - started


def run_taskvine(pool, root, sink_root, manifest, fan_in, histogram_bins,
                 repetition, contract, library):
    root.mkdir(parents=True)
    baseline = taskvine_stats(pool.executor.manager)
    sampler = PeakSampler((os.getpid(), pool.factory._factory_proc.pid)).start()
    started = time.monotonic()
    with taskvine_directory(root):
        final, logical_tasks, _ = build_taskvine(
            pool, manifest, fan_in, histogram_bins, library
        )
    built = time.monotonic()
    outcome = vine.futures.wait((final,), timeout=900)
    if outcome.not_done:
        raise TimeoutError("HEP-S final result did not complete")
    value = final.result()
    executed_and_fetched = time.monotonic()
    sink = None
    sink_seconds = 0.0
    if contract == "TV-durable-sink":
        sink, sink_seconds = durable_sink(sink_root, contract, repetition, value)
    completed = time.monotonic()
    peak = sampler.stop()
    counts = difference(taskvine_stats(pool.executor.manager), baseline)
    if (counts["tasks_submitted"] != logical_tasks or
            counts["tasks_done"] != logical_tasks or
            counts["tasks_successful"] != logical_tasks or
            counts["tasks_failed"] != 0):
        raise RuntimeError({"logical_tasks": logical_tasks, "stats": counts})
    summary = result_summary(value)
    serialized = cloudpickle.dumps(value)
    requested_bytes = sink["bytes"] if sink else len(serialized)
    return {
        "contract": contract,
        "repetition": repetition,
        "status": "PASS",
        "counts": {
            "logical": logical_tasks,
            "submitted": counts["tasks_submitted"],
            "completed": counts["tasks_done"],
            "failed": counts["tasks_failed"],
            "retried": 0,
        },
        "timings": {
            "build_seconds": built - started,
            "execute_seconds": executed_and_fetched - built,
            "fetch_seconds": 0.0,
            "durable_sink_seconds": sink_seconds,
            "total_seconds": completed - started,
        },
        "results": {
            "digest": summary["digest"],
            "requested_bytes": requested_bytes,
            "record_count": summary["record_count"],
        },
        "metrics": {
            "useful_cpu_seconds": summary["useful_cpu_seconds"],
            "peak_rss_bytes": peak["rss_bytes"],
            "manager_bytes_sent": counts["bytes_sent"],
            "manager_bytes_received": counts["bytes_received"],
            "network_bytes": None,
            "process_read_bytes": None,
            "process_write_bytes": None,
        },
        "error": None,
    }


def validate_artifact(report, schema_path):
    schema = json.loads(schema_path.read_text())
    errors = []
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        errors.append("schema is not draft 2020-12")
    required = schema.get("required", ())
    errors.extend(f"missing top-level field {key}" for key in required if key not in report)
    unknown_top = set(report) - set(schema.get("properties", {}))
    if unknown_top:
        errors.append(f"unknown top-level fields {sorted(unknown_top)}")
    if report.get("artifact_type") != ARTIFACT_TYPE:
        errors.append("artifact_type mismatch")
    if report.get("schema_version") != SCHEMA_VERSION:
        errors.append("schema_version mismatch")
    if not set(report.get("contracts", ())).issubset(CONTRACTS):
        errors.append("unknown contract")
    for run in report.get("runs", ()):
        run_schema = schema["$defs"]["run"]
        unknown_run = set(run) - set(run_schema["properties"])
        missing_run = set(run_schema["required"]) - set(run)
        if unknown_run:
            errors.append(f"unknown run fields {sorted(unknown_run)}")
        if missing_run:
            errors.append(f"missing run fields {sorted(missing_run)}")
        if run.get("contract") not in report.get("contracts", ()):
            errors.append("run contract was not declared")
        if run.get("status") != "PASS":
            errors.append("non-PASS run")
        counts = run.get("counts", {})
        if not (counts.get("logical") == counts.get("submitted") ==
                counts.get("completed") and counts.get("failed") == 0):
            errors.append(f"inexact task counts for {run.get('contract')}")
        digest = run.get("results", {}).get("digest", "")
        if len(digest) != 64:
            errors.append(f"invalid result digest for {run.get('contract')}")
    if report.get("status") != ("PASS" if not errors else "FAIL"):
        errors.append("artifact status does not match validation")
    return errors


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


def summarize_runs(runs):
    result = {}
    for contract in sorted({item["contract"] for item in runs}):
        values = [item for item in runs if item["contract"] == contract]
        result[contract] = {
            "repetitions": len(values),
            "median_total_seconds": statistics.median(
                item["timings"]["total_seconds"] for item in values
            ),
            "median_requested_bytes": statistics.median(
                item["results"]["requested_bytes"] for item in values
            ),
        }
    return result


def cleanup_complete(pools, output):
    processes = []
    datavine = pools.get("datavine")
    if datavine is not None:
        processes.extend((datavine.service, datavine.factory))
    taskvine = pools.get("taskvine")
    if taskvine is not None:
        processes.append(getattr(taskvine.factory, "_factory_proc", None))
    stopped = all(
        process is None or process.poll() is not None for process in processes
    )
    return stopped and not any(output.rglob("*.part"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--sampler-calibration", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--contracts", default=",".join(CONTRACTS))
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--cores", type=int, default=1)
    parser.add_argument("--batch-type", choices=("local", "condor"), default="local")
    parser.add_argument("--fan-in", type=int, default=4)
    parser.add_argument("--histogram-bins", type=int, default=64)
    args = parser.parse_args()
    if min(args.repetitions, args.workers, args.cores, args.histogram_bins) < 1:
        parser.error("repetitions, workers, cores, and histogram-bins must be positive")
    if args.fan_in < 2:
        parser.error("fan-in must be at least two")
    contracts = tuple(item.strip() for item in args.contracts.split(",") if item.strip())
    if not contracts or len(set(contracts)) != len(contracts) or not set(contracts).issubset(CONTRACTS):
        parser.error("contracts must be unique named benchmark contracts")
    output = args.output_dir.resolve()
    if output.exists() and any(output.iterdir()):
        parser.error(f"output directory is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    manifest, manifest_gate = load_input_manifest(args.manifest.resolve())
    calibration = json.loads(args.sampler_calibration.read_text())
    if (calibration.get("schema") != CALIBRATION_SCHEMA or
            calibration.get("status") != "PASS"):
        parser.error("sampler calibration is not a PASS artifact")
    expected, reduction_tasks, levels = reference_result(
        manifest, args.fan_in, args.histogram_bins
    )
    logical_tasks = 2 * len(manifest["files"]) + reduction_tasks
    repository = Path(__file__).resolve().parents[2]
    pools = {}
    runs = []
    try:
        if any(contract.startswith("TV-") for contract in contracts):
            pools["taskvine"] = ScientificTaskVinePool(
                repository, output / "pools/taskvine",
                args.workers, args.cores, args.batch_type,
            )
        for repetition in range(1, args.repetitions + 1):
            for contract in contracts:
                root = output / "runs" / f"r{repetition}-{contract.lower()}"
                if contract == "DV-native":
                    # A DataVine service owns exactly one workflow.  Create a
                    # fresh service/factory pair for each measured run so
                    # repetitions exercise the production singleton boundary.
                    datavine = ScientificDataVinePool(
                        repository,
                        output / "pools" / f"datavine-r{repetition}",
                        args.workers, args.cores, args.batch_type,
                    )
                    pools["datavine"] = datavine
                    try:
                        run = run_datavine(
                            datavine, root, manifest, args.fan_in,
                            args.histogram_bins, repetition,
                        )
                    finally:
                        datavine.close()
                else:
                    run = run_taskvine(
                        pools["taskvine"], root, output / "durable-sinks",
                        manifest, args.fan_in, args.histogram_bins,
                        repetition, contract, pools["taskvine"].library,
                    )
                if run["results"]["digest"] != expected["digest"]:
                    raise RuntimeError(f"{contract}: result mismatch")
                peer = next((item for item in runs
                    if item["contract"] == contract), None)
                if peer and peer["results"]["digest"] != run["results"]["digest"]:
                    raise RuntimeError(f"{contract}: repetition mismatch")
                root.mkdir(parents=True, exist_ok=True)
                (root / "result.json").write_text(
                    json.dumps(run, indent=2, sort_keys=True) + "\n"
                )
                runs.append(run)
                print(json.dumps({
                    "contract": contract,
                    "repetition": repetition,
                    "seconds": run["timings"]["total_seconds"],
                    "status": run["status"],
                }, sort_keys=True), flush=True)
    finally:
        if "datavine" in pools:
            pools["datavine"].close()
        if "taskvine" in pools:
            pools["taskvine"].close()
    cleanup_gate = cleanup_complete(pools, output)
    digests = {item["results"]["digest"] for item in runs}
    gates = {
        "artifact_schema": True,
        "input_manifest": manifest_gate["status"] == "PASS",
        "exact_results": digests == {expected["digest"]},
        "exact_task_counts": all(
            item["counts"]["logical"] == logical_tasks and
            item["counts"]["submitted"] == logical_tasks and
            item["counts"]["completed"] == logical_tasks and
            item["counts"]["failed"] == 0 for item in runs
        ),
        "resource_accounting": calibration["status"] == "PASS",
        "cleanup": cleanup_gate,
    }
    commit = subprocess.run(
        ("git", "rev-parse", "HEAD"), cwd=repository, check=True,
        text=True, stdout=subprocess.PIPE,
    ).stdout.strip()
    scope = (
        "cluster-pilot" if args.batch_type != "local" else
        "local-pilot" if args.workers * args.cores == 1 else "local-scale"
    )
    report = {
        "artifact_type": ARTIFACT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": "PASS" if all(gates.values()) else "FAIL",
        "scope": scope,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "environment": {
            "commit": commit,
            "python": sys.version,
            "platform": platform.platform(),
            "hostname": platform.node(),
            "python_user_site_enabled": bool(site.ENABLE_USER_SITE),
            "workers": args.workers,
            "cores_per_worker": args.cores,
            "batch_type": args.batch_type,
            "backend_order": list(contracts),
            "generator_manifest_sha256": manifest["manifest_sha256"],
            "sampler_calibration_sha256": file_sha256(args.sampler_calibration),
        },
        "workload": {
            "name": "HEP-S-local",
            "configuration": {
                "shards": len(manifest["files"]),
                "shard_bytes": manifest["shard_bytes"],
                "histogram_bins": args.histogram_bins,
                "fan_in": args.fan_in,
                "reduction_levels": levels,
            },
            "logical_tasks": logical_tasks,
            "logical_edges": 2 * len(manifest["files"]) + reduction_tasks - 1,
            "logical_input_bytes": manifest["logical_bytes"],
            "logical_intermediate_bytes": 0,
            "requested_bytes": min(
                item["results"]["requested_bytes"] for item in runs
            ),
            "placement_lower_bound_bytes": None,
        },
        "contracts": list(contracts),
        "runs": runs,
        "gates": gates,
        "limitations": [
            "This is a local foundation gate, not a distributed performance claim.",
            "Per-worker network and disk deltas remain null until worker identity sampling is integrated.",
            "TaskVine Futures currently combine terminal wait and result fetch timing.",
            "TaskVine shard decode is inside the kernel while DataVine decode is an executor stage; end-to-end includes both, but useful_cpu_seconds is not directly comparable.",
        ],
        "summary": summarize_runs(runs),
    }
    # Summary is deliberately outside the strict v1 schema until the report
    # renderer contract is frozen.
    summary = report.pop("summary")
    errors = validate_artifact(report, repository / "acceptance/scientific-workflows/schema.json")
    if errors:
        raise RuntimeError({"artifact_validation": errors})
    report["summary"] = summary
    # The schema is strict; keep the derived summary in a sibling artifact.
    final_report = dict(report)
    final_report.pop("summary")
    (output / "summary.json").write_text(
        json.dumps(final_report, indent=2, sort_keys=True) + "\n"
    )
    (output / "derived-summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps({
        "status": final_report["status"],
        "output": str(output),
        "runs": len(runs),
        "logical_tasks": logical_tasks,
        "digest": expected["digest"],
    }, sort_keys=True))
    return 0 if final_report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
