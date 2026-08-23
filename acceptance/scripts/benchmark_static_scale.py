#!/usr/bin/env python3
"""Run one sealed-before-admission DataVine workflow at graph/data scale."""

import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import subprocess
import sys
import time

import cloudpickle

from benchmark_cpu_fork import terminate_group
from compare_workflows import (
    PeakSampler,
    physical_counts,
    runtime_stages,
    start_resident_factory,
)
from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


SCHEMA = "datavine.workflow/v1"
DELTA_SCHEMA = "datavine.workflow-delta/v1"
CODEC = {"name": "python/cloudpickle", "version": "3"}


def scale_kernel(*values):
    """Small real CPU kernel with ten deterministic logical outputs."""

    deadline = time.process_time_ns() + 2_000_000
    state = sum(int(value) for value in values) & ((1 << 64) - 1)
    while time.process_time_ns() < deadline:
        state ^= (state << 13) & ((1 << 64) - 1)
        state ^= state >> 7
        state ^= (state << 17) & ((1 << 64) - 1)
    base = sum(int(value) for value in values)
    return tuple(base + index for index in range(10))


def encoded(document):
    return json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def object_record(data_id, codec, digest):
    return {
        "data_id": data_id,
        "codec": codec,
        "origin": {"kind": "object", "sha256": digest},
        "content_sha256": digest,
    }


def output_record(data_id, task_id, output_index):
    return {
        "data_id": data_id,
        "codec": CODEC,
        "origin": {
            "kind": "output",
            "task_id": task_id,
            "output_index": output_index,
        },
    }


def tree_bytes(*roots):
    total = 0
    files = 0
    for root in roots:
        root = Path(root)
        if root.is_file():
            total += root.stat().st_size
            files += 1
            continue
        for directory, _, names in os.walk(root):
            for name in names:
                path = Path(directory) / name
                try:
                    total += path.stat().st_size
                    files += 1
                except FileNotFoundError:
                    pass
    return {"files": files, "bytes": total}


def worker_inventory(vine_status, port):
    try:
        completed = subprocess.run(
            (str(vine_status), "-W", "localhost", str(port)),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        return []
    if completed.returncode:
        return []
    inventory = []
    for line in completed.stdout.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 6:
            continue
        try:
            used = int(float(fields[4]))
            available = int(float(fields[5]))
        except ValueError:
            continue
        inventory.append(max(used, available))
    return inventory


def wait_scale_workers(vine_status, port, workers, cores, factory, timeout):
    """Accept an exact pool even after sealed work immediately occupies cores."""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if factory.poll() is not None:
            raise RuntimeError(f"vine_factory exited with {factory.returncode}")
        inventory = worker_inventory(vine_status, port)
        if len(inventory) == workers and all(value == cores for value in inventory):
            return inventory
        time.sleep(0.5)
    raise TimeoutError(f"did not observe exactly {workers}x{cores} workers")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=100_000)
    parser.add_argument("--inputs-per-task", type=int, default=10)
    parser.add_argument("--outputs-per-task", type=int, default=10)
    parser.add_argument("--chunk-tasks", type=int, default=5_000)
    parser.add_argument("--sample-tasks", type=int, default=100)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--cores", type=int, default=16)
    parser.add_argument("--batch-type", choices=("condor", "local"), default="condor")
    parser.add_argument("--timeout", type=float, default=7_200)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    if min(
        args.tasks,
        args.inputs_per_task,
        args.outputs_per_task,
        args.chunk_tasks,
        args.sample_tasks,
        args.workers,
        args.cores,
    ) < 1:
        parser.error("all scale arguments must be positive")
    if args.outputs_per_task != 10:
        parser.error("scale_kernel has exactly 10 outputs")
    if args.inputs_per_task != 10:
        parser.error("scale_kernel has exactly 10 external inputs")
    if args.sample_tasks > args.tasks:
        parser.error("sample-tasks cannot exceed tasks")
    return args


def main():
    args = parse_args()
    output = Path(args.output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        raise RuntimeError(f"output directory is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    repository = Path(__file__).resolve().parents[2]
    service_log_path = output / "service.log"
    factory_log_path = output / "factory.log"
    token = "datavine-static-scale"
    workflow_id = "static-scale-100k-1mio"
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
    factory = None
    factory_log = None
    client = None
    stage_sampler = None
    execute_sampler = None
    result = None
    started = time.monotonic()
    try:
        contact_line = service.stdout.readline()
        if not contact_line:
            raise RuntimeError("DataVine service exited before publishing contact")
        contact = json.loads(contact_line)
        client = WorkflowClient(contact["endpoint"], token, timeout=300)
        client.rpc_profile(reset=True)
        capabilities = client.workflow_capabilities()
        if not capabilities.get("wait_terminal"):
            raise RuntimeError("service does not provide non-polling terminal wait")

        payloads = [cloudpickle.dumps(index) for index in range(args.inputs_per_task)]
        function_payload = cloudpickle.dumps(scale_kernel)
        output_files = [
            f"datavine-python-output-{index}"
            for index in range(args.outputs_per_task)
        ]
        invocation_payload = cloudpickle.dumps(
            {
                "args": tuple(("ref", index + 1) for index in range(args.inputs_per_task)),
                "kwargs": {},
                "output_count": args.outputs_per_task,
                "output_files": output_files,
            }
        )
        payloads.extend((function_payload, invocation_payload))
        digests = [hashlib.sha256(payload).hexdigest() for payload in payloads]
        client.put_objects(zip(payloads, digests))
        function_data_id = args.inputs_per_task + 1
        invocation_data_id = function_data_id + 1
        first_output_data_id = invocation_data_id + 1
        initial_data = [
            object_record(index + 1, CODEC, digests[index])
            for index in range(args.inputs_per_task)
        ]
        initial_data.append(
            object_record(
                function_data_id,
                {"name": "python/callable", "version": "1"},
                digests[args.inputs_per_task],
            )
        )
        initial_data.append(
            object_record(
                invocation_data_id,
                {"name": "bytes", "version": "1"},
                digests[args.inputs_per_task + 1],
            )
        )
        initial = {
            "schema": SCHEMA,
            "workflow_id": workflow_id,
            "idempotency_key": f"{workflow_id}-initial",
            "mode": "streaming",
            "tasks": [],
            "data": initial_data,
            "requested_outputs": [],
            "policy": {
                "maximum_tasks": args.tasks,
                "maximum_edges": args.tasks * args.inputs_per_task,
            },
            "metadata": {
                "execution_semantics": "sealed-before-worker-admission",
                "semantic_task_batching": False,
            },
        }

        stage_sampler = PeakSampler((os.getpid(), service.pid)).start()
        stage_started = time.monotonic()
        initial_body = encoded(initial)
        info = client.submit_workflow(initial_body)
        generation = int(info["generation"])
        chunk_sizes = []
        sample_stride = max(1, args.tasks // args.sample_tasks)
        sampled_tasks = set(range(1, args.tasks + 1, sample_stride))
        sampled_tasks = set(sorted(sampled_tasks)[: args.sample_tasks])
        requested_ids = []
        common_inputs = [
            {"position": index, "data_id": index + 1}
            for index in range(args.inputs_per_task)
        ]
        compact_inputs = [item["data_id"] for item in common_inputs]
        common_executor = {
            "kind": "python",
            "version": "callable-v1",
            "payload_ref": invocation_data_id,
            "function_ref": function_data_id,
            "function_digest": digests[args.inputs_per_task],
            "output_files": output_files,
        }
        task_defaults = {
            "executor": common_executor,
            "retry": {"maximum_attempts": 1},
            "resources": {"cores": 1},
        }
        data_defaults = {"codec": CODEC}
        for chunk_start in range(1, args.tasks + 1, args.chunk_tasks):
            chunk_end = min(args.tasks, chunk_start + args.chunk_tasks - 1)
            tasks = []
            data = []
            requested = []
            for task_id in range(chunk_start, chunk_end + 1):
                output_start = first_output_data_id + (
                    task_id - 1
                ) * args.outputs_per_task
                output_ids = list(
                    range(output_start, output_start + args.outputs_per_task)
                )
                tasks.append([task_id, compact_inputs, output_ids])
                for output_index, data_id in enumerate(output_ids):
                    data.append([data_id, task_id, output_index])
                if task_id in sampled_tasks:
                    requested.extend(output_ids)
            requested_ids.extend(requested)
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
            print(
                json.dumps(
                    {
                        "phase": "load",
                        "tasks_loaded": chunk_end,
                        "generation": generation,
                        "payload_bytes": len(body),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            del body, delta, tasks, data, requested
            gc.collect()
        info = client.seal_workflow(workflow_id, generation)
        sealed = time.monotonic()
        stage_peak = stage_sampler.stop()
        stage_sampler = None
        if info["state"] not in {"sealed", "running"}:
            raise RuntimeError(info)

        factory, factory_log, factory_command = start_resident_factory(
            output / "factory-state",
            contact["manager_port"],
            args.workers,
            args.cores,
            repository / "taskvine/src/worker/vine_worker",
            factory_log_path,
            args.batch_type,
        )
        execute_sampler = PeakSampler((os.getpid(), service.pid, factory.pid)).start()
        inventory = wait_scale_workers(
            repository / "taskvine/src/tools/vine_status",
            contact["manager_port"],
            args.workers,
            args.cores,
            factory,
            timeout=3600,
        )
        admitted = time.monotonic()
        print(
            json.dumps(
                {
                    "phase": "execute",
                    "workers": len(inventory),
                    "cores_per_worker": sorted(inventory),
                },
                sort_keys=True,
            ),
            flush=True,
        )
        info = client.wait_workflow(workflow_id, timeout=args.timeout)
        terminal = time.monotonic()
        if info["state"] != "completed":
            raise RuntimeError(info)
        if info["tasks"] != args.tasks:
            raise AssertionError(("tasks", info["tasks"], args.tasks))
        expected_data = first_output_data_id - 1 + (
            args.tasks * args.outputs_per_task
        )
        if info["data"] != expected_data:
            raise AssertionError(("data", info["data"], expected_data))
        if info["requested_outputs"] != len(requested_ids):
            raise AssertionError(
                ("requested", info["requested_outputs"], len(requested_ids))
            )
        serialized = client.fetch_workflow_results(workflow_id, requested_ids)
        values = [cloudpickle.loads(value) for value in serialized]
        expected_values = tuple(sum(range(args.inputs_per_task)) + index
                                for index in range(args.outputs_per_task))
        for index in range(0, len(values), args.outputs_per_task):
            if tuple(values[index:index + args.outputs_per_task]) != expected_values:
                raise AssertionError((index, values[index:index + args.outputs_per_task]))
        fetched = time.monotonic()
        execute_peak = execute_sampler.stop()
        execute_sampler = None
        post_inventory = wait_scale_workers(
            repository / "taskvine/src/tools/vine_status",
            contact["manager_port"],
            args.workers,
            args.cores,
            factory,
            timeout=60,
        )
        service_log.flush()
        counts = physical_counts(service_log_path, workflow_id)
        stages = runtime_stages(service_log_path, workflow_id)
        if counts != {"submissions": args.tasks, "completions": args.tasks}:
            raise AssertionError(("physical_tasks", counts))
        if stages.get("manager_workers_removed", 0):
            raise AssertionError(("workers_removed", stages))
        if stages.get("publication_durable_outputs") != len(requested_ids):
            raise AssertionError(("durable_outputs", stages))

        result = {
            "artifact_type": "datavine-static-scale-v1",
            "status": "PASS",
            "workflow_id": workflow_id,
            "execution_semantics": "sealed-before-worker-admission",
            "semantic_task_batching": False,
            "graph": {
                "logical_tasks": args.tasks,
                "physical_tasks": counts,
                "input_references": args.tasks * args.inputs_per_task,
                "logical_outputs": args.tasks * args.outputs_per_task,
                "data_records": info["data"],
                "requested_outputs": len(requested_ids),
                "sampled_tasks": len(sampled_tasks),
            },
            "admission": {
                "workers": len(inventory),
                "cores_per_worker": inventory,
                "post_workers": len(post_inventory),
                "post_cores_per_worker": post_inventory,
            },
            "transport": {
                "initial_payload_bytes": len(initial_body),
                "delta_count": len(chunk_sizes),
                "maximum_delta_payload_bytes": max(chunk_sizes),
                "total_delta_payload_bytes": sum(chunk_sizes),
                "rpc_profile": client.rpc_profile(reset=True),
            },
            "timing": {
                "graph_load_seconds": sealed - stage_started,
                "worker_admission_seconds": admitted - sealed,
                "sealed_to_terminal_seconds": terminal - sealed,
                "post_admission_seconds": terminal - admitted,
                "result_validation_seconds": fetched - terminal,
                "total_seconds": fetched - started,
                "tasks_per_second": args.tasks / (terminal - sealed),
                "input_references_per_second": (
                    args.tasks * args.inputs_per_task / (terminal - sealed)
                ),
                "logical_outputs_per_second": (
                    args.tasks * args.outputs_per_task / (terminal - sealed)
                ),
            },
            "peak": {"load": stage_peak, "execute": execute_peak},
            "runtime_stages": stages,
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
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "output": str(output),
                    "tasks_per_second": result["timing"]["tasks_per_second"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
    except BaseException as error:
        failure = {
            "status": "FAIL",
            "type": type(error).__name__,
            "detail": str(error),
            "elapsed_seconds": time.monotonic() - started,
        }
        (output / "failure.json").write_text(
            json.dumps(failure, indent=2, sort_keys=True) + "\n"
        )
        raise
    finally:
        if stage_sampler is not None:
            try:
                stage_sampler.stop()
            except (EOFError, RuntimeError):
                pass
        if execute_sampler is not None:
            try:
                execute_sampler.stop()
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
