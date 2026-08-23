#!/usr/bin/env python3
"""Run the traditional TaskVine side of the 100k static-scale comparison."""

import argparse
import gc
import json
import os
from pathlib import Path
import platform
import resource
import subprocess
import sys
import time

import cloudpickle
import ndcctools.taskvine as vine

from benchmark_cpu_fork import terminate_group
from compare_workflows import (
    PeakSampler,
    difference,
    start_resident_factory,
    taskvine_directory,
    taskvine_stats,
)


MASK64 = (1 << 64) - 1


def taskvine_scale_kernel(retain_outputs):
    """Read ten mounted inputs, run 2 ms of CPU, and optionally retain outputs."""

    values = []
    for index in range(10):
        with open(f"scale-input-{index}", "rb") as stream:
            values.append(cloudpickle.load(stream))
    deadline = time.process_time_ns() + 2_000_000
    state = sum(int(value) for value in values) & MASK64
    while time.process_time_ns() < deadline:
        state ^= (state << 13) & MASK64
        state ^= state >> 7
        state ^= (state << 17) & MASK64
    base = sum(int(value) for value in values)
    if retain_outputs:
        for index in range(10):
            with open(f"scale-output-{index}", "wb") as stream:
                cloudpickle.dump(base + index, stream)
    # FunctionCall always has one protocol result file.  Keep it tiny: the ten
    # scientific values above are the benchmark's logical outputs.
    return None


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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=100_000)
    parser.add_argument("--inputs-per-task", type=int, default=10)
    parser.add_argument("--outputs-per-task", type=int, default=10)
    parser.add_argument("--progress-tasks", type=int, default=5_000)
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
        args.progress_tasks,
        args.sample_tasks,
        args.workers,
        args.cores,
    ) < 1:
        parser.error("all scale arguments must be positive")
    if args.inputs_per_task != 10 or args.outputs_per_task != 10:
        parser.error("the shared kernel has exactly 10 inputs and 10 outputs")
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
    factory = None
    factory_log = None
    manager = None
    load_sampler = None
    execute_sampler = None
    started = time.monotonic()
    completed = 0
    validated_values = 0
    try:
        with taskvine_directory(output):
            manager = vine.Manager(port=0)
            manager.set_name("taskvine-static-scale-100k")
            library_name = "taskvine-static-scale-functions"
            library = manager.create_library_from_functions(
                library_name,
                taskvine_scale_kernel,
                add_env=False,
                exec_mode="fork",
            )
            library.set_cores(args.cores)
            manager.install_library(library)

            input_payloads = [
                cloudpickle.dumps(index) for index in range(args.inputs_per_task)
            ]
            input_files = [
                manager.declare_buffer(payload, cache="workflow")
                for payload in input_payloads
            ]
            sample_stride = max(1, args.tasks // args.sample_tasks)
            sampled_tasks = set(range(1, args.tasks + 1, sample_stride))
            sampled_tasks = set(sorted(sampled_tasks)[: args.sample_tasks])
            sampled_files = {}

            # Traditional TaskVine has no separate sealed-IR state.  Admit the
            # exact pool first, then submit the entire graph without driving
            # the manager event loop.  Thus no task executes during graph load,
            # while the measured execution starts with a proven 16x16 pool.
            factory, factory_log, factory_command = start_resident_factory(
                output / "factory-state",
                manager.port,
                args.workers,
                args.cores,
                repository / "taskvine/src/worker/vine_worker",
                output / "factory.log",
                args.batch_type,
            )
            admission_deadline = time.monotonic() + min(args.timeout, 3600)
            while True:
                if time.monotonic() >= admission_deadline:
                    raise TimeoutError("did not admit the exact empty worker pool")
                if factory.poll() is not None:
                    raise RuntimeError(f"vine_factory exited with {factory.returncode}")
                unexpected = manager.wait(1)
                if unexpected is not None:
                    raise AssertionError(("task_completed_before_graph_load", unexpected.id))
                manager._refresh_stats()
                if (
                    int(manager.stats.workers_connected) == args.workers
                    and int(manager.stats.total_cores) == args.workers * args.cores
                ):
                    break
            admission = time.monotonic()
            print(
                json.dumps(
                    {
                        "phase": "admission",
                        "workers": args.workers,
                        "cores_per_worker": [args.cores] * args.workers,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

            baseline = taskvine_stats(manager)
            load_sampler = PeakSampler((os.getpid(), factory.pid)).start()
            load_started = time.monotonic()
            for logical_task_id in range(1, args.tasks + 1):
                retained = logical_task_id in sampled_tasks
                task = vine.FunctionCall(
                    library_name, "taskvine_scale_kernel", retained
                )
                task.enable_temp_output()
                task.set_cores(1)
                task.set_tag(str(logical_task_id))
                for index, input_file in enumerate(input_files):
                    task.add_input(input_file, f"scale-input-{index}")
                if retained:
                    outputs = []
                    for index in range(args.outputs_per_task):
                        file = manager.declare_temp()
                        task.add_output(file, f"scale-output-{index}")
                        outputs.append(file)
                    sampled_files[logical_task_id] = outputs
                manager.submit(task)
                if logical_task_id % args.progress_tasks == 0:
                    print(
                        json.dumps(
                            {
                                "phase": "load",
                                "tasks_loaded": logical_task_id,
                                "manager_task_table": len(manager._task_table),
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
            sealed = time.monotonic()
            load_peak = load_sampler.stop()
            load_sampler = None
            if len(manager._task_table) != args.tasks:
                raise AssertionError(
                    ("manager_task_table", len(manager._task_table), args.tasks)
                )
            manager._refresh_stats()
            if int(manager.stats.tasks_done) != baseline["tasks_done"]:
                raise AssertionError("a task executed while the static graph was loading")
            execute_sampler = PeakSampler((os.getpid(), factory.pid)).start()
            deadline = time.monotonic() + args.timeout
            expected_values = tuple(
                sum(range(args.inputs_per_task)) + index
                for index in range(args.outputs_per_task)
            )
            failed = []
            while completed < args.tasks:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"{args.tasks - completed} TaskVine tasks remain"
                    )
                if factory.poll() is not None:
                    raise RuntimeError(f"vine_factory exited with {factory.returncode}")
                task = manager.wait(1)
                manager._refresh_stats()
                if task is None:
                    continue
                logical_task_id = int(task.tag)
                if not task.successful():
                    failed.append(
                        {
                            "logical_task_id": logical_task_id,
                            "physical_task_id": task.id,
                            "result": task.result,
                            "exit_code": task.exit_code,
                        }
                    )
                protocol_output = task._output_file
                task._output_file = None
                retained_outputs = sampled_files.pop(logical_task_id, [])
                if task.successful() and retained_outputs:
                    values = []
                    for file in retained_outputs:
                        if not manager.fetch_file(file):
                            raise RuntimeError(
                                f"could not fetch retained output for {logical_task_id}"
                            )
                        values.append(cloudpickle.loads(file.contents()))
                    if tuple(values) != expected_values:
                        raise AssertionError(
                            ("output_values", logical_task_id, values)
                        )
                    validated_values += len(values)
                task.__del__()
                for file in [protocol_output, *retained_outputs]:
                    manager.undeclare_file(file)
                completed += 1
                if completed % args.progress_tasks == 0:
                    print(
                        json.dumps(
                            {
                                "phase": "execute",
                                "tasks_completed": completed,
                                "manager_task_table": len(manager._task_table),
                                "validated_values": validated_values,
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
            terminal = time.monotonic()
            if failed:
                raise RuntimeError({"failed_tasks": failed[:20], "count": len(failed)})
            if sampled_files:
                raise AssertionError(("uncompleted_samples", sorted(sampled_files)))
            if validated_values != args.sample_tasks * args.outputs_per_task:
                raise AssertionError(
                    (
                        "validated_values",
                        validated_values,
                        args.sample_tasks * args.outputs_per_task,
                    )
                )

            manager._refresh_stats()
            after = taskvine_stats(manager)
            stats = difference(after, baseline)
            if stats["tasks_submitted"] != args.tasks:
                raise AssertionError(("tasks_submitted", stats))
            if stats["tasks_done"] != args.tasks:
                raise AssertionError(("tasks_done", stats))
            if stats["tasks_successful"] != args.tasks or stats["tasks_failed"]:
                raise AssertionError(("task_results", stats))
            if stats["workers_removed"]:
                raise AssertionError(("workers_removed", stats))
            if len(manager._task_table):
                raise AssertionError(("retained_tasks", len(manager._task_table)))
            if (
                int(manager.stats.workers_connected) != args.workers
                or int(manager.stats.total_cores) != args.workers * args.cores
            ):
                raise AssertionError(
                    (
                        "post_worker_pool",
                        int(manager.stats.workers_connected),
                        int(manager.stats.total_cores),
                    )
                )
            execute_peak = execute_sampler.stop()
            execute_sampler = None
            storage = tree_bytes(output)
            for file in input_files:
                manager.undeclare_file(file)
            input_files.clear()
            gc.collect()

            result = {
                "artifact_type": "taskvine-static-scale-v1",
                "status": "PASS",
                "workflow_id": "static-scale-100k-1mio",
                "execution_semantics": (
                    "exact-pool-admitted-before-submit; "
                    "all-tasks-submitted-before-manager-event-loop-execution"
                ),
                "semantic_task_batching": False,
                "graph": {
                    "logical_tasks": args.tasks,
                    "physical_tasks": {
                        "submissions": stats["tasks_submitted"],
                        "completions": stats["tasks_done"],
                    },
                    "input_references": args.tasks * args.inputs_per_task,
                    "logical_outputs": args.tasks * args.outputs_per_task,
                    "requested_outputs": validated_values,
                    "sampled_tasks": len(sampled_tasks),
                    "shared_input_objects": len(input_payloads),
                    "functioncall_protocol_outputs": args.tasks,
                },
                "admission": {
                    "workers": args.workers,
                    "cores_per_worker": [args.cores] * args.workers,
                    "post_workers": int(manager.stats.workers_connected),
                    "post_total_cores": int(manager.stats.total_cores),
                },
                "timing": {
                    "graph_load_seconds": sealed - load_started,
                    "worker_admission_seconds": admission - started,
                    "sealed_to_terminal_seconds": terminal - sealed,
                    "post_admission_seconds": terminal - admission,
                    "total_seconds": terminal - started,
                    "tasks_per_second": args.tasks / (terminal - sealed),
                    "input_references_per_second": (
                        args.tasks * args.inputs_per_task / (terminal - sealed)
                    ),
                    "logical_outputs_per_second": (
                        args.tasks * args.outputs_per_task / (terminal - sealed)
                    ),
                },
                "peak": {"load": load_peak, "execute": execute_peak},
                "manager_stats": stats,
                "storage_before_cleanup": storage,
                "factory_command": list(factory_command),
                "environment": {
                    "workers": args.workers,
                    "cores_per_worker": args.cores,
                    "batch_type": args.batch_type,
                    "hostname": platform.node(),
                    "python": sys.version,
                    "maximum_rss_kib": resource.getrusage(
                        resource.RUSAGE_SELF
                    ).ru_maxrss,
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
            "completed": completed,
            "validated_values": validated_values,
            "elapsed_seconds": time.monotonic() - started,
        }
        (output / "failure.json").write_text(
            json.dumps(failure, indent=2, sort_keys=True) + "\n"
        )
        raise
    finally:
        for sampler in (load_sampler, execute_sampler):
            if sampler is not None:
                try:
                    sampler.stop()
                except (EOFError, RuntimeError):
                    pass
        terminate_group(factory)
        if factory_log is not None:
            factory_log.close()
        if manager is not None:
            manager.__del__()


if __name__ == "__main__":
    main()
