#!/usr/bin/env python3
"""Run the traditional TaskVine FunctionCall-fork comparison workload."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import resource
import shutil
import subprocess
import sys
import tempfile
import time

import ndcctools.taskvine as vine

from benchmark_cpu_fork import terminate_group
from compare_workflows import PeakSampler, difference, start_resident_factory, taskvine_stats
from data_intensive_workload import SIZE_PROFILES, Workload, assert_full_contract, canonical_json


MASK = (1 << 64) - 1


def taskvine_file_kernel(stage, output_bytes):
    """The same random pread, CPU-time, and output kernel as DataVine."""

    import hashlib as _hashlib
    import json as _json
    import os as _os
    from pathlib import Path as _Path
    import time as _time

    def step(value):
        value ^= (value << 13) & MASK
        value ^= value >> 7
        value ^= (value << 17) & MASK
        return value & MASK

    data_root = _Path("datavine/data")
    inputs = sorted(
        (int(item.name), item) for item in data_root.iterdir() if item.name.isdigit()
    )
    if not inputs:
        raise RuntimeError("data-intensive task has no inputs")
    state = int.from_bytes(_hashlib.sha256(
        (stage + ":" + ",".join(str(item[0]) for item in inputs)).encode()
    ).digest()[:8], "big") or 1
    ordered = list(inputs)
    for index in range(len(ordered) - 1, 0, -1):
        state = step(state)
        target = state % (index + 1)
        ordered[index], ordered[target] = ordered[target], ordered[index]
    digest = _hashlib.sha256()
    read_bytes = 0
    chunk_bytes = 64 * 1024
    for data_id, path in ordered:
        size = path.stat().st_size
        offsets = list(range(0, size, chunk_bytes))
        for index in range(len(offsets) - 1, 0, -1):
            state = step(state)
            target = state % (index + 1)
            offsets[index], offsets[target] = offsets[target], offsets[index]
        descriptor = _os.open(path, _os.O_RDONLY | _os.O_CLOEXEC)
        try:
            for offset in offsets:
                expected = min(chunk_bytes, size - offset)
                block = _os.pread(descriptor, expected, offset)
                if len(block) != expected:
                    raise RuntimeError(f"short read data {data_id} offset {offset}")
                digest.update(data_id.to_bytes(8, "big"))
                digest.update(offset.to_bytes(8, "big"))
                digest.update(block)
                read_bytes += len(block)
        finally:
            _os.close(descriptor)
    bucket = state % 1000
    if bucket < 800:
        cpu_ms = 2 + state % 9
    elif bucket < 950:
        cpu_ms = 10 + state % 41
    elif bucket < 990:
        cpu_ms = 50 + state % 451
    elif bucket < 999:
        cpu_ms = 500 + state % 1501
    else:
        cpu_ms = 2000 + state % 3001
    deadline = _time.process_time_ns() + cpu_ms * 1_000_000
    sink = state
    while _time.process_time_ns() < deadline:
        sink = step(sink)
    seed = _hashlib.sha256(stage.encode() + digest.digest()).digest()
    block = _hashlib.shake_256(seed).digest(min(int(output_bytes), 1 << 20))
    output = _Path("datavine-python-output-0")
    with output.open("xb", buffering=0) as stream:
        remaining = int(output_bytes)
        while remaining:
            count = min(len(block), remaining)
            stream.write(block[:count])
            remaining -= count
    return _json.dumps({
        "stage": stage, "inputs": len(inputs), "read_bytes": read_bytes,
        "output_bytes": int(output_bytes), "cpu_ms": cpu_ms,
        "sink": sink & 0xffff,
    }, sort_keys=True)


def atomic_json(path, value):
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def load_dataset(root, workload):
    path = root / "dataset-manifest.json"
    manifest = json.loads(path.read_text())
    copy = dict(manifest)
    claimed = copy.pop("manifest_sha256", None)
    gates = {
        "manifest_pass": manifest.get("status") == "PASS" and all(manifest.get("gates", {}).values()),
        "contract": manifest.get("contract", {}).get("contract_sha256") == workload.contract()["contract_sha256"],
        "source_files": manifest.get("source_files") == workload.source_files,
        "source_bytes": manifest.get("logical_bytes") == workload.source_bytes,
        "manifest_digest": hashlib.sha256(canonical_json(copy)).hexdigest() == claimed,
    }
    if not all(gates.values()):
        raise RuntimeError({"dataset": str(path), "gates": gates})
    return manifest


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--cohorts", type=int, default=64)
    parser.add_argument("--scale", type=int, default=64)
    parser.add_argument("--size-profile", choices=("tiny", "full"), default="full")
    parser.add_argument("--workers", type=int, default=128)
    parser.add_argument("--cores", type=int, default=16)
    parser.add_argument("--memory", type=int, default=4096,
                        help="memory in MiB requested and advertised per worker")
    parser.add_argument("--batch-type", choices=("local", "condor"), default="condor")
    parser.add_argument("--timeout", type=float, default=24 * 60 * 60)
    parser.add_argument("--progress-tasks", type=int, default=10_000)
    parser.add_argument("--acceptance", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()
    if min(args.cohorts, args.scale, args.workers, args.cores, args.memory,
           args.progress_tasks) < 1:
        parser.error("numeric arguments must be positive")
    return args


def add_inputs(task, files, data_ids):
    for file, data_id in zip(files, data_ids):
        task.add_input(file, f"datavine/data/{data_id}")


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
    active_passing = sum(value >= required_active for value in active)
    active_fraction = active_passing / len(active) if active else 0.0
    required_active_fraction = 0.90
    return {
        "samples": len(samples),
        "central_samples": len(central),
        "minimum_ready_plus_running": min(ready_running) if ready_running else None,
        "maximum_ready_plus_running": max(ready_running) if ready_running else None,
        "minimum_active_cores": min(active) if active else None,
        "maximum_active_cores": max(active) if active else None,
        "required_ready_plus_running": required_ready,
        "required_active_cores": required_active,
        "active_samples_at_or_above_required": active_passing,
        "active_sample_fraction": active_fraction,
        "required_active_sample_fraction": required_active_fraction,
        "gates": {
            "central_window_observed": bool(central),
            "ready_parallelism": bool(ready_running) and min(ready_running) >= required_ready,
            "active_parallelism": (
                bool(active) and active_fraction >= required_active_fraction
            ),
        },
    }


def main():
    args = parse_args()
    workload = Workload(args.cohorts, args.scale, args.size_profile)
    contract = workload.contract()
    if args.acceptance:
        assert_full_contract(workload)
        if (args.workers, args.cores, args.memory, args.batch_type) != (
                128, 16, 4096, "condor"):
            raise ValueError(
                "acceptance requires exactly 128 Condor workers x 16 cores "
                "with 4096 MiB each"
            )
    if args.plan_only:
        print(json.dumps({"status": "PASS", "contract": contract, "execution": vars(args)}, indent=2, sort_keys=True, default=str))
        return 0
    dataset_root = args.dataset_root.resolve()
    dataset = load_dataset(dataset_root, workload)
    output = args.output_dir.resolve()
    if output.exists() and any(output.iterdir()):
        raise RuntimeError(f"output directory is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    sink_root = output / "requested-results"
    sink_root.mkdir()
    repository = Path(__file__).resolve().parents[2]
    manager = factory = factory_log = load_peak = execute_peak = None
    runtime_info_root = Path(tempfile.mkdtemp(
        prefix="taskvine-data-intensive-run-info-", dir="/tmp"
    ))
    run_succeeded = False
    started = time.monotonic()
    completed = failed = 0
    a_files = []
    b_files = []
    c_files = []
    try:
        previous = Path.cwd()
        os.chdir(output)
        # Runtime diagnostics are control-plane data.  Full-scale debug,
        # taskgraph, and transaction streams reached 4 GiB by 44k tasks and
        # stalled manager-worker traffic when written beside the NFS workload.
        manager = vine.Manager(port=0, run_info_path=str(runtime_info_root))
        manager.set_name(f"taskvine-data-intensive-{int(time.time())}")
        library_name = "taskvine-data-intensive-functions"
        library = manager.create_library_from_functions(
            library_name, taskvine_file_kernel, add_env=False, exec_mode="fork"
        )
        library.set_cores(args.cores)
        manager.install_library(library)
        # The manager dispatches at most attempt-schedule-depth ready tasks in
        # one scheduling pass.  Function libraries are installed lazily on the
        # workers selected by that first pass.  The default depth is 100, so a
        # 128-worker pool otherwise starts exactly 100 library instances and
        # deterministic tie ordering can keep refilling those same workers.
        # Cover the complete admitted pool in the first pass.  This does not
        # batch logical tasks: all 1,048,576 calls remain independent physical
        # TaskVine submissions and completions.
        schedule_depth = max(100, args.workers)
        if manager.tune("attempt-schedule-depth", schedule_depth) != 0:
            raise RuntimeError("TaskVine scheduling-depth tune is unavailable")
        baseline = taskvine_stats(manager)
        load_peak = PeakSampler((os.getpid(),)).start()
        load_started = time.monotonic()
        profile = SIZE_PROFILES[workload.size_profile]
        for a_global in range(workload.a_tasks):
            data_ids = workload.a_input_data_ids(a_global)
            inputs = [
                manager.declare_url(
                    (dataset_root / workload.source_path(a_global * 36 + slot)).as_uri(),
                    cache=False,
                )
                for slot in range(36)
            ]
            output_file = manager.declare_temp()
            a_files.append(output_file)
            task = vine.FunctionCall(library_name, "taskvine_file_kernel", "A", profile["a_output"])
            task.enable_temp_output()
            task.set_cores(1)
            task.set_tag(f"A:{a_global}")
            add_inputs(task, inputs, data_ids)
            task.add_output(output_file, "datavine-python-output-0")
            manager.submit(task)
            if (a_global + 1) % args.progress_tasks == 0:
                print(json.dumps({"phase": "load-A", "loaded": a_global + 1, "total": workload.a_tasks}), flush=True)
        for b_global in range(workload.b_tasks):
            data_ids = workload.b_input_data_ids(b_global)
            inputs = [a_files[(data_id - workload.a_data_first) // 37] for data_id in data_ids]
            output_file = manager.declare_temp()
            b_files.append(output_file)
            task = vine.FunctionCall(library_name, "taskvine_file_kernel", "B", profile["b_output"])
            task.enable_temp_output()
            task.set_cores(1)
            task.set_tag(f"B:{b_global}")
            add_inputs(task, inputs, data_ids)
            task.add_output(output_file, "datavine-python-output-0")
            manager.submit(task)
            if (b_global + 1) % args.progress_tasks == 0:
                print(json.dumps({"phase": "load-B", "loaded": b_global + 1, "total": workload.b_tasks}), flush=True)
        for c_global in range(workload.c_tasks):
            data_ids = workload.c_input_data_ids(c_global)
            inputs = [b_files[data_id - workload.b_data_first] for data_id in data_ids]
            output_file = manager.declare_file(str(sink_root / f"c-{c_global:06d}.bin"))
            c_files.append(output_file)
            task = vine.FunctionCall(library_name, "taskvine_file_kernel", "C", profile["c_output"])
            task.enable_temp_output()
            task.set_cores(1)
            task.set_tag(f"C:{c_global}")
            add_inputs(task, inputs, data_ids)
            task.add_output(output_file, "datavine-python-output-0")
            manager.submit(task)
            if (c_global + 1) % args.progress_tasks == 0:
                print(json.dumps({"phase": "load-C", "loaded": c_global + 1, "total": workload.c_tasks}), flush=True)
        loaded = time.monotonic()
        load_peak_result = load_peak.stop()
        load_peak = None
        if len(manager._task_table) != workload.tasks:
            raise AssertionError(("manager task table", len(manager._task_table), workload.tasks))
        manager._refresh_stats()
        if int(manager.stats.tasks_done) != 0:
            raise AssertionError("task executed during static graph load")
        # Keep scheduling closed while the exact resident pool connects. Using
        # workers+1 makes the threshold deliberately unreachable until this
        # driver observes 128x16 and opens the gate explicitly.
        if manager.tune("wait-for-workers", args.workers + 1) != 0:
            raise RuntimeError("TaskVine worker-admission scheduling gate is unavailable")
        factory, factory_log, factory_command = start_resident_factory(
            output / "factory-state", manager.port, args.workers, args.cores,
            repository / "taskvine/src/worker/vine_worker", output / "factory.log",
            args.batch_type,
            memory_mib=args.memory,
        )
        deadline = time.monotonic() + min(args.timeout, 3600)
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError("did not admit exact TaskVine worker pool")
            if factory.poll() is not None:
                raise RuntimeError(f"vine_factory exited with {factory.returncode}")
            unexpected = manager.wait(1)
            if unexpected is not None:
                raise AssertionError(("task completed before exact pool admission", unexpected.id))
            manager._refresh_stats()
            if (
                int(manager.stats.workers_connected) == args.workers
                and int(manager.stats.total_cores) == args.workers * args.cores
            ):
                break
        if manager.tune("wait-for-workers", 0) != 0:
            raise RuntimeError("could not open TaskVine scheduling gate")
        admitted = time.monotonic()
        execute_peak = PeakSampler((os.getpid(), factory.pid)).start()
        samples = []
        next_sample = time.monotonic()
        execution_deadline = time.monotonic() + args.timeout
        while completed < workload.tasks:
            if time.monotonic() >= execution_deadline:
                raise TimeoutError(f"{workload.tasks - completed} tasks remain")
            task = manager.wait(1)
            now = time.monotonic()
            if now >= next_sample:
                manager._refresh_stats()
                samples.append({
                    "monotonic_seconds": now,
                    "tasks_waiting": int(manager.stats.tasks_waiting),
                    "tasks_running": int(manager.stats.tasks_running),
                    "tasks_complete": int(manager.stats.tasks_done) - baseline["tasks_done"],
                    "workers": int(manager.stats.workers_connected),
                    "total_cores": int(manager.stats.total_cores),
                    "active_cores": int(manager.stats.committed_cores),
                })
                next_sample = now + 1.0
            if task is None:
                continue
            if not task.successful():
                failed += 1
            protocol_output = task._output_file
            task._output_file = None
            task.__del__()
            if protocol_output is not None:
                manager.undeclare_file(protocol_output)
            completed += 1
            if completed % args.progress_tasks == 0:
                print(json.dumps({"phase": "execute", "completed": completed, "total": workload.tasks, "failed": failed}), flush=True)
        terminal = time.monotonic()
        execute_peak_result = execute_peak.stop()
        execute_peak = None
        # CRC's Condor pool is opportunistic: an otherwise healthy worker job
        # can be evicted and immediately restarted on another host.  Do not
        # confuse that scheduler churn with a failed logical task, but require
        # the factory to restore the exact 128x16 resident pool before this run
        # can pass.  Churn and failed attempts remain explicit in the artifact.
        recovery_deadline = time.monotonic() + min(args.timeout, 300)
        while True:
            manager._refresh_stats()
            if (
                int(manager.stats.workers_connected) == args.workers
                and int(manager.stats.total_cores) == args.workers * args.cores
            ):
                break
            if time.monotonic() >= recovery_deadline:
                raise TimeoutError("TaskVine worker pool did not recover after execution")
            if factory.poll() is not None:
                raise RuntimeError(f"vine_factory exited with {factory.returncode}")
            manager.wait(1)
        recovered = time.monotonic()
        manager._refresh_stats()
        stats = difference(taskvine_stats(manager), baseline)
        sample_count = min(16, workload.c_tasks)
        stride = max(1, workload.c_tasks // sample_count)
        sample_paths = [sink_root / f"c-{index:06d}.bin" for index in range(0, workload.c_tasks, stride)][:sample_count]
        expected_size = profile["c_output"]
        sampled = [path.stat().st_size for path in sample_paths]
        sampled_hashes = [hashlib.sha256(path.read_bytes()).hexdigest() for path in sample_paths]
        parallelism = parallelism_summary(samples, workload.tasks, args.workers, args.cores)
        gates = {
            "dataset_manifest": dataset["status"] == "PASS",
            "exact_logical_tasks": completed == workload.tasks,
            "exact_physical_submissions": stats["tasks_submitted"] == workload.tasks,
            "exact_physical_completions": stats["tasks_done"] == workload.tasks,
            "all_tasks_successful": (
                failed == 0
                and stats["tasks_successful"] == workload.tasks
                and stats["tasks_exhausted_attempts"] == 0
            ),
            "worker_churn_recovered": (
                int(manager.stats.workers_connected) == args.workers
                and int(manager.stats.total_cores) == args.workers * args.cores
            ),
            "exact_worker_pool": int(manager.stats.workers_connected) == args.workers and int(manager.stats.total_cores) == args.workers * args.cores,
            "sampled_outputs": len(sampled) == sample_count and set(sampled) == {expected_size},
            "sharedfs_source_transport": True,
            "source_inputs_task_scoped": True,
            **parallelism["gates"],
        }
        result = {
            "schema": "datavine.data-intensive-taskvine-run/v1",
            "status": "PASS" if all(gates.values()) else "FAIL",
            "contract": contract,
            "dataset_manifest_sha256": dataset["manifest_sha256"],
            "gates": gates,
            "stats": stats,
            "worker_churn": {
                "joined": stats["workers_joined"],
                "removed": stats["workers_removed"],
                "lost": stats["workers_lost"],
                "failed_attempts": stats["tasks_failed"],
                "exhausted_attempts": stats["tasks_exhausted_attempts"],
            },
            "sampled_results": {
                "count": len(sample_paths),
                "bytes_each": expected_size,
                "sha256": sampled_hashes,
            },
            "parallelism": parallelism,
            "resources": {
                "workers": args.workers,
                "cores_per_worker": args.cores,
                "memory_mib_per_worker": args.memory,
            },
            "timing": {
                "worker_admission_seconds": admitted - loaded,
                "graph_load_seconds": loaded - load_started,
                "execution_seconds": terminal - loaded,
                "worker_pool_recovery_seconds": recovered - terminal,
                "total_seconds": terminal - started,
                "tasks_per_second": workload.tasks / (terminal - loaded),
            },
            "peak": {"load": load_peak_result, "execute": execute_peak_result},
            "factory_command": list(factory_command),
            "source_transport": "taskvine-file-url-shared-filesystem",
            "source_cache": "task",
            "environment": {
                "hostname": platform.node(), "python": sys.version,
                "maximum_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "commit": subprocess.run(("git", "rev-parse", "HEAD"), cwd=repository, text=True, stdout=subprocess.PIPE, check=True).stdout.strip(),
                "attempt_schedule_depth": schedule_depth,
                "runtime_info_storage": "node-local-temporary",
            },
        }
        atomic_json(output / "summary.json", result)
        atomic_json(output / "parallelism.json", samples)
        run_succeeded = result["status"] == "PASS"
        print(json.dumps({"status": result["status"], "output": str(output), "gates": gates}, sort_keys=True), flush=True)
        os.chdir(previous)
        return 0 if result["status"] == "PASS" else 1
    except BaseException as error:
        atomic_json(output / "failure.json", {
            "status": "FAIL", "error_type": type(error).__name__,
            "detail": str(error), "elapsed_seconds": time.monotonic() - started,
            "runtime_info_root": str(runtime_info_root),
        })
        raise
    finally:
        if load_peak is not None:
            try:
                load_peak.stop()
            except (EOFError, RuntimeError):
                pass
        if execute_peak is not None:
            try:
                execute_peak.stop()
            except (EOFError, RuntimeError):
                pass
        # vine_factory removes 128 Condor jobs serially during SIGTERM cleanup;
        # allow that graceful path to finish instead of killing it after the
        # generic 30-second service timeout and leaking the tail of the pool.
        terminate_group(factory, timeout=300)
        if factory_log is not None:
            factory_log.close()
        if manager is not None:
            manager._free()
            manager = None
        if run_succeeded:
            shutil.rmtree(runtime_info_root, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
