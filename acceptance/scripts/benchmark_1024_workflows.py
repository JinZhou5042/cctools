#!/usr/bin/env python3
"""Fixed-core, paired DataVine versus TaskVine workflow campaign."""

import argparse
import cloudpickle
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import site
import statistics
import subprocess
import sys
import time

import compare_workflows as compare_workflows_module

from compare_workflows import (
    DataVinePool,
    TaskVinePool,
    cgroup_cpu_quota,
    run_datavine,
    run_taskvine,
)


ARTIFACT_TYPE = "datavine-taskvine-1024-core-result"
SCHEMA_VERSION = 1
SAFE_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,95}$")


def timed_node(key, cpu_ms, output_bytes, parents=(), seed=1):
    return {
        "key": str(key),
        "iterations": float(cpu_ms),
        "parents": list(parents),
        "output_bytes": int(output_bytes),
        "seed": int(seed),
        "kind": "timed",
    }


def finish_case(case_id, phase, topology, nodes=None, sinks=None, **parameters):
    if not SAFE_ID.fullmatch(case_id):
        raise ValueError(f"unsafe case id: {case_id}")
    if topology == "dynamic":
        spec = {
            "mode": "dynamic",
            "kind": "timed",
            "steps": int(parameters["steps"]),
            "iterations": float(parameters["target_cpu_ms"]),
            "output_bytes": int(parameters["output_bytes"]),
        }
        tasks = spec["steps"]
        edges = max(0, tasks - 1)
        logical_edge_bytes = edges * int(parameters["output_bytes"])
        requested_bytes = int(parameters["output_bytes"])
        max_indegree = max_outdegree = 1 if tasks > 1 else 0
    else:
        spec = {"nodes": list(nodes), "sinks": list(sinks)}
        by_key = {item["key"]: item for item in spec["nodes"]}
        if len(by_key) != len(spec["nodes"]):
            raise ValueError(f"{case_id}: duplicate task key")
        outdegrees = {key: 0 for key in by_key}
        logical_edge_bytes = 0
        for item in spec["nodes"]:
            for parent in item["parents"]:
                if parent not in by_key:
                    raise ValueError(f"{case_id}: missing parent {parent}")
                outdegrees[parent] += 1
                logical_edge_bytes += int(by_key[parent]["output_bytes"])
        if any(key not in by_key for key in spec["sinks"]):
            raise ValueError(f"{case_id}: missing sink")
        tasks = len(spec["nodes"])
        edges = sum(len(item["parents"]) for item in spec["nodes"])
        requested_bytes = sum(by_key[key]["output_bytes"] for key in spec["sinks"])
        max_indegree = max((len(item["parents"]) for item in spec["nodes"]), default=0)
        max_outdegree = max(outdegrees.values(), default=0)
    return {
        "case_id": case_id,
        "phase": phase,
        "topology": topology,
        "parameters": parameters,
        "graph": {
            "logical_tasks": tasks,
            "logical_edges": edges,
            "logical_edge_payload_bytes": logical_edge_bytes,
            "requested_payload_bytes": requested_bytes,
            "max_indegree": max_indegree,
            "max_outdegree": max_outdegree,
        },
        "spec": spec,
    }


def independent_case(case_id, phase, width, waves, cpu_ms, output_bytes):
    count = int(width) * int(waves)
    nodes = [
        timed_node(f"n-{index}", cpu_ms, output_bytes, seed=index + 1)
        for index in range(count)
    ]
    return finish_case(
        case_id, phase, "independent", nodes, [item["key"] for item in nodes],
        width=width, waves=waves, target_cpu_ms=cpu_ms,
        output_bytes=output_bytes,
    )


def broadcast_case(case_id, phase, width, cpu_ms, input_bytes, output_bytes=0):
    nodes = [timed_node("root", cpu_ms, input_bytes, seed=1)]
    nodes.extend(
        timed_node(f"consumer-{index}", cpu_ms, output_bytes, ("root",), index + 2)
        for index in range(width)
    )
    return finish_case(
        case_id, phase, "broadcast", nodes,
        [f"consumer-{index}" for index in range(width)], width=width,
        target_cpu_ms=cpu_ms, input_bytes=input_bytes, output_bytes=output_bytes,
    )


def regular_pipeline_case(case_id, phase, width, depth, degree, cpu_ms, payload):
    degree = min(int(degree), int(width))
    nodes = []
    for level in range(depth):
        for lane in range(width):
            parents = () if level == 0 else tuple(
                f"l{level - 1}-{(lane + offset) % width}" for offset in range(degree)
            )
            nodes.append(timed_node(
                f"l{level}-{lane}", cpu_ms, payload, parents,
                seed=level * width + lane + 1,
            ))
    return finish_case(
        case_id, phase, "regular-pipeline", nodes,
        [f"l{depth - 1}-{lane}" for lane in range(width)], width=width,
        depth=depth, degree=degree, target_cpu_ms=cpu_ms,
        payload_bytes=payload,
    )


def chain_case(case_id, phase, lanes, depth, cpu_ms, payload):
    return regular_pipeline_case(case_id, phase, lanes, depth, 1, cpu_ms, payload)


def fan_case(case_id, phase, direction, width, degree, cpu_ms, payload):
    degree = min(int(degree), int(width))
    nodes = []
    if direction == "out":
        producers = math.ceil(width / degree)
        nodes.extend(timed_node(f"p-{i}", cpu_ms, payload, seed=i + 1)
                     for i in range(producers))
        nodes.extend(timed_node(
            f"c-{i}", cpu_ms, payload, (f"p-{i // degree}",),
            seed=producers + i + 1,
        ) for i in range(width))
        sinks = [f"c-{i}" for i in range(width)]
        topology = "fan-out"
    else:
        nodes.extend(timed_node(f"p-{i}", cpu_ms, payload, seed=i + 1)
                     for i in range(width))
        consumers = math.ceil(width / degree)
        for group in range(consumers):
            parents = tuple(
                f"p-{i}" for i in range(group * degree, min(width, (group + 1) * degree))
            )
            nodes.append(timed_node(
                f"c-{group}", cpu_ms, payload, parents, seed=width + group + 1
            ))
        sinks = [f"c-{i}" for i in range(consumers)]
        topology = "fan-in"
    return finish_case(
        case_id, phase, topology, nodes, sinks, width=width, degree=degree,
        target_cpu_ms=cpu_ms, payload_bytes=payload,
    )


def reduction_case(case_id, phase, width, degree, cpu_ms, payload):
    nodes = [timed_node(f"l0-{i}", cpu_ms, payload, seed=i + 1)
             for i in range(width)]
    current = [f"l0-{i}" for i in range(width)]
    level = 1
    while len(current) > 1:
        following = []
        for group, start in enumerate(range(0, len(current), degree)):
            key = f"l{level}-{group}"
            nodes.append(timed_node(
                key, cpu_ms, payload, tuple(current[start:start + degree]),
                seed=level * width + group + 1,
            ))
            following.append(key)
        current = following
        level += 1
    return finish_case(
        case_id, phase, "reduction-tree", nodes, current, width=width,
        degree=degree, levels=level, target_cpu_ms=cpu_ms,
        payload_bytes=payload,
    )


def heavy_tail_case(case_id, phase, width, payload):
    pattern = (0, 1, 1, 10, 10, 10, 100, 100, 1000)
    nodes = [
        timed_node(f"n-{i}", pattern[i % len(pattern)], payload, seed=i + 1)
        for i in range(width)
    ]
    return finish_case(
        case_id, phase, "heavy-tail", nodes, [item["key"] for item in nodes],
        width=width, target_cpu_ms="mixed-0-1000", output_bytes=payload,
    )


def build_cases(manifest, total_cores):
    base = manifest["base_case"]
    width = int(total_cores)
    depth = int(base["depth"])
    base_cpu = float(base["target_cpu_ms"])
    base_payload = int(base["payload_bytes"])
    large_width = min(width, int(manifest["large_payload_width"]))
    cases = []
    cases.append(independent_case("admission_immediate", "admission", width, 1, 0, 0))
    for cpu_ms in manifest["axes"]["target_cpu_ms"]:
        cases.append(independent_case(
            f"cpu_{int(cpu_ms)}ms", "cpu", width, 4, cpu_ms, 0
        ))
    for payload in manifest["axes"]["payload_bytes"]:
        case_width = large_width if payload >= 32 * 1024 * 1024 else width
        cases.append(independent_case(
            f"output_{payload}b", "data", case_width, 1, base_cpu, payload
        ))
        cases.append(broadcast_case(
            f"input_broadcast_{payload}b", "data", case_width,
            base_cpu, payload, 0,
        ))
    for degree in (1, 4, 16, 64):
        effective = min(degree, width)
        cases.append(regular_pipeline_case(
            f"degree_{degree}", "degree", width, 2, effective,
            base_cpu, base_payload,
        ))
    cases.extend((
        independent_case("topology_map", "topology", width, 1, base_cpu, base_payload),
        chain_case("topology_chains", "topology", width, depth, base_cpu, base_payload),
        fan_case("topology_fanout16", "topology", "out", width,
                 min(16, width), base_cpu, base_payload),
        fan_case("topology_fanin16", "topology", "in", width,
                 min(16, width), base_cpu, base_payload),
        regular_pipeline_case("topology_pipeline2", "topology", width, depth,
                              min(2, width), base_cpu, base_payload),
        reduction_case("topology_reduce16", "topology", width,
                       min(16, width), base_cpu, base_payload),
        broadcast_case("topology_broadcast1024", "topology", width,
                       base_cpu, base_payload, base_payload),
        heavy_tail_case("topology_heavy_tail", "topology", width, base_payload),
        finish_case(
            "topology_dynamic", "topology", "dynamic", steps=8,
            target_cpu_ms=base_cpu, output_bytes=base_payload,
        ),
    ))
    for cpu_ms in (0, 100):
        for payload in (0, 65536, 1048576):
            for degree in (1, 16, 64):
                effective = min(degree, width)
                cases.append(regular_pipeline_case(
                    f"interaction_c{cpu_ms}_p{payload}_d{degree}",
                    "interaction", width, 2, effective, cpu_ms, payload,
                ))
    # Single-axis causal control for the bounded 32 MiB broadcast.  Large
    # payload cases use large_width to cap aggregate data, whereas the normal
    # zero-byte broadcast uses all 1024 tasks; this same-width control is what
    # lets attribution separate payload cost from fixed topology cost.
    cases.append(broadcast_case(
        "confirmation_input_broadcast_0b_w128", "confirmation", large_width,
        base_cpu, 0, 0,
    ))
    unique = {case["case_id"]: case for case in cases}
    if len(unique) != len(cases):
        raise ValueError("campaign generated duplicate case ids")
    return cases


def validate_manifest(manifest):
    errors = []
    if manifest.get("artifact_type") != "datavine-taskvine-1024-core-campaign":
        errors.append("wrong artifact_type")
    if manifest.get("schema_version") != 1:
        errors.append("unsupported schema_version")
    resource = manifest.get("resource_contract", {})
    if resource.get("workers", 0) * resource.get("cores_per_worker", 0) != resource.get("total_cores"):
        errors.append("workers * cores_per_worker != total_cores")
    if resource.get("total_cores") != 1024:
        errors.append("publication resource contract is not 1024 cores")
    if resource.get("cores_per_task") != 1 or not resource.get("exact_admission"):
        errors.append("one-core task or exact admission contract missing")
    stats = manifest.get("statistics", {})
    if stats.get("final_repetitions", 0) < 10:
        errors.append("final campaign requires at least ten repetitions")
    if stats.get("bootstrap_resamples", 0) < 10000:
        errors.append("bootstrap contract requires at least 10000 resamples")
    return errors


def inventory_snapshot(repository, pool, expected_workers, expected_cores):
    if hasattr(pool, "executor"):
        pool.executor.manager._refresh_stats()
        stats = pool.executor.manager.stats
        values = list(pool.inventory)
        details = pool.executor.manager.status("workers")
        passed = (
            int(stats.workers_connected) == expected_workers and
            int(stats.total_cores) == expected_workers * expected_cores and
            len(details) == expected_workers and
            len(values) == expected_workers and
            all(value == expected_cores for value in values)
        )
        return {
            "status": "PASS" if passed else "FAIL",
            "source": "TaskVine manager stats/status and startup inventory",
            "workers": int(stats.workers_connected),
            "total_cores": int(stats.total_cores),
            "cores_per_worker": values,
            "worker_details": details,
            "raw_stdout": None,
            "raw_stderr": None,
        }
    binary = repository / "taskvine/src/tools/vine_status"
    command = [str(binary), "-W", "localhost", str(pool.manager_port)]
    completed = subprocess.run(
        command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        timeout=30,
    )
    rows = [line for line in completed.stdout.splitlines()[1:] if line.strip()]
    values = list(pool.inventory)
    passed = (
        completed.returncode == 0 and len(rows) == expected_workers and
        len(values) == expected_workers and
        all(value == expected_cores for value in values)
    )
    return {
        "status": "PASS" if passed else "FAIL",
        "command": command,
        "returncode": completed.returncode,
        "workers": len(rows),
        "cores_per_worker": values,
        "raw_stdout": completed.stdout,
        "raw_stderr": completed.stderr,
    }


def wait_for_exact_inventory(repository, pool, expected_workers, expected_cores,
                             timeout, poll_seconds=2.0):
    """Pause on worker churn, but never admit a partial pool into a run."""
    started = time.monotonic()
    attempts = 0
    last = None
    while True:
        attempts += 1
        last = inventory_snapshot(
            repository, pool, expected_workers, expected_cores
        )
        if last["status"] == "PASS":
            last["admission_wait_seconds"] = time.monotonic() - started
            last["admission_attempts"] = attempts
            return last
        if time.monotonic() - started >= timeout:
            last["admission_wait_seconds"] = time.monotonic() - started
            last["admission_attempts"] = attempts
            return last
        if hasattr(pool, "executor"):
            # TaskVine's Python manager has no background network thread;
            # drive its event loop so replacement Worker connections queued
            # on the listen socket can actually be accepted.
            pool.executor.manager.wait(int(max(1, poll_seconds)))
        else:
            time.sleep(poll_seconds)


def compact_admission(admission, expected_cores):
    """Keep the per-run proof without duplicating full inventories 100s of times."""
    return {
        "status": admission["status"],
        "workers": admission["workers"],
        "total_cores": admission.get(
            "total_cores", admission["workers"] * expected_cores
        ),
        "cores_per_worker": admission["cores_per_worker"],
        "admission_wait_seconds": admission.get("admission_wait_seconds", 0.0),
        "admission_attempts": admission.get("admission_attempts", 1),
    }


def median(values):
    return statistics.median(values)


def bootstrap_interval(values, resamples, confidence, seed):
    if not values:
        return [None, None]
    rng = random.Random(seed)
    samples = []
    for _ in range(resamples):
        samples.append(median([values[rng.randrange(len(values))] for _ in values]))
    samples.sort()
    tail = (1.0 - confidence) / 2.0
    low = samples[max(0, math.floor(tail * len(samples)))]
    high = samples[min(len(samples) - 1, math.ceil((1.0 - tail) * len(samples)) - 1)]
    return [low, high]


def median_stage(runs, name):
    values = [item.get("runtime_stages", {}).get(name) for item in runs]
    values = [float(item) for item in values if item is not None]
    return median(values) if values else 0.0


def median_taskvine_stat(runs, name):
    values = [item.get("taskvine_stats", {}).get(name) for item in runs]
    values = [float(item) for item in values if item is not None]
    return median(values) if values else 0.0


def median_ingest_metric(runs, name):
    values = [item.get("ingest_profile", {}).get(name) for item in runs
              if item.get("ingest_profile")]
    values = [float(item) for item in values if item is not None]
    return median(values) if values else 0.0


def classify_regression(case, tv_runs, dv_runs, ratio_ci):
    tv_seconds = median([item["total_seconds"] for item in tv_runs])
    dv_seconds = median([item["total_seconds"] for item in dv_runs])
    gap = dv_seconds - tv_seconds
    result = {
        "required": ratio_ci[1] is not None and ratio_ci[1] < 1.0,
        "status": "NOT_REQUIRED",
        "classification": None,
        "absolute_excess_seconds": max(0.0, gap),
        "explained_fraction": None,
        "evidence": {},
        "improvement": None,
    }
    if not result["required"]:
        return result
    stages = {
        name: median_stage(dv_runs, name)
        for name in (
            "setup_seconds", "materialize_seconds", "submit_seconds",
            "manager_lock_seconds", "submission_event_seconds",
            "publish_seconds", "publication_prepare_seconds",
            "publication_queue_seconds", "publication_commit_seconds",
            "python_decode_seconds", "python_function_seconds",
            "python_serialize_seconds", "python_fsync_seconds",
        )
    }
    separated_fetch = all("fetch_seconds" in item for item in tv_runs + dv_runs)
    dv_fetch = median([item.get("fetch_seconds", 0.0) for item in dv_runs])
    tv_fetch = median([item.get("fetch_seconds", 0.0) for item in tv_runs])
    fetch_excess = max(0.0, dv_fetch - tv_fetch)
    client_wait_fetch = median([
        max(0.0, item["total_seconds"] - item["build_submit_seconds"] -
            item.get("runtime_stages", {}).get("run_seconds", 0.0))
        for item in dv_runs
    ])
    build_excess = max(
        0.0,
        median([item["build_submit_seconds"] for item in dv_runs]) -
        median([item["build_submit_seconds"] for item in tv_runs]),
    )
    dv_workflow_build = median([
        item.get("workflow_build_seconds", 0.0) for item in dv_runs
    ])
    dv_workflow_submit = median([
        item.get("workflow_submit_seconds", 0.0) for item in dv_runs
    ])
    client_ingest = min(build_excess, dv_workflow_submit)
    client_build_control = max(0.0, build_excess - client_ingest)
    poll_residual = median([
        max(0.0, item.get("terminal_wait_seconds", 0.0) -
            item.get("runtime_stages", {}).get("setup_seconds", 0.0) -
            item.get("runtime_stages", {}).get("run_seconds", 0.0))
        for item in dv_runs
    ]) if separated_fetch else 0.0
    # Only synchronous, non-overlapping runtime-lane stages may explain a wall
    # gap directly.  queue/commit/decode/function/serialize/fsync are summed
    # across concurrent publication workers and are work-amplification signals,
    # not critical-path seconds.
    publication = stages["publication_prepare_seconds"] + stages["publish_seconds"]
    fixed_control = (
        client_build_control + poll_residual + stages["setup_seconds"] +
        median_stage(dv_runs, "checkpoint_seconds") +
        stages["submission_event_seconds"] +
        median_stage(dv_runs, "completion_event_seconds")
    )
    dv_worker = median_stage(dv_runs, "manager_time_workers_execute_good_us") / 1e6
    tv_worker = median_taskvine_stat(tv_runs, "time_workers_execute_good") / 1e6
    executor_excess = max(0.0, dv_worker - tv_worker)
    dv_transfer = (
        median_stage(dv_runs, "manager_time_send_good_us") +
        median_stage(dv_runs, "manager_time_receive_good_us")
    ) / 1e6
    tv_transfer = (
        median_taskvine_stat(tv_runs, "time_send_good") +
        median_taskvine_stat(tv_runs, "time_receive_good")
    ) / 1e6
    transfer_excess = max(0.0, dv_transfer - tv_transfer)
    runtime_invocations = median_stage(dv_runs, "runtime_invocations")
    dynamic_roundtrip_excess = 0.0
    if case.get("spec", {}).get("mode") == "dynamic":
        # Dynamic sessions intentionally expose one submit/result dependency at
        # a time.  Unlike TaskVine's resident library path, the current
        # DataVine session re-enters the runtime for every append plus seal.
        # Terminal-wait excess is therefore on the critical path (not summed
        # concurrent service time), and equal measured useful CPU removes the
        # user function itself from this boundary cost.
        dynamic_roundtrip_excess = max(
            0.0,
            median([item.get("terminal_wait_seconds", 0.0) for item in dv_runs]) -
            median([item.get("terminal_wait_seconds", 0.0) for item in tv_runs]),
        )
    candidates = [
        ("dynamic-session-roundtrips", dynamic_roundtrip_excess,
         "keep one WorkflowSession runtime lane resident across append/result cycles; submit appended tasks directly to that lane and seal it once"),
        ("client-object-ingest", client_ingest,
         "increase bounded post-serialization object I/O concurrency; preserve one file per object and leave batch registration/chunking as a future interface"),
        ("result-fetch", fetch_excess,
         "add batched/multi-result fetch and avoid one RPC/decode per requested DataID"),
        ("publication", publication,
         "batch retained-output serialization, hashing, fsync and result fetch"),
        ("fixed-control", fixed_control,
         "reduce RPC polling, workflow setup, checkpoint and completion projection"),
        ("graph-materialization", stages["materialize_seconds"],
         "compact task/DataID materialization and remove per-edge copies"),
        ("scheduler-submit", stages["submit_seconds"] + stages["manager_lock_seconds"],
         "bulk native submission/completion draining without semantic batching"),
    ]
    candidates.sort(key=lambda item: item[1], reverse=True)
    # Prefer a directly differenced, separated critical-path excess when it
    # already explains the regression.  An absolute DataVine-only stage may be
    # larger, but cannot by itself establish why DataVine is slower.
    direct = [item for item in candidates
              if item[0] in ("dynamic-session-roundtrips", "result-fetch",
                             "client-object-ingest") and
              gap > 0 and item[1] / gap >= 0.5]
    winner = max(direct, key=lambda item: item[1]) if direct else candidates[0]
    fraction = winner[1] / gap if gap > 0 else 0.0
    if fraction >= 0.5:
        result.update({
            "status": "CONFIRMED",
            "classification": winner[0],
            "explained_fraction": min(1.0, fraction),
            "improvement": winner[2],
        })
        if not separated_fetch and result["classification"] == "fixed-control":
            result.update({
                "status": "SCREENING_ONLY",
                "classification": "client-wait-fetch-unseparated",
                "improvement": "repeat with terminal wait and result fetch split",
            })
    elif gap > 0:
        selected = []
        cumulative = 0.0
        for candidate in candidates:
            if candidate[1] <= 0:
                continue
            selected.append(candidate)
            cumulative += candidate[1]
            if cumulative / gap >= 0.8:
                break
        if cumulative / gap >= 0.8:
            result.update({
                "status": "CONFIRMED",
                "classification": "+".join(item[0] for item in selected),
                "explained_fraction": min(1.0, cumulative / gap),
                "improvement": "; ".join(item[2] for item in selected),
            })
    tv_cpu = median([item.get("useful_cpu_seconds", 0.0) for item in tv_runs])
    dv_cpu = median([item.get("useful_cpu_seconds", 0.0) for item in dv_runs])
    cpu_relative_difference = abs(tv_cpu - dv_cpu) / max(tv_cpu, dv_cpu, 1e-12)
    if (result["classification"] is None and
          case["graph"]["logical_edge_payload_bytes"] == 0 and
          case["graph"]["requested_payload_bytes"] == 0 and
          cpu_relative_difference <= 0.05):
        # With equal measured kernel CPU and no edge/requested payload, the
        # paired excess is causally constrained to framework/control work.
        result.update({
            "status": "CONFIRMED",
            "classification": "framework-control",
            "explained_fraction": 1.0,
            "improvement": "reduce per-task FunctionCall, RPC polling, workflow setup, durable zero-byte publication and completion projection",
        })
    if result["classification"] is None:
        result.update({
            "status": "UNRESOLVED",
            "classification": "unresolved",
            "explained_fraction": fraction,
            "improvement": "run a confirmation pair with one suspected axis changed",
        })
    result["evidence"] = {
        "taskvine_median_seconds": tv_seconds,
        "datavine_median_seconds": dv_seconds,
        "datavine_stage_medians": stages,
        "largest_measured_component": {"name": winner[0], "seconds": winner[1]},
        "critical_path_candidates": [
            {"name": name, "seconds": seconds,
             "fraction_of_gap": (seconds / gap if gap > 0 else None)}
            for name, seconds, _ in candidates
        ],
        "client_wait_fetch_residual_seconds": client_wait_fetch,
        "separated_fetch_timing": separated_fetch,
        "datavine_fetch_seconds": dv_fetch if separated_fetch else None,
        "taskvine_fetch_seconds": tv_fetch if separated_fetch else None,
        "fetch_excess_seconds": fetch_excess if separated_fetch else None,
        "build_submit_excess_seconds": build_excess,
        "datavine_workflow_build_seconds": dv_workflow_build,
        "datavine_workflow_submit_seconds": dv_workflow_submit,
        "client_object_ingest_excess_seconds": client_ingest,
        "datavine_ingest_profile_medians": {
            "object_put_wall_seconds": median_ingest_metric(
                dv_runs, "object_put_wall_nanoseconds"
            ) / 1e9,
            "object_sharedfs_aggregate_seconds": median_ingest_metric(
                dv_runs, "object_sharedfs_nanoseconds"
            ) / 1e9,
            "object_rpc_aggregate_seconds": median_ingest_metric(
                dv_runs, "object_put_rpc_nanoseconds"
            ) / 1e9,
            "python_serialization_seconds": (
                median_ingest_metric(
                    dv_runs, "python_function_serialize_nanoseconds"
                ) + median_ingest_metric(
                    dv_runs, "python_value_serialize_nanoseconds"
                ) + median_ingest_metric(
                    dv_runs, "python_invocation_serialize_nanoseconds"
                )
            ) / 1e9,
            "object_put_parallelism": median_ingest_metric(
                dv_runs, "object_put_parallelism"
            ),
        },
        "terminal_poll_residual_seconds": poll_residual if separated_fetch else None,
        "runtime_invocations": runtime_invocations,
        "dynamic_session_roundtrip_excess_seconds": dynamic_roundtrip_excess,
        "dynamic_session_contract": (
            "sequential append/result dependency with one final seal"
            if case.get("spec", {}).get("mode") == "dynamic" else None
        ),
        "worker_execution_excess_seconds": executor_excess,
        "manager_transfer_excess_seconds": transfer_excess,
        "parallel_publication_service_seconds": {
            "queue": stages["publication_queue_seconds"],
            "commit": stages["publication_commit_seconds"],
            "decode": stages["python_decode_seconds"],
            "function": stages["python_function_seconds"],
            "serialize": stages["python_serialize_seconds"],
            "fsync": stages["python_fsync_seconds"],
            "wall_gap_attribution_allowed": False,
        },
        "logical_edge_payload_bytes": case["graph"]["logical_edge_payload_bytes"],
        "requested_payload_bytes": case["graph"]["requested_payload_bytes"],
        "taskvine_useful_cpu_seconds": tv_cpu,
        "datavine_useful_cpu_seconds": dv_cpu,
        "useful_cpu_relative_difference": cpu_relative_difference,
    }
    return result


def resolve_with_zero_payload_controls(rows):
    """Resolve residual regressions using a topology-identical zero-byte case.

    A single-axis matched pair decomposes the target absolute gap into the
    payload-independent control gap and its payload-path difference-in-
    differences residual.  No concurrent service-time sum is treated as wall
    time in this attribution.
    """
    byte_parameters = {"input_bytes", "output_bytes", "payload_bytes"}

    def nonbyte_parameters(row):
        return {
            key: value for key, value in row["parameters"].items()
            if key not in byte_parameters
        }

    def same_structure(left, right):
        graph_keys = (
            "logical_tasks", "logical_edges", "max_indegree", "max_outdegree",
        )
        return (
            left["topology"] == right["topology"] and
            nonbyte_parameters(left) == nonbyte_parameters(right) and
            all(left["graph"][key] == right["graph"][key] for key in graph_keys)
        )

    for row in rows:
        bottleneck = row["bottleneck"]
        if bottleneck.get("status") != "UNRESOLVED":
            continue
        target_gap = bottleneck.get("absolute_excess_seconds", 0.0)
        if target_gap <= 0:
            continue
        candidates = [
            control for control in rows
            if control is not row and same_structure(row, control) and
            control["graph"]["logical_edge_payload_bytes"] == 0 and
            control["graph"]["requested_payload_bytes"] == 0 and
            control["bottleneck"].get("status") == "CONFIRMED"
        ]
        if not candidates:
            continue
        control = max(
            candidates,
            key=lambda item: item["bottleneck"].get("absolute_excess_seconds", 0.0),
        )
        control_gap = control["bottleneck"]["absolute_excess_seconds"]
        if control_gap <= 0:
            continue
        control_fraction = min(1.0, control_gap / target_gap)
        payload_residual = max(0.0, target_gap - control_gap)
        classification = control["bottleneck"]["classification"]
        bottleneck.update({
            "status": "CONFIRMED",
            "classification": (
                f"zero-payload-control:{classification}+payload-path-residual"
                if payload_residual > 0 else
                f"zero-payload-control:{classification}"
            ),
            "explained_fraction": 1.0,
            "improvement": (
                f"{control['bottleneck']['improvement']}; reduce parent-payload "
                "decode/materialization and preserve worker-local reuse"
            ),
        })
        bottleneck["evidence"]["matched_zero_payload_control"] = {
            "case_id": control["case_id"],
            "control_absolute_excess_seconds": control_gap,
            "target_absolute_excess_seconds": target_gap,
            "fraction_reproduced_without_payload": control_fraction,
            "payload_independent_control_seconds": min(control_gap, target_gap),
            "payload_specific_residual_seconds": payload_residual,
            "payload_specific_fraction": payload_residual / target_gap,
            "causal_constraint": (
                "same graph, CPU, degree and sinks; only payload bytes change"
            ),
        }


def summarize(cases, runs, manifest):
    by_case = {case["case_id"]: case for case in cases}
    rows = []
    stats = manifest["statistics"]
    for case_id in sorted(by_case):
        selected = [item for item in runs if item["case_id"] == case_id]
        tv = [item["result"] for item in selected if item["backend"] == "TV-native"]
        dv = [item["result"] for item in selected if item["backend"] == "DV-native"]
        pairs = []
        for repetition in sorted({item["repetition"] for item in selected}):
            left = next((item["result"] for item in selected
                         if item["repetition"] == repetition and item["backend"] == "TV-native"), None)
            right = next((item["result"] for item in selected
                          if item["repetition"] == repetition and item["backend"] == "DV-native"), None)
            if left and right:
                pairs.append(left["total_seconds"] / right["total_seconds"])
        ci = bootstrap_interval(
            pairs, int(stats["bootstrap_resamples"]), float(stats["confidence"]),
            int(hashlib.sha256(case_id.encode()).hexdigest()[:16], 16),
        ) if len(pairs) >= 3 else [None, None]
        row = {
            "case_id": case_id,
            "phase": by_case[case_id]["phase"],
            "topology": by_case[case_id]["topology"],
            "graph": by_case[case_id]["graph"],
            "parameters": by_case[case_id]["parameters"],
            "paired_repetitions": len(pairs),
            "taskvine_median_seconds": median([item["total_seconds"] for item in tv]) if tv else None,
            "datavine_median_seconds": median([item["total_seconds"] for item in dv]) if dv else None,
            "datavine_to_taskvine_rate": median(pairs) if pairs else None,
            "paired_bootstrap_95pct": ci,
        }
        row["bottleneck"] = classify_regression(by_case[case_id], tv, dv, ci) if tv and dv else {
            "required": False, "status": "INCOMPLETE", "classification": None
        }
        rows.append(row)
    resolve_with_zero_payload_controls(rows)
    return rows


def atomic_json(path, value):
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def compact_record(record):
    """Bound aggregate state; each run file retains its exact result values."""
    compact = dict(record)
    result = dict(compact["result"])
    values = result.pop("results", None)
    if values is not None:
        encoded = json.dumps(
            values, sort_keys=True, separators=(",", ":"),
        ).encode()
        result["results_count"] = len(values)
        result["results_sha256"] = hashlib.sha256(encoded).hexdigest()
    compact["result"] = result
    return compact


def compact_campaign_records(campaign):
    for name in ("warmups", "invalidated_warmups", "runs", "invalidated_runs"):
        campaign[name] = [compact_record(item) for item in campaign.get(name, ())]


def completed_pairs(records, repetition_key):
    grouped = {}
    for record in records:
        key = (record[repetition_key], record["case_id"])
        grouped.setdefault(key, set()).add(record["backend"])
    return {
        key for key, backends in grouped.items()
        if backends == {"TV-native", "DV-native"}
    }


def result_digest(result):
    if result.get("results_sha256"):
        return result["results_sha256"]
    values = result.get("results")
    if values is None:
        return None
    encoded = json.dumps(
        values, sort_keys=True, separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def paired_results_equal(records, repetition_key):
    grouped = {}
    for record in records:
        key = (record[repetition_key], record["case_id"])
        grouped.setdefault(key, {})[record["backend"]] = result_digest(
            record["result"])
    return bool(grouped) and all(
        set(pair) == {"TV-native", "DV-native"} and
        pair["TV-native"] is not None and
        pair["TV-native"] == pair["DV-native"]
        for pair in grouped.values()
    )


def next_attempt(output, kind, repetition, case_id):
    root = output / ("warmups" if kind == "w" else "runs")
    prefix = f"{kind}{repetition}-a"
    paths = root.glob(f"{prefix}*-{case_id}-*") if root.exists() else ()
    attempts = []
    for path in paths:
        match = re.match(rf"^{re.escape(prefix)}([0-9]+)-", path.name)
        if match:
            attempts.append(int(match.group(1)))
    return max(attempts, default=0)


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def worker_removals(result):
    """Return Worker removals observed inside one backend's measured window."""
    if result["backend"] == "taskvine":
        return int(result.get("taskvine_stats", {}).get("workers_removed", 0))
    return int(result.get("runtime_stages", {}).get("manager_workers_removed", 0))


def execution_identity(case_id, kind, repetition, attempt):
    if kind not in ("w", "r") or min(repetition, attempt) < 1:
        raise ValueError("invalid execution identity")
    return f"{case_id}__{kind}{repetition}a{attempt}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path(
        "acceptance/1024-core-workflows/campaign-v1.json"))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--phase", action="append", default=[])
    parser.add_argument("--case", action="append", default=[])
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--cores-per-worker", type=int)
    parser.add_argument("--batch-type", choices=("local", "condor"))
    parser.add_argument("--admission-recovery-timeout", type=float, default=900.0)
    parser.add_argument("--max-invalidated-attempts", type=int, default=20)
    parser.add_argument("--allow-nonpublication-scale", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true",
                        help="resume accepted pairs from campaign-state.json")
    args = parser.parse_args()
    manifest_path = args.manifest.resolve()
    manifest = json.loads(manifest_path.read_text())
    errors = validate_manifest(manifest)
    if errors:
        parser.error("; ".join(errors))
    contract = manifest["resource_contract"]
    workers = args.workers or int(contract["workers"])
    cores = args.cores_per_worker or int(contract["cores_per_worker"])
    total_cores = workers * cores
    batch_type = args.batch_type or contract["batch_type"]
    publication_scale = total_cores == 1024 and batch_type == "condor"
    if not publication_scale and not args.allow_nonpublication_scale:
        parser.error("non-1024/non-Condor execution requires --allow-nonpublication-scale")
    if min(workers, cores, args.repetitions or 1) < 1:
        parser.error("workers, cores and repetitions must be positive")
    affinity = len(os.sched_getaffinity(0))
    quota = cgroup_cpu_quota()
    capacity = min(affinity, quota or affinity)
    if batch_type == "local" and total_cores > capacity:
        parser.error(f"requested {total_cores} local cores exceed capacity {capacity:g}")
    cases = build_cases(manifest, total_cores)
    if args.phase:
        phases = set(args.phase)
        cases = [case for case in cases if case["phase"] in phases]
    if args.case:
        patterns = [re.compile(value) for value in args.case]
        cases = [case for case in cases
                 if any(pattern.search(case["case_id"]) for pattern in patterns)]
    if not cases:
        parser.error("case selection is empty")
    repetitions = args.repetitions or int(manifest["statistics"]["screening_repetitions"])
    matrix = [{key: value for key, value in case.items() if key != "spec"} for case in cases]
    if args.dry_run:
        print(json.dumps({
            "status": "DRY_RUN", "publication_scale": publication_scale,
            "workers": workers, "cores_per_worker": cores,
            "total_cores": total_cores, "repetitions": repetitions,
            "cases": matrix,
        }, indent=2, sort_keys=True))
        return 0
    output = args.output_dir.resolve()
    if output.exists() and any(output.iterdir()) and not args.resume:
        parser.error(f"output directory is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    repository = Path(__file__).resolve().parents[2]
    # This driver imports the shared kernels as a module, whereas the original
    # comparison script executes them from __main__.  Force by-value transport
    # so remote library sandboxes do not need the benchmark source installed.
    cloudpickle.register_pickle_by_value(compare_workflows_module)
    commit = subprocess.run(
        ("git", "rev-parse", "HEAD"), cwd=repository, check=True,
        text=True, stdout=subprocess.PIPE,
    ).stdout.strip()
    git_status = subprocess.run(
        ("git", "status", "--porcelain=v1"), cwd=repository, check=True,
        text=True, stdout=subprocess.PIPE,
    ).stdout.splitlines()
    source_paths = (
        Path(__file__).resolve(),
        repository / "acceptance/scripts/compare_workflows.py",
        repository / "taskvine/src/datavine/vine_datavine_workflow_runtime.c",
        repository / "taskvine/src/manager/vine_manager.c",
        repository / "taskvine/src/manager/vine_file_replica_table.c",
        repository / "taskvine/src/bindings/python3/taskvine.i",
        repository / "taskvine/src/bindings/python3/ndcctools/taskvine/file.py",
        repository / "taskvine/src/bindings/python3/ndcctools/taskvine/futures.py",
        manifest_path,
        repository / "taskvine/src/tools/datavine_workflow",
        repository / "taskvine/src/worker/vine_worker",
        Path(compare_workflows_module.vine.cvine.__file__).resolve(),
        Path(compare_workflows_module.vine.cvine._cvine.__file__).resolve(),
        Path(compare_workflows_module.vine.futures.__file__).resolve(),
    )
    new_campaign = {
        "artifact_type": ARTIFACT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": "RUNNING",
        "scope": "publication-1024-core" if publication_scale else "nonpublication-validation",
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "manifest": str(manifest_path),
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "environment": {
            "commit": commit, "python": sys.version, "platform": platform.platform(),
            "hostname": platform.node(), "python_user_site_enabled": bool(site.ENABLE_USER_SITE),
            "workers": workers, "cores_per_worker": cores, "total_cores": total_cores,
            "batch_type": batch_type,
            "git_status_porcelain": git_status,
            "source_sha256": {
                str(path.relative_to(repository) if path.is_relative_to(repository) else path): sha256_file(path)
                for path in source_paths
            },
        },
        "cases": matrix,
        "warmups": [],
        "invalidated_warmups": [],
        "runs": [],
        "invalidated_runs": [],
        "admission": {},
        "summary": [],
        "gates": {},
    }
    if args.resume:
        state_path = output / "campaign-state.json"
        if not state_path.exists():
            parser.error(f"resume state does not exist: {state_path}")
        campaign = json.loads(state_path.read_text())
        if campaign.get("manifest_sha256") != new_campaign["manifest_sha256"]:
            parser.error("resume manifest does not match")
        previous_environment = campaign.get("environment", {})
        expected_environment = {
            "workers": workers,
            "cores_per_worker": cores,
            "total_cores": total_cores,
            "batch_type": batch_type,
        }
        for name, expected in expected_environment.items():
            if previous_environment.get(name) != expected:
                parser.error(f"resume resource contract changed: {name}")
        compact_campaign_records(campaign)
        campaign["status"] = "RUNNING"
        campaign.pop("error", None)
        campaign.setdefault("resume_events", []).append({
            "at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "reason": "bounded-state checkpoint resume",
            "runner_sha256": sha256_file(Path(__file__).resolve()),
            "source_sha256": new_campaign["environment"]["source_sha256"],
        })
    else:
        campaign = new_campaign
    atomic_json(output / "campaign-state.json", campaign)
    pools = {}
    try:
        pool_generation = len(campaign.get("resume_events", ()))
        while any(
            (output / f"pools/{name}-resume-{pool_generation}").exists()
            for name in ("taskvine", "datavine")
        ):
            pool_generation += 1
        pool_suffix = f"-resume-{pool_generation}" if pool_generation else ""
        pools["TV-native"] = TaskVinePool(
            repository, output / f"pools/taskvine{pool_suffix}",
            workers, cores, batch_type
        )
        pools["DV-native"] = DataVinePool(
            repository, output / f"pools/datavine{pool_suffix}",
            workers, cores, batch_type
        )
        for backend, pool in pools.items():
            admission = wait_for_exact_inventory(
                repository, pool, workers, cores,
                args.admission_recovery_timeout,
            )
            campaign["admission"][backend] = admission
            if admission["status"] != "PASS":
                raise RuntimeError({"admission_failed": backend, "details": admission})
        warmup_repetitions = int(manifest["statistics"]["warmup_repetitions"])
        accepted_warmups = completed_pairs(campaign["warmups"], "warmup_repetition")
        for warmup_repetition in range(1, warmup_repetitions + 1):
            for case_index, case in enumerate(cases):
                if (warmup_repetition, case["case_id"]) in accepted_warmups:
                    continue
                order = (
                    ("TV-native", "DV-native")
                    if (case_index + warmup_repetition) % 2 else
                    ("DV-native", "TV-native")
                )
                execution_attempt = next_attempt(
                    output, "w", warmup_repetition, case["case_id"])
                while True:
                    execution_attempt += 1
                    if execution_attempt > args.max_invalidated_attempts:
                        raise RuntimeError("too many invalidated warmup attempts")
                    paired = {}
                    records = []
                    invalid = False
                    for backend in order:
                        admission = wait_for_exact_inventory(
                            repository, pools[backend], workers, cores,
                            args.admission_recovery_timeout,
                        )
                        if admission["status"] != "PASS":
                            raise RuntimeError({
                                "admission_failed_before_warmup": backend,
                                "case": case["case_id"],
                                "warmup_repetition": warmup_repetition,
                                "details": admission,
                            })
                        run_root = (
                            output / "warmups" /
                            f"w{warmup_repetition}-a{execution_attempt}-"
                            f"{case['case_id']}-{backend.lower()}"
                        )
                        runner = run_taskvine if backend == "TV-native" else run_datavine
                        identity = execution_identity(
                            case["case_id"], "w", warmup_repetition,
                            execution_attempt,
                        )
                        result = runner(
                            pools[backend], run_root, identity,
                            case["spec"], 0,
                        )
                        if result["logical_tasks"] != case["graph"]["logical_tasks"]:
                            raise RuntimeError("warmup logical task count mismatch")
                        post_admission = wait_for_exact_inventory(
                            repository, pools[backend], workers, cores,
                            args.admission_recovery_timeout,
                        )
                        removals = worker_removals(result)
                        invalid |= removals > 0 or post_admission["admission_attempts"] > 1
                        record = {
                            "case_id": case["case_id"], "phase": case["phase"],
                            "backend": backend, "backend_order": list(order),
                            "warmup_repetition": warmup_repetition,
                            "execution_attempt": execution_attempt,
                            "execution_identity": identity,
                            "admission": compact_admission(admission, cores),
                            "post_admission": compact_admission(post_admission, cores),
                            "workers_removed_during_run": removals,
                            "result": result,
                        }
                        atomic_json(run_root / "campaign-result.json", record)
                        records.append(record)
                        paired[backend] = result
                    if paired["TV-native"]["results"] != paired["DV-native"]["results"]:
                        raise RuntimeError({
                            "case": case["case_id"],
                            "warmup_repetition": warmup_repetition,
                            "error": "backend warmup result mismatch",
                        })
                    if invalid:
                        campaign["invalidated_warmups"].extend(
                            compact_record(item) for item in records)
                        status = "WARMUP_INVALIDATED_WORKER_CHURN"
                    else:
                        campaign["warmups"].extend(
                            compact_record(item) for item in records)
                        status = "WARMUP_PASS"
                    atomic_json(output / "campaign-state.json", campaign)
                    print(json.dumps({
                        "case": case["case_id"],
                        "warmup_repetition": warmup_repetition,
                        "execution_attempt": execution_attempt,
                        "status": status,
                    }, sort_keys=True), flush=True)
                    if not invalid:
                        break
        accepted_runs = completed_pairs(campaign["runs"], "repetition")
        for repetition in range(1, repetitions + 1):
            order = ("TV-native", "DV-native") if repetition % 2 else (
                "DV-native", "TV-native")
            for case in cases:
                if (repetition, case["case_id"]) in accepted_runs:
                    continue
                execution_attempt = next_attempt(
                    output, "r", repetition, case["case_id"])
                while True:
                    execution_attempt += 1
                    if execution_attempt > args.max_invalidated_attempts:
                        raise RuntimeError("too many invalidated measured attempts")
                    paired = {}
                    records = []
                    invalid = False
                    for backend in order:
                        admission = wait_for_exact_inventory(
                            repository, pools[backend], workers, cores,
                            args.admission_recovery_timeout,
                        )
                        if admission["status"] != "PASS":
                            raise RuntimeError({
                                "admission_failed_before_run": backend,
                                "case": case["case_id"], "repetition": repetition,
                                "details": admission,
                            })
                        run_root = (
                            output / "runs" /
                            f"r{repetition}-a{execution_attempt}-"
                            f"{case['case_id']}-{backend.lower()}"
                        )
                        runner = run_taskvine if backend == "TV-native" else run_datavine
                        identity = execution_identity(
                            case["case_id"], "r", repetition,
                            execution_attempt,
                        )
                        result = runner(
                            pools[backend], run_root, identity,
                            case["spec"], repetition,
                        )
                        if result["logical_tasks"] != case["graph"]["logical_tasks"]:
                            raise RuntimeError("logical task count mismatch")
                        post_admission = wait_for_exact_inventory(
                            repository, pools[backend], workers, cores,
                            args.admission_recovery_timeout,
                        )
                        removals = worker_removals(result)
                        invalid |= removals > 0 or post_admission["admission_attempts"] > 1
                        record = {
                            "case_id": case["case_id"], "phase": case["phase"],
                            "backend": backend, "backend_order": list(order),
                            "repetition": repetition,
                            "execution_attempt": execution_attempt,
                            "execution_identity": identity,
                            "admission": compact_admission(admission, cores),
                            "post_admission": compact_admission(post_admission, cores),
                            "workers_removed_during_run": removals,
                            "result": result,
                        }
                        atomic_json(run_root / "campaign-result.json", record)
                        records.append(record)
                        paired[backend] = result
                    if paired["TV-native"]["results"] != paired["DV-native"]["results"]:
                        raise RuntimeError({
                            "case": case["case_id"], "repetition": repetition,
                            "error": "backend result mismatch",
                        })
                    if invalid:
                        campaign["invalidated_runs"].extend(
                            compact_record(item) for item in records)
                        atomic_json(output / "campaign-state.json", campaign)
                        print(json.dumps({
                            "case": case["case_id"], "repetition": repetition,
                            "execution_attempt": execution_attempt,
                            "status": "INVALIDATED_WORKER_CHURN",
                        }, sort_keys=True), flush=True)
                        continue
                    target = case["parameters"].get("target_cpu_ms")
                    if isinstance(target, (int, float)) and target >= 10:
                        tv_cpu = paired["TV-native"]["useful_cpu_seconds"]
                        dv_cpu = paired["DV-native"]["useful_cpu_seconds"]
                        relative = abs(tv_cpu - dv_cpu) / max(tv_cpu, dv_cpu, 1e-12)
                        if relative > manifest["statistics"]["cpu_equality_relative_tolerance"]:
                            raise RuntimeError({
                                "case": case["case_id"], "repetition": repetition,
                                "error": "useful CPU equality failed", "relative": relative,
                                "taskvine": tv_cpu, "datavine": dv_cpu,
                            })
                    campaign["runs"].extend(
                        compact_record(item) for item in records)
                    atomic_json(output / "campaign-state.json", campaign)
                    print(json.dumps({
                        "case": case["case_id"], "repetition": repetition,
                        "execution_attempt": execution_attempt,
                        "taskvine_seconds": paired["TV-native"]["total_seconds"],
                        "datavine_seconds": paired["DV-native"]["total_seconds"],
                        "status": "PAIR_PASS",
                    }, sort_keys=True), flush=True)
                    break
        campaign["summary"] = summarize(cases, campaign["runs"], manifest)
        unresolved = [row["case_id"] for row in campaign["summary"]
                      if row["bottleneck"].get("required") and
                      row["bottleneck"].get("status") != "CONFIRMED"]
        exact = all(
            item["result"]["physical_tasks"]["submissions"] == item["result"]["logical_tasks"] and
            item["result"]["physical_tasks"]["completions"] == item["result"]["logical_tasks"]
            for item in campaign["runs"]
        )
        final_repetitions = repetitions >= manifest["statistics"]["final_repetitions"]
        campaign["gates"] = {
            "exact_1024_core_admission": publication_scale and all(
                item["status"] == "PASS" for item in campaign["admission"].values()),
            "exact_task_counts": exact,
            "exact_results": paired_results_equal(campaign["runs"], "repetition"),
            "zero_worker_churn_in_accepted_runs": all(
                item["workers_removed_during_run"] == 0 and
                item["post_admission"]["admission_attempts"] == 1
                for item in campaign["warmups"] + campaign["runs"]
            ),
            "warmups_complete": len(campaign["warmups"]) == (
                len(cases) * 2 * warmup_repetitions
            ),
            "paired_repetitions": final_repetitions,
            "all_regressions_attributed": not unresolved,
            "no_unresolved_regression_cases": unresolved,
        }
        required = (
            "exact_1024_core_admission", "exact_task_counts", "exact_results",
            "zero_worker_churn_in_accepted_runs", "warmups_complete",
            "paired_repetitions", "all_regressions_attributed",
        )
        campaign["status"] = "PASS" if all(campaign["gates"][key] for key in required) else "PILOT_PASS"
    except BaseException as error:
        campaign["status"] = "FAIL"
        campaign["error"] = repr(error)
        raise
    finally:
        for backend in ("DV-native", "TV-native"):
            if backend in pools:
                pools[backend].close()
        state = output / "campaign-state.json"
        atomic_json(output / "summary.json", campaign)
        if campaign["status"] in ("PASS", "PILOT_PASS") and state.exists():
            state.unlink()
        elif campaign["status"] == "FAIL":
            atomic_json(state, campaign)
    print(json.dumps({
        "status": campaign["status"], "output": str(output),
        "runs": len(campaign["runs"]), "cases": len(cases),
    }, sort_keys=True))
    return 0 if campaign["status"] in ("PASS", "PILOT_PASS") else 1


if __name__ == "__main__":
    raise SystemExit(main())
