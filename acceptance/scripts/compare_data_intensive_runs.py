#!/usr/bin/env python3
"""Fail-closed comparison of paired DataVine and TaskVine benchmark runs."""

import argparse
import json
from pathlib import Path
import statistics
import sys


REQUIRED_GATES = {
    "datavine": {
        "dataset_manifest", "exact_logical_counts", "exact_physical_counts",
        "worker_churn_recovered", "exact_worker_pool", "sampled_outputs",
        "all_outputs_worker_local", "durable_outputs_only_requested",
        "manager_output_payload_bypass", "exact_sharedfs_source_bytes",
        "data_path_is_runtime_bottleneck", "gc_pressure_accounted",
        "central_window_observed", "ready_parallelism", "active_parallelism",
    },
    "taskvine": {
        "dataset_manifest", "exact_logical_tasks",
        "exact_physical_submissions", "exact_physical_completions",
        "all_tasks_successful", "worker_churn_recovered", "exact_worker_pool",
        "sampled_outputs", "sharedfs_source_transport", "source_inputs_task_scoped",
        "central_window_observed", "ready_parallelism", "active_parallelism",
    },
}


def gates_pass(value, backend):
    gates = value.get("gates")
    return (
        isinstance(gates, dict)
        and REQUIRED_GATES[backend].issubset(gates)
        and all(gates[name] is True for name in REQUIRED_GATES[backend])
    )


def load(path):
    path = path / "summary.json" if path.is_dir() else path
    return path, json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--datavine", type=Path, action="append", required=True)
    parser.add_argument("--taskvine", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minimum-repetitions", type=int, default=5)
    parser.add_argument("--allow-pilot", action="store_true")
    args = parser.parse_args()
    if len(args.datavine) != len(args.taskvine):
        parser.error("DataVine and TaskVine run counts must match")
    pairs = []
    errors = []
    for index, (dv_arg, tv_arg) in enumerate(zip(args.datavine, args.taskvine)):
        dv_path, dv = load(dv_arg)
        tv_path, tv = load(tv_arg)
        contract = dv.get("contract", {})
        pair_errors = []
        if dv.get("status") != "PASS" or not gates_pass(dv, "datavine"):
            pair_errors.append("DataVine run is not fully PASS")
        if tv.get("status") != "PASS" or not gates_pass(tv, "taskvine"):
            pair_errors.append("TaskVine run is not fully PASS")
        if contract.get("contract_sha256") != tv.get("contract", {}).get("contract_sha256"):
            pair_errors.append("contract digest mismatch")
        hashes_equal = dv.get("sampled_results", {}).get("sha256") == tv.get("sampled_results", {}).get("sha256")
        if not hashes_equal:
            pair_errors.append("sampled result digest mismatch")
        dv_seconds = float(dv.get("timing", {}).get("execution_seconds", 0))
        tv_seconds = float(tv.get("timing", {}).get("execution_seconds", 0))
        if dv_seconds <= 0 or tv_seconds <= 0:
            pair_errors.append("invalid execution time")
        dv_manager_bytes = (
            int(dv.get("runtime_stages", {}).get("manager_bytes_sent", 0))
            + int(dv.get("runtime_stages", {}).get("manager_bytes_received", 0))
        )
        tv_manager_bytes = (
            int(tv.get("stats", {}).get("bytes_sent", 0))
            + int(tv.get("stats", {}).get("bytes_received", 0))
        )
        if dv_manager_bytes >= tv_manager_bytes:
            pair_errors.append("DataVine did not reduce manager data-plane bytes")
        pair = {
            "pair": index,
            "datavine": str(dv_path),
            "taskvine": str(tv_path),
            "status": "PASS" if not pair_errors else "FAIL",
            "errors": pair_errors,
            "contract_sha256": contract.get("contract_sha256"),
            "tasks": contract.get("counts", {}).get("tasks"),
            "workflow_files": contract.get("counts", {}).get("workflow_files"),
            "result_hashes_equal": hashes_equal,
            "datavine_execution_seconds": dv_seconds,
            "taskvine_execution_seconds": tv_seconds,
            "speedup_taskvine_over_datavine": tv_seconds / dv_seconds if dv_seconds else None,
            "datavine_manager_bytes": dv_manager_bytes,
            "taskvine_manager_bytes": tv_manager_bytes,
            "manager_byte_reduction_fraction": (
                1 - dv_manager_bytes / tv_manager_bytes if tv_manager_bytes else None
            ),
        }
        pairs.append(pair)
        errors.extend(f"pair {index}: {error}" for error in pair_errors)
    repetitions = len(pairs)
    full_contract = all(
        item["tasks"] == 1_048_576 and item["workflow_files"] == 10_485_760
        for item in pairs
    )
    repetition_gate = repetitions >= args.minimum_repetitions
    full_scale_scope = full_contract and repetition_gate and not errors
    production_scope = full_scale_scope and repetitions >= 5
    if not args.allow_pilot and not full_scale_scope:
        errors.append("full-scale claim requires full contract and requested minimum repetitions")
    speedups = [item["speedup_taskvine_over_datavine"] for item in pairs if item["status"] == "PASS"]
    reductions = [item["manager_byte_reduction_fraction"] for item in pairs if item["status"] == "PASS"]
    gates = {
        "all_pairs_pass": all(item["status"] == "PASS" for item in pairs),
        "full_contract": full_contract,
        "minimum_repetitions": repetition_gate,
        "median_speedup_above_one": bool(speedups) and statistics.median(speedups) > 1,
        "median_manager_byte_reduction_positive": bool(reductions) and statistics.median(reductions) > 0,
    }
    claim_gates = dict(gates)
    if args.allow_pilot:
        claim_gates.pop("full_contract")
        claim_gates.pop("minimum_repetitions")
    if production_scope:
        scope = "production-128x16"
    elif full_scale_scope and repetitions == 1:
        scope = "full-scale-single-pair"
    elif full_scale_scope:
        scope = "full-scale-multi-pair"
    else:
        scope = "pilot"
    result = {
        "schema": "datavine.data-intensive-comparison/v1",
        "status": "PASS" if not errors and all(claim_gates.values()) else "FAIL",
        "scope": scope,
        "repetitions": repetitions,
        "required_repetitions": args.minimum_repetitions,
        "gates": gates,
        "pairs": pairs,
        "median_speedup_taskvine_over_datavine": statistics.median(speedups) if speedups else None,
        "median_manager_byte_reduction_fraction": statistics.median(reductions) if reductions else None,
        "errors": errors,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".part")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    temporary.replace(args.output)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
