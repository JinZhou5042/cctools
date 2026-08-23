#!/usr/bin/env python3
"""Rebuild derived gates/summary from complete raw fixed-core campaign runs."""

import argparse
import datetime
import json
from pathlib import Path

import benchmark_1024_workflows as benchmark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--manifest", type=Path, default=Path(
        "acceptance/1024-core-workflows/campaign-v1.json"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    artifact = json.loads(args.artifact.read_text())
    manifest = json.loads(args.manifest.read_text())
    if artifact.get("artifact_type") != benchmark.ARTIFACT_TYPE:
        parser.error("unsupported artifact type")
    total_cores = int(artifact["environment"]["total_cores"])
    case_ids = {item["case_id"] for item in artifact.get("runs", ())}
    cases = [item for item in benchmark.build_cases(manifest, total_cores)
             if item["case_id"] in case_ids]
    if {item["case_id"] for item in cases} != case_ids:
        parser.error("raw runs contain unknown case ids")
    expected_pairs = {
        (case["case_id"], repetition, backend)
        for case in cases
        for repetition in {item["repetition"] for item in artifact["runs"]}
        for backend in ("TV-native", "DV-native")
    }
    actual_pairs = {
        (item["case_id"], item["repetition"], item["backend"])
        for item in artifact["runs"]
    }
    if len(actual_pairs) != len(artifact["runs"]):
        parser.error("duplicate raw run identity")
    missing = sorted(expected_pairs - actual_pairs)
    if missing:
        parser.error(f"incomplete raw campaign: {missing[:5]}")
    artifact["summary"] = benchmark.summarize(cases, artifact["runs"], manifest)
    repository = Path(__file__).resolve().parents[2]
    artifact["analysis_provenance"] = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "measurement_records_changed": False,
        "source_sha256": {
            "acceptance/scripts/benchmark_1024_workflows.py": benchmark.sha256_file(
                repository / "acceptance/scripts/benchmark_1024_workflows.py"
            ),
            "acceptance/scripts/finalize_1024_workflow_campaign.py": benchmark.sha256_file(
                Path(__file__).resolve()
            ),
        },
    }
    unresolved = [row["case_id"] for row in artifact["summary"]
                  if row["bottleneck"].get("required") and
                  row["bottleneck"].get("status") != "CONFIRMED"]
    exact = all(
        item["result"]["physical_tasks"]["submissions"] == item["result"]["logical_tasks"] and
        item["result"]["physical_tasks"]["completions"] == item["result"]["logical_tasks"]
        for item in artifact["runs"]
    )
    repetitions = len({item["repetition"] for item in artifact["runs"]})
    warmup_repetitions = int(manifest["statistics"].get("warmup_repetitions", 1))
    admission = artifact.get("admission", {})
    publication_scale = artifact.get("scope") == "publication-1024-core"
    artifact["gates"] = {
        "exact_1024_core_admission": publication_scale and
            set(admission) == {"TV-native", "DV-native"} and
            all(item.get("status") == "PASS" for item in admission.values()),
        "exact_task_counts": exact,
        "exact_results": benchmark.paired_results_equal(
            artifact["runs"], "repetition"
        ),
        "zero_worker_churn_in_accepted_runs": all(
            item.get("workers_removed_during_run") == 0 and
            item.get("post_admission", {}).get("admission_attempts") == 1
            for item in artifact.get("warmups", ()) + artifact["runs"]
        ),
        "warmups_complete": len(artifact.get("warmups", ())) == (
            len(cases) * 2 * warmup_repetitions
        ),
        "paired_repetitions": repetitions >= manifest["statistics"]["final_repetitions"],
        "all_regressions_attributed": not unresolved,
        "no_unresolved_regression_cases": unresolved,
    }
    required = (
        "exact_1024_core_admission", "exact_task_counts", "exact_results",
        "zero_worker_churn_in_accepted_runs", "warmups_complete",
        "paired_repetitions", "all_regressions_attributed",
    )
    artifact["status"] = "PASS" if all(artifact["gates"][key] for key in required) else "PILOT_PASS"
    artifact.pop("error", None)
    output = args.output or args.artifact
    benchmark.atomic_json(output, artifact)
    print(json.dumps({
        "status": artifact["status"], "runs": len(artifact["runs"]),
        "cases": len(cases), "unresolved": unresolved,
        "output": str(output),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
