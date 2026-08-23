#!/usr/bin/env python3
"""Create an auditable compact view of a complete 1024-core campaign."""

import argparse
import hashlib
import json
from pathlib import Path


def canonical_sha256(value):
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def compact_run(item):
    result = item["result"]
    results = result.get("results")
    return {
        "case_id": item["case_id"],
        "phase": item.get("phase"),
        "repetition": item.get("repetition"),
        "warmup_repetition": item.get("warmup_repetition"),
        "backend": item["backend"],
        "backend_order": item.get("backend_order"),
        "execution_attempt": item.get("execution_attempt"),
        "execution_identity": item.get("execution_identity"),
        "admission": item.get("admission"),
        "post_admission": item.get("post_admission"),
        "workers_removed_during_run": item.get("workers_removed_during_run"),
        "status": result.get("status"),
        "workflow": result.get("workflow"),
        "logical_tasks": result.get("logical_tasks"),
        "physical_tasks": result.get("physical_tasks"),
        "total_seconds": result.get("total_seconds"),
        "build_submit_seconds": result.get("build_submit_seconds"),
        "post_submit_seconds": result.get("post_submit_seconds"),
        "terminal_wait_seconds": result.get("terminal_wait_seconds"),
        "fetch_seconds": result.get("fetch_seconds"),
        "useful_cpu_seconds": result.get("useful_cpu_seconds"),
        "tasks_per_second": result.get("tasks_per_second"),
        "peak": result.get("peak"),
        "resident_worker_gate": result.get("resident_worker_gate"),
        "taskvine_stats": result.get("taskvine_stats"),
        "runtime_stages": result.get("runtime_stages"),
        "client_cleanup": result.get("client_cleanup"),
        "result_count": (
            len(results) if hasattr(results, "__len__") else
            result.get("results_count")
        ),
        "results_sha256": (
            canonical_sha256(results) if results is not None else
            result.get("results_sha256")
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = json.loads(args.summary.read_text())
    compact = {
        "artifact_type": "datavine.taskvine.1024-core.compact-summary",
        "schema_version": 1,
        "source": {
            "path": str(args.summary.resolve()),
            "bytes": args.summary.stat().st_size,
            "sha256": file_sha256(args.summary),
        },
        "created_at": source.get("created_at"),
        "scope": source.get("scope"),
        "status": source.get("status"),
        "manifest": source.get("manifest"),
        "manifest_sha256": source.get("manifest_sha256"),
        "environment": source.get("environment"),
        "admission": source.get("admission"),
        "resume_events": source.get("resume_events", ()),
        "analysis_provenance": source.get("analysis_provenance"),
        "gates": source.get("gates"),
        "counts": {
            "cases": len(source.get("summary", ())),
            "warmups": len(source.get("warmups", ())),
            "runs": len(source.get("runs", ())),
            "invalidated_warmups": len(source.get("invalidated_warmups", ())),
            "invalidated_runs": len(source.get("invalidated_runs", ())),
        },
        "summary": source.get("summary", ()),
        "warmups": [compact_run(item) for item in source.get("warmups", ())],
        "runs": [compact_run(item) for item in source.get("runs", ())],
        "invalidated_warmups": [
            compact_run(item) for item in source.get("invalidated_warmups", ())
        ],
        "invalidated_runs": [
            compact_run(item) for item in source.get("invalidated_runs", ())
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(json.dumps(compact, indent=2, sort_keys=True) + "\n")
    temporary.replace(args.output)
    print(json.dumps({
        "status": compact["status"],
        "output": str(args.output),
        "bytes": args.output.stat().st_size,
        "runs": compact["counts"]["runs"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
