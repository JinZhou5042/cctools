#!/usr/bin/env python3
"""Run or resume the five-pair data-intensive acceptance campaign."""

import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

from data_intensive_workload import Workload, assert_full_contract
from compare_data_intensive_runs import gates_pass


SCHEMA = "datavine.data-intensive-campaign/v1"


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def accepted_summary(path, contract_sha256, backend):
    try:
        value = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    if value.get("status") != "PASS" or not gates_pass(value, backend):
        return None
    if value.get("contract", {}).get("contract_sha256") != contract_sha256:
        return None
    return value


def current_commit(repository):
    return subprocess.run(
        ("git", "rev-parse", "HEAD"), cwd=repository, check=True,
        text=True, stdout=subprocess.PIPE,
    ).stdout.strip()


def clean_repository(repository):
    return not subprocess.run(
        ("git", "status", "--porcelain"), cwd=repository, check=True,
        text=True, stdout=subprocess.PIPE,
    ).stdout.strip()


def schedule(repetitions):
    result = []
    for repetition in range(1, repetitions + 1):
        order = ("taskvine", "datavine") if repetition % 2 else ("datavine", "taskvine")
        result.extend((repetition, backend) for backend in order)
    return result


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-root", required=True, type=Path)
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--workers", type=int, default=128)
    parser.add_argument("--cores", type=int, default=16)
    parser.add_argument("--timeout", type=float, default=24 * 60 * 60)
    parser.add_argument("--acceptance", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()
    if min(args.repetitions, args.workers, args.cores) < 1:
        parser.error("repetitions, workers, and cores must be positive")
    if args.acceptance and (args.repetitions, args.workers, args.cores) != (5, 128, 16):
        parser.error("acceptance requires five pairs at exactly 128 workers x 16 cores")
    return args


def main():
    args = parse_args()
    repository = Path(__file__).resolve().parents[2]
    workload = Workload()
    contract = assert_full_contract(workload)
    campaign_schedule = schedule(args.repetitions)
    if args.plan_only:
        print(json.dumps({
            "status": "PASS", "contract": contract,
            "schedule": campaign_schedule,
            "workers": args.workers, "cores": args.cores,
        }, indent=2, sort_keys=True))
        return 0
    if not clean_repository(repository):
        raise RuntimeError("campaign requires a clean repository worktree")
    commit = current_commit(repository)
    dataset_root = args.dataset_root.resolve()
    run_root = args.run_root.resolve()
    dataset = json.loads((dataset_root / "dataset-manifest.json").read_text())
    if (
        dataset.get("status") != "PASS"
        or not all(dataset.get("gates", {}).values())
        or not dataset.get("full_hash")
        or dataset.get("contract", {}).get("contract_sha256") != contract["contract_sha256"]
    ):
        raise RuntimeError("campaign requires the accepted full-hash dataset manifest")
    go_binary = os.environ.get("DATAVINE_GO_BINARY")
    if not go_binary or not os.access(go_binary, os.X_OK):
        raise RuntimeError("DATAVINE_GO_BINARY must name an executable production binary")
    run_root.mkdir(parents=True, exist_ok=True)
    state_path = run_root / "campaign-state.json"
    state = {
        "schema": SCHEMA,
        "status": "RUNNING",
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "repository": str(repository),
        "commit": commit,
        "hostname": platform.node(),
        "contract_sha256": contract["contract_sha256"],
        "dataset_manifest_sha256": dataset["manifest_sha256"],
        "workers": args.workers,
        "cores_per_worker": args.cores,
        "repetitions": args.repetitions,
        "schedule": [list(item) for item in campaign_schedule],
        "runs": [],
    }
    if state_path.exists():
        previous = json.loads(state_path.read_text())
        immutable = ("commit", "contract_sha256", "dataset_manifest_sha256", "workers", "cores_per_worker", "repetitions")
        if any(previous.get(key) != state.get(key) for key in immutable):
            raise RuntimeError("existing campaign state does not match this invocation")
        state["started_at"] = previous.get("started_at", state["started_at"])
        state["runs"] = previous.get("runs", [])
    atomic_json(state_path, state)

    runners = {
        "datavine": repository / "acceptance/scripts/benchmark_data_intensive_datavine.py",
        "taskvine": repository / "acceptance/scripts/benchmark_data_intensive_taskvine.py",
    }
    for repetition, backend in campaign_schedule:
        if current_commit(repository) != commit or not clean_repository(repository):
            raise RuntimeError("repository changed during campaign")
        output = run_root / f"{backend}-r{repetition}"
        summary_path = output / "summary.json"
        summary = accepted_summary(
            summary_path, contract["contract_sha256"], backend
        )
        if summary is not None:
            outcome = "resumed-pass"
        else:
            if output.exists() and any(output.iterdir()):
                raise RuntimeError(f"non-PASS run directory requires inspection: {output}")
            command = (
                sys.executable, str(runners[backend]), "--acceptance",
                "--dataset-root", str(dataset_root), "--output-dir", str(output),
                "--workers", str(args.workers), "--cores", str(args.cores),
                "--batch-type", "condor", "--timeout", str(args.timeout),
            )
            state["current"] = {
                "repetition": repetition, "backend": backend,
                "output": str(output),
                "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
            atomic_json(state_path, state)
            try:
                subprocess.run(command, cwd=repository, check=True)
            except BaseException as error:
                state["status"] = "FAIL"
                state["error"] = f"{type(error).__name__}: {error}"
                atomic_json(state_path, state)
                raise
            summary = accepted_summary(
                summary_path, contract["contract_sha256"], backend
            )
            if summary is None:
                raise RuntimeError(f"runner exited without an accepted summary: {output}")
            outcome = "executed-pass"
        state.pop("current", None)
        state.pop("error", None)
        state["status"] = "RUNNING"
        state["runs"] = [
            item for item in state["runs"]
            if (item["repetition"], item["backend"]) != (repetition, backend)
        ]
        state["runs"].append({
            "repetition": repetition, "backend": backend,
            "outcome": outcome, "summary": str(summary_path),
            "execution_seconds": summary["timing"]["execution_seconds"],
        })
        state["runs"].sort(key=lambda item: (item["repetition"], item["backend"]))
        atomic_json(state_path, state)

    comparison = run_root / "comparison.json"
    command = [
        sys.executable,
        str(repository / "acceptance/scripts/compare_data_intensive_runs.py"),
    ]
    for repetition in range(1, args.repetitions + 1):
        command.extend(("--datavine", str(run_root / f"datavine-r{repetition}")))
        command.extend(("--taskvine", str(run_root / f"taskvine-r{repetition}")))
    command.extend((
        "--minimum-repetitions", str(args.repetitions),
        "--output", str(comparison),
    ))
    completed = subprocess.run(command, cwd=repository)
    result = json.loads(comparison.read_text())
    comparison_pass = (
        completed.returncode == 0
        and result.get("status") == "PASS"
        and result.get("scope") == "production-128x16"
    )
    state["status"] = "PASS" if comparison_pass else "FAIL"
    state["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    state["comparison"] = str(comparison)
    atomic_json(state_path, state)
    print(json.dumps({
        "status": state["status"], "state": str(state_path),
        "comparison": str(comparison),
    }, sort_keys=True))
    return 0 if comparison_pass else 1


if __name__ == "__main__":
    sys.exit(main())
