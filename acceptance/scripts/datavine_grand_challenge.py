#!/usr/bin/env python3
"""Configurable Grand Challenge workload for the independent DataVine runtime.

The default is intentionally small for development.  The accepted scale is
requested explicitly with ``--tasks 10000`` so smoke runs cannot be mistaken
for Ultimate Acceptance evidence.
"""

import argparse
import hashlib
import json
import pathlib
import sys
import time

from ndcctools.taskvine.datavine import Workflow

sys.path.insert(
    0,
    str(pathlib.Path(__file__).resolve().parents[2] / "taskvine" / "test"),
)
from datavine_phase4_demand_pull import run_case


HOT = {"kind": "hot-config", "version": 1, "salt": "datavine-grand"}
REPEATED_HOT_BINDINGS = (HOT,) * 8


def split_value(seed, config, *, ordinal):
    assert config == HOT
    return seed * 3 + ordinal, seed * 5 + ordinal


def combine(left, right, *repeated, config):
    assert config == HOT
    assert repeated == REPEATED_HOT_BINDINGS
    return left + right


def make_payload(size, seed, *, config):
    assert config == HOT
    return bytes((index + seed) % 251 for index in range(size))


def summarize_payloads(medium, large, *, config):
    assert config == HOT
    return (len(medium), len(large), hashlib.sha256(medium + large).hexdigest())


def finalize(value, payload_summary, *, config, alias):
    assert config == HOT
    assert alias[0] is alias[1]["nested"]
    assert alias[0] == config
    return hashlib.sha256(
        f"{value}:{payload_summary}:{config['version']}".encode()
    ).hexdigest()


def build_workflow(task_count, medium_bytes, large_bytes):
    workflow = Workflow()
    roots = []
    # Two-output roots exercise slot identity and repeated immutable edata.
    for ordinal in range(max(2, min(task_count // 4, 128))):
        root = workflow.add_task(
            split_value, ordinal, HOT, ordinal=ordinal, output_count=2
        )
        roots.append(root)

    # The bulk of the graph is a repeated fan-out with periodic diamonds.
    leaves = []
    leaf_values = []
    for ordinal in range(task_count - len(roots) - 2):
        root = roots[ordinal % len(roots)]
        branch = workflow.add_task(
            combine,
            root.output(0),
            root.output(1),
            *REPEATED_HOT_BINDINGS,
            config=HOT,
        )
        if ordinal % 4 == 0:
            branch2 = workflow.add_task(
                combine,
                branch.output(),
                root.output(0),
                *REPEATED_HOT_BINDINGS,
                config=HOT,
            )
            branch = workflow.add_task(
                combine,
                branch.output(),
                branch2.output(),
                *REPEATED_HOT_BINDINGS,
                config=HOT,
            )
            leaf_values.append(24 * (ordinal % len(roots)))
        else:
            leaf_values.append(10 * (ordinal % len(roots)))
        leaves.append(branch)

    if not leaves:
        raise ValueError("task_count must be at least 4")
    medium = workflow.add_task(
        make_payload, medium_bytes, 17, config=HOT
    )
    large = workflow.add_task(
        make_payload, large_bytes, 29, config=HOT
    )
    payload_summary = workflow.add_task(
        summarize_payloads, medium.output(), large.output(), config=HOT
    )
    final = workflow.add_task(
        finalize,
        leaves[-1].output(),
        payload_summary.output(),
        config=HOT,
        alias=[HOT, {"nested": HOT}],
    )
    workflow.validate()
    medium_payload = bytes((index + 17) % 251 for index in range(medium_bytes))
    large_payload = bytes((index + 29) % 251 for index in range(large_bytes))
    payload_summary_value = (
        len(medium_payload),
        len(large_payload),
        hashlib.sha256(medium_payload + large_payload).hexdigest(),
    )
    expected = hashlib.sha256(
        f"{leaf_values[-1]}:{payload_summary_value}:{HOT['version']}".encode()
    ).hexdigest()
    return workflow, final.task_id, expected


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--medium-bytes", type=int, default=64 * 1024)
    parser.add_argument("--large-bytes", type=int, default=0)
    parser.add_argument("--worker-loss", action="store_true")
    parser.add_argument(
        "--mode",
        choices=(
            "full",
            "failures",
            "no-prefetch",
            "peer-off",
            "pruning-off",
            "persistence-legacy",
            "legacy",
        ),
        default="full",
    )
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--worker-cores", type=int, default=2)
    parser.add_argument("--process-runner", action="store_true")
    parser.add_argument("--workflow-timeout", type=float, default=600)
    parser.add_argument("--factory-manager")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.tasks < 4:
        parser.error("--tasks must be at least 4")

    if args.medium_bytes < 0 or args.large_bytes < 0:
        parser.error("payload sizes must be non-negative")
    if args.workers < 1:
        parser.error("--workers must be positive")
    if args.worker_cores < 1:
        parser.error("--worker-cores must be positive")
    if args.mode == "legacy":
        print(json.dumps({"status": "UNAVAILABLE", "mode": "legacy"}))
        return 2
    failure_mode = args.worker_loss or args.mode == "failures"
    peer_transfers = args.mode != "peer-off"
    prefetch = args.mode != "no-prefetch"
    persistence = args.mode == "persistence-legacy"
    workflow, target, expected = build_workflow(
        args.tasks, args.medium_bytes, args.large_bytes
    )
    # The final digest is deterministic but depends on the generated graph.
    # The target is fetched from the scheduler snapshot rather than guessed
    # from task ordering, which keeps retries and output slots testable.
    started = time.monotonic()
    snapshot = run_case(
        f"grand-{args.tasks}",
        workflow,
        target,
        expected,
        factory_manager=args.factory_manager,
        worker_count=args.workers,
        worker_cores=args.worker_cores,
        peer_transfers=peer_transfers,
        prefetch=prefetch,
        persistence=persistence,
        persistence_attempts_by_task=({target: 1} if persistence else None),
        use_worker_library=not args.process_runner,
        scheduler_wait_timeout=1,
        workflow_timeout=args.workflow_timeout,
        inject_worker_loss_after=(1.0 if failure_mode else None),
        replacement_worker_delay=(1 if failure_mode else None),
    )
    report = {
        "artifact_type": "datavine-grand-challenge-run",
        "status": "PASS",
        "tasks": len(workflow.tasks),
        "task_to_data_bindings": sum(
            1 + len(task.args) + len(task.kwargs)
            for task in workflow.tasks
        ),
        "target_task_id": target,
        "failure_mode": "worker-loss" if failure_mode else "none",
        "mode": args.mode,
        "execution_boundary": (
            "process" if args.process_runner else "persistent-library"
        ),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "workflow_timeout_seconds": args.workflow_timeout,
        "scheduler_report": snapshot["scheduler_report"],
    }
    print(json.dumps(report, sort_keys=True) if args.json else report)


if __name__ == "__main__":
    main()
