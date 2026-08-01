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


def split_value(seed, config, *, ordinal):
    assert config == HOT
    return seed * 3 + ordinal, seed * 5 + ordinal


def combine(left, right, *, config):
    assert config == HOT
    return left + right


def finalize(value, *, config, alias):
    assert config == HOT
    assert alias[0] is alias[1]["nested"]
    assert alias[0] == config
    return hashlib.sha256(f"{value}:{config['version']}".encode()).hexdigest()


def build_workflow(task_count):
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
            config=HOT,
        )
        if ordinal % 4 == 0:
            branch2 = workflow.add_task(
                combine, branch.output(), root.output(0), config=HOT
            )
            branch = workflow.add_task(
                combine, branch.output(), branch2.output(), config=HOT
            )
            leaf_values.append(24 * (ordinal % len(roots)))
        else:
            leaf_values.append(10 * (ordinal % len(roots)))
        leaves.append(branch)

    if not leaves:
        raise ValueError("task_count must be at least 4")
    final = workflow.add_task(
        finalize,
        leaves[-1].output(),
        config=HOT,
        alias=[HOT, {"nested": HOT}],
    )
    workflow.validate()
    expected = hashlib.sha256(
        f"{leaf_values[-1]}:{HOT['version']}".encode()
    ).hexdigest()
    return workflow, final.task_id, expected


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--factory-manager")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.tasks < 4:
        parser.error("--tasks must be at least 4")

    workflow, target, expected = build_workflow(args.tasks)
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
        worker_count=2 if args.factory_manager else 2,
        worker_cores=2,
        prefetch=True,
        persistence=False,
    )
    report = {
        "artifact_type": "datavine-grand-challenge-run",
        "status": "PASS",
        "tasks": len(workflow.tasks),
        "target_task_id": target,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "scheduler_report": snapshot["scheduler_report"],
    }
    print(json.dumps(report, sort_keys=True) if args.json else report)


if __name__ == "__main__":
    main()
