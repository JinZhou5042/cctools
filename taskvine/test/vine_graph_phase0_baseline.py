#!/usr/bin/env python3

import argparse
import contextlib
import dataclasses
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time

import cloudpickle

import ndcctools.taskvine.vine_graph.vine_graph as vine_graph_mod
from ndcctools.taskvine.vine_graph import VineGraph, Workflow


TEST_DIR = Path(__file__).resolve().parent
SHARED_PAYLOAD = b"datavine-phase0-shared-input\n" * 32768
RECOVERY_PAYLOAD_SIZE = 2 * 1024 * 1024
STATS_FIELDS = (
    "tasks_submitted",
    "tasks_done",
    "tasks_failed",
    "tasks_recovery",
    "workers_removed",
    "workers_released",
    "bytes_sent",
    "bytes_received",
    "time_send",
    "time_receive",
)


def add(left, right):
    return left + right


def multiply(value, factor):
    return value * factor


def summarize_normal(left, right):
    return {"left": left, "right": right, "total": left + right}


def shared_digest(left, right, ordinal):
    assert left is right
    return {
        "ordinal": ordinal,
        "size": len(left),
        "sha256": hashlib.sha256(left).hexdigest(),
    }


def summarize_shared(*records):
    return {
        "ordinals": [record["ordinal"] for record in records],
        "sizes": [record["size"] for record in records],
        "digests": [record["sha256"] for record in records],
    }


def produce_recovery_payload(size):
    return bytes((index % 251 for index in range(size)))


def digest_recovery_payload(payload):
    return hashlib.sha256(payload).hexdigest()


def decorate_digest(digest, marker):
    return f"{marker}:{digest}"


def make_normal_workflow():
    workflow = Workflow()
    root = workflow.add_task(add, 7, 5)
    left = workflow.add_task(multiply, root.output(), 3)
    right = workflow.add_task(add, root.output(), 11)
    target = workflow.add_task(summarize_normal, left.output(), right.output())
    oracle = {"left": 36, "right": 23, "total": 59}
    return workflow, target, oracle


def make_shared_workflow():
    workflow = Workflow()
    records = [
        workflow.add_task(shared_digest, SHARED_PAYLOAD, SHARED_PAYLOAD, ordinal)
        for ordinal in range(4)
    ]
    target = workflow.add_task(
        summarize_shared, *(record.output() for record in records)
    )
    digest = hashlib.sha256(SHARED_PAYLOAD).hexdigest()
    oracle = {
        "ordinals": [0, 1, 2, 3],
        "sizes": [len(SHARED_PAYLOAD)] * 4,
        "digests": [digest] * 4,
    }
    return workflow, target, oracle


def make_recovery_workflow():
    workflow = Workflow()
    payload = workflow.add_task(produce_recovery_payload, RECOVERY_PAYLOAD_SIZE)
    digest = workflow.add_task(digest_recovery_payload, payload.output())
    marked = workflow.add_task(decorate_digest, digest.output(), "recovered")
    target = workflow.add_task(decorate_digest, marked.output(), "complete")
    expected_digest = hashlib.sha256(
        produce_recovery_payload(RECOVERY_PAYLOAD_SIZE)
    ).hexdigest()
    oracle = f"complete:recovered:{expected_digest}"
    return workflow, target, oracle


def stats_snapshot(manager):
    manager._refresh_stats()
    return {field: int(getattr(manager.stats, field)) for field in STATS_FIELDS}


def stats_delta(before, after):
    return {field: after[field] - before[field] for field in STATS_FIELDS}


def tree_usage(path):
    root = Path(path)
    files = [entry for entry in root.rglob("*") if entry.is_file()] if root.exists() else []
    return {
        "files": len(files),
        "bytes": sum(entry.stat().st_size for entry in files),
    }


def parse_run_metrics(stdout):
    patterns = {
        "makespan_seconds": r"^=== Makespan: ([0-9.]+) seconds$",
        "tasks_completed": r"^=== Total tasks completed: ([0-9]+)$",
        "throughput_tasks_per_second": r"^=== Throughput: ([0-9.]+) tasks/s$",
    }
    parsed = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, stdout, re.MULTILINE)
        if not match:
            raise AssertionError(f"missing run metric {key}")
        parsed[key] = float(match.group(1)) if key != "tasks_completed" else int(match.group(1))
    return parsed


def run_case(
    manager,
    name,
    builder,
    work_root,
    failure_step,
    indexed_data_identity,
    shadow_data_graph,
    data_controller,
    worker_data_agent,
):
    case_root = work_root / name
    output_dir = case_root / "outputs"
    checkpoint_dir = case_root / "checkpoints"
    for path in (output_dir, checkpoint_dir):
        path.mkdir(parents=True, exist_ok=True)

    workflow, target, oracle = builder()
    serialized_workflow = cloudpickle.dumps(workflow)
    before = stats_snapshot(manager)
    params = {
        "checkpoint-dir": str(checkpoint_dir),
        "checkpoint-fraction": 0,
        "extra-task-output-size-mb": [0.0, 0.0],
        "extra-task-sleep-time": [0.0, 0.0],
        "failure-injection-step-percent": failure_step if name == "worker-loss" else -1,
        "libcores": 1,
        "output-dir": str(output_dir),
        "prune-depth": 1,
        "task-group": 0,
        "task-priority-mode": "fifo",
        "temp-replica-count": 1,
        "wait-for-workers": 1,
        "watch-library-logfiles": 1,
        "indexed-data-identity": indexed_data_identity,
        "shadow-data-graph": shadow_data_graph,
        "data-controller": data_controller,
        "worker-data-agent": worker_data_agent,
    }

    capture = io.StringIO()
    started = time.monotonic()
    with contextlib.redirect_stdout(capture):
        result = manager.run(
            workflow,
            targets=[target],
            params=params,
            hoisting_modules=[sys.modules[__name__]],
            env_files={str(Path(__file__).resolve()): "vine_graph_phase0_baseline.py"},
        )
    wall_seconds = time.monotonic() - started
    stdout = capture.getvalue()
    print(stdout, end="")

    actual = result[target]
    assert actual == oracle, f"{name}: expected {oracle!r}, got {actual!r}"
    after = stats_snapshot(manager)
    delta = stats_delta(before, after)

    if name != "worker-loss":
        assert delta["tasks_recovery"] == 0, f"{name}: unexpected recovery task"
    else:
        assert delta["workers_released"] >= 1, "worker-loss: failure hook did not release a worker"
        assert delta["tasks_recovery"] >= 1, "worker-loss: lost temp output did not trigger recovery"

    identity_summary = None
    shadow_report = None
    controller_report = None
    worker_data_report = None
    if data_controller:
        controller = workflow.data_controller
        assert controller is not None
        assert workflow.indexed_data_identity is None
        assert workflow.shadow_data_graph is None
        identity_summary = controller.summary()
        shadow_report = controller.comparison_report()
        controller_report = controller.audit_report()
        assert identity_summary["tasks"] == len(workflow.task_dict)
        assert identity_summary["idata"] == len(workflow.task_dict)
        assert shadow_report["mismatches"] == []
        assert controller_report["mismatches"] == []
        assert controller_report["audited_tasks"] == len(workflow.task_dict)
        if worker_data_agent:
            worker_data_report = (
                controller.worker_preparation_audit_report()
            )
            assert worker_data_report["mismatches"] == []
            assert (
                worker_data_report["audited_tasks"]
                == len(workflow.task_dict)
            )
        stable_snapshot = (
            dict(controller.edata),
            dict(controller.idata),
            dict(controller.tasks),
        )
        workflow.finalize(
            indexed_data_identity=True,
            shadow_data_graph=True,
            data_controller=True,
            worker_data_agent=bool(worker_data_agent),
        )
        rebuilt = workflow.data_controller
        assert stable_snapshot == (
            dict(rebuilt.edata),
            dict(rebuilt.idata),
            dict(rebuilt.tasks),
        )
        assert rebuilt.comparison_report() == shadow_report
    elif indexed_data_identity:
        identity = workflow.indexed_data_identity
        assert identity is not None
        identity_summary = identity.summary()
        assert identity_summary["tasks"] == len(workflow.task_dict)
        assert identity_summary["idata"] == len(workflow.task_dict)
        if name == "shared-input":
            shared_id = identity.edata.lookup(SHARED_PAYLOAD)
            assert shared_id is not None
            shared_binding_ids = []
            for task_id in range(1, 5):
                binding = identity.task_bindings[task_id]
                shared_binding_ids.extend(
                    input_binding.data_id
                    for input_binding in binding.inputs
                    if input_binding.slot_kind == "positional"
                    and input_binding.slot in (0, 1)
                )
            assert len(shared_binding_ids) == 8
            assert set(shared_binding_ids) == {shared_id}

        # Logical output IDs and task bindings must not change when the same
        # workflow is finalized again after execution or recovery.
        stable_snapshot = (
            dict(identity.task_ids),
            dict(identity.idata),
            dict(identity.task_bindings),
        )
        if shadow_data_graph:
            shadow_report = workflow.shadow_data_graph.comparison_report()
            assert shadow_report["mismatches"] == []
            assert shadow_report["counts"]["tasks"] == len(workflow.task_dict)
            assert (
                shadow_report["counts"]["workflow_dependency_edges"]
                == shadow_report["counts"]["shadow_dependency_edges"]
            )
        else:
            assert workflow.shadow_data_graph is None

        workflow.finalize(
            indexed_data_identity=True,
            shadow_data_graph=bool(shadow_data_graph),
        )
        rebuilt = workflow.indexed_data_identity
        assert stable_snapshot == (
            rebuilt.task_ids,
            rebuilt.idata,
            rebuilt.task_bindings,
        )
        if shadow_data_graph:
            assert workflow.shadow_data_graph.comparison_report() == shadow_report
    else:
        assert workflow.indexed_data_identity is None
        assert workflow.shadow_data_graph is None
        assert workflow.data_controller is None

    case_report = {
        "acceptance": "PASS",
        "oracle": oracle,
        "result": actual,
        "workflow": {
            "tasks": len(workflow.task_dict),
            "callables": len(workflow.callables),
            "cloudpickle_bytes": len(serialized_workflow),
            "cloudpickle_sha256": hashlib.sha256(serialized_workflow).hexdigest(),
        },
        "performance": {
            "wall_seconds": round(wall_seconds, 6),
            **parse_run_metrics(stdout),
        },
        "manager_stats_delta": delta,
        "storage_after_cleanup": {
            "outputs": tree_usage(output_dir),
            "checkpoints": tree_usage(checkpoint_dir),
        },
    }
    if identity_summary is not None:
        case_report["indexed_data_identity"] = identity_summary
    if shadow_report is not None:
        case_report["shadow_data_graph"] = shadow_report
    if controller_report is not None:
        case_report["data_controller"] = controller_report
    if worker_data_report is not None:
        case_report["worker_data_agent"] = worker_data_report
    return case_report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--port-file")
    parser.add_argument("--manager-name")
    parser.add_argument("--work-root", required=True)
    parser.add_argument("--result-file", required=True)
    parser.add_argument("--timeout", type=float, default=300.0)
    # 60% crosses once in the four-node recovery chain (at 75%) and advances
    # the next threshold beyond 100%, so exactly one worker is released.
    parser.add_argument("--failure-step", type=float, default=60.0)
    parser.add_argument(
        "--indexed-data-identity", type=int, choices=(0, 1), default=0
    )
    parser.add_argument(
        "--shadow-data-graph", type=int, choices=(0, 1), default=0
    )
    parser.add_argument(
        "--data-controller", type=int, choices=(0, 1), default=0
    )
    parser.add_argument(
        "--worker-data-agent", type=int, choices=(0, 1), default=0
    )
    args = parser.parse_args()

    work_root = Path(args.work_root).resolve()
    if work_root.exists():
        shutil.rmtree(work_root)
    work_root.mkdir(parents=True)

    def on_alarm(signum, frame):
        raise TimeoutError(f"Phase 0 baseline exceeded {args.timeout} seconds")

    import signal

    old_handler = signal.signal(signal.SIGALRM, on_alarm)
    signal.setitimer(signal.ITIMER_REAL, args.timeout)

    def context_loader(graph_pickle):
        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.insert(0, cwd)
        return {"graph": cloudpickle.loads(graph_pickle)}

    vine_graph_mod.context_loader_func = context_loader
    cloudpickle.register_pickle_by_value(sys.modules[__name__])

    report = {
        "schema_version": 1,
        "git_commit": os.environ.get("DATAVINE_GIT_COMMIT", "unknown"),
        "indexed_data_identity": args.indexed_data_identity,
        "shadow_data_graph": args.shadow_data_graph,
        "data_controller": args.data_controller,
        "worker_data_agent": args.worker_data_agent,
        "cases": {},
    }
    try:
        with VineGraph(
            port=args.port,
            name=args.manager_name,
            run_info_path=str(work_root / "run-info"),
            run_info_template="manager",
        ) as manager:
            if args.port_file:
                Path(args.port_file).write_text(str(manager.port))
            report["manager"] = {
                "name": manager.name,
                "port": manager.port,
                "runtime_directory": manager.runtime_directory,
            }
            report["cases"]["normal"] = run_case(
                manager,
                "normal",
                make_normal_workflow,
                work_root,
                args.failure_step,
                args.indexed_data_identity,
                args.shadow_data_graph,
                args.data_controller,
                args.worker_data_agent,
            )
            report["cases"]["shared-input"] = run_case(
                manager,
                "shared-input",
                make_shared_workflow,
                work_root,
                args.failure_step,
                args.indexed_data_identity,
                args.shadow_data_graph,
                args.data_controller,
                args.worker_data_agent,
            )
            report["cases"]["worker-loss"] = run_case(
                manager,
                "worker-loss",
                make_recovery_workflow,
                work_root,
                args.failure_step,
                args.indexed_data_identity,
                args.shadow_data_graph,
                args.data_controller,
                args.worker_data_agent,
            )
            report["runtime_storage_before_manager_exit"] = tree_usage(
                manager.runtime_directory
            )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)

    report["acceptance"] = "PASS"
    Path(args.result_file).write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    print(f"Phase 0 baseline PASS: {args.result_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
