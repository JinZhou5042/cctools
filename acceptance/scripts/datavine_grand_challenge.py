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


def advance_chain(value, step, *, config, payload_bytes=0):
    assert config == HOT
    if isinstance(value, bytes):
        value = int.from_bytes(value[:8], "big")
    value = (value * 33 + step) % 1000000007
    if not payload_bytes:
        return value
    seed = value.to_bytes(8, "big")
    return (seed * ((payload_bytes + 7) // 8))[:payload_bytes]


def finalize(value, chain_value, payload_summary, *, config, alias):
    assert config == HOT
    assert alias[0] is alias[1]["nested"]
    assert alias[0] == config
    chain_token = (
        hashlib.sha256(chain_value).hexdigest()
        if isinstance(chain_value, bytes)
        else chain_value
    )
    return hashlib.sha256(
        f"{value}:{chain_token}:{payload_summary}:{config['version']}".encode()
    ).hexdigest()


def build_workflow(task_count, medium_bytes, large_bytes, lineage_bytes=0):
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
    # This ordered spine provides a deterministic long lineage and safe points
    # for repeated process-loss injection. Each selected task can complete only
    # after every earlier selected task has recovered.
    chain_tasks = []
    chain_value = roots[0].output(0)
    chain_expected = 0
    chain_length = max(16, min(128, task_count // 20))
    for step in range(chain_length):
        chain_task = workflow.add_task(
            advance_chain,
            chain_value,
            step,
            config=HOT,
            payload_bytes=lineage_bytes,
        )
        chain_tasks.append(chain_task)
        chain_value = chain_task.output()
        chain_expected = advance_chain(
            chain_expected,
            step,
            config=HOT,
            payload_bytes=lineage_bytes,
        )

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
        chain_value,
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
    chain_expected_token = (
        hashlib.sha256(chain_expected).hexdigest()
        if isinstance(chain_expected, bytes)
        else chain_expected
    )
    expected = hashlib.sha256(
        f"{leaf_values[-1]}:{chain_expected_token}:{payload_summary_value}:"
        f"{HOT['version']}".encode()
    ).hexdigest()
    return workflow, final.task_id, expected, [
        task.task_id for task in chain_tasks
    ]


def select_loss_schedule(chain_task_ids, count):
    if count < 1:
        raise ValueError("worker loss count must be positive")
    if count > len(chain_task_ids):
        raise ValueError(
            "worker loss count exceeds deterministic chain length"
        )
    return tuple(
        chain_task_ids[((index + 1) * len(chain_task_ids)) // (count + 1)]
        for index in range(count)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--medium-bytes", type=int, default=64 * 1024)
    parser.add_argument("--large-bytes", type=int, default=0)
    parser.add_argument("--lineage-bytes", type=int, default=0)
    parser.add_argument(
        "--controller-inline-idata-bytes", type=int, default=8 * 1024 * 1024
    )
    parser.add_argument("--worker-loss", action="store_true")
    parser.add_argument("--worker-loss-count", type=int, default=2)
    parser.add_argument("--frontier-recovery", action="store_true")
    parser.add_argument("--storage-frontier-stride", type=int, default=0)
    parser.add_argument("--storage-budget-bytes", type=int, default=0)
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
    parser.add_argument(
        "--worker-disk-cache-bytes", type=int, default=64 * 1024 * 1024
    )
    parser.add_argument("--worker-disk-cache-items", type=int, default=2048)
    parser.add_argument(
        "--worker-disk-cache-admission-bytes",
        type=int,
        default=64 * 1024 * 1024,
    )
    parser.add_argument(
        "--worker-disk-cache-admission-items", type=int, default=512
    )
    parser.add_argument("--process-runner", action="store_true")
    parser.add_argument("--pruning-grace-seconds", type=float, default=30)
    parser.add_argument(
        "--hard-delete-pruned-sharedfs", action="store_true"
    )
    parser.add_argument("--workflow-timeout", type=float, default=600)
    parser.add_argument("--factory-manager")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.tasks < 4:
        parser.error("--tasks must be at least 4")

    if min(
        args.medium_bytes,
        args.large_bytes,
        args.lineage_bytes,
        args.controller_inline_idata_bytes,
    ) < 0:
        parser.error("payload sizes must be non-negative")
    if args.workers < 1:
        parser.error("--workers must be positive")
    if args.worker_cores < 1:
        parser.error("--worker-cores must be positive")
    if args.worker_loss_count < 1:
        parser.error("--worker-loss-count must be positive")
    if args.storage_frontier_stride < 0:
        parser.error("--storage-frontier-stride must be non-negative")
    if args.storage_budget_bytes < 0:
        parser.error("--storage-budget-bytes must be non-negative")
    if args.storage_frontier_stride and not args.frontier_recovery:
        parser.error(
            "--storage-frontier-stride requires --frontier-recovery"
        )
    if args.pruning_grace_seconds < 0:
        parser.error("--pruning-grace-seconds must be non-negative")
    if min(
        args.worker_disk_cache_bytes,
        args.worker_disk_cache_items,
        args.worker_disk_cache_admission_bytes,
        args.worker_disk_cache_admission_items,
    ) < 0:
        parser.error("worker cache bounds must be non-negative")
    minimum_output_admission = max(
        args.medium_bytes, args.large_bytes, args.lineage_bytes
    ) + 4096
    if args.worker_disk_cache_admission_bytes < minimum_output_admission:
        parser.error(
            "worker cache byte admission must fit the largest generated "
            f"output ({minimum_output_admission} bytes including margin)"
        )
    if args.mode == "legacy":
        print(json.dumps({"status": "UNAVAILABLE", "mode": "legacy"}))
        return 2
    failure_mode = (
        args.worker_loss
        or args.mode == "failures"
        or args.frontier_recovery
    )
    if args.frontier_recovery and not failure_mode:
        parser.error("--frontier-recovery requires a failure mode")
    peer_transfers = args.mode != "peer-off"
    prefetch = args.mode != "no-prefetch"
    persistence = (
        args.mode == "persistence-legacy" or args.frontier_recovery
    )
    workflow, target, expected, chain_task_ids = build_workflow(
        args.tasks,
        args.medium_bytes,
        args.large_bytes,
        args.lineage_bytes,
    )
    persistence_attempts = None
    prune_after_persistence = None
    worker_loss_data = None
    expected_rollback_depths = []
    durability_frontiers = []
    if args.frontier_recovery:
        chain_length = len(chain_task_ids)
        if args.storage_frontier_stride:
            stride = args.storage_frontier_stride
            if stride < 4:
                parser.error(
                    "--storage-frontier-stride must be at least four "
                    "to contain the three deterministic recovery depths"
                )
            frontier_indexes = list(
                range(stride - 1, chain_length, stride)
            )
            if not frontier_indexes or frontier_indexes[-1] != chain_length - 1:
                frontier_indexes.append(chain_length - 1)
            eligible_frontiers = frontier_indexes[:-1]
            if len(eligible_frontiers) < 3:
                parser.error(
                    "dense storage schedule requires at least four frontiers"
                )
            selected_frontier_indexes = tuple(
                eligible_frontiers[
                    ((position + 1) * len(eligible_frontiers)) // 4
                ]
                for position in range(3)
            )
            rollback_depths = (3, 2, 1)
        else:
            frontier_indexes = (
                chain_length // 4,
                (chain_length * 5) // 8,
                (chain_length * 13) // 16,
            )
            selected_frontier_indexes = frontier_indexes
            rollback_depths = (
                max(1, (chain_length * 3) // 16),
                max(1, chain_length // 8),
                max(1, chain_length // 16),
            )
        loss_indexes = tuple(
            frontier + depth
            for frontier, depth in zip(
                selected_frontier_indexes, rollback_depths
            )
        )
        if loss_indexes[-1] >= chain_length:
            parser.error("deterministic chain is too short for frontiers")
        durability_frontiers = [
            chain_task_ids[index] for index in frontier_indexes
        ]
        loss_schedule = tuple(
            chain_task_ids[index] for index in loss_indexes
        )
        persistence_attempts = {
            task_id: 1 for task_id in durability_frontiers
        }
        worker_loss_data = {
            chain_task_ids[loss_index]: tuple(
                chain_task_ids[frontier_index + 1:loss_index + 1]
            )
            for frontier_index, loss_index in zip(
                selected_frontier_indexes, loss_indexes
            )
        }
        prune_after_persistence = {}
        previous_frontier = 0
        for position, frontier_index in enumerate(frontier_indexes):
            prune_after_persistence[
                chain_task_ids[frontier_index]
            ] = tuple(
                chain_task_ids[
                    previous_frontier:frontier_index
                ]
            )
            previous_frontier = frontier_index
        expected_rollback_depths = list(rollback_depths)
    else:
        loss_schedule = (
            select_loss_schedule(
                chain_task_ids, args.worker_loss_count
            )
            if failure_mode
            else ()
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
        worker_disk_cache_bytes=args.worker_disk_cache_bytes,
        worker_disk_cache_items=args.worker_disk_cache_items,
        worker_disk_cache_admission_bytes=(
            args.worker_disk_cache_admission_bytes
        ),
        worker_disk_cache_admission_items=(
            args.worker_disk_cache_admission_items
        ),
        persistence=persistence,
        persistence_attempts_by_task=(
            persistence_attempts
            if args.frontier_recovery
            else ({target: 1} if persistence else None)
        ),
        prune_after_persistence_by_task=(
            None if args.mode == "pruning-off"
            else prune_after_persistence
        ),
        max_inline_idata_bytes=args.controller_inline_idata_bytes,
        use_worker_library=not args.process_runner,
        scheduler_wait_timeout=1,
        workflow_timeout=args.workflow_timeout,
        inject_worker_loss_schedule=loss_schedule,
        inject_worker_loss_data_by_task=worker_loss_data,
        worker_loss_process_shutdown=failure_mode,
        frontier_pruning_grace_seconds=args.pruning_grace_seconds,
        hard_delete_pruned_sharedfs=(
            args.hard_delete_pruned_sharedfs
            and args.mode != "pruning-off"
        ),
    )
    scheduler_report = snapshot["scheduler_report"]
    retained_sharedfs_bytes = sum(
        snapshot["sharedfs_storage"][key]
        for key in ("durable_bytes", "quarantine_bytes")
    )
    storage_budget_exceeded = bool(
        args.storage_budget_bytes
        and retained_sharedfs_bytes > args.storage_budget_bytes
    )
    if args.storage_budget_bytes:
        if args.mode == "pruning-off" and not storage_budget_exceeded:
            raise AssertionError(
                "pruning-off mode did not exceed the storage budget"
            )
        if args.mode != "pruning-off" and storage_budget_exceeded:
            raise AssertionError(
                "pruning-enabled mode exceeded the storage budget"
            )
    durability_frontier_data_ids = sorted(
        scheduler_report["logical_output_slots"][str(task_id)][0]
        for task_id in durability_frontiers
    )
    if failure_mode:
        if scheduler_report["worker_loss_injections"] != len(loss_schedule):
            raise AssertionError(
                "not every scheduled worker loss was injected"
            )
        if not scheduler_report["worker_loss_process_shutdown"]:
            raise AssertionError("worker loss did not shut down a process")
        if scheduler_report["recovery_reexecutions"] < len(loss_schedule):
            raise AssertionError(
                "repeated worker loss did not trigger ordinary recomputation"
            )
        if scheduler_report["legacy_recovery_tasks"]:
            raise AssertionError("legacy recovery tasks were created")
        if scheduler_report["peer_transfer_faults"][
            "peer_release_pending"
        ]:
            raise AssertionError("peer release obligations remain pending")
        if args.frontier_recovery:
            rollback_depths = [
                wave["rollback_depth"]
                for wave in scheduler_report["recovery_waves"]
            ]
            if rollback_depths != expected_rollback_depths:
                raise AssertionError(
                    "durability frontier recovery depths do not match: "
                    f"{rollback_depths} != {expected_rollback_depths}"
                )
            if scheduler_report[
                "persistence_required_data_ids"
            ] != durability_frontier_data_ids:
                raise AssertionError("durability frontiers were not persisted")
            expected_outstanding = (
                durability_frontier_data_ids
                if args.mode == "pruning-off"
                else durability_frontier_data_ids[-1:]
            )
            if scheduler_report[
                "persistence_outstanding_data_ids"
            ] != expected_outstanding:
                raise AssertionError(
                    "durability obligations do not match pruning mode"
                )
            runtime_pruned = scheduler_report["runtime_pruned_data_ids"]
            if args.mode == "pruning-off" and runtime_pruned:
                raise AssertionError("pruning-off mode pruned logical data")
            if args.mode != "pruning-off" and not runtime_pruned:
                raise AssertionError("frontier recovery did not prune data")
            if not snapshot["durable_hashes_valid"]:
                raise AssertionError("durable frontier hash validation failed")
            if snapshot["persistence_temporary_files"]:
                raise AssertionError("persistence temporary files leaked")
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
        "worker_loss_schedule": list(loss_schedule),
        "worker_loss_count_requested": len(loss_schedule),
        "lineage_chain_tasks": len(chain_task_ids),
        "lineage_payload_bytes": args.lineage_bytes,
        "controller_inline_idata_bytes": args.controller_inline_idata_bytes,
        "frontier_recovery": args.frontier_recovery,
        "durability_frontiers": durability_frontiers,
        "durability_frontier_data_ids": durability_frontier_data_ids,
        "expected_rollback_depths": expected_rollback_depths,
        "durable_hashes_valid": snapshot["durable_hashes_valid"],
        "persistence_temporary_files": snapshot[
            "persistence_temporary_files"
        ],
        "sharedfs_storage": snapshot["sharedfs_storage"],
        "storage_budget": {
            "limit_bytes": args.storage_budget_bytes,
            "retained_bytes": retained_sharedfs_bytes,
            "exceeded": storage_budget_exceeded,
        },
        "storage_frontier_stride": args.storage_frontier_stride,
        "pruning_grace_seconds": args.pruning_grace_seconds,
        "hard_delete_pruned_sharedfs": (
            args.hard_delete_pruned_sharedfs
            and args.mode != "pruning-off"
        ),
        "worker_disk_cache": {
            "capacity_bytes": args.worker_disk_cache_bytes,
            "capacity_items": args.worker_disk_cache_items,
            "admission_bytes": args.worker_disk_cache_admission_bytes,
            "admission_items": args.worker_disk_cache_admission_items,
        },
        "mode": args.mode,
        "execution_boundary": (
            "process" if args.process_runner else "persistent-library"
        ),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "workflow_timeout_seconds": args.workflow_timeout,
        "scheduler_report": scheduler_report,
    }
    print(json.dumps(report, sort_keys=True) if args.json else report)


if __name__ == "__main__":
    main()
