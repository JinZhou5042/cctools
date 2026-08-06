"""Python workflow driver over the C TaskVine scheduler."""

import collections
import concurrent.futures
import cloudpickle
import hashlib
import json
import os
from pathlib import Path
import queue
import threading
import time
import urllib.error
import uuid

from ..cache import WorkerCacheAdmission
from ..diagnostics import rank_bottlenecks
from ..workflow import iter_output_refs
from ..protocol import DataVineRemoteError
from .configuration import configure_runtime
from .execution_state import (
    ExecutionState,
    PersistenceState,
    PruningState,
)
from .persistence import PersistencePolicy
from .readiness import ReadyQueue, build_cache_plan
from .recovery import select_recovery_audit_data_ids
from .registration import WorkflowRegistrar
from .reporting import (
    format_logical_outputs,
    format_manager_metrics,
    select_report_scope,
)
from .run_context import WorkflowRunContext
from .task_factory import DataVineCall, TaskFactory, ensure_worker_library


class WorkflowDriver:
    _registration_batch_size = 4096
    _compute_attempt_limit = 3
    _compute_submission_window = 1024

    def __init__(
        self,
        controller_client,
        bulk_origin_dir=None,
        bulk_threshold=8 * 1024 * 1024,
    ):
        self.controller = controller_client
        self._registrar = WorkflowRegistrar(
            controller_client,
            bulk_origin_dir=bulk_origin_dir,
            bulk_threshold=bulk_threshold,
            task_batch_size=self._registration_batch_size,
        )
        self._commands = queue.Queue()
        self._thread = threading.Thread(
            target=self._run,
            name="datavine-workflow-driver",
            daemon=False,
        )
        self._owner_ident = None
        self._started = False
        self._ready = threading.Event()
        self._stop_requested = threading.Event()
        self._manager = None
        self._peer_transfers_enabled = True
        self._run_context = WorkflowRunContext()
        self._last_run_report = {}
        self._worker_reconciliation_deferrals = 0
        self._active_worker_ids = frozenset()
        self._worker_reconciliations = 0
        self._worker_status_polls = 0
        self._last_worker_status_poll = 0.0
        self._worker_status_poll_interval = 1.0
        self._reconciled_affected_data_ids = ()
        self._cache_admission = WorkerCacheAdmission(controller_client)
        self._control_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="datavine-control-events",
        )

    @property
    def thread_ident(self):
        return self._owner_ident

    def start(self):
        if self._started:
            raise RuntimeError("Workflow Driver already started")
        self._started = True
        self._thread.start()
        if not self._ready.wait(timeout=10):
            raise RuntimeError("Workflow Driver thread did not start")
        return self

    def call(self, operation, *args, **kwargs):
        return self.submit(operation, *args, **kwargs).result()

    def submit(self, operation, *args, **kwargs):
        if not self._started:
            raise RuntimeError("Workflow Driver is not started")
        future = concurrent.futures.Future()
        self._commands.put((operation, args, kwargs, future))
        return future

    def stop(self):
        if not self._started:
            return
        self._stop_requested.set()
        future = self.submit("_stop")
        try:
            future.result(timeout=30)
        except concurrent.futures.TimeoutError as exc:
            raise RuntimeError(
                "Workflow Driver operation did not stop within 30 seconds"
            ) from exc
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            raise RuntimeError("Workflow Driver thread did not stop")
        self._started = False

    def _raise_if_stopping(self):
        if self._stop_requested.is_set():
            raise concurrent.futures.CancelledError(
                "Workflow Driver stop requested"
            )

    def _run(self):
        self._owner_ident = threading.get_ident()
        self._ready.set()
        while True:
            operation, args, kwargs, future = self._commands.get()
            if operation == "_stop":
                self._control_executor.shutdown(wait=True)
                if self._manager is not None:
                    self._manager._free()
                    self._manager = None
                future.set_result(True)
                return
            try:
                method = getattr(self, f"_op_{operation}")
                future.set_result(method(*args, **kwargs))
            except BaseException as exc:
                future.set_exception(exc)

    def _assert_owner(self):
        if threading.get_ident() != self._owner_ident:
            raise RuntimeError(
                "Workflow Driver state mutation outside owner thread"
            )

    def _op_register_edata(self, metadata, serialized_bytes):
        self._assert_owner()
        return self.controller.register_edata(metadata, serialized_bytes)

    def _op_controller_snapshot(self):
        self._assert_owner()
        return self.controller.snapshot()

    def _op_worker_count(self):
        self._assert_owner()
        if self._manager is None:
            return 0
        # Drive the manager event loop so newly connected workers become
        # visible even before the first logical task is submitted.
        self._manager.wait(1)
        return len(self._sync_worker_epochs(force=True))

    def _op_warm_worker_library(self):
        self._assert_owner()
        if self._manager is None:
            raise RuntimeError("TaskVine Manager is not initialized")
        ensure_worker_library(self._manager)
        workers = self._manager.status("workers")
        task_ids = {
            self._manager.submit(
                DataVineCall(
                    "datavine-worker-v2",
                    "warm_datavine_worker",
                    self.controller.endpoint,
                    self.controller.token,
                    self.controller.native_endpoint,
                )
            )
            for _ in range(
                sum(int(worker["cores_total"]) for worker in workers)
            )
        }
        while task_ids:
            self._raise_if_stopping()
            completed = self._manager.wait(1)
            if completed is None or completed.id not in task_ids:
                continue
            if not completed.successful():
                raise RuntimeError(
                    "DataVine worker library warmup failed: "
                    f"result={completed.result} exit_code={completed.exit_code}"
                )
            task_ids.remove(completed.id)
        return True

    def _sync_worker_epochs(self, force=False):
        self._reconciled_affected_data_ids = ()
        now = time.monotonic()
        if (
            not force
            and now - self._last_worker_status_poll
            < self._worker_status_poll_interval
        ):
            return set(self._active_worker_ids)
        workers = self._manager.status("workers")
        self._last_worker_status_poll = now
        self._worker_status_polls += 1
        worker_ids = {
            worker["workerid"]
            for worker in workers
            if worker.get("workerid")
        }
        if len(worker_ids) != len(workers):
            # TaskVine can expose a connecting/disconnecting status row
            # before its WorkerID is available. Do not treat incomplete
            # observation as global-loss truth. A later complete snapshot
            # performs the authoritative reconciliation.
            self._worker_reconciliation_deferrals += 1
            self._last_worker_status_poll = 0.0
            return worker_ids
        observed_worker_ids = frozenset(worker_ids)
        if observed_worker_ids == self._active_worker_ids:
            return worker_ids
        for worker_id in sorted(worker_ids):
            self.controller.claim_worker(worker_id)
        reconciliation = self.controller.reconcile_workers(worker_ids)
        self._reconciled_affected_data_ids = tuple(
            reconciliation.get("affected_data_ids", ())
        )
        self._cache_admission.sync_workers(worker_ids)
        self._active_worker_ids = observed_worker_ids
        self._worker_reconciliations += 1
        return worker_ids

    def _op_last_run_report(self):
        self._assert_owner()
        return dict(self._last_run_report)

    def _op_apply_pruning(
        self,
        grace_seconds=60,
        data_ids=None,
        now=None,
    ):
        self._assert_owner()
        if self._manager is None:
            raise RuntimeError("TaskVine Manager is not configured")
        plan = self.controller.pruning_plan()
        if not plan["records"]:
            return {
                "controller": {
                    "cancelled_persistence": [],
                    "applied": [],
                    "deferred": [],
                    "plan": plan,
                },
                "worker_prunes": [],
            }
        graph_revision = plan["records"][0]["graph_revision"]
        state_revision = plan["records"][0]["state_revision"]
        result = self.controller.apply_pruning(
            graph_revision,
            state_revision,
            grace_seconds,
            data_ids,
            now,
        )
        pending_by_data = {}
        for record in result["applied"]:
            if record["action"] != "invalidate-worker-pending-delete":
                continue
            pending_by_data.setdefault(record["data_id"], []).append(
                record
            )
        worker_prunes = []
        for data_id, records in sorted(pending_by_data.items()):
            confirmed = 0
            for record in records:
                source_url = record.get("source_url")
                if not source_url:
                    raise RuntimeError(
                        f"worker prune i:{data_id} lacks data endpoint"
                    )
                self.controller.prune_source(source_url)
                self.controller.confirm_replica_pruned(
                    f"i:{data_id}",
                    record["replica_id"],
                    record["generation"],
                )
                confirmed += 1
            worker_prunes.append(
                {
                    "data_id": data_id,
                    "requested": len(records),
                    "confirmed": confirmed,
                    "failed": 0,
                    "tracker_released": True,
                }
            )
        return {
            "controller": result,
            "worker_prunes": worker_prunes,
        }

    def _op_create_manager(
        self,
        port=0,
        name=None,
        run_info_path=None,
        peer_transfers=True,
        manager_logs=False,
    ):
        self._assert_owner()
        if self._manager is not None:
            raise RuntimeError("TaskVine Manager already exists")
        from ndcctools.taskvine import Manager, cvine

        kwargs = {"port": port, "name": name}
        if run_info_path is not None:
            kwargs["run_info_path"] = run_info_path
        self._manager = Manager(**kwargs)
        if (
            not manager_logs
            and self._manager.tune("disable-manager-logs", 1) < 0
        ):
            raise RuntimeError("could not disable TaskVine Manager logs")
        debug_file = os.environ.get("DATAVINE_MANAGER_DEBUG_FILE")
        if debug_file and not cvine.vine_enable_debug_log(debug_file):
            raise RuntimeError("could not enable TaskVine Manager debug log")
        if os.environ.get("DATAVINE_WATCH_LIBRARY_LOGFILES"):
            if self._manager.tune("watch-library-logfiles", 1) < 0:
                raise RuntimeError(
                    "TaskVine Manager rejected library log collection"
                )
        if peer_transfers:
            self._manager.enable_peer_transfers()
        else:
            self._manager.disable_peer_transfers()
        self._peer_transfers_enabled = bool(peer_transfers)
        return self._manager.port

    def _op_register_workflow(self, workflow):
        self._assert_owner()
        workflow.validate()
        self._run_context = WorkflowRunContext()
        return self._registrar.register(workflow, self._run_context)

    def _task_record(self, task_id):
        record = self._run_context.task_records.get(int(task_id))
        if record is None:
            record = self.controller.get_task(task_id)
            self._run_context.task_records[int(task_id)] = record
        return record

    def _op_run_workflow(
        self,
        workflow,
        environment=None,
        wait_timeout=1,
        persist_outputs=False,
        inject_global_loss_after=None,
        inject_worker_loss_after=None,
        worker_disk_cache_bytes=None,
        worker_dram_cache_bytes=256 * 1024 * 1024,
        worker_disk_cache_items=None,
        worker_disk_cache_admission_items=None,
        worker_disk_cache_admission_bytes=None,
        result_task_ids=None,
        inject_external_persistence_cancel=False,
        inject_external_persistence_failures=0,
        external_persistence_max_retries=3,
        external_persistence_retry_base_seconds=0.25,
        external_persistence_retry_max_seconds=5,
        external_persistence_failure_delay=2,
        inject_global_loss_during_persistence=False,
        persistence_attempts_by_task=None,
        inject_worker_loss_schedule=None,
        inject_worker_loss_data_by_task=None,
        prune_after_persistence_by_task=None,
        worker_loss_process_shutdown=False,
        frontier_pruning_ack_delay=0,
        inject_peer_source_losses=0,
        inject_peer_source_loss_after_bytes=0,
        defer_peer_source_loss_after_bytes=False,
        peer_transfer_pruning_probe_task_ids=(),
        inject_peer_corruptions=0,
        inject_idata_release_failures=0,
        peer_release_retry_seconds=0.1,
        peer_release_capacity=1024,
        frontier_pruning_grace_seconds=30,
        hard_delete_pruned_sharedfs=False,
        detailed_report=False,
    ):
        self._assert_owner()
        worker_dram_cache_bytes = int(worker_dram_cache_bytes)
        if worker_dram_cache_bytes < 0:
            raise ValueError("worker DRAM cache capacity is negative")
        workflow_run_started = time.monotonic()
        if self._manager is None:
            raise RuntimeError("create_manager must be called first")
        ensure_worker_library(self._manager)
        reconciliation_deferrals_before = (
            self._worker_reconciliation_deferrals
        )
        tuning = configure_runtime(
            worker_disk_cache_admission_items=(
                worker_disk_cache_admission_items
            ),
            worker_disk_cache_admission_bytes=(
                worker_disk_cache_admission_bytes
            ),
            peer_source_losses=inject_peer_source_losses,
            peer_source_loss_after_bytes=(
                inject_peer_source_loss_after_bytes
            ),
            defer_peer_source_loss_after_bytes=(
                defer_peer_source_loss_after_bytes
            ),
            peer_corruptions=inject_peer_corruptions,
            idata_release_failures=inject_idata_release_failures,
            peer_release_retry_seconds=peer_release_retry_seconds,
            peer_release_capacity=peer_release_capacity,
        )
        peer_source_losses = tuning.peer_source_losses
        peer_source_loss_after_bytes = (
            tuning.peer_source_loss_after_bytes
        )
        defer_peer_source_loss_after_bytes = (
            tuning.defer_peer_source_loss_after_bytes
        )
        peer_corruptions = tuning.peer_corruptions
        idata_release_failures = tuning.idata_release_failures
        peer_release_retry_seconds = tuning.peer_release_retry_seconds
        peer_release_capacity = tuning.peer_release_capacity
        transfer_faults_enabled = bool(
            peer_source_losses
            or peer_source_loss_after_bytes
            or peer_corruptions
            or idata_release_failures
        )
        self.controller.configure_transfer_faults(
            source_losses=peer_source_losses,
            source_loss_after_bytes=peer_source_loss_after_bytes,
            defer_source_loss=defer_peer_source_loss_after_bytes,
            corruptions=peer_corruptions,
            release_failures=idata_release_failures,
            release_capacity=peer_release_capacity,
        )
        controller_snapshot = self.controller.snapshot()
        output_ids = self._op_register_workflow(workflow)
        task_factory = TaskFactory(
            self._manager,
            self.controller,
            self._run_context,
            self._task_record,
            worker_dram_cache_bytes,
            allow_peer_transfer=self._peer_transfers_enabled,
            transfer_faults=transfer_faults_enabled,
            diagnostics=detailed_report,
        )
        compute_submission_window = max(
            self._compute_submission_window,
            4
            * sum(
                int(worker["cores_total"])
                for worker in self._manager.status("workers")
            ),
        )
        workflow_registration_elapsed = (
            time.monotonic() - workflow_run_started
        )
        peer_transfer_pruning_probe_task_ids = tuple(
            sorted(
                {
                    int(task_id)
                    for task_id
                    in peer_transfer_pruning_probe_task_ids
                }
            )
        )
        unknown_probe_tasks = (
            set(peer_transfer_pruning_probe_task_ids) - set(output_ids)
        )
        if unknown_probe_tasks:
            raise ValueError(
                "peer transfer pruning probe has unknown TaskIDs "
                f"{sorted(unknown_probe_tasks)}"
            )
        producer_by_data_id = {
            data_id: task_id
            for task_id, data_ids in self._run_context.logical_output_slots.items()
            for data_id in data_ids
        }
        if result_task_ids is None:
            result_task_ids = tuple(output_ids)
        else:
            result_task_ids = tuple(int(value) for value in result_task_ids)
            unknown_results = set(result_task_ids) - set(output_ids)
            if unknown_results:
                raise KeyError(
                    f"unknown result TaskIDs {sorted(unknown_results)}"
                )
        task_by_id = {task.task_id: task for task in workflow.tasks}
        execution = ExecutionState(pending=set(task_by_id))
        physical_completion_queue = collections.deque()

        def record_worker_dram_cache(value):
            (
                worker_id,
                capacity_bytes,
                bytes_used,
                items,
                hits,
                misses,
                admissions,
                evictions,
            ) = value
            if not worker_id:
                raise RuntimeError("DRAM cache report lacks WorkerID")
            execution.worker_dram_cache[worker_id] = {
                "capacity_bytes": int(capacity_bytes),
                "bytes": int(bytes_used),
                "items": int(items),
                "hits": int(hits),
                "misses": int(misses),
                "admissions": int(admissions),
                "evictions": int(evictions),
            }

        explicit_persistence_frontiers = (
            persistence_attempts_by_task is not None
        )
        if persistence_attempts_by_task is None:
            persistence_attempts_by_task = {
                task_id: 1 for task_id in task_by_id
            }
        else:
            persistence_attempts_by_task = {
                int(task_id): int(attempt)
                for task_id, attempt
                in persistence_attempts_by_task.items()
            }
            unknown_persistence = (
                set(persistence_attempts_by_task) - set(task_by_id)
            )
            if unknown_persistence:
                raise KeyError(
                    "unknown persistence TaskIDs "
                    f"{sorted(unknown_persistence)}"
                )
            if any(
                attempt < 1
                for attempt in persistence_attempts_by_task.values()
            ):
                raise ValueError(
                    "persistence attempt thresholds must be positive"
                )
        persistence_frontier_tasks = (
            set(persistence_attempts_by_task)
            if explicit_persistence_frontiers
            else set()
        )
        if inject_worker_loss_schedule is None:
            worker_loss_schedule = (
                ()
                if inject_worker_loss_after is None
                else (int(inject_worker_loss_after),)
            )
        else:
            worker_loss_schedule = tuple(
                int(task_id)
                for task_id in inject_worker_loss_schedule
            )
            unknown_losses = set(worker_loss_schedule) - set(task_by_id)
            if unknown_losses:
                raise KeyError(
                    f"unknown worker-loss TaskIDs "
                    f"{sorted(unknown_losses)}"
                )
        inject_worker_loss_data_by_task = {
            int(trigger_task_id): tuple(
                int(producer_task_id)
                for producer_task_id in producer_task_ids
            )
            for trigger_task_id, producer_task_ids in (
                inject_worker_loss_data_by_task or {}
            ).items()
        }
        unknown_loss_data = {
            task_id
            for trigger_task_id, producer_task_ids
            in inject_worker_loss_data_by_task.items()
            for task_id in (trigger_task_id, *producer_task_ids)
            if task_id not in task_by_id
        }
        if unknown_loss_data:
            raise KeyError(
                "unknown worker-loss data TaskIDs "
                f"{sorted(unknown_loss_data)}"
            )
        prune_after_persistence_by_task = {
            int(frontier_task_id): tuple(
                int(producer_task_id)
                for producer_task_id in producer_task_ids
            )
            for frontier_task_id, producer_task_ids in (
                prune_after_persistence_by_task or {}
            ).items()
        }
        unknown_prune_tasks = {
            task_id
            for frontier_task_id, producer_task_ids
            in prune_after_persistence_by_task.items()
            for task_id in (frontier_task_id, *producer_task_ids)
            if task_id not in task_by_id
        }
        if unknown_prune_tasks:
            raise KeyError(
                "unknown frontier-prune TaskIDs "
                f"{sorted(unknown_prune_tasks)}"
            )
        dependencies = {
            task.task_id: {
                reference.producer_task_id
                for value in (*task.args, *task.kwargs.values())
                for reference in iter_output_refs(value)
            }
            for task in workflow.tasks
        }
        dependents = {task_id: set() for task_id in task_by_id}
        for task_id, parent_ids in dependencies.items():
            for parent_id in parent_ids:
                dependents[parent_id].add(task_id)
        ready_queue = ReadyQueue(
            dependencies, dependents, execution.pending, execution.done
        )

        def cache_size(data_key):
            kind, token = data_key.split(":", 1)
            if kind == "e":
                metadata = self._run_context.edata_info.get(int(token))
                if metadata is None:
                    metadata = self.controller.get_edata_metadata(
                        int(token)
                    )
                    self._run_context.edata_info[int(token)] = metadata
                return metadata["size"]
            return 0

        cache_plan = build_cache_plan(
            task_by_id,
            self._task_record,
            self._run_context.nested_idata_by_task,
            self._run_context.logical_output_slots,
            cache_size,
            retention_items=worker_disk_cache_items,
            retention_bytes=worker_disk_cache_bytes,
            admission_items=worker_disk_cache_admission_items,
            admission_bytes=worker_disk_cache_admission_bytes,
        )
        task_cache_inputs = cache_plan.task_inputs
        remaining_cache_uses = cache_plan.remaining_uses
        max_task_cache_items = cache_plan.max_task_items
        max_task_known_cache_bytes = cache_plan.max_known_input_bytes
        effective_retention_items = cache_plan.retention_items
        effective_retention_bytes = cache_plan.retention_bytes
        persistence = PersistenceState()
        pruning = PruningState()
        frontier_pruning_ack_delay = float(
            frontier_pruning_ack_delay
        )
        if frontier_pruning_ack_delay < 0:
            raise ValueError(
                "frontier pruning acknowledgement delay cannot be negative"
            )
        frontier_pruning_grace_seconds = float(
            frontier_pruning_grace_seconds
        )
        if frontier_pruning_grace_seconds < 0:
            raise ValueError(
                "frontier pruning grace period cannot be negative"
            )
        hard_delete_pruned_sharedfs = bool(
            hard_delete_pruned_sharedfs
        )

        def start_frontier_worker_prunes(active, records):
            pending_by_data = {}
            for record in records:
                if (
                    record["action"]
                    == "invalidate-worker-pending-delete"
                ):
                    pending_by_data.setdefault(
                        record["data_id"], []
                    ).append(record)
            for data_id, data_records in sorted(
                pending_by_data.items()
            ):
                for record in data_records:
                    source_url = record.get("source_url")
                    if source_url:
                        self.controller.prune_source(source_url)
                    self.controller.confirm_replica_pruned(
                        f"i:{data_id}",
                        record["replica_id"],
                        record["generation"],
                    )
                active["reconciled_worker_prunes"].append(
                    {
                        "data_id": data_id,
                        "requested": len(data_records),
                        "confirmed": len(data_records),
                        "failed": 0,
                        "reconciled": 0,
                        "tracker_released": True,
                    }
                )
        persistence_policy = PersistencePolicy.from_options(
            inject_external_persistence_failures,
            external_persistence_max_retries,
            external_persistence_retry_base_seconds,
            external_persistence_retry_max_seconds,
            external_persistence_failure_delay,
        )
        persistence_capacity = int(
            (
                controller_snapshot.get("persistence_executor")
                or {}
            ).get("workers", 1)
        )
        peer_transfer_pruning_probes = []
        peer_transfer_pruning_probe_triggered = False
        completed_task_states = []
        output_projections = {}
        projection_count = 0
        replica_projections = []
        control_futures = collections.deque()
        control_pipeline = {
            "batches": 0,
            "completed_tasks": 0,
            "output_events": 0,
            "replica_events": 0,
            "inflight_high_water": 0,
            "backpressure_seconds": 0.0,
        }
        peer_release_pending = []

        def drain_peer_releases():
            now = time.monotonic()
            for release in tuple(peer_release_pending):
                if release["not_before"] > now:
                    continue
                self.controller.release_native_source(
                    release["lease_id"], True
                )
                self.controller.complete_release_retry()
                peer_release_pending.remove(release)

        def apply_control_events(
            batches, replicas, completed_task_ids
        ):
            result = self.controller.project_scheduler_events(
                batches, replicas, completed_task_ids
            )
            expected = sum(
                len(outputs) for _, _, outputs in batches
            ) + len(replicas)
            if result != {
                "projected": expected,
                "completed": len(completed_task_ids),
            }:
                raise RuntimeError(
                    "Controller returned incomplete scheduler events"
                )

        def submit_control_events():
            nonlocal projection_count
            if not projection_count and not completed_task_states:
                return
            batches = tuple(
                (worker_id, worker_epoch, outputs)
                for (worker_id, worker_epoch), outputs
                in output_projections.items()
            )
            replicas = tuple(replica_projections)
            completed = tuple(completed_task_states)
            output_projections.clear()
            replica_projections.clear()
            completed_task_states.clear()
            projection_count = 0
            control_pipeline["batches"] += 1
            control_pipeline["completed_tasks"] += len(completed)
            control_pipeline["output_events"] += sum(
                len(outputs) for _, _, outputs in batches
            )
            control_pipeline["replica_events"] += len(replicas)
            control_futures.append(
                self._control_executor.submit(
                    apply_control_events,
                    batches,
                    replicas,
                    completed,
                )
            )
            control_pipeline["inflight_high_water"] = max(
                control_pipeline["inflight_high_water"],
                len(control_futures),
            )
            while len(control_futures) >= 8:
                started = time.monotonic()
                control_futures.popleft().result()
                control_pipeline["backpressure_seconds"] += (
                    time.monotonic() - started
                )

        def flush_control_events():
            submit_control_events()
            while control_futures:
                control_futures.popleft().result()

        def flush_output_projections():
            flush_control_events()

        def record_output_projection(outputs):
            nonlocal projection_count
            key = (
                outputs[0]["worker_id"],
                int(outputs[0]["worker_epoch"]),
            )
            output_projections.setdefault(key, []).extend(outputs)
            projection_count += len(outputs)
            if projection_count >= 4096:
                submit_control_events()

        def record_replica_projection(replica):
            nonlocal projection_count
            replica_projections.append(replica)
            projection_count += 1
            if projection_count >= 4096:
                submit_control_events()

        def flush_completed_task_states():
            flush_control_events()

        def record_completed_task_state(task_id):
            completed_task_states.append(task_id)
            if len(completed_task_states) >= 256:
                submit_control_events()

        def record_pending_task_state(task_id):
            flush_output_projections()
            flush_completed_task_states()
            self.controller.set_task_state(task_id, "pending")

        def queue_frontier_pruning_if_ready(frontier_task_id):
            if (
                frontier_task_id not in prune_after_persistence_by_task
                or frontier_task_id in pruning.applied
                or frontier_task_id in pruning.pending
            ):
                return
            if any(
                self.controller.idata_status(data_id)["durability"]
                != "durable"
                for data_id in self._run_context.logical_output_slots[
                    frontier_task_id
                ]
            ):
                return
            flush_completed_task_states()
            pruning.pending[frontier_task_id] = [
                output_ids[task_id]
                for task_id in prune_after_persistence_by_task[
                    frontier_task_id
                ]
            ]

        def queue_task_persistence(logical_id, output_data_ids):
            threshold = persistence_attempts_by_task.get(logical_id)
            if (
                threshold is None
                or self._run_context.attempts[logical_id] < threshold
                or not persist_outputs
            ):
                return
            flush_output_projections()
            for output_data_id in output_data_ids:
                status = self.controller.persist_idata(output_data_id)
                persistence.required.add(output_data_id)
                persistence.requested.add(output_data_id)
                request = status.get("persistence_request", {})
                if request.get("mode") != "worker":
                    persistence.controller_pending[
                        output_data_id
                    ] = time.monotonic()
                    continue
                if (
                    inject_external_persistence_cancel
                    or inject_global_loss_during_persistence
                ):
                    request = {**request, "inject_cancel_delay": True}
                if (
                    persistence.injected_external_failures
                    < persistence_policy.injected_failures
                ):
                    request = {
                        **request,
                        "inject_failure_during_write": True,
                        "inject_failure_delay": (
                            persistence_policy.failure_delay_seconds
                        ),
                    }
                    persistence.injected_external_failures += 1
                persistence.pending.append(
                    (time.monotonic(), output_data_id, request)
                )

        def maybe_inject_worker_loss(logical_id):
            if (
                execution.worker_loss_injections
                >= len(worker_loss_schedule)
                or worker_loss_schedule[
                    execution.worker_loss_injections
                ]
                != logical_id
            ):
                return False
            flush_output_projections()
            from ndcctools.taskvine import cvine

            workers_before = sorted(self._sync_worker_epochs(force=True))
            if not workers_before:
                raise RuntimeError("no worker available for loss injection")
            target_replica_worker_ids = []
            if worker_loss_process_shutdown:
                target_data_id = output_ids[logical_id]
                target_sources = self.controller.replica_sources(
                    f"i:{target_data_id}"
                )["sources"]
                target_replica_worker_ids = sorted(
                    {
                        source["worker_id"]
                        for source in target_sources
                        if source.get("worker_id") in workers_before
                        and str(source.get("tier", "")).startswith(
                            "worker-"
                        )
                    }
                )
                if not target_replica_worker_ids:
                    raise RuntimeError(
                        "no connected volatile replica worker for "
                        f"loss target i:{target_data_id}"
                    )
                released_worker_id = target_replica_worker_ids[0]
                if not cvine.vine_manager_shut_down_worker_by_id(
                    self._manager._taskvine, released_worker_id
                ):
                    raise RuntimeError(
                        "could not shut down deterministic worker "
                        f"{released_worker_id}"
                    )
            else:
                if not cvine.vine_manager_release_random_worker(
                    self._manager._taskvine
                ):
                    raise RuntimeError("could not release worker")
                released_worker_id = None
            workers_after = sorted(self._sync_worker_epochs(force=True))
            if worker_loss_process_shutdown:
                disconnect_deadline = time.monotonic() + 10
                while released_worker_id in self._active_worker_ids:
                    if time.monotonic() >= disconnect_deadline:
                        raise TimeoutError(
                            "deterministically shut down worker did not "
                            f"disconnect: {released_worker_id}"
                        )
                    completed_during_loss = self._manager.wait(1)
                    if completed_during_loss is not None:
                        physical_completion_queue.append(
                            completed_during_loss
                        )
                    self._sync_worker_epochs(force=True)
                workers_after = sorted(self._active_worker_ids)
                execution.recovery_audit_data_ids.update(
                    int(data_key.split(":", 1)[1])
                    for data_key in self._reconciled_affected_data_ids
                    if str(data_key).startswith("i:")
                )
            else:
                released = sorted(
                    set(workers_before) - set(workers_after)
                )
                if len(released) == 1:
                    released_worker_id = released[0]
            lost_task_ids = inject_worker_loss_data_by_task.get(
                logical_id, (logical_id,)
            )
            for lost_task_id in lost_task_ids:
                lost_data_id = output_ids[lost_task_id]
                self.controller.invalidate_idata(lost_data_id)
            execution.recovery_audit_data_ids.update(
                output_ids[task_id] for task_id in lost_task_ids
            )
            removed_persistence = [
                entry
                for entry in persistence.pending
                if entry[1] == output_ids[logical_id]
            ]
            persistence.injected_external_failures -= sum(
                bool(entry[2].get("inject_failure_during_write"))
                for entry in removed_persistence
            )
            persistence.pending = [
                entry
                for entry in persistence.pending
                if entry[1] != output_ids[logical_id]
            ]
            execution.worker_loss_events.append(
                {
                    "trigger_task_id": logical_id,
                    "released_worker_id": released_worker_id,
                    "workers_before": workers_before,
                    "workers_after": workers_after,
                    "lost_task_ids": list(lost_task_ids),
                    "target_replica_worker_ids": (
                        target_replica_worker_ids
                    ),
                    "process_shutdown": bool(worker_loss_process_shutdown),
                }
            )
            execution.worker_loss_injections += 1
            return True

        workflow_execution_started = time.monotonic()
        while (
            execution.has_work()
            or persistence.has_work()
            or pruning.has_work()
        ):
            self._raise_if_stopping()
            drain_peer_releases()
            for data_id, not_before in tuple(
                persistence.controller_pending.items()
            ):
                if not_before > time.monotonic():
                    continue
                status = self.controller.idata_status(data_id)
                if status["durability"] in ("queued", "writing"):
                    continue
                if status["durability"] == "failed":
                    persistence.failures += 1
                    if not status["available"]:
                        persistence.controller_pending.pop(data_id)
                        execution.recovery_audit_data_ids.add(data_id)
                        continue
                    retry_key = (data_id, int(status["attempt"]))
                    retries = persistence.retry_counts[retry_key]
                    if retries >= persistence_policy.maximum_retries:
                        raise RuntimeError(
                            f"IDataID {data_id} Controller persistence "
                            f"exhausted {retries} retries: status={status}"
                        )
                    delay = persistence_policy.retry_delay(retries)
                    persistence.retry_counts[retry_key] += 1
                    persistence.retries += 1
                    persistence.retry_delay_seconds += delay
                    retry_status = self.controller.persist_idata(data_id)
                    retry_request = retry_status.get(
                        "persistence_request", {}
                    )
                    if retry_request.get("mode") != "controller":
                        raise RuntimeError(
                            f"IDataID {data_id} Controller persistence "
                            "retry changed execution mode"
                        )
                    persistence.controller_pending[data_id] = (
                        time.monotonic() + delay
                    )
                    continue
                if status["durability"] != "durable":
                    raise RuntimeError(
                        f"IDataID {data_id} Controller persistence "
                        f"failed: status={status}"
                    )
                persistence.controller_pending.pop(data_id)
                persistence.controller_tasks_completed += 1
                persistence.controller_bytes += int(status["size"])
                queue_frontier_pruning_if_ready(
                    producer_by_data_id[data_id]
                )
            for data_id, recovery in tuple(
                persistence.suspended_recovery.items()
            ):
                if not recovery["persistence_drained"]:
                    continue
                execution.pending.add(recovery["logical_id"])
                persistence.suspended_recovery.pop(data_id)
                ready_queue.rebuild(execution.pending, execution.done)
            recovery_wave = []

            def require_available(data_id):
                task_id = producer_by_data_id[data_id]
                if task_id not in execution.done:
                    return
                output_status = self.controller.idata_status(
                    data_id
                )
                if output_status["available"]:
                    return
                record_pending_task_state(task_id)
                execution.done.remove(task_id)
                execution.pending.add(task_id)
                execution.recovery_reexecutions += 1
                recovery_wave.append(task_id)
                producer_record = self._task_record(task_id)
                for input_data_id in producer_record.input_data_ids:
                    require_available(input_data_id)

            if execution.recovery_audit_data_ids:
                audit_data_ids = select_recovery_audit_data_ids(
                    execution.recovery_audit_data_ids,
                    producer_by_data_id,
                    dependencies,
                    dependents,
                    (task.task_id for task in workflow.tasks),
                )
                execution.recovery_audit_data_ids.clear()
                for data_id in audit_data_ids:
                    require_available(data_id)
            if recovery_wave:
                flush_completed_task_states()
                recovered_inputs = {
                    f"i:{data_id}"
                    for task_id in recovery_wave
                    for data_id in self._run_context.logical_output_slots[
                        task_id
                    ]
                }
                for physical_id, logical_id in tuple(
                    execution.running.items()
                ):
                    if not (
                        task_cache_inputs[logical_id] & recovered_inputs
                    ):
                        continue
                    if not self._manager.cancel_by_task_id(physical_id):
                        continue
                    execution.running.pop(physical_id)
                    execution.pending.add(logical_id)
                    record_pending_task_state(logical_id)
                    for output_data_id in (
                        self._run_context.logical_output_slots[logical_id]
                    ):
                        self.controller.invalidate_idata(output_data_id)
                plan = self.controller.pruning_plan()
                execution.recovery_waves.append(
                    {
                        "tasks": recovery_wave,
                        "rollback_depth": len(recovery_wave),
                        "recovery_depths": plan["recovery_depths"],
                    }
                )
                ready_queue.rebuild(execution.pending, execution.done)

            if (
                pruning.active is not None
                and time.monotonic()
                >= pruning.active["poll_after"]
            ):
                if pruning.active["deferred_data_ids"]:
                    operation_id = pruning.active[
                        "continuation_operation_id"
                    ]
                    if operation_id is None:
                        operation_id = f"pruning:{uuid.uuid4().hex}"
                        pruning.active[
                            "continuation_operation_id"
                        ] = operation_id
                    try:
                        continuation = (
                            self.controller.continue_deferred_pruning(
                                operation_id,
                                sorted(
                                    pruning.active[
                                        "deferred_data_ids"
                                    ]
                                )
                            )
                        )
                    except (
                        urllib.error.URLError,
                        TimeoutError,
                        OSError,
                    ) as exc:
                        pruning.active[
                            "controller_continuation_retries"
                        ].append(
                            {
                                "operation_id": operation_id,
                                "error": type(exc).__name__,
                            }
                        )
                        pruning.active["poll_after"] = (
                            time.monotonic() + 0.1
                        )
                        continuation = None
                    if continuation is not None:
                        pruning.active[
                            "continuation_operation_id"
                        ] = None
                        pruning.active[
                            "controller_continuations"
                        ].append(continuation)
                        still_deferred = {
                            item["data_id"]
                            for item in continuation["deferred"]
                        }
                        resolved = (
                            pruning.active[
                                "deferred_data_ids"
                            ]
                            - still_deferred
                        )
                        cancelled = {
                            item["data_id"]
                            for item in continuation["cancelled"]
                        }
                        if cancelled:
                            pruning.active[
                                "cancelled_data_ids"
                            ].update(cancelled)
                        start_frontier_worker_prunes(
                            pruning.active,
                            continuation["applied"],
                        )
                        pruning.active[
                            "deferred_data_ids"
                        ] -= resolved
                all_complete = not pruning.active[
                    "deferred_data_ids"
                ]
                worker_prunes = list(
                    pruning.active["reconciled_worker_prunes"]
                )
                if (
                    not all_complete
                    and time.monotonic()
                    >= pruning.active["deadline"]
                ):
                    raise TimeoutError(
                        "asynchronous frontier pruning acknowledgement "
                        "timed out"
                    )
                if all_complete:
                    frontier_task_id = pruning.active[
                        "frontier_task_id"
                    ]
                    remaining_data_ids = pruning.active[
                        "remaining_data_ids"
                    ]
                    pruning.events.append(
                        {
                            "frontier_task_id": frontier_task_id,
                            "data_ids": pruning.active[
                                "data_ids"
                            ],
                            "result": {
                                "controller": pruning.active[
                                    "controller_result"
                                ],
                                "controller_continuations": (
                                    pruning.active[
                                        "controller_continuations"
                                    ]
                                ),
                                "controller_continuation_retries": (
                                    pruning.active[
                                        "controller_continuation_retries"
                                    ]
                                ),
                                "worker_prunes": worker_prunes,
                            },
                            "cancelled_data_ids": sorted(
                                pruning.active[
                                    "cancelled_data_ids"
                                ]
                            ),
                        }
                    )
                    persistence.required.difference_update(
                        set(
                            pruning.active["data_ids"]
                        )
                        - pruning.active[
                            "cancelled_data_ids"
                        ]
                    )
                    if remaining_data_ids:
                        pruning.pending[frontier_task_id] = (
                            remaining_data_ids
                        )
                    pruning.applied.add(frontier_task_id)
                    pruning.active = None

            if (
                pruning.active is None
                and pruning.pending
            ):
                active_inputs = {
                    data_key
                    for running_logical_id in execution.running.values()
                    for data_key in task_cache_inputs[running_logical_id]
                }
                safe_frontiers = [
                    frontier_task_id
                    for frontier_task_id, data_ids
                    in pruning.pending.items()
                    if not (
                        {f"i:{data_id}" for data_id in data_ids}
                        & active_inputs
                    )
                    and not (
                        set(data_ids)
                        & {
                            data_id
                            for data_id, _ in persistence.running.values()
                        }
                    )
                ]
                if safe_frontiers:
                    frontier_task_id = min(safe_frontiers)
                    requested_data_ids = pruning.pending[
                        frontier_task_id
                    ]
                    # Worker reconciliation and recovery can advance the
                    # Controller proof between the read and POST.  Retry
                    # only this optimistic revision check with a fresh proof;
                    # never apply a stale pruning decision.
                    result = None
                    prune_data_ids = []
                    for pruning_retry in range(3):
                        plan = self.controller.pruning_plan()
                        eligible = set(plan["prunable"]) | set(
                            plan["cancel_persistence"]
                        )
                        already_absent = {
                            record["data_id"]
                            for record in plan["records"]
                            if record["decision"] == "absent"
                        }
                        prune_data_ids = sorted(
                            set(requested_data_ids) & eligible
                        )
                        if not prune_data_ids:
                            break
                        try:
                            result = self.controller.apply_pruning(
                                plan["records"][0]["graph_revision"],
                                plan["records"][0]["state_revision"],
                                frontier_pruning_grace_seconds,
                                prune_data_ids,
                                None,
                            )
                            break
                        except DataVineRemoteError as exc:
                            if (
                                not any(
                                    marker in str(exc)
                                    for marker in (
                                        "pruning proof revision changed",
                                        "are not prunable",
                                        "proof changed before pruning",
                                    )
                                )
                                or pruning_retry == 2
                            ):
                                raise
                    if result is None:
                        result = {
                            "cancelled_persistence": [],
                            "applied": [],
                            "deferred": [],
                        }
                    pruning.pending.pop(frontier_task_id)
                    now_value = time.monotonic()
                    pruning.active = {
                        "frontier_task_id": frontier_task_id,
                        "data_ids": prune_data_ids,
                        "remaining_data_ids": sorted(
                            set(requested_data_ids)
                            - set(prune_data_ids)
                            - already_absent
                        ),
                        "controller_result": result,
                        "controller_continuations": [],
                        "controller_continuation_retries": [],
                        "continuation_operation_id": None,
                        "deferred_data_ids": {
                            item["data_id"]
                            for item in result["deferred"]
                        },
                        "cancelled_data_ids": set(),
                        "reconciled_worker_prunes": [],
                        "poll_after": (
                            now_value + frontier_pruning_ack_delay
                        ),
                        "deadline": now_value + 30,
                    }
                    start_frontier_worker_prunes(
                        pruning.active,
                        [
                            record
                            for record in result["applied"]
                            if record["data_id"]
                            not in pruning.active[
                                "deferred_data_ids"
                            ]
                        ],
                    )

            def persistence_frontier_ready(task_id):
                if not persist_outputs:
                    return True
                for parent_task_id in dependencies[task_id]:
                    if (
                        parent_task_id
                        not in persistence_frontier_tasks
                    ):
                        continue
                    threshold = persistence_attempts_by_task.get(
                        parent_task_id
                    )
                    if (
                        threshold is None
                        or self._run_context.attempts.get(parent_task_id, 0)
                        < threshold
                    ):
                        continue
                    if (
                        parent_task_id
                        in prune_after_persistence_by_task
                        and parent_task_id
                        not in pruning.applied
                    ):
                        return False
                    if any(
                        self.controller.idata_status(data_id)[
                            "durability"
                        ] != "durable"
                        for data_id in self._run_context.logical_output_slots[
                            parent_task_id
                        ]
                    ):
                        return False
                return True

            ready = ready_queue.take(
                execution.pending,
                persistence_frontier_ready,
                max(
                    0,
                    compute_submission_window
                    - len(execution.running),
                ),
            )
            for task_id in ready:
                attempt = self._run_context.attempts.get(task_id, 0) + 1
                self._run_context.attempts[task_id] = attempt
                task_build_started = time.monotonic()
                physical = task_factory.make_physical_task(
                    task_id,
                    environment,
                    attempt,
                )
                execution.physical_task_build_seconds += (
                    time.monotonic() - task_build_started
                )
                task_submit_started = time.monotonic()
                physical_id = self._manager.submit(physical)
                execution.physical_task_submit_seconds += (
                    time.monotonic() - task_submit_started
                )
                execution.physical_submissions += 1
                execution.running[physical_id] = task_id
                execution.pending.remove(task_id)
            while (
                persistence.pending
                and len(persistence.running) < persistence_capacity
            ):
                persistence.pending.sort(key=lambda entry: entry[0])
                not_before, data_id, request = persistence.pending[0]
                if not_before > time.monotonic():
                    break
                persistence.pending.pop(0)
                physical = task_factory.make_persistence_task(
                    data_id, request, environment
                )
                physical_id = self._manager.submit(physical)
                persistence.running[physical_id] = (data_id, request)
            if (
                not execution.running
                and not persistence.running
            ):
                if persistence.pending:
                    delay = max(
                        0,
                        persistence.pending[0][0] - time.monotonic(),
                    )
                    time.sleep(min(float(wait_timeout), delay))
                    continue
                if persistence.suspended_recovery:
                    self._manager.wait(wait_timeout)
                    self._sync_worker_epochs()
                    continue
                if persistence.controller_pending:
                    delay = max(
                        0,
                        min(persistence.controller_pending.values())
                        - time.monotonic(),
                    )
                    time.sleep(
                        min(float(wait_timeout), max(0.05, delay))
                    )
                    continue
                if not (
                    execution.has_work()
                    or persistence.pending
                    or persistence.running
                    or persistence.suspended_recovery
                    or pruning.has_work()
                ):
                    break
                if pruning.active is not None:
                    self._manager.wait(wait_timeout)
                    self._sync_worker_epochs()
                    continue
                blocked = {}
                for task_id in sorted(execution.pending):
                    unavailable_inputs = []
                    persistence_frontiers = {}
                    for input_data_id in self._task_record(
                        task_id
                    ).input_data_ids:
                        status = self.controller.idata_status(
                            input_data_id
                        )
                        if not status["available"]:
                            unavailable_inputs.append(input_data_id)
                    for parent_task_id in sorted(
                        dependencies[task_id]
                        & persistence_frontier_tasks
                    ):
                        persistence_frontiers[parent_task_id] = {
                            "attempt": self._run_context.attempts.get(
                                parent_task_id, 0
                            ),
                            "threshold": (
                                persistence_attempts_by_task.get(
                                    parent_task_id
                                )
                            ),
                            "pruning_applied": (
                                parent_task_id
                                in pruning.applied
                            ),
                            "durability": [
                                self.controller.idata_status(data_id)[
                                    "durability"
                                ]
                                for data_id in self._run_context.logical_output_slots[
                                    parent_task_id
                                ]
                            ],
                        }
                    blocked[task_id] = {
                        "unfinished_dependencies": sorted(
                            dependencies[task_id] - execution.done
                        ),
                        "unavailable_inputs": unavailable_inputs,
                        "persistence_frontiers": persistence_frontiers,
                    }
                raise RuntimeError(
                    "workflow cannot make progress: "
                    f"pending={blocked} done={sorted(execution.done)} "
                    f"frontier_pruning_pending="
                    f"{sorted(pruning.pending)}"
                )
            if physical_completion_queue:
                completed = physical_completion_queue.popleft()
            else:
                completed = self._manager.wait(wait_timeout)
            workers_before_sync = self._active_worker_ids
            if (
                time.monotonic() - self._last_worker_status_poll
                >= self._worker_status_poll_interval
            ):
                flush_output_projections()
            active_workers = frozenset(self._sync_worker_epochs())
            workers_lost = workers_before_sync - active_workers
            if workers_lost:
                execution.recovery_audit_data_ids.update(
                    int(data_key.split(":", 1)[1])
                    for data_key in self._reconciled_affected_data_ids
                    if str(data_key).startswith("i:")
                )
            # TaskVine workers may forsake a task whose input transfer fails
            # and the C manager will normally retry that physical task
            # internally.  DataVine must regain ownership of this failure so
            # that the logical task can be recovered from stable lineage
            # instead of looping on a stale attempt indefinitely.
            if workers_lost:
                for physical_id, logical_id in tuple(execution.running.items()):
                    missing_inputs = []
                    for input_data_id in self._task_record(
                        logical_id
                    ).input_data_ids:
                        if not self.controller.idata_status(input_data_id)[
                            "available"
                        ]:
                            missing_inputs.append(input_data_id)
                    if not missing_inputs:
                        continue
                    # The C manager may already have moved this attempt into
                    # its own retrieval/retry path. In that case cancellation
                    # is no longer owned by DataVine; wait for the normal
                    # Manager event instead of treating backpressure as loss.
                    if not self._manager.cancel_by_task_id(physical_id):
                        continue
                    execution.running.pop(physical_id)
                    execution.pending.add(logical_id)
                    ready_queue.mark_pending(logical_id)
                    record_pending_task_state(logical_id)
                    for output_data_id in self._run_context.logical_output_slots[
                        logical_id
                    ]:
                        self.controller.invalidate_idata(output_data_id)
                    execution.recovery_reexecutions += 1
                    execution.unavailable_input_recoveries.append(
                        {
                            "task_id": logical_id,
                            "physical_task_id": physical_id,
                            "missing_inputs": missing_inputs,
                            "attempt": self._run_context.attempts[logical_id],
                        }
                    )
            if (
                defer_peer_source_loss_after_bytes
                and not peer_transfer_pruning_probe_triggered
            ):
                peer_faults = self.controller.transfer_fault_stats()
                if peer_faults[
                    "deferred_peer_source_loss_pending"
                ]:
                    plan = self.controller.pruning_plan()
                    records = {
                        int(record["data_id"]): record
                        for record in plan["records"]
                    }
                    selected = []
                    for task_id in (
                        peer_transfer_pruning_probe_task_ids
                    ):
                        for data_id in self._run_context.logical_output_slots[
                            task_id
                        ]:
                            selected.append(records[int(data_id)])
                    peer_transfer_pruning_probes.append(
                        {
                            "partial_bytes": peer_faults[
                                "peer_transfer_progress_max_bytes"
                            ],
                            "records": selected,
                            "graph_revision": (
                                selected[0]["graph_revision"]
                                if selected
                                else None
                            ),
                            "state_revision": (
                                selected[0]["state_revision"]
                                if selected
                                else None
                            ),
                        }
                    )
                    if not self.controller.trigger_deferred_source_loss():
                        raise RuntimeError(
                            "deferred peer source loss disappeared "
                            "before explicit Scheduler trigger"
                        )
                    peer_transfer_pruning_probe_triggered = True
            if completed is None:
                if (
                    inject_global_loss_during_persistence
                    and persistence.global_losses == 0
                ):
                    for data_id, active_request in (
                        persistence.running.values()
                    ):
                        status = self.controller.idata_status(data_id)
                        if status["durability"] != "writing":
                            continue
                        before = self.controller.pruning_plan()
                        record_before = next(
                            record
                            for record in before["records"]
                            if record["data_id"] == data_id
                        )
                        if (
                            record_before["decision"] != "keep"
                            or "persistence-writing"
                            not in record_before["reasons"]
                        ):
                            raise RuntimeError(
                                "active persistence was not protected "
                                "from pruning"
                            )
                        self.controller.invalidate_idata(data_id)
                        after = self.controller.pruning_plan()
                        record_after = next(
                            record
                            for record in after["records"]
                            if record["data_id"] == data_id
                        )
                        if (
                            record_after["decision"] != "absent"
                            or "no-accepted-replica"
                            not in record_after["reasons"]
                        ):
                            raise RuntimeError(
                                "globally lost IData was not protected "
                                "from pruning"
                            )
                        logical_id = producer_by_data_id[data_id]
                        if logical_id not in execution.done:
                            raise RuntimeError(
                                "persistence loss target was not "
                                "logically completed"
                            )
                        record_pending_task_state(logical_id)
                        execution.done.remove(logical_id)
                        persistence.suspended_recovery[data_id] = {
                            "logical_id": logical_id,
                            "request_id": active_request["request_id"],
                            "persistence_drained": False,
                        }
                        execution.recovery_reexecutions += 1
                        persistence.global_losses += 1
                        persistence.loss_pruning_plans.append(
                            {
                                "data_id": data_id,
                                "before": record_before,
                                "after": record_after,
                            }
                        )
                        break
                if (
                    inject_external_persistence_cancel
                    and persistence.cancellations == 0
                ):
                    for data_id, _ in persistence.running.values():
                        status = self.controller.idata_status(data_id)
                        if status["durability"] == "writing":
                            response = (
                                self.controller.cancel_persistence(
                                    data_id,
                                    "injected-active-cancellation",
                                )
                            )
                            if response["action"] != "cancelling":
                                raise RuntimeError(
                                    "active persistence cancellation "
                                    "did not enter cancelling"
                                )
                            persistence.cancellations += 1
                            break
                flush_output_projections()
                self._cache_admission.enforce(
                    effective_retention_bytes,
                    effective_retention_items,
                    remaining_cache_uses,
                    {
                        data_key
                        for logical_id in execution.running.values()
                        for data_key in task_cache_inputs[logical_id]
                    },
                )
                continue
            if completed.id in persistence.running:
                data_id, request = persistence.running.pop(completed.id)
                persistence_result = (
                    completed.output
                    if isinstance(completed.output, dict)
                    and completed.output.get("protocol")
                    == "datavine-persist-v1"
                    else None
                )
                persistence_error = (
                    persistence_result.get("error")
                    if persistence_result is not None
                    else None
                )
                suspended = persistence.suspended_recovery.get(data_id)
                if (
                    suspended is not None
                    and suspended["request_id"]
                    == request["request_id"]
                ):
                    suspended["persistence_drained"] = True
                    continue
                if not completed.successful() or persistence_error:
                    persistence.failure_records.append(
                        {
                            "data_id": data_id,
                            "request_id": request["request_id"],
                            "result": completed.result,
                            "exit_code": completed.exit_code,
                            "error": str(
                                persistence_error or completed.output
                            )[:2048],
                        }
                    )
                    injected_failure_observed = (
                        "DATAVINE_PERSISTENCE_INJECTED_FAILURE"
                        in str(persistence_error or completed.output)
                    )
                    if injected_failure_observed:
                        persistence.injected_failures_observed += 1
                    elif request.get("inject_failure_during_write"):
                        persistence.injected_external_failures -= 1
                    status = self.controller.idata_status(data_id)
                    if status["durability"] not in (
                        "failed",
                        "cancelled",
                        "durable",
                    ):
                        self.controller.fail_external_persistence(
                            data_id,
                            request["request_id"],
                            (
                                "worker persistence task failed: "
                                f"result={completed.result} "
                                f"exit={completed.exit_code}"
                            ),
                        )
                        status = self.controller.idata_status(data_id)
                    if status["durability"] == "durable":
                        pass
                    elif status["durability"] == "cancelled":
                        if not status["available"]:
                            continue
                        retry_status = self.controller.persist_idata(
                            data_id
                        )
                        persistence.pending.append(
                            (
                                time.monotonic(),
                                data_id,
                                retry_status["persistence_request"],
                            )
                        )
                        continue
                    elif status["durability"] == "failed":
                        persistence.failures += 1
                        if not status["available"]:
                            execution.recovery_audit_data_ids.add(data_id)
                            continue
                        retry_key = (
                            data_id,
                            int(request["attempt"]),
                        )
                        retries = persistence.retry_counts[retry_key]
                        if retries >= persistence_policy.maximum_retries:
                            raise RuntimeError(
                                f"IDataID {data_id} persistence exhausted "
                                f"{retries} retries: "
                                f"stdout={persistence_error or completed.output}"
                            )
                        delay = persistence_policy.retry_delay(retries)
                        persistence.retry_counts[retry_key] += 1
                        persistence.retries += 1
                        persistence.retry_delay_seconds += delay
                        retry_status = self.controller.persist_idata(
                            data_id
                        )
                        retry_request = retry_status[
                            "persistence_request"
                        ]
                        if (
                            persistence.injected_external_failures
                            < persistence_policy.injected_failures
                        ):
                            retry_request = {
                                **retry_request,
                                "inject_failure_during_write": True,
                                "inject_failure_delay": (
                                    persistence_policy.failure_delay_seconds
                                ),
                            }
                            persistence.injected_external_failures += 1
                        persistence.pending.append(
                            (
                                time.monotonic() + delay,
                                data_id,
                                retry_request,
                            )
                        )
                        continue
                    else:
                        current_request_id = (
                            status.get("persistence_request") or {}
                        ).get("request_id")
                        if (
                            int(status["attempt"])
                            > int(request["attempt"])
                            or (
                                current_request_id is not None
                                and current_request_id
                                != request["request_id"]
                            )
                        ):
                            continue
                        raise RuntimeError(
                            f"IDataID {data_id} persistence task failed "
                            f"without a terminal Controller state: "
                            f"request={request['request_id']} "
                            f"status={status} "
                            f"result={completed.result} "
                            f"exit={completed.exit_code} "
                            f"stdout={persistence_error or completed.output}"
                        )
                status = self.controller.idata_status(data_id)
                current_request_id = (
                    status.get("persistence_request") or {}
                ).get("request_id")
                if status["durability"] != "durable" and (
                    int(status["attempt"]) > int(request["attempt"])
                    or (
                        current_request_id is not None
                        and current_request_id != request["request_id"]
                    )
                ):
                    continue
                if status["durability"] == "cancelled":
                    if not status["available"]:
                        continue
                    retry_status = self.controller.persist_idata(
                        data_id
                    )
                    persistence.pending.append(
                        (
                            time.monotonic(),
                            data_id,
                            retry_status["persistence_request"],
                        )
                    )
                    continue
                if status["durability"] != "durable":
                    raise RuntimeError(
                        f"IDataID {data_id} persistence did not publish"
                    )
                persistence.tasks_completed += 1
                persistence.worker_bytes += int(status["size"])
                queue_frontier_pruning_if_ready(
                    producer_by_data_id[data_id]
                )
                continue
            # Cancelled attempts can arrive late after their logical task has
            # already returned to pending.
            if completed.id not in execution.running:
                continue
            logical_id = execution.running.pop(completed.id)
            if detailed_report:
                execution.physical_task_metrics.append(
                    {
                        "physical_task_id": int(completed.id),
                        **{
                            name: int(completed.get_metric(name))
                            for name in (
                                "time_when_submitted",
                                "time_when_done",
                                "time_workers_execute_last",
                                "time_workers_execute_last_start",
                                "time_workers_execute_last_end",
                                "bytes_sent",
                                "bytes_received",
                            )
                        },
                    }
                )
            if pruning.active is not None:
                pruning.completions_while_active += 1
            if persistence.running or persistence.pending:
                persistence.compute_completions_while_active += 1
            task_output = completed.output if completed.successful() else None
            structured_task = (
                task_output
                if (
                    isinstance(task_output, tuple)
                    and len(task_output) == 6
                    and task_output[0] == "datavine-task-v4"
                )
                else None
            )
            if structured_task is not None:
                (
                    _,
                    returned_task_id,
                    outputs,
                    events,
                    error,
                    diagnostics,
                ) = structured_task
                if int(returned_task_id) != logical_id:
                    raise RuntimeError(
                        "worker returned a mismatched logical TaskID"
                    )
                if diagnostics is not None:
                    (
                        worker_seconds,
                        timing_rows,
                        controller_retries,
                        cache_snapshot,
                    ) = diagnostics
                    execution.worker_seconds += float(worker_seconds)
                    for name, seconds in timing_rows:
                        execution.worker_timing_seconds[name] += float(seconds)
                    execution.worker_controller_retries += int(
                        controller_retries
                    )
                    record_worker_dram_cache(cache_snapshot)
                if error is not None:
                    reported_missing = set()
                    marker = "IData is not available: i:"
                    for line in str(error).splitlines():
                        if marker not in line:
                            continue
                        token = line.rsplit(marker, 1)[1].strip()
                        if token.isdigit():
                            reported_missing.add(int(token))
                    for data_id in reported_missing:
                        self.controller.invalidate_idata(data_id)
                    lost_inputs = sorted(reported_missing | {
                        int(data_key.split(":", 1)[1])
                        for data_key in task_cache_inputs[logical_id]
                        if data_key.startswith("i:")
                        and not self.controller.idata_status(
                            int(data_key.split(":", 1)[1])
                        )["available"]
                    })
                    if not lost_inputs and "IData is not available" not in error:
                        raise RuntimeError(
                            f"TaskID {logical_id} failed: {error}"
                        )
                    if len(execution.transient_input_failures) < 8:
                        failure = {
                            "task_id": logical_id,
                            "attempt": self._run_context.attempts[
                                logical_id
                            ],
                            "lost_inputs": lost_inputs,
                            "error": str(error)[:2048],
                            "data_events": [
                                line
                                for line in events
                                if line.startswith(
                                    "DATAVINE_PEER_"
                                )
                            ][:8],
                        }
                        execution.transient_input_failures.append(failure)
                    execution.recovery_audit_data_ids.update(lost_inputs)
                    record_pending_task_state(logical_id)
                    execution.pending.add(logical_id)
                    ready_queue.mark_pending(logical_id)
                    continue
                expected_output_ids = self._run_context.logical_output_slots[
                    logical_id
                ]
                if len(outputs) != len(expected_output_ids):
                    raise RuntimeError(
                        f"TaskID {logical_id} returned an invalid output count"
                )
                decoded_outputs = []
                for output_index, output_data_id in enumerate(
                    expected_output_ids
                ):
                    output = outputs[output_index]
                    if (
                        len(output) not in (6, 7)
                        or int(output[0]) != output_index
                    ):
                        raise RuntimeError(
                            f"TaskID {logical_id} returned mismatched output"
                        )
                    decoded_outputs.append(
                        {
                            "task_id": logical_id,
                            "output_index": output_index,
                            "data_id": f"i:{output_data_id}",
                            "attempt": self._run_context.attempts[logical_id],
                            "content_hash": str(output[1]),
                            "size": int(output[2]),
                            "worker_id": str(output[3]),
                            "worker_epoch": int(output[4]),
                            "tier": str(output[5]),
                            "replica_id": (
                                f"taskvine-{output[3]}-i-{output_data_id}"
                            ),
                            **(
                                {"source_endpoint": str(output[6])}
                                if len(output) == 7
                                else {}
                            ),
                        }
                    )
                outputs = decoded_outputs
                if self.controller.native_endpoint:
                    record_output_projection(outputs)
                for line in events:
                    if line.startswith("DATAVINE_REPLICA_OBSERVED "):
                        observation = json.loads(
                            line[len("DATAVINE_REPLICA_OBSERVED "):]
                        )
                        if observation.get("tier") == "worker-disk":
                            self._cache_admission.observe(observation)
                        record_replica_projection(observation)
                    elif line.startswith(
                        "DATAVINE_PEER_RELEASE_PENDING "
                    ):
                        release = json.loads(
                            line[len("DATAVINE_PEER_RELEASE_PENDING "):]
                        )
                        peer_release_pending.append(
                            {
                                "lease_id": release["lease_id"],
                                "not_before": (
                                    time.monotonic()
                                    + peer_release_retry_seconds
                                ),
                            }
                        )
                execution.local_idata_hits += sum(
                    line.count("DATAVINE_LOCAL_IDATA") for line in events
                )
                execution.peer_idata_fetches += sum(
                    line.startswith("DATAVINE_PEER_FETCH i:")
                    for line in events
                )
                for output in outputs:
                    if (
                        output.get("worker_id")
                        and output.get("tier") == "worker-disk"
                    ):
                        self._cache_admission.observe(output)
                queue_task_persistence(logical_id, expected_output_ids)
                record_completed_task_state(logical_id)
                execution.done.add(logical_id)
                ready_queue.mark_done(logical_id, execution.pending)
                if logical_id not in execution.completed_once:
                    execution.completed_once.add(logical_id)
                    for data_key in task_cache_inputs[logical_id]:
                        remaining_cache_uses[data_key] -= 1
                maybe_inject_worker_loss(logical_id)
                continue
            if not completed.successful():
                # A worker can disappear after its replica was selected but
                # before a dependent task starts. Reconcile first, then turn
                # a globally lost input into ordinary logical recomputation
                # instead of making the failed consumer terminal.
                self._sync_worker_epochs(force=True)
                expected_output_ids = self._run_context.logical_output_slots[
                    logical_id
                ]
                published_output_ids = [
                    data_id
                    for data_id in expected_output_ids
                    if (
                        self.controller.idata_status(data_id)[
                            "attempt"
                        ] == self._run_context.attempts[logical_id]
                        and self.controller.idata_status(data_id)[
                            "content_hash"
                        ]
                        is not None
                    )
                ]
                if (
                    published_output_ids
                    and len(published_output_ids)
                    < len(expected_output_ids)
                ):
                    for output_data_id in expected_output_ids:
                        self.controller.invalidate_idata(
                            output_data_id
                        )
                    record_pending_task_state(logical_id)
                    execution.pending.add(logical_id)
                    ready_queue.mark_pending(logical_id)
                    continue
                if len(published_output_ids) == len(expected_output_ids):
                    queue_task_persistence(
                        logical_id, expected_output_ids
                    )
                    record_completed_task_state(logical_id)
                    execution.done.add(logical_id)
                    ready_queue.mark_done(logical_id, execution.pending)
                    if logical_id not in execution.completed_once:
                        execution.completed_once.add(logical_id)
                        for data_key in task_cache_inputs[logical_id]:
                            remaining_cache_uses[data_key] -= 1
                    continue
                lost_inputs = []
                for data_key in task_cache_inputs[logical_id]:
                    if not data_key.startswith("i:"):
                        continue
                    input_data_id = int(data_key.split(":", 1)[1])
                    if not self.controller.idata_status(input_data_id)[
                        "available"
                    ]:
                        lost_inputs.append(input_data_id)
                if lost_inputs:
                    execution.recovery_audit_data_ids.update(lost_inputs)
                    record_pending_task_state(logical_id)
                    execution.pending.add(logical_id)
                    ready_queue.mark_pending(logical_id)
                    continue
                if (
                    self._run_context.attempts[logical_id]
                    < self._compute_attempt_limit
                ):
                    record_pending_task_state(logical_id)
                    execution.pending.add(logical_id)
                    ready_queue.mark_pending(logical_id)
                    execution.recovery_reexecutions += 1
                    continue
                record_pending_task_state(logical_id)
                raise RuntimeError(
                    f"TaskID {logical_id} failed: result={completed.result} "
                    f"exit={completed.exit_code} "
                    f"stdout={completed.std_output!r}"
                )
            raise RuntimeError(
                f"TaskID {logical_id} returned an invalid DataVine result "
                f"{completed.std_output!r}"
            )
        flush_output_projections()
        flush_completed_task_states()
        workflow_execution_elapsed = (
            time.monotonic() - workflow_execution_started
        )
        result_values = {
            task_id: (
                self._load_result(output_data_ids[0])
                if len(output_data_ids) == 1
                else tuple(
                    self._load_result(data_id)
                    for data_id in output_data_ids
                )
            )
            for task_id, output_data_ids
            in self._run_context.logical_output_slots.items()
            if task_id in result_task_ids
        }
        self._cache_admission.enforce(
            effective_retention_bytes,
            effective_retention_items,
            remaining_cache_uses,
            (),
        )
        if not self._cache_admission.within_capacity(
            worker_disk_cache_bytes, worker_disk_cache_items
        ):
            raise RuntimeError("worker cache cannot satisfy its capacity")
        if persist_outputs:
            for output_data_id in sorted(persistence.required):
                self._wait_durable(output_data_id)
        if hard_delete_pruned_sharedfs and pruning.events:
            if frontier_pruning_grace_seconds and self._stop_requested.wait(
                frontier_pruning_grace_seconds
            ):
                self._raise_if_stopping()
            for delete_retry in range(3):
                plan = self.controller.pruning_plan()
                try:
                    pruning.sharedfs_delete = (
                        self.controller.hard_delete_quarantined(
                            plan["records"][0]["graph_revision"],
                            plan["records"][0]["state_revision"],
                        )
                    )
                    break
                except DataVineRemoteError as exc:
                    if (
                        "quarantine proof revision changed" not in str(exc)
                        or delete_retry == 2
                    ):
                        raise
        if (
            defer_peer_source_loss_after_bytes
            and not peer_transfer_pruning_probe_triggered
        ):
            raise RuntimeError(
                "deferred peer source loss never reached its "
                "positive-byte pruning probe: "
                f"{self.controller.transfer_fault_stats()}"
            )
        peer_release_drain_started = time.monotonic()
        peer_release_drain_iterations = 0
        peer_release_drain_deadline = (
            peer_release_drain_started
            + max(30.0, peer_release_retry_seconds + 30.0)
        )
        while True:
            self._raise_if_stopping()
            drain_peer_releases()
            peer_faults = self.controller.transfer_fault_stats()
            if peer_faults["peer_release_pending"] == 0:
                break
            if time.monotonic() >= peer_release_drain_deadline:
                raise TimeoutError(
                    "DataVine peer lease release obligations did not "
                    f"drain: {peer_faults}"
                )
            self._manager.wait(1)
            self._sync_worker_epochs()
            peer_release_drain_iterations += 1
        peer_release_drain_seconds = (
            time.monotonic() - peer_release_drain_started
        )
        self._manager._refresh_stats()
        manager_stats = self._manager.stats
        report_task_ids, report_data_ids = select_report_scope(
            task_by_id,
            producer_by_data_id,
            result_task_ids,
            self._run_context.logical_output_slots,
            detailed_report,
        )
        final_output_status = {
            int(status["data_id"]): status
            for status in self.controller.idata_status_batch(
                report_data_ids
            )
        }
        controller_request_metrics = (
            self.controller.request_metrics()
            if hasattr(self.controller, "request_metrics")
            else None
        )
        registration_timing = dict(
            self._run_context.registration_timing
        )
        workflow_timing = {
            "registration": workflow_registration_elapsed,
            "execution_loop": workflow_execution_elapsed,
            "reporting_and_cleanup": (
                time.monotonic()
                - workflow_run_started
                - workflow_registration_elapsed
                - workflow_execution_elapsed
            ),
        }
        self._last_run_report = {
            "logical_tasks": len(output_ids),
            "execution_boundary": "persistent-library",
            **format_logical_outputs(
                self._run_context.logical_output_slots,
                self._run_context.attempts,
                final_output_status,
                report_task_ids,
                report_data_ids,
                detailed_report,
            ),
            "physical_attempts": sum(self._run_context.attempts.values()),
            "physical_compute_submissions": execution.physical_submissions,
            "compute_submission_window": compute_submission_window,
            "physical_task_build_seconds": (
                execution.physical_task_build_seconds
            ),
            "physical_task_submit_seconds": (
                execution.physical_task_submit_seconds
            ),
            "logical_tasks_per_physical_submission": (
                sum(self._run_context.attempts.values()) / execution.physical_submissions
                if execution.physical_submissions
                else 0
            ),
            "worker_seconds": execution.worker_seconds,
            "worker_timing_seconds": dict(
                execution.worker_timing_seconds
            ),
            "physical_task_metrics": execution.physical_task_metrics,
            "workflow_timing_seconds": workflow_timing,
            "registration_timing_seconds": registration_timing,
            "performance_bottlenecks": rank_bottlenecks(
                workflow_timing,
                registration_timing,
                controller_request_metrics,
                worker_timing=execution.worker_timing_seconds,
                manager_timing_us=format_manager_metrics(manager_stats)[
                    "manager_timing_us"
                ],
                task_count=len(output_ids),
            ),
            "recovery_reexecutions": execution.recovery_reexecutions,
            "unavailable_input_recoveries": (
                execution.unavailable_input_recoveries
            ),
            "transient_input_failures": (
                execution.transient_input_failures
            ),
            "recovery_waves": execution.recovery_waves,
            "loss_injected": execution.loss_injected,
            "local_idata_hits": execution.local_idata_hits,
            "peer_idata_fetches": execution.peer_idata_fetches,
            "worker_controller_retries": execution.worker_controller_retries,
            "control_event_pipeline": control_pipeline,
            "worker_dram_cache": {
                "workers": execution.worker_dram_cache,
                **{
                    key: sum(
                        int(value[key])
                        for value in execution.worker_dram_cache.values()
                    )
                    for key in (
                        "bytes",
                        "items",
                        "hits",
                        "misses",
                        "admissions",
                        "evictions",
                    )
                },
                "capacity_bytes_per_worker": worker_dram_cache_bytes,
            },
            "worker_loss_injected": bool(execution.worker_loss_injections),
            "worker_loss_injections": execution.worker_loss_injections,
            "worker_loss_schedule": list(worker_loss_schedule),
            "worker_loss_events": execution.worker_loss_events,
            "worker_loss_process_shutdown": bool(
                worker_loss_process_shutdown
            ),
            "peer_source_losses_requested": peer_source_losses,
            "peer_source_loss_after_bytes_requested": (
                peer_source_loss_after_bytes
            ),
            "deferred_peer_source_loss_after_bytes": (
                defer_peer_source_loss_after_bytes
            ),
            "peer_transfer_pruning_probe_task_ids": list(
                peer_transfer_pruning_probe_task_ids
            ),
            "peer_transfer_pruning_probes": (
                peer_transfer_pruning_probes
            ),
            "peer_corruptions_requested": peer_corruptions,
            "idata_release_failures_requested": (
                idata_release_failures
            ),
            "peer_release_retry_seconds": peer_release_retry_seconds,
            "peer_release_capacity": peer_release_capacity,
            "peer_release_drain_iterations": (
                peer_release_drain_iterations
            ),
            "peer_release_drain_seconds": peer_release_drain_seconds,
            "peer_transfer_faults": (
                self.controller.transfer_fault_stats()
            ),
            "worker_loss_data_by_task": {
                str(task_id): list(data_task_ids)
                for task_id, data_task_ids
                in inject_worker_loss_data_by_task.items()
            },
            "persistence_required_data_ids": sorted(
                persistence.requested
            ),
            "persistence_outstanding_data_ids": sorted(
                persistence.required
            ),
            "frontier_pruning": pruning.events,
            "frontier_pruning_grace_seconds": (
                frontier_pruning_grace_seconds
            ),
            "sharedfs_hard_delete": pruning.sharedfs_delete,
            "compute_completions_while_frontier_pruning": (
                pruning.completions_while_active
            ),
            "runtime_pruned_data_ids": sorted(
                {
                    data_id
                    for event in pruning.events
                    for data_id in event["data_ids"]
                    if data_id
                    not in event["cancelled_data_ids"]
                }
            ),
            "persistence_tasks_completed": (
                persistence.tasks_completed
            ),
            "persistence_controller_tasks_completed": (
                persistence.controller_tasks_completed
            ),
            "persistence_worker_bytes": persistence.worker_bytes,
            "persistence_controller_bytes": (
                persistence.controller_bytes
            ),
            "persistence_cancellations": persistence.cancellations,
            "persistence_failures": persistence.failures,
            "persistence_failure_records": list(
                persistence.failure_records
            ),
            "persistence_injected_failures_observed": (
                persistence.injected_failures_observed
            ),
            "persistence_retries": persistence.retries,
            "persistence_retry_delay_seconds": (
                persistence.retry_delay_seconds
            ),
            "compute_completions_while_persistence_active": (
                persistence.compute_completions_while_active
            ),
            "persistence_global_losses": persistence.global_losses,
            "persistence_loss_pruning_plans": (
                persistence.loss_pruning_plans
            ),
            "edata_serializations": self._run_context.serialization_count,
            "bulk_edata_serializations": self._run_context.bulk_serialization_count,
            "worker_reconciliation_deferrals": (
                self._worker_reconciliation_deferrals
                - reconciliation_deferrals_before
            ),
            "worker_reconciliations": self._worker_reconciliations,
            "worker_status_polls": self._worker_status_polls,
            **format_manager_metrics(manager_stats),
            "scheduler_controller_requests": controller_request_metrics,
            "scheduler_controller_retries": (
                self.controller.transient_retry_count
                if hasattr(self.controller, "transient_retry_count")
                else None
            ),
            "worker_disk_cache_admission_items": (
                worker_disk_cache_admission_items
            ),
            "worker_disk_cache_max_task_items": max_task_cache_items,
            "worker_disk_cache_admission_bytes": (
                worker_disk_cache_admission_bytes
            ),
            "worker_disk_cache_max_known_task_input_bytes": (
                max_task_known_cache_bytes
            ),
            "worker_disk_cache_effective_retention_bytes": (
                effective_retention_bytes
            ),
            "worker_disk_cache_effective_retention_items": (
                effective_retention_items
            ),
            **self._cache_admission.report(
                worker_disk_cache_bytes,
                worker_disk_cache_items,
            ),
        }
        return result_values

    def _load_result(self, data_id):
        status = self.controller.idata_status(data_id)
        if status["controller_inline"]:
            payload = self.controller.fetch_idata(data_id)
        elif status["durability"] == "durable":
            path = Path(status["durable_path"])
            payload = path.read_bytes()
            if (
                len(payload) != status["size"]
                or hashlib.sha256(payload).hexdigest()
                != status["content_hash"]
            ):
                raise IOError(
                    f"durable result IDataID {data_id} is corrupt"
                )
        else:
            sources = self.controller.replica_sources(
                f"i:{data_id}"
            )["sources"]
            payload = None
            failures = []
            for source in sources:
                source_url = source.get("source_url")
                if not source_url:
                    continue
                try:
                    payload = self.controller.fetch_source(
                        source_url,
                        status["content_hash"],
                        status["size"],
                    )
                    break
                except Exception as exc:
                    failures.append(
                        f"{source.get('worker_id')}:"
                        f"{type(exc).__name__}:{exc}"
                    )
            if payload is None:
                raise RuntimeError(
                    f"IDataID {data_id} has no readable result replica: "
                    + "; ".join(failures[:8])
                )
        return cloudpickle.loads(payload)

    def _wait_durable(self, data_id, timeout=60, retries=1):
        deadline = time.monotonic() + timeout
        while True:
            self._raise_if_stopping()
            status = self.controller.idata_status(data_id)
            if status["durability"] == "durable":
                return status
            if status["durability"] == "failed":
                if retries > 0:
                    retries -= 1
                    self.controller.persist_idata(data_id)
                    continue
                raise RuntimeError(
                    f"IDataID {data_id} persistence failed: "
                    f"{status['persistence_error']}"
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"IDataID {data_id} persistence did not complete"
                )
            time.sleep(0.05)
