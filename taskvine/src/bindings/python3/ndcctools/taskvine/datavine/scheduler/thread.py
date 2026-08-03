"""Single-owner Task Scheduler thread."""

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
from ..placement.policy import PrefetchCandidate, select_prefetch
from ..workflow import iter_output_refs
from ..protocol import DataVineRemoteError
from .configuration import configure_runtime
from .execution_state import (
    ExecutionState,
    PersistenceState,
    PruningState,
    PublicationState,
)
from .persistence import PersistencePolicy
from .readiness import build_cache_plan, plan_ready_batches, select_ready_tasks
from .recovery import select_recovery_audit_data_ids
from .registration import WorkflowRegistrar
from .reporting import (
    format_logical_outputs,
    format_manager_metrics,
    format_worker_caches,
    select_report_scope,
)
from .run_context import WorkflowRunContext
from .task_factory import TaskFactory, ensure_worker_library


class _LogicalCompletion:
    """Per-logical-task view of a batched physical completion."""

    def __init__(self, physical, output):
        self.id = physical.id
        self.output = output
        self.result = physical.result
        self.exit_code = physical.exit_code
        self._successful = (
            physical.successful()
            and "DATAVINE_TASK_FAILURE " not in output
        )

    def successful(self):
        return self._successful


class TaskSchedulerThread:
    _registration_batch_size = 4096

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
            name="datavine-task-scheduler",
            daemon=False,
        )
        self._owner_ident = None
        self._started = False
        self._ready = threading.Event()
        self._stop_requested = threading.Event()
        self._manager = None
        self._run_context = WorkflowRunContext()
        self._edata_files = {}
        self._idata_files = {}
        self._last_run_report = {}
        self._worker_reconciliation_deferrals = 0
        self._active_worker_ids = frozenset()
        self._worker_reconciliations = 0
        self._worker_status_polls = 0
        self._last_worker_status_poll = 0.0
        self._worker_status_poll_interval = 1.0
        self._reconciled_affected_data_ids = ()
        self._cache_admission = WorkerCacheAdmission(controller_client)

    @property
    def thread_ident(self):
        return self._owner_ident

    def start(self):
        if self._started:
            raise RuntimeError("Task Scheduler already started")
        self._started = True
        self._thread.start()
        if not self._ready.wait(timeout=10):
            raise RuntimeError("Task Scheduler thread did not start")
        return self

    def call(self, operation, *args, **kwargs):
        return self.submit(operation, *args, **kwargs).result()

    def submit(self, operation, *args, **kwargs):
        if not self._started:
            raise RuntimeError("Task Scheduler is not started")
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
                "Task Scheduler operation did not stop within 30 seconds"
            ) from exc
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            raise RuntimeError("Task Scheduler thread did not stop")
        self._started = False

    def _raise_if_stopping(self):
        if self._stop_requested.is_set():
            raise concurrent.futures.CancelledError(
                "Task Scheduler stop requested"
            )

    def _run(self):
        self._owner_ident = threading.get_ident()
        self._ready.set()
        while True:
            operation, args, kwargs, future = self._commands.get()
            if operation == "_stop":
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
                "Task Scheduler state mutation outside scheduler thread"
            )

    @staticmethod
    def _logical_outputs_from_batch(output, task_ids):
        task_ids = tuple(task_ids)
        if (
            len(task_ids) == 1
            or not isinstance(output, str)
            or "DATAVINE_TASK_BEGIN " not in output
        ):
            return {task_id: output for task_id in task_ids}
        selected = {task_id: [] for task_id in task_ids}
        active = None
        for line in output.splitlines():
            if line.startswith("DATAVINE_TASK_BEGIN "):
                active = int(line.rsplit(" ", 1)[1])
                continue
            if line.startswith("DATAVINE_TASK_END "):
                active = None
                continue
            if active in selected:
                selected[active].append(line)
        return {
            task_id: "\n".join(lines) + ("\n" if lines else "")
            for task_id, lines in selected.items()
        }

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
        from ndcctools.taskvine import FunctionCall

        task = FunctionCall(
            "datavine-worker-v2",
            "execute_datavine_tasks",
            self.controller.endpoint,
            self.controller.token,
            (),
            0,
        )
        task.set_exec_method("direct")
        task_id = self._manager.submit(task)
        while True:
            self._raise_if_stopping()
            completed = self._manager.wait(1)
            if completed is None or completed.id != task_id:
                continue
            if not completed.successful():
                raise RuntimeError(
                    "DataVine worker library warmup failed: "
                    f"{completed.output!r}"
                )
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

    def _file_for_data_key(self, data_key):
        kind, token = str(data_key).split(":", 1)
        data_id = int(token)
        if kind == "e":
            return self._edata_files.get(data_id)
        if kind == "i":
            return self._idata_files.get(data_id)
        raise ValueError(f"unknown qualified DataID {data_key!r}")

    def _op_last_run_report(self):
        self._assert_owner()
        return dict(self._last_run_report)

    def _op_apply_pruning(
        self,
        grace_seconds=60,
        data_ids=None,
        now=None,
        acknowledgement_timeout=30,
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
            file_object = self._idata_files.get(data_id)
            if file_object is None:
                raise RuntimeError(
                    f"no TaskVine file for local prune i:{data_id}"
                )
            before = self._manager.prune_file_status(file_object)
            requested = self._manager.prune_file(file_object)
            if requested != len(records):
                raise RuntimeError(
                    f"replica authority mismatch for i:{data_id}: "
                    f"Controller={len(records)} TaskVine={requested}"
                )
            deadline = time.monotonic() + float(
                acknowledgement_timeout
            )
            while True:
                self._raise_if_stopping()
                self._manager.wait(1)
                self._sync_worker_epochs()
                status = self._manager.prune_file_status(file_object)
                confirmed = (
                    status["confirmed"] - before["confirmed"]
                )
                failed = status["failed"] - before["failed"]
                if confirmed + failed >= requested:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"worker prune acknowledgement timed out "
                        f"for i:{data_id}"
                    )
            if failed:
                raise RuntimeError(
                    f"{failed} worker prune operations failed "
                    f"for i:{data_id}"
                )
            for record in records:
                self.controller.confirm_replica_pruned(
                    f"i:{data_id}",
                    record["replica_id"],
                    record["generation"],
                )
            worker_prunes.append(
                {
                    "data_id": data_id,
                    "requested": requested,
                    "confirmed": confirmed,
                    "failed": failed,
                }
            )
            if not self._manager.forget_prune_file_status(file_object):
                raise RuntimeError(
                    f"could not release completed prune state "
                    f"for i:{data_id}"
                )
            if any(
                self._manager.prune_file_status(file_object).values()
            ):
                raise RuntimeError(
                    f"completed prune state leaked for i:{data_id}"
                )
            worker_prunes[-1]["tracker_released"] = True
        return {
            "controller": result,
            "worker_prunes": worker_prunes,
        }

    def _op_create_manager(
        self, port=0, name=None, run_info_path=None, peer_transfers=True
    ):
        self._assert_owner()
        if self._manager is not None:
            raise RuntimeError("TaskVine Manager already exists")
        from ndcctools.taskvine import Manager, cvine

        kwargs = {"port": port, "name": name}
        if run_info_path is not None:
            kwargs["run_info_path"] = run_info_path
        self._manager = Manager(**kwargs)
        debug_file = os.environ.get("DATAVINE_MANAGER_DEBUG_FILE")
        if debug_file and not cvine.vine_enable_debug_log(debug_file):
            raise RuntimeError("could not enable TaskVine Manager debug log")
        if os.environ.get("DATAVINE_WATCH_LIBRARY_LOGFILES"):
            if self._manager.tune("watch-library-logfiles", 1) < 0:
                raise RuntimeError(
                    "TaskVine Manager rejected library log collection"
                )
        if not self._manager.set_datavine_controller(
            self.controller.endpoint, self.controller.token
        ):
            raise RuntimeError(
                "TaskVine Manager rejected Data Controller configuration"
            )
        if peer_transfers:
            self._manager.enable_peer_transfers()
        else:
            self._manager.disable_peer_transfers()
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
        prefetch=True,
        prefetch_byte_budget=64 * 1024 * 1024,
        prefetch_item_budget=16,
        inject_prefetch_failure=False,
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
        inject_partial_publication_after=None,
        frontier_pruning_ack_delay=0,
        inject_peer_source_losses=0,
        inject_peer_source_loss_after_bytes=0,
        defer_peer_source_loss_after_bytes=False,
        peer_transfer_pruning_probe_task_ids=(),
        inject_peer_corruptions=0,
        inject_idata_release_failures=0,
        peer_release_retry_seconds=0.1,
        peer_release_capacity=1024,
        use_worker_library=False,
        frontier_pruning_grace_seconds=30,
        hard_delete_pruned_sharedfs=False,
        library_batch_size=4096,
        detailed_report=False,
    ):
        self._assert_owner()
        worker_dram_cache_bytes = int(worker_dram_cache_bytes)
        if worker_dram_cache_bytes < 0:
            raise ValueError("worker DRAM cache capacity is negative")
        workflow_run_started = time.monotonic()
        if self._manager is None:
            raise RuntimeError("create_manager must be called first")
        if use_worker_library:
            ensure_worker_library(self._manager)
        reconciliation_deferrals_before = (
            self._worker_reconciliation_deferrals
        )
        tuning = configure_runtime(
            self._manager,
            library_batch_size=library_batch_size,
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
        library_batch_size = tuning.library_batch_size
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
        controller_snapshot = self.controller.snapshot()
        output_ids = self._op_register_workflow(workflow)
        task_factory = TaskFactory(
            self._manager,
            self.controller,
            self._run_context,
            self._task_record,
            self._edata_files,
            self._idata_files,
            worker_dram_cache_bytes,
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
        inject_partial_publication_after = {
            int(task_id): int(output_index)
            for task_id, output_index in (
                inject_partial_publication_after or {}
            ).items()
        }
        for task_id, output_index in (
            inject_partial_publication_after.items()
        ):
            if task_id not in self._run_context.logical_output_slots:
                raise KeyError(
                    f"unknown partial-publication TaskID {task_id}"
                )
            output_count = len(self._run_context.logical_output_slots[task_id])
            if output_index < 0 or output_index >= output_count - 1:
                raise ValueError(
                    "partial-publication fault must follow a "
                    "non-final output slot"
                )
        if result_task_ids is None:
            result_task_ids = tuple(output_ids)
        else:
            result_task_ids = tuple(int(value) for value in result_task_ids)
            unknown_results = set(result_task_ids) - set(output_ids)
            if unknown_results:
                raise KeyError(
                    f"unknown result TaskIDs {sorted(unknown_results)}"
                )
        required_result_data_ids = {
            data_id
            for task_id in result_task_ids
            for data_id in self._run_context.logical_output_slots[task_id]
        }
        task_by_id = {task.task_id: task for task in workflow.tasks}
        execution = ExecutionState(pending=set(task_by_id))

        def record_worker_dram_cache(lines):
            for line in lines:
                if not line.startswith("DATAVINE_DRAM_CACHE "):
                    continue
                value = json.loads(line.split(" ", 1)[1])
                worker_id = value.pop("worker_id")
                if not worker_id:
                    raise RuntimeError("DRAM cache report lacks WorkerID")
                execution.worker_dram_cache[worker_id] = value

        publication = PublicationState()
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
            else:
                metadata = self.controller.idata_status(int(token))
                return metadata["size"]

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
        cache_known_sizes = cache_plan.known_sizes
        max_task_cache_items = cache_plan.max_task_items
        max_task_known_cache_bytes = cache_plan.max_known_input_bytes
        effective_retention_items = cache_plan.retention_items
        effective_retention_bytes = cache_plan.retention_bytes
        effective_library_batch_size = (
            library_batch_size
            if (
                use_worker_library
                and not persist_outputs
                and inject_global_loss_after is None
                and not worker_loss_schedule
                and not inject_partial_publication_after
                and not peer_source_losses
                and not peer_source_loss_after_bytes
                and not peer_corruptions
                and not idata_release_failures
            )
            else 1
        )
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
            existing = {
                entry["data_id"]
                for entry in active["worker_entries"]
            }
            for data_id, data_records in sorted(
                pending_by_data.items()
            ):
                if data_id in existing:
                    raise RuntimeError(
                        "duplicate physical frontier-prune request "
                        f"for i:{data_id}"
                    )
                file_object = self._idata_files[data_id]
                self._sync_worker_epochs(force=True)
                active_records = [
                    record
                    for record in data_records
                    if record.get("worker_id") in self._active_worker_ids
                ]
                inactive_records = [
                    record
                    for record in data_records
                    if record not in active_records
                ]
                for record in inactive_records:
                    self.controller.confirm_replica_pruned(
                        f"i:{data_id}",
                        record["replica_id"],
                        record["generation"],
                    )
                reconciled = len(inactive_records)
                before = self._manager.prune_file_status(
                    file_object
                )
                pending_records = []
                missing_records = []
                for record in active_records:
                    requested = self._manager.prune_file_on_worker(
                        file_object, record["worker_id"]
                    )
                    if requested == 1:
                        pending_records.append(record)
                    elif requested == 0:
                        self.controller.confirm_replica_pruned(
                            f"i:{data_id}",
                            record["replica_id"],
                            record["generation"],
                        )
                        missing_records.append(record)
                    else:
                        raise RuntimeError(
                            "worker-specific prune returned invalid count "
                            f"{requested} for i:{data_id} on "
                            f"{record['worker_id']}"
                        )
                reconciled += len(missing_records)
                requested = len(pending_records)
                if not requested:
                    active["reconciled_worker_prunes"].append(
                        {
                            "data_id": data_id,
                            "requested": 0,
                            "confirmed": 0,
                            "failed": 0,
                            "reconciled": reconciled,
                            "tracker_released": True,
                        }
                    )
                    continue
                active["worker_entries"].append(
                    {
                        "data_id": data_id,
                        "records": pending_records,
                        "file": file_object,
                        "before": before,
                        "requested": requested,
                        "reconciled": reconciled,
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
        prefetch_running = set()
        prefetch_selected = self._submit_prefetches(
            task_factory,
            prefetch,
            prefetch_byte_budget,
            prefetch_item_budget,
            inject_prefetch_failure,
        )
        prefetch_running.update(
            value["physical_task_id"] for value in prefetch_selected
        )
        prefetch_completed = 0
        prefetch_failed = 0
        prefetch_overlapped = False
        peer_transfer_pruning_probes = []
        peer_transfer_pruning_probe_triggered = False

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

        workflow_execution_started = time.monotonic()
        while (
            execution.has_work()
            or prefetch_running
            or persistence.has_work()
            or pruning.has_work()
        ):
            self._raise_if_stopping()
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
                self._idata_files[data_id] = task_factory.durable_idata_file(
                    data_id, status
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
                status = self._manager.prune_file_status(
                    recovery["file"]
                )
                confirmed = (
                    status["confirmed"]
                    - recovery["before"]["confirmed"]
                )
                failed = (
                    status["failed"] - recovery["before"]["failed"]
                )
                if failed:
                    raise RuntimeError(
                        f"physical prune failed before recovery "
                        f"of i:{data_id}"
                    )
                if confirmed < recovery["requested"]:
                    continue
                if (
                    recovery["requested"]
                    and not self._manager.forget_prune_file_status(
                        recovery["file"]
                    )
                ):
                    raise RuntimeError(
                        f"could not release prune barrier for i:{data_id}"
                    )
                execution.pending.add(recovery["logical_id"])
                persistence.suspended_recovery.pop(data_id)
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
                self._manager.prune_file(
                    self._idata_files[data_id]
                )
                self.controller.set_task_state(task_id, "pending")
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
                recovered_inputs = {
                    f"i:{data_id}"
                    for task_id in recovery_wave
                    for data_id in self._run_context.logical_output_slots[
                        task_id
                    ]
                }
                for physical_id, logical_ids in tuple(
                    execution.running.items()
                ):
                    if not any(
                        task_cache_inputs[logical_id] & recovered_inputs
                        for logical_id in logical_ids
                    ):
                        continue
                    if not self._manager.cancel_by_task_id(physical_id):
                        continue
                    execution.running.pop(physical_id)
                    for logical_id in logical_ids:
                        execution.pending.add(logical_id)
                        self.controller.set_task_state(
                            logical_id, "pending"
                        )
                        for output_data_id in (
                            self._run_context.logical_output_slots[
                                logical_id
                            ]
                        ):
                            self.controller.invalidate_idata(
                                output_data_id
                            )
                plan = self.controller.pruning_plan()
                execution.recovery_waves.append(
                    {
                        "tasks": recovery_wave,
                        "rollback_depth": len(recovery_wave),
                        "recovery_depths": plan["recovery_depths"],
                    }
                )

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
                for entry in pruning.active["worker_entries"]:
                    status = self._manager.prune_file_status(
                        entry["file"]
                    )
                    confirmed = (
                        status["confirmed"]
                        - entry["before"]["confirmed"]
                    )
                    failed = (
                        status["failed"] - entry["before"]["failed"]
                    )
                    if failed:
                        raise RuntimeError(
                            f"{failed} asynchronous worker prune "
                            f"operations failed for i:{entry['data_id']}"
                        )
                    if confirmed < entry["requested"]:
                        all_complete = False
                        continue
                    worker_prunes.append(
                        {
                            "data_id": entry["data_id"],
                            "requested": entry["requested"],
                            "confirmed": confirmed,
                            "failed": failed,
                            "reconciled": entry["reconciled"],
                        }
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
                    for entry in pruning.active[
                        "worker_entries"
                    ]:
                        for record in entry["records"]:
                            self.controller.confirm_replica_pruned(
                                f"i:{entry['data_id']}",
                                record["replica_id"],
                                record["generation"],
                            )
                        if not self._manager.forget_prune_file_status(
                            entry["file"]
                        ):
                            raise RuntimeError(
                                "could not release asynchronous prune "
                                f"tracker for i:{entry['data_id']}"
                            )
                        if any(
                            self._manager.prune_file_status(
                                entry["file"]
                            ).values()
                        ):
                            raise RuntimeError(
                                "asynchronous prune tracker leaked for "
                                f"i:{entry['data_id']}"
                            )
                        next(
                            item
                            for item in worker_prunes
                            if item["data_id"] == entry["data_id"]
                        )["tracker_released"] = True
                    frontier_task_id = pruning.active[
                        "frontier_task_id"
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
                    pruning.applied.add(frontier_task_id)
                    pruning.active = None

            if (
                pruning.active is None
                and pruning.pending
            ):
                active_inputs = {
                    data_key
                    for logical_ids in execution.running.values()
                    for running_logical_id in logical_ids
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
                        {f"i:{data_id}" for data_id in data_ids}
                        & self._cache_admission.prune_by_data
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
                    prune_data_ids = pruning.pending.pop(
                        frontier_task_id
                    )
                    # Worker reconciliation and recovery can advance the
                    # Controller proof between the read and POST.  Retry
                    # only this optimistic revision check with a fresh proof;
                    # never apply a stale pruning decision.
                    for pruning_retry in range(3):
                        plan = self.controller.pruning_plan()
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
                                "pruning proof revision changed"
                                not in str(exc)
                                or pruning_retry == 2
                            ):
                                raise
                    now_value = time.monotonic()
                    pruning.active = {
                        "frontier_task_id": frontier_task_id,
                        "data_ids": prune_data_ids,
                        "controller_result": result,
                        "controller_continuations": [],
                        "controller_continuation_retries": [],
                        "continuation_operation_id": None,
                        "deferred_data_ids": {
                            item["data_id"]
                            for item in result["deferred"]
                        },
                        "cancelled_data_ids": set(),
                        "worker_entries": [],
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

            ready = select_ready_tasks(
                execution.pending,
                dependencies,
                execution.done,
                task_cache_inputs,
                self._cache_admission.prune_by_data,
                persistence_frontier_ready,
            )
            connected_library_slots = max(
                1, len(self._active_worker_ids) * 4
            )
            ready_batches = plan_ready_batches(
                ready,
                execution.unbatchable,
                task_cache_inputs,
                cache_known_sizes,
                effective_library_batch_size,
                connected_library_slots,
            )
            for task_ids in ready_batches:
                attempts = []
                for task_id in task_ids:
                    attempt = self._run_context.attempts.get(task_id, 0) + 1
                    self._run_context.attempts[task_id] = attempt
                    attempts.append(attempt)
                partial_output_index = (
                    inject_partial_publication_after.get(task_ids[0])
                    if len(task_ids) == 1 and attempts[0] == 1
                    else None
                )
                task_build_started = time.monotonic()
                if partial_output_index is not None:
                    physical = task_factory.make_physical_task(
                        task_ids[0],
                        environment,
                        attempts[0],
                        partial_output_index,
                        use_worker_library,
                    )
                else:
                    physical = task_factory.make_physical_batch_task(
                        task_ids,
                        environment,
                        attempts,
                        use_worker_library,
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
                execution.running[physical_id] = tuple(task_ids)
                self.controller.set_task_states(task_ids, "running")
                for task_id in task_ids:
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
                and not prefetch_running
                and not persistence.running
            ):
                if persistence.pending:
                    delay = max(
                        0,
                        persistence.pending[0][0] - time.monotonic(),
                    )
                    time.sleep(min(float(wait_timeout), delay))
                    continue
                if self._cache_admission.evictions:
                    self._manager.wait(wait_timeout)
                    self._sync_worker_epochs()
                    self._cache_admission.poll(self._manager)
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
                    or prefetch_running
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
                        "pruning_inputs": sorted(
                            task_cache_inputs[task_id]
                            & self._cache_admission.prune_by_data
                        ),
                    }
                raise RuntimeError(
                    "workflow cannot make progress: "
                    f"pending={blocked} done={sorted(execution.done)} "
                    f"frontier_pruning_pending="
                    f"{sorted(pruning.pending)}"
                )
            queued_logical_id = None
            if execution.completion_queue:
                queued_logical_id, completed = (
                    execution.completion_queue.popleft()
                )
            else:
                completed = self._manager.wait(wait_timeout)
            workers_before_sync = self._active_worker_ids
            if queued_logical_id is None:
                active_workers = frozenset(self._sync_worker_epochs())
                workers_lost = workers_before_sync - active_workers
                self._cache_admission.poll(self._manager)
            else:
                active_workers = workers_before_sync
                workers_lost = frozenset()
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
                for physical_id, logical_ids in tuple(execution.running.items()):
                    missing_inputs = []
                    for logical_id in logical_ids:
                        for input_data_id in self._task_record(
                            logical_id
                        ).input_data_ids:
                            if not self.controller.idata_status(
                                input_data_id
                            )["available"]:
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
                    for logical_id in logical_ids:
                        execution.pending.add(logical_id)
                        self.controller.set_task_state(
                            logical_id, "pending"
                        )
                        for output_data_id in self._run_context.logical_output_slots[
                            logical_id
                        ]:
                            self.controller.invalidate_idata(output_data_id)
                    execution.recovery_reexecutions += len(logical_ids)
                    execution.unavailable_input_recoveries.append(
                        {
                            "task_ids": list(logical_ids),
                            "physical_task_id": physical_id,
                            "missing_inputs": missing_inputs,
                            "attempts": [
                                self._run_context.attempts[logical_id]
                                for logical_id in logical_ids
                            ],
                        }
                    )
            if (
                defer_peer_source_loss_after_bytes
                and not peer_transfer_pruning_probe_triggered
            ):
                peer_faults = (
                    self._manager.datavine_peer_transfer_fault_stats()
                )
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
                    if self._manager.tune(
                        "datavine-trigger-deferred-peer-source-loss",
                        1,
                    ) != 0:
                        raise RuntimeError(
                            "deferred peer source loss disappeared "
                            "before explicit Scheduler trigger"
                        )
                    peer_transfer_pruning_probe_triggered = True
            if completed is None:
                for physical_id, logical_ids in tuple(execution.running.items()):
                    if len(logical_ids) != 1:
                        continue
                    logical_id = logical_ids[0]
                    output_index = inject_partial_publication_after.get(
                        logical_id
                    )
                    if (
                        output_index is None
                        or logical_id in publication.triggered_tasks
                        or self._run_context.attempts[logical_id] != 1
                    ):
                        continue
                    expected_data_ids = self._run_context.logical_output_slots[
                        logical_id
                    ]
                    published_data_id = expected_data_ids[output_index]
                    prepared = [
                        record
                        for record in self.controller.replica_records(
                            f"i:{published_data_id}"
                        )["records"]
                        if (
                            record["state"] == "preparing"
                            and record["attempt"] == 1
                            and record.get("worker_id")
                        )
                    ]
                    if not prepared:
                        continue
                    if len(prepared) != 1:
                        raise RuntimeError(
                            "partial publication has ambiguous worker "
                            "ownership"
                        )
                    worker_id = prepared[0]["worker_id"]
                    if not self._manager.cancel_by_task_id(physical_id):
                        raise RuntimeError(
                            "could not cancel partial publication task"
                        )
                    from ndcctools.taskvine import cvine
                    if not cvine.vine_manager_shut_down_worker_by_id(
                        self._manager._taskvine, worker_id
                    ):
                        raise RuntimeError(
                            "could not shut down partial publication "
                            f"worker {worker_id}"
                        )
                    for output_data_id in expected_data_ids:
                        self.controller.invalidate_idata(output_data_id)
                    publication.triggered_tasks.add(logical_id)
                    publication.cancelled_physical_tasks[physical_id] = {
                        "task_id": logical_id,
                        "attempt": 1,
                        "published_data_ids": [published_data_id],
                        "expected_data_ids": list(expected_data_ids),
                        "worker_id": worker_id,
                        "physical_task_id": physical_id,
                    }
                    break
                if publication.cancelled_physical_tasks:
                    continue
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
                        file_object = self._idata_files[data_id]
                        prune_before = (
                            self._manager.prune_file_status(file_object)
                        )
                        prune_requested = self._manager.prune_file(
                            file_object
                        )
                        self.controller.set_task_state(
                            logical_id, "pending"
                        )
                        execution.done.remove(logical_id)
                        persistence.suspended_recovery[data_id] = {
                            "logical_id": logical_id,
                            "file": file_object,
                            "before": prune_before,
                            "requested": prune_requested,
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
                self._cache_admission.enforce(
                    self._manager,
                    self._file_for_data_key,
                    effective_retention_bytes,
                    effective_retention_items,
                    remaining_cache_uses,
                {
                    data_key
                    for logical_ids in execution.running.values()
                    for logical_id in logical_ids
                    for data_key in task_cache_inputs[logical_id]
                    },
                )
                continue
            if completed.id in prefetch_running:
                prefetch_running.remove(completed.id)
                if completed.successful():
                    prefetch_completed += 1
                else:
                    prefetch_failed += 1
                if not execution.done:
                    prefetch_overlapped = True
                continue
            if completed.id in persistence.running:
                data_id, request = persistence.running.pop(completed.id)
                suspended = persistence.suspended_recovery.get(data_id)
                if (
                    suspended is not None
                    and suspended["request_id"]
                    == request["request_id"]
                ):
                    suspended["persistence_drained"] = True
                if not completed.successful():
                    if (
                        "DATAVINE_PERSISTENCE_INJECTED_FAILURE"
                        in completed.output
                    ):
                        persistence.injected_failures_observed += 1
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
                        retry_key = (
                            data_id,
                            int(request["attempt"]),
                        )
                        retries = persistence.retry_counts[retry_key]
                        if retries >= persistence_policy.maximum_retries:
                            raise RuntimeError(
                                f"IDataID {data_id} persistence exhausted "
                                f"{retries} retries: "
                                f"stdout={completed.output}"
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
                            f"stdout={completed.output}"
                        )
                status = self.controller.idata_status(data_id)
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
                self._idata_files[data_id] = task_factory.durable_idata_file(
                    data_id, status
                )
                persistence.tasks_completed += 1
                persistence.worker_bytes += int(status["size"])
                queue_frontier_pruning_if_ready(
                    producer_by_data_id[data_id]
                )
                continue
            # A physical attempt that DataVine cancelled while reclaiming an
            # unavailable input may still be returned by Manager.wait.  Its
            # logical task has already been moved back to execution.pending; late
            # completion must be ignored rather than treated as a duplicate.
            if queued_logical_id is None:
                if completed.id not in execution.running:
                    continue
                logical_ids = execution.running.pop(completed.id)
                execution.physical_batch_metrics.append(
                    {
                        "physical_task_id": int(completed.id),
                        "logical_tasks": len(logical_ids),
                        **{
                            name: int(completed.get_metric(name))
                            for name in (
                                "time_when_submitted",
                                "time_when_done",
                                "time_workers_execute_last",
                                "bytes_sent",
                                "bytes_received",
                            )
                        },
                    }
                )
                structured_batch = (
                    completed.output
                    if (
                        isinstance(completed.output, dict)
                        and completed.output.get("protocol")
                        == "datavine-batch-v2"
                    )
                    else None
                )
                if structured_batch is not None:
                    execution.batch_worker_seconds += float(
                        structured_batch["worker_seconds"]
                    )
                    returned_task_ids = tuple(
                        int(result["task_id"])
                        for result in structured_batch["tasks"]
                    )
                    if returned_task_ids != tuple(logical_ids):
                        raise RuntimeError(
                            "batch returned mismatched logical TaskIDs"
                        )
                    outputs_by_task = {}
                    outputs = []
                    batch_events = []
                    for result in structured_batch["tasks"]:
                        if result["error"] is not None:
                            raise RuntimeError(
                                "successful structured batch contained "
                                f"TaskID {result['task_id']} error "
                                f"{result['error']}"
                            )
                        batch_events.extend(result["events"])
                        task_id = int(result["task_id"])
                        task_outputs = result["outputs"]
                        outputs_by_task[task_id] = task_outputs
                        outputs.extend(task_outputs)
                    expected_output_count = sum(
                        len(self._run_context.logical_output_slots[task_id])
                        for task_id in logical_ids
                    )
                    if len(outputs) != expected_output_count:
                        raise RuntimeError(
                            "batch returned an invalid output count"
                        )
                    for logical_id in logical_ids:
                        expected_output_ids = (
                            self._run_context.logical_output_slots[logical_id]
                        )
                        task_outputs = outputs_by_task[logical_id]
                        if len(task_outputs) != len(expected_output_ids):
                            raise RuntimeError(
                                f"TaskID {logical_id} returned an invalid "
                                "output count"
                            )
                        for output_index, output_data_id in enumerate(
                            expected_output_ids
                        ):
                            output = task_outputs[output_index]
                            if (
                                int(output["task_id"]) != logical_id
                                or int(output["output_index"])
                                != output_index
                                or output["data_id"]
                                != f"i:{output_data_id}"
                                or int(output["attempt"])
                                != self._run_context.attempts[logical_id]
                            ):
                                raise RuntimeError(
                                    f"TaskID {logical_id} returned a "
                                    "mismatched output preparation"
                                )
                    committed = self.controller.commit_outputs(outputs)
                    if len(committed) != len(outputs):
                        raise RuntimeError(
                            "Controller returned incomplete output commits"
                        )
                    for line in batch_events:
                        if line.startswith("DATAVINE_REPLICA_OBSERVED "):
                            self._cache_admission.observe(
                                json.loads(
                                    line[len("DATAVINE_REPLICA_OBSERVED "):]
                                )
                            )
                    execution.local_idata_hits += sum(
                        line.count("DATAVINE_LOCAL_IDATA")
                        for line in batch_events
                    )
                    execution.worker_controller_retries += sum(
                        int(line.split(" ", 1)[1])
                        for line in batch_events
                        if line.startswith(
                            "DATAVINE_CONTROLLER_RETRIES "
                        )
                    )
                    record_worker_dram_cache(batch_events)
                    for logical_id in logical_ids:
                        for output in outputs_by_task[logical_id]:
                            self._cache_admission.observe(output)
                        queue_task_persistence(
                            logical_id,
                            self._run_context.logical_output_slots[
                                logical_id
                            ],
                        )
                        execution.deferred_completed_states.append(logical_id)
                        execution.done.add(logical_id)
                        if logical_id not in execution.completed_once:
                            execution.completed_once.add(logical_id)
                            for data_key in task_cache_inputs[logical_id]:
                                remaining_cache_uses[data_key] -= 1
                    continue
                if len(logical_ids) > 1:
                    if completed.successful():
                        raise RuntimeError(
                            "successful batch returned no v2 result"
                        )
                    for logical_id in logical_ids:
                        execution.unbatchable.add(logical_id)
                        self.controller.set_task_state(
                            logical_id, "pending"
                        )
                        execution.pending.add(logical_id)
                    continue
                logical_outputs = self._logical_outputs_from_batch(
                    completed.output, logical_ids
                )
                for batch_logical_id in logical_ids:
                    execution.completion_queue.append(
                        (
                            batch_logical_id,
                            _LogicalCompletion(
                                completed,
                                logical_outputs[batch_logical_id],
                            ),
                        )
                    )
                logical_id, completed = execution.completion_queue.popleft()
                if execution.completion_queue:
                    execution.running[completed.id] = ()
            else:
                logical_id = queued_logical_id
                if not execution.completion_queue:
                    execution.running.pop(completed.id, None)
            if pruning.active is not None:
                pruning.completions_while_active += 1
            if persistence.running or persistence.pending:
                persistence.compute_completions_while_active += 1
            completed_output = (
                completed.output
                if isinstance(completed.output, str)
                else ""
            )
            execution.local_idata_hits += completed_output.count(
                "DATAVINE_LOCAL_IDATA"
            )
            execution.worker_controller_retries += sum(
                int(line.split(" ", 1)[1])
                for line in completed_output.splitlines()
                if line.startswith("DATAVINE_CONTROLLER_RETRIES ")
            )
            record_worker_dram_cache(completed_output.splitlines())
            if not completed.successful():
                # A worker can disappear after its replica was selected but
                # before a dependent task starts. Reconcile first, then turn
                # a globally lost input into ordinary logical recomputation
                # instead of making the failed consumer terminal.
                self._sync_worker_epochs(force=True)
                partial_failure = publication.cancelled_physical_tasks.pop(
                    completed.id, None
                )
                if partial_failure is not None:
                    publication.failures.append(
                        partial_failure
                    )
                    self.controller.set_task_state(
                        logical_id, "pending"
                    )
                    execution.pending.add(logical_id)
                    continue
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
                        self._manager.prune_file(
                            self._idata_files[output_data_id]
                        )
                    publication.failures.append(
                        {
                            "task_id": logical_id,
                            "attempt": self._run_context.attempts[logical_id],
                            "published_data_ids": published_output_ids,
                            "expected_data_ids": list(
                                expected_output_ids
                            ),
                        }
                    )
                    self.controller.set_task_state(
                        logical_id, "pending"
                    )
                    execution.pending.add(logical_id)
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
                    self.controller.set_task_state(
                        logical_id, "pending"
                    )
                    execution.pending.add(logical_id)
                    continue
                self.controller.set_task_state(logical_id, "pending")
                raise RuntimeError(
                    f"TaskID {logical_id} failed: result={completed.result} "
                    f"exit={completed.exit_code} "
                    f"stdout={completed.output!r}"
                )
            if not isinstance(completed.output, str):
                raise RuntimeError(
                    f"TaskID {logical_id} returned non-text successful "
                    f"output {completed.output!r}"
                )
            observation_lines = [
                line[len("DATAVINE_REPLICA_OBSERVED "):]
                for line in completed.output.splitlines()
                if line.startswith("DATAVINE_REPLICA_OBSERVED ")
            ]
            for line in observation_lines:
                self._cache_admission.observe(json.loads(line))
            preparation_lines = [
                line[len("DATAVINE_REPLICA_PREPARED "):]
                for line in completed.output.splitlines()
                if line.startswith("DATAVINE_REPLICA_PREPARED ")
            ]
            expected_output_ids = self._run_context.logical_output_slots[logical_id]
            if len(preparation_lines) != len(expected_output_ids):
                raise RuntimeError(
                    f"TaskID {logical_id} returned "
                    f"{len(preparation_lines)} replica preparations; "
                    f"expected {len(expected_output_ids)}"
                )
            preparations = {
                int(preparation["output_index"]): preparation
                for preparation in map(json.loads, preparation_lines)
            }
            if set(preparations) != set(range(len(expected_output_ids))):
                raise RuntimeError(
                    f"TaskID {logical_id} returned invalid output slots"
                )
            for output_index, output_data_id in enumerate(
                expected_output_ids
            ):
                preparation = preparations[output_index]
                if (
                    preparation["data_id"] != f"i:{output_data_id}"
                    or preparation["attempt"]
                    != self._run_context.attempts[logical_id]
                ):
                    raise RuntimeError(
                        f"TaskID {logical_id} returned mismatched replica"
                    )
                self.controller.commit_replica(
                    preparation["data_id"],
                    preparation["replica_id"],
                    preparation["generation"],
                    preparation["attempt"],
                    preparation["content_hash"],
                    preparation["size"],
                )
                self._cache_admission.observe(preparation)
                # Every output slot must be published before the logical task
                # can complete.
                output_status = self.controller.idata_status(
                    output_data_id
                )
                if (
                    not output_status["available"]
                    or output_status["attempt"]
                    != self._run_context.attempts[logical_id]
                    or output_status["content_hash"]
                    != preparation["content_hash"]
                    or output_status["size"] != preparation["size"]
                ):
                    raise RuntimeError(
                        f"TaskID {logical_id} output {output_index} "
                        "publication is not available"
                    )
            queue_task_persistence(logical_id, expected_output_ids)
            if effective_library_batch_size > 1:
                execution.deferred_completed_states.append(logical_id)
            else:
                self.controller.set_task_state(logical_id, "completed")
            execution.done.add(logical_id)
            if logical_id not in execution.completed_once:
                execution.completed_once.add(logical_id)
                for data_key in task_cache_inputs[logical_id]:
                    remaining_cache_uses[data_key] -= 1
            if (
                inject_global_loss_after == logical_id
                and not execution.loss_injected
            ):
                self.controller.invalidate_idata(
                    output_ids[logical_id]
                )
                execution.recovery_audit_data_ids.add(output_ids[logical_id])
                execution.loss_injected = True
            if (
                execution.worker_loss_injections < len(worker_loss_schedule)
                and worker_loss_schedule[execution.worker_loss_injections]
                == logical_id
            ):
                from ndcctools.taskvine import cvine
                workers_before = sorted(
                    self._sync_worker_epochs(force=True)
                )
                if not workers_before:
                    raise RuntimeError(
                        "no worker available for loss injection"
                    )
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
                            if source.get("worker_id")
                            in workers_before
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
                        raise RuntimeError(
                            "could not release worker"
                        )
                workers_after = sorted(
                    self._sync_worker_epochs(force=True)
                )
                if worker_loss_process_shutdown:
                    disconnect_deadline = time.monotonic() + 10
                    while released_worker_id in self._active_worker_ids:
                        if time.monotonic() >= disconnect_deadline:
                            raise TimeoutError(
                                "deterministically shut down worker did not "
                                f"disconnect: {released_worker_id}"
                            )
                        self._manager.wait(1)
                        self._sync_worker_epochs(force=True)
                    workers_after = sorted(self._active_worker_ids)
                    execution.recovery_audit_data_ids.update(
                        int(data_key.split(":", 1)[1])
                        for data_key in self._reconciled_affected_data_ids
                        if str(data_key).startswith("i:")
                    )
                if not worker_loss_process_shutdown:
                    released = sorted(
                        set(workers_before) - set(workers_after)
                    )
                    released_worker_id = (
                        released[0] if len(released) == 1 else None
                    )
                lost_task_ids = inject_worker_loss_data_by_task.get(
                    logical_id, (logical_id,)
                )
                for lost_task_id in lost_task_ids:
                    self.controller.invalidate_idata(
                        output_ids[lost_task_id]
                    )
                    self._manager.prune_file(
                        self._idata_files[output_ids[lost_task_id]]
                    )
                execution.recovery_audit_data_ids.update(
                    output_ids[task_id] for task_id in lost_task_ids
                )
                removed_persistence = [
                    entry
                    for entry in persistence.pending
                    if entry[1] == output_ids[logical_id]
                ]
                persistence.injected_external_failures -= sum(
                    bool(
                        entry[2].get(
                            "inject_failure_during_write"
                        )
                    )
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
                        "process_shutdown": bool(
                            worker_loss_process_shutdown
                        ),
                    }
                )
                execution.worker_loss_injections += 1
                # Prevent a downstream dispatch until the next scheduler
                # turn computes the target-driven recovery closure.
            if not execution.completion_queue:
                self._cache_admission.enforce(
                    self._manager,
                    self._file_for_data_key,
                    effective_retention_bytes,
                    effective_retention_items,
                    remaining_cache_uses,
                    {
                        data_key
                        for logical_ids in execution.running.values()
                        for running_logical_id in logical_ids
                        for data_key in task_cache_inputs[
                            running_logical_id
                        ]
                    }
                    | {
                        f"i:{data_id}"
                        for data_id in required_result_data_ids
                    },
                )
        workflow_execution_elapsed = (
            time.monotonic() - workflow_execution_started
        )
        if execution.deferred_completed_states:
            self.controller.set_task_states(
                execution.deferred_completed_states, "completed"
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
        cache_deadline = time.monotonic() + 30
        while (
            self._cache_admission.evictions
            or not self._cache_admission.within_capacity(
                worker_disk_cache_bytes, worker_disk_cache_items
            )
        ):
            self._raise_if_stopping()
            self._manager.wait(1)
            self._sync_worker_epochs()
            self._cache_admission.poll(self._manager)
            self._cache_admission.enforce(
                self._manager,
                self._file_for_data_key,
                effective_retention_bytes,
                effective_retention_items,
                remaining_cache_uses,
                (),
            )
            if time.monotonic() >= cache_deadline:
                raise TimeoutError(
                    "worker cache eviction acknowledgements timed out"
                )
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
                f"{self._manager.datavine_peer_transfer_fault_stats()}"
            )
        peer_release_drain_started = time.monotonic()
        peer_release_drain_iterations = 0
        peer_release_drain_deadline = (
            peer_release_drain_started
            + max(30.0, peer_release_retry_seconds + 30.0)
        )
        while True:
            self._raise_if_stopping()
            peer_faults = (
                self._manager.datavine_peer_transfer_fault_stats()
            )
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
        physical_cache_workers = self._manager.status("workers")
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
            "execution_boundary": (
                "persistent-library" if use_worker_library else "process"
            ),
            **format_logical_outputs(
                self._run_context.logical_output_slots,
                self._run_context.attempts,
                final_output_status,
                report_task_ids,
                report_data_ids,
                detailed_report,
            ),
            "partial_publication_failures": (
                publication.failures
            ),
            "physical_attempts": sum(self._run_context.attempts.values()),
            "physical_compute_submissions": execution.physical_submissions,
            "physical_task_build_seconds": (
                execution.physical_task_build_seconds
            ),
            "physical_task_submit_seconds": (
                execution.physical_task_submit_seconds
            ),
            "library_batch_size": effective_library_batch_size,
            "logical_tasks_per_physical_submission": (
                sum(self._run_context.attempts.values()) / execution.physical_submissions
                if execution.physical_submissions
                else 0
            ),
            "batch_worker_seconds": execution.batch_worker_seconds,
            "physical_batch_metrics": execution.physical_batch_metrics,
            "workflow_timing_seconds": workflow_timing,
            "registration_timing_seconds": registration_timing,
            "performance_bottlenecks": rank_bottlenecks(
                workflow_timing,
                registration_timing,
                controller_request_metrics,
            ),
            "recovery_reexecutions": execution.recovery_reexecutions,
            "unavailable_input_recoveries": (
                execution.unavailable_input_recoveries
            ),
            "recovery_waves": execution.recovery_waves,
            "loss_injected": execution.loss_injected,
            "local_idata_hits": execution.local_idata_hits,
            "worker_controller_retries": execution.worker_controller_retries,
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
                self._manager.datavine_peer_transfer_fault_stats()
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
            "prefetch_selected": len(prefetch_selected),
            "prefetch_completed": prefetch_completed,
            "prefetch_failed": prefetch_failed,
            "prefetch_overlapped": prefetch_overlapped,
            "prefetch_bytes": sum(
                value["size"] for value in prefetch_selected
            ),
            "prefetch_task_ids": [
                value["physical_task_id"]
                for value in prefetch_selected
            ],
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
            "worker_physical_cache": format_worker_caches(
                physical_cache_workers
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
            file_object = self._idata_files[data_id]
            if not self._manager.fetch_file(file_object):
                raise RuntimeError(
                    f"IDataID {data_id} result transfer failed"
                )
            from ndcctools.taskvine import cvine

            payload = cvine.vine_file_contents_as_bytes(
                file_object._file
            )
            if (
                len(payload) != status["size"]
                or hashlib.sha256(payload).hexdigest()
                != status["content_hash"]
            ):
                raise IOError(f"result IDataID {data_id} is corrupt")
        return cloudpickle.loads(payload)


    def _submit_prefetches(
        self,
        task_factory,
        enabled,
        byte_budget,
        item_budget,
        inject_failure,
    ):
        if not enabled:
            return ()
        from ndcctools.taskvine import Task

        fanout = {}
        for task_id in sorted(self._run_context.logical_outputs):
            record = self._task_record(task_id)
            data_ids = [record.function_data_id]
            data_ids.extend(
                data_id
                for kind, data_id in record.positional
                if kind in ("e", "c")
            )
            data_ids.extend(
                data_id
                for _, (kind, data_id) in record.keyword
                if kind in ("e", "c")
            )
            for data_id in data_ids:
                fanout[data_id] = fanout.get(data_id, 0) + 1
        candidates = []
        for data_id, uses in fanout.items():
            info = self.controller.get_edata_metadata(data_id)
            candidates.append(
                PrefetchCandidate(data_id, info["size"], uses)
            )
        selected = select_prefetch(
            candidates, int(byte_budget), int(item_budget)
        )
        submitted = []
        for candidate in selected:
            task = Task("/bin/false" if inject_failure else "/bin/true")
            task.set_tag(f"prefetch-e{candidate.data_id}")
            task.set_cores(0)
            task.set_priority(-1000)
            task.add_input(
                task_factory.edata_file(candidate.data_id),
                f"datavine-prefetch-e{candidate.data_id}.pkl",
            )
            submitted.append(
                {
                    "physical_task_id": self._manager.submit(task),
                    "data_id": candidate.data_id,
                    "size": candidate.size,
                }
            )
        return tuple(submitted)

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
