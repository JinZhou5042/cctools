"""Single-owner Task Scheduler thread."""

import collections
import concurrent.futures
import cloudpickle
import dataclasses
import hashlib
import json
import os
from pathlib import Path
import queue
import shlex
import threading
import time
import urllib.error
import urllib.parse
import uuid

from ..models import EDataRecord, TaskRecord
from ..cache import WorkerCacheAdmission
from ..placement.policy import PrefetchCandidate, select_prefetch
from ..serialization import serialize
from ..workflow import OutputRef, iter_output_refs


class TaskSchedulerThread:
    def __init__(
        self,
        controller_client,
        bulk_origin_dir=None,
        bulk_threshold=8 * 1024 * 1024,
    ):
        self.controller = controller_client
        self._bulk_origin_dir = (
            Path(bulk_origin_dir).resolve()
            if bulk_origin_dir is not None
            else None
        )
        self._bulk_threshold = int(bulk_threshold)
        if self._bulk_threshold < 1:
            raise ValueError("bulk threshold must be positive")
        if self._bulk_origin_dir is not None:
            self._bulk_origin_dir.mkdir(parents=True, exist_ok=True)
        self._commands = queue.Queue()
        self._thread = threading.Thread(
            target=self._run,
            name="datavine-task-scheduler",
            daemon=False,
        )
        self._owner_ident = None
        self._started = False
        self._ready = threading.Event()
        self._manager = None
        self._logical_outputs = {}
        self._logical_output_slots = {}
        self._edata_files = {}
        self._idata_files = {}
        self._attempts = {}
        self._last_run_report = {}
        self._nested_idata_by_task = {}
        self._edata_by_object = {}
        self._serialization_count = 0
        self._bulk_serialization_count = 0
        self._worker_reconciliation_deferrals = 0
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
        self.call("_stop")
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            raise RuntimeError("Task Scheduler thread did not stop")
        self._started = False

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
        return len(self._sync_worker_epochs())

    def _sync_worker_epochs(self):
        workers = self._manager.status("workers")
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
            return worker_ids
        for worker_id in sorted(worker_ids):
            self.controller.claim_worker(worker_id)
        self.controller.reconcile_workers(worker_ids)
        self._cache_admission.sync_workers(worker_ids)
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
        from ndcctools.taskvine import Manager

        kwargs = {"port": port, "name": name}
        if run_info_path is not None:
            kwargs["run_info_path"] = run_info_path
        self._manager = Manager(**kwargs)
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

    def _register_value(self, value, domain="value"):
        cache_key = (str(domain), id(value))
        cached = self._edata_by_object.get(cache_key)
        if cached is not None and cached[0] is value:
            return cached[1]
        metadata, payload = serialize(value)
        metadata = dataclasses.replace(metadata, domain=str(domain))
        self._serialization_count += 1
        if (
            self._bulk_origin_dir is not None
            and len(payload) >= self._bulk_threshold
        ):
            self._bulk_serialization_count += 1
            digest = EDataRecord.digest(metadata, payload)
            path = self._bulk_origin_dir / f"edata-{digest}.pkl"
            if not path.exists():
                temporary = self._bulk_origin_dir / (
                    f".edata-{digest}-{uuid.uuid4().hex}.tmp"
                )
                try:
                    with temporary.open("xb") as stream:
                        stream.write(payload)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, path)
                    os.chmod(path, 0o444)
                    directory = os.open(
                        self._bulk_origin_dir, os.O_RDONLY
                    )
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                finally:
                    temporary.unlink(missing_ok=True)
            os.chmod(path, 0o444)
            result = self.controller.register_edata_origin(
                metadata, path, digest, len(payload)
            )
        else:
            result = self.controller.register_edata(metadata, payload)
        data_id = int(result["data_id"])
        self._edata_by_object[cache_key] = (value, data_id)
        return data_id

    def _op_register_workflow(self, workflow):
        self._assert_owner()
        workflow.validate()
        self._logical_outputs = {}
        self._logical_output_slots = {}
        self._edata_by_object = {}
        self._serialization_count = 0
        self._bulk_serialization_count = 0
        for task in workflow.tasks:
            output_data_ids = tuple(
                self.controller.allocate_idata(task.task_id, output_index)
                for output_index in range(task.output_count)
            )
            self._logical_output_slots[task.task_id] = output_data_ids
            self._logical_outputs[task.task_id] = output_data_ids[0]
        for task in workflow.tasks:
            self._nested_idata_by_task[task.task_id] = set()
            positional = tuple(
                self._binding(task.task_id, value) for value in task.args
            )
            keyword = tuple(
                (name, self._binding(task.task_id, value))
                for name, value in sorted(task.kwargs.items())
            )
            record = TaskRecord(
                task.task_id,
                self._register_value(task.function, "function"),
                positional,
                keyword,
                self._logical_output_slots[task.task_id],
                tuple(
                    sorted(
                        {
                            self._logical_output_slots[
                                reference.producer_task_id
                            ][reference.output_index]
                            for value in (
                                *task.args,
                                *task.kwargs.values(),
                            )
                            for reference in iter_output_refs(value)
                        }
                    )
                ),
            )
            self.controller.register_task(record)
        result = dict(self._logical_outputs)
        self._edata_by_object.clear()
        return result

    def _binding(self, task_id, value):
        if isinstance(value, OutputRef):
            return (
                "i",
                self._logical_output_slots[value.producer_task_id][
                    value.output_index
                ],
            )
        references = tuple(iter_output_refs(value))
        if references:
            self._nested_idata_by_task[task_id].update(
                self._logical_output_slots[
                    reference.producer_task_id
                ][reference.output_index]
                for reference in references
            )
            return ("c", self._register_value(value, "container"))
        return ("e", self._register_value(value))

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
        inject_peer_corruptions=0,
    ):
        self._assert_owner()
        if self._manager is None:
            raise RuntimeError("create_manager must be called first")
        reconciliation_deferrals_before = (
            self._worker_reconciliation_deferrals
        )
        admission_items = (
            -1
            if worker_disk_cache_admission_items is None
            else int(worker_disk_cache_admission_items)
        )
        if admission_items < -1:
            raise ValueError(
                "worker disk cache admission item capacity is negative"
            )
        if self._manager.tune(
            "datavine-cache-capacity-items", admission_items
        ) != 0:
            raise RuntimeError(
                "TaskVine Manager rejected cache admission capacity"
            )
        admission_bytes = (
            -1
            if worker_disk_cache_admission_bytes is None
            else int(worker_disk_cache_admission_bytes)
        )
        if admission_bytes < -1:
            raise ValueError(
                "worker disk cache admission byte capacity is negative"
            )
        if self._manager.tune(
            "datavine-cache-capacity-bytes", admission_bytes
        ) != 0:
            raise RuntimeError(
                "TaskVine Manager rejected cache byte admission capacity"
            )
        peer_source_losses = int(inject_peer_source_losses)
        if peer_source_losses < 0:
            raise ValueError(
                "peer source-loss injection count is negative"
            )
        if self._manager.tune(
            "datavine-fault-peer-source-loss", peer_source_losses
        ) != 0:
            raise RuntimeError(
                "TaskVine Manager rejected peer source-loss injection"
            )
        peer_source_loss_after_bytes = int(
            inject_peer_source_loss_after_bytes
        )
        if peer_source_loss_after_bytes < 0:
            raise ValueError(
                "peer source-loss byte threshold is negative"
            )
        if self._manager.tune(
            "datavine-fault-peer-source-loss-after-bytes",
            peer_source_loss_after_bytes,
        ) != 0:
            raise RuntimeError(
                "TaskVine Manager rejected byte-counted "
                "peer source-loss injection"
            )
        peer_corruptions = int(inject_peer_corruptions)
        if peer_corruptions < 0:
            raise ValueError("peer corruption count is negative")
        if self._manager.tune(
            "datavine-fault-peer-corruption", peer_corruptions
        ) != 0:
            raise RuntimeError(
                "TaskVine Manager rejected peer corruption injection"
            )
        output_ids = self._op_register_workflow(workflow)
        producer_by_data_id = {
            data_id: task_id
            for task_id, data_ids in self._logical_output_slots.items()
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
            if task_id not in self._logical_output_slots:
                raise KeyError(
                    f"unknown partial-publication TaskID {task_id}"
                )
            output_count = len(self._logical_output_slots[task_id])
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
        controller_snapshot = self.controller.snapshot()
        idata_inline_threshold = int(
            controller_snapshot[
                "idata_inline_object_capacity_bytes"
            ]
        )
        task_by_id = {task.task_id: task for task in workflow.tasks}
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
        task_cache_inputs = {}
        remaining_cache_uses = {}
        for task_id in sorted(task_by_id):
            record = self.controller.get_task(task_id)
            keys = {f"e:{record.function_data_id}"}
            keys.update(
                f"{'e' if kind == 'c' else kind}:{data_id}"
                for kind, data_id in record.positional
                if kind in ("e", "c", "i")
            )
            keys.update(
                f"{'e' if kind == 'c' else kind}:{data_id}"
                for _, (kind, data_id) in record.keyword
                if kind in ("e", "c", "i")
            )
            keys.update(
                f"i:{data_id}"
                for data_id in self._nested_idata_by_task.get(task_id, ())
            )
            task_cache_inputs[task_id] = keys
            for data_key in keys:
                remaining_cache_uses[data_key] = (
                    remaining_cache_uses.get(data_key, 0) + 1
                )
        max_task_cache_items = max(
            (
                len(keys) + len(self._logical_output_slots[task_id])
                for task_id, keys in task_cache_inputs.items()
            ),
            default=0,
        )
        if (
            worker_disk_cache_admission_items is not None
            and int(worker_disk_cache_admission_items)
            < max_task_cache_items
        ):
            raise ValueError(
                "worker disk cache admission capacity "
                f"{worker_disk_cache_admission_items} cannot fit the "
                f"largest task working set of {max_task_cache_items} items"
            )
        cache_known_sizes = {}
        for data_key in remaining_cache_uses:
            kind, token = data_key.split(":", 1)
            if kind == "e":
                metadata = self.controller.get_edata_metadata(int(token))
                cache_known_sizes[data_key] = int(
                    metadata["size"] or 0
                )
            else:
                metadata = self.controller.idata_status(int(token))
                cache_known_sizes[data_key] = int(
                    metadata["size"] or 0
                )
        max_task_known_cache_bytes = max(
            (
                sum(
                    max(0, cache_known_sizes[data_key])
                    for data_key in keys
                )
                for keys in task_cache_inputs.values()
            ),
            default=0,
        )
        if (
            worker_disk_cache_admission_bytes is not None
            and int(worker_disk_cache_admission_bytes)
            < max_task_known_cache_bytes
        ):
            raise ValueError(
                "worker disk cache admission capacity "
                f"{worker_disk_cache_admission_bytes} bytes cannot fit "
                "the largest known task input working set of "
                f"{max_task_known_cache_bytes} bytes"
            )
        effective_retention_items = worker_disk_cache_items
        if worker_disk_cache_admission_items is not None:
            admission_headroom = max(
                0,
                int(worker_disk_cache_admission_items)
                - max_task_cache_items,
            )
            if (
                effective_retention_items is None
                or int(effective_retention_items) > admission_headroom
            ):
                effective_retention_items = admission_headroom
        effective_retention_bytes = worker_disk_cache_bytes
        if worker_disk_cache_admission_bytes is not None:
            admission_byte_headroom = max(
                0,
                int(worker_disk_cache_admission_bytes)
                - max_task_known_cache_bytes,
            )
            if (
                effective_retention_bytes is None
                or int(effective_retention_bytes)
                > admission_byte_headroom
            ):
                effective_retention_bytes = admission_byte_headroom
        pending = set(task_by_id)
        running = {}
        persistence_pending = []
        persistence_running = {}
        controller_persistence_pending = {}
        persistence_tasks_completed = 0
        persistence_controller_tasks_completed = 0
        persistence_required = set()
        frontier_pruning = []
        frontier_pruning_applied = set()
        frontier_pruning_pending = {}
        frontier_pruning_active = None
        frontier_pruning_ack_delay = float(
            frontier_pruning_ack_delay
        )
        if frontier_pruning_ack_delay < 0:
            raise ValueError(
                "frontier pruning acknowledgement delay cannot be negative"
            )
        compute_completions_while_frontier_pruning = 0
        persistence_worker_bytes = 0
        persistence_controller_bytes = 0
        persistence_cancellations = 0

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
                before = self._manager.prune_file_status(
                    file_object
                )
                requested = self._manager.prune_file(file_object)
                if requested != len(data_records):
                    raise RuntimeError(
                        "replica authority mismatch for "
                        f"i:{data_id}: Controller={len(data_records)} "
                        f"TaskVine={requested}"
                    )
                active["worker_entries"].append(
                    {
                        "data_id": data_id,
                        "records": data_records,
                        "file": file_object,
                        "before": before,
                        "requested": requested,
                    }
                )
        persistence_failures = 0
        persistence_injected_failures_observed = 0
        persistence_retries = 0
        persistence_retry_delay_seconds = 0.0
        compute_completions_while_persistence_active = 0
        persistence_global_losses = 0
        persistence_loss_pruning_plans = []
        suspended_persistence_recovery = {}
        injected_external_persistence_failures = 0
        inject_external_persistence_failures = int(
            inject_external_persistence_failures
        )
        external_persistence_max_retries = int(
            external_persistence_max_retries
        )
        external_persistence_retry_base_seconds = float(
            external_persistence_retry_base_seconds
        )
        external_persistence_retry_max_seconds = float(
            external_persistence_retry_max_seconds
        )
        external_persistence_failure_delay = float(
            external_persistence_failure_delay
        )
        if (
            inject_external_persistence_failures < 0
            or external_persistence_max_retries < 0
            or external_persistence_retry_base_seconds < 0
            or external_persistence_retry_max_seconds < 0
            or external_persistence_failure_delay < 0
        ):
            raise ValueError(
                "external persistence failure/retry values "
                "cannot be negative"
            )
        persistence_retry_counts = collections.defaultdict(int)
        persistence_capacity = int(
            (
                controller_snapshot.get("persistence_executor")
                or {}
            ).get("workers", 1)
        )
        prefetch_running = set()
        prefetch_selected = self._submit_prefetches(
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
        done = set()
        completed_once = set()
        recovery_reexecutions = 0
        recovery_waves = []
        loss_injected = False
        worker_loss_injections = 0
        worker_loss_events = []
        local_idata_hits = 0
        partial_publication_failures = []
        partial_publication_triggered = set()
        partial_publication_cancelled = {}

        def queue_frontier_pruning_if_ready(frontier_task_id):
            if (
                frontier_task_id not in prune_after_persistence_by_task
                or frontier_task_id in frontier_pruning_applied
                or frontier_task_id in frontier_pruning_pending
            ):
                return
            if any(
                self.controller.idata_status(data_id)["durability"]
                != "durable"
                for data_id in self._logical_output_slots[
                    frontier_task_id
                ]
            ):
                return
            frontier_pruning_pending[frontier_task_id] = [
                output_ids[task_id]
                for task_id in prune_after_persistence_by_task[
                    frontier_task_id
                ]
            ]

        while (
            pending
            or running
            or prefetch_running
            or persistence_pending
            or persistence_running
            or controller_persistence_pending
            or suspended_persistence_recovery
            or frontier_pruning_pending
            or frontier_pruning_active
        ):
            for data_id, not_before in tuple(
                controller_persistence_pending.items()
            ):
                if not_before > time.monotonic():
                    continue
                status = self.controller.idata_status(data_id)
                if status["durability"] in ("queued", "writing"):
                    continue
                if status["durability"] == "failed":
                    persistence_failures += 1
                    retry_key = (data_id, int(status["attempt"]))
                    retries = persistence_retry_counts[retry_key]
                    if retries >= external_persistence_max_retries:
                        raise RuntimeError(
                            f"IDataID {data_id} Controller persistence "
                            f"exhausted {retries} retries: status={status}"
                        )
                    delay = (
                        external_persistence_retry_base_seconds
                        * (2 ** min(retries, 30))
                    )
                    delay = min(
                        delay,
                        external_persistence_retry_max_seconds,
                    )
                    persistence_retry_counts[retry_key] += 1
                    persistence_retries += 1
                    persistence_retry_delay_seconds += delay
                    retry_status = self.controller.persist_idata(data_id)
                    retry_request = retry_status.get(
                        "persistence_request", {}
                    )
                    if retry_request.get("mode") != "controller":
                        raise RuntimeError(
                            f"IDataID {data_id} Controller persistence "
                            "retry changed execution mode"
                        )
                    controller_persistence_pending[data_id] = (
                        time.monotonic() + delay
                    )
                    continue
                if status["durability"] != "durable":
                    raise RuntimeError(
                        f"IDataID {data_id} Controller persistence "
                        f"failed: status={status}"
                    )
                self._idata_files[data_id] = self._durable_idata_file(
                    data_id, status
                )
                controller_persistence_pending.pop(data_id)
                persistence_controller_tasks_completed += 1
                persistence_controller_bytes += int(status["size"])
                queue_frontier_pruning_if_ready(
                    producer_by_data_id[data_id]
                )
            for data_id, recovery in tuple(
                suspended_persistence_recovery.items()
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
                pending.add(recovery["logical_id"])
                suspended_persistence_recovery.pop(data_id)
            recovery_wave = []

            def require_available(data_id):
                nonlocal recovery_reexecutions
                task_id = producer_by_data_id[data_id]
                if task_id not in done:
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
                done.remove(task_id)
                pending.add(task_id)
                recovery_reexecutions += 1
                recovery_wave.append(task_id)
                producer_record = self.controller.get_task(task_id)
                for input_data_id in producer_record.input_data_ids:
                    require_available(input_data_id)

            for pending_task_id in sorted(pending):
                pending_record = self.controller.get_task(
                    pending_task_id
                )
                for input_data_id in pending_record.input_data_ids:
                    require_available(input_data_id)
            for result_task_id in sorted(result_task_ids):
                for result_data_id in self._logical_output_slots[
                    result_task_id
                ]:
                    require_available(result_data_id)
            if recovery_wave:
                plan = self.controller.pruning_plan()
                recovery_waves.append(
                    {
                        "tasks": recovery_wave,
                        "rollback_depth": len(recovery_wave),
                        "recovery_depths": plan["recovery_depths"],
                    }
                )

            if (
                frontier_pruning_active is not None
                and time.monotonic()
                >= frontier_pruning_active["poll_after"]
            ):
                if frontier_pruning_active["deferred_data_ids"]:
                    operation_id = frontier_pruning_active[
                        "continuation_operation_id"
                    ]
                    if operation_id is None:
                        operation_id = f"pruning:{uuid.uuid4().hex}"
                        frontier_pruning_active[
                            "continuation_operation_id"
                        ] = operation_id
                    try:
                        continuation = (
                            self.controller.continue_deferred_pruning(
                                operation_id,
                                sorted(
                                    frontier_pruning_active[
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
                        frontier_pruning_active[
                            "controller_continuation_retries"
                        ].append(
                            {
                                "operation_id": operation_id,
                                "error": type(exc).__name__,
                            }
                        )
                        frontier_pruning_active["poll_after"] = (
                            time.monotonic() + 0.1
                        )
                        continuation = None
                    if continuation is not None:
                        frontier_pruning_active[
                            "continuation_operation_id"
                        ] = None
                        frontier_pruning_active[
                            "controller_continuations"
                        ].append(continuation)
                        still_deferred = {
                            item["data_id"]
                            for item in continuation["deferred"]
                        }
                        resolved = (
                            frontier_pruning_active[
                                "deferred_data_ids"
                            ]
                            - still_deferred
                        )
                        cancelled = {
                            item["data_id"]
                            for item in continuation["cancelled"]
                        }
                        if cancelled:
                            frontier_pruning_active[
                                "cancelled_data_ids"
                            ].update(cancelled)
                        start_frontier_worker_prunes(
                            frontier_pruning_active,
                            continuation["applied"],
                        )
                        frontier_pruning_active[
                            "deferred_data_ids"
                        ] -= resolved
                all_complete = not frontier_pruning_active[
                    "deferred_data_ids"
                ]
                worker_prunes = []
                for entry in frontier_pruning_active["worker_entries"]:
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
                        }
                    )
                if (
                    not all_complete
                    and time.monotonic()
                    >= frontier_pruning_active["deadline"]
                ):
                    raise TimeoutError(
                        "asynchronous frontier pruning acknowledgement "
                        "timed out"
                    )
                if all_complete:
                    for entry in frontier_pruning_active[
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
                    frontier_task_id = frontier_pruning_active[
                        "frontier_task_id"
                    ]
                    frontier_pruning.append(
                        {
                            "frontier_task_id": frontier_task_id,
                            "data_ids": frontier_pruning_active[
                                "data_ids"
                            ],
                            "result": {
                                "controller": frontier_pruning_active[
                                    "controller_result"
                                ],
                                "controller_continuations": (
                                    frontier_pruning_active[
                                        "controller_continuations"
                                    ]
                                ),
                                "controller_continuation_retries": (
                                    frontier_pruning_active[
                                        "controller_continuation_retries"
                                    ]
                                ),
                                "worker_prunes": worker_prunes,
                            },
                            "cancelled_data_ids": sorted(
                                frontier_pruning_active[
                                    "cancelled_data_ids"
                                ]
                            ),
                        }
                    )
                    frontier_pruning_applied.add(frontier_task_id)
                    frontier_pruning_active = None

            if (
                frontier_pruning_active is None
                and frontier_pruning_pending
            ):
                active_inputs = {
                    data_key
                    for running_logical_id in running.values()
                    for data_key in task_cache_inputs[
                        running_logical_id
                    ]
                }
                safe_frontiers = [
                    frontier_task_id
                    for frontier_task_id, data_ids
                    in frontier_pruning_pending.items()
                    if not (
                        {f"i:{data_id}" for data_id in data_ids}
                        & active_inputs
                    )
                    and not (
                        set(data_ids)
                        & {
                            data_id
                            for data_id, _ in persistence_running.values()
                        }
                    )
                ]
                if safe_frontiers:
                    frontier_task_id = min(safe_frontiers)
                    prune_data_ids = frontier_pruning_pending.pop(
                        frontier_task_id
                    )
                    plan = self.controller.pruning_plan()
                    result = self.controller.apply_pruning(
                        plan["records"][0]["graph_revision"],
                        plan["records"][0]["state_revision"],
                        0,
                        prune_data_ids,
                        None,
                    )
                    now_value = time.monotonic()
                    frontier_pruning_active = {
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
                        "poll_after": (
                            now_value + frontier_pruning_ack_delay
                        ),
                        "deadline": now_value + 30,
                    }
                    start_frontier_worker_prunes(
                        frontier_pruning_active,
                        [
                            record
                            for record in result["applied"]
                            if record["data_id"]
                            not in frontier_pruning_active[
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
                        or self._attempts.get(parent_task_id, 0)
                        < threshold
                    ):
                        continue
                    if (
                        parent_task_id
                        in prune_after_persistence_by_task
                        and parent_task_id
                        not in frontier_pruning_applied
                    ):
                        return False
                    if any(
                        self.controller.idata_status(data_id)[
                            "durability"
                        ] != "durable"
                        for data_id in self._logical_output_slots[
                            parent_task_id
                        ]
                    ):
                        return False
                return True

            ready = sorted(
                task_id
                for task_id in pending
                if dependencies[task_id] <= done
                and persistence_frontier_ready(task_id)
                and not (
                    task_cache_inputs[task_id]
                    & self._cache_admission.prune_by_data
                )
            )
            for task_id in ready:
                attempt = self._attempts.get(task_id, 0) + 1
                self._attempts[task_id] = attempt
                physical = self._make_physical_task(
                    task_id,
                    environment,
                    attempt,
                    idata_inline_threshold,
                    inject_partial_publication_after.get(task_id)
                    if attempt == 1
                    else None,
                )
                physical_id = self._manager.submit(physical)
                self.controller.set_task_state(task_id, "running")
                running[physical_id] = task_id
                pending.remove(task_id)
            while (
                persistence_pending
                and len(persistence_running) < persistence_capacity
            ):
                persistence_pending.sort(key=lambda entry: entry[0])
                not_before, data_id, request = persistence_pending[0]
                if not_before > time.monotonic():
                    break
                persistence_pending.pop(0)
                physical = self._make_persistence_task(
                    data_id, request, environment
                )
                physical_id = self._manager.submit(physical)
                persistence_running[physical_id] = (data_id, request)
            if (
                not running
                and not prefetch_running
                and not persistence_running
            ):
                if persistence_pending:
                    delay = max(
                        0,
                        persistence_pending[0][0] - time.monotonic(),
                    )
                    time.sleep(min(float(wait_timeout), delay))
                    continue
                if self._cache_admission.evictions:
                    self._manager.wait(wait_timeout)
                    self._sync_worker_epochs()
                    self._cache_admission.poll(self._manager)
                    continue
                if suspended_persistence_recovery:
                    self._manager.wait(wait_timeout)
                    self._sync_worker_epochs()
                    continue
                if controller_persistence_pending:
                    delay = max(
                        0,
                        min(controller_persistence_pending.values())
                        - time.monotonic(),
                    )
                    time.sleep(
                        min(float(wait_timeout), max(0.05, delay))
                    )
                    continue
                if not (
                    pending
                    or running
                    or prefetch_running
                    or persistence_pending
                    or persistence_running
                    or suspended_persistence_recovery
                    or frontier_pruning_pending
                    or frontier_pruning_active
                ):
                    break
                if frontier_pruning_active is not None:
                    self._manager.wait(wait_timeout)
                    self._sync_worker_epochs()
                    continue
                blocked = {}
                for task_id in sorted(pending):
                    unavailable_inputs = []
                    persistence_frontiers = {}
                    for input_data_id in self.controller.get_task(
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
                            "attempt": self._attempts.get(
                                parent_task_id, 0
                            ),
                            "threshold": (
                                persistence_attempts_by_task.get(
                                    parent_task_id
                                )
                            ),
                            "pruning_applied": (
                                parent_task_id
                                in frontier_pruning_applied
                            ),
                            "durability": [
                                self.controller.idata_status(data_id)[
                                    "durability"
                                ]
                                for data_id in self._logical_output_slots[
                                    parent_task_id
                                ]
                            ],
                        }
                    blocked[task_id] = {
                        "unfinished_dependencies": sorted(
                            dependencies[task_id] - done
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
                    f"pending={blocked} done={sorted(done)} "
                    f"frontier_pruning_pending="
                    f"{sorted(frontier_pruning_pending)}"
                )
            completed = self._manager.wait(wait_timeout)
            self._sync_worker_epochs()
            self._cache_admission.poll(self._manager)
            if completed is None:
                for physical_id, logical_id in tuple(running.items()):
                    output_index = (
                        inject_partial_publication_after.get(logical_id)
                    )
                    if (
                        output_index is None
                        or logical_id in partial_publication_triggered
                        or self._attempts[logical_id] != 1
                    ):
                        continue
                    expected_data_ids = self._logical_output_slots[
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
                    partial_publication_triggered.add(logical_id)
                    partial_publication_cancelled[physical_id] = {
                        "task_id": logical_id,
                        "attempt": 1,
                        "published_data_ids": [published_data_id],
                        "expected_data_ids": list(expected_data_ids),
                        "worker_id": worker_id,
                        "physical_task_id": physical_id,
                    }
                    break
                if partial_publication_cancelled:
                    continue
                if (
                    inject_global_loss_during_persistence
                    and persistence_global_losses == 0
                ):
                    for data_id, active_request in (
                        persistence_running.values()
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
                        if logical_id not in done:
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
                        done.remove(logical_id)
                        suspended_persistence_recovery[data_id] = {
                            "logical_id": logical_id,
                            "file": file_object,
                            "before": prune_before,
                            "requested": prune_requested,
                            "request_id": active_request["request_id"],
                            "persistence_drained": False,
                        }
                        recovery_reexecutions += 1
                        persistence_global_losses += 1
                        persistence_loss_pruning_plans.append(
                            {
                                "data_id": data_id,
                                "before": record_before,
                                "after": record_after,
                            }
                        )
                        break
                if (
                    inject_external_persistence_cancel
                    and persistence_cancellations == 0
                ):
                    for data_id, _ in persistence_running.values():
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
                            persistence_cancellations += 1
                            break
                self._cache_admission.enforce(
                    self._manager,
                    self._file_for_data_key,
                    effective_retention_bytes,
                    effective_retention_items,
                    remaining_cache_uses,
                    {
                        data_key
                        for logical_id in running.values()
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
                if not done:
                    prefetch_overlapped = True
                continue
            if completed.id in persistence_running:
                data_id, request = persistence_running.pop(completed.id)
                suspended = suspended_persistence_recovery.get(data_id)
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
                        persistence_injected_failures_observed += 1
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
                        persistence_pending.append(
                            (
                                time.monotonic(),
                                data_id,
                                retry_status["persistence_request"],
                            )
                        )
                        continue
                    elif status["durability"] == "failed":
                        persistence_failures += 1
                        retry_key = (
                            data_id,
                            int(request["attempt"]),
                        )
                        retries = persistence_retry_counts[retry_key]
                        if retries >= external_persistence_max_retries:
                            raise RuntimeError(
                                f"IDataID {data_id} persistence exhausted "
                                f"{retries} retries: "
                                f"stdout={completed.output}"
                            )
                        delay = (
                            external_persistence_retry_base_seconds
                            * (2 ** min(retries, 30))
                        )
                        delay = min(
                            delay,
                            external_persistence_retry_max_seconds,
                        )
                        persistence_retry_counts[retry_key] += 1
                        persistence_retries += 1
                        persistence_retry_delay_seconds += delay
                        retry_status = self.controller.persist_idata(
                            data_id
                        )
                        retry_request = retry_status[
                            "persistence_request"
                        ]
                        if (
                            injected_external_persistence_failures
                            < inject_external_persistence_failures
                        ):
                            retry_request = {
                                **retry_request,
                                "inject_failure_during_write": True,
                                "inject_failure_delay": (
                                    external_persistence_failure_delay
                                ),
                            }
                            injected_external_persistence_failures += 1
                        persistence_pending.append(
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
                    persistence_pending.append(
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
                self._idata_files[data_id] = self._durable_idata_file(
                    data_id, status
                )
                persistence_tasks_completed += 1
                persistence_worker_bytes += int(status["size"])
                queue_frontier_pruning_if_ready(
                    producer_by_data_id[data_id]
                )
                continue
            logical_id = running.pop(completed.id)
            if frontier_pruning_active is not None:
                compute_completions_while_frontier_pruning += 1
            if persistence_running or persistence_pending:
                compute_completions_while_persistence_active += 1
            local_idata_hits += (completed.output or "").count(
                "DATAVINE_LOCAL_IDATA"
            )
            if not completed.successful():
                # A worker can disappear after its replica was selected but
                # before a dependent task starts. Reconcile first, then turn
                # a globally lost input into ordinary logical recomputation
                # instead of making the failed consumer terminal.
                self._sync_worker_epochs()
                partial_failure = partial_publication_cancelled.pop(
                    completed.id, None
                )
                if partial_failure is not None:
                    partial_publication_failures.append(
                        partial_failure
                    )
                    self.controller.set_task_state(
                        logical_id, "pending"
                    )
                    pending.add(logical_id)
                    continue
                expected_output_ids = self._logical_output_slots[
                    logical_id
                ]
                published_output_ids = [
                    data_id
                    for data_id in expected_output_ids
                    if (
                        self.controller.idata_status(data_id)[
                            "attempt"
                        ] == self._attempts[logical_id]
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
                    partial_publication_failures.append(
                        {
                            "task_id": logical_id,
                            "attempt": self._attempts[logical_id],
                            "published_data_ids": published_output_ids,
                            "expected_data_ids": list(
                                expected_output_ids
                            ),
                        }
                    )
                    self.controller.set_task_state(
                        logical_id, "pending"
                    )
                    pending.add(logical_id)
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
                    pending.add(logical_id)
                    continue
                self.controller.set_task_state(logical_id, "pending")
                raise RuntimeError(
                    f"TaskID {logical_id} failed: result={completed.result} "
                    f"exit={completed.exit_code} stdout={completed.output}"
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
            expected_output_ids = self._logical_output_slots[logical_id]
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
                    != self._attempts[logical_id]
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
                    != self._attempts[logical_id]
                    or output_status["content_hash"]
                    != preparation["content_hash"]
                    or output_status["size"] != preparation["size"]
                ):
                    raise RuntimeError(
                        f"TaskID {logical_id} output {output_index} "
                        "publication is not available"
                    )
                if output_status["controller_inline"]:
                    self.controller.fetch_idata(output_data_id)
            if (
                persist_outputs
                and logical_id in persistence_attempts_by_task
                and self._attempts[logical_id]
                >= persistence_attempts_by_task[logical_id]
            ):
                for output_data_id in expected_output_ids:
                    persistence_status = self.controller.persist_idata(
                        output_data_id
                    )
                    persistence_required.add(output_data_id)
                    request = persistence_status.get(
                        "persistence_request", {}
                    )
                    if request.get("mode") == "worker":
                        if (
                            inject_external_persistence_cancel
                            or inject_global_loss_during_persistence
                        ):
                            request = {
                                **request,
                                "inject_cancel_delay": True,
                            }
                        if (
                            injected_external_persistence_failures
                            < inject_external_persistence_failures
                        ):
                            request = {
                                **request,
                                "inject_failure_during_write": True,
                                "inject_failure_delay": (
                                    external_persistence_failure_delay
                                ),
                            }
                            injected_external_persistence_failures += 1
                        persistence_pending.append(
                            (
                                time.monotonic(),
                                output_data_id,
                                request,
                            )
                        )
                    else:
                        controller_persistence_pending[
                            output_data_id
                        ] = time.monotonic()
            self.controller.set_task_state(logical_id, "completed")
            done.add(logical_id)
            if logical_id not in completed_once:
                completed_once.add(logical_id)
                for data_key in task_cache_inputs[logical_id]:
                    remaining_cache_uses[data_key] -= 1
            if (
                inject_global_loss_after == logical_id
                and not loss_injected
            ):
                self.controller.invalidate_idata(
                    output_ids[logical_id]
                )
                loss_injected = True
            if (
                worker_loss_injections < len(worker_loss_schedule)
                and worker_loss_schedule[worker_loss_injections]
                == logical_id
            ):
                from ndcctools.taskvine import cvine
                workers_before = sorted(self._sync_worker_epochs())
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
                workers_after = sorted(self._sync_worker_epochs())
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
                removed_persistence = [
                    entry
                    for entry in persistence_pending
                    if entry[1] == output_ids[logical_id]
                ]
                injected_external_persistence_failures -= sum(
                    bool(
                        entry[2].get(
                            "inject_failure_during_write"
                        )
                    )
                    for entry in removed_persistence
                )
                persistence_pending = [
                    entry
                    for entry in persistence_pending
                    if entry[1] != output_ids[logical_id]
                ]
                worker_loss_events.append(
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
                worker_loss_injections += 1
                # Prevent a downstream dispatch until the next scheduler
                # turn computes the target-driven recovery closure.
            self._cache_admission.enforce(
                self._manager,
                self._file_for_data_key,
                effective_retention_bytes,
                effective_retention_items,
                remaining_cache_uses,
                {
                    data_key
                    for running_logical_id in running.values()
                    for data_key in task_cache_inputs[
                        running_logical_id
                    ]
                },
            )
        cache_deadline = time.monotonic() + 30
        while (
            self._cache_admission.evictions
            or not self._cache_admission.within_capacity(
                worker_disk_cache_bytes, worker_disk_cache_items
            )
        ):
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
            for output_data_id in sorted(persistence_required):
                self._wait_durable(output_data_id)
        physical_cache_workers = self._manager.status("workers")
        self._manager._refresh_stats()
        self._last_run_report = {
            "logical_tasks": len(output_ids),
            "logical_output_slots": {
                str(task_id): list(data_ids)
                for task_id, data_ids
                in sorted(self._logical_output_slots.items())
            },
            "logical_output_status": {
                str(data_id): {
                    key: status[key]
                    for key in (
                        "producer_task_id",
                        "producer_output_index",
                        "attempt",
                        "content_hash",
                        "size",
                        "available",
                        "durability",
                    )
                }
                for data_id in sorted(producer_by_data_id)
                for status in (self.controller.idata_status(data_id),)
            },
            "attempts_by_task": {
                str(task_id): attempt
                for task_id, attempt in sorted(self._attempts.items())
            },
            "partial_publication_failures": (
                partial_publication_failures
            ),
            "physical_attempts": sum(self._attempts.values()),
            "recovery_reexecutions": recovery_reexecutions,
            "recovery_waves": recovery_waves,
            "legacy_recovery_tasks": int(
                self._manager.stats.tasks_recovery
            ),
            "loss_injected": loss_injected,
            "local_idata_hits": local_idata_hits,
            "worker_loss_injected": bool(worker_loss_injections),
            "worker_loss_injections": worker_loss_injections,
            "worker_loss_schedule": list(worker_loss_schedule),
            "worker_loss_events": worker_loss_events,
            "worker_loss_process_shutdown": bool(
                worker_loss_process_shutdown
            ),
            "peer_source_losses_requested": peer_source_losses,
            "peer_source_loss_after_bytes_requested": (
                peer_source_loss_after_bytes
            ),
            "peer_corruptions_requested": peer_corruptions,
            "peer_transfer_faults": (
                self._manager.datavine_peer_transfer_fault_stats()
            ),
            "worker_loss_data_by_task": {
                str(task_id): list(data_task_ids)
                for task_id, data_task_ids
                in inject_worker_loss_data_by_task.items()
            },
            "persistence_required_data_ids": sorted(
                persistence_required
            ),
            "frontier_pruning": frontier_pruning,
            "compute_completions_while_frontier_pruning": (
                compute_completions_while_frontier_pruning
            ),
            "runtime_pruned_data_ids": sorted(
                {
                    data_id
                    for event in frontier_pruning
                    for data_id in event["data_ids"]
                    if data_id
                    not in event["cancelled_data_ids"]
                }
            ),
            "persistence_tasks_completed": (
                persistence_tasks_completed
            ),
            "persistence_controller_tasks_completed": (
                persistence_controller_tasks_completed
            ),
            "persistence_worker_bytes": persistence_worker_bytes,
            "persistence_controller_bytes": (
                persistence_controller_bytes
            ),
            "persistence_cancellations": persistence_cancellations,
            "persistence_failures": persistence_failures,
            "persistence_injected_failures_observed": (
                persistence_injected_failures_observed
            ),
            "persistence_retries": persistence_retries,
            "persistence_retry_delay_seconds": (
                persistence_retry_delay_seconds
            ),
            "compute_completions_while_persistence_active": (
                compute_completions_while_persistence_active
            ),
            "persistence_global_losses": persistence_global_losses,
            "persistence_loss_pruning_plans": (
                persistence_loss_pruning_plans
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
            "edata_serializations": self._serialization_count,
            "bulk_edata_serializations": self._bulk_serialization_count,
            "worker_reconciliation_deferrals": (
                self._worker_reconciliation_deferrals
                - reconciliation_deferrals_before
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
            "worker_physical_cache": [
                {
                    key: worker.get(key)
                    for key in (
                        "workerid",
                        "cache_items",
                        "cache_bytes",
                        "cache_items_high_water",
                        "cache_bytes_high_water",
                        "cache_prune_pending_items",
                        "cache_prune_pending_bytes",
                        "cache_admission_rejections",
                        "cache_capacity_configured",
                        "cache_capacity_items",
                        "cache_capacity_bytes",
                        "worker_cache_items",
                        "worker_cache_bytes",
                        "worker_cache_items_high_water",
                        "worker_cache_bytes_high_water",
                        "worker_cache_admission_rejections",
                    )
                }
                for worker in physical_cache_workers
            ],
            **self._cache_admission.report(
                worker_disk_cache_bytes,
                worker_disk_cache_items,
            ),
        }
        return {
            task_id: (
                self._load_result(output_ids_for_task[0])
                if len(output_ids_for_task) == 1
                else tuple(
                    self._load_result(output_data_id)
                    for output_data_id in output_ids_for_task
                )
            )
            for task_id, output_ids_for_task
            in self._logical_output_slots.items()
            if task_id in result_task_ids
        }

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
            raise RuntimeError(
                f"large result IDataID {data_id} requires durability"
            )
        return cloudpickle.loads(payload)

    def _edata_file(self, data_id):
        file_object = self._edata_files.get(data_id)
        if file_object is None:
            info = self.controller.get_edata_metadata(data_id)
            if info["storage"] == "bulk-origin":
                file_object = self._manager.declare_file(
                    info["origin_path"],
                    cache="worker",
                    peer_transfer=True,
                )
            else:
                url = (
                    self.controller.endpoint
                    + f"/v1/edata/{data_id}?"
                    + urllib.parse.urlencode(
                        {"token": self.controller.token}
                    )
                )
                file_object = self._manager.declare_url(
                    url, cache="worker", peer_transfer=True
                )
            self._edata_files[data_id] = file_object
            if not file_object.set_datavine_data_id(f"e:{data_id}"):
                raise RuntimeError(
                    f"could not bind TaskVine file to EDataID e:{data_id}"
                )
            if not file_object.set_datavine_content_hash(
                info["serialized_sha256"]
            ):
                raise RuntimeError(
                    f"could not bind EDataID e:{data_id} content hash"
                )
        return file_object

    def _idata_output_file(self, data_id, attempt):
        url = (
            self.controller.endpoint
            + f"/v1/idata/{int(data_id)}?"
            + urllib.parse.urlencode(
                {
                    "token": self.controller.token,
                    "attempt": int(attempt),
                }
            )
        )
        file_object = self._manager.declare_url(
            url, cache="worker", peer_transfer=True
        )
        if not file_object.set_datavine_data_id(f"i:{int(data_id)}"):
            raise RuntimeError(
                f"could not bind TaskVine file to IDataID i:{data_id}"
            )
        self._idata_files[int(data_id)] = file_object
        return file_object

    def _submit_prefetches(
        self,
        enabled,
        byte_budget,
        item_budget,
        inject_failure,
    ):
        if not enabled:
            return ()
        from ndcctools.taskvine import Task

        fanout = {}
        for task_id in sorted(self._logical_outputs):
            record = self.controller.get_task(task_id)
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
                self._edata_file(candidate.data_id),
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

    def _make_physical_task(
        self,
        task_id,
        environment,
        attempt,
        idata_inline_threshold,
        kill_worker_after_output_index=None,
    ):
        from ndcctools.taskvine import Task

        record = self.controller.get_task(task_id)
        output_names = tuple(
            f"datavine-idata-{data_id}.pkl"
            for data_id in record.output_data_ids
        )
        command = " ".join(
            shlex.quote(value)
            for value in (
                "python",
                "-m",
                "ndcctools.taskvine.datavine.worker.runner",
                "--controller",
                self.controller.endpoint,
                "--token",
                self.controller.token,
                "--task-id",
                str(task_id),
                "--attempt",
                str(attempt),
                *(
                    value
                    for output_name in output_names
                    for value in ("--output-file", output_name)
                ),
                "--idata-inline-threshold",
                str(idata_inline_threshold),
                *(
                    (
                        "--pause-after-output-index",
                        str(kill_worker_after_output_index),
                    )
                    if kill_worker_after_output_index is not None
                    else ()
                ),
            )
        )
        task = Task(command)
        task.set_tag(str(task_id))
        task.set_cores(1)
        task.set_retries(
            0 if kill_worker_after_output_index is not None else 5
        )
        edata_ids = {record.function_data_id}
        edata_ids.update(
            data_id
            for kind, data_id in record.positional
            if kind in ("e", "c")
        )
        edata_ids.update(
            data_id
            for _, (kind, data_id) in record.keyword
            if kind in ("e", "c")
        )
        for data_id in sorted(edata_ids):
            file_object = self._edata_file(data_id)
            task.add_input(
                file_object, f"datavine-edata-{data_id}.pkl"
            )
        idata_ids = {
            data_id
            for kind, data_id in record.positional
            if kind == "i"
        }
        idata_ids.update(
            data_id
            for _, (kind, data_id) in record.keyword
            if kind == "i"
        )
        idata_ids.update(self._nested_idata_by_task.get(task_id, ()))
        for data_id in sorted(idata_ids):
            task.add_input(
                self._idata_files[data_id],
                f"datavine-idata-{data_id}.pkl",
            )
        for output_data_id, output_name in zip(
            record.output_data_ids, output_names
        ):
            output_file = self._idata_output_file(
                output_data_id, attempt
            )
            task.add_output(output_file, output_name)
        if environment is not None:
            task.add_environment(environment)
        return task

    def _make_persistence_task(self, data_id, request, environment):
        from ndcctools.taskvine import Task

        input_name = f"datavine-persist-i{int(data_id)}.pkl"
        command = " ".join(
            shlex.quote(value)
            for value in (
                "python",
                "-m",
                "ndcctools.taskvine.datavine.worker.persist",
                "--controller",
                self.controller.endpoint,
                "--token",
                self.controller.token,
                "--data-id",
                str(int(data_id)),
                "--request-id",
                request["request_id"],
                "--input-file",
                input_name,
                *(
                    ("--delay-before-complete", "3")
                    if request.get("inject_cancel_delay")
                    else ()
                ),
                *(
                    (
                        "--inject-failure-during-write",
                        "--delay-before-failure",
                        str(request.get("inject_failure_delay", 0)),
                    )
                    if request.get("inject_failure_during_write")
                    else ()
                ),
            )
        )
        task = Task(command)
        task.set_tag(f"persist-i{int(data_id)}")
        task.set_cores(0)
        task.set_priority(-500)
        task.set_retries(0)
        task.add_input(self._idata_files[int(data_id)], input_name)
        if environment is not None:
            task.add_environment(environment)
        return task

    def _durable_idata_file(self, data_id, status):
        file_object = self._manager.declare_file(
            status["durable_path"],
            cache="worker",
            peer_transfer=True,
        )
        if not file_object.set_datavine_data_id(
            f"i:{int(data_id)}"
        ):
            raise RuntimeError(
                f"could not bind durable IDataID i:{data_id}"
            )
        if not file_object.set_datavine_content_hash(
            status["content_hash"]
        ):
            raise RuntimeError(
                f"could not bind durable IDataID i:{data_id} "
                "content hash"
            )
        return file_object
