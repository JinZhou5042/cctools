"""Single-owner Task Scheduler thread."""

import concurrent.futures
import cloudpickle
import dataclasses
import json
import os
from pathlib import Path
import queue
import shlex
import threading
import time
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
        self._edata_by_object = {}
        self._serialization_count = 0
        self._bulk_serialization_count = 0
        for task in workflow.tasks:
            self._logical_outputs[
                task.task_id
            ] = self.controller.allocate_idata(task.task_id)
            self._idata_files[
                self._logical_outputs[task.task_id]
            ] = self._manager.declare_temp()
            self._idata_files[
                self._logical_outputs[task.task_id]
            ].set_datavine_data_id(
                f"i:{self._logical_outputs[task.task_id]}"
            )
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
                self._logical_outputs[task.task_id],
                tuple(
                    sorted(
                        {
                            self._logical_outputs[
                                reference.producer_task_id
                            ]
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
            return ("i", self._logical_outputs[value.producer_task_id])
        references = tuple(iter_output_refs(value))
        if references:
            self._nested_idata_by_task[task_id].update(
                self._logical_outputs[reference.producer_task_id]
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
        output_ids = self._op_register_workflow(workflow)
        task_by_id = {task.task_id: task for task in workflow.tasks}
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
            (len(keys) + 1 for keys in task_cache_inputs.values()),
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
        recovery_reexecutions = 0
        loss_injected = False
        worker_loss_injected = False
        local_idata_hits = 0
        while pending or running or prefetch_running:
            for completed_task_id in tuple(done):
                output_data_id = output_ids[completed_task_id]
                output_status = self.controller.idata_status(output_data_id)
                if not output_status["available"]:
                    self._manager.prune_file(
                        self._idata_files[output_data_id]
                    )
                    self.controller.set_task_state(
                        completed_task_id, "pending"
                    )
                    done.remove(completed_task_id)
                    pending.add(completed_task_id)
                    recovery_reexecutions += 1
            ready = sorted(
                task_id
                for task_id in pending
                if dependencies[task_id] <= done
                and not (
                    task_cache_inputs[task_id]
                    & self._cache_admission.prune_by_data
                )
            )
            for task_id in ready:
                attempt = self._attempts.get(task_id, 0) + 1
                self._attempts[task_id] = attempt
                physical = self._make_physical_task(
                    task_id, environment, attempt
                )
                physical_id = self._manager.submit(physical)
                self.controller.set_task_state(task_id, "running")
                running[physical_id] = task_id
                pending.remove(task_id)
            if not running and not prefetch_running:
                if self._cache_admission.evictions:
                    self._manager.wait(wait_timeout)
                    self._sync_worker_epochs()
                    self._cache_admission.poll(self._manager)
                    continue
                raise RuntimeError("workflow cannot make progress")
            completed = self._manager.wait(wait_timeout)
            self._sync_worker_epochs()
            self._cache_admission.poll(self._manager)
            if completed is None:
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
            logical_id = running.pop(completed.id)
            local_idata_hits += completed.output.count(
                "DATAVINE_LOCAL_IDATA"
            )
            if not completed.successful():
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
            if len(preparation_lines) != 1:
                raise RuntimeError(
                    f"TaskID {logical_id} returned "
                    f"{len(preparation_lines)} replica preparations"
                )
            preparation = json.loads(preparation_lines[0])
            if (
                preparation["data_id"]
                != f"i:{output_ids[logical_id]}"
                or preparation["attempt"] != self._attempts[logical_id]
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
            # Publication is the completion contract, not process exit alone.
            self.controller.fetch_idata(output_ids[logical_id])
            if persist_outputs:
                self.controller.persist_idata(output_ids[logical_id])
            self.controller.set_task_state(logical_id, "completed")
            done.add(logical_id)
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
                inject_worker_loss_after == logical_id
                and not worker_loss_injected
            ):
                from ndcctools.taskvine import cvine
                if not cvine.vine_manager_release_random_worker(
                    self._manager._taskvine
                ):
                    raise RuntimeError(
                        "no worker available for loss injection"
                    )
                self.controller.invalidate_idata(
                    output_ids[logical_id]
                )
                worker_loss_injected = True
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
            for output_data_id in output_ids.values():
                self._wait_durable(output_data_id)
        physical_cache_workers = self._manager.status("workers")
        self._last_run_report = {
            "logical_tasks": len(output_ids),
            "physical_attempts": sum(self._attempts.values()),
            "recovery_reexecutions": recovery_reexecutions,
            "loss_injected": loss_injected,
            "local_idata_hits": local_idata_hits,
            "worker_loss_injected": worker_loss_injected,
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
            task_id: cloudpickle.loads(
                self.controller.fetch_idata(output_data_id)
            )
            for task_id, output_data_id in output_ids.items()
        }

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

    def _make_physical_task(self, task_id, environment, attempt):
        from ndcctools.taskvine import Task

        record = self.controller.get_task(task_id)
        output_name = f"datavine-idata-{record.output_data_id}.pkl"
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
                "--output-file",
                output_name,
            )
        )
        task = Task(command)
        task.set_tag(str(task_id))
        task.set_cores(1)
        task.set_retries(5)
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
        task.add_output(
            self._idata_files[record.output_data_id], output_name
        )
        if environment is not None:
            task.add_environment(environment)
        return task
