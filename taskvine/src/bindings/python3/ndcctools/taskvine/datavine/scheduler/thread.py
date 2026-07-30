"""Single-owner Task Scheduler thread."""

import concurrent.futures
import cloudpickle
import json
import queue
import shlex
import threading
import time
import urllib.parse

from ..models import TaskRecord
from ..placement.policy import PrefetchCandidate, select_prefetch
from ..serialization import serialize
from ..workflow import OutputRef, iter_output_refs


class TaskSchedulerThread:
    def __init__(self, controller_client):
        self.controller = controller_client
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
        self._worker_by_task = {}

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
        self._sync_worker_epochs()
        return len(self._manager.status("workers"))

    def _sync_worker_epochs(self):
        workers = self._manager.status("workers")
        worker_ids = {
            worker["workerid"]
            for worker in workers
            if worker.get("workerid")
        }
        if len(worker_ids) != len(workers):
            raise RuntimeError("TaskVine worker status lacks workerid")
        return self.controller.reconcile_workers(worker_ids)

    def _op_last_run_report(self):
        self._assert_owner()
        return dict(self._last_run_report)

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
        if peer_transfers:
            self._manager.enable_peer_transfers()
        else:
            self._manager.disable_peer_transfers()
        return self._manager.port

    def _register_value(self, value):
        metadata, payload = serialize(value)
        result = self.controller.register_edata(metadata, payload)
        return int(result["data_id"])

    def _op_register_workflow(self, workflow):
        self._assert_owner()
        workflow.validate()
        self._logical_outputs = {}
        for task in workflow.tasks:
            self._logical_outputs[
                task.task_id
            ] = self.controller.allocate_idata(task.task_id)
            self._idata_files[
                self._logical_outputs[task.task_id]
            ] = self._manager.declare_temp()
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
                self._register_value(task.function),
                positional,
                keyword,
                self._logical_outputs[task.task_id],
            )
            self.controller.register_task(record)
        return dict(self._logical_outputs)

    def _binding(self, task_id, value):
        if isinstance(value, OutputRef):
            return ("i", self._logical_outputs[value.producer_task_id])
        references = tuple(iter_output_refs(value))
        if references:
            self._nested_idata_by_task[task_id].update(
                self._logical_outputs[reference.producer_task_id]
                for reference in references
            )
            return ("c", self._register_value(value))
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
    ):
        self._assert_owner()
        if self._manager is None:
            raise RuntimeError("create_manager must be called first")
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
                    done.remove(completed_task_id)
                    pending.add(completed_task_id)
                    recovery_reexecutions += 1
            ready = sorted(
                task_id
                for task_id in pending
                if dependencies[task_id] <= done
            )
            for task_id in ready:
                attempt = self._attempts.get(task_id, 0) + 1
                self._attempts[task_id] = attempt
                physical = self._make_physical_task(
                    task_id, environment, attempt
                )
                physical_id = self._manager.submit(physical)
                running[physical_id] = task_id
                pending.remove(task_id)
            if not running and not prefetch_running:
                raise RuntimeError("workflow cannot make progress")
            completed = self._manager.wait(wait_timeout)
            self._sync_worker_epochs()
            if completed is None:
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
                raise RuntimeError(
                    f"TaskID {logical_id} failed: result={completed.result} "
                    f"exit={completed.exit_code} stdout={completed.output}"
                )
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
            self._worker_by_task[logical_id] = (
                preparation["worker_id"],
                preparation["worker_epoch"],
            )
            # Publication is the completion contract, not process exit alone.
            self.controller.fetch_idata(output_ids[logical_id])
            if persist_outputs:
                self.controller.persist_idata(output_ids[logical_id])
            done.add(logical_id)
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
                worker_id, worker_epoch = self._worker_by_task[logical_id]
                self.controller.disconnect_worker(
                    worker_id, worker_epoch
                )
                self.controller.invalidate_idata(
                    output_ids[logical_id]
                )
                worker_loss_injected = True
        if persist_outputs:
            for output_data_id in output_ids.values():
                self._wait_durable(output_data_id)
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
            url = (
                self.controller.endpoint
                + f"/v1/edata/{data_id}?"
                + urllib.parse.urlencode({"token": self.controller.token})
            )
            file_object = self._manager.declare_url(
                url, cache="worker", peer_transfer=True
            )
            self._edata_files[data_id] = file_object
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
