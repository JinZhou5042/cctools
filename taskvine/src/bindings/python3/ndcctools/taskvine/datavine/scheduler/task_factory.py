"""TaskVine file declarations and physical task construction."""

import shlex
import urllib.parse


def ensure_worker_library(manager):
    """Install the DataVine worker library once per Manager."""

    if manager.check_library_exists("datavine-worker-v2"):
        return
    from ..worker.library import (
        execute_datavine_task,
        execute_datavine_tasks,
    )

    library = manager.create_library_from_functions(
        "datavine-worker-v2",
        execute_datavine_task,
        execute_datavine_tasks,
        add_env=False,
        exec_mode="fork",
    )
    manager.install_library(library)


class TaskFactory:
    """Build physical compute and persistence tasks for one workflow run."""

    def __init__(
        self,
        manager,
        controller,
        context,
        task_record,
        edata_files,
        idata_files,
    ):
        self.manager = manager
        self.controller = controller
        self.context = context
        self.task_record = task_record
        self.edata_files = edata_files
        self.idata_files = idata_files

    def edata_file(self, data_id):
        file_object = self.edata_files.get(data_id)
        if file_object is not None:
            return file_object
        info = self.context.edata_info.get(data_id)
        if info is None or (
            info.get("storage") == "bulk-origin"
            and not info.get("origin_path")
        ):
            info = self.controller.get_edata_metadata(data_id)
            self.context.edata_info[data_id] = info
        if info["storage"] == "bulk-origin":
            file_object = self.manager.declare_file(
                info["origin_path"], cache="worker", peer_transfer=True
            )
        else:
            url = (
                self.controller.endpoint
                + f"/v1/edata/{data_id}?"
                + urllib.parse.urlencode(
                    {"token": self.controller.token}
                )
            )
            file_object = self.manager.declare_url(
                url, cache="worker", peer_transfer=True
            )
        self.edata_files[data_id] = file_object
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

    def inline_idata_file(self, data_id, content_hash):
        url = (
            self.controller.endpoint
            + f"/v1/idata/{int(data_id)}?"
            + urllib.parse.urlencode({"token": self.controller.token})
        )
        file_object = self.manager.declare_url(
            url, cache="worker", peer_transfer=True
        )
        if not file_object.set_datavine_data_id(f"i:{int(data_id)}"):
            raise RuntimeError(
                f"could not bind TaskVine file to IDataID i{int(data_id)}"
            )
        if not file_object.set_datavine_content_hash(str(content_hash)):
            raise RuntimeError(
                f"could not bind IDataID i{int(data_id)} content hash"
            )
        return file_object

    def idata_output_file(self, data_id, attempt):
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
        file_object = self.manager.declare_url(
            url, cache="worker", peer_transfer=True
        )
        if not file_object.set_datavine_data_id(f"i:{int(data_id)}"):
            raise RuntimeError(
                f"could not bind TaskVine file to IDataID i:{data_id}"
            )
        self.idata_files[int(data_id)] = file_object
        return file_object

    def make_physical_task(
        self,
        task_id,
        environment,
        attempt,
        idata_inline_threshold,
        kill_worker_after_output_index=None,
        use_worker_library=False,
    ):
        from ndcctools.taskvine import FunctionCall, Task

        record = self.task_record(task_id)
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
        if (
            use_worker_library
            and kill_worker_after_output_index is None
        ):
            task = FunctionCall(
                "datavine-worker-v2",
                "execute_datavine_task",
                self.controller.endpoint,
                self.controller.token,
                task_id,
                attempt,
                output_names,
                idata_inline_threshold,
            )
            task.set_exec_method("fork")
        else:
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
            task.add_input(
                self.edata_file(data_id),
                f"datavine-edata-{data_id}.pkl",
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
        idata_ids.update(
            self.context.nested_idata_by_task.get(task_id, ())
        )
        for data_id in sorted(idata_ids):
            task.add_input(
                self.idata_files[data_id],
                f"datavine-idata-{data_id}.pkl",
            )
        for output_data_id, output_name in zip(
            record.output_data_ids, output_names
        ):
            task.add_output(
                self.idata_output_file(output_data_id, attempt),
                output_name,
            )
        if environment is not None:
            task.add_environment(environment)
        return task

    def make_physical_batch_task(
        self,
        task_ids,
        environment,
        attempts,
        idata_inline_threshold,
        use_worker_library,
    ):
        task_ids = tuple(task_ids)
        attempts = tuple(attempts)
        if len(task_ids) == 1:
            return self.make_physical_task(
                task_ids[0],
                environment,
                attempts[0],
                idata_inline_threshold,
                None,
                use_worker_library,
            )
        if not use_worker_library:
            raise RuntimeError("process tasks cannot be physically batched")

        from ndcctools.taskvine import FunctionCall

        calls = []
        edata_ids = set()
        idata_ids = set()
        for task_id, attempt in zip(task_ids, attempts):
            record = self.task_record(task_id)
            output_names = tuple(
                f"datavine-idata-{data_id}.pkl"
                for data_id in record.output_data_ids
            )
            calls.append((task_id, attempt, output_names))
            edata_ids.add(record.function_data_id)
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
            idata_ids.update(
                data_id
                for kind, data_id in record.positional
                if kind == "i"
            )
            idata_ids.update(
                data_id
                for _, (kind, data_id) in record.keyword
                if kind == "i"
            )
            idata_ids.update(
                self.context.nested_idata_by_task.get(task_id, ())
            )

        task = FunctionCall(
            "datavine-worker-v2",
            "execute_datavine_tasks",
            self.controller.endpoint,
            self.controller.token,
            calls,
            {
                data_id: self.context.edata_payloads[data_id]
                for data_id in edata_ids
                if data_id in self.context.edata_payloads
            },
            {
                task_id: self.task_record(task_id).to_dict()
                for task_id in task_ids
            },
            idata_inline_threshold,
        )
        task.set_exec_method("fork")
        task.set_tag(",".join(map(str, task_ids)))
        task.set_cores(1)
        task.set_retries(5)
        for data_id in sorted(edata_ids):
            if data_id in self.context.edata_payloads:
                continue
            task.add_input(
                self.edata_file(data_id),
                f"datavine-edata-{data_id}.pkl",
            )
        for data_id in sorted(idata_ids):
            task.add_input(
                self.idata_files[data_id],
                f"datavine-idata-{data_id}.pkl",
            )
        if environment is not None:
            task.add_environment(environment)
        return task

    def make_persistence_task(self, data_id, request, environment):
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
        task.add_input(self.idata_files[int(data_id)], input_name)
        if environment is not None:
            task.add_environment(environment)
        return task

    def durable_idata_file(self, data_id, status):
        file_object = self.manager.declare_file(
            status["durable_path"], cache="worker", peer_transfer=True
        )
        if not file_object.set_datavine_data_id(f"i:{int(data_id)}"):
            raise RuntimeError(
                f"could not bind durable IDataID i:{data_id}"
            )
        if not file_object.set_datavine_content_hash(
            status["content_hash"]
        ):
            raise RuntimeError(
                f"could not bind durable IDataID i:{data_id} content hash"
            )
        return file_object
