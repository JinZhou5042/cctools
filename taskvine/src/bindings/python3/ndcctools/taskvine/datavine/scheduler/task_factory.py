"""TaskVine file declarations and physical task construction."""

import base64
import cloudpickle
import urllib.parse

from ndcctools.taskvine import Task
from ndcctools.taskvine import cvine


class DataVineCall(Task):
    """Function invocation carried entirely by TaskVine control frames."""

    def __init__(self, library, function, *args, **kwargs):
        super().__init__(function)
        self.set_library_required(library)
        self._invocation = cloudpickle.dumps(
            {
                "fn_args": args,
                "fn_kwargs": kwargs,
                "remote_task_exec_method": "direct",
            }
        )
        self._decoded_output = None

    def submit_finalize(self):
        if not self.manager.check_library_exists(
            self.get_library_required()
        ):
            raise ValueError("DataVine worker library is not installed")
        cvine.vine_task_set_function_input(
            self._task, self._invocation
        )
        self._invocation = None

    @property
    def output(self):
        if self._decoded_output is None:
            encoded = self.std_output
            envelope = cloudpickle.loads(
                base64.b64decode(encoded.encode("ascii"), validate=True)
            )
            self._decoded_output = (
                envelope["Result"]
                if envelope["Success"]
                else envelope["Reason"]
            )
        return self._decoded_output


def ensure_worker_library(manager):
    """Install the DataVine worker library once per Manager."""

    if manager.check_library_exists("datavine-worker-v2"):
        return
    from ..worker.library import (
        execute_datavine_task,
        persist_datavine_idata,
        warm_datavine_worker,
    )

    library = manager.create_library_from_functions(
        "datavine-worker-v2",
        execute_datavine_task,
        persist_datavine_idata,
        warm_datavine_worker,
        add_env=False,
        exec_mode="direct",
    )
    workers = manager.status("workers")
    library.set_function_slots(
        min(int(worker["cores_total"]) for worker in workers)
        if workers
        else 1
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
        worker_dram_cache_bytes,
        allow_peer_transfer=True,
    ):
        self.manager = manager
        self.controller = controller
        self.context = context
        self.task_record = task_record
        self.edata_files = edata_files
        self.idata_files = idata_files
        self.worker_dram_cache_bytes = int(worker_dram_cache_bytes)
        self.allow_peer_transfer = bool(allow_peer_transfer)

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
            file_object = self.manager.declare_url_cached(
                url,
                f"datavine-e-{data_id}-{info['serialized_sha256']}",
                cache="worker",
                peer_transfer=True,
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
        file_object = self.manager.declare_url_cached(
            url,
            f"datavine-i-{int(data_id)}-attempt-{int(attempt)}",
            cache="worker",
            peer_transfer=True,
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
    ):
        record = self.task_record(task_id)
        task = DataVineCall(
            "datavine-worker-v2",
            "execute_datavine_task",
            self.controller.endpoint,
            self.controller.token,
            task_id,
            attempt,
            self.worker_dram_cache_bytes,
            record.to_dict(),
            self.allow_peer_transfer,
        )
        task.set_tag(str(task_id))
        task.set_cores(1)
        task.set_retries(0)
        if environment is not None:
            task.add_environment(environment)
        return task

    def make_persistence_task(self, data_id, request, environment):
        task = DataVineCall(
            "datavine-worker-v2",
            "persist_datavine_idata",
            self.controller.endpoint,
            self.controller.token,
            int(data_id),
            request["request_id"],
            bool(request.get("inject_cancel_delay")),
            bool(request.get("inject_failure_during_write")),
            float(request.get("inject_failure_delay", 0)),
        )
        task.set_tag(f"persist-i{int(data_id)}")
        task.set_cores(0)
        task.set_priority(-500)
        task.set_retries(0)
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
