"""Resolve DataIDs on a worker, execute a task, and publish its output."""

import cloudpickle
import dataclasses
import os
import time

from ..client import ControllerClient
from .control import SourceResolver
from .cache import PROCESS_CACHE
from .inputs import InputResolver
from .publication import publish_task_outputs
from .replicas import WorkerReplicaReporter
from .service import WorkerDataService


@dataclasses.dataclass(frozen=True)
class WorkerRuntime:
    client: ControllerClient
    worker_id: str
    worker_epoch: int
    source_endpoint: str
    source_resolver: SourceResolver


def initialize_worker(controller, token, native_controller):
    controller = str(controller)
    token = str(token)
    worker_id = os.environ.get("VINE_WORKER_ID")
    if not worker_id:
        raise RuntimeError("TaskVine worker incarnation is unavailable")
    controller_key = (controller, token, native_controller)
    runtime_key = controller_key + (worker_id,)
    with PROCESS_CACHE.context_lock:
        runtime = PROCESS_CACHE.runtimes.get(runtime_key)
        if runtime is not None:
            return runtime
        client = PROCESS_CACHE.clients.get(controller_key)
        if client is None:
            client = ControllerClient(
                controller,
                token,
                transient_retries=8,
                native_endpoint=native_controller,
            )
            PROCESS_CACHE.clients[controller_key] = client
        PROCESS_CACHE.disk.configure(worker_id)
        if PROCESS_CACHE.data_service is None:
            PROCESS_CACHE.data_service = WorkerDataService(PROCESS_CACHE)
        source_endpoint = PROCESS_CACHE.data_service.endpoint(
            controller, token
        )
        worker_epoch = int(
            client.claim_worker(worker_id, source_endpoint)["epoch"]
        )
        runtime = WorkerRuntime(
            client,
            worker_id,
            worker_epoch,
            source_endpoint,
            SourceResolver(client),
        )
        PROCESS_CACHE.runtimes[runtime_key] = runtime
        return runtime


def execute_task(
    controller,
    token,
    native_controller,
    task_id,
    attempt=1,
    emit=print,
    capture_output=None,
    cache_values=None,
    timings=None,
    allow_peer_transfer=True,
    transfer_faults=False,
):
    started = time.monotonic()
    controller = str(controller)
    token = str(token)
    task_id = int(task_id)
    attempt = int(attempt)
    if attempt < 1:
        raise ValueError("attempt must be positive")

    runtime = initialize_worker(controller, token, native_controller)
    client = runtime.client
    runtime_ready = time.monotonic()
    task_key = (controller, token, task_id)
    task = PROCESS_CACHE.task_records.get(task_key)
    if task is None:
        task = client.get_task(task_id)
        PROCESS_CACHE.task_records[task_key] = task
    reporter = WorkerReplicaReporter(
        client,
        controller,
        token,
        runtime.worker_id,
        runtime.worker_epoch,
        runtime.source_endpoint,
        emit,
        PROCESS_CACHE,
    )
    resolver = InputResolver(
        controller,
        token,
        client,
        reporter,
        PROCESS_CACHE,
        emit,
        runtime.source_resolver,
        cache_values,
        allow_peer_transfer,
        transfer_faults=transfer_faults,
        timings=timings,
    )
    setup_done = time.monotonic()
    function_key = (
        controller,
        token,
        task.function_data_id,
    )
    with PROCESS_CACHE.lock:
        function = PROCESS_CACHE.functions.get(function_key)
    if function is None:
        function = cloudpickle.loads(
            resolver.fetch_edata(task.function_data_id)
        )
        with PROCESS_CACHE.lock:
            PROCESS_CACHE.functions[function_key] = function

    positional = [resolver.resolve(binding) for binding in task.positional]
    keyword = {
        name: resolver.resolve(binding) for name, binding in task.keyword
    }
    inputs_done = time.monotonic()
    result = function(*positional, **keyword)
    compute_done = time.monotonic()
    publish_task_outputs(
        task,
        result,
        attempt,
        reporter,
        runtime.worker_id,
        runtime.worker_epoch,
        capture_output,
        timings,
    )
    publication_done = time.monotonic()

    if timings is not None:
        timings.update(
            {
                "setup": setup_done - started,
                "setup_runtime": runtime_ready - started,
                "setup_task": setup_done - runtime_ready,
                "input_resolution": inputs_done - setup_done,
                "compute": compute_done - inputs_done,
                "local_publication": publication_done - compute_done,
            }
        )

    return 0
