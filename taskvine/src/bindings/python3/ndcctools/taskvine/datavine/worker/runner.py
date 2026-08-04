"""Resolve DataIDs on a worker, execute a task, and publish its output."""

import cloudpickle
import os
import time

from ..scheduler.client import ControllerClient
from .control import SourceResolver
from .cache import PROCESS_CACHE
from .inputs import InputResolver
from .publication import publish_task_outputs
from .replicas import WorkerReplicaReporter
from .service import WorkerDataService


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

    controller_key = (controller, token, native_controller)
    with PROCESS_CACHE.lock:
        client = PROCESS_CACHE.clients.get(controller_key)
        if client is None:
            client = ControllerClient(
                controller,
                token,
                transient_retries=8,
                native_endpoint=native_controller,
            )
            PROCESS_CACHE.clients[controller_key] = client
    retry_count_before = client.thread_transient_retry_count
    worker_id = os.environ.get("VINE_WORKER_ID")
    if not worker_id:
        raise RuntimeError("TaskVine worker incarnation is unavailable")
    PROCESS_CACHE.disk.configure(worker_id)
    claim_key = (controller, token, worker_id)
    with PROCESS_CACHE.lock:
        if PROCESS_CACHE.data_service is None:
            PROCESS_CACHE.data_service = WorkerDataService(PROCESS_CACHE)
        source_endpoint = PROCESS_CACHE.data_service.endpoint(
            controller, token
        )
        worker_epoch = PROCESS_CACHE.worker_claims.get(claim_key)
        if worker_epoch is None:
            worker_epoch = int(
                client.claim_worker(worker_id, source_endpoint)["epoch"]
            )
            PROCESS_CACHE.worker_claims[claim_key] = worker_epoch
    task_key = (controller, token, task_id)
    task = PROCESS_CACHE.task_records.get(task_key)
    if task is None:
        task = client.get_task(task_id)
        PROCESS_CACHE.task_records[task_key] = task
    reporter = WorkerReplicaReporter(
        client,
        controller,
        token,
        worker_id,
        worker_epoch,
        source_endpoint,
        emit,
        PROCESS_CACHE,
    )
    resolver_key = controller_key + (worker_id, worker_epoch)
    with PROCESS_CACHE.lock:
        source_resolver = PROCESS_CACHE.source_resolvers.get(resolver_key)
        if source_resolver is None:
            source_resolver = SourceResolver(client)
            PROCESS_CACHE.source_resolvers[resolver_key] = source_resolver
    resolver = InputResolver(
        controller,
        token,
        client,
        reporter,
        PROCESS_CACHE,
        emit,
        source_resolver,
        cache_values,
        allow_peer_transfer,
        transfer_faults=transfer_faults,
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
        worker_id,
        worker_epoch,
        emit,
        capture_output,
    )
    publication_done = time.monotonic()

    if timings is not None:
        timings.update(
            {
                "setup": setup_done - started,
                "input_resolution": inputs_done - setup_done,
                "compute": compute_done - inputs_done,
                "local_publication": publication_done - compute_done,
            }
        )

    emit(
        "DATAVINE_CONTROLLER_RETRIES "
        f"{client.thread_transient_retry_count - retry_count_before}"
    )
    return 0
