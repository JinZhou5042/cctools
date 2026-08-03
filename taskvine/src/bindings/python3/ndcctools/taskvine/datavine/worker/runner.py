"""Resolve DataIDs on a worker, execute a task, and publish its output."""

import cloudpickle
import os

from ..scheduler.client import ControllerClient
from .arguments import parse_worker_arguments
from .cache import PROCESS_CACHE
from .inputs import InputResolver
from .publication import publish_task_outputs
from .replicas import WorkerReplicaReporter


def main(
    argv=None,
    emit=print,
    capture_output=None,
    trust_taskvine_inputs=False,
    cache_values=None,
):
    args = parse_worker_arguments(argv)

    controller_key = (args.controller, args.token)
    with PROCESS_CACHE.lock:
        client = PROCESS_CACHE.clients.get(controller_key)
        if client is None:
            client = ControllerClient(
                args.controller,
                args.token,
                transient_retries=8,
            )
            PROCESS_CACHE.clients[controller_key] = client
    retry_count_before = client.thread_transient_retry_count
    worker_id = os.environ.get("VINE_WORKER_ID")
    if not worker_id:
        raise RuntimeError("TaskVine worker incarnation is unavailable")
    claim_key = (args.controller, args.token, worker_id)
    with PROCESS_CACHE.lock:
        worker_epoch = PROCESS_CACHE.worker_claims.get(claim_key)
        if worker_epoch is None:
            worker_epoch = int(client.claim_worker(worker_id)["epoch"])
            PROCESS_CACHE.worker_claims[claim_key] = worker_epoch
    task_key = (args.controller, args.token, args.task_id)
    task = PROCESS_CACHE.task_records.get(task_key)
    if task is None:
        task = client.get_task(args.task_id)
        PROCESS_CACHE.task_records[task_key] = task
    reporter = WorkerReplicaReporter(
        client,
        args.controller,
        args.token,
        worker_id,
        worker_epoch,
        emit,
        PROCESS_CACHE,
    )
    resolver = InputResolver(
        args.controller,
        args.token,
        client,
        reporter,
        PROCESS_CACHE,
        emit,
        trust_taskvine_inputs,
        cache_values,
    )
    function = cloudpickle.loads(
        resolver.fetch_edata(task.function_data_id)
    )

    positional = [resolver.resolve(binding) for binding in task.positional]
    keyword = {
        name: resolver.resolve(binding) for name, binding in task.keyword
    }
    result = function(*positional, **keyword)
    publish_task_outputs(
        task,
        result,
        args,
        client,
        reporter,
        worker_id,
        worker_epoch,
        emit,
        capture_output,
    )

    emit(
        "DATAVINE_CONTROLLER_RETRIES "
        f"{client.thread_transient_retry_count - retry_count_before}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
