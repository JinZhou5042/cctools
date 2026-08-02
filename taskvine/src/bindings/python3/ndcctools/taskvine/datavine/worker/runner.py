"""Resolve DataIDs on a worker, execute a task, and publish its output."""

import argparse
import cloudpickle
import copy
import hashlib
import json
import os
from pathlib import Path
import threading
import time

from ..models import EDataRecord
from ..scheduler.client import ControllerClient
from ..workflow import iter_output_refs


# These caches live only for the lifetime of one Python execution process.
# They are especially useful in the persistent TaskVine library, while the
# legacy one-process-per-task path naturally starts with empty caches.
_CLIENTS = {}
_WORKER_CLAIMS = {}
_TASK_RECORDS = {}
_EDATA_METADATA = {}
_REPLICA_REPORTS = {}
_CACHE_LOCK = threading.RLock()


def main(argv=None, emit=print):
    parser = argparse.ArgumentParser(prog="datavine_worker_runner")
    parser.add_argument("--controller", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--task-id", required=True, type=int)
    parser.add_argument("--attempt", default=1, type=int)
    parser.add_argument("--output-file", action="append", default=[])
    parser.add_argument(
        "--pause-after-output-index",
        default=-1,
        type=int,
    )
    parser.add_argument(
        "--idata-inline-threshold",
        default=8 * 1024 * 1024,
        type=int,
    )
    args = parser.parse_args(argv)
    if args.idata_inline_threshold < 0:
        raise ValueError("IData inline threshold cannot be negative")

    controller_key = (args.controller, args.token)
    with _CACHE_LOCK:
        client = _CLIENTS.get(controller_key)
        if client is None:
            client = ControllerClient(args.controller, args.token)
            _CLIENTS[controller_key] = client
    worker_id = os.environ.get("VINE_WORKER_ID")
    if not worker_id:
        raise RuntimeError("TaskVine worker incarnation is unavailable")
    claim_key = (args.controller, args.token, worker_id)
    with _CACHE_LOCK:
        worker_epoch = _WORKER_CLAIMS.get(claim_key)
        if worker_epoch is None:
            worker_epoch = int(client.claim_worker(worker_id)["epoch"])
            _WORKER_CLAIMS[claim_key] = worker_epoch
    task_key = (args.controller, args.token, args.task_id)
    task = _TASK_RECORDS.get(task_key)
    if task is None:
        task = client.get_task(args.task_id)
        _TASK_RECORDS[task_key] = task
    objects = {}

    def replica_id(data_key):
        return (
            f"taskvine-{worker_id}-{data_key.replace(':', '-')}"
        )

    def report_local(data_key, attempt, content_hash, payload):
        report_key = (
            args.controller,
            args.token,
            worker_id,
            worker_epoch,
            data_key,
            int(attempt),
            content_hash,
            len(payload),
        )
        cached_report = _REPLICA_REPORTS.get(report_key)
        if (
            cached_report is None
            or time.monotonic() - cached_report[0] >= 1.0
        ):
            replica = client.report_replica(
                data_key,
                replica_id(data_key),
                attempt,
                "worker-disk",
                content_hash,
                len(payload),
                worker_id,
                worker_epoch,
            )
            _REPLICA_REPORTS[report_key] = (
                time.monotonic(),
                replica,
            )
        else:
            replica = cached_report[1]
        emit(
            "DATAVINE_REPLICA_OBSERVED "
            + json.dumps(
                {
                    "data_id": replica["data_id"],
                    "replica_id": replica["replica_id"],
                    "generation": replica["generation"],
                    "attempt": replica["attempt"],
                    "content_hash": replica["content_hash"],
                    "size": replica["size"],
                    "worker_id": worker_id,
                    "worker_epoch": worker_epoch,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )

    def reject_reported_local(data_key):
        local_replica_id = replica_id(data_key)
        for source in client.replica_sources(data_key)["sources"]:
            if source["replica_id"] == local_replica_id:
                client.invalidate_replica(
                    data_key,
                    local_replica_id,
                    source["generation"],
                    worker_id,
                    worker_epoch,
                )
                for report_key in tuple(_REPLICA_REPORTS):
                    if (
                        report_key[2] == worker_id
                        and report_key[4] == data_key
                    ):
                        _REPLICA_REPORTS.pop(report_key, None)
                return

    def fetch_edata(data_id):
        metadata_key = (args.controller, args.token, int(data_id))
        info = _EDATA_METADATA.get(metadata_key)
        if info is None:
            info = client.get_edata_metadata(data_id)
            _EDATA_METADATA[metadata_key] = info

        def fallback():
            if info["storage"] != "bulk-origin":
                return client.fetch_edata_record(data_id)[1]
            origin = Path(info["origin_path"])
            payload = origin.read_bytes()
            if (
                len(payload) != info["size"]
                or EDataRecord.digest(info["metadata"], payload)
                != info["content_hash"]
            ):
                raise RuntimeError(
                    f"EDataID {data_id} bulk origin checksum mismatch"
                )
            emit(f"DATAVINE_BULK_ORIGIN e{data_id}")
            return payload

        cache_path = Path(f"datavine-edata-{data_id}.pkl")
        if cache_path.is_file():
            payload = cache_path.read_bytes()
            if (
                len(payload) != info["size"]
                or EDataRecord.digest(info["metadata"], payload)
                != info["content_hash"]
            ):
                # A worker cache or peer replica is soft state. Reject it and
                # fall back to the Controller-owned stable source.
                reject_reported_local(f"e:{data_id}")
                return fallback()
            report_local(
                f"e:{data_id}", 1, info["content_hash"], payload
            )
            return payload
        return fallback()

    function = cloudpickle.loads(fetch_edata(task.function_data_id))

    def resolve(binding):
        kind, data_id = binding
        key = (kind, data_id)
        if key in objects:
            return objects[key]
        if kind == "e":
            payload = fetch_edata(data_id)
        elif kind == "c":
            template = cloudpickle.loads(fetch_edata(data_id))
            memo = {}
            for reference in iter_output_refs(template):
                producer_key = (
                    args.controller,
                    args.token,
                    reference.producer_task_id,
                )
                producer = _TASK_RECORDS.get(producer_key)
                if producer is None:
                    producer = client.get_task(
                        reference.producer_task_id
                    )
                    _TASK_RECORDS[producer_key] = producer
                memo[id(reference)] = resolve(
                    (
                        "i",
                        producer.output_data_ids[
                            reference.output_index
                        ],
                    )
                )
            objects[key] = copy.deepcopy(template, memo)
            return objects[key]
        elif kind == "i":
            cache_path = Path(f"datavine-idata-{data_id}.pkl")
            if cache_path.is_file():
                payload = cache_path.read_bytes()
                status = client.idata_status(data_id)
                if (
                    hashlib.sha256(payload).hexdigest()
                    != status["content_hash"]
                ):
                    reject_reported_local(f"i:{data_id}")
                    payload = client.fetch_idata(data_id)
                else:
                    report_local(
                        f"i:{data_id}",
                        status["attempt"],
                        status["content_hash"],
                        payload,
                    )
                    emit(f"DATAVINE_LOCAL_IDATA i{data_id}")
            else:
                payload = client.fetch_idata(data_id)
        else:
            raise ValueError(f"unknown binding kind {kind}")
        objects[key] = cloudpickle.loads(payload)
        return objects[key]

    positional = [resolve(binding) for binding in task.positional]
    keyword = {
        name: resolve(binding) for name, binding in task.keyword
    }
    result = function(*positional, **keyword)
    if len(task.output_data_ids) == 1:
        output_values = (result,)
    else:
        if not isinstance(result, (tuple, list)):
            raise TypeError(
                f"TaskID {task.task_id} declared "
                f"{len(task.output_data_ids)} outputs but returned "
                f"{type(result).__name__}"
            )
        if len(result) != len(task.output_data_ids):
            raise ValueError(
                f"TaskID {task.task_id} declared "
                f"{len(task.output_data_ids)} outputs but returned "
                f"{len(result)} values"
            )
        output_values = tuple(result)
    if args.output_file and len(args.output_file) != len(
        task.output_data_ids
    ):
        raise ValueError(
            "output file count does not match logical output count"
        )
    total_bytes = 0

    for output_index, (output_data_id, output_value) in enumerate(
        zip(task.output_data_ids, output_values)
    ):
        payload = cloudpickle.dumps(output_value)
        total_bytes += len(payload)
        stage = Path(
            args.output_file[output_index]
            if args.output_file
            else f".datavine-idata-{output_data_id}.stage"
        )
        with stage.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        staged_payload = stage.read_bytes()
        if len(staged_payload) <= args.idata_inline_threshold:
            publication = client.publish_idata(
                output_data_id, args.attempt, staged_payload
            )
        else:
            publication = client.publish_idata_metadata(
                output_data_id,
                args.attempt,
                hashlib.sha256(staged_payload).hexdigest(),
                len(staged_payload),
            )
        prepared = client.prepare_replica(
            f"i:{output_data_id}",
            replica_id(f"i:{output_data_id}"),
            args.attempt,
            "worker-disk",
            publication["content_hash"],
            publication["size"],
            worker_id,
            worker_epoch,
        )
        emit(
            "DATAVINE_REPLICA_PREPARED "
            + json.dumps(
                {
                    "data_id": prepared["data_id"],
                    "output_index": output_index,
                    "replica_id": prepared["replica_id"],
                    "generation": prepared["generation"],
                    "attempt": prepared["attempt"],
                    "content_hash": prepared["content_hash"],
                    "size": prepared["size"],
                    "worker_id": worker_id,
                    "worker_epoch": worker_epoch,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        if not args.output_file:
            stage.unlink(missing_ok=True)
        emit(
            "DATAVINE "
            f"task={task.task_id} slot={output_index} "
            f"output=i{output_data_id} bytes={len(payload)}"
        )
        if output_index == args.pause_after_output_index:
            time.sleep(30)
    emit(
        f"DATAVINE_OUTPUTS task={task.task_id} "
        f"count={len(task.output_data_ids)} bytes={total_bytes}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
