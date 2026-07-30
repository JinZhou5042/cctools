"""Resolve DataIDs on a worker, execute a task, and publish its output."""

import argparse
import cloudpickle
import copy
import hashlib
import json
import os
from pathlib import Path

from ..models import EDataRecord
from ..scheduler.client import ControllerClient
from ..workflow import iter_output_refs


def main(argv=None):
    parser = argparse.ArgumentParser(prog="datavine_worker_runner")
    parser.add_argument("--controller", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--task-id", required=True, type=int)
    parser.add_argument("--attempt", default=1, type=int)
    parser.add_argument("--output-file")
    args = parser.parse_args(argv)

    client = ControllerClient(args.controller, args.token)
    worker_id = os.environ.get("VINE_WORKER_ID")
    if not worker_id:
        raise RuntimeError("TaskVine worker incarnation is unavailable")
    worker_epoch = 1
    client.join_worker(worker_id, worker_epoch)
    task = client.get_task(args.task_id)
    objects = {}

    def replica_id(data_key):
        return (
            f"taskvine-{worker_id}-{data_key.replace(':', '-')}"
        )

    def report_local(data_key, attempt, content_hash, payload):
        client.report_replica(
            data_key,
            replica_id(data_key),
            attempt,
            "worker-disk",
            content_hash,
            len(payload),
            worker_id,
            worker_epoch,
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
                return

    def fetch_edata(data_id):
        info = client.get_edata_metadata(data_id)

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
            print(f"DATAVINE_BULK_ORIGIN e{data_id}")
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
                producer = client.get_task(reference.producer_task_id)
                memo[id(reference)] = resolve(
                    ("i", producer.output_data_id)
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
                    print(f"DATAVINE_LOCAL_IDATA i{data_id}")
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
    payload = cloudpickle.dumps(result)
    stage = Path(
        args.output_file
        or f".datavine-idata-{task.output_data_id}.stage"
    )
    try:
        with stage.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        staged_payload = stage.read_bytes()
        publication = client.publish_idata(
            task.output_data_id, args.attempt, staged_payload
        )
        prepared = client.prepare_replica(
            f"i:{task.output_data_id}",
            replica_id(f"i:{task.output_data_id}"),
            args.attempt,
            "worker-disk",
            publication["content_hash"],
            publication["size"],
            worker_id,
            worker_epoch,
        )
        print(
            "DATAVINE_REPLICA_PREPARED "
            + json.dumps(
                {
                    "data_id": prepared["data_id"],
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
    finally:
        if args.output_file is None:
            stage.unlink(missing_ok=True)
    print(
        "DATAVINE "
        f"task={task.task_id} output=i{task.output_data_id} "
        f"bytes={len(payload)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
