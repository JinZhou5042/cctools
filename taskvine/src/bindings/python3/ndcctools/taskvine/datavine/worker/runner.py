"""Worker entry point: resolve DataIDs, execute, and publish serialized output."""

import argparse
import cloudpickle
import copy
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
    task = client.get_task(args.task_id)
    objects = {}

    def fetch_edata(data_id):
        cache_path = Path(f"datavine-edata-{data_id}.pkl")
        if cache_path.is_file():
            payload = cache_path.read_bytes()
            info = client.get_edata_metadata(data_id)
            if (
                len(payload) != info["size"]
                or EDataRecord.digest(info["metadata"], payload)
                != info["content_hash"]
            ):
                # A worker cache or peer replica is soft state. Reject it and
                # fall back to the Controller-owned stable source.
                return client.fetch_edata_record(data_id)[1]
            return payload
        return client.fetch_edata_record(data_id)[1]

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
                import hashlib
                if (
                    len(payload) == 0
                    or hashlib.sha256(payload).hexdigest()
                    != status["content_hash"]
                ):
                    payload = client.fetch_idata(data_id)
                else:
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
        client.publish_idata(
            task.output_data_id, args.attempt, staged_payload
        )
    finally:
        if args.output_file is None:
            stage.unlink(missing_ok=True)
    print(
        f"DATAVINE task={task.task_id} output=i{task.output_data_id} bytes={len(payload)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
