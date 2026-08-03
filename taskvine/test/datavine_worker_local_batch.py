#!/usr/bin/env python3

import os
from pathlib import Path
import tempfile

import cloudpickle

from ndcctools.taskvine.datavine.controller.service import ControllerService
from ndcctools.taskvine.datavine.models import TaskRecord
from ndcctools.taskvine.datavine.scheduler.client import ControllerClient
from ndcctools.taskvine.datavine.serialization import serialize
from ndcctools.taskvine.datavine.worker.cache import PROCESS_CACHE
from ndcctools.taskvine.datavine.worker.library import execute_datavine_tasks


def increment(value):
    return value + 1


def main():
    service = ControllerService("127.0.0.1", 0, "batch-token")
    host, port = service.start()
    endpoint = f"http://{host}:{port}"
    client = ControllerClient(endpoint, "batch-token")
    old_worker_id = os.environ.get("VINE_WORKER_ID")
    old_cwd = os.getcwd()
    PROCESS_CACHE.clear()
    try:
        function_metadata, function_payload = serialize(increment)
        function_id = client.register_edata(
            function_metadata, function_payload
        )["data_id"]
        values = []
        for value in (10, 20):
            metadata, payload = serialize(value)
            data_id = client.register_edata(metadata, payload)["data_id"]
            values.append((data_id, payload))
        output_ids = client.allocate_idata_batch(((1, 0), (2, 0)))
        tasks = tuple(
            TaskRecord(
                task_id,
                function_id,
                (("e", values[task_id - 1][0]),),
                (),
                (output_ids[task_id - 1],),
                (),
            )
            for task_id in (1, 2)
        )
        client.register_tasks(tasks)

        with tempfile.TemporaryDirectory() as workspace:
            os.chdir(workspace)
            Path(f"datavine-edata-{function_id}.pkl").write_bytes(
                function_payload
            )
            for data_id, payload in values:
                Path(f"datavine-edata-{data_id}.pkl").write_bytes(payload)
            os.environ["VINE_WORKER_ID"] = "worker-local-batch"
            result = execute_datavine_tasks(
                endpoint,
                "batch-token",
                tuple(
                    (
                        task.task_id,
                        1,
                        (f"datavine-idata-{task.output_data_id}.pkl",),
                    )
                    for task in tasks
                ),
                1024 * 1024,
            )
            assert result["protocol"] == "datavine-batch-v2"
            outputs = [
                output
                for task_result in result["tasks"]
                for output in task_result["outputs"]
            ]
            assert len(outputs) == 2
            assert all("payload" not in output for output in outputs)
            assert tuple(
                cloudpickle.loads(
                    Path(
                        f"datavine-idata-{data_id}.pkl"
                    ).read_bytes()
                )
                for data_id in output_ids
            ) == (11, 21)

            worker_client = PROCESS_CACHE.clients[(endpoint, "batch-token")]
            metrics = worker_client.request_metrics()
            assert metrics["POST /v1/tasks/get-batch"]["count"] == 1
            assert metrics["POST /v1/replicas/prepare-outputs"]["count"] == 1
            assert not any(
                route.startswith("POST /v1/idata/")
                for route in metrics
            )
            assert all(
                client.idata_status(data_id)["available"] is False
                for data_id in output_ids
            )
            client.commit_outputs(outputs)
            assert all(
                client.idata_status(data_id)["available"] is True
                for data_id in output_ids
            )
    finally:
        os.chdir(old_cwd)
        if old_worker_id is None:
            os.environ.pop("VINE_WORKER_ID", None)
        else:
            os.environ["VINE_WORKER_ID"] = old_worker_id
        PROCESS_CACHE.clear()
        service.stop()

    print("DataVine Worker-local batch contract PASS")


if __name__ == "__main__":
    main()
