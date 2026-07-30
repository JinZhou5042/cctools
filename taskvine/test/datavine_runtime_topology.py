#!/usr/bin/env python3

import json
import os
from pathlib import Path
import platform
import pickle
import subprocess
import sys
import tempfile
import time

import cloudpickle

from ndcctools.taskvine.datavine import (
    ControllerClient,
    SerializationMetadata,
    TaskSchedulerThread,
    TaskRecord,
)
from ndcctools.taskvine.datavine.serialization import serialize
from ndcctools.taskvine.datavine.protocol import DataVineRemoteError


def metadata_for(value):
    value_type = type(value)
    return SerializationMetadata(
        serializer="cloudpickle",
        serializer_version=cloudpickle.__version__,
        protocol=pickle.HIGHEST_PROTOCOL,
        python_implementation=platform.python_implementation(),
        python_version=(sys.version_info.major, sys.version_info.minor),
        type_module=value_type.__module__,
        type_qualname=value_type.__qualname__,
    )


def increment(value):
    return value + 1


def wait_for_json(path, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size:
            return json.loads(path.read_text())
        time.sleep(0.05)
    raise TimeoutError(f"Controller did not create {path}")


def main():
    with tempfile.TemporaryDirectory(
        prefix="datavine-runtime-topology-"
    ) as temp_dir:
        ready_file = Path(temp_dir) / "controller-ready.json"
        token = "phase4b-component-token"
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "ndcctools.taskvine.datavine.controller.cli",
                "--host",
                "127.0.0.1",
                "--port",
                "0",
                "--token",
                token,
                "--max-edata-bytes",
                "1048576",
                "--ready-file",
                str(ready_file),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        scheduler = None
        try:
            ready = wait_for_json(ready_file)
            assert ready["pid"] == process.pid
            assert ready["pid"] != os.getpid()
            assert ready["controller_thread"]
            endpoint = f"http://127.0.0.1:{ready['port']}"
            client = ControllerClient(endpoint, token)
            health = client.health()
            assert health["status"] == "ready"
            assert health["controller_thread"] == ready["controller_thread"]

            scheduler = TaskSchedulerThread(client).start()
            assert scheduler.thread_ident is not None
            assert scheduler.thread_ident != ready["controller_thread"]
            value = {"shared": [1, 2, 3]}
            payload = cloudpickle.dumps(
                value, protocol=pickle.HIGHEST_PROTOCOL
            )
            metadata = metadata_for(value)
            first = scheduler.call(
                "register_edata", metadata, payload
            )
            second = scheduler.call(
                "register_edata", metadata, payload
            )
            assert first["data_id"] == second["data_id"] == 1
            assert client.fetch_edata(1, metadata) == payload
            snapshot = scheduler.call("controller_snapshot")
            assert snapshot["edata"] == 1
            assert snapshot["registrations"] == 2
            assert snapshot["deduplicated_registrations"] == 1

            try:
                ControllerClient(endpoint, "wrong-token").health()
            except DataVineRemoteError:
                pass
            else:
                raise AssertionError("Controller accepted wrong token")

            function_metadata, function_payload = serialize(increment)
            function_id = client.register_edata(
                function_metadata, function_payload
            )["data_id"]
            argument_metadata, argument_payload = serialize(41)
            argument_id = client.register_edata(
                argument_metadata, argument_payload
            )["data_id"]
            output_id = client.allocate_idata(1)
            client.register_task(
                TaskRecord(
                    1,
                    function_id,
                    (("e", argument_id),),
                    (),
                    output_id,
                )
            )
            (Path(temp_dir) / f"datavine-edata-{function_id}.pkl").write_bytes(
                b"corrupt-peer-replica"
            )
            fallback = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "ndcctools.taskvine.datavine.worker.runner",
                    "--controller",
                    endpoint,
                    "--token",
                    token,
                    "--task-id",
                    "1",
                ],
                cwd=temp_dir,
                text=True,
                capture_output=True,
                timeout=30,
            )
            assert fallback.returncode == 0, fallback.stderr
            assert cloudpickle.loads(client.fetch_idata(output_id)) == 42
        finally:
            if scheduler is not None:
                scheduler.stop()
            process.terminate()
            stdout, stderr = process.communicate(timeout=15)
            if process.returncode != 0:
                raise AssertionError(
                    f"Controller exit={process.returncode}\n"
                    f"stdout={stdout}\nstderr={stderr}"
                )

    print("DataVine standalone runtime topology component test PASS")


if __name__ == "__main__":
    main()
