#!/usr/bin/env python3

import base64
import hashlib
import json
from pathlib import Path
import runpy
import signal
import subprocess
import sys
import tempfile
import time
from unittest import mock

from ndcctools.taskvine.datavine.workflow_client import (
    WorkflowClient,
    WorkflowClientError,
)
from ndcctools.taskvine.datavine.workflow import Workflow


def wait_for(client, workflow_id, expected, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = client.describe_workflow(workflow_id)
        if info["state"] in expected:
            return info
        time.sleep(0.1)
    raise AssertionError(
        f"workflow did not reach {expected}: "
        f"{client.describe_workflow(workflow_id)}"
    )


def assert_result_info(
    client,
    workflow_id,
    data_id,
    value,
    *,
    task_id,
    output_index,
    codec,
    requested=True,
    attempt=1,
):
    info = client.workflow_result_info(workflow_id, data_id)
    assert info == {
        "workflow_id": workflow_id,
        "data_id": data_id,
        "size": len(value),
        "sha256": hashlib.sha256(value).hexdigest(),
        "attempt": attempt,
        "producer_task_id": task_id,
        "producer_output_index": output_index,
        "requested": requested,
        "codec": {"name": codec, "version": "1"},
    }, info
    return info


def main():
    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    token = "workflow-execution-test-token"

    executor = runpy.run_path(
        str(repository / "taskvine/src/tools/datavine_python_executor")
    )
    frame_payload = bytes(range(256)) * 512
    writes = []

    def partial_write(fd, value):
        assert fd == 99
        chunk = bytes(value[:7])
        writes.append(chunk)
        return len(chunk)

    with mock.patch.object(executor["os"], "write", partial_write):
        executor["send_frame"](99, frame_payload)
    assert b"".join(writes) == (
        str(len(frame_payload)).encode() + b"\n" + frame_payload
    )
    workflow = {
        "schema": "datavine.workflow/v1",
        "workflow_id": "native-command-chain",
        "idempotency_key": "native-command-chain-v1",
        "mode": "sealed",
        "task_defaults": {
            "executor": {
                "kind": "command",
                "version": "1",
                "argv": ["/bin/cat", "{{data:1}}"],
            },
            "resources": {"cores": 1},
            "retry": {"maximum_attempts": 2},
        },
        "data_defaults": {"codec": {"name": "text/utf-8", "version": "1"}},
        "data": [
            {
                "data_id": 1,
                "origin": {
                    "kind": "inline",
                    "base64": base64.b64encode(b"hello native runtime\n").decode(),
                },
            },
            [2, 10, 0],
            [3, 20, 0],
        ],
        "tasks": [
            [10, [1], [2]],
            {
                "task_id": 20,
                "executor": {
                    "kind": "command",
                    "version": "1",
                    "argv": [
                        "/usr/bin/sed",
                        "y/abcdefghijklmnopqrstuvwxyz/ABCDEFGHIJKLMNOPQRSTUVWXYZ/",
                        "{{data:2}}",
                    ],
                },
                "inputs": [{"position": 0, "data_id": 2}],
                "output_data_ids": [3],
            },
        ],
        "requested_outputs": [3],
        "policy": {"maximum_tasks": 2, "maximum_edges": 1},
    }

    class UnsupportedRuntime:
        mutated = False

        def workflow_capabilities(self):
            return {"schema_versions": [], "executor_kinds": []}

        def submit_workflow(self, document):
            self.mutated = True

    preflight = Workflow("preflight-v1")
    preflight.request(preflight.command(["/bin/true"]))
    unsupported = UnsupportedRuntime()
    try:
        preflight.submit(unsupported)
    except RuntimeError as error:
        assert "no workflow mutation was attempted" in str(error)
    else:
        raise AssertionError("unsupported runtime was not rejected")
    assert not unsupported.mutated
    with tempfile.TemporaryDirectory(prefix="datavine-workflow-execution-") as root:
        root = Path(root)
        service = subprocess.Popen(
            (str(service_binary), "serve", str(root / "native.journal"), token),
            stdout=subprocess.PIPE,
            stderr=None,
            text=True,
        )
        worker = None
        try:
            line = service.stdout.readline()
            if not line:
                raise AssertionError("native workflow service exited before contact")
            contact = json.loads(line)
            worker = subprocess.Popen(
                (
                    str(worker_binary),
                    "--cores=1",
                    "--memory=512",
                    "--disk=512",
                    "--idle-timeout=120",
                    "localhost",
                    str(contact["manager_port"]),
                ),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            client = WorkflowClient(contact["endpoint"], token)
            submitted = client.submit_workflow(workflow)
            assert submitted["state"] in {"sealed", "running"}, submitted
            terminal = wait_for(
                client, workflow["workflow_id"], {"completed", "failed"}
            )
            assert terminal["state"] == "completed", (
                terminal,
                worker.stderr.read() if worker.poll() is not None else "",
            )
            event_types = [
                item["type"] for item in client.watch_workflow(workflow["workflow_id"])
            ]
            assert event_types == [
                "accepted",
                "started",
                "task_submitted",
                "task_completed",
                "task_submitted",
                "task_completed",
                "completed",
            ], event_types
            assert client.fetch_workflow_result(workflow["workflow_id"], 3) == (
                b"HELLO NATIVE RUNTIME\n"
            )
            result_info = assert_result_info(
                client,
                workflow["workflow_id"],
                3,
                b"HELLO NATIVE RUNTIME\n",
                task_id=20,
                output_index=0,
                codec="text/utf-8",
            )
            try:
                client.fetch_workflow_result(workflow["workflow_id"], 2)
            except WorkflowClientError as error:
                assert error.status == 5, error
            else:
                raise AssertionError("non-requested intermediate was not pruned")
        finally:
            if worker is not None and worker.poll() is None:
                worker.send_signal(signal.SIGTERM)
                worker.communicate(timeout=20)
            if service.poll() is None:
                service.send_signal(signal.SIGTERM)
            stdout, stderr = service.communicate(timeout=20)
            if service.returncode != 0:
                raise AssertionError((service.returncode, stdout, stderr))

        restarted = subprocess.Popen(
            (str(service_binary), "serve", str(root / "native.journal"), token),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        restarted_worker = None
        try:
            line = restarted.stdout.readline()
            if not line:
                raise AssertionError(restarted.stderr.read())
            contact = json.loads(line)
            restarted_worker = subprocess.Popen(
                (
                    str(worker_binary),
                    "--cores=1",
                    "--memory=512",
                    "--disk=512",
                    "--idle-timeout=120",
                    "localhost",
                    str(contact["manager_port"]),
                ),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            client = WorkflowClient(contact["endpoint"], token)
            assert client.describe_workflow(workflow["workflow_id"])["state"] == (
                "completed"
            )
            assert client.fetch_workflow_result(workflow["workflow_id"], 3) == (
                b"HELLO NATIVE RUNTIME\n"
            )
            assert (
                client.workflow_result_info(workflow["workflow_id"], 3)
                == result_info
            )
            try:
                client.workflow_result_info(workflow["workflow_id"], 2)
            except WorkflowClientError as error:
                assert error.status == 5, error
            else:
                raise AssertionError(
                    "pruned intermediate reappeared after journal replay"
                )

            multi = {
                "schema": "datavine.workflow/v1",
                "workflow_id": "native-multi-output",
                "idempotency_key": "native-multi-output-v1",
                "mode": "sealed",
                "tasks": [
                    {
                        "task_id": 1,
                        "executor": {
                            "kind": "command",
                            "version": "1",
                            "argv": [
                                "/bin/sh",
                                "-c",
                                "printf left > left; printf right > right",
                            ],
                            "output_files": ["left", "right"],
                        },
                        "inputs": [],
                        "output_data_ids": [1, 2],
                    },
                    {
                        "task_id": 2,
                        "executor": {
                            "kind": "command",
                            "version": "1",
                            "argv": [
                                "/bin/cat",
                                "{{data:1}}",
                                "{{data:2}}",
                            ],
                        },
                        "inputs": [
                            {"position": 0, "data_id": 1},
                            {"position": 1, "data_id": 2},
                        ],
                        "output_data_ids": [3],
                    },
                ],
                "data": [
                    {
                        "data_id": 1,
                        "codec": {"name": "bytes", "version": "1"},
                        "origin": {
                            "kind": "output",
                            "task_id": 1,
                            "output_index": 0,
                        },
                    },
                    {
                        "data_id": 2,
                        "codec": {"name": "bytes", "version": "1"},
                        "origin": {
                            "kind": "output",
                            "task_id": 1,
                            "output_index": 1,
                        },
                    },
                    {
                        "data_id": 3,
                        "codec": {"name": "bytes", "version": "1"},
                        "origin": {
                            "kind": "output",
                            "task_id": 2,
                            "output_index": 0,
                        },
                    },
                ],
                "requested_outputs": [1, 2, 3],
                "policy": {"maximum_tasks": 2, "maximum_edges": 2},
            }
            client.submit_workflow(multi)
            assert wait_for(client, multi["workflow_id"], {"completed", "failed"})[
                "state"
            ] == "completed"
            assert client.fetch_workflow_result(multi["workflow_id"], 1) == b"left"
            assert client.fetch_workflow_result(multi["workflow_id"], 2) == b"right"
            assert client.fetch_workflow_result(multi["workflow_id"], 3) == (
                b"leftright"
            )
            assert_result_info(
                client,
                multi["workflow_id"],
                1,
                b"left",
                task_id=1,
                output_index=0,
                codec="bytes",
            )
            assert_result_info(
                client,
                multi["workflow_id"],
                2,
                b"right",
                task_id=1,
                output_index=1,
                codec="bytes",
            )
            assert_result_info(
                client,
                multi["workflow_id"],
                3,
                b"leftright",
                task_id=2,
                output_index=0,
                codec="bytes",
            )

            adaptor = Workflow(
                "python-thin-adaptor-v1",
                workflow_id="python-thin-adaptor",
                maximum_tasks=1,
                maximum_edges=0,
            )
            adaptor_output = adaptor.command(
                ["/usr/bin/printf", "python-adaptor"]
            )
            adaptor.request(adaptor_output)
            adaptor_code = "\n".join(
                (
                    "import json,sys",
                    "from ndcctools.taskvine.datavine.workflow_client import WorkflowClient",
                    "from ndcctools.taskvine.datavine.workflow import Workflow",
                    "workflow=Workflow('python-thin-adaptor-v1',workflow_id='python-thin-adaptor',maximum_tasks=1,maximum_edges=0)",
                    "output=workflow.command(['/usr/bin/printf','python-adaptor'])",
                    "workflow.request(output)",
                    "client=WorkflowClient(sys.argv[1],sys.argv[2])",
                    "print(json.dumps(workflow.submit(client)))",
                )
            )
            submitted = subprocess.run(
                (
                    sys.executable,
                    "-c",
                    adaptor_code,
                    contact["endpoint"],
                    token,
                ),
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            assert len(json.loads(submitted.stdout)["digest"]) == 40
            assert wait_for(
                client, "python-thin-adaptor", {"completed", "failed"}
            )["state"] == "completed"
            assert client.fetch_workflow_result("python-thin-adaptor", 1) == (
                b"python-adaptor"
            )

            callable_code = "\n".join(
                (
                    "import json,sys",
                    "from ndcctools.taskvine.datavine.workflow_client import WorkflowClient",
                    "from ndcctools.taskvine.datavine.workflow import Workflow",
                    "workflow=Workflow('python-callable-adaptor-v1',workflow_id='python-callable-adaptor',maximum_tasks=1,maximum_edges=0)",
                    "value=workflow.python_value(41)",
                    "output=workflow.python_callable(lambda item: item + 1,value)",
                    "workflow.request(output)",
                    "workflow.submit(WorkflowClient(sys.argv[1],sys.argv[2]))",
                    "print(json.dumps({'output_data_id': output.data_id}))",
                )
            )
            callable_submitted = subprocess.run(
                (sys.executable, "-c", callable_code, contact["endpoint"], token),
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            callable_output_data_id = json.loads(
                callable_submitted.stdout
            )["output_data_id"]
            assert wait_for(
                client, "python-callable-adaptor", {"completed", "failed"}
            )["state"] == "completed"
            import cloudpickle

            assert cloudpickle.loads(
                client.fetch_workflow_result(
                    "python-callable-adaptor", callable_output_data_id
                )
            ) == 42

            python_executor = Workflow(
                "native-python-executor-v1",
                workflow_id="native-python-executor",
                maximum_tasks=1,
                maximum_edges=0,
            )
            python_input = python_executor.inline(b"opaque python payload")
            python_output = python_executor.python_source(
                "import os, pathlib\n"
                "value = pathlib.Path(os.environ['DATAVINE_DATA_1']).read_bytes()\n"
                "pathlib.Path('datavine-python-output-0').write_bytes(value.upper())\n",
                inputs=(python_input,),
            )
            python_executor.request(python_output).submit(client)
            assert wait_for(
                client, "native-python-executor", {"completed", "failed"}
            )["state"] == "completed"
            assert client.fetch_workflow_result("native-python-executor", 3) == (
                b"OPAQUE PYTHON PAYLOAD"
            )
            assert_result_info(
                client,
                "native-python-executor",
                3,
                b"OPAQUE PYTHON PAYLOAD",
                task_id=1,
                output_index=0,
                codec="bytes",
            )

            live = {
				"schema": "datavine.workflow/v1",
				"workflow_id": "live-result-before-workflow-end",
				"idempotency_key": "live-result-before-workflow-end-v1",
				"mode": "sealed",
				"tasks": [
					{
						"task_id": 1,
						"executor": {"kind": "command", "version": "1", "argv": ["/usr/bin/printf", "ready-now"]},
						"inputs": [],
						"output_data_ids": [1],
					},
					{
						"task_id": 2,
						"executor": {"kind": "command", "version": "1", "argv": ["/bin/sh", "-c", "sleep 2; printf done"]},
						"inputs": [{"position": 0, "data_id": 1}],
						"output_data_ids": [2],
					},
				],
				"data": [
					{"data_id": 1, "codec": {"name": "bytes", "version": "1"}, "origin": {"kind": "output", "task_id": 1, "output_index": 0}},
					{"data_id": 2, "codec": {"name": "bytes", "version": "1"}, "origin": {"kind": "output", "task_id": 2, "output_index": 0}},
				],
				"requested_outputs": [1, 2],
				"policy": {"maximum_tasks": 2, "maximum_edges": 1},
			}
            client.submit_workflow(live)
            deadline = time.monotonic() + 10
            while True:
                try:
                    assert client.fetch_workflow_result(live["workflow_id"], 1) == b"ready-now"
                    break
                except WorkflowClientError as error:
                    assert error.status == 5, error
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.02)
            assert client.describe_workflow(live["workflow_id"])["state"] == "running"
            assert wait_for(client, live["workflow_id"], {"completed", "failed"})["state"] == "completed"

            large = {
				"schema": "datavine.workflow/v1",
				"workflow_id": "large-result-outside-workflow-journal",
				"idempotency_key": "large-result-outside-workflow-journal-v1",
				"mode": "sealed",
				"tasks": [{
					"task_id": 1,
					"executor": {"kind": "command", "version": "1", "argv": ["/bin/sh", "-c", "head -c 2097152 /dev/zero > blob"], "output_files": ["blob"]},
					"inputs": [],
					"output_data_ids": [1],
				}],
				"data": [{"data_id": 1, "codec": {"name": "bytes", "version": "1"}, "origin": {"kind": "output", "task_id": 1, "output_index": 0}}],
				"requested_outputs": [1],
				"policy": {"maximum_tasks": 1, "maximum_edges": 0},
			}
            client.submit_workflow(large)
            assert wait_for(client, large["workflow_id"], {"completed", "failed"})["state"] == "completed"
            assert len(client.fetch_workflow_result(large["workflow_id"], 1)) == 2 * 1024 * 1024
        finally:
            if restarted_worker is not None and restarted_worker.poll() is None:
                restarted_worker.send_signal(signal.SIGTERM)
                restarted_worker.communicate(timeout=20)
            restarted.send_signal(signal.SIGTERM)
            stdout, stderr = restarted.communicate(timeout=20)
            assert restarted.returncode == 0, (restarted.returncode, stdout, stderr)

        assert (root / "native.journal").stat().st_size < 1024 * 1024
        assert not (root / "native.journal.data/catalog").exists()
        result_files = list((root / "native.journal.data").rglob("*.data"))
        largest = max(result_files, key=lambda path: path.stat().st_size)
        assert largest.stat().st_size >= 2 * 1024 * 1024
        with largest.open("r+b") as stream:
            stream.write(b"X")
        corrupt = subprocess.run(
            (str(service_binary), "serve", str(root / "native.journal"), token),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
        assert corrupt.returncode != 0, corrupt.stdout

    print(
        "DataVine native command workflow PASS "
        "c-owner=1 worker=1 dag=2 multi-output=1 python-executor=1 "
        "python-adaptor-exit=1 callable-adaptor-exit=1 capability-preflight=1 "
        "durable-result=1 selective-pruning=1 "
        "live-result=1 payload-free-workflow-journal=1 corrupt-result-rejected=1 "
        "result-identity=sha256+attempt+producer+codec"
    )


if __name__ == "__main__":
    main()
