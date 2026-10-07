#!/usr/bin/env python3

import asyncio
import json
from pathlib import Path
import signal
import subprocess
import tempfile
import time

from ndcctools.taskvine.datavine import (
    ManagedWorkflowGenerator,
    Workflow,
    WorkflowSession,
    managed_call,
)
from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


def wait_terminal(client, workflow_id, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = client.describe_workflow(workflow_id)
        if info["state"] in {"completed", "failed", "cancelled"}:
            return info
        time.sleep(0.05)
    raise AssertionError(client.describe_workflow(workflow_id))


async def await_value(future):
    return await future


def delayed_increment(value):
    import time as worker_time

    worker_time.sleep(0.5)
    return value + 1


def sleep_return(seconds, value):
    import time as worker_time

    worker_time.sleep(seconds)
    return value


def record_and_return(marker, value):
    from pathlib import Path as WorkerPath

    with WorkerPath(marker).open("a") as stream:
        stream.write("run\n")
    return value


def slow_increment(value):
    import time as worker_time

    worker_time.sleep(3)
    return value + 1


def crash_once_then_return(marker, value):
    import os
    from pathlib import Path as WorkerPath

    path = WorkerPath(marker)
    if not path.exists():
        path.write_text("crashed")
        os._exit(71)
    return value


def python_descendants(parent_pid):
    parents = {int(parent_pid)}
    changed = True
    while changed:
        changed = False
        for stat in Path("/proc").glob("[0-9]*/stat"):
            try:
                fields = stat.read_text().split(") ", 1)[1].split()
                pid, parent = int(stat.parent.name), int(fields[1])
            except (OSError, IndexError, ValueError):
                continue
            if parent in parents and pid not in parents:
                parents.add(pid)
                changed = True
    commands = []
    for pid in parents - {int(parent_pid)}:
        try:
            commands.append(Path(f"/proc/{pid}/cmdline").read_bytes())
        except OSError:
            pass
    return [command for command in commands if b"python" in command.lower()]


def start_notebook_owner(service_binary, worker_binary, journal, token):
    service = subprocess.Popen(
        (str(service_binary), "serve", str(journal), token),
        stdout=subprocess.PIPE,
        stderr=None,
        text=True,
    )
    contact = json.loads(service.stdout.readline())
    worker = subprocess.Popen(
        (
            str(worker_binary), "--cores=3", "--memory=1024", "--disk=1024",
            "--idle-timeout=30", "localhost", str(contact["manager_port"]),
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    return service, worker, contact, WorkflowClient(contact["endpoint"], token)


def stop_notebook_owner(service, worker, client=None):
    if client is not None:
        client.close()
    if worker is not None and worker.poll() is None:
        worker.send_signal(signal.SIGTERM)
        worker.communicate(timeout=20)
    if service.poll() is None:
        service.send_signal(signal.SIGTERM)
    stdout, stderr = service.communicate(timeout=20)
    if service.returncode != 0:
        raise AssertionError((service.returncode, stdout, stderr))


def main():
    repository = Path(__file__).resolve().parents[2]
    service_binary = repository / "taskvine/src/tools/datavine_workflow"
    worker_binary = repository / "taskvine/src/worker/vine_worker"
    token = "notebook-workflow-test-token"
    workflow_id = "notebook-normal-python"
    registry = Workflow(
        "callable-registry-v1", maximum_tasks=2, maximum_edges=0
    )
    registry.python_callable(delayed_increment, 0)
    registry.python_callable(delayed_increment, 1)
    registry_document = registry.document()
    registry_executors = [task["executor"] for task in registry_document["tasks"]]
    assert registry_executors[0]["version"] == "callable-v1"
    assert registry_executors[0]["function_ref"] == registry_executors[1]["function_ref"]
    assert registry_executors[0]["payload_ref"] != registry_executors[1]["payload_ref"]
    assert all(
        executor["output_files"] == ["datavine-python-output-0"]
        for executor in registry_executors
    )
    assert all("result_transport" not in executor for executor in registry_executors)
    assert len([
        data for data in registry_document["data"]
        if data["codec"]["name"] == "python/callable"
    ]) == 1
    with tempfile.TemporaryDirectory(prefix="datavine-notebook-") as root:
        root = Path(root)
        service, worker, contact, client = start_notebook_owner(
            service_binary, worker_binary, root / "journal", token
        )
        replacement = client
        try:
            session = WorkflowSession.create(
                client,
                workflow_id,
                maximum_tasks=10,
                maximum_edges=9,
                idempotency_key="notebook-initial-v1",
            )

            first = session.submit(lambda value: value + 1, 6)
            value = first.result(timeout=30)
            assert value == 7

            # Ordinary user-side Python controls dynamic graph construction.
            if value == 7:
                current = first
                for operation in (lambda item: item * item,):
                    current = session.submit(operation, current)
            assert asyncio.run(await_value(current)) == 49

            interrupted = session.submit(delayed_increment, current)
            try:
                interrupted.result(timeout=0.01)
            except TimeoutError:
                pass
            else:
                raise AssertionError("local notebook timeout did not interrupt wait")

            # Simulate a replacement kernel/client: no local scheduler state is
            # required to recover the Future or append the next task.
            interrupted_data_id = interrupted.data_id
            client.close()
            replacement = WorkflowClient(contact["endpoint"], token)
            attached = WorkflowSession.attach(replacement, workflow_id)
            recovered = attached.future(
                interrupted_data_id, ("python/cloudpickle", "3")
            )
            assert recovered.result(timeout=30) == 50
            pair = attached.submit(
                lambda item: (item, item + 1), recovered, output_count=2
            )
            assert tuple(item.result(timeout=30) for item in pair) == (50, 51)
            combined = attached.submit(lambda left, right: left + right, *pair)
            assert combined.result(timeout=30) == 101
            final = attached.submit(lambda item: item + 1, combined)
            assert final.result(timeout=30) == 102
            retry_marker = str(Path(root) / "python-executor-crashed-once")
            replaced = attached.submit(
                crash_once_then_return,
                retry_marker,
                77,
                maximum_attempts=2,
            )
            assert replaced.result(timeout=30) == 77
            time.sleep(0.2)
            python_processes = python_descendants(worker.pid)
            # One fork preloader serves every callable. Data persistence is
            # owned by the Controller and needs no Python helper process.
            assert len(python_processes) == 1, python_processes
            assert all(
                b"datavine_executor" in process
                for process in python_processes
            )
            attached.seal()
            assert wait_terminal(replacement, workflow_id)["state"] == "completed"
            try:
                WorkflowSession.attach(replacement, workflow_id)
            except ValueError as error:
                assert "requires an open workflow" in str(error)
            else:
                raise AssertionError("terminal workflow accepted a dynamic attach")

            stop_notebook_owner(service, worker, replacement)
            service, worker, contact, replacement = start_notebook_owner(
                service_binary, worker_binary, root / "sparse.journal", token
            )
            sparse_id = "sparse-attach"
            replacement.submit_workflow({
                "schema": "datavine.workflow/v1",
                "workflow_id": sparse_id,
                "idempotency_key": "sparse-attach-v1",
                "mode": "streaming",
                "tasks": [{
                    "task_id": 10,
                    "executor": {
                        "kind": "command", "version": "1",
                        "argv": ["/usr/bin/printf", "sparse"],
                    },
                    "inputs": [],
                    "output_data_ids": [20],
                }],
                "data": [{
                    "data_id": 20,
                    "codec": {"name": "bytes", "version": "1"},
                    "origin": {
                        "kind": "output", "task_id": 10, "output_index": 0,
                    },
                }],
                "requested_outputs": [20],
                "policy": {"maximum_tasks": 2, "maximum_edges": 1},
            })
            sparse_session = WorkflowSession.attach(replacement, sparse_id)
            sparse_input = sparse_session.future(20)
            assert sparse_input.result(timeout=30) == b"sparse"
            sparse_output = sparse_session.command(
                ["/bin/cat", sparse_input], inputs=[sparse_input]
            )
            assert sparse_output.data_id == 21
            assert sparse_output.result(timeout=30) == b"sparse"
            sparse_session.seal()
            assert wait_terminal(replacement, sparse_id)["state"] == "completed"

            stop_notebook_owner(service, worker, replacement)
            service, worker, contact, replacement = start_notebook_owner(
                service_binary, worker_binary, root / "generator.journal", token
            )
            crash_marker = Path(root) / "generator-crashed-once"

            def generator_factory():
                first_value = yield managed_call(lambda: 7)
                if not crash_marker.exists():
                    crash_marker.write_text("crashed")
                    import os

                    os._exit(23)
                squared = yield managed_call(lambda item: item * item, first_value)
                return squared

            managed = ManagedWorkflowGenerator(
                replacement,
                "managed-generator-restart",
                Path(root) / "generator.checkpoint",
                maximum_tasks=4,
                maximum_edges=3,
                maximum_restarts=2,
            )
            assert managed.run(generator_factory) == 49
            assert wait_terminal(
                replacement, "managed-generator-restart"
            )["state"] == "completed"

            stop_notebook_owner(service, worker, replacement)
            service, worker, contact, replacement = start_notebook_owner(
                service_binary, worker_binary, root / "cancel.journal", token
            )
            cancel_session = WorkflowSession.create(
                replacement, "fork-cancel", maximum_tasks=1, maximum_edges=0,
                idempotency_key="fork-cancel-v1",
            )
            cancel_session.submit(sleep_return, 30.0, "never")
            child_deadline = time.monotonic() + 20
            while len(python_descendants(worker.pid)) < 2:
                if time.monotonic() >= child_deadline:
                    raise AssertionError("fork child did not start before cancellation")
                time.sleep(0.05)
            cancel_session.cancel()
            assert wait_terminal(replacement, "fork-cancel")["state"] == "cancelled"
            cleanup_deadline = time.monotonic() + 3
            while len(python_descendants(worker.pid)) != 1:
                if time.monotonic() >= cleanup_deadline:
                    raise AssertionError(python_descendants(worker.pid))
                time.sleep(0.05)

            stop_notebook_owner(service, worker, replacement)
            service, worker, contact, replacement = start_notebook_owner(
                service_binary, worker_binary, root / "wall-time.journal", token
            )
            timeout_session = WorkflowSession.create(
                replacement, "fork-wall-time", maximum_tasks=1, maximum_edges=0,
                idempotency_key="fork-wall-time-v1",
            )
            timed = timeout_session.submit(
                sleep_return, 30.0, "never",
                resources={"wall_time_seconds": 1},
            )
            try:
                timed.result(timeout=20)
            except RuntimeError:
                pass
            else:
                raise AssertionError("wall-time task unexpectedly succeeded")
            assert wait_terminal(replacement, "fork-wall-time")["state"] == "failed"
            assert len(python_descendants(worker.pid)) == 1

            stop_notebook_owner(service, worker, replacement)
            service, worker, contact, replacement = start_notebook_owner(
                service_binary, worker_binary, root / "parallel.journal", token
            )
            parallel_session = WorkflowSession.create(
                replacement, "single-reactor-parallel", maximum_tasks=2,
                maximum_edges=0, idempotency_key="single-reactor-parallel-v1",
            )
            # A task that specifies memory but omits cores must still default
            # to one core; otherwise TaskVine may reserve the whole worker and
            # serialize the independent ready task behind the slow task.
            slow = parallel_session.submit(
                sleep_return, 10.0, "slow", resources={"memory_mb": 1}
            )
            fast_started = time.monotonic()
            fast = parallel_session.submit(
                lambda: "fast", resources={"memory_mb": 1}
            )
            assert fast.result(timeout=30) == "fast"
            fast_seconds = time.monotonic() - fast_started
            assert fast_seconds < 5.0, fast_seconds
            try:
                slow_value = slow.result(timeout=30)
            except TimeoutError as error:
                raise AssertionError({
                    "workflow": replacement.describe_workflow(
                        "single-reactor-parallel"
                    ),
                    "python_processes": python_descendants(worker.pid),
                }) from error
            assert slow_value == "slow"
            parallel_session.seal()
            assert wait_terminal(replacement, "single-reactor-parallel")[
                "state"
            ] == "completed"

        finally:
            stop_notebook_owner(service, worker, replacement)
        profile_path = Path(root) / "journal.profile"
        assert profile_path.is_file()
        assert "dominant_stage=" in profile_path.read_text()

    # An unrequested callable intermediate lives only in worker cache. A
    # service restart must recompute its producer, not claim completion from a
    # stale task event whose data no longer exists.
    with tempfile.TemporaryDirectory(prefix="datavine-temp-restart-") as root:
        root = Path(root)
        journal = root / "journal"
        marker = root / "producer-runs"

        def start_owner():
            owner = subprocess.Popen(
                (str(service_binary), "serve", str(journal), token),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            contact = json.loads(owner.stdout.readline())
            owner_worker = subprocess.Popen(
                (
                    str(worker_binary), "--cores=2", "--memory=1024",
                    "--disk=1024", "--idle-timeout=30", "localhost",
                    str(contact["manager_port"]),
                ),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            return owner, owner_worker, WorkflowClient(contact["endpoint"], token)

        owner, owner_worker, restart_client = start_owner()
        workflow = Workflow(
            "worker-local-restart-v1",
            workflow_id="worker-local-restart",
            maximum_tasks=2,
            maximum_edges=1,
        )
        intermediate = workflow.python_callable(record_and_return, str(marker), 7)
        final = workflow.python_callable(slow_increment, intermediate)
        workflow.request(final).submit(restart_client)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            events = restart_client.watch_workflow("worker-local-restart")
            if any(
                event["type"] == "task_completed" and event.get("task_id") == 1
                for event in events
            ):
                break
            time.sleep(0.05)
        else:
            raise AssertionError("worker-local producer did not complete")
        owner.kill()
        owner.communicate(timeout=20)
        if owner_worker.poll() is None:
            owner_worker.send_signal(signal.SIGTERM)
        owner_worker.communicate(timeout=20)
        restart_client.close()

        owner, owner_worker, restart_client = start_owner()
        try:
            assert wait_terminal(
                restart_client, "worker-local-restart", timeout=30
            )["state"] == "completed"
            import cloudpickle

            assert cloudpickle.loads(restart_client.fetch_workflow_result(
                "worker-local-restart", final.data_id
            )) == 8
            assert marker.read_text().count("run\n") >= 2
        finally:
            restart_client.close()
            if owner_worker.poll() is None:
                owner_worker.send_signal(signal.SIGTERM)
            owner_worker.communicate(timeout=20)
            if owner.poll() is None:
                owner.send_signal(signal.SIGTERM)
            owner.communicate(timeout=20)

    print(
        "DataVine notebook workflow PASS future=1 await=1 if-for=1 "
        "local-timeout=detached attach=1 result=102 multi-output-downstream=1 "
        "managed-generator-restart=1 "
        "worker-local-restart=recompute "
        "python-executor-crash-retry=1 python-preloader=1 fork-children=0 "
        "callable-register-once=1 "
        "fork-cancel=1 wall-time-kill=1 "
        "single-workflow-task-parallelism=1 "
        "default-profile=1 "
        "sparse-frontier-attach=1 terminal-attach=reject resource-default-core=1"
    )


if __name__ == "__main__":
    main()
