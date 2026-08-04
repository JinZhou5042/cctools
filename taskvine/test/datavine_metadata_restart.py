#!/usr/bin/env python3

import hashlib
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

from ndcctools.taskvine.datavine.controller.service import ControllerService
from ndcctools.taskvine.datavine.controller.state import ControllerState
from ndcctools.taskvine.datavine.models import TaskRecord
from ndcctools.taskvine.datavine.native import NativeControllerClient
from ndcctools.taskvine.datavine.serialization import serialize


def start(root):
    state = ControllerState(max_replicas=128)
    state.configure_persistence(root)
    service = ControllerService("127.0.0.1", 0, "metadata-token", state)
    service.start()
    native = NativeControllerClient(
        f"tcp://127.0.0.1:{service.native_address[1]}",
        "metadata-token",
    )
    return state, service, native


def wait_durable(state, data_id):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if state.get_idata(data_id).durability == "durable":
            return
        time.sleep(0.01)
    raise TimeoutError("IData did not become durable")


def register_function(state, native):
    metadata, payload = serialize(bytes)
    record = state.register_edata(metadata, payload)
    native.register_edata((
        (
            record.data_id,
            record.content_hash,
            record.serialized_sha256,
            metadata.to_dict(),
            payload,
        ),
    ))
    return record, payload


def crash_fixture(arguments, ready):
    process = subprocess.Popen(
        (sys.executable, __file__, *arguments),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and not ready.exists():
        if process.poll() is not None:
            raise RuntimeError("metadata crash fixture exited")
        time.sleep(0.01)
    if not ready.exists():
        process.kill()
        process.wait()
        raise TimeoutError("metadata crash fixture was not ready")
    os.kill(process.pid, signal.SIGKILL)
    process.wait()


def populate_crash(root, ready):
    state, _, native = start(root)
    function, _ = register_function(state, native)
    first = state.allocate_idata(10)
    second = state.allocate_idata(2)
    state.register_tasks((
        TaskRecord(10, function.data_id, (), (), first.data_id, ()),
        TaskRecord(
            2,
            function.data_id,
            (("i", first.data_id),),
            (),
            second.data_id,
            (first.data_id,),
        ),
    ))
    state.publish_idata(first.data_id, 1, b"crash-output")
    state.publish_idata(second.data_id, 1, b"crash-required")
    state.set_task_state(10, "completed")
    state.set_task_state(2, "running")
    state.set_required_output(second.data_id)
    Path(ready).write_text("ready")
    while True:
        time.sleep(1)


def populate_quarantine(root, ready, committed):
    state, _, native = start(root)
    function, _ = register_function(state, native)
    output = state.allocate_idata(9)
    state.register_task(
        TaskRecord(9, function.data_id, (), (), output.data_id, ())
    )
    state.publish_idata(output.data_id, 1, b"quarantine-anchor")
    state.request_persistence(output.data_id)
    wait_durable(state, output.data_id)
    state.set_task_state(9, "completed")
    if committed:
        plan = state.pruning_plan()
        state.apply_pruning(
            plan["records"][0]["graph_revision"],
            plan["records"][0]["state_revision"],
            60,
            (output.data_id,),
        )
    else:
        replica = next(
            record
            for record in state.replicas.records_for("i:1")
            if record.tier == "sharedfs"
        )
        state.pruning.quarantine_file(
            1,
            replica.replica_id,
            replica.generation,
            state.get_idata(1).durable_path,
        )
    Path(ready).write_text("ready")
    while True:
        time.sleep(1)


def crash_recovery():
    with tempfile.TemporaryDirectory(
        prefix="datavine-metadata-crash-"
    ) as root:
        ready = Path(root) / "ready"
        crash_fixture(("--populate", root, str(ready)), ready)
        state, service, _ = start(root)
        try:
            assert state.get_task(10).output_data_id == 1
            assert state.get_task(2).input_data_ids == (1,)
            assert state.get_idata(1).serialized_bytes == b"crash-output"
            assert state.pruning.pruner.task_states == {
                10: "completed",
                2: "running",
            }
            assert state.pruning.pruner.data_states[2].required_output
        finally:
            service.stop()


def quarantine_crash_recovery():
    for committed in (False, True):
        with tempfile.TemporaryDirectory(
            prefix="datavine-quarantine-crash-"
        ) as root:
            ready = Path(root) / "ready"
            crash_fixture(
                (
                    "--quarantine",
                    root,
                    str(ready),
                    "1" if committed else "0",
                ),
                ready,
            )
            state, service, _ = start(root)
            try:
                record = state.get_idata(1)
                assert record.durability == "durable"
                assert Path(record.durable_path).read_bytes() == (
                    b"quarantine-anchor"
                )
                assert state.pruning.pruner.data_states[1].durable
            finally:
                service.stop()


def corrupt_metadata_rejected():
    with tempfile.TemporaryDirectory(
        prefix="datavine-metadata-corrupt-"
    ) as root:
        state, service, native = start(root)
        register_function(state, native)
        service.stop()
        path = Path(root) / "controller-metadata.sqlite3"
        with path.open("r+b") as stream:
            stream.seek(0)
            stream.write(b"broken!!")
        rejected = ControllerState(max_replicas=128)
        try:
            rejected.configure_persistence(root)
        except sqlite3.DatabaseError:
            pass
        else:
            rejected.stop()
            raise AssertionError("corrupt Controller metadata was accepted")


def main():
    with tempfile.TemporaryDirectory(
        prefix="datavine-metadata-restart-"
    ) as root:
        state, service, native = start(root)
        function, payload = register_function(state, native)
        first = state.allocate_idata(1)
        second = state.allocate_idata(2)
        state.register_tasks((
            TaskRecord(1, function.data_id, (), (), first.data_id, ()),
            TaskRecord(
                2,
                function.data_id,
                (("i", first.data_id),),
                (),
                second.data_id,
                (first.data_id,),
            ),
        ))
        first_payload = b"durable-first"
        state.publish_idata(first.data_id, 1, first_payload)
        state.publish_idata(second.data_id, 1, b"required-second")
        state.request_persistence(first.data_id)
        wait_durable(state, first.data_id)
        state.set_task_state(1, "completed")
        state.set_task_state(2, "running")
        state.set_required_output(second.data_id)
        expected = state.pruning.plan().semantic()
        service.stop()

        state, service, _ = start(root)
        try:
            assert state.get_edata(function.data_id).serialized_bytes == payload
            assert state.get_task(2).input_data_ids == (first.data_id,)
            assert state.get_idata(first.data_id).serialized_bytes == first_payload
            assert state.get_idata(first.data_id).durability == "durable"
            assert state.pruning.pruner.task_states == {
                1: "completed",
                2: "running",
            }
            assert state.pruning.pruner.data_states[
                second.data_id
            ].required_output
            assert state.pruning.plan().semantic() == expected
            snapshot = state.snapshot()["metadata"]
            assert snapshot["records"] == {
                "data-state": 2,
                "edata": 1,
                "idata": 2,
                "task": 2,
                "task-state": 2,
            }
            assert hashlib.sha256(
                state.get_idata(first.data_id).serialized_bytes
            ).hexdigest() == state.get_idata(first.data_id).content_hash
        finally:
            service.stop()
    crash_recovery()
    quarantine_crash_recovery()
    corrupt_metadata_rejected()
    print("DataVine Controller metadata restart PASS")


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--populate":
        populate_crash(sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 5 and sys.argv[1] == "--quarantine":
        populate_quarantine(
            sys.argv[2], sys.argv[3], bool(int(sys.argv[4]))
        )
    else:
        main()
