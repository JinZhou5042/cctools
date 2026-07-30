#!/usr/bin/env python3

import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time

from ndcctools.taskvine.datavine.controller.service import ControllerService
from ndcctools.taskvine.datavine.controller.state import ControllerState
from ndcctools.taskvine.datavine.scheduler.client import ControllerClient


def expect_error(fragment, function, *args, **kwargs):
    try:
        function(*args, **kwargs)
    except (KeyError, RuntimeError, ValueError) as exc:
        assert fragment in str(exc), (fragment, str(exc))
    else:
        raise AssertionError(f"expected failure containing {fragment!r}")


def wait_for(state, data_id, durability, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = state.idata_status(data_id)
        if status["durability"] == durability:
            return status
        time.sleep(0.01)
    raise AssertionError(
        f"IDataID {data_id} did not reach {durability}: "
        f"{state.idata_status(data_id)}"
    )


def allocate_value(state, producer, attempt, payload):
    record = state.allocate_idata(producer)
    state.publish_idata(record.data_id, attempt, payload)
    return record.data_id


def queued_cancellation_and_capacity(root):
    writing = threading.Event()
    release = threading.Event()

    def hook(request, transition):
        if request.data_id == 1 and transition == "writing":
            writing.set()
            assert release.wait(timeout=5)

    state = ControllerState()
    state.configure_persistence(
        root,
        workers=1,
        queue_capacity=1,
        terminal_capacity=2,
        transition_hook=hook,
    )
    try:
        first = allocate_value(state, 1, 1, b"first")
        second = allocate_value(state, 2, 1, b"second")
        third = allocate_value(state, 3, 1, b"third")
        state.request_persistence(first)
        assert writing.wait(timeout=5)
        queued = state.request_persistence(second)
        duplicate = state.request_persistence(second)
        assert queued == duplicate
        expect_error(
            "queue admission capacity",
            state.request_persistence,
            third,
        )
        assert state.cancel_persistence(second, "shadow-prune") == "cancelled"
        release.set()
        wait_for(state, first, "durable")
        cancelled = wait_for(state, second, "cancelled")
        assert cancelled["persistence_request"]["cancel_reason"] == "shadow-prune"
        assert state.idata_status(third)["durability"] == "volatile"
        files = sorted(Path(root).glob("idata-*.pkl"))
        assert len(files) == 1
        snapshot = state.snapshot()
        assert snapshot["persistence_executor"]["queue_capacity"] == 1
        assert snapshot["persistence_executor"]["queued_high_water"] == 1
        assert snapshot["persistence_executor"]["callback_failures"] == 0
        assert snapshot["persistence_cleanup_failures"] == 0
        assert snapshot["persistence_requests"] == 2
        return snapshot
    finally:
        release.set()
        state.stop()


def active_cancellation(root):
    before_commit = threading.Event()
    release = threading.Event()

    def hook(request, transition):
        if transition == "before-commit":
            before_commit.set()
            assert release.wait(timeout=5)

    state = ControllerState()
    state.configure_persistence(root, transition_hook=hook)
    try:
        data_id = allocate_value(state, 1, 1, b"cancel-active")
        state.request_persistence(data_id)
        assert before_commit.wait(timeout=5)
        assert (
            state.cancel_persistence(data_id, "became-prunable")
            == "cancelling"
        )
        release.set()
        status = wait_for(state, data_id, "cancelled")
        assert status["durable_path"] is None
        assert not list(Path(root).glob("idata-*.pkl"))
        return state.snapshot()
    finally:
        release.set()
        state.stop()


def stale_completion_after_new_attempt(root):
    committing = threading.Event()
    release = threading.Event()

    def hook(request, transition):
        if request.attempt == 1 and transition == "committing":
            committing.set()
            assert release.wait(timeout=5)

    state = ControllerState()
    state.configure_persistence(root, transition_hook=hook)
    try:
        old_payload = b"attempt-one"
        new_payload = b"attempt-two"
        data_id = allocate_value(state, 1, 1, old_payload)
        state.request_persistence(data_id)
        assert committing.wait(timeout=5)

        # Cancellation is now too late to stop the atomic rename. Publishing
        # attempt 2 is allowed, but the old callback must not acknowledge it.
        state.publish_idata(data_id, 2, new_payload)
        release.set()
        deadline = time.monotonic() + 5
        while (
            state.snapshot()["persistence_stale_completions"] != 1
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        status = state.idata_status(data_id)
        assert status["attempt"] == 2
        assert status["content_hash"] == hashlib.sha256(new_payload).hexdigest()
        assert status["durability"] == "volatile"
        assert not list(Path(root).glob("*attempt-1-*.pkl"))

        state.request_persistence(data_id)
        durable = wait_for(state, data_id, "durable")
        path = Path(durable["durable_path"])
        assert path.read_bytes() == new_payload
        expect_error(
            "cannot supersede durable",
            state.publish_idata,
            data_id,
            3,
            b"attempt-three",
        )
        snapshot = state.snapshot()
        assert snapshot["persistence_stale_completions"] == 1
        assert snapshot["persistence_cleanup_failures"] == 0
        assert (
            snapshot["replica_directory"]["replica_states"]["available"]
            == 2
        )
        return snapshot
    finally:
        release.set()
        state.stop()


def protocol_active_cancellation(root):
    before_commit = threading.Event()
    release = threading.Event()

    def hook(request, transition):
        if transition == "before-commit":
            before_commit.set()
            assert release.wait(timeout=5)

    state = ControllerState()
    state.configure_persistence(root, transition_hook=hook)
    data_id = allocate_value(state, 1, 1, b"protocol-cancel")
    service = ControllerService("127.0.0.1", 0, "race-token", state)
    host, port = service.start()
    client = ControllerClient(f"http://{host}:{port}", "race-token")
    try:
        client.persist_idata(data_id)
        assert before_commit.wait(timeout=5)
        response = client.cancel_persistence(
            data_id, "protocol-shadow-prune"
        )
        assert response["action"] == "cancelling"
        release.set()
        wait_for(state, data_id, "cancelled")
        return state.snapshot()
    finally:
        release.set()
        service.stop()


def main():
    with tempfile.TemporaryDirectory(
        prefix="datavine-persistence-races-"
    ) as root:
        queued = queued_cancellation_and_capacity(Path(root) / "queued")
        active = active_cancellation(Path(root) / "active")
        stale = stale_completion_after_new_attempt(Path(root) / "stale")
        protocol = protocol_active_cancellation(
            Path(root) / "protocol"
        )
    report = {
        "queued_capacity": queued["persistence_executor"],
        "active_cancelled": active["durability"]["cancelled"],
        "stale_completions": stale["persistence_stale_completions"],
        "final_durable": stale["durability"]["durable"],
        "protocol_cancelled": protocol["durability"]["cancelled"],
    }
    print(json.dumps(report, sort_keys=True))
    print("DataVine persistence generation/race component test PASS")


if __name__ == "__main__":
    main()
