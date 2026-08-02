#!/usr/bin/env python3

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from ndcctools.taskvine.datavine.controller.service import (
    ControllerService,
)
from ndcctools.taskvine.datavine.controller.state import ControllerState
from ndcctools.taskvine.datavine.models import TaskRecord
from ndcctools.taskvine.datavine.protocol import DataVineRemoteError
from ndcctools.taskvine.datavine.scheduler.client import ControllerClient
from ndcctools.taskvine.datavine.serialization import serialize


def start_blocked_fetch(client, data_id, entered, release, result):
    def run():
        try:
            result["payload"] = client.fetch_edata_record(data_id)[1]
        except Exception as exc:
            result["error"] = repr(exc)

    thread = threading.Thread(target=run)
    thread.start()
    assert entered.wait(5)
    return thread


def expect_overload(function):
    try:
        function()
    except DataVineRemoteError as exc:
        assert "503" in str(exc), exc
        return
    raise AssertionError("request unexpectedly bypassed admission")


def byte_serving_case():
    entered = threading.Event()
    release = threading.Event()

    def serving_hook(data_key):
        entered.set()
        assert release.wait(10), data_key

    state = ControllerState(max_edata_bytes=4 * 1024 * 1024)
    metadata, payload = serialize(b"x" * (512 * 1024))
    record = state.register_edata(metadata, payload)
    large_metadata, large_payload = serialize(b"y" * (1536 * 1024))
    large = state.register_edata(large_metadata, large_payload)
    service = ControllerService(
        "127.0.0.1",
        0,
        "admission-token",
        state,
        max_request_concurrency=4,
        max_serving_concurrency=1,
        max_serving_bytes=1024 * 1024,
        serving_hook=serving_hook,
    )
    _, port = service.start()
    client = ControllerClient(
        f"http://127.0.0.1:{port}", "admission-token", timeout=5
    )
    result = {}
    thread = start_blocked_fetch(
        client, record.data_id, entered, release, result
    )
    try:
        expect_overload(
            lambda: client.fetch_edata_record(record.data_id)
        )
        snapshot = client.snapshot()
        assert snapshot["byte_serving"]["active"] == 1
        assert snapshot["byte_serving"]["rejected"] == 1
        expect_overload(
            lambda: client.fetch_edata_record(large.data_id)
        )
    finally:
        release.set()
        thread.join(10)
        service.stop()
    assert not thread.is_alive()
    assert result == {"payload": payload}, result
    final = service.byte_serving.snapshot()
    assert final["active"] == 0
    assert final["inflight_bytes"] == 0
    assert final["active_high_water"] == 1
    assert final["admitted"] == 1
    assert final["rejected"] == 2
    assert final["bytes_served"] == len(payload)
    return final


def request_admission_case():
    entered = threading.Event()
    release = threading.Event()

    def serving_hook(data_key):
        entered.set()
        assert release.wait(10), data_key

    state = ControllerState(max_edata_bytes=2 * 1024 * 1024)
    metadata, payload = serialize(b"z" * (256 * 1024))
    record = state.register_edata(metadata, payload)
    service = ControllerService(
        "127.0.0.1",
        0,
        "request-token",
        state,
        max_request_concurrency=1,
        max_serving_concurrency=1,
        max_serving_bytes=1024 * 1024,
        serving_hook=serving_hook,
    )
    _, port = service.start()
    client = ControllerClient(
        f"http://127.0.0.1:{port}", "request-token", timeout=5
    )
    result = {}
    thread = start_blocked_fetch(
        client, record.data_id, entered, release, result
    )
    try:
        expect_overload(client.health)
    finally:
        release.set()
        thread.join(10)
    snapshot = client.snapshot()
    service.stop()
    assert result == {"payload": payload}, result
    admission = snapshot["request_admission"]
    assert admission["active_capacity"] == 1
    assert admission["active_high_water"] == 1
    assert admission["rejected"] == 1
    return admission


def worker_retry_case():
    entered = threading.Event()
    release = threading.Event()

    def serving_hook(data_key):
        entered.set()
        assert release.wait(10), data_key

    state = ControllerState(max_edata_bytes=1024 * 1024)
    metadata, payload = serialize(b"retry" * 1024)
    record = state.register_edata(metadata, payload)
    service = ControllerService(
        "127.0.0.1",
        0,
        "retry-token",
        state,
        max_request_concurrency=1,
        max_serving_concurrency=1,
        max_serving_bytes=1024 * 1024,
        serving_hook=serving_hook,
    )
    _, port = service.start()
    endpoint = f"http://127.0.0.1:{port}"
    blocking = ControllerClient(endpoint, "retry-token", timeout=5)
    retrying = ControllerClient(
        endpoint,
        "retry-token",
        timeout=5,
        transient_retries=8,
        retry_base_seconds=0.02,
        retry_max_seconds=0.05,
    )
    result = {}
    thread = start_blocked_fetch(
        blocking, record.data_id, entered, release, result
    )
    timer = threading.Timer(0.12, release.set)
    timer.start()
    try:
        assert retrying.health()["status"] == "ready"
        assert retrying.transient_retry_count >= 1
        snapshot = retrying.snapshot()
    finally:
        release.set()
        timer.cancel()
        timer.join(5)
        thread.join(10)
        service.stop()
    assert result == {"payload": payload}, result
    assert snapshot["request_admission"]["rejected"] >= 1
    return {
        "rejected_before_retry": snapshot["request_admission"][
            "rejected"
        ],
        "client_retries": retrying.transient_retry_count,
        "status": "PASS",
    }


def idata_attempt_source_case():
    state = ControllerState(max_edata_bytes=1024 * 1024)
    metadata, function_payload = serialize(bytes)
    function = state.register_edata(metadata, function_payload)
    record = state.allocate_idata(41)
    state.register_task(
        TaskRecord(41, function.data_id, (), (), record.data_id, ())
    )
    state.publish_idata(record.data_id, 1, b"attempt-one")
    service = ControllerService(
        "127.0.0.1",
        0,
        "attempt-token",
        state,
        max_serving_bytes=1024 * 1024,
    )
    _, port = service.start()

    def source(attempt):
        query = urllib.parse.urlencode(
            {"token": "attempt-token", "attempt": attempt}
        )
        return (
            f"http://127.0.0.1:{port}/v1/idata/{record.data_id}?"
            f"{query}"
        )

    try:
        with urllib.request.urlopen(source(1), timeout=5) as response:
            assert response.read() == b"attempt-one"
        state.publish_idata(record.data_id, 2, b"attempt-two")
        try:
            urllib.request.urlopen(source(1), timeout=5)
        except urllib.error.HTTPError as error:
            assert error.code == 409, error
        else:
            raise AssertionError("stale IData attempt source was accepted")
        with urllib.request.urlopen(source(2), timeout=5) as response:
            assert response.read() == b"attempt-two"
    finally:
        service.stop()
    return {
        "old_attempt_status": 409,
        "current_attempt": 2,
        "status": "PASS",
    }


def main():
    started = time.monotonic()
    result = {
        "byte_serving": byte_serving_case(),
        "request_admission": request_admission_case(),
        "worker_retry": worker_retry_case(),
        "idata_attempt_source": idata_attempt_source_case(),
        "elapsed_seconds": time.monotonic() - started,
        "status": "PASS",
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
