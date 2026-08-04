#!/usr/bin/env python3

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

from ndcctools.taskvine.datavine.controller.service import ControllerService
from ndcctools.taskvine.datavine.controller.state import ControllerState
from ndcctools.taskvine.datavine.native import (
    NativeControllerClient,
    NativeControllerError,
)


def start(root):
    state = ControllerState(max_replicas=128)
    state.configure_persistence(root)
    service = ControllerService("127.0.0.1", 0, "restart-token", state)
    service.start()
    return service


def client(service):
    return NativeControllerClient(
        f"tcp://127.0.0.1:{service.native_address[1]}",
        "restart-token",
    )


def start_process(root, ready):
    process = subprocess.Popen(
        (
            sys.executable,
            "-m",
            "ndcctools.taskvine.datavine.controller.cli",
            "--host",
            "127.0.0.1",
            "--token",
            "restart-token",
            "--persistence-dir",
            str(root),
            "--ready-file",
            str(ready),
        ),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Controller exited before becoming ready")
        try:
            state = json.loads(ready.read_text())
            return process, NativeControllerClient(
                f"tcp://127.0.0.1:{state['native_port']}",
                "restart-token",
            )
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.01)
    process.kill()
    process.wait()
    raise TimeoutError("Controller did not become ready")


def crash_recovery(root):
    ready = Path(root) / "ready"
    process, native = start_process(root, ready)
    payload = b"process-crash-edata"
    digest = hashlib.sha256(payload).hexdigest()
    native.register_edata(((7, digest, digest, {}, payload),))
    os.kill(process.pid, signal.SIGKILL)
    process.wait()
    ready.unlink()
    process, recovered = start_process(root, ready)
    try:
        assert recovered.get_edata(7)["payload"] == payload
    finally:
        process.terminate()
        process.wait(timeout=30)


def corrupt_journal_rejected():
    with tempfile.TemporaryDirectory(
        prefix="datavine-corrupt-journal-"
    ) as root:
        service = start(root)
        native = client(service)
        payload = b"corruption-must-fail-closed"
        digest = hashlib.sha256(payload).hexdigest()
        native.register_edata(((1, digest, digest, {}, payload),))
        service.stop()
        journal = Path(root) / "controller-native.wal"
        with journal.open("r+b") as stream:
            stream.seek(-1, os.SEEK_END)
            value = stream.read(1)
            stream.seek(-1, os.SEEK_END)
            stream.write(bytes((value[0] ^ 1,)))
        state = ControllerState(max_replicas=128)
        state.configure_persistence(root)
        rejected = ControllerService(
            "127.0.0.1", 0, "restart-token", state
        )
        try:
            rejected.start()
        except RuntimeError:
            pass
        else:
            raise AssertionError("corrupt native journal was accepted")
        finally:
            state.stop()


def main():
    with tempfile.TemporaryDirectory(prefix="datavine-native-restart-") as root:
        service = start(root)
        contender_state = ControllerState(max_replicas=128)
        contender_state.configure_persistence(root)
        contender = ControllerService(
            "127.0.0.1", 0, "restart-token", contender_state
        )
        try:
            contender.start()
        except RuntimeError:
            pass
        else:
            raise AssertionError("two Controllers opened one native journal")
        finally:
            contender_state.stop()
        first = client(service)
        payload = b"persistent-edata"
        digest = hashlib.sha256(payload).hexdigest()
        first.register_edata(((1, digest, digest, {}, payload),))
        first.allocate_idata(((1, 1, 0),))
        assert first.claim_worker("source", "http://source") == 1
        assert first.claim_worker("destination", "http://destination") == 1
        published = first.publish_outputs(
            "source",
            1,
            ({
                "data_id": 1,
                "attempt": 1,
                "content_hash": digest,
                "size": len(payload),
            },),
        )
        assert published[0]["generation"] == 1
        service.stop()

        service = start(root)
        recovered = client(service)
        assert recovered.get_edata(1)["payload"] == payload
        assert recovered.claim_worker("source", "http://source") == 1
        assert recovered.claim_worker("destination", "http://destination") == 1
        source = recovered.resolve_source(
            "i:1", "destination", 1, "restart-read"
        )
        assert source["source"]["worker_id"] == "source"
        recovered.release_source("restart-read", True)
        recovered.disconnect_worker("source", 1)
        assert recovered.claim_worker("source", "http://source") == 2
        try:
            recovered.resolve_source(
                "i:1", "destination", 1, "stale-read"
            )
        except NativeControllerError as exc:
            assert exc.status == 5
        else:
            raise AssertionError("stale epoch replica survived restart")
        service.stop()
        crash_recovery(root)
    corrupt_journal_rejected()
    print("DataVine native Controller restart recovery PASS")


if __name__ == "__main__":
    main()
