#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

from ndcctools.taskvine.datavine import (
    ControllerClient,
    TaskSchedulerThread,
    Workflow,
)


SHARED = b"datavine-demand-pull\n" * 32768


def add(left, right):
    return left + right


def shared_size(left, right, ordinal):
    assert left is right
    return len(left) + ordinal


def sleepy_payload(size, delay):
    time.sleep(delay)
    return bytes(index % 251 for index in range(size))


def checksum(payload):
    return hashlib.sha256(payload).hexdigest()


def wait_json(path, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size:
            return json.loads(path.read_text())
        time.sleep(0.05)
    raise TimeoutError(path)


def start_worker(port, workspace=None):
    workspace_args = []
    if workspace is not None:
        workspace_args = [
            "--workspace",
            str(workspace),
            "--keep-workspace",
        ]
    return subprocess.Popen(
        [
            os.environ.get("VINE_WORKER", "vine_worker"),
            "127.0.0.1",
            str(port),
            "--cores",
            "2",
            *workspace_args,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def run_case(
    name,
    workflow,
    target_task_id,
    oracle,
    inject_loss=False,
    factory_manager=None,
    worker_count=1,
    peer_transfers=True,
    persistence=False,
    persistence_fail_first=False,
    inject_global_loss_after=None,
    inject_worker_loss_after=None,
    replacement_worker_delay=None,
    prefetch=True,
    prefetch_byte_budget=64 * 1024 * 1024,
    prefetch_item_budget=16,
    inject_prefetch_failure=False,
    apply_pruning=False,
):
    with tempfile.TemporaryDirectory(prefix=f"datavine-{name}-") as root:
        root = Path(root)
        ready_path = root / "ready.json"
        token = f"token-{name}"
        controller_host = socket.getfqdn() if factory_manager else "127.0.0.1"
        controller = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "ndcctools.taskvine.datavine.controller.cli",
                "--host",
                "0.0.0.0" if factory_manager else "127.0.0.1",
                "--advertise-host",
                controller_host,
                "--token",
                token,
                "--ready-file",
                str(ready_path),
                "--max-edata-bytes",
                str(64 * 1024 * 1024),
                *(
                    [
                        "--persistence-dir",
                        str(root / "durable"),
                        "--persistence-workers",
                        "1",
                        *(
                            ["--persistence-fail-first"]
                            if persistence_fail_first
                            else []
                        ),
                    ]
                    if persistence
                    else []
                ),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        scheduler = None
        workers = []
        replacement_timer = None
        try:
            ready = wait_json(ready_path)
            client = ControllerClient(
                f"http://{controller_host}:{ready['port']}", token
            )
            scheduler = TaskSchedulerThread(client).start()
            port = scheduler.call(
                "create_manager",
                0,
                factory_manager or f"datavine-{name}",
                str(root / "run-info"),
                peer_transfers,
            )
            if not factory_manager:
                workers.extend(
                    start_worker(
                        port,
                        (
                            root / f"worker-{index}"
                            if apply_pruning
                            else None
                        ),
                    )
                    for index in range(worker_count)
                )
            deadline = time.monotonic() + (
                600 if factory_manager else 30
            )
            while scheduler.call("worker_count") < worker_count:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"expected {worker_count} TaskVine workers"
                    )
                time.sleep(1)
            future = scheduler.submit(
                "run_workflow",
                workflow,
                None,
                1,
                persistence,
                inject_global_loss_after,
                inject_worker_loss_after,
                prefetch,
                prefetch_byte_budget,
                prefetch_item_budget,
                inject_prefetch_failure,
            )
            if (
                replacement_worker_delay is not None
                and not factory_manager
            ):
                replacement_timer = threading.Timer(
                    replacement_worker_delay,
                    lambda: workers.append(start_worker(port)),
                )
                replacement_timer.start()
            if inject_loss:
                time.sleep(1.5)
                os.killpg(workers[-1].pid, signal.SIGKILL)
                workers[-1].wait(timeout=10)
                workers.append(start_worker(port))
            results = future.result(
                timeout=600 if factory_manager else 90
            )
            assert results[target_task_id] == oracle
            if apply_pruning:
                cache_before = sorted(
                    str(path.relative_to(root))
                    for path in root.glob("worker-*/cache/*")
                    if not path.name.endswith(".meta")
                )
                pruning_result = scheduler.call(
                    "apply_pruning", 0, None, None, 30
                )
                cache_after = sorted(
                    str(path.relative_to(root))
                    for path in root.glob("worker-*/cache/*")
                    if not path.name.endswith(".meta")
                )
            else:
                cache_before = []
                cache_after = []
                pruning_result = None
            snapshot = client.snapshot()
            snapshot["scheduler_report"] = scheduler.call(
                "last_run_report"
            )
            worker_ids = set()
            worker_disconnections = 0
            running_task_ids = []
            for transaction_log in (root / "run-info").rglob("transactions"):
                for line in transaction_log.read_text().splitlines():
                    fields = line.split()
                    if (
                        len(fields) > 6
                        and fields[2] == "TASK"
                        and fields[4] == "RUNNING"
                    ):
                        worker_ids.add(fields[5])
                        running_task_ids.append(int(fields[3]))
                    if (
                        len(fields) > 5
                        and fields[2] == "WORKER"
                        and fields[4] == "DISCONNECTION"
                    ):
                        worker_disconnections += 1
            snapshot["taskvine_workers_used"] = len(worker_ids)
            snapshot["taskvine_worker_disconnections"] = (
                worker_disconnections
            )
            snapshot["taskvine_running_order"] = running_task_ids
            snapshot["pruning_result"] = pruning_result
            snapshot["worker_cache_before_pruning"] = cache_before
            snapshot["worker_cache_after_pruning"] = cache_after
            snapshot["durable_files"] = sorted(
                path.name
                for path in (root / "durable").glob("idata-*.pkl")
            ) if persistence else []
            if persistence:
                for data_id in range(1, len(workflow.tasks) + 1):
                    status = client.idata_status(data_id)
                    durable_bytes = Path(
                        status["durable_path"]
                    ).read_bytes()
                    assert (
                        hashlib.sha256(durable_bytes).hexdigest()
                        == status["content_hash"]
                    )
                snapshot["durable_hashes_valid"] = True
            assert snapshot["available_idata"] == (
                0 if apply_pruning else len(workflow.tasks)
            )
            assert snapshot["tasks"] == len(workflow.tasks)
            return snapshot
        finally:
            if replacement_timer is not None:
                replacement_timer.cancel()
                replacement_timer.join(timeout=5)
            if scheduler is not None:
                scheduler.stop()
            for worker in workers:
                if worker.poll() is None:
                    worker.terminate()
                    worker.wait(timeout=10)
            controller.terminate()
            _, stderr = controller.communicate(timeout=10)
            if controller.returncode != 0:
                raise AssertionError(stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()
    normal = Workflow()
    root = normal.add_task(add, 7, 5)
    target = normal.add_task(add, root.output(), 11)
    normal_snapshot = run_case(
        "normal", normal, target.task_id, 23,
        factory_manager=args.factory_manager,
    )

    shared = Workflow()
    parts = [
        shared.add_task(shared_size, SHARED, SHARED, ordinal)
        for ordinal in range(4)
    ]
    target = shared.add_task(add, parts[0].output(), parts[3].output())
    shared_snapshot = run_case(
        "shared",
        shared,
        target.task_id,
        2 * len(SHARED) + 3,
        factory_manager=args.factory_manager,
    )
    assert shared_snapshot["deduplicated_registrations"] >= 7

    recovery_snapshot = None
    if not args.factory_manager:
        recovery = Workflow()
        payload = recovery.add_task(sleepy_payload, 1024 * 1024, 4)
        target = recovery.add_task(checksum, payload.output())
        import hashlib
        oracle = hashlib.sha256(
            bytes(index % 251 for index in range(1024 * 1024))
        ).hexdigest()
        recovery_snapshot = run_case(
            "worker-loss", recovery, target.task_id, oracle, inject_loss=True
        )
        replica_states = recovery_snapshot[
            "replica_directory"
        ]["replica_states"]
        assert replica_states["preparing"] == 0
        assert replica_states["invalid"] >= 1
        assert (
            recovery_snapshot["replica_directory"]["active_workers"]
            == 1
        )

    print(
        json.dumps(
            {
                "normal": normal_snapshot,
                "shared": shared_snapshot,
                "worker_loss": recovery_snapshot,
            },
            sort_keys=True,
        )
    )
    print("DataVine Phase 4 independent demand-pull E2E PASS")


if __name__ == "__main__":
    main()
