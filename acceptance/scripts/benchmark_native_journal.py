#!/usr/bin/env python3

import argparse
import concurrent.futures
import hashlib
import tempfile
import time

from ndcctools.taskvine.datavine.controller.service import ControllerService
from ndcctools.taskvine.datavine.controller.state import ControllerState
from ndcctools.taskvine.datavine.native import NativeControllerClient


def run_case(records, clients, journal):
    temporary = tempfile.TemporaryDirectory(
        prefix="datavine-journal-benchmark-"
    )
    state = ControllerState(max_replicas=max(1024, records * 2))
    if journal:
        state.configure_persistence(temporary.name)
    service = ControllerService("127.0.0.1", 0, "benchmark-token", state)
    service.start()
    endpoint = f"tcp://127.0.0.1:{service.native_address[1]}"
    admin = NativeControllerClient(endpoint, "benchmark-token")
    admin.allocate_idata((data_id, data_id, 0) for data_id in range(1, records + 1))
    workers = []
    for index in range(clients):
        worker = NativeControllerClient(endpoint, "benchmark-token")
        worker_id = f"worker-{index}"
        epoch = worker.claim_worker(worker_id, f"http://worker-{index}")
        workers.append((worker, worker_id, epoch))
    digest = hashlib.sha256(b"x").hexdigest()

    def publish(index):
        worker, worker_id, epoch = workers[index]
        for data_id in range(index + 1, records + 1, clients):
            worker.publish_outputs(
                worker_id,
                epoch,
                ({
                    "data_id": data_id,
                    "attempt": 1,
                    "content_hash": digest,
                    "size": 1,
                },),
            )

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(clients) as pool:
        tuple(pool.map(publish, range(clients)))
    elapsed = time.monotonic() - started
    metrics = service.snapshot()["native_journal"]
    service.stop()
    temporary.cleanup()
    return elapsed, metrics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=10000)
    parser.add_argument("--clients", type=int, default=8)
    args = parser.parse_args()
    if args.records < args.clients or args.clients < 1:
        parser.error("records must be at least clients")
    for journal in (False, True):
        elapsed, metrics = run_case(args.records, args.clients, journal)
        print(
            f"journal={'on' if journal else 'off'} "
            f"records={args.records} clients={args.clients} "
            f"seconds={elapsed:.6f} "
            f"records_per_second={args.records / elapsed:.0f} "
            f"commits={metrics['commits']} syncs={metrics['syncs']} "
            f"maximum_group={metrics['maximum_group']} "
            f"sync_seconds={metrics['sync_nanoseconds'] / 1e9:.6f}"
        )


if __name__ == "__main__":
    main()
