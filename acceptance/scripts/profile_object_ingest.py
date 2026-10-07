#!/usr/bin/env python3
"""Measure cold and deduplicated Controller-RPC ingest without Worker noise."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time

from ndcctools.taskvine.datavine.workflow_client import WorkflowClient


def start_service(executable, journal, token):
    process = subprocess.Popen(
        (str(executable), "serve", str(journal), token),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    line = process.stdout.readline()
    if not line:
        raise RuntimeError(process.stderr.read())
    return process, json.loads(line)["endpoint"]


def stop_service(process):
    process.send_signal(signal.SIGTERM)
    _, stderr = process.communicate(timeout=30)
    if process.returncode:
        raise RuntimeError(stderr)


def timed_put(client, objects, workers):
    started = time.monotonic()
    results = client.put_objects(objects, workers=workers)
    seconds = time.monotonic() - started
    return {
        "seconds": seconds,
        "objects_per_second": len(objects) / seconds,
        "objects": len(objects),
        "bytes": sum(result["size"] for result in results),
        "deduplicated": sum(result["deduplicated"] for result in results),
        "rpc_seconds_aggregate": sum(
            result["rpc_nanoseconds"] for result in results
        ) / 1e9,
    }


def run(executable, scratch_parent, count, payload_bytes, workers):
    root = Path(tempfile.mkdtemp(
        prefix=f"datavine-ingest-{workers}-", dir=scratch_parent
    ))
    token = "object-ingest-profile-token"
    service = None
    try:
        service, endpoint = start_service(executable, root / "journal", token)
        client = WorkflowClient(endpoint, token, timeout=120)
        objects = []
        for index in range(count):
            prefix = index.to_bytes(8, "big")
            payload = prefix + bytes([index % 251]) * max(0, payload_bytes - 8)
            objects.append((payload, hashlib.sha256(payload).hexdigest()))
        cold = timed_put(client, objects, workers)
        warm = timed_put(client, objects, workers)
        client.close()
        return {"writers": workers, "cold": cold, "warm": warm}
    finally:
        if service is not None and service.poll() is None:
            stop_service(service)
        shutil.rmtree(root)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scratch-parent", type=Path, required=True)
    parser.add_argument("--objects", type=int, default=4096)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--writers", type=int, nargs="+", default=(1, 4))
    args = parser.parse_args()
    args.scratch_parent.mkdir(parents=True, exist_ok=True)
    artifact = {
        "artifact_type": "datavine-object-ingest-profile",
        "schema_version": 2,
        "objects": args.objects,
        "payload_bytes": args.payload_bytes,
        "runs": [
            run(
                args.executable.resolve(), args.scratch_parent,
                args.objects, args.payload_bytes, writers,
            )
            for writers in args.writers
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    print(json.dumps(artifact, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
