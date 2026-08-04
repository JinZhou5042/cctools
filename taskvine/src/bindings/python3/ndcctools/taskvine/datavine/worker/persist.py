"""Persist one validated worker-local IData realization to SharedFS."""

import argparse
import hashlib
import os
from pathlib import Path
import time
import uuid

from ..scheduler.client import ControllerClient


def _open_source(client, args):
    if args.input_file:
        return Path(args.input_file).open("rb"), None
    status = client.idata_status(args.data_id)
    if status["controller_inline"]:
        return client.fetch_idata_stream(args.data_id), None
    worker_id = os.environ.get("VINE_WORKER_ID")
    if not worker_id:
        raise RuntimeError("worker identity is required for peer persistence")
    client.claim_worker(worker_id)
    failed = set()
    while True:
        resolved = client.resolve_worker_source(
            f"i:{args.data_id}",
            worker_id,
            f"taskvine:{uuid.uuid4().hex}",
            allow_local_source=True,
        )
        source = resolved["source"]
        lease_id = resolved["lease"]["lease_id"]
        if source["replica_id"] in failed:
            client.release_replica(lease_id, False)
            raise RuntimeError("Controller repeated a failed persistence source")
        try:
            source_url = source.get("source_url")
            if not source_url:
                raise RuntimeError(
                    "selected persistence source has no data endpoint"
                )
            return client.open_source(source_url), lease_id
        except Exception:
            failed.add(source["replica_id"])
            client.release_replica(lease_id, False)
            client.invalidate_observed_replica(
                source["data_id"],
                source["replica_id"],
                source["attempt"],
                source["content_hash"],
                source["size"],
                source["worker_id"],
                source["worker_epoch"],
            )


def main(argv=None):
    parser = argparse.ArgumentParser(prog="datavine_worker_persist")
    parser.add_argument("--controller", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--data-id", required=True, type=int)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--input-file")
    parser.add_argument("--delay-before-complete", type=float, default=0)
    parser.add_argument(
        "--inject-failure-during-write", action="store_true"
    )
    parser.add_argument("--delay-before-failure", type=float, default=0)
    args = parser.parse_args(argv)
    client = ControllerClient(
        args.controller,
        args.token,
        transient_retries=8,
    )
    request = client.begin_external_persistence(
        args.data_id, args.request_id
    )
    if request["state"] == "durable":
        print(
            f"DATAVINE_PERSISTED i:{args.data_id} "
            f"{args.request_id} idempotent"
        )
        return 0
    target = Path(request["target_path"])
    temporary = target.parent / (
        f".{args.request_id}.{os.getpid()}.tmp"
    )
    lease_id = None
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        size = 0
        source, lease_id = _open_source(client, args)
        with source as reader, temporary.open("wb") as writer:
            while True:
                chunk = reader.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                digest.update(chunk)
                writer.write(chunk)
                if args.inject_failure_during_write:
                    writer.flush()
                    if args.delay_before_failure > 0:
                        time.sleep(args.delay_before_failure)
                    print(
                        "DATAVINE_PERSISTENCE_INJECTED_FAILURE "
                        f"i:{args.data_id} {args.request_id}",
                        flush=True,
                    )
                    raise OSError(
                        "DATAVINE_PERSISTENCE_INJECTED_FAILURE: "
                        "temporary SharedFS unavailability"
                    )
            writer.flush()
            os.fsync(writer.fileno())
        if (
            size != int(request["size"])
            or digest.hexdigest() != request["content_hash"]
        ):
            raise IOError(
                "worker persistence source validation failed: "
                f"request={args.request_id} "
                f"expected_size={request['size']} actual_size={size} "
                f"expected_hash={request['content_hash']} "
                f"actual_hash={digest.hexdigest()}"
            )
        if lease_id is not None:
            client.release_replica(lease_id, True)
            lease_id = None
        temporary.replace(target)
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        if args.delay_before_complete > 0:
            time.sleep(args.delay_before_complete)
        completion = client.complete_external_persistence(
            args.data_id, args.request_id
        )
        durability = completion["durability"]
        event = (
            "DATAVINE_PERSISTED"
            if durability == "durable"
            else "DATAVINE_PERSISTENCE_CANCELLED"
        )
        print(f"{event} i:{args.data_id} {args.request_id}")
    except Exception as exc:
        if lease_id is not None:
            client.release_replica(lease_id, False)
        temporary.unlink(missing_ok=True)
        client.fail_external_persistence(
            args.data_id, args.request_id, exc
        )
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
