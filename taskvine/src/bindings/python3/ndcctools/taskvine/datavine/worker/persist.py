"""Persist one validated worker-local IData realization to SharedFS."""

import argparse
import hashlib
import os
from pathlib import Path
import time

from ..scheduler.client import ControllerClient


def main(argv=None):
    parser = argparse.ArgumentParser(prog="datavine_worker_persist")
    parser.add_argument("--controller", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--data-id", required=True, type=int)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--delay-before-complete", type=float, default=0)
    parser.add_argument(
        "--inject-failure-during-write", action="store_true"
    )
    parser.add_argument("--delay-before-failure", type=float, default=0)
    args = parser.parse_args(argv)
    client = ControllerClient(args.controller, args.token)
    request = client.begin_external_persistence(
        args.data_id, args.request_id
    )
    if request["state"] == "durable":
        print(
            f"DATAVINE_PERSISTED i:{args.data_id} "
            f"{args.request_id} idempotent"
        )
        return 0
    source = Path(args.input_file)
    target = Path(request["target_path"])
    temporary = target.parent / (
        f".{args.request_id}.{os.getpid()}.tmp"
    )
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        size = 0
        with source.open("rb") as reader, temporary.open("wb") as writer:
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
                        "injected temporary SharedFS unavailability"
                    )
            writer.flush()
            os.fsync(writer.fileno())
        if (
            size != int(request["size"])
            or digest.hexdigest() != request["content_hash"]
        ):
            raise IOError("worker persistence source validation failed")
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
        temporary.unlink(missing_ok=True)
        client.fail_external_persistence(
            args.data_id, args.request_id, exc
        )
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
