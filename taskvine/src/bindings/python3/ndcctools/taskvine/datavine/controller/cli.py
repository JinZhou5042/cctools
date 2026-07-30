"""Command line entry point for the standalone Data Controller."""

import argparse
import json
import os
import secrets
import signal
import threading

from .service import ControllerService
from .state import ControllerState


def main(argv=None):
    parser = argparse.ArgumentParser(prog="datavine_controller")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--advertise-host", default=None)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--token", default=None)
    parser.add_argument("--token-file", default=None)
    parser.add_argument(
        "--max-edata-bytes", type=int, default=256 * 1024 * 1024
    )
    parser.add_argument(
        "--max-request-concurrency", type=int, default=32
    )
    parser.add_argument(
        "--max-serving-concurrency", type=int, default=8
    )
    parser.add_argument(
        "--max-serving-bytes", type=int, default=64 * 1024 * 1024
    )
    parser.add_argument("--ready-file")
    parser.add_argument("--persistence-dir")
    parser.add_argument("--persistence-workers", type=int, default=1)
    parser.add_argument("--persistence-fail-first", action="store_true")
    args = parser.parse_args(argv)
    token = args.token
    if args.token_file:
        with open(args.token_file, encoding="utf-8") as stream:
            token = stream.read().strip()
    token = token or secrets.token_urlsafe(32)
    state = ControllerState(args.max_edata_bytes)
    if args.persistence_dir:
        state.configure_persistence(
            args.persistence_dir,
            args.persistence_workers,
            args.persistence_fail_first,
        )
    service = ControllerService(
        args.host,
        args.port,
        token,
        state,
        args.max_request_concurrency,
        args.max_serving_concurrency,
        args.max_serving_bytes,
    )
    _, port = service.start()
    ready = {
        "pid": os.getpid(),
        "controller_thread": service.thread_ident,
        "host": args.advertise_host or args.host,
        "port": port,
        "token": token,
        "protocol_version": 1,
    }
    encoded = json.dumps(ready, sort_keys=True)
    if args.ready_file:
        with open(args.ready_file, "w", encoding="utf-8") as stream:
            stream.write(encoded + "\n")
    print(encoded, flush=True)
    stopped = threading.Event()

    def on_signal(signum, frame):
        stopped.set()

    old_term = signal.signal(signal.SIGTERM, on_signal)
    old_int = signal.signal(signal.SIGINT, on_signal)
    try:
        stopped.wait()
    finally:
        service.stop()
        signal.signal(signal.SIGTERM, old_term)
        signal.signal(signal.SIGINT, old_int)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
