"""Worker-scoped byte service for Controller-selected transfers."""

import argparse
import fcntl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import threading
import time
import urllib.parse

from .cache import WorkerDiskStore


def _alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, ValueError):
        return False


def _worker_pid():
    pid = os.getppid()
    while pid > 1:
        try:
            if Path(f"/proc/{pid}/comm").read_text().strip() == "vine_worker":
                return pid
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith("PPid:"):
                    pid = int(line.split()[1])
                    break
            else:
                return 0
        except (FileNotFoundError, PermissionError, ValueError):
            return 0
    return 0


def _metadata(path):
    try:
        pid, owner, port, capability = path.read_text().split()
        pid, owner, port = int(pid), int(owner), int(port)
        if not (_alive(pid) and _alive(owner)):
            return None
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            pass
        return pid, owner, port, capability
    except (OSError, ValueError):
        return None


class WorkerDataService:
    def __init__(self, cache):
        root = cache.disk.root
        if root is None:
            raise RuntimeError("worker disk store is not configured")
        metadata_path = root / ".data-service"
        with (root / ".data-service.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            metadata = _metadata(metadata_path)
            if metadata is None:
                owner = _worker_pid()
                if not owner:
                    raise RuntimeError("TaskVine worker process is unavailable")
                metadata_path.unlink(missing_ok=True)
                error_path = root / ".data-service.stderr"
                with error_path.open("wb") as errors:
                    process = subprocess.Popen(
                        (
                            sys.executable,
                            "-m",
                            __name__,
                            "--serve",
                            str(root),
                            str(owner),
                        ),
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=errors,
                        start_new_session=True,
                        close_fds=True,
                    )
                deadline = time.monotonic() + 30
                while metadata is None and time.monotonic() < deadline:
                    if process.poll() is not None:
                        break
                    time.sleep(0.01)
                    metadata = _metadata(metadata_path)
                if metadata is None:
                    detail = error_path.read_text(errors="replace").strip()
                    raise RuntimeError(
                        "worker data service did not start: "
                        f"python={sys.executable} exit={process.poll()} "
                        f"error={detail or 'none'}"
                    )
                error_path.unlink(missing_ok=True)
        _, _, port, capability = metadata
        self.base_endpoint = (
            f"http://{socket.getfqdn()}:{port}/{capability}"
        )
        self.disk = cache.disk

    def endpoint(self, controller, token):
        return f"{self.base_endpoint}/{self.disk.scope(controller, token)}"


def _serve(root, owner_pid):
    root = Path(root)
    metadata_path = root / ".data-service"
    disk = WorkerDiskStore()
    disk.root = root
    capability = secrets.token_urlsafe(24)

    class Handler(BaseHTTPRequestHandler):
        def _request_data(self):
            parsed = urllib.parse.urlparse(self.path)
            prefix = f"/{capability}/"
            if not parsed.path.startswith(prefix):
                return None
            pieces = parsed.path[len(prefix):].split("/")
            query = urllib.parse.parse_qs(parsed.query)
            if (
                len(pieces) != 4
                or len(pieces[0]) != 16
                or any(c not in "0123456789abcdef" for c in pieces[0])
                or pieces[1] != "data"
                or pieces[2] not in ("e", "i")
                or not pieces[3].isdigit()
                or len(query.get("sha256", ())) != 1
                or len(query.get("size", ())) != 1
            ):
                return None
            try:
                size = int(query["size"][0])
            except ValueError:
                return None
            return (
                pieces[0],
                f"{pieces[2]}:{int(pieces[3])}",
                query["sha256"][0],
                size,
            )

        def do_GET(self):
            request = self._request_data()
            payload = disk.find_data(*request) if request else None
            if payload is None:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("X-DataVine-SHA256", request[2])
            self.end_headers()
            self.wfile.write(payload)

        def do_DELETE(self):
            request = self._request_data()
            if request is None:
                self.send_error(404)
                return
            disk.remove_data(*request)
            self.send_response(204)
            self.end_headers()

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
    server.daemon_threads = True
    temporary = metadata_path.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(
        f"{os.getpid()} {int(owner_pid)} {server.server_port} {capability}\n"
    )
    os.replace(temporary, metadata_path)

    def watch_owner():
        while _alive(owner_pid):
            time.sleep(0.25)
        server.shutdown()

    threading.Thread(target=watch_owner, daemon=True).start()
    try:
        server.serve_forever()
    finally:
        server.server_close()
        try:
            if int(metadata_path.read_text().split()[0]) == os.getpid():
                metadata_path.unlink()
        except (FileNotFoundError, IndexError, OSError, ValueError):
            pass


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", nargs=2, metavar=("ROOT", "OWNER_PID"))
    args = parser.parse_args(argv)
    if args.serve is None:
        parser.error("--serve is required")
    _serve(args.serve[0], int(args.serve[1]))


if __name__ == "__main__":
    main()
