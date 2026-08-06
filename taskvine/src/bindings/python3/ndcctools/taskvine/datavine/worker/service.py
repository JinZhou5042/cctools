"""Worker-scoped byte service for Controller-selected transfers."""

import argparse
import fcntl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import secrets
import signal
import socket
import socketserver
import struct
import subprocess
import sys
import threading
import time
import urllib.parse

from .cache import WorkerDiskStore, WorkerMemoryStore


_PUT = struct.Struct("!16scQ64sQQqB")
_GET = struct.Struct("!16scQ")
_PUT_RESPONSE = struct.Struct("!BQQQQQQQ")
_GET_RESPONSE = struct.Struct("!QQQQQQQQ")
_MISSING = (1 << 64) - 1
_SNAPSHOT_FIELDS = (
    "capacity_bytes", "bytes", "items", "hits", "misses",
    "admissions", "evictions",
)


def _recv_exact(stream, size):
    chunks = []
    while size:
        chunk = stream.recv(size)
        if not chunk:
            raise EOFError("worker data service connection closed")
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)


def _socket_address(capability):
    return "\0datavine-" + str(capability)


def _alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, ValueError):
        return False


def _worker_pid():
    pid = os.getppid()
    worker = 0
    while pid > 1:
        try:
            if Path(f"/proc/{pid}/comm").read_text().strip() == "vine_worker":
                worker = pid
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith("PPid:"):
                    pid = int(line.split()[1])
                    break
            else:
                return 0
        except (FileNotFoundError, PermissionError, ValueError):
            break
    return worker


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
        self._socket_address = _socket_address(capability)
        self._connections = threading.local()
        self._snapshot = dict.fromkeys(_SNAPSHOT_FIELDS, 0)
        self.disk = cache.disk

    def endpoint(self, controller, token):
        return f"{self.base_endpoint}/{self.disk.scope(controller, token)}"

    def _identity(self, controller, token, data_key):
        kind, data_id = str(data_key).split(":", 1)
        return (
            self.disk.scope(controller, token).encode("ascii"),
            kind.encode("ascii"),
            int(data_id),
        )

    def _exchange(
        self, request, payload=b"", response_size=1, size_prefixed=False
    ):
        for attempt in range(2):
            connection = getattr(self._connections, "socket", None)
            try:
                if connection is None:
                    connection = socket.socket(
                        socket.AF_UNIX, socket.SOCK_STREAM
                    )
                    connection.settimeout(2)
                    connection.connect(self._socket_address)
                    self._connections.socket = connection
                connection.sendall(request + payload)
                response = _recv_exact(connection, response_size)
                if not size_prefixed:
                    return response
                size = struct.unpack_from("!Q", response)[0]
                return response, (
                    None
                    if size == _MISSING
                    else _recv_exact(connection, size)
                )
            except (EOFError, OSError):
                connection.close()
                self._connections.socket = None
                if attempt:
                    raise

    def put_data(
        self,
        controller,
        token,
        data_key,
        content_hash,
        payload,
        capacity,
        score=None,
        protected=False,
    ):
        scope, kind, data_id = self._identity(
            controller, token, data_key
        )
        response = self._exchange(
            b"P"
            + _PUT.pack(
                scope,
                kind,
                data_id,
                str(content_hash).encode("ascii"),
                len(payload),
                int(capacity),
                int(
                    score
                    if score is not None
                    else 1_000_000 // max(1, len(payload))
                ),
                int(bool(protected)),
            ),
            payload,
            _PUT_RESPONSE.size,
        )
        values = _PUT_RESPONSE.unpack(response)
        admitted, snapshot = values[0], values[1:]
        self._snapshot.update(
            dict(zip(_SNAPSHOT_FIELDS, snapshot))
        )
        return bool(admitted)

    def get_local_data(self, controller, token, data_key):
        scope, kind, data_id = self._identity(
            controller, token, data_key
        )
        response, payload = self._exchange(
            b"G" + _GET.pack(scope, kind, data_id),
            b"",
            _GET_RESPONSE.size,
            size_prefixed=True,
        )
        values = _GET_RESPONSE.unpack(response)
        _, snapshot = values[0], values[1:]
        self._snapshot.update(dict(zip(_SNAPSHOT_FIELDS, snapshot)))
        return payload

    def snapshot(self):
        return dict(self._snapshot)


def _serve(root, owner_pid):
    root = Path(root)
    metadata_path = root / ".data-service"
    disk = WorkerDiskStore()
    disk.root = root
    memory = WorkerMemoryStore()
    capability = secrets.token_urlsafe(24)

    class LocalHandler(socketserver.BaseRequestHandler):
        def handle(self):
            while True:
                operation = self.request.recv(1)
                if not operation:
                    return
                if operation == b"P":
                    (
                        scope,
                        kind,
                        data_id,
                        digest,
                        size,
                        capacity,
                        score,
                        protected,
                    ) = (
                        _PUT.unpack(_recv_exact(self.request, _PUT.size))
                    )
                    payload = _recv_exact(self.request, size)
                    result = memory.put(
                        scope.decode("ascii"),
                        f"{kind.decode('ascii')}:{data_id}",
                        digest.decode("ascii"),
                        payload,
                        capacity,
                        score,
                        protected,
                    )
                    self.request.sendall(_PUT_RESPONSE.pack(*result))
                elif operation == b"G":
                    scope, kind, data_id = _GET.unpack(
                        _recv_exact(self.request, _GET.size)
                    )
                    payload = memory.local(
                        scope.decode("ascii"),
                        f"{kind.decode('ascii')}:{data_id}",
                    )
                    snapshot = memory.snapshot()
                    self.request.sendall(
                        _GET_RESPONSE.pack(
                            _MISSING if payload is None else len(payload),
                            *(snapshot[field] for field in _SNAPSHOT_FIELDS),
                        )
                    )
                    if payload is not None:
                        self.request.sendall(payload)
                else:
                    return

    class LocalServer(socketserver.ThreadingUnixStreamServer):
        request_queue_size = socket.SOMAXCONN

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _empty(self, status):
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def _kill_request(self):
            parsed = urllib.parse.urlparse(self.path)
            prefix = f"/{capability}/"
            pieces = (
                parsed.path[len(prefix):].split("/")
                if parsed.path.startswith(prefix)
                else ()
            )
            return (
                len(pieces) == 2
                and len(pieces[0]) == 16
                and all(c in "0123456789abcdef" for c in pieces[0])
                and pieces[1] == "kill"
            )

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
            ):
                return None
            if (
                len(query.get("sha256", ())) != 1
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
            if request is None:
                payload = None
            else:
                payload = memory.find(*request)
                if payload is None:
                    payload = disk.find_data(*request)
            if payload is None:
                self.send_error(404)
                return
            query = urllib.parse.parse_qs(
                urllib.parse.urlparse(self.path).query
            )
            if query.get("fault") == ["corrupt"] and payload:
                payload = bytes((payload[0] ^ 1,)) + payload[1:]
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("X-DataVine-SHA256", request[2])
            self.end_headers()
            self.wfile.write(payload)

        def do_DELETE(self):
            if self._kill_request():
                self._empty(204)
                self.wfile.flush()

                def terminate():
                    time.sleep(0.02)
                    os.killpg(os.getpgid(int(owner_pid)), signal.SIGKILL)
                    os._exit(0)

                threading.Thread(target=terminate, daemon=True).start()
                return
            request = self._request_data()
            if request is None:
                self.send_error(404)
                return
            memory.remove(*request)
            disk.remove_data(*request)
            self._empty(204)

        def log_message(self, format, *args):
            return

    local_server = LocalServer(
        _socket_address(capability), LocalHandler
    )
    local_server.daemon_threads = True
    threading.Thread(
        target=local_server.serve_forever, daemon=True
    ).start()
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
        local_server.shutdown()

    threading.Thread(target=watch_owner, daemon=True).start()
    try:
        server.serve_forever()
    finally:
        server.server_close()
        local_server.server_close()
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
