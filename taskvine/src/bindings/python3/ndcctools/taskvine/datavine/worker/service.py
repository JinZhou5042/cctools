"""Worker-local byte service for Controller-selected peer transfers."""

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import secrets
import socket
import threading
import urllib.parse


class WorkerDataService:
    def __init__(self, cache):
        self.cache = cache
        self.capability = secrets.token_urlsafe(24)
        service = self

        class Handler(BaseHTTPRequestHandler):
            def _request_data(self):
                parsed = urllib.parse.urlparse(self.path)
                prefix = f"/{service.capability}/"
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
                if request is None:
                    self.send_error(404)
                    return
                scope, data_key, content_hash, size = request
                with service.cache.lock:
                    payload = service.cache.data.find_data(
                        data_key, content_hash, size
                    )
                    if payload is None:
                        payload = service.cache.disk.find_data(
                            scope, data_key, content_hash, size
                        )
                if (
                    payload is None
                    or len(payload) != size
                    or hashlib.sha256(payload).hexdigest() != content_hash
                ):
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("X-DataVine-SHA256", content_hash)
                self.end_headers()
                self.wfile.write(payload)

            def do_DELETE(self):
                request = self._request_data()
                if request is None:
                    self.send_error(404)
                    return
                with service.cache.lock:
                    _, data_key, content_hash, size = request
                    service.cache.data.remove_data(
                        data_key, content_hash, size
                    )
                    service.cache.disk.remove_data(*request)
                self.send_response(204)
                self.end_headers()

            def log_message(self, format, *args):
                return

        self.server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
        host = socket.getfqdn()
        self.base_endpoint = (
            f"http://{host}:{self.server.server_port}/{self.capability}"
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            name="datavine-worker-data",
            daemon=True,
        )
        self.thread.start()

    def endpoint(self, controller, token):
        scope = self.cache.disk.scope(controller, token)
        return f"{self.base_endpoint}/{scope}"
