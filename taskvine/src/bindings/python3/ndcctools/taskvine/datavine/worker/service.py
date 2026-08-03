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
                prefix = f"/{service.capability}/data/"
                if not parsed.path.startswith(prefix):
                    return None
                pieces = parsed.path[len(prefix):].split("/")
                query = urllib.parse.parse_qs(parsed.query)
                if (
                    len(pieces) != 2
                    or pieces[0] not in ("e", "i")
                    or not pieces[1].isdigit()
                    or len(query.get("sha256", ())) != 1
                    or len(query.get("size", ())) != 1
                ):
                    return None
                try:
                    size = int(query["size"][0])
                except ValueError:
                    return None
                return (
                    f"{pieces[0]}:{int(pieces[1])}",
                    query["sha256"][0],
                    size,
                )

            def do_GET(self):
                request = self._request_data()
                if request is None:
                    self.send_error(404)
                    return
                data_key, content_hash, size = request
                with service.cache.lock:
                    payload = service.cache.data.find_data(
                        data_key, content_hash, size
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
                    service.cache.data.remove_data(*request)
                self.send_response(204)
                self.end_headers()

            def log_message(self, format, *args):
                return

        self.server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
        host = socket.getfqdn()
        self.endpoint = (
            f"http://{host}:{self.server.server_port}/{self.capability}"
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            name="datavine-worker-data",
            daemon=True,
        )
        self.thread.start()
