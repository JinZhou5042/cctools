"""Standalone Data Controller HTTP service running on its own thread."""

import base64
import http.server
import json
import threading
import urllib.parse

from ..models import SerializationMetadata, TaskRecord
from ..protocol import API_PREFIX, PROTOCOL_VERSION, TOKEN_HEADER
from .state import ControllerState


class ControllerService:
    def __init__(self, host, port, token, state=None):
        if not token:
            raise ValueError("Controller token is required")
        self.host = host
        self.port = int(port)
        self.token = token
        self.state = state or ControllerState()
        self._server = None
        self._thread = None

    def start(self):
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def _authorized(self):
                query = urllib.parse.parse_qs(
                    urllib.parse.urlparse(self.path).query
                )
                return (
                    self.headers.get(TOKEN_HEADER) == owner.token
                    or query.get("token") == [owner.token]
                )

            def _json(self, status, value):
                payload = json.dumps(
                    value, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def _error(self, status, message):
                self._json(status, {"error": str(message)})

            def do_GET(self):
                if not self._authorized():
                    self._error(403, "forbidden")
                    return
                parsed = urllib.parse.urlparse(self.path)
                if parsed.path == f"{API_PREFIX}/health":
                    self._json(
                        200,
                        {
                            "status": "ready",
                            "protocol_version": PROTOCOL_VERSION,
                            "controller_thread": owner._thread.ident,
                        },
                    )
                    return
                if parsed.path == f"{API_PREFIX}/snapshot":
                    self._json(200, owner.state.snapshot())
                    return
                prefix = f"{API_PREFIX}/edata/"
                if parsed.path.startswith(prefix):
                    token = parsed.path[len(prefix):]
                    metadata_only = token.endswith("/metadata")
                    if metadata_only:
                        token = token[:-len("/metadata")]
                    if not token.isdigit():
                        self._error(400, "invalid EDataID")
                        return
                    try:
                        record = owner.state.get_edata(int(token))
                    except KeyError as exc:
                        self._error(404, exc)
                        return
                    if metadata_only:
                        self._json(
                            200,
                            {
                                "data_id": record.data_id,
                                "content_hash": record.content_hash,
                                "size": len(record.serialized_bytes),
                                "metadata": record.metadata.to_dict(),
                            },
                        )
                        return
                    owner.state.record_edata_fetch(record.data_id)
                    payload = record.serialized_bytes
                    self.send_response(200)
                    self.send_header(
                        "Content-Type", "application/octet-stream"
                    )
                    self.send_header("Content-Length", str(len(payload)))
                    self.send_header(
                        "X-DataVine-SHA256", record.content_hash
                    )
                    self.send_header(
                        "X-DataVine-Metadata",
                        base64.urlsafe_b64encode(
                            json.dumps(
                                record.metadata.to_dict(),
                                sort_keys=True,
                                separators=(",", ":"),
                            ).encode("utf-8")
                        ).decode("ascii"),
                    )
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                task_prefix = f"{API_PREFIX}/tasks/"
                if parsed.path.startswith(task_prefix):
                    token = parsed.path[len(task_prefix):]
                    try:
                        record = owner.state.get_task(int(token))
                    except (KeyError, ValueError) as exc:
                        self._error(404, exc)
                        return
                    self._json(200, record.to_dict())
                    return
                idata_prefix = f"{API_PREFIX}/idata/"
                if parsed.path.startswith(idata_prefix):
                    token = parsed.path[len(idata_prefix):]
                    status_only = token.endswith("/status")
                    if status_only:
                        token = token[:-len("/status")]
                    try:
                        if status_only:
                            self._json(
                                200, owner.state.idata_status(int(token))
                            )
                            return
                        record = owner.state.get_idata(int(token))
                    except (KeyError, ValueError) as exc:
                        self._error(404, exc)
                        return
                    if record.serialized_bytes is None:
                        self._error(409, "IData is not available")
                        return
                    payload = record.serialized_bytes
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Length", str(len(payload)))
                    self.send_header("X-DataVine-SHA256", record.content_hash)
                    self.send_header("X-DataVine-Attempt", str(record.attempt))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                self._error(404, "not found")

            def do_POST(self):
                if not self._authorized():
                    self._error(403, "forbidden")
                    return
                if self.path == f"{API_PREFIX}/idata/allocate":
                    try:
                        request = self._read_json()
                        record = owner.state.allocate_idata(
                            request["producer_task_id"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, {"data_id": record.data_id})
                    return
                if self.path == f"{API_PREFIX}/tasks/register":
                    try:
                        record = owner.state.register_task(
                            TaskRecord.from_dict(self._read_json())
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, record.to_dict())
                    return
                if self.path.startswith(f"{API_PREFIX}/idata/") and self.path.endswith("/persist"):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):-len("/persist")
                    ]
                    try:
                        owner.state.request_persistence(int(token))
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(202, owner.state.idata_status(int(token)))
                    return
                if self.path.startswith(f"{API_PREFIX}/idata/") and self.path.endswith("/persist/cancel"):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):-len("/persist/cancel")
                    ]
                    try:
                        request = self._read_json()
                        action = owner.state.cancel_persistence(
                            int(token), request.get("reason", "obsolete")
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "action": action,
                            "status": owner.state.idata_status(int(token)),
                        },
                    )
                    return
                if self.path.startswith(f"{API_PREFIX}/idata/") and self.path.endswith("/invalidate"):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):-len("/invalidate")
                    ]
                    try:
                        action = owner.state.invalidate_volatile_idata(
                            int(token)
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "action": action,
                            "status": owner.state.idata_status(int(token)),
                        },
                    )
                    return
                if self.path.startswith(f"{API_PREFIX}/idata/") and self.path.endswith("/publish"):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):-len("/publish")
                    ]
                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                        if length < 0:
                            raise ValueError("invalid request size")
                        payload = self.rfile.read(length)
                        record = owner.state.publish_idata(
                            int(token),
                            int(self.headers.get("X-DataVine-Attempt", "1")),
                            payload,
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "data_id": record.data_id,
                            "content_hash": record.content_hash,
                            "size": len(record.serialized_bytes),
                            "attempt": record.attempt,
                        },
                    )
                    return
                if self.path != f"{API_PREFIX}/edata/register":
                    self._error(404, "not found")
                    return
                try:
                    request = self._read_json()
                    metadata = SerializationMetadata.from_dict(
                        request["metadata"]
                    )
                    payload = base64.b64decode(
                        request["serialized_bytes"], validate=True
                    )
                    record = owner.state.register_edata(metadata, payload)
                except MemoryError as exc:
                    self._error(507, exc)
                    return
                except Exception as exc:
                    self._error(400, exc)
                    return
                self._json(
                    200,
                    {
                        "data_id": record.data_id,
                        "content_hash": record.content_hash,
                        "size": len(record.serialized_bytes),
                    },
                )

            def _read_json(self):
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > owner.state.max_edata_bytes * 2:
                    raise ValueError("invalid request size")
                return json.loads(self.rfile.read(length))

            def log_message(self, format_string, *args):
                return

        # One dedicated Controller thread owns request ordering and state
        # transitions. Bounded concurrency is introduced later at explicit
        # data-transfer/persistence admission points, not implicitly here.
        self._server = http.server.HTTPServer(
            (self.host, self.port), Handler
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="datavine-controller",
            daemon=False,
        )
        self._thread.start()
        return self._server.server_address

    @property
    def thread_ident(self):
        return self._thread.ident if self._thread else None

    def stop(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                raise RuntimeError("Controller thread did not stop")
        self.state.stop()
        self._server = None
        self._thread = None
