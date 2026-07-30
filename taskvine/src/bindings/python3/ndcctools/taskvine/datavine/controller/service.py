"""Standalone Data Controller HTTP service running on its own thread."""

import base64
import dataclasses
import http.server
import json
import threading
import urllib.parse

from ..models import SerializationMetadata, TaskRecord
from ..protocol import API_PREFIX, PROTOCOL_VERSION, TOKEN_HEADER
from .state import ControllerState


class ByteServingAdmission:
    """Bound concurrent Controller byte responses without owning payloads."""

    def __init__(self, max_concurrency, max_inflight_bytes):
        self.max_concurrency = int(max_concurrency)
        self.max_inflight_bytes = int(max_inflight_bytes)
        if self.max_concurrency < 1 or self.max_inflight_bytes < 1:
            raise ValueError("byte-serving capacities must be positive")
        self._lock = threading.Lock()
        self._active = 0
        self._inflight_bytes = 0
        self._active_high_water = 0
        self._bytes_high_water = 0
        self._admitted = 0
        self._rejected = 0
        self._bytes_served = 0

    def acquire(self, size):
        size = int(size)
        if size < 0:
            raise ValueError("serving size cannot be negative")
        with self._lock:
            if (
                self._active >= self.max_concurrency
                or self._inflight_bytes + size > self.max_inflight_bytes
            ):
                self._rejected += 1
                return False
            self._active += 1
            self._inflight_bytes += size
            self._admitted += 1
            self._active_high_water = max(
                self._active_high_water, self._active
            )
            self._bytes_high_water = max(
                self._bytes_high_water, self._inflight_bytes
            )
            return True

    def release(self, size, completed):
        size = int(size)
        with self._lock:
            if self._active < 1 or self._inflight_bytes < size:
                raise RuntimeError("invalid byte-serving release")
            self._active -= 1
            self._inflight_bytes -= size
            if completed:
                self._bytes_served += size

    def snapshot(self):
        with self._lock:
            return {
                "active": self._active,
                "active_capacity": self.max_concurrency,
                "active_high_water": self._active_high_water,
                "inflight_bytes": self._inflight_bytes,
                "inflight_byte_capacity": self.max_inflight_bytes,
                "inflight_byte_high_water": self._bytes_high_water,
                "admitted": self._admitted,
                "rejected": self._rejected,
                "bytes_served": self._bytes_served,
            }


class BoundedThreadingHTTPServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, handler, max_requests):
        self.max_requests = int(max_requests)
        if self.max_requests < 1:
            raise ValueError("request concurrency must be positive")
        self._request_slots = threading.BoundedSemaphore(
            self.max_requests
        )
        self._request_lock = threading.Lock()
        self._request_active = 0
        self._request_high_water = 0
        self._request_rejected = 0
        super().__init__(address, handler)

    def process_request(self, request, client_address):
        if not self._request_slots.acquire(blocking=False):
            with self._request_lock:
                self._request_rejected += 1
            try:
                request.sendall(
                    b"HTTP/1.1 503 Service Unavailable\r\n"
                    b"Content-Length: 0\r\n"
                    b"Connection: close\r\n\r\n"
                )
            finally:
                self.shutdown_request(request)
            return
        with self._request_lock:
            self._request_active += 1
            self._request_high_water = max(
                self._request_high_water, self._request_active
            )
        try:
            super().process_request(request, client_address)
        except Exception:
            self._release_request_slot()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._release_request_slot()

    def _release_request_slot(self):
        with self._request_lock:
            self._request_active -= 1
        self._request_slots.release()

    def admission_snapshot(self):
        with self._request_lock:
            return {
                "active": self._request_active,
                "active_capacity": self.max_requests,
                "active_high_water": self._request_high_water,
                "rejected": self._request_rejected,
            }


class ControllerService:
    def __init__(
        self,
        host,
        port,
        token,
        state=None,
        max_request_concurrency=32,
        max_serving_concurrency=8,
        max_serving_bytes=64 * 1024 * 1024,
        serving_hook=None,
    ):
        if not token:
            raise ValueError("Controller token is required")
        self.host = host
        self.port = int(port)
        self.token = token
        self.state = state or ControllerState()
        self.max_request_concurrency = int(max_request_concurrency)
        self.byte_serving = ByteServingAdmission(
            max_serving_concurrency, max_serving_bytes
        )
        self.serving_hook = serving_hook
        self._server = None
        self._thread = None

    def snapshot(self):
        value = self.state.snapshot()
        value["byte_serving"] = self.byte_serving.snapshot()
        value["request_admission"] = (
            self._server.admission_snapshot()
            if self._server is not None
            else None
        )
        return value

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
                    self._json(200, owner.snapshot())
                    return
                if parsed.path == f"{API_PREFIX}/pruning/plan":
                    try:
                        plan = owner.state.pruning_plan()
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, plan)
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
                                "size": record.serialized_size,
                                "metadata": record.metadata.to_dict(),
                                "storage": (
                                    "controller-memory"
                                    if record.serialized_bytes is not None
                                    else "bulk-origin"
                                ),
                                "origin_path": record.stable_path,
                            },
                        )
                        return
                    payload = record.serialized_bytes
                    if payload is None:
                        self._error(
                            409,
                            "EData uses a stable bulk origin and is not "
                            "served by the Controller",
                        )
                        return
                    if not owner.byte_serving.acquire(len(payload)):
                        self._error(503, "byte serving capacity exceeded")
                        return
                    owner.state.record_edata_fetch(record.data_id)
                    completed = False
                    try:
                        if owner.serving_hook is not None:
                            owner.serving_hook(f"e:{record.data_id}")
                        self.send_response(200)
                        self.send_header(
                            "Content-Type", "application/octet-stream"
                        )
                        self.send_header(
                            "Content-Length", str(len(payload))
                        )
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
                        completed = True
                    finally:
                        owner.byte_serving.release(
                            len(payload), completed
                        )
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
                    requested_attempt = urllib.parse.parse_qs(
                        parsed.query
                    ).get("attempt")
                    if requested_attempt is not None:
                        try:
                            requested_attempt = int(
                                requested_attempt[0]
                            )
                        except (TypeError, ValueError, IndexError):
                            self._error(400, "invalid IData attempt")
                            return
                        if requested_attempt != record.attempt:
                            self._error(
                                409,
                                "IData attempt no longer current",
                            )
                            return
                    if record.serialized_bytes is None:
                        self._error(409, "IData is not available")
                        return
                    payload = record.serialized_bytes
                    if not owner.byte_serving.acquire(len(payload)):
                        self._error(503, "byte serving capacity exceeded")
                        return
                    completed = False
                    try:
                        if owner.serving_hook is not None:
                            owner.serving_hook(f"i:{record.data_id}")
                        self.send_response(200)
                        self.send_header(
                            "Content-Type", "application/octet-stream"
                        )
                        self.send_header(
                            "Content-Length", str(len(payload))
                        )
                        self.send_header(
                            "X-DataVine-SHA256", record.content_hash
                        )
                        self.send_header(
                            "X-DataVine-Attempt", str(record.attempt)
                        )
                        self.end_headers()
                        self.wfile.write(payload)
                        completed = True
                    finally:
                        owner.byte_serving.release(
                            len(payload), completed
                        )
                    return
                replica_prefix = f"{API_PREFIX}/replicas/"
                if (
                    parsed.path.startswith(replica_prefix)
                    and parsed.path.endswith("/sources")
                ):
                    token = parsed.path[
                        len(replica_prefix):-len("/sources")
                    ]
                    pieces = token.strip("/").split("/")
                    if (
                        len(pieces) != 2
                        or pieces[0] not in ("e", "i")
                        or not pieces[1].isdigit()
                    ):
                        self._error(400, "invalid qualified DataID")
                        return
                    data_key = f"{pieces[0]}:{int(pieces[1])}"
                    try:
                        sources = owner.state.replicas.candidates(data_key)
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "data_id": data_key,
                            "sources": [
                                source.source_dict()
                                for source in sources
                            ],
                        },
                    )
                    return
                self._error(404, "not found")

            def do_POST(self):
                if not self._authorized():
                    self._error(403, "forbidden")
                    return
                if self.path == f"{API_PREFIX}/workers/join":
                    try:
                        request = self._read_json()
                        worker = owner.state.join_worker(
                            request["worker_id"], request["epoch"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, dataclasses.asdict(worker))
                    return
                if self.path == f"{API_PREFIX}/workers/claim":
                    try:
                        request = self._read_json()
                        worker = owner.state.claim_worker(
                            request["worker_id"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, dataclasses.asdict(worker))
                    return
                if self.path == f"{API_PREFIX}/workers/disconnect":
                    try:
                        request = self._read_json()
                        worker = owner.state.disconnect_worker(
                            request["worker_id"], request["epoch"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, dataclasses.asdict(worker))
                    return
                if self.path == f"{API_PREFIX}/workers/reconcile":
                    try:
                        request = self._read_json()
                        disconnected = owner.state.reconcile_workers(
                            request["active_worker_ids"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "disconnected": [
                                dataclasses.asdict(worker)
                                for worker in disconnected
                            ]
                        },
                    )
                    return
                if self.path in (
                    f"{API_PREFIX}/replicas/prepare",
                    f"{API_PREFIX}/replicas/report",
                ):
                    try:
                        request = self._read_json()
                        arguments = (
                            request["data_id"],
                            request["replica_id"],
                            request["attempt"],
                            request["tier"],
                            request["content_hash"],
                            request["size"],
                            request["worker_id"],
                            request["worker_epoch"],
                        )
                        if self.path.endswith("/prepare"):
                            replica = owner.state.prepare_worker_replica(
                                *arguments
                            )
                        else:
                            replica = owner.state.report_worker_replica(
                                *arguments
                            )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, replica.source_dict())
                    return
                if self.path == f"{API_PREFIX}/replicas/commit":
                    try:
                        request = self._read_json()
                        replica = owner.state.commit_worker_replica(
                            request["data_id"],
                            request["replica_id"],
                            request["generation"],
                            request["attempt"],
                            request["content_hash"],
                            request["size"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, replica.source_dict())
                    return
                if self.path == f"{API_PREFIX}/replicas/invalidate":
                    try:
                        request = self._read_json()
                        replica = owner.state.invalidate_worker_replica(
                            request["data_id"],
                            request["replica_id"],
                            request["generation"],
                            request["worker_id"],
                            request["worker_epoch"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, replica.source_dict())
                    return
                if (
                    self.path
                    == f"{API_PREFIX}/replicas/invalidate-observed"
                ):
                    try:
                        request = self._read_json()
                        replica = (
                            owner.state
                            .invalidate_observed_worker_replica(
                                request["data_id"],
                                request["replica_id"],
                                request["attempt"],
                                request["content_hash"],
                                request["size"],
                                request["worker_id"],
                                request["worker_epoch"],
                            )
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, replica.source_dict())
                    return
                if self.path == f"{API_PREFIX}/replicas/acquire":
                    try:
                        request = self._read_json()
                        lease = owner.state.acquire_replica(
                            request["data_id"],
                            request["replica_id"],
                            request["generation"],
                            request["destination_worker_id"],
                            request["destination_worker_epoch"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, dataclasses.asdict(lease))
                    return
                if (
                    self.path
                    == f"{API_PREFIX}/replicas/acquire-observed"
                ):
                    try:
                        request = self._read_json()
                        lease = owner.state.acquire_observed_transfer(
                            request["data_id"],
                            request["source_worker_id"],
                            request["destination_worker_id"],
                            request["transfer_id"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, dataclasses.asdict(lease))
                    return
                if self.path == f"{API_PREFIX}/replicas/release":
                    try:
                        request = self._read_json()
                        lease = owner.state.release_replica(
                            request["lease_id"], request["success"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, dataclasses.asdict(lease))
                    return
                if self.path == f"{API_PREFIX}/replicas/pruned":
                    try:
                        request = self._read_json()
                        result = owner.state.confirm_worker_pruned(
                            request["data_id"],
                            request["replica_id"],
                            request["generation"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, result)
                    return
                if self.path == f"{API_PREFIX}/pruning/task-state":
                    try:
                        request = self._read_json()
                        plan = owner.state.set_task_state(
                            request["task_id"], request["state"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, plan)
                    return
                if self.path == f"{API_PREFIX}/pruning/required-output":
                    try:
                        request = self._read_json()
                        plan = owner.state.set_required_output(
                            request["data_id"],
                            request.get("required", True),
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, plan)
                    return
                if self.path == f"{API_PREFIX}/pruning/apply":
                    try:
                        request = self._read_json()
                        result = owner.state.apply_pruning(
                            request["graph_revision"],
                            request["state_revision"],
                            request.get("grace_seconds", 60),
                            request.get("data_ids"),
                            request.get("now"),
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, result)
                    return
                if self.path == f"{API_PREFIX}/pruning/restore":
                    try:
                        request = self._read_json()
                        result = owner.state.restore_quarantined(
                            request["data_id"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, {"restored": result})
                    return
                if self.path == f"{API_PREFIX}/pruning/hard-delete":
                    try:
                        request = self._read_json()
                        result = owner.state.hard_delete_quarantined(
                            request["graph_revision"],
                            request["state_revision"],
                            request.get("now"),
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, result)
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
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/persist")
                ):
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
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/persist/cancel")
                ):
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
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/invalidate")
                ):
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
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/publish")
                ):
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
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/publish-metadata")
                ):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):
                        -len("/publish-metadata")
                    ]
                    try:
                        request = self._read_json()
                        record = owner.state.publish_idata_metadata(
                            int(token),
                            request["attempt"],
                            request["content_hash"],
                            request["size"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "data_id": record.data_id,
                            "content_hash": record.content_hash,
                            "size": record.serialized_size,
                            "attempt": record.attempt,
                            "controller_inline": False,
                        },
                    )
                    return
                if self.path == f"{API_PREFIX}/edata/register-origin":
                    try:
                        request = self._read_json()
                        metadata = SerializationMetadata.from_dict(
                            request["metadata"]
                        )
                        record = owner.state.register_edata_origin(
                            metadata,
                            request["origin_path"],
                            request["content_hash"],
                            request["size"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "data_id": record.data_id,
                            "content_hash": record.content_hash,
                            "size": record.serialized_size,
                            "storage": (
                                "controller-memory"
                                if record.serialized_bytes is not None
                                else "bulk-origin"
                            ),
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
                        "size": record.serialized_size,
                        "storage": (
                            "controller-memory"
                            if record.serialized_bytes is not None
                            else "bulk-origin"
                        ),
                    },
                )

            def _read_json(self):
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > owner.state.max_edata_bytes * 2:
                    raise ValueError("invalid request size")
                return json.loads(self.rfile.read(length))

            def log_message(self, format_string, *args):
                return

        self._server = BoundedThreadingHTTPServer(
            (self.host, self.port),
            Handler,
            self.max_request_concurrency,
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
