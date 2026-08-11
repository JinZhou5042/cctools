"""Small standard-library client for the public DataVine workflow protocol."""

import itertools
import json
import socket
import struct
import threading
import urllib.parse


_MAGIC = 0x44564331
_VERSION = 1
_MAX_BODY = 64 * 1024 * 1024
_AUTH = 1
_SUBMIT = 20
_APPEND = 21
_SEAL = 22
_DESCRIBE = 23
_WATCH = 24
_CANCEL = 25
_CAPABILITIES = 26
_RESULT = 27
_RESULT_INFO = 28
_FRONTIER = 29


class WorkflowClientError(RuntimeError):
    def __init__(self, status, opcode, body=b""):
        self.status = int(status)
        self.opcode = int(opcode)
        self.code = self.path = self.detail = None
        if len(body) >= 8:
            code, path_size, detail_size = struct.unpack_from("!IHH", body)
            if len(body) == 8 + path_size + detail_size:
                self.code = code
                self.path = body[8:8 + path_size].decode("utf-8")
                self.detail = body[8 + path_size:].decode("utf-8")
        message = f"workflow opcode {opcode} failed with status {status}"
        if self.path:
            message += f" at {self.path}: {self.detail}"
        super().__init__(message)


class WorkflowEventCursorExpired(RuntimeError):
    def __init__(self, requested, oldest):
        self.requested = int(requested)
        self.oldest = int(oldest)
        super().__init__(
            f"workflow event cursor {requested} expired; oldest retained event is {oldest}; "
            "reconstruct from describe/result state"
        )


class WorkflowClient:
    def __init__(self, endpoint, token, timeout=30):
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.scheme != "tcp" or not parsed.hostname or not parsed.port:
            raise ValueError("workflow endpoint must use tcp://host:port")
        self.endpoint = str(endpoint)
        self.host = parsed.hostname
        self.port = parsed.port
        self.token = str(token).encode("utf-8")
        self.timeout = float(timeout)
        self._local = threading.local()
        self._ids = itertools.count(1)
        self._id_lock = threading.Lock()

    @staticmethod
    def _read(stream, size):
        chunks = []
        while size:
            chunk = stream.recv(size)
            if not chunk:
                raise ConnectionError("workflow connection closed")
            chunks.append(chunk)
            size -= len(chunk)
        return b"".join(chunks)

    def _exchange(self, stream, opcode, payload=b""):
        if len(payload) > _MAX_BODY:
            raise ValueError(
                f"workflow RPC payload is {len(payload)} bytes; "
                f"maximum is {_MAX_BODY}; use append_delta batches"
            )
        with self._id_lock:
            request_id = next(self._ids)
        stream.sendall(
            struct.pack(
                "!IHHIQ", _MAGIC, _VERSION, opcode, len(payload), request_id
            )
            + payload
        )
        header = self._read(stream, 24)
        magic, version, response_opcode, status, size, response_id = (
            struct.unpack("!IHHIIQ", header)
        )
        if (
            magic != _MAGIC
            or version != _VERSION
            or response_opcode != opcode
            or response_id != request_id
            or size > _MAX_BODY
        ):
            raise RuntimeError("invalid workflow response header")
        body = self._read(stream, size) if size else b""
        if status:
            raise WorkflowClientError(status, opcode, body)
        return body

    def _connect(self):
        stream = socket.create_connection((self.host, self.port), self.timeout)
        stream.settimeout(self.timeout)
        stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._local.stream = stream
        self._exchange(stream, _AUTH, self.token)
        return stream

    def close(self):
        stream = getattr(self._local, "stream", None)
        if stream is not None:
            stream.close()
            self._local.stream = None

    def request(self, opcode, payload=b""):
        stream = getattr(self._local, "stream", None) or self._connect()
        try:
            return self._exchange(stream, opcode, payload)
        except WorkflowClientError:
            raise
        except (ConnectionError, OSError, RuntimeError):
            self.close()
            return self._exchange(self._connect(), opcode, payload)

    @staticmethod
    def _info(body):
        if len(body) < 104 or body[:4] != b"DWI1":
            raise RuntimeError("invalid workflow info")
        values = struct.unpack_from("!IQQQQQQIH", body, 4)
        state, generation, event_id, tasks, data, edges, requested, streaming, id_size = values
        if len(body) != 104 + id_size:
            raise RuntimeError("truncated workflow info")
        states = {
            1: "open",
            2: "sealed",
            3: "cancelled",
            4: "running",
            5: "completed",
            6: "failed",
            7: "running_open",
            8: "open_quiescent",
        }
        return {
            "workflow_id": body[104:].decode("utf-8"),
            "digest": body[64:104].decode("ascii"),
            "generation": generation,
            "event_id": event_id,
            "state": states.get(state, f"unknown:{state}"),
            "tasks": tasks,
            "data": data,
            "edges": edges,
            "requested_outputs": requested,
            "streaming": bool(streaming),
        }

    @staticmethod
    def _identifier(workflow_id):
        identifier = str(workflow_id).encode("utf-8")
        if not identifier or len(identifier) > 256:
            raise ValueError("invalid workflow_id")
        return identifier

    @staticmethod
    def _document(document):
        """Encode only; the native service is the single validation owner."""

        if isinstance(document, bytes):
            return document
        if isinstance(document, str):
            return document.encode("utf-8")
        return json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

    def _payload(self, workflow_id, format, *values, suffix=b""):
        identifier = self._identifier(workflow_id)
        return struct.pack(format, len(identifier), *values) + identifier + suffix

    def workflow_capabilities(self):
        return json.loads(self.request(_CAPABILITIES))

    def submit_workflow(self, document):
        return self._info(self.request(_SUBMIT, self._document(document)))

    def append_workflow(self, workflow_id, expected_generation, document):
        return self._info(
            self.request(
                _APPEND,
                self._payload(
                    workflow_id,
                    "!H2xQ",
                    int(expected_generation),
                    suffix=self._document(document),
                ),
            )
        )

    def seal_workflow(self, workflow_id, expected_generation):
        return self._info(
            self.request(
                _SEAL,
                self._payload(workflow_id, "!H2xQ", int(expected_generation)),
            )
        )

    def describe_workflow(self, workflow_id):
        return self._info(self.request(_DESCRIBE, self._payload(workflow_id, "!H")))

    def workflow_frontier(self, workflow_id):
        body = self.request(_FRONTIER, self._payload(workflow_id, "!H"))
        if len(body) != 32:
            raise RuntimeError("invalid workflow frontier")
        task_id, data_id, maximum_tasks, maximum_edges = struct.unpack(
            "!QQQQ", body
        )
        return {
            "maximum_task_id": task_id,
            "maximum_data_id": data_id,
            "maximum_tasks": maximum_tasks,
            "maximum_edges": maximum_edges,
        }

    def cancel_workflow(self, workflow_id):
        return self._info(self.request(_CANCEL, self._payload(workflow_id, "!H")))

    def watch_workflow(self, workflow_id, after_event_id=0, limit=64):
        body = self.request(
            _WATCH,
            self._payload(
                workflow_id, "!HHQ", int(limit), int(after_event_id)
            ),
        )
        if len(body) < 4 or len(body) != 4 + 76 * struct.unpack_from("!I", body)[0]:
            raise RuntimeError("invalid workflow event page")
        names = {
            1: "accepted", 2: "appended", 3: "sealed", 4: "cancelled",
            5: "started", 6: "completed", 7: "failed", 8: "recovered",
            9: "task_submitted", 10: "task_completed", 11: "task_retry",
            12: "task_failed",
            13: "quiescent",
            14: "resumed",
        }
        events = []
        for index in range(struct.unpack_from("!I", body)[0]):
            offset = 4 + index * 76
            event_type, event_id, generation = struct.unpack_from("!IQQ", body, offset)
            events.append({
                "event_id": event_id,
                "generation": generation,
                "type": names.get(event_type, f"unknown:{event_type}"),
                "digest": body[offset + 20:offset + 60].decode("ascii"),
                "task_id": struct.unpack_from("!Q", body, offset + 60)[0],
                "attempt": struct.unpack_from("!I", body, offset + 68)[0],
                "result": struct.unpack_from("!i", body, offset + 72)[0],
            })
        if events and events[0]["event_id"] > int(after_event_id) + 1:
            raise WorkflowEventCursorExpired(
                int(after_event_id), events[0]["event_id"]
            )
        return events

    def fetch_workflow_result(self, workflow_id, data_id):
        return self.request(
            _RESULT, self._payload(workflow_id, "!H2xQ", int(data_id))
        )

    def workflow_result_info(self, workflow_id, data_id):
        return json.loads(
            self.request(
                _RESULT_INFO,
                self._payload(workflow_id, "!H2xQ", int(data_id)),
            )
        )
