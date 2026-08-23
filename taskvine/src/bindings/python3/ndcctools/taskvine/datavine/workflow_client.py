"""Small standard-library client for the public DataVine workflow protocol."""

import concurrent.futures
import itertools
import hashlib
import json
import os
import pathlib
import socket
import struct
import threading
import time
import urllib.parse
from queue import Empty, Queue


_MAGIC = 0x44564331
_VERSION = 1
_MAX_BODY = 64 * 1024 * 1024
_MAX_OBJECT = _MAX_BODY - 64
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
_OBJECT_PUT = 30
_OBJECT_GET = 31
_OBJECT_PATH = 32
_RESULT_PATH = 34
_RESULT_DESCRIPTORS = 36
_WAIT_TERMINAL = 37
_OBJECT_PUT_WORKERS = 16

_OPCODE_NAMES = {
    _AUTH: "auth",
    _SUBMIT: "workflow_submit",
    _APPEND: "workflow_append",
    _SEAL: "workflow_seal",
    _DESCRIBE: "workflow_describe",
    _WATCH: "workflow_watch",
    _CANCEL: "workflow_cancel",
    _CAPABILITIES: "workflow_capabilities",
    _RESULT: "workflow_fetch_result",
    _RESULT_INFO: "workflow_result_info",
    _FRONTIER: "workflow_frontier",
    _OBJECT_PUT: "object_put",
    _OBJECT_GET: "object_get",
    _OBJECT_PATH: "object_path",
    _RESULT_PATH: "workflow_result_path",
    _RESULT_DESCRIPTORS: "workflow_result_descriptors",
    _WAIT_TERMINAL: "workflow_wait_terminal",
}


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
        self._object_root = None
        self._object_root_lock = threading.Lock()
        self._capabilities = None
        self._capabilities_lock = threading.Lock()
        self._rpc_profile_lock = threading.Lock()
        self._rpc_requests = 0
        self._rpc_connections = 0
        self._rpc_by_opcode = {}

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
        with self._rpc_profile_lock:
            self._rpc_requests += 1
            self._rpc_by_opcode[opcode] = self._rpc_by_opcode.get(opcode, 0) + 1
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
        with self._rpc_profile_lock:
            self._rpc_connections += 1
        self._exchange(stream, _AUTH, self.token)
        return stream

    def rpc_profile(self, reset=False):
        """Return actual protocol attempts made by this client."""

        with self._rpc_profile_lock:
            profile = {
                "connections": self._rpc_connections,
                "requests": self._rpc_requests,
                "by_opcode": {
                    _OPCODE_NAMES.get(opcode, str(opcode)): count
                    for opcode, count in sorted(self._rpc_by_opcode.items())
                },
            }
            if reset:
                self._rpc_requests = 0
                self._rpc_connections = 0
                self._rpc_by_opcode.clear()
        return profile

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
            9: "staged",
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
        with self._capabilities_lock:
            if self._capabilities is None:
                capabilities = json.loads(self.request(_CAPABILITIES))
                object_root = pathlib.Path(capabilities.get("object_root", ""))
                if not object_root.is_absolute():
                    raise RuntimeError("runtime advertised an unsafe object root")
                with self._object_root_lock:
                    self._object_root = object_root
                self._capabilities = capabilities
            return self._capabilities

    def put_object(self, payload, sha256=None):
        """Atomically install one immutable object directly on SharedFS."""

        payload = bytes(payload)
        if len(payload) > _MAX_OBJECT:
            raise ValueError(
                f"content object is {len(payload)} bytes; the single-file v1 "
                f"limit is {_MAX_OBJECT}; chunked objects are not yet supported"
            )
        hash_started = time.perf_counter_ns()
        digest = hashlib.sha256(payload).hexdigest() if sha256 is None else str(sha256)
        hash_nanoseconds = (
            time.perf_counter_ns() - hash_started if sha256 is None else 0
        )
        if len(digest) != 64 or any(
            character not in "0123456789abcdef" for character in digest
        ):
            raise ValueError("object sha256 must be lowercase hexadecimal")
        encoded_digest = digest.encode("ascii")
        path, rpc_nanoseconds, path_sharedfs_nanoseconds = (
            self._shared_object_path(digest, encoded_digest)
        )
        deduplicated = path.exists()
        sharedfs_started = time.perf_counter_ns()
        if not deduplicated:
            temporary = path.with_name(
                f"{path.name}.part-{os.getpid()}-{threading.get_ident()}"
            )
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                try:
                    os.link(temporary, path)
                except FileExistsError:
                    deduplicated = True
                directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass
        sharedfs_nanoseconds = (
            path_sharedfs_nanoseconds
            + time.perf_counter_ns() - sharedfs_started
        )
        try:
            status = path.stat()
        except OSError as error:
            raise RuntimeError("object store installation disappeared") from error
        if not path.is_file() or status.st_size != len(payload):
            raise RuntimeError("object store installed the wrong size or type")
        return {
            "sha256": digest,
            "size": len(payload),
            "deduplicated": deduplicated,
            "hash_nanoseconds": hash_nanoseconds,
            "rpc_nanoseconds": rpc_nanoseconds,
            "sharedfs_nanoseconds": sharedfs_nanoseconds,
        }

    def _shared_object_path(self, digest, encoded_digest):
        """Discover the immutable store root once, then derive sharded paths."""

        rpc_nanoseconds = 0
        root = self._object_root
        if root is None:
            with self._object_root_lock:
                root = self._object_root
                if root is None:
                    started = time.perf_counter_ns()
                    path_body = self.request(_OBJECT_PATH, encoded_digest)
                    rpc_nanoseconds = time.perf_counter_ns() - started
                    try:
                        path = pathlib.Path(path_body.decode("utf-8"))
                    except UnicodeDecodeError as error:
                        raise RuntimeError("invalid SharedFS object path") from error
                    if (
                        not path.is_absolute()
                        or path.name != digest
                        or path.parent.name != digest[2:4]
                        or path.parent.parent.name != digest[:2]
                    ):
                        raise RuntimeError("unsafe SharedFS object path")
                    root = path.parents[2]
                    self._object_root = root
        path = root / digest[:2] / digest[2:4] / digest
        started = time.perf_counter_ns()
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        sharedfs_nanoseconds = time.perf_counter_ns() - started
        return path, rpc_nanoseconds, sharedfs_nanoseconds

    def put_objects(self, objects, workers=_OBJECT_PUT_WORKERS):
        """Persist distinct pre-serialized objects with bounded I/O concurrency."""

        items = tuple((bytes(payload), str(digest)) for payload, digest in objects)
        if not items:
            return []
        parallelism = min(max(1, int(workers)), len(items))
        if parallelism == 1:
            return [self.put_object(payload, sha256=digest)
                    for payload, digest in items]

        def put(item):
            payload, digest = item
            return self.put_object(payload, sha256=digest)

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=parallelism,
                thread_name_prefix="datavine-object-put") as executor:
            return list(executor.map(put, items))

    def get_object(self, sha256):
        digest = str(sha256).encode("ascii")
        if len(digest) != 64:
            raise ValueError("object sha256 must contain 64 hexadecimal characters")
        path = pathlib.Path(self.request(_OBJECT_PATH, digest).decode("utf-8"))
        if not path.is_absolute() or path.name.encode("ascii") != digest:
            raise RuntimeError("unsafe SharedFS object path")
        payload = path.read_bytes()
        if hashlib.sha256(payload).hexdigest().encode("ascii") != digest:
            raise RuntimeError("object store returned corrupt content")
        return payload

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

    def wait_workflow(self, workflow_id, timeout=None):
        """Wait without polling until a workflow reaches a terminal state."""

        stream = getattr(self._local, "stream", None) or self._connect()
        previous_timeout = stream.gettimeout()
        stream.settimeout(self.timeout if timeout is None else float(timeout))
        try:
            body = self._exchange(
                stream, _WAIT_TERMINAL, self._payload(workflow_id, "!H")
            )
        except socket.timeout as error:
            self.close()
            raise TimeoutError(
                f"workflow {workflow_id} did not become terminal before timeout"
            ) from error
        except WorkflowClientError:
            raise
        except (ConnectionError, OSError, RuntimeError):
            self.close()
            raise
        finally:
            if getattr(self._local, "stream", None) is stream:
                stream.settimeout(previous_timeout)
        return self._info(body)

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
        descriptor = self.workflow_result_descriptors(
            workflow_id, (data_id,)
        )[0]
        return self._read_result(descriptor)

    @staticmethod
    def _read_result(descriptor):
        path = descriptor["path"]
        payload = path.read_bytes()
        if (
            len(payload) != descriptor["size"]
            or hashlib.sha256(payload).hexdigest() != descriptor["sha256"]
        ):
            raise RuntimeError("SharedFS result failed size/digest validation")
        return payload

    def fetch_workflow_results(self, workflow_id, data_ids, concurrency=8):
        """Fetch independent results concurrently while preserving input order."""

        identifiers = tuple(int(data_id) for data_id in data_ids)
        if not identifiers:
            return []
        descriptors = self.workflow_result_descriptors(
            workflow_id, identifiers
        )
        workers = min(len(identifiers), max(1, int(concurrency)))
        if workers == 1:
            return [self._read_result(item) for item in descriptors]
        pending = Queue()
        for index, descriptor in enumerate(descriptors):
            pending.put((index, descriptor))
        results = [None] * len(identifiers)
        failures = Queue()

        def fetcher():
            try:
                while True:
                    try:
                        index, descriptor = pending.get_nowait()
                    except Empty:
                        break
                    try:
                        results[index] = self._read_result(descriptor)
                    finally:
                        pending.task_done()
            except BaseException as error:
                failures.put(error)

        threads = [threading.Thread(target=fetcher) for _ in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if not failures.empty():
            raise failures.get()
        return results

    def workflow_result_descriptors(self, workflow_id, data_ids):
        identifiers = tuple(int(data_id) for data_id in data_ids)
        if not identifiers or any(data_id < 1 for data_id in identifiers):
            raise ValueError("result descriptors require positive DataIDs")
        identifier = self._identifier(workflow_id)
        payload = (
            struct.pack("!H2xI", len(identifier), len(identifiers))
            + identifier
            + b"".join(struct.pack("!Q", data_id) for data_id in identifiers)
        )
        body = self.request(_RESULT_DESCRIPTORS, payload)
        if len(body) < 8 or body[:4] != b"DVR1":
            raise RuntimeError("invalid result descriptor response")
        count = struct.unpack_from("!I", body, 4)[0]
        if count != len(identifiers):
            raise RuntimeError("result descriptor count mismatch")
        descriptors = []
        offset = 8
        for expected in identifiers:
            if len(body) - offset < 88:
                raise RuntimeError("truncated result descriptor")
            data_id, size, attempt, path_size = struct.unpack_from(
                "!QQIH", body, offset
            )
            digest = body[offset + 24:offset + 88].decode("ascii")
            offset += 88
            if len(body) - offset < path_size:
                raise RuntimeError("truncated result descriptor path")
            try:
                path = pathlib.Path(
                    body[offset:offset + path_size].decode("utf-8")
                )
            except UnicodeDecodeError as error:
                raise RuntimeError("invalid result descriptor path") from error
            offset += path_size
            if (
                data_id != expected or attempt < 1
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
                or not path.is_absolute()
                or path.name != f"{data_id}.{attempt}.data"
            ):
                raise RuntimeError("unsafe result descriptor")
            descriptors.append({
                "data_id": data_id,
                "size": size,
                "attempt": attempt,
                "sha256": digest,
                "path": path,
            })
        if offset != len(body):
            raise RuntimeError("result descriptor response has trailing data")
        return descriptors

    def workflow_result_info(self, workflow_id, data_id):
        return json.loads(
            self.request(
                _RESULT_INFO,
                self._payload(workflow_id, "!H2xQ", int(data_id)),
            )
        )
