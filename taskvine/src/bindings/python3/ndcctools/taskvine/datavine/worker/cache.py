"""Process-local state owned by a persistent DataVine worker library."""

import dataclasses
import hashlib
import os
from pathlib import Path
import tempfile
import threading


class SerializedDataCache:
    def __init__(self):
        self.capacity = 0
        self.bytes = 0
        self.hits = 0
        self.misses = 0
        self.admissions = 0
        self.evictions = 0
        self._clock = 0
        self._entries = {}
        self._data_keys = {}

    def configure(self, capacity):
        capacity = int(capacity)
        if capacity < 0:
            raise ValueError("worker DRAM cache capacity is negative")
        self.capacity = capacity
        self._evict_to_capacity()

    def get(self, key):
        entry = self._entries.get(key)
        if entry is None:
            self.misses += 1
            return None
        self._clock += 1
        entry[1] += 1
        entry[2] = self._clock
        self.hits += 1
        return entry[0]

    def put_data(
        self,
        controller,
        token,
        data_key,
        content_hash,
        payload,
        score=None,
    ):
        prefix = (str(controller), str(token), str(data_key))
        key = prefix + (str(content_hash), len(payload))
        existing = self._data_keys.pop(prefix, None)
        if existing is not None and existing != key:
            self._remove(existing)
        admitted = self.put(
            key,
            payload,
            score,
        )
        if admitted:
            self._data_keys[prefix] = key
        return admitted

    def get_local_data(self, controller, token, data_key):
        prefix = (str(controller), str(token), str(data_key))
        key = self._data_keys.get(prefix)
        return self.get(key) if key is not None else self._miss()

    def _miss(self):
        self.misses += 1
        return None

    def _remove(self, key):
        entry = self._entries.pop(key, None)
        if entry is None:
            return None
        self.bytes -= len(entry[0])
        if (
            isinstance(key, tuple)
            and len(key) >= 3
            and self._data_keys.get(key[:3]) == key
        ):
            self._data_keys.pop(key[:3], None)
        return entry

    @staticmethod
    def _value(entry):
        _, hits, touched, score = entry
        return (int(score) * (hits + 1), touched)

    def put(self, key, payload, score=None):
        if not isinstance(payload, bytes):
            raise TypeError("worker cache payload must be bytes")
        if not self.capacity or len(payload) > self.capacity:
            return False
        old = self._remove(key)
        if old is not None and isinstance(key, tuple) and len(key) >= 3:
            self._data_keys[key[:3]] = key
        self._clock += 1
        candidate = [
            payload,
            0,
            self._clock,
            int(score) if score is not None else 1_000_000 // max(1, len(payload)),
        ]
        while self._entries and self.bytes + len(payload) > self.capacity:
            victim_key, victim = min(
                self._entries.items(), key=lambda item: self._value(item[1])
            )
            if self._value(candidate) <= self._value(victim):
                if old is not None:
                    self._entries[key] = old
                    self.bytes += len(old[0])
                return False
            self._remove(victim_key)
            self.evictions += 1
        self._entries[key] = candidate
        self.bytes += len(payload)
        self.admissions += 1
        return True

    def _evict_to_capacity(self):
        while self._entries and self.bytes > self.capacity:
            key, entry = min(
                self._entries.items(), key=lambda item: self._value(item[1])
            )
            self._remove(key)
            self.evictions += 1

    def snapshot(self):
        return {
            "capacity_bytes": self.capacity,
            "bytes": self.bytes,
            "items": len(self._entries),
            "hits": self.hits,
            "misses": self.misses,
            "admissions": self.admissions,
            "evictions": self.evictions,
        }

    def clear(self):
        self.bytes = 0
        self._entries.clear()
        self._data_keys.clear()


class WorkerDiskStore:
    def __init__(self):
        self.root = None
        self._data_paths = {}

    def configure(self, worker_id):
        worker = hashlib.sha256(str(worker_id).encode()).hexdigest()[:16]
        worker_temp = os.environ.get("WORKER_TMPDIR")
        root = Path(worker_temp or tempfile.gettempdir()) / (
            f"datavine-worker-{os.getuid()}-{worker}"
        )
        if root == self.root:
            return
        root.mkdir(mode=0o700, exist_ok=True)
        self.root = root
        self._data_paths.clear()
        for path in root.iterdir():
            fields = path.name.rsplit("-", 2)
            if len(fields) != 3 or len(fields[1]) != 64:
                continue
            prefix = fields[0].split("-", 2)
            if len(prefix) != 3 or prefix[1] not in ("e", "i"):
                continue
            try:
                data_key = f"{prefix[1]}:{int(prefix[2])}"
                int(fields[2])
            except ValueError:
                continue
            self._data_paths[(prefix[0], data_key)] = path

    @staticmethod
    def scope(controller, token):
        return hashlib.sha256(
            f"{controller}\0{token}".encode()
        ).hexdigest()[:16]

    def _path(self, scope, data_key, content_hash, size):
        kind, token_id = str(data_key).split(":", 1)
        name = (
            f"{scope}-{kind}-{int(token_id)}-"
            f"{content_hash}-{int(size)}"
        )
        return self.root / name

    @staticmethod
    def _read(path, content_hash=None, size=None):
        try:
            payload = path.read_bytes()
        except FileNotFoundError:
            return None
        if size is not None and len(payload) != int(size):
            return None
        if content_hash is not None and hashlib.sha256(payload).hexdigest() != content_hash:
            return None
        return payload

    def put_data(self, controller, token, data_key, content_hash, payload):
        scope = self.scope(controller, token)
        path = self._path(
            scope, data_key, content_hash, len(payload)
        )
        temporary = path.with_name(
            f".{path.name}.{os.getpid()}.{threading.get_ident()}"
        )
        temporary.write_bytes(payload)
        os.replace(temporary, path)
        old = self._data_paths.get((scope, str(data_key)))
        self._data_paths[(scope, str(data_key))] = path
        if old is not None and old != path:
            try:
                old.unlink()
            except FileNotFoundError:
                pass

    def find_data(self, scope, data_key, content_hash, size):
        return self._read(
            self._path(scope, data_key, content_hash, size),
            content_hash,
            size,
        )

    def get_local_data(self, controller, token, data_key):
        key = (self.scope(controller, token), str(data_key))
        path = self._data_paths.get(key)
        if path is None:
            return None
        fields = path.name.rsplit("-", 2)
        payload = self._read(path, fields[1], int(fields[2]))
        if payload is None:
            self._data_paths.pop(key, None)
        return payload

    def remove_data(self, scope, data_key, content_hash, size):
        kind, token_id = str(data_key).split(":", 1)
        path = self._path(
            scope, f"{kind}:{int(token_id)}", content_hash, size
        )
        try:
            path.unlink()
        except FileNotFoundError:
            return False
        key = (str(scope), f"{kind}:{int(token_id)}")
        if self._data_paths.get(key) == path:
            self._data_paths.pop(key, None)
        return True


@dataclasses.dataclass
class WorkerProcessCache:
    clients: dict = dataclasses.field(default_factory=dict)
    runtimes: dict = dataclasses.field(default_factory=dict)
    output_publishers: dict = dataclasses.field(default_factory=dict)
    task_records: dict = dataclasses.field(default_factory=dict)
    functions: dict = dataclasses.field(default_factory=dict)
    replica_reports: dict = dataclasses.field(default_factory=dict)
    data_service: object | None = None
    data: SerializedDataCache = dataclasses.field(
        default_factory=SerializedDataCache
    )
    disk: WorkerDiskStore = dataclasses.field(default_factory=WorkerDiskStore)
    lock: threading.RLock = dataclasses.field(
        default_factory=threading.RLock
    )
    context_lock: threading.RLock = dataclasses.field(
        default_factory=threading.RLock
    )
    fetch_locks: tuple = dataclasses.field(
        default_factory=lambda: tuple(
            threading.RLock() for _ in range(64)
        )
    )

    def fetch_lock(self, key):
        return self.fetch_locks[hash(key) % len(self.fetch_locks)]

    def clear(self):
        with self.context_lock:
            self.output_publishers.clear()
            self.runtimes.clear()
            self.clients.clear()
            self.task_records.clear()
            self.functions.clear()
            self.replica_reports.clear()
        with self.lock:
            self.data.clear()


PROCESS_CACHE = WorkerProcessCache()
