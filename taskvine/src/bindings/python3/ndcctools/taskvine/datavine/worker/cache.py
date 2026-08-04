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

    def get_data(self, controller, token, data_key, content_hash, size):
        return self.get(
            (
                str(controller),
                str(token),
                str(data_key),
                str(content_hash),
                int(size),
            )
        )

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
        for existing in tuple(self._entries):
            if existing[:3] == prefix and existing != key:
                entry = self._entries.pop(existing)
                self.bytes -= len(entry[0])
        return self.put(
            key,
            payload,
            score,
        )

    def get_local_data(self, controller, token, data_key):
        prefix = (str(controller), str(token), str(data_key))
        for key in tuple(self._entries):
            if key[:3] == prefix:
                return self.get(key)
        self.misses += 1
        return None

    def find_data(self, data_key, content_hash, size):
        suffix = (str(data_key), str(content_hash), int(size))
        for key in tuple(self._entries):
            if key[2:] == suffix:
                return self.get(key)
        self.misses += 1
        return None

    def remove_data(self, data_key, content_hash, size):
        suffix = (str(data_key), str(content_hash), int(size))
        removed = False
        for key in tuple(self._entries):
            if key[2:] != suffix:
                continue
            entry = self._entries.pop(key)
            self.bytes -= len(entry[0])
            removed = True
        return removed

    @staticmethod
    def _value(entry):
        _, hits, touched, score = entry
        return (int(score) * (hits + 1), touched)

    def put(self, key, payload, score=None):
        if not isinstance(payload, bytes):
            raise TypeError("worker cache payload must be bytes")
        if not self.capacity or len(payload) > self.capacity:
            return False
        old = self._entries.pop(key, None)
        if old is not None:
            self.bytes -= len(old[0])
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
            self._entries.pop(victim_key)
            self.bytes -= len(victim[0])
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
            self._entries.pop(key)
            self.bytes -= len(entry[0])
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


class WorkerDiskStore:
    def __init__(self):
        self.root = None

    def configure(self, worker_id):
        worker = hashlib.sha256(str(worker_id).encode()).hexdigest()[:16]
        worker_temp = os.environ.get("WORKER_TMPDIR")
        self.root = Path(worker_temp or tempfile.gettempdir()) / (
            f"datavine-worker-{os.getuid()}-{worker}"
        )
        self.root.mkdir(mode=0o700, exist_ok=True)

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
        path = self._path(
            self.scope(controller, token), data_key, content_hash, len(payload)
        )
        temporary = path.with_name(
            f".{path.name}.{os.getpid()}.{threading.get_ident()}"
        )
        temporary.write_bytes(payload)
        os.replace(temporary, path)

    def get_data(self, controller, token, data_key, content_hash, size):
        return self.find_data(
            self.scope(controller, token), data_key, content_hash, size
        )

    def find_data(self, scope, data_key, content_hash, size):
        return self._read(
            self._path(scope, data_key, content_hash, size),
            content_hash,
            size,
        )

    def get_local_data(self, controller, token, data_key):
        kind, token_id = str(data_key).split(":", 1)
        prefix = (
            f"{self.scope(controller, token)}-{kind}-{int(token_id)}-"
        )
        for path in self.root.glob(f"{prefix}*"):
            fields = path.name.rsplit("-", 2)
            if len(fields) != 3:
                continue
            try:
                size = int(fields[2])
            except ValueError:
                continue
            payload = self._read(path, fields[1], size)
            if payload is not None:
                return payload
        return None

    def remove_data(self, scope, data_key, content_hash, size):
        kind, token_id = str(data_key).split(":", 1)
        path = self._path(
            scope, f"{kind}:{int(token_id)}", content_hash, size
        )
        try:
            path.unlink()
        except FileNotFoundError:
            return False
        return True


@dataclasses.dataclass
class WorkerProcessCache:
    clients: dict = dataclasses.field(default_factory=dict)
    output_publishers: dict = dataclasses.field(default_factory=dict)
    source_resolvers: dict = dataclasses.field(default_factory=dict)
    worker_claims: dict = dataclasses.field(default_factory=dict)
    task_records: dict = dataclasses.field(default_factory=dict)
    functions: dict = dataclasses.field(default_factory=dict)
    edata_metadata: dict = dataclasses.field(default_factory=dict)
    replica_reports: dict = dataclasses.field(default_factory=dict)
    data_service: object | None = None
    data: SerializedDataCache = dataclasses.field(
        default_factory=SerializedDataCache
    )
    disk: WorkerDiskStore = dataclasses.field(default_factory=WorkerDiskStore)
    lock: threading.RLock = dataclasses.field(
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
        with self.lock:
            publishers = tuple(self.output_publishers.values())
            publishers += tuple(self.source_resolvers.values())
            self.output_publishers.clear()
            self.source_resolvers.clear()
            self.clients.clear()
            self.worker_claims.clear()
            self.task_records.clear()
            self.functions.clear()
            self.edata_metadata.clear()
            self.replica_reports.clear()
            self.data.clear()
        for publisher in publishers:
            publisher.close()


PROCESS_CACHE = WorkerProcessCache()
