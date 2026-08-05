"""Worker-local data stores and persistent-library state."""

import dataclasses
import hashlib
import os
from pathlib import Path
import tempfile
import threading


class WorkerMemoryStore:
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
        self._lock = threading.Lock()

    @staticmethod
    def _value(entry):
        return (entry[3] * (entry[1] + 1), entry[2])

    def _summary(self):
        return (
            self.capacity,
            self.bytes,
            len(self._entries),
            self.hits,
            self.misses,
            self.admissions,
            self.evictions,
        )

    def put(
        self,
        scope,
        data_key,
        content_hash,
        payload,
        capacity,
        score,
        protected,
    ):
        if not isinstance(payload, bytes):
            raise TypeError("worker cache payload must be bytes")
        prefix = (str(scope), str(data_key))
        key = prefix + (str(content_hash), len(payload))
        with self._lock:
            self.capacity = max(0, int(capacity))
            self._clock += 1
            current = self._entries.get(key)
            if current is not None:
                current[2] = self._clock
                current[4] = current[4] or bool(protected)
                return True, *self._summary()
            old_key = self._data_keys.get(prefix)
            old = self._entries.get(old_key)
            old_size = len(old[0]) if old is not None else 0
            candidate = [
                payload,
                0,
                self._clock,
                int(score),
                bool(protected),
            ]
            required = self.bytes - old_size + len(payload)
            selected = []
            if required > self.capacity:
                victims = sorted(
                    (
                        (victim_key, entry)
                        for victim_key, entry in self._entries.items()
                        if victim_key != old_key and not entry[4]
                    ),
                    key=lambda item: self._value(item[1]),
                )
                while required > self.capacity and victims:
                    victim_key, victim = victims.pop(0)
                    if not protected and self._value(
                        candidate
                    ) <= self._value(victim):
                        break
                    selected.append(victim_key)
                    required -= len(victim[0])
            if len(payload) > self.capacity or required > self.capacity:
                return False, *self._summary()
            if old is not None:
                self._entries.pop(old_key, None)
                self.bytes -= old_size
            for victim_key in selected:
                victim = self._entries.pop(victim_key)
                self.bytes -= len(victim[0])
                if self._data_keys.get(victim_key[:2]) == victim_key:
                    self._data_keys.pop(victim_key[:2], None)
                self.evictions += 1
            self._entries[key] = candidate
            self._data_keys[prefix] = key
            self.bytes += len(payload)
            self.admissions += 1
            return True, *self._summary()

    def _hit(self, key):
        entry = self._entries.get(key)
        if entry is None:
            self.misses += 1
            return None
        self._clock += 1
        entry[1] += 1
        entry[2] = self._clock
        self.hits += 1
        return entry[0]

    def find(self, scope, data_key, content_hash, size):
        key = (str(scope), str(data_key), str(content_hash), int(size))
        with self._lock:
            return self._hit(key)

    def local(self, scope, data_key):
        prefix = (str(scope), str(data_key))
        with self._lock:
            return self._hit(self._data_keys.get(prefix))

    def remove(self, scope, data_key, content_hash, size):
        key = (str(scope), str(data_key), str(content_hash), int(size))
        with self._lock:
            entry = self._entries.pop(key, None)
            if entry is None:
                return False
            self.bytes -= len(entry[0])
            if self._data_keys.get(key[:2]) == key:
                self._data_keys.pop(key[:2], None)
            return True

    def snapshot(self):
        with self._lock:
            return {
                "capacity_bytes": self.capacity,
                "bytes": self.bytes,
                "items": len(self._entries),
                "hits": self.hits,
                "misses": self.misses,
                "admissions": self.admissions,
                "evictions": self.evictions,
            }


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
        try:
            with path.open("xb") as stream:
                stream.write(payload)
        except FileExistsError:
            if self._read(path, content_hash, len(payload)) is None:
                path.write_bytes(payload)
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
    cacheable_edata: set = dataclasses.field(default_factory=set)
    replica_reports: dict = dataclasses.field(default_factory=dict)
    data_service: object | None = None
    dram_capacity: int = 0
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
            self.cacheable_edata.clear()
            self.replica_reports.clear()


PROCESS_CACHE = WorkerProcessCache()
