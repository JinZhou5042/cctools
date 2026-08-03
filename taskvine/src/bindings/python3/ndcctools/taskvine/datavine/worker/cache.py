"""Process-local state owned by a persistent DataVine worker library."""

import dataclasses
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
        return self.put(
            (
                str(controller),
                str(token),
                str(data_key),
                str(content_hash),
                len(payload),
            ),
            payload,
            score,
        )

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


@dataclasses.dataclass
class WorkerProcessCache:
    clients: dict = dataclasses.field(default_factory=dict)
    worker_claims: dict = dataclasses.field(default_factory=dict)
    task_records: dict = dataclasses.field(default_factory=dict)
    edata_metadata: dict = dataclasses.field(default_factory=dict)
    replica_reports: dict = dataclasses.field(default_factory=dict)
    data_service: object | None = None
    data: SerializedDataCache = dataclasses.field(
        default_factory=SerializedDataCache
    )
    lock: threading.RLock = dataclasses.field(
        default_factory=threading.RLock
    )

    def clear(self):
        with self.lock:
            self.clients.clear()
            self.worker_claims.clear()
            self.task_records.clear()
            self.edata_metadata.clear()
            self.replica_reports.clear()
            self.data.clear()


PROCESS_CACHE = WorkerProcessCache()
