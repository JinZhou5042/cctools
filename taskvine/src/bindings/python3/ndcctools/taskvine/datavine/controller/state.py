"""Thread-safe Controller-owned logical and serialized state."""

import dataclasses
import hashlib
import threading

from ..models import EDataRecord, IDataRecord, SerializationMetadata, TaskRecord


class ControllerState:
    def __init__(self, max_edata_bytes=256 * 1024 * 1024):
        if max_edata_bytes <= 0:
            raise ValueError("max_edata_bytes must be positive")
        self.max_edata_bytes = int(max_edata_bytes)
        self._lock = threading.RLock()
        self._next_edata_id = 1
        self._edata = {}
        self._buckets = {}
        self._edata_bytes = 0
        self._registrations = 0
        self._edata_fetches = {}
        self._next_idata_id = 1
        self._idata = {}
        self._tasks = {}
        self._publications = 0
        self._persistence = None
        self._persistence_failures = {}
        self._persistence_active = 0
        self._persistence_max_active = 0
        self._persistence_requests = 0

    def configure_persistence(
        self, root, workers=1, fail_first=False
    ):
        from ..persistence.manager import PersistenceManager

        with self._lock:
            if self._persistence is not None:
                raise RuntimeError("persistence already configured")
            self._persistence = PersistenceManager(
                root,
                self._persistence_writing,
                self._persistence_complete,
                workers,
                fail_first,
            )

    def register_edata(self, metadata, serialized_bytes):
        if not isinstance(metadata, SerializationMetadata):
            raise TypeError("metadata must be SerializationMetadata")
        if not isinstance(serialized_bytes, bytes):
            raise TypeError("serialized_bytes must be bytes")
        digest = EDataRecord.digest(metadata, serialized_bytes)
        bucket_key = (metadata, digest)
        with self._lock:
            self._registrations += 1
            for data_id in self._buckets.get(bucket_key, ()):
                record = self._edata[data_id]
                if record.serialized_bytes == serialized_bytes:
                    return record
            if self._edata_bytes + len(serialized_bytes) > self.max_edata_bytes:
                raise MemoryError("Controller EData capacity exceeded")
            data_id = self._next_edata_id
            self._next_edata_id += 1
            record = EDataRecord(
                data_id, digest, metadata, serialized_bytes
            )
            self._edata[data_id] = record
            self._buckets.setdefault(bucket_key, []).append(data_id)
            self._edata_bytes += len(serialized_bytes)
            return record

    def get_edata(self, data_id):
        with self._lock:
            try:
                return self._edata[int(data_id)]
            except KeyError:
                raise KeyError(f"unknown EDataID {data_id}") from None

    def record_edata_fetch(self, data_id):
        with self._lock:
            self.get_edata(data_id)
            self._edata_fetches[int(data_id)] = (
                self._edata_fetches.get(int(data_id), 0) + 1
            )

    def allocate_idata(self, producer_task_id):
        with self._lock:
            data_id = self._next_idata_id
            self._next_idata_id += 1
            record = IDataRecord(data_id, int(producer_task_id))
            self._idata[data_id] = record
            return record

    def register_task(self, task):
        if not isinstance(task, TaskRecord):
            raise TypeError("task must be TaskRecord")
        with self._lock:
            if task.task_id in self._tasks:
                if self._tasks[task.task_id] != task:
                    raise ValueError(f"conflicting TaskID {task.task_id}")
                return task
            if task.function_data_id not in self._edata:
                raise KeyError(f"unknown function EDataID {task.function_data_id}")
            for kind, data_id in task.positional:
                self._validate_binding(kind, data_id)
            for _, binding in task.keyword:
                self._validate_binding(*binding)
            output = self._idata.get(task.output_data_id)
            if output is None:
                raise KeyError(f"unknown output IDataID {task.output_data_id}")
            if output.producer_task_id != task.task_id:
                raise ValueError("IData producer does not match TaskID")
            self._tasks[task.task_id] = task
            return task

    def _validate_binding(self, kind, data_id):
        if kind in ("e", "c") and data_id in self._edata:
            return
        if kind == "i" and data_id in self._idata:
            return
        raise KeyError(f"unknown {kind}DataID {data_id}")

    def get_task(self, task_id):
        with self._lock:
            try:
                return self._tasks[int(task_id)]
            except KeyError:
                raise KeyError(f"unknown TaskID {task_id}") from None

    def get_idata(self, data_id):
        with self._lock:
            try:
                return self._idata[int(data_id)]
            except KeyError:
                raise KeyError(f"unknown IDataID {data_id}") from None

    def publish_idata(self, data_id, attempt, serialized_bytes):
        if not isinstance(serialized_bytes, bytes):
            raise TypeError("serialized_bytes must be bytes")
        with self._lock:
            old = self.get_idata(data_id)
            attempt = int(attempt)
            if attempt < old.attempt:
                raise ValueError("stale IData publication")
            digest = hashlib.sha256(serialized_bytes).hexdigest()
            if attempt == old.attempt and old.serialized_bytes is not None:
                if old.content_hash != digest or old.serialized_bytes != serialized_bytes:
                    raise ValueError("conflicting IData publication")
                return old
            record = IDataRecord(
                old.data_id,
                old.producer_task_id,
                digest,
                serialized_bytes,
                attempt,
                "volatile",
                None,
            )
            self._idata[data_id] = record
            self._publications += 1
            return record

    def request_persistence(self, data_id):
        with self._lock:
            if self._persistence is None:
                raise RuntimeError("persistence is disabled")
            old = self.get_idata(data_id)
            if old.serialized_bytes is None:
                raise ValueError("cannot persist unavailable IData")
            if old.durability in ("queued", "writing", "durable"):
                return old
            record = dataclasses.replace(old, durability="queued")
            self._idata[old.data_id] = record
            self._persistence_requests += 1
            self._persistence.submit(
                old.data_id, old.serialized_bytes, old.content_hash
            )
            return record

    def _persistence_writing(self, data_id):
        with self._lock:
            self._persistence_active += 1
            self._persistence_max_active = max(
                self._persistence_max_active,
                self._persistence_active,
            )
            old = self.get_idata(data_id)
            self._idata[data_id] = dataclasses.replace(
                old, durability="writing"
            )

    def _persistence_complete(self, data_id, path, error):
        with self._lock:
            self._persistence_active -= 1
            old = self.get_idata(data_id)
            if error is None:
                self._idata[data_id] = dataclasses.replace(
                    old, durability="durable", durable_path=path
                )
                self._persistence_failures.pop(data_id, None)
            else:
                self._idata[data_id] = dataclasses.replace(
                    old, durability="failed", durable_path=None
                )
                self._persistence_failures[data_id] = error

    def idata_status(self, data_id):
        with self._lock:
            value = self.get_idata(data_id)
            return {
                "data_id": value.data_id,
                "producer_task_id": value.producer_task_id,
                "available": value.serialized_bytes is not None,
                "attempt": value.attempt,
                "content_hash": value.content_hash,
                "durability": value.durability,
                "durable_path": value.durable_path,
                "persistence_error": self._persistence_failures.get(
                    value.data_id
                ),
            }

    def invalidate_volatile_idata(self, data_id):
        with self._lock:
            old = self.get_idata(data_id)
            if old.durability == "durable" and old.durable_path:
                with open(old.durable_path, "rb") as stream:
                    payload = stream.read()
                if hashlib.sha256(payload).hexdigest() != old.content_hash:
                    raise IOError("durable recovery checksum mismatch")
                self._idata[data_id] = dataclasses.replace(
                    old, serialized_bytes=payload
                )
                return "restored-durable"
            self._idata[data_id] = dataclasses.replace(
                old,
                serialized_bytes=None,
                durability="volatile",
                durable_path=None,
            )
            return "globally-lost"

    def stop(self):
        persistence = self._persistence
        if persistence is not None:
            persistence.stop()
            self._persistence = None

    def snapshot(self):
        with self._lock:
            return {
                "edata": len(self._edata),
                "edata_bytes": self._edata_bytes,
                "edata_capacity_bytes": self.max_edata_bytes,
                "registrations": self._registrations,
                "deduplicated_registrations": (
                    self._registrations - len(self._edata)
                ),
                "edata_payload_fetches": sum(self._edata_fetches.values()),
                "edata_fetches_by_id": {
                    str(key): value
                    for key, value in sorted(self._edata_fetches.items())
                },
                "edata_sizes_by_id": {
                    str(key): len(record.serialized_bytes)
                    for key, record in sorted(self._edata.items())
                },
                "tasks": len(self._tasks),
                "idata": len(self._idata),
                "available_idata": sum(
                    value.serialized_bytes is not None
                    for value in self._idata.values()
                ),
                "publications": self._publications,
                "durability": {
                    state: sum(
                        record.durability == state
                        for record in self._idata.values()
                    )
                    for state in (
                        "volatile", "queued", "writing", "durable", "failed"
                    )
                },
                "persistence_active": self._persistence_active,
                "persistence_max_active": self._persistence_max_active,
                "persistence_requests": self._persistence_requests,
            }
