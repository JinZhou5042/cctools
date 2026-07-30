"""Thread-safe Controller-owned logical and serialized state."""

import dataclasses
import hashlib
from pathlib import Path
import threading

from ..models import EDataRecord, IDataRecord, SerializationMetadata, TaskRecord
from ..persistence.manager import PersistenceRequest
from .replicas import ReplicaDirectory


class ControllerState:
    def __init__(
        self,
        max_edata_bytes=256 * 1024 * 1024,
        replica_directory=None,
    ):
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
        self._persistence_sequence = 0
        self._persistence_jobs = {}
        self._persistence_active_ids = set()
        self._persistence_stale_completions = 0
        self._persistence_cleanup_failures = 0
        self.replicas = replica_directory or ReplicaDirectory()

    def configure_persistence(
        self,
        root,
        workers=1,
        fail_first=False,
        queue_capacity=64,
        terminal_capacity=1024,
        transition_hook=None,
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
                queue_capacity,
                terminal_capacity,
                transition_hook,
            )

    def _publish_replica(
        self,
        data_key,
        replica_id,
        attempt,
        tier,
        content_hash,
        size,
    ):
        replica = self.replicas.prepare_replica(
            data_key,
            replica_id,
            attempt,
            tier,
            content_hash,
            size,
        )
        return self.replicas.commit_replica(
            data_key,
            replica_id,
            replica.generation,
            attempt,
            content_hash,
            size,
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
            record = EDataRecord(
                data_id, digest, metadata, serialized_bytes
            )
            self._publish_replica(
                f"e:{data_id}",
                f"controller-edata-{data_id}",
                1,
                "controller-memory",
                digest,
                len(serialized_bytes),
            )
            self._next_edata_id += 1
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
            if attempt < 1:
                raise ValueError("IData publication attempt must be positive")
            if attempt < old.attempt:
                raise ValueError("stale IData publication")
            digest = hashlib.sha256(serialized_bytes).hexdigest()
            if attempt == old.attempt and old.serialized_bytes is not None:
                if old.content_hash != digest or old.serialized_bytes != serialized_bytes:
                    raise ValueError("conflicting IData publication")
                return old
            if attempt > old.attempt:
                if old.durability == "durable":
                    raise ValueError("cannot supersede durable IData")
                self._cancel_persistence_locked(
                    old.data_id, "superseded-attempt"
                )
            self._publish_replica(
                f"i:{old.data_id}",
                (
                    f"controller-idata-{old.data_id}-"
                    f"attempt-{attempt}"
                ),
                attempt,
                "controller-memory",
                digest,
                len(serialized_bytes),
            )
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
            self._persistence_sequence += 1
            request = PersistenceRequest(
                request_id=(
                    f"i{old.data_id}-a{old.attempt}-"
                    f"p{self._persistence_sequence}"
                ),
                data_id=old.data_id,
                attempt=old.attempt,
                payload=old.serialized_bytes,
                content_hash=old.content_hash,
            )
            record = dataclasses.replace(old, durability="queued")
            self._idata[old.data_id] = record
            self._persistence_jobs[old.data_id] = {
                "request_id": request.request_id,
                "attempt": request.attempt,
                "content_hash": request.content_hash,
                "state": "queued",
                "cancel_reason": None,
            }
            self._persistence_requests += 1
            try:
                self._persistence.submit(request)
            except Exception:
                self._idata[old.data_id] = old
                self._persistence_jobs.pop(old.data_id, None)
                self._persistence_requests -= 1
                raise
            return record

    def cancel_persistence(self, data_id, reason="obsolete"):
        with self._lock:
            return self._cancel_persistence_locked(data_id, reason)

    def _cancel_persistence_locked(self, data_id, reason):
        data_id = int(data_id)
        job = self._persistence_jobs.get(data_id)
        if job is None or job["state"] not in ("queued", "writing"):
            return "not-active"
        result = self._persistence.cancel(job["request_id"])
        if result in ("cancelled", "cancelling"):
            job["state"] = result
            job["cancel_reason"] = str(reason)
            old = self.get_idata(data_id)
            self._idata[data_id] = dataclasses.replace(
                old, durability="cancelled"
            )
        return result

    def _persistence_writing(self, request):
        with self._lock:
            job = self._persistence_jobs.get(request.data_id)
            current_idata = self._idata.get(request.data_id)
            if (
                job is None
                or current_idata is None
                or job["request_id"] != request.request_id
                or job["attempt"] != request.attempt
                or job["content_hash"] != request.content_hash
                or current_idata.attempt != request.attempt
                or current_idata.content_hash != request.content_hash
            ):
                return
            job["state"] = "writing"
            self._persistence_active_ids.add(request.request_id)
            self._persistence_active = len(
                self._persistence_active_ids
            )
            self._persistence_max_active = max(
                self._persistence_max_active,
                self._persistence_active,
            )
            old = current_idata
            self._idata[request.data_id] = dataclasses.replace(
                old, durability="writing"
            )

    def _persistence_complete(self, request, path, error):
        with self._lock:
            self._persistence_active_ids.discard(request.request_id)
            self._persistence_active = len(
                self._persistence_active_ids
            )
            job = self._persistence_jobs.get(request.data_id)
            old = self._idata.get(request.data_id)
            current = (
                job is not None
                and old is not None
                and job["request_id"] == request.request_id
                and old.attempt == request.attempt
                and old.content_hash == request.content_hash
            )
            if not current:
                self._persistence_stale_completions += 1
                if path is not None:
                    self._discard_persistence_path(path)
                return
            if error == "cancelled":
                job["state"] = "cancelled"
                self._idata[request.data_id] = dataclasses.replace(
                    old, durability="cancelled", durable_path=None
                )
                return
            if error is not None:
                job["state"] = "failed"
                self._idata[request.data_id] = dataclasses.replace(
                    old, durability="failed", durable_path=None
                )
                self._persistence_failures[request.data_id] = error
                return
            try:
                self._publish_replica(
                    f"i:{request.data_id}",
                    (
                        f"sharedfs-idata-{request.data_id}-"
                        f"attempt-{request.attempt}"
                    ),
                    request.attempt,
                    "sharedfs",
                    request.content_hash,
                    len(request.payload),
                )
            except Exception as exc:
                self._discard_persistence_path(path)
                job["state"] = "failed"
                self._idata[request.data_id] = dataclasses.replace(
                    old, durability="failed", durable_path=None
                )
                self._persistence_failures[request.data_id] = str(exc)
                return
            job["state"] = "durable"
            self._idata[request.data_id] = dataclasses.replace(
                old, durability="durable", durable_path=path
            )
            self._persistence_failures.pop(request.data_id, None)

    def _discard_persistence_path(self, path):
        try:
            Path(path).unlink(missing_ok=True)
        except OSError:
            self._persistence_cleanup_failures += 1

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
                "persistence_request": dict(
                    self._persistence_jobs.get(value.data_id, {})
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
                self._publish_replica(
                    f"i:{old.data_id}",
                    (
                        f"controller-idata-{old.data_id}-"
                        f"attempt-{old.attempt}"
                    ),
                    old.attempt,
                    "controller-memory",
                    old.content_hash,
                    len(payload),
                )
                return "restored-durable"
            self._cancel_persistence_locked(old.data_id, "global-loss")
            try:
                replica = self.replicas.get_replica(
                    f"i:{old.data_id}",
                    (
                        f"controller-idata-{old.data_id}-"
                        f"attempt-{old.attempt}"
                    ),
                )
            except KeyError:
                replica = None
            if replica is not None:
                self.replicas.invalidate_replica(
                    replica.data_id,
                    replica.replica_id,
                    replica.generation,
                )
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
                        "volatile",
                        "queued",
                        "writing",
                        "durable",
                        "failed",
                        "cancelled",
                    )
                },
                "persistence_active": self._persistence_active,
                "persistence_max_active": self._persistence_max_active,
                "persistence_requests": self._persistence_requests,
                "persistence_stale_completions": (
                    self._persistence_stale_completions
                ),
                "persistence_cleanup_failures": (
                    self._persistence_cleanup_failures
                ),
                "persistence_executor": (
                    self._persistence.snapshot()
                    if self._persistence is not None
                    else None
                ),
                "replica_directory": self.replicas.snapshot(),
            }
