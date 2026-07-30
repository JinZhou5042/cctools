"""Thread-safe Controller-owned logical and serialized state."""

import dataclasses
import hashlib
import os
from pathlib import Path
import threading

from ..models import (
    EDataRecord,
    IDataRecord,
    SerializationMetadata,
    TaskRecord,
)
from ..persistence.manager import PersistenceRequest
from .pruning import PruningAuthority
from .replicas import ReplicaDirectory


class ControllerState:
    def __init__(
        self,
        max_edata_bytes=256 * 1024 * 1024,
        replica_directory=None,
        pruning_audit_capacity=10000,
        bulk_origin_root=None,
        max_idata_bytes=256 * 1024 * 1024,
        max_inline_idata_bytes=8 * 1024 * 1024,
    ):
        if max_edata_bytes <= 0:
            raise ValueError("max_edata_bytes must be positive")
        if max_idata_bytes < 0:
            raise ValueError("max_idata_bytes cannot be negative")
        if max_inline_idata_bytes < 0:
            raise ValueError(
                "max_inline_idata_bytes cannot be negative"
            )
        if max_inline_idata_bytes > max_idata_bytes:
            raise ValueError(
                "max_inline_idata_bytes cannot exceed max_idata_bytes"
            )
        self.max_edata_bytes = int(max_edata_bytes)
        self.max_idata_bytes = int(max_idata_bytes)
        self.max_inline_idata_bytes = int(max_inline_idata_bytes)
        self.bulk_origin_root = (
            Path(bulk_origin_root).resolve()
            if bulk_origin_root is not None
            else None
        )
        if self.bulk_origin_root is not None:
            self.bulk_origin_root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._next_edata_id = 1
        self._edata = {}
        self._buckets = {}
        self._edata_bytes = 0
        self._edata_bulk_bytes = 0
        self._registrations = 0
        self._edata_fetches = {}
        self._next_idata_id = 1
        self._idata = {}
        self._idata_bytes = 0
        self._idata_bytes_high_water = 0
        self._idata_metadata_publications = 0
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
        self.pruning = PruningAuthority(pruning_audit_capacity)

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
            self.pruning.configure_filesystem(root)

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
                if self._edata_matches_bytes(record, serialized_bytes):
                    return record
            projected_bytes = self._edata_bytes + len(serialized_bytes)
            if projected_bytes > self.max_edata_bytes:
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

    @staticmethod
    def _edata_matches_bytes(record, serialized_bytes):
        if record.serialized_bytes is not None:
            return record.serialized_bytes == serialized_bytes
        if len(serialized_bytes) != record.serialized_size:
            return False
        view = memoryview(serialized_bytes)
        offset = 0
        with open(record.stable_path, "rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                if chunk != view[offset:offset + len(chunk)]:
                    return False
                offset += len(chunk)
        return offset == len(serialized_bytes)

    @staticmethod
    def _edata_origins_equal(left, right, size):
        remaining = int(size)
        with open(left, "rb") as first, open(right, "rb") as second:
            while remaining:
                amount = min(1024 * 1024, remaining)
                if first.read(amount) != second.read(amount):
                    return False
                remaining -= amount
            return first.read(1) == second.read(1) == b""

    @staticmethod
    def _edata_inline_origin_equal(serialized_bytes, path):
        view = memoryview(serialized_bytes)
        offset = 0
        with open(path, "rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                if chunk != view[offset:offset + len(chunk)]:
                    return False
                offset += len(chunk)
        return offset == len(serialized_bytes)

    def register_edata_origin(
        self, metadata, stable_path, content_hash, serialized_size
    ):
        if not isinstance(metadata, SerializationMetadata):
            raise TypeError("metadata must be SerializationMetadata")
        if self.bulk_origin_root is None:
            raise RuntimeError("Controller bulk origin is not configured")
        requested = Path(stable_path)
        if requested.is_symlink():
            raise ValueError("bulk origin cannot be a symbolic link")
        resolved = requested.resolve(strict=True)
        try:
            resolved.relative_to(self.bulk_origin_root)
        except ValueError:
            raise ValueError("bulk origin escapes configured root") from None
        if not resolved.is_file():
            raise ValueError("bulk origin must be a regular file")
        if resolved.name != f"edata-{content_hash}.pkl":
            raise ValueError("bulk origin filename is not content-addressed")
        stat = resolved.stat()
        serialized_size = int(serialized_size)
        if stat.st_size != serialized_size:
            raise ValueError("bulk origin size mismatch")
        digest = hashlib.sha256()
        digest.update(metadata.identity_bytes())
        digest.update(b"\0")
        with resolved.open("rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        if digest.hexdigest() != str(content_hash):
            raise ValueError("bulk origin content hash mismatch")
        bucket_key = (metadata, str(content_hash))
        with self._lock:
            self._registrations += 1
            for data_id in self._buckets.get(bucket_key, ()):
                record = self._edata[data_id]
                if record.serialized_size != serialized_size:
                    continue
                if record.serialized_bytes is not None:
                    if self._edata_inline_origin_equal(
                        record.serialized_bytes, resolved
                    ):
                        return record
                elif self._edata_origins_equal(
                    record.stable_path, resolved, serialized_size
                ):
                    return record
            data_id = self._next_edata_id
            record = EDataRecord(
                data_id,
                str(content_hash),
                metadata,
                None,
                str(resolved),
                serialized_size,
            )
            self._publish_replica(
                f"e:{data_id}",
                f"bulk-origin-edata-{data_id}",
                1,
                "sharedfs",
                record.content_hash,
                serialized_size,
            )
            self._next_edata_id += 1
            self._edata[data_id] = record
            self._buckets.setdefault(bucket_key, []).append(data_id)
            self._edata_bulk_bytes += serialized_size
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

    def allocate_idata(self, producer_task_id, producer_output_index=0):
        with self._lock:
            data_id = self._next_idata_id
            self._next_idata_id += 1
            record = IDataRecord(
                data_id,
                int(producer_task_id),
                int(producer_output_index),
            )
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
                raise KeyError(
                    f"unknown function EDataID {task.function_data_id}"
                )
            for kind, data_id in task.positional:
                self._validate_binding(kind, data_id)
            for _, binding in task.keyword:
                self._validate_binding(*binding)
            for output_index, output_data_id in enumerate(
                task.output_data_ids
            ):
                output = self._idata.get(output_data_id)
                if output is None:
                    raise KeyError(
                        f"unknown output IDataID {output_data_id}"
                    )
                if (
                    output.producer_task_id != task.task_id
                    or output.producer_output_index != output_index
                ):
                    raise ValueError(
                        "IData producer slot does not match TaskID"
                    )
            direct_inputs = {
                data_id
                for kind, data_id in task.positional
                if kind == "i"
            }
            direct_inputs.update(
                data_id
                for _, (kind, data_id) in task.keyword
                if kind == "i"
            )
            normalized_inputs = tuple(
                sorted(set(task.input_data_ids))
            )
            if task.input_data_ids != normalized_inputs:
                raise ValueError(
                    "TaskRecord IData dependencies must be unique and sorted"
                )
            if not direct_inputs <= set(task.input_data_ids):
                raise ValueError("TaskRecord omits direct IData dependency")
            self.pruning.register_task(
                task.task_id,
                task.input_data_ids,
                task.output_data_ids,
            )
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
                if (
                    old.content_hash != digest
                    or old.serialized_bytes != serialized_bytes
                ):
                    raise ValueError("conflicting IData publication")
                return old
            if len(serialized_bytes) > self.max_inline_idata_bytes:
                raise MemoryError(
                    "IData exceeds Controller inline object capacity"
                )
            old_bytes = (
                len(old.serialized_bytes)
                if old.serialized_bytes is not None
                else 0
            )
            projected_bytes = (
                self._idata_bytes - old_bytes + len(serialized_bytes)
            )
            if projected_bytes > self.max_idata_bytes:
                raise MemoryError("Controller IData capacity exceeded")
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
                old.producer_output_index,
                digest,
                serialized_bytes,
                attempt,
                "volatile",
                None,
                len(serialized_bytes),
            )
            self._idata[data_id] = record
            self._idata_bytes = projected_bytes
            self._idata_bytes_high_water = max(
                self._idata_bytes_high_water, self._idata_bytes
            )
            self._publications += 1
            self.pruning.set_data_state(
                data_id,
                available=True,
                durable=False,
                persistence="none",
            )
            return record

    def publish_idata_metadata(
        self, data_id, attempt, content_hash, serialized_size
    ):
        with self._lock:
            old = self.get_idata(data_id)
            attempt = int(attempt)
            serialized_size = int(serialized_size)
            content_hash = str(content_hash)
            if attempt < 1 or serialized_size < 0:
                raise ValueError("invalid IData metadata publication")
            if (
                len(content_hash) != 64
                or any(
                    value not in "0123456789abcdef"
                    for value in content_hash
                )
            ):
                raise ValueError("invalid IData content hash")
            if attempt < old.attempt:
                raise ValueError("stale IData publication")
            if attempt == old.attempt and old.content_hash is not None:
                if (
                    old.content_hash != content_hash
                    or old.serialized_size != serialized_size
                ):
                    raise ValueError("conflicting IData publication")
                return old
            if attempt > old.attempt:
                if old.durability == "durable":
                    raise ValueError("cannot supersede durable IData")
                self._cancel_persistence_locked(
                    old.data_id, "superseded-attempt"
                )
            old_bytes = (
                len(old.serialized_bytes)
                if old.serialized_bytes is not None
                else 0
            )
            self.replicas.advance_attempt(
                f"i:{old.data_id}", attempt
            )
            record = IDataRecord(
                old.data_id,
                old.producer_task_id,
                old.producer_output_index,
                content_hash,
                None,
                attempt,
                "volatile",
                None,
                serialized_size,
            )
            self._idata[data_id] = record
            self._idata_bytes -= old_bytes
            self._publications += 1
            self._idata_metadata_publications += 1
            self.pruning.set_data_state(
                data_id,
                available=False,
                durable=False,
                persistence="none",
            )
            return record

    def join_worker(self, worker_id, epoch):
        with self._lock:
            return self.replicas.join_worker(worker_id, epoch)

    def claim_worker(self, worker_id):
        with self._lock:
            return self.replicas.claim_worker(worker_id)

    def disconnect_worker(self, worker_id, epoch):
        with self._lock:
            return self.replicas.disconnect_worker(worker_id, epoch)

    def reconcile_workers(self, active_worker_ids):
        with self._lock:
            return self.replicas.reconcile_workers(active_worker_ids)

    def acquire_replica(
        self,
        data_id,
        replica_id,
        generation,
        destination_worker_id,
        destination_worker_epoch,
    ):
        with self._lock:
            return self.replicas.acquire_source(
                data_id,
                replica_id,
                generation,
                destination_worker_id,
                destination_worker_epoch,
            )

    def acquire_observed_transfer(
        self,
        data_id,
        source_worker_id,
        destination_worker_id,
        transfer_id,
    ):
        with self._lock:
            return self.replicas.acquire_observed_transfer(
                data_id,
                source_worker_id,
                destination_worker_id,
                transfer_id,
            )

    def release_replica(self, lease_id, success):
        with self._lock:
            return self.replicas.release_source(lease_id, success)

    def invalidate_worker_replica(
        self,
        data_id,
        replica_id,
        generation,
        worker_id,
        worker_epoch,
    ):
        with self._lock:
            return self.replicas.invalidate_worker_replica(
                data_id,
                replica_id,
                generation,
                worker_id,
                worker_epoch,
            )

    def invalidate_observed_worker_replica(
        self,
        data_id,
        replica_id,
        attempt,
        content_hash,
        size,
        worker_id,
        worker_epoch,
    ):
        with self._lock:
            return self.replicas.invalidate_observed_worker_replica(
                data_id,
                replica_id,
                attempt,
                content_hash,
                size,
                worker_id,
                worker_epoch,
            )

    def confirm_worker_pruned(
        self, data_id, replica_id, generation
    ):
        with self._lock:
            replica = self.replicas.confirm_worker_pruned(
                data_id, replica_id, generation
            )
            audit = self.pruning.audit(
                "confirm-worker-pruned",
                int(str(data_id).split(":", 1)[1]),
                "worker-cache-unlink-acknowledged",
                self.replicas.revision,
                replica.replica_id,
                replica.generation,
            )
            return {
                "replica": replica.source_dict(),
                "audit": audit.to_dict(),
            }

    def _validate_replica_identity(
        self, data_key, attempt, content_hash, size
    ):
        try:
            kind, token = str(data_key).split(":", 1)
            data_id = int(token)
        except (TypeError, ValueError):
            raise ValueError("invalid qualified DataID") from None
        attempt = int(attempt)
        size = int(size)
        if kind == "e":
            record = self.get_edata(data_id)
            expected_attempt = 1
            expected_hash = record.content_hash
            expected_size = record.serialized_size
        elif kind == "i":
            record = self.get_idata(data_id)
            expected_attempt = record.attempt
            expected_hash = record.content_hash
            expected_size = record.serialized_size
        else:
            raise ValueError("invalid qualified DataID")
        if (
            attempt != expected_attempt
            or content_hash != expected_hash
            or size != expected_size
        ):
            raise ValueError("replica does not match logical data identity")
        return record

    def prepare_worker_replica(
        self,
        data_key,
        replica_id,
        attempt,
        tier,
        content_hash,
        size,
        worker_id,
        worker_epoch,
    ):
        with self._lock:
            self._validate_replica_identity(
                data_key, attempt, content_hash, size
            )
            if tier not in ("worker-dram", "worker-disk"):
                raise ValueError("worker report requires worker tier")
            return self.replicas.prepare_replica(
                data_key,
                replica_id,
                attempt,
                tier,
                content_hash,
                size,
                worker_id,
                worker_epoch,
            )

    def commit_worker_replica(
        self,
        data_key,
        replica_id,
        generation,
        attempt,
        content_hash,
        size,
    ):
        with self._lock:
            self._validate_replica_identity(
                data_key, attempt, content_hash, size
            )
            replica = self.replicas.commit_replica(
                data_key,
                replica_id,
                generation,
                attempt,
                content_hash,
                size,
            )
            if data_key.startswith("i:"):
                value = self.get_idata(int(data_key.split(":", 1)[1]))
                self.pruning.set_data_state(
                    value.data_id,
                    available=True,
                    durable=value.durability == "durable",
                )
            return replica

    def report_worker_replica(
        self,
        data_key,
        replica_id,
        attempt,
        tier,
        content_hash,
        size,
        worker_id,
        worker_epoch,
    ):
        replica = self.prepare_worker_replica(
            data_key,
            replica_id,
            attempt,
            tier,
            content_hash,
            size,
            worker_id,
            worker_epoch,
        )
        return self.commit_worker_replica(
            data_key,
            replica_id,
            replica.generation,
            attempt,
            content_hash,
            size,
        )

    def request_persistence(self, data_id):
        with self._lock:
            if self._persistence is None:
                raise RuntimeError("persistence is disabled")
            old = self.get_idata(data_id)
            if old.durability in ("queued", "writing", "durable"):
                return old
            self._persistence_sequence += 1
            request_id = (
                f"i{old.data_id}-a{old.attempt}-"
                f"p{self._persistence_sequence}"
            )
            if old.serialized_bytes is None:
                if not self.replicas.candidates(f"i:{old.data_id}"):
                    raise ValueError(
                        "cannot persist unavailable IData"
                    )
                active_external = sum(
                    job.get("mode") == "worker"
                    and job["state"] in (
                        "queued",
                        "writing",
                        "cancelling",
                    )
                    for job in self._persistence_jobs.values()
                )
                capacity = (
                    self._persistence.queue_capacity
                    + self._persistence.worker_count
                )
                if active_external >= capacity:
                    raise RuntimeError(
                        "external persistence admission capacity exceeded"
                    )
                target = self._persistence.target_path(
                    old.data_id, old.attempt, old.content_hash
                )
                record = dataclasses.replace(old, durability="queued")
                self._idata[old.data_id] = record
                self._persistence_jobs[old.data_id] = {
                    "request_id": request_id,
                    "attempt": old.attempt,
                    "content_hash": old.content_hash,
                    "size": old.serialized_size,
                    "state": "queued",
                    "cancel_reason": None,
                    "mode": "worker",
                    "target_path": str(target),
                }
                self._persistence_requests += 1
                self.pruning.set_data_state(
                    old.data_id, persistence="queued"
                )
                return record
            request = PersistenceRequest(
                request_id=(
                    request_id
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
                "mode": "controller",
            }
            self._persistence_requests += 1
            try:
                self._persistence.submit(request)
            except Exception:
                self._idata[old.data_id] = old
                self._persistence_jobs.pop(old.data_id, None)
                self._persistence_requests -= 1
                raise
            self.pruning.set_data_state(
                old.data_id, persistence="queued"
            )
            return record

    def begin_external_persistence(self, data_id, request_id):
        with self._lock:
            old = self.get_idata(data_id)
            job = self._persistence_jobs.get(old.data_id)
            if (
                job is None
                or job.get("mode") != "worker"
                or job["request_id"] != str(request_id)
            ):
                raise ValueError("unknown external persistence request")
            if job["state"] == "durable":
                return dict(job)
            if job["state"] == "writing":
                return dict(job)
            if job["state"] != "queued":
                raise ValueError(
                    f"cannot begin persistence in state {job['state']}"
                )
            if (
                len(self._persistence_active_ids)
                >= self._persistence.worker_count
            ):
                raise RuntimeError(
                    "global persistence concurrency exceeded"
                )
            job["state"] = "writing"
            self._idata[old.data_id] = dataclasses.replace(
                old, durability="writing"
            )
            self._persistence_active_ids.add(job["request_id"])
            self._persistence_active = len(
                self._persistence_active_ids
            )
            self._persistence_max_active = max(
                self._persistence_max_active,
                self._persistence_active,
            )
            self.pruning.set_data_state(
                old.data_id, persistence="writing"
            )
            return dict(job)

    def complete_external_persistence(self, data_id, request_id):
        cancelled_target = None
        with self._lock:
            old = self.get_idata(data_id)
            job = self._persistence_jobs.get(old.data_id)
            if (
                job is not None
                and job.get("mode") == "worker"
                and job["request_id"] == str(request_id)
                and job["state"] == "durable"
                and old.durability == "durable"
                and old.durable_path == job["target_path"]
            ):
                return old
            if (
                job is not None
                and job.get("mode") == "worker"
                and job["request_id"] == str(request_id)
                and job["state"] in ("cancelled", "cancelling")
            ):
                cancelled_target = Path(job["target_path"])
                job["state"] = "cancelled"
                self._persistence_active_ids.discard(
                    job["request_id"]
                )
                self._persistence_active = len(
                    self._persistence_active_ids
                )
                record = dataclasses.replace(
                    old,
                    durability="cancelled",
                    durable_path=None,
                )
                self._idata[old.data_id] = record
                self.pruning.set_data_state(
                    old.data_id, persistence="none"
                )
            else:
                record = None
            if record is None:
                if (
                    job is None
                    or job.get("mode") != "worker"
                    or job["request_id"] != str(request_id)
                    or job["state"] != "writing"
                    or old.attempt != job["attempt"]
                    or old.content_hash != job["content_hash"]
                    or old.serialized_size != job["size"]
                ):
                    raise ValueError(
                        "stale external persistence completion"
                    )
                target = Path(job["target_path"])
                expected = {
                    key: job[key]
                    for key in (
                        "request_id",
                        "attempt",
                        "content_hash",
                        "size",
                        "target_path",
                    )
                }
        if cancelled_target is not None:
            cancelled_target.unlink(missing_ok=True)
            return record
        digest = hashlib.sha256()
        size = 0
        with target.open("rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                digest.update(chunk)
        if (
            size != expected["size"]
            or digest.hexdigest() != expected["content_hash"]
        ):
            target.unlink(missing_ok=True)
            raise IOError("external persistence validation failed")
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        with self._lock:
            old = self.get_idata(data_id)
            job = self._persistence_jobs.get(old.data_id)
            if (
                job is not None
                and job.get("mode") == "worker"
                and job["request_id"] == expected["request_id"]
                and job["state"] in ("cancelled", "cancelling")
            ):
                job["state"] = "cancelled"
                self._persistence_active_ids.discard(
                    job["request_id"]
                )
                self._persistence_active = len(
                    self._persistence_active_ids
                )
                record = dataclasses.replace(
                    old,
                    durability="cancelled",
                    durable_path=None,
                )
                self._idata[old.data_id] = record
                self.pruning.set_data_state(
                    old.data_id, persistence="none"
                )
                cancelled_after_validation = True
            else:
                cancelled_after_validation = False
            if (
                not cancelled_after_validation
                and (
                    job is None
                    or job.get("mode") != "worker"
                    or job["state"] != "writing"
                    or any(
                        job[key] != value
                        for key, value in expected.items()
                    )
                    or old.attempt != expected["attempt"]
                    or old.content_hash != expected["content_hash"]
                    or old.serialized_size != expected["size"]
                )
            ):
                target.unlink(missing_ok=True)
                self._persistence_stale_completions += 1
                raise ValueError("stale external persistence completion")
            if cancelled_after_validation:
                target.unlink(missing_ok=True)
                return record
            self._publish_replica(
                f"i:{old.data_id}",
                (
                    f"sharedfs-idata-{old.data_id}-"
                    f"attempt-{old.attempt}"
                ),
                old.attempt,
                "sharedfs",
                old.content_hash,
                old.serialized_size,
            )
            job["state"] = "durable"
            self._persistence_active_ids.discard(job["request_id"])
            self._persistence_active = len(
                self._persistence_active_ids
            )
            record = dataclasses.replace(
                old, durability="durable", durable_path=str(target)
            )
            self._idata[old.data_id] = record
            self.pruning.set_data_state(
                old.data_id,
                available=True,
                durable=True,
                persistence="none",
            )
            return record

    def fail_external_persistence(
        self, data_id, request_id, error
    ):
        with self._lock:
            old = self.get_idata(data_id)
            job = self._persistence_jobs.get(old.data_id)
            if (
                job is None
                or job.get("mode") != "worker"
                or job["request_id"] != str(request_id)
            ):
                return "stale"
            if job["state"] == "durable":
                return "too-late"
            cancelled = job["state"] in (
                "cancelled",
                "cancelling",
            )
            job["state"] = "cancelled" if cancelled else "failed"
            self._persistence_active_ids.discard(job["request_id"])
            self._persistence_active = len(
                self._persistence_active_ids
            )
            self._idata[old.data_id] = dataclasses.replace(
                old,
                durability="cancelled" if cancelled else "failed",
                durable_path=None,
            )
            if cancelled:
                Path(job["target_path"]).unlink(missing_ok=True)
            else:
                self._persistence_failures[old.data_id] = str(error)
            self.pruning.set_data_state(
                old.data_id, persistence="none"
            )
            return "cancelled" if cancelled else "failed"

    def cancel_persistence(self, data_id, reason="obsolete"):
        with self._lock:
            return self._cancel_persistence_locked(data_id, reason)

    def _cancel_persistence_locked(self, data_id, reason):
        data_id = int(data_id)
        job = self._persistence_jobs.get(data_id)
        if job is None or job["state"] not in ("queued", "writing"):
            return "not-active"
        if job.get("mode") == "worker":
            if job["state"] == "queued":
                result = "cancelled"
            else:
                result = "cancelling"
            job["state"] = result
        else:
            result = self._persistence.cancel(job["request_id"])
        if result in ("cancelled", "cancelling"):
            job["state"] = result
            job["cancel_reason"] = str(reason)
            old = self.get_idata(data_id)
            self._idata[data_id] = dataclasses.replace(
                old, durability="cancelled"
            )
            self.pruning.set_data_state(
                data_id, persistence="none"
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
            if (
                len(self._persistence_active_ids)
                >= self._persistence.worker_count
            ):
                raise RuntimeError(
                    "global persistence concurrency exceeded"
                )
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
            self.pruning.set_data_state(
                request.data_id, persistence="writing"
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
                self.pruning.set_data_state(
                    request.data_id, persistence="none"
                )
                return
            if error is not None:
                job["state"] = "failed"
                self._idata[request.data_id] = dataclasses.replace(
                    old, durability="failed", durable_path=None
                )
                self.pruning.set_data_state(
                    request.data_id, persistence="none"
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
                self.pruning.set_data_state(
                    request.data_id, persistence="none"
                )
                self._persistence_failures[request.data_id] = str(exc)
                return
            job["state"] = "durable"
            self._idata[request.data_id] = dataclasses.replace(
                old, durability="durable", durable_path=path
            )
            self.pruning.set_data_state(
                request.data_id,
                available=True,
                durable=True,
                persistence="none",
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
            sources = self.replicas.candidates(f"i:{value.data_id}")
            available = bool(sources)
            return {
                "data_id": value.data_id,
                "producer_task_id": value.producer_task_id,
                "producer_output_index": value.producer_output_index,
                "available": available,
                "rematerializable": bool(
                    value.serialized_bytes is not None
                    or (
                        value.durability == "durable"
                        and value.durable_path
                    )
                ),
                "controller_inline": (
                    value.serialized_bytes is not None
                ),
                "attempt": value.attempt,
                "content_hash": value.content_hash,
                "size": value.serialized_size,
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
                digest = hashlib.sha256()
                size = 0
                with open(old.durable_path, "rb") as stream:
                    while True:
                        chunk = stream.read(1024 * 1024)
                        if not chunk:
                            break
                        size += len(chunk)
                        digest.update(chunk)
                if (
                    digest.hexdigest() != old.content_hash
                    or size != old.serialized_size
                ):
                    raise IOError("durable recovery checksum mismatch")
                self._publish_replica(
                    f"i:{old.data_id}",
                    (
                        f"sharedfs-idata-{old.data_id}-"
                        f"attempt-{old.attempt}"
                    ),
                    old.attempt,
                    "sharedfs",
                    old.content_hash,
                    size,
                )
                self.pruning.set_data_state(
                    old.data_id, available=True, durable=True
                )
                return "validated-durable"
            self._cancel_persistence_locked(old.data_id, "global-loss")
            for replica in self.replicas.records_for(
                f"i:{old.data_id}"
            ):
                if (
                    replica.attempt != old.attempt
                    or replica.state
                    in ("invalid", "pruned", "quarantined")
                ):
                    continue
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
            self._idata_bytes -= (
                len(old.serialized_bytes)
                if old.serialized_bytes is not None
                else 0
            )
            self.pruning.set_data_state(
                old.data_id,
                available=False,
                durable=False,
                persistence="none",
            )
            return "globally-lost"

    def set_task_state(self, task_id, state):
        with self._lock:
            self.get_task(task_id)
            return self.pruning.set_task_state(task_id, state).to_dict()

    def set_required_output(self, data_id, required=True):
        with self._lock:
            self.get_idata(data_id)
            return self.pruning.set_data_state(
                data_id, required_output=bool(required)
            ).to_dict()

    def pruning_plan(self):
        with self._lock:
            return self.pruning.plan().to_dict()

    def _pruning_record(self, plan, data_id):
        for record in plan.records:
            if record.data_id == int(data_id):
                return record
        raise KeyError(f"unknown pruning IDataID {data_id}")

    def apply_pruning(
        self,
        graph_revision,
        state_revision,
        grace_seconds=60,
        data_ids=None,
        now=None,
    ):
        """Compare a proof revision and quarantine/invalidate proven data."""
        with self._lock:
            plan = self.pruning.validate_revision(
                graph_revision, state_revision
            )
            cancelled = []
            for data_id in plan.cancel_persistence:
                action = self._cancel_persistence_locked(
                    data_id, "pruning-obsolete"
                )
                if action in ("cancelled", "cancelling"):
                    cancelled.append(data_id)
                    self.pruning.audit(
                        "cancel-persistence",
                        data_id,
                        "obsolete-persistence",
                        self.replicas.revision,
                    )
            if cancelled:
                plan = self.pruning.plan()
            selected = (
                set(plan.prunable)
                if data_ids is None
                else {int(data_id) for data_id in data_ids}
            )
            unknown = selected - set(plan.prunable)
            if unknown:
                raise ValueError(
                    f"IDataIDs are not prunable: {sorted(unknown)}"
                )
            applied = []
            deferred = []
            for data_id in sorted(selected):
                record = self._pruning_record(
                    self.pruning.plan(), data_id
                )
                if record.decision != "prune":
                    raise ValueError(
                        f"IDataID {data_id} proof changed before pruning"
                    )
                result = self._prune_idata_locked(
                    data_id, record, grace_seconds, now
                )
                applied.extend(result["applied"])
                deferred.extend(result["deferred"])
            return {
                "cancelled_persistence": cancelled,
                "applied": applied,
                "deferred": deferred,
                "plan": self.pruning.plan().to_dict(),
                "replica_revision": self.replicas.revision,
            }

    def _prune_idata_locked(
        self, data_id, proof, grace_seconds, now
    ):
        old = self.get_idata(data_id)
        applied = []
        deferred = []
        sharedfs_id = (
            f"sharedfs-idata-{old.data_id}-attempt-{old.attempt}"
        )
        for replica in self.replicas.records_for(f"i:{data_id}"):
            if replica.state != "available":
                continue
            if replica.active_leases:
                retiring = self.replicas.invalidate_replica(
                    replica.data_id,
                    replica.replica_id,
                    replica.generation,
                )
                deferred.append(
                    {
                        "data_id": data_id,
                        "replica_id": retiring.replica_id,
                        "action": "retiring-active-read",
                    }
                )
                continue
            if replica.tier == "sharedfs":
                if (
                    old.durable_path is None
                    or replica.replica_id != sharedfs_id
                ):
                    raise ValueError(
                        "SharedFS replica lacks owned durable path"
                    )
                quarantine_path = self.pruning.quarantine_file(
                    data_id,
                    replica.replica_id,
                    replica.generation,
                    old.durable_path,
                )
                revision = self.replicas.revision
                try:
                    quarantined = self.replicas.quarantine(
                        replica.data_id,
                        replica.replica_id,
                        replica.generation,
                        grace_seconds,
                        revision,
                        now,
                    )
                except Exception:
                    self.pruning.restore_file(
                        data_id,
                        replica.replica_id,
                        replica.generation,
                    )
                    raise
                action = "quarantine-sharedfs"
                path = str(quarantine_path)
                generation = quarantined.generation
            else:
                invalid = self.replicas.invalidate_replica(
                    replica.data_id,
                    replica.replica_id,
                    replica.generation,
                )
                action = (
                    "invalidate-worker-pending-delete"
                    if replica.tier in ("worker-dram", "worker-disk")
                    else "drop-controller-replica"
                )
                path = None
                generation = invalid.generation
            audit = self.pruning.audit(
                action,
                data_id,
                ",".join(proof.reasons),
                self.replicas.revision,
                replica.replica_id,
                generation,
                path,
            )
            applied.append(audit.to_dict())
        self._idata[data_id] = dataclasses.replace(
            old,
            serialized_bytes=None,
            durability="volatile",
            durable_path=None,
        )
        self.pruning.set_data_state(
            data_id,
            available=self.replicas.globally_available(f"i:{data_id}"),
            durable=False,
            persistence="none",
        )
        return {"applied": applied, "deferred": deferred}

    def restore_quarantined(self, data_id):
        with self._lock:
            old = self.get_idata(data_id)
            restored = []
            for replica in self.replicas.records_for(f"i:{data_id}"):
                if replica.state != "quarantined":
                    continue
                self.pruning.validate_quarantined_file(
                    data_id,
                    replica.replica_id,
                    replica.generation,
                    replica.content_hash,
                    replica.size,
                )
                path = self.pruning.restore_file(
                    data_id, replica.replica_id, replica.generation
                )
                try:
                    restored_replica = (
                        self.replicas.restore_quarantine(
                            replica.data_id,
                            replica.replica_id,
                            replica.generation,
                        )
                    )
                except Exception:
                    self.pruning.quarantine_file(
                        data_id,
                        replica.replica_id,
                        replica.generation,
                        path,
                    )
                    raise
                payload = Path(path).read_bytes()
                self._idata[data_id] = dataclasses.replace(
                    old,
                    serialized_bytes=payload,
                    durability="durable",
                    durable_path=str(path),
                )
                self._publish_replica(
                    f"i:{data_id}",
                    (
                        f"controller-idata-{data_id}-"
                        f"attempt-{old.attempt}"
                    ),
                    old.attempt,
                    "controller-memory",
                    old.content_hash,
                    len(payload),
                )
                self.pruning.set_data_state(
                    data_id, available=True, durable=True
                )
                audit = self.pruning.audit(
                    "restore-quarantine",
                    data_id,
                    "new-or-recovery-consumer",
                    self.replicas.revision,
                    restored_replica.replica_id,
                    restored_replica.generation,
                    path,
                )
                restored.append(audit.to_dict())
            if not restored:
                raise ValueError("IDataID has no quarantined replica")
            return restored

    def hard_delete_quarantined(
        self, graph_revision, state_revision, now=None
    ):
        with self._lock:
            plan = self.pruning.validate_revision(
                graph_revision, state_revision
            )
            records = {
                record.data_id: record for record in plan.records
            }
            deleted = []
            for data_id in self.pruning.pruner.graph.data_ids:
                for replica in self.replicas.records_for(
                    f"i:{data_id}"
                ):
                    if replica.state != "quarantined":
                        continue
                    proof = records[data_id]
                    if (
                        proof.decision not in ("prune", "absent")
                        or set(proof.reasons)
                        - {"no-accepted-replica"}
                    ):
                        raise ValueError(
                            "quarantine is no longer proven prunable"
                        )
                    revision = self.replicas.revision
                    self.replicas.validate_hard_delete(
                        replica.data_id,
                        replica.replica_id,
                        replica.generation,
                        revision,
                        now,
                    )
                    path = self.pruning.delete_file(
                        data_id,
                        replica.replica_id,
                        replica.generation,
                    )
                    pruned = self.replicas.hard_delete_quarantine(
                        replica.data_id,
                        replica.replica_id,
                        replica.generation,
                        revision,
                        now,
                    )
                    audit = self.pruning.audit(
                        "hard-delete-sharedfs",
                        data_id,
                        "proof-remains-valid-after-grace",
                        self.replicas.revision,
                        pruned.replica_id,
                        pruned.generation,
                        path,
                    )
                    deleted.append(audit.to_dict())
            return {
                "deleted": deleted,
                "plan": self.pruning.plan().to_dict(),
                "replica_revision": self.replicas.revision,
            }

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
                "edata_inline_records": sum(
                    record.serialized_bytes is not None
                    for record in self._edata.values()
                ),
                "edata_bulk_records": sum(
                    record.stable_path is not None
                    for record in self._edata.values()
                ),
                "edata_bulk_bytes": self._edata_bulk_bytes,
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
                    str(key): record.serialized_size
                    for key, record in sorted(self._edata.items())
                },
                "tasks": len(self._tasks),
                "idata": len(self._idata),
                "idata_bytes": self._idata_bytes,
                "idata_bytes_high_water": self._idata_bytes_high_water,
                "idata_capacity_bytes": self.max_idata_bytes,
                "idata_inline_object_capacity_bytes": (
                    self.max_inline_idata_bytes
                ),
                "idata_inline_records": sum(
                    value.serialized_bytes is not None
                    for value in self._idata.values()
                ),
                "idata_metadata_records": sum(
                    value.content_hash is not None
                    and value.serialized_bytes is None
                    for value in self._idata.values()
                ),
                "idata_metadata_publications": (
                    self._idata_metadata_publications
                ),
                "available_idata": sum(
                    bool(self.replicas.candidates(f"i:{value.data_id}"))
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
                "external_persistence_requests": sum(
                    job.get("mode") == "worker"
                    for job in self._persistence_jobs.values()
                ),
                "external_persistence_durable": sum(
                    job.get("mode") == "worker"
                    and job["state"] == "durable"
                    for job in self._persistence_jobs.values()
                ),
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
                "pruning": self.pruning.snapshot(),
            }
