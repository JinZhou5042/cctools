"""Thread-safe Controller-owned logical and serialized state."""

import collections
import contextlib
import dataclasses
import hashlib
from pathlib import Path
import threading

from ..models import EDataRecord
from .pruning import PruningAuthority
from .replicas import ReplicaDirectory
from .metadata import MetadataStore
from .edata_state import EDataStateMixin
from .idata_task_state import IDataTaskStateMixin
from .replica_state import ReplicaStateMixin
from .persistence_state import PersistenceStateMixin
from .pruning_state import PruningStateMixin
from .status_state import StatusStateMixin
from .stores import DenseIdStore

class ControllerState(
    EDataStateMixin,
    IDataTaskStateMixin,
    PersistenceStateMixin,
    PruningStateMixin,
    ReplicaStateMixin,
    StatusStateMixin,
):
    def __init__(
        self,
        max_edata_bytes=256 * 1024 * 1024,
        replica_directory=None,
        pruning_audit_capacity=10000,
        bulk_origin_root=None,
        max_idata_bytes=256 * 1024 * 1024,
        max_inline_idata_bytes=8 * 1024 * 1024,
        completed_lease_capacity=65536,
        completed_pruning_operation_capacity=1024,
        completed_pruning_operation_bytes=64 * 1024 * 1024,
        max_replicas=10_000_000,
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
        if int(completed_pruning_operation_capacity) < 1:
            raise ValueError(
                "completed pruning operation capacity must be positive"
            )
        if int(completed_lease_capacity) < 1:
            raise ValueError(
                "completed lease capacity must be positive"
            )
        if int(completed_pruning_operation_bytes) < 1:
            raise ValueError(
                "completed pruning operation byte capacity must be positive"
            )
        self.max_edata_bytes = int(max_edata_bytes)
        self.max_idata_bytes = int(max_idata_bytes)
        self.max_inline_idata_bytes = int(max_inline_idata_bytes)
        self.max_replicas = int(max_replicas)
        self.bulk_origin_root = (
            Path(bulk_origin_root).resolve()
            if bulk_origin_root is not None
            else None
        )
        if self.bulk_origin_root is not None:
            self.bulk_origin_root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._persistence_capacity = threading.Condition(self._lock)
        self._next_edata_id = 1
        self._edata = {}
        self._buckets = {}
        self._edata_bytes = 0
        self._edata_bulk_bytes = 0
        self._registrations = 0
        self._edata_fetches = {}
        self._next_idata_id = 1
        self._idata = DenseIdStore()
        self._idata_bytes = 0
        self._idata_bytes_high_water = 0
        self._idata_metadata_publications = 0
        self._tasks = DenseIdStore()
        self._task_depths = {}
        self._task_registration_order = {}
        self._next_task_registration_sequence = 1
        self._edata_consumers = {}
        self._publications = 0
        self._persistence = None
        self._metadata = None
        self._metadata_depth = 0
        self._metadata_dirty = {
            "edata": set(),
            "idata": set(),
            "task": set(),
            "task-state": set(),
            "data-state": set(),
        }
        self._restoring_metadata = False
        self._metadata_requires_restore = False
        self.native_journal_path = None
        self._persistence_failures = {}
        self._persistence_active = 0
        self._persistence_max_active = 0
        self._persistence_requests = 0
        self._persistence_sequence = 0
        self._persistence_jobs = {}
        self._persistence_active_ids = set()
        self._persistence_stale_completions = 0
        self._persistence_cleanup_failures = 0
        self.replicas = replica_directory or ReplicaDirectory(
            max_replicas=self.max_replicas,
            max_completed_leases=int(completed_lease_capacity)
        )
        self.pruning = PruningAuthority(pruning_audit_capacity)
        self._deferred_pruning = {}
        self._completed_pruning_operation_capacity = int(
            completed_pruning_operation_capacity
        )
        self._completed_pruning_operation_byte_capacity = int(
            completed_pruning_operation_bytes
        )
        self._completed_pruning_operations = collections.OrderedDict()
        self._completed_pruning_operation_bytes = 0
        self._completed_pruning_operation_bytes_high_water = 0
        self._pruning_continuation_idempotent = 0
        self._pruning_continuation_evictions = 0

    def _release_persistence_slot(self, request_id):
        self._persistence_active_ids.discard(str(request_id))
        self._persistence_active = len(self._persistence_active_ids)
        self._persistence_capacity.notify_all()

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
            metadata = MetadataStore(root)
            persistence = None
            try:
                persistence = PersistenceManager(
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
            except Exception:
                if persistence is not None:
                    persistence.stop()
                metadata.close()
                raise
            self.native_journal_path = str(
                persistence.root / "controller-native.wal"
            )
            self._persistence = persistence
            self._metadata = metadata
            self._metadata_requires_restore = metadata.has_records()

    @contextlib.contextmanager
    def _metadata_batch(self):
        if (
            self._metadata_depth == 0
            and self._metadata_requires_restore
            and not self._restoring_metadata
        ):
            raise RuntimeError("Controller metadata must be restored first")
        self._metadata_depth += 1
        try:
            yield
        finally:
            self._metadata_depth -= 1
            if self._metadata_depth == 0:
                self._flush_metadata_locked()

    def _mark_metadata(self, kind, values):
        if self._restoring_metadata:
            return
        self._metadata_dirty[kind].update(map(int, values))

    def _mark_idata_metadata(self, data_ids):
        data_ids = tuple(map(int, data_ids))
        self._mark_metadata("idata", data_ids)
        self._mark_metadata("data-state", data_ids)

    def _flush_metadata_locked(self):
        if self._metadata is None or self._restoring_metadata:
            return
        dirty = self._metadata_dirty
        if not any(dirty.values()):
            return
        records = []
        for data_id in dirty["edata"]:
            record = self._edata[data_id]
            records.append((
                "edata",
                data_id,
                None,
                {
                    "content_hash": record.content_hash,
                    "serialized_sha256": record.serialized_sha256,
                    "metadata": record.metadata,
                    "stable_path": record.stable_path,
                    "serialized_size": record.serialized_size,
                    "inline": record.serialized_bytes is not None,
                },
            ))
        records.extend(
            ("idata", data_id, None, self._idata[data_id])
            for data_id in dirty["idata"]
        )
        records.extend(
            (
                "task",
                task_id,
                self._task_registration_order[task_id],
                self._tasks[task_id],
            )
            for task_id in dirty["task"]
        )
        self._metadata.commit(
            records,
            (
                (task_id, self.pruning.pruner.task_states[task_id])
                for task_id in dirty["task-state"]
            ),
            (
                (data_id, self.pruning.pruner.data_states[data_id])
                for data_id in dirty["data-state"]
            ),
        )
        for values in dirty.values():
            values.clear()

    def restore_metadata(self, native_edata, native_replicas):
        with self._lock:
            if self._metadata is None:
                return
            if not self._metadata_requires_restore:
                return
            if self._edata or len(self._idata) or len(self._tasks):
                raise RuntimeError("metadata restore requires empty state")
            self._restoring_metadata = True
            reconciled = set()
            try:
                for data_id, value in self._metadata.load("edata"):
                    payload = None
                    stable_path = value["stable_path"]
                    if value["inline"]:
                        native = native_edata(data_id)
                        payload = native["payload"]
                        if (
                            native["content_hash"] != value["content_hash"]
                            or native["serialized_sha256"]
                            != value["serialized_sha256"]
                        ):
                            raise RuntimeError("native EData metadata mismatch")
                    record = EDataRecord(
                        data_id,
                        value["content_hash"],
                        value["serialized_sha256"],
                        value["metadata"],
                        payload,
                        stable_path,
                        value["serialized_size"],
                    )
                    self._edata[data_id] = record
                    self._buckets.setdefault(
                        (record.metadata, record.content_hash), []
                    ).append(data_id)
                    if payload is not None:
                        self._edata_bytes += len(payload)
                    else:
                        self._edata_bulk_bytes += record.serialized_size
                self._next_edata_id = max(self._edata, default=0) + 1
                for data_id, record in self._metadata.load("idata"):
                    durable_path = record.durable_path
                    if record.content_hash and record.attempt:
                        recovered_path = self._persistence.target_path(
                            record.data_id,
                            record.attempt,
                            record.content_hash,
                        )
                        if durable_path is None and recovered_path.is_file():
                            durable_path = str(recovered_path)
                    durable = (
                        durable_path is not None
                        and self._valid_durable_path(record, durable_path)
                    )
                    if durable and (
                        record.durability != "durable"
                        or record.durable_path != durable_path
                    ):
                        record = dataclasses.replace(
                            record,
                            durability="durable",
                            durable_path=durable_path,
                        )
                        reconciled.add(data_id)
                    elif record.durability == "durable" and not durable:
                        record = dataclasses.replace(
                            record, durability="volatile", durable_path=None
                        )
                        self._persistence_failures[data_id] = (
                            "durable recovery validation failed"
                        )
                        reconciled.add(data_id)
                    elif record.durability in ("queued", "writing"):
                        record = dataclasses.replace(
                            record, durability="volatile", durable_path=None
                        )
                        reconciled.add(data_id)
                    self._idata[data_id] = record
                    if record.serialized_bytes is not None:
                        self._idata_bytes += len(record.serialized_bytes)
                self._idata_bytes_high_water = self._idata_bytes
                self._next_idata_id = self._idata.allocated_slots + 1
                tasks = [record for _, record in self._metadata.load("task")]
                self.register_tasks(tasks)
                data_states = dict(self._metadata.load("data-state"))
                updates = []
                for data_id, record in self._idata.items():
                    state = data_states.get(data_id)
                    if state is None:
                        continue
                    durable = record.durability == "durable"
                    updates.append((
                        data_id,
                        {
                            "available": (
                                record.serialized_bytes is not None or durable
                            ),
                            "durable": durable,
                            "pinned": state.pinned,
                            "required_output": state.required_output,
                            "persistence": "none",
                        },
                    ))
                self.pruning.set_data_states(updates)
                for task_id, state in self._metadata.load("task-state"):
                    if state != "pending":
                        self.pruning.set_task_state(task_id, state)
                self._rebuild_persistent_replicas_locked()
                self._rebuild_native_replicas_locked(native_replicas)
            finally:
                self._restoring_metadata = False
            self._mark_idata_metadata(reconciled)
            self._flush_metadata_locked()
            self._metadata_requires_restore = False

    @staticmethod
    def _valid_durable_path(record, path):
        digest = hashlib.sha256()
        size = 0
        try:
            with Path(path).open("rb") as stream:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    digest.update(chunk)
        except OSError:
            return False
        return (
            size == record.serialized_size
            and digest.hexdigest() == record.content_hash
        )

    def _rebuild_persistent_replicas_locked(self):
        for record in self._edata.values():
            self._publish_replica(
                f"e:{record.data_id}",
                (
                    f"controller-edata-{record.data_id}"
                    if record.serialized_bytes is not None
                    else f"bulk-origin-edata-{record.data_id}"
                ),
                1,
                (
                    "controller-memory"
                    if record.serialized_bytes is not None
                    else "sharedfs"
                ),
                record.content_hash,
                record.serialized_size,
            )
        for record in self._idata.values():
            if record.serialized_bytes is not None:
                self._publish_replica(
                    f"i:{record.data_id}",
                    f"controller-idata-{record.data_id}-attempt-{record.attempt}",
                    record.attempt,
                    "controller-memory",
                    record.content_hash,
                    record.serialized_size,
                )
            if record.durability == "durable":
                self._publish_replica(
                    f"i:{record.data_id}",
                    f"sharedfs-idata-{record.data_id}-attempt-{record.attempt}",
                    record.attempt,
                    "sharedfs",
                    record.content_hash,
                    record.serialized_size,
                )

    def _rebuild_native_replicas_locked(self, native_replicas):
        available = set()
        for data_id, _ in self._idata.items():
            for record in native_replicas(f"i:{data_id}"):
                if record["state"] != "available":
                    continue
                self.replicas.join_worker(
                    record["worker_id"],
                    record["worker_epoch"],
                    record["source_endpoint"],
                )
                prepared = self.replicas.prepare_replica(
                    f"i:{data_id}",
                    record["replica_id"],
                    record["attempt"],
                    record["tier"],
                    record["content_hash"],
                    record["size"],
                    record["worker_id"],
                    record["worker_epoch"],
                    record["source_endpoint"],
                )
                self.replicas.commit_replica(
                    f"i:{data_id}",
                    record["replica_id"],
                    prepared.generation,
                    record["attempt"],
                    record["content_hash"],
                    record["size"],
                )
                available.add(data_id)
        self.pruning.set_data_states(
            (
                (data_id, {"available": True})
                for data_id in available
            )
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

    def stop(self):
        persistence = self._persistence
        if persistence is not None:
            persistence.stop()
            self._persistence = None
        with self._lock:
            self._flush_metadata_locked()
            if self._metadata is not None:
                self._metadata.close()
                self._metadata = None

    def snapshot(self):
        with self._lock:
            replica_snapshot = self.replicas.snapshot()
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
                "available_idata": replica_snapshot[
                    "available_idata"
                ],
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
                "metadata": (
                    self._metadata.snapshot()
                    if self._metadata is not None
                    else None
                ),
                "replica_directory": replica_snapshot,
                "pruning": self.pruning.snapshot(),
                "deferred_pruning": {
                    str(data_id): list(records)
                    for data_id, records in sorted(
                        self._deferred_pruning.items()
                    )
                },
                "completed_pruning_operation_tombstones": len(
                    self._completed_pruning_operations
                ),
                "completed_pruning_operation_capacity": (
                    self._completed_pruning_operation_capacity
                ),
                "completed_pruning_operation_bytes": (
                    self._completed_pruning_operation_bytes
                ),
                "completed_pruning_operation_byte_capacity": (
                    self._completed_pruning_operation_byte_capacity
                ),
                "completed_pruning_operation_bytes_high_water": (
                    self._completed_pruning_operation_bytes_high_water
                ),
                "pruning_continuation_idempotent": (
                    self._pruning_continuation_idempotent
                ),
                "pruning_continuation_evictions": (
                    self._pruning_continuation_evictions
                ),
            }
