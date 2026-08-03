"""Thread-safe Controller-owned logical and serialized state."""

import collections
from pathlib import Path
import threading

from .pruning import PruningAuthority
from .replicas import ReplicaDirectory
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
        self._edata_consumers = {}
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
        self.replicas = replica_directory or ReplicaDirectory(
            max_replicas=int(max_replicas),
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

    def stop(self):
        persistence = self._persistence
        if persistence is not None:
            persistence.stop()
            self._persistence = None

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
