"""Controller-owned physical replica, worker epoch, and source lease state."""

import dataclasses
import hashlib
import collections
import re
import secrets
import threading
import time


REPLICA_TIERS = frozenset(
    (
        "controller-memory",
        "worker-dram",
        "worker-disk",
        "sharedfs",
        "external",
    )
)
WORKER_TIERS = frozenset(("worker-dram", "worker-disk"))
REPLICA_STATES = frozenset(
    (
        "preparing",
        "available",
        "retiring",
        "invalid",
        "quarantined",
        "pruned",
    )
)
TIER_COST = {
    "worker-dram": 0,
    "worker-disk": 1,
    "controller-memory": 2,
    "sharedfs": 3,
    "external": 4,
}
DATA_KEY_PATTERN = re.compile(r"^[ei]:[1-9][0-9]*$")
TRANSFER_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


@dataclasses.dataclass(frozen=True)
class WorkerEpoch:
    worker_id: str
    epoch: int
    active: bool


@dataclasses.dataclass(frozen=True)
class ReplicaRecord:
    data_id: str
    replica_id: str
    generation: int
    attempt: int
    tier: str
    content_hash: str
    size: int
    state: str
    worker_id: str | None = None
    worker_epoch: int | None = None
    active_leases: int = 0
    quarantine_until: float | None = None

    def source_dict(self):
        return {
            "data_id": self.data_id,
            "replica_id": self.replica_id,
            "generation": self.generation,
            "attempt": self.attempt,
            "tier": self.tier,
            "content_hash": self.content_hash,
            "size": self.size,
            "state": self.state,
            "load": self.active_leases,
            "worker_id": self.worker_id,
            "worker_epoch": self.worker_epoch,
        }


@dataclasses.dataclass(frozen=True)
class SourceLease:
    lease_id: str
    data_id: str
    replica_id: str
    generation: int
    destination_worker_id: str
    destination_worker_epoch: int
    active: bool = True
    success: bool | None = None


class ReplicaDirectory:
    """Fail-closed physical directory; logical lineage lives elsewhere."""

    def __init__(
        self,
        max_replicas=10_000_000,
        max_workers=10000,
        max_active_leases=1024,
        max_completed_leases=65536,
    ):
        capacities = {
            "max_replicas": int(max_replicas),
            "max_workers": int(max_workers),
            "max_active_leases": int(max_active_leases),
            "max_completed_leases": int(max_completed_leases),
        }
        if any(value < 1 for value in capacities.values()):
            raise ValueError("replica directory capacities must be positive")
        self._lock = threading.RLock()
        self._workers = {}
        self._replicas = {}
        self._replica_keys_by_data = {}
        self._active_leases = {}
        self._completed_leases = collections.OrderedDict()
        self._max_replicas = capacities["max_replicas"]
        self._max_workers = capacities["max_workers"]
        self._max_active_leases = capacities["max_active_leases"]
        self._max_completed_leases = capacities[
            "max_completed_leases"
        ]
        self._latest_attempt = {}
        self._revision = 0
        self._stale_rejections = 0
        self._lease_high_water = 0
        self._replica_high_water = 0
        self._peer_transfer_acquires = 0
        self._peer_transfer_idempotent = 0
        self._peer_transfer_releases = 0
        self._peer_transfer_failures = 0
        self._source_selection_requests = 0
        self._source_selection_misses = 0
        self._worker_loss_lease_expirations = 0

    @property
    def revision(self):
        with self._lock:
            return self._revision

    def _changed(self):
        self._revision += 1

    def _reject_stale(self, message):
        self._stale_rejections += 1
        raise ValueError(message)

    def join_worker(self, worker_id, epoch):
        worker_id = str(worker_id)
        epoch = int(epoch)
        if not worker_id or epoch < 1:
            raise ValueError("invalid worker identity or epoch")
        with self._lock:
            old = self._workers.get(worker_id)
            if old is not None:
                if old.epoch == epoch and old.active:
                    return old
                if epoch <= old.epoch:
                    self._reject_stale("stale worker epoch")
                self._invalidate_worker_replicas(old.worker_id, old.epoch)
                self._expire_worker_leases(old.worker_id, old.epoch)
            elif len(self._workers) >= self._max_workers:
                raise RuntimeError("worker directory capacity exceeded")
            record = WorkerEpoch(worker_id, epoch, True)
            self._workers[worker_id] = record
            self._changed()
            return record

    def claim_worker(self, worker_id):
        """Return or allocate the Controller-owned worker incarnation."""
        worker_id = str(worker_id)
        if not worker_id:
            raise ValueError("invalid worker identity")
        with self._lock:
            old = self._workers.get(worker_id)
            if old is None:
                epoch = 1
            elif old.active:
                return old
            else:
                epoch = old.epoch + 1
            return self.join_worker(worker_id, epoch)

    def disconnect_worker(self, worker_id, epoch):
        worker_id = str(worker_id)
        epoch = int(epoch)
        with self._lock:
            old = self._workers.get(worker_id)
            if old is None or old.epoch != epoch:
                self._reject_stale("stale worker disconnect")
            if not old.active:
                return old
            self._workers[worker_id] = dataclasses.replace(
                old, active=False
            )
            self._invalidate_worker_replicas(worker_id, epoch)
            self._expire_worker_leases(worker_id, epoch)
            self._changed()
            return self._workers[worker_id]

    def reconcile_workers(self, active_worker_ids):
        """Invalidate active incarnations absent from Scheduler truth."""
        active_worker_ids = {
            str(worker_id) for worker_id in active_worker_ids
        }
        with self._lock:
            disconnected = []
            affected_data_ids = set()
            for worker_id, worker in tuple(self._workers.items()):
                if worker.active and worker_id not in active_worker_ids:
                    self._workers[worker_id] = dataclasses.replace(
                        worker, active=False
                    )
                    affected_data_ids.update(
                        self._invalidate_worker_replicas(
                            worker.worker_id, worker.epoch
                        )
                    )
                    self._expire_worker_leases(
                        worker.worker_id, worker.epoch
                    )
                    disconnected.append(self._workers[worker_id])
            if disconnected:
                self._changed()
            return tuple(disconnected), tuple(sorted(affected_data_ids))

    def _invalidate_worker_replicas(self, worker_id, epoch):
        affected_data_ids = set()
        for key, record in tuple(self._replicas.items()):
            if (
                record.worker_id == worker_id
                and record.worker_epoch == epoch
                and record.state in ("preparing", "available")
            ):
                state = (
                    "retiring" if record.active_leases else "invalid"
                )
                self._replicas[key] = dataclasses.replace(
                    record, state=state
                )
                affected_data_ids.add(record.data_id)
        return affected_data_ids

    def _expire_worker_leases(self, worker_id, epoch):
        """Fail transfers owned by a dead source or destination epoch."""
        worker_id = str(worker_id)
        epoch = int(epoch)
        expired = []
        for lease_id, lease in tuple(self._active_leases.items()):
            record = self._replicas[
                (lease.data_id, lease.replica_id)
            ]
            source_matches = (
                record.worker_id == worker_id
                and record.worker_epoch == epoch
            )
            destination_matches = (
                lease.destination_worker_id == worker_id
                and lease.destination_worker_epoch == epoch
            )
            if source_matches or destination_matches:
                self._complete_source_lease(lease_id, False)
                expired.append(lease_id)
        self._worker_loss_lease_expirations += len(expired)
        return tuple(expired)

    def _validate_hash(self, content_hash):
        content_hash = str(content_hash)
        if len(content_hash) != 64:
            raise ValueError("content hash must be SHA-256 hex")
        try:
            bytes.fromhex(content_hash)
        except ValueError as exc:
            raise ValueError("content hash must be SHA-256 hex") from exc
        return content_hash

    def _normalize_data_id(self, data_id):
        data_id = str(data_id)
        if DATA_KEY_PATTERN.fullmatch(data_id) is None:
            raise ValueError("DataID must be qualified as e:<id> or i:<id>")
        return data_id

    def _validate_worker(self, worker_id, worker_epoch):
        current = self._workers.get(str(worker_id))
        if (
            current is None
            or not current.active
            or current.epoch != int(worker_epoch)
        ):
            self._reject_stale("replica report from stale worker epoch")

    def advance_attempt(self, data_id, attempt):
        data_id = self._normalize_data_id(data_id)
        attempt = int(attempt)
        if attempt < 1:
            raise ValueError("invalid attempt")
        with self._lock:
            old = self._latest_attempt.get(data_id, 0)
            if attempt < old:
                self._reject_stale("stale logical attempt")
            if attempt == old:
                return old
            self._latest_attempt[data_id] = attempt
            for key in tuple(self._replica_keys_by_data.get(data_id, ())):
                record = self._replicas[key]
                if (
                    record.data_id == data_id
                    and record.attempt < attempt
                    and record.tier not in ("sharedfs", "external")
                    and record.state
                    in ("preparing", "available", "retiring")
                ):
                    self._replicas[key] = dataclasses.replace(
                        record,
                        state=(
                            "retiring"
                            if record.active_leases
                            else "invalid"
                        ),
                    )
            self._changed()
            return attempt

    def prepare_replica(
        self,
        data_id,
        replica_id,
        attempt,
        tier,
        content_hash,
        size,
        worker_id=None,
        worker_epoch=None,
    ):
        data_id = self._normalize_data_id(data_id)
        replica_id = str(replica_id)
        attempt = int(attempt)
        tier = str(tier)
        size = int(size)
        content_hash = self._validate_hash(content_hash)
        if not replica_id or size < 0:
            raise ValueError("invalid replica identity or size")
        if tier not in REPLICA_TIERS:
            raise ValueError(f"invalid replica tier {tier!r}")
        with self._lock:
            if tier in WORKER_TIERS:
                if worker_id is None or worker_epoch is None:
                    raise ValueError("worker replica requires worker epoch")
                self._validate_worker(worker_id, worker_epoch)
            elif worker_id is not None or worker_epoch is not None:
                raise ValueError("non-worker replica has worker identity")
            self.advance_attempt(data_id, attempt)
            key = (data_id, replica_id)
            old = self._replicas.get(key)
            identity = (
                attempt,
                tier,
                content_hash,
                size,
                worker_id,
                worker_epoch,
            )
            if old is not None and old.state in (
                "preparing", "available"
            ):
                old_identity = (
                    old.attempt,
                    old.tier,
                    old.content_hash,
                    old.size,
                    old.worker_id,
                    old.worker_epoch,
                )
                if old_identity == identity:
                    return old
                raise ValueError("conflicting live replica declaration")
            if old is None and len(self._replicas) >= self._max_replicas:
                raise RuntimeError("replica directory capacity exceeded")
            generation = 1 if old is None else old.generation + 1
            record = ReplicaRecord(
                data_id=data_id,
                replica_id=replica_id,
                generation=generation,
                attempt=attempt,
                tier=tier,
                content_hash=content_hash,
                size=size,
                state="preparing",
                worker_id=(
                    str(worker_id) if worker_id is not None else None
                ),
                worker_epoch=(
                    int(worker_epoch)
                    if worker_epoch is not None
                    else None
                ),
            )
            self._replicas[key] = record
            self._replica_keys_by_data.setdefault(data_id, set()).add(key)
            self._replica_high_water = max(
                self._replica_high_water, len(self._replicas)
            )
            self._changed()
            return record

    def commit_replica(
        self,
        data_id,
        replica_id,
        generation,
        attempt,
        content_hash,
        size,
    ):
        data_id = self._normalize_data_id(data_id)
        key = (data_id, str(replica_id))
        generation = int(generation)
        attempt = int(attempt)
        content_hash = self._validate_hash(content_hash)
        size = int(size)
        with self._lock:
            record = self._replicas.get(key)
            if record is None:
                raise KeyError("unknown replica")
            if record.generation != generation:
                self._reject_stale("stale replica generation")
            if attempt < self._latest_attempt.get(data_id, 0):
                self._reject_stale("stale replica completion attempt")
            if record.state == "available":
                if (
                    record.attempt == attempt
                    and record.content_hash == content_hash
                    and record.size == size
                ):
                    return record
                raise ValueError("conflicting duplicate replica commit")
            if record.state != "preparing":
                raise ValueError(
                    f"cannot commit replica in state {record.state}"
                )
            if record.tier in WORKER_TIERS:
                self._validate_worker(
                    record.worker_id, record.worker_epoch
                )
            if (
                record.attempt != attempt
                or record.content_hash != content_hash
                or record.size != size
            ):
                raise ValueError("replica content metadata mismatch")
            record = dataclasses.replace(record, state="available")
            self._replicas[key] = record
            self._changed()
            return record

    def report_bytes(
        self,
        data_id,
        replica_id,
        attempt,
        tier,
        payload,
        worker_id=None,
        worker_epoch=None,
    ):
        payload = bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        record = self.prepare_replica(
            data_id,
            replica_id,
            attempt,
            tier,
            digest,
            len(payload),
            worker_id,
            worker_epoch,
        )
        return self.commit_replica(
            data_id,
            replica_id,
            record.generation,
            attempt,
            digest,
            len(payload),
        )

    def candidates(self, data_id):
        data_id = self._normalize_data_id(data_id)
        with self._lock:
            records = []
            latest_attempt = self._latest_attempt.get(data_id, 0)
            for key in self._replica_keys_by_data.get(data_id, ()):
                record = self._replicas[key]
                if (
                    record.data_id != data_id
                    or record.state != "available"
                    or record.attempt != latest_attempt
                ):
                    continue
                if record.tier in WORKER_TIERS:
                    worker = self._workers.get(record.worker_id)
                    if (
                        worker is None
                        or not worker.active
                        or worker.epoch != record.worker_epoch
                    ):
                        continue
                records.append(record)
            return tuple(
                sorted(
                    records,
                    key=lambda value: (
                        value.active_leases,
                        TIER_COST[value.tier],
                        value.replica_id,
                    ),
                )
            )

    def _select_worker_source(
        self, data_id, destination_worker_id, excluded_worker_ids=()
    ):
        data_id = self._normalize_data_id(data_id)
        destination_worker_id = str(destination_worker_id)
        excluded = {
            str(worker_id) for worker_id in excluded_worker_ids
        }
        with self._lock:
            self._source_selection_requests += 1
            destination = self._workers.get(destination_worker_id)
            if destination is None or not destination.active:
                self._source_selection_misses += 1
                self._reject_stale(
                    "destination worker is unavailable"
                )
            latest_attempt = self._latest_attempt.get(data_id, 0)
            candidates = []
            for key in self._replica_keys_by_data.get(data_id, ()):
                record = self._replicas[key]
                worker = self._workers.get(record.worker_id)
                if (
                    record.state != "available"
                    or record.attempt != latest_attempt
                    or record.tier not in WORKER_TIERS
                    or record.worker_id == destination_worker_id
                    or record.worker_id in excluded
                    or worker is None
                    or not worker.active
                    or worker.epoch != record.worker_epoch
                ):
                    continue
                candidates.append(record)
            if not candidates:
                self._source_selection_misses += 1
                raise KeyError("no available worker source")
            return min(
                candidates,
                key=lambda record: (
                    record.active_leases,
                    TIER_COST[record.tier],
                    record.replica_id,
                ),
            )

    def resolve_worker_source(
        self,
        data_id,
        destination_worker_id,
        transfer_id,
        excluded_worker_ids=(),
    ):
        transfer_id = str(transfer_id)
        if (
            TRANSFER_ID_PATTERN.fullmatch(transfer_id) is None
            or not transfer_id.startswith("taskvine:")
        ):
            raise ValueError("invalid transfer identity")
        with self._lock:
            existing = self._active_leases.get(transfer_id)
            if existing is not None:
                if (
                    existing.data_id == self._normalize_data_id(data_id)
                    and existing.destination_worker_id
                    == str(destination_worker_id)
                ):
                    source = self._replicas[
                        (existing.data_id, existing.replica_id)
                    ]
                    self._peer_transfer_idempotent += 1
                    return source, existing
                raise ValueError("conflicting transfer identity")
            if transfer_id in self._completed_leases:
                raise ValueError("transfer identity already completed")
            source = self._select_worker_source(
                data_id,
                destination_worker_id,
                excluded_worker_ids,
            )
            destination = self._workers[str(destination_worker_id)]
            lease = self._acquire_record(
                source,
                destination.worker_id,
                destination.epoch,
                transfer_id,
            )
            self._peer_transfer_acquires += 1
            return self._replicas[(source.data_id, source.replica_id)], lease

    def acquire_source(
        self,
        data_id,
        replica_id,
        generation,
        destination_worker_id,
        destination_worker_epoch,
    ):
        data_id = self._normalize_data_id(data_id)
        key = (data_id, str(replica_id))
        with self._lock:
            self._validate_worker(
                destination_worker_id, destination_worker_epoch
            )
            record = self._replicas.get(key)
            if (
                record is None
                or record.generation != int(generation)
                or record.state != "available"
            ):
                self._reject_stale(
                    "selected source disappeared before transfer"
                )
            if record.tier in WORKER_TIERS:
                self._validate_worker(
                    record.worker_id, record.worker_epoch
                )
            lease_id = secrets.token_hex(16)
            while (
                lease_id in self._active_leases
                or lease_id in self._completed_leases
            ):
                lease_id = secrets.token_hex(16)
            return self._acquire_record(
                record,
                destination_worker_id,
                destination_worker_epoch,
                lease_id,
            )

    def _acquire_record(
        self,
        record,
        destination_worker_id,
        destination_worker_epoch,
        lease_id,
    ):
        if len(self._active_leases) >= self._max_active_leases:
            raise RuntimeError("source lease admission capacity exceeded")
        key = (record.data_id, record.replica_id)
        lease = SourceLease(
            lease_id=str(lease_id),
            data_id=record.data_id,
            replica_id=record.replica_id,
            generation=record.generation,
            destination_worker_id=str(destination_worker_id),
            destination_worker_epoch=int(destination_worker_epoch),
        )
        self._active_leases[lease.lease_id] = lease
        self._replicas[key] = dataclasses.replace(
            record, active_leases=record.active_leases + 1
        )
        self._lease_high_water = max(
            self._lease_high_water, len(self._active_leases)
        )
        self._changed()
        return lease

    def release_source(self, lease_id, success):
        lease_id = str(lease_id)
        with self._lock:
            lease = self._active_leases.get(lease_id)
            if lease is None:
                lease = self._completed_leases.get(lease_id)
                if lease is None:
                    raise KeyError("unknown source lease")
                if lease.success == bool(success):
                    return lease
                raise ValueError("conflicting duplicate lease release")
            return self._complete_source_lease(lease_id, bool(success))

    def _complete_source_lease(self, lease_id, success):
        lease = self._active_leases[lease_id]
        key = (lease.data_id, lease.replica_id)
        record = self._replicas[key]
        if record.generation != lease.generation:
            self._reject_stale("lease references stale generation")
        leases = record.active_leases - 1
        if leases < 0:
            raise RuntimeError("source lease count underflow")
        state = record.state
        if state == "retiring" and leases == 0:
            state = "invalid"
        self._replicas[key] = dataclasses.replace(
            record, active_leases=leases, state=state
        )
        lease = dataclasses.replace(
            lease, active=False, success=bool(success)
        )
        del self._active_leases[lease_id]
        self._completed_leases[lease_id] = lease
        while len(self._completed_leases) > self._max_completed_leases:
            self._completed_leases.popitem(last=False)
        if lease_id.startswith("taskvine:"):
            self._peer_transfer_releases += 1
            if not success:
                self._peer_transfer_failures += 1
        self._changed()
        return lease

    def invalidate_replica(self, data_id, replica_id, generation):
        key = (self._normalize_data_id(data_id), str(replica_id))
        with self._lock:
            record = self._replicas.get(key)
            if record is None:
                raise KeyError("unknown replica")
            if record.generation != int(generation):
                self._reject_stale("stale invalidation generation")
            if record.state in ("invalid", "pruned"):
                return record
            if record.state == "quarantined":
                raise ValueError("quarantined replica needs prune or restore")
            state = "retiring" if record.active_leases else "invalid"
            record = dataclasses.replace(record, state=state)
            self._replicas[key] = record
            self._changed()
            return record

    def cancel_invalidation(self, data_id, replica_id, generation):
        """Restore a not-yet-deleted replica after prune proof invalidation."""
        key = (self._normalize_data_id(data_id), str(replica_id))
        with self._lock:
            record = self._replicas.get(key)
            if record is None:
                raise KeyError("unknown replica")
            if record.generation != int(generation):
                self._reject_stale("stale invalidation cancellation")
            if record.state == "available":
                return record
            if record.state not in ("retiring", "invalid"):
                raise ValueError(
                    f"cannot cancel invalidation in state {record.state}"
                )
            record = dataclasses.replace(record, state="available")
            self._replicas[key] = record
            self._changed()
            return record

    def invalidate_worker_replica(
        self,
        data_id,
        replica_id,
        generation,
        worker_id,
        worker_epoch,
    ):
        key = (self._normalize_data_id(data_id), str(replica_id))
        with self._lock:
            self._validate_worker(worker_id, worker_epoch)
            record = self._replicas.get(key)
            if record is None:
                raise KeyError("unknown replica")
            if (
                record.worker_id != str(worker_id)
                or record.worker_epoch != int(worker_epoch)
            ):
                raise ValueError("worker cannot invalidate foreign replica")
            return self.invalidate_replica(
                data_id, replica_id, generation
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
        data_id = self._normalize_data_id(data_id)
        key = (data_id, str(replica_id))
        attempt = int(attempt)
        content_hash = self._validate_hash(content_hash)
        size = int(size)
        with self._lock:
            self._validate_worker(worker_id, worker_epoch)
            record = self._replicas.get(key)
            if record is None:
                raise KeyError("unknown observed replica")
            identity = (
                record.attempt,
                record.content_hash,
                record.size,
                record.worker_id,
                record.worker_epoch,
            )
            observed = (
                attempt,
                content_hash,
                size,
                str(worker_id),
                int(worker_epoch),
            )
            if identity != observed:
                self._reject_stale(
                    "observed replica identity is no longer current"
                )
            return self.invalidate_replica(
                data_id, replica_id, record.generation
            )

    def confirm_worker_pruned(
        self, data_id, replica_id, generation
    ):
        key = (self._normalize_data_id(data_id), str(replica_id))
        with self._lock:
            record = self._replicas.get(key)
            if record is None:
                raise KeyError("unknown replica")
            if record.generation != int(generation):
                self._reject_stale("stale prune confirmation")
            if record.tier not in WORKER_TIERS:
                raise ValueError("prune confirmation requires worker replica")
            if record.active_leases:
                raise ValueError("active read prevents prune confirmation")
            if record.state == "pruned":
                return record
            if record.state != "invalid":
                raise ValueError(
                    f"cannot confirm prune in state {record.state}"
                )
            record = dataclasses.replace(record, state="pruned")
            self._replicas[key] = record
            self._changed()
            return record

    def quarantine(
        self,
        data_id,
        replica_id,
        generation,
        grace_seconds,
        expected_revision,
        now=None,
    ):
        key = (self._normalize_data_id(data_id), str(replica_id))
        grace_seconds = float(grace_seconds)
        now = time.time() if now is None else float(now)
        if grace_seconds < 0:
            raise ValueError("negative quarantine grace period")
        with self._lock:
            if int(expected_revision) != self._revision:
                self._reject_stale("pruning proof revision changed")
            record = self._replicas.get(key)
            if record is None:
                raise KeyError("unknown replica")
            if (
                record.generation != int(generation)
                or record.state != "available"
                or record.tier != "sharedfs"
                or record.active_leases
            ):
                raise ValueError("replica is not safe to quarantine")
            record = dataclasses.replace(
                record,
                state="quarantined",
                quarantine_until=now + grace_seconds,
            )
            self._replicas[key] = record
            self._changed()
            return record

    def restore_quarantine(self, data_id, replica_id, generation):
        key = (self._normalize_data_id(data_id), str(replica_id))
        with self._lock:
            record = self._replicas.get(key)
            if (
                record is None
                or record.generation != int(generation)
                or record.state != "quarantined"
            ):
                raise ValueError("replica is not quarantined")
            record = dataclasses.replace(
                record, state="available", quarantine_until=None
            )
            self._replicas[key] = record
            self._changed()
            return record

    def hard_delete_quarantine(
        self,
        data_id,
        replica_id,
        generation,
        expected_revision,
        now=None,
    ):
        with self._lock:
            record = self.validate_hard_delete(
                data_id,
                replica_id,
                generation,
                expected_revision,
                now,
            )
            record = dataclasses.replace(
                record, state="pruned", quarantine_until=None
            )
            key = (record.data_id, record.replica_id)
            self._replicas[key] = record
            self._changed()
            return record

    def validate_hard_delete(
        self,
        data_id,
        replica_id,
        generation,
        expected_revision,
        now=None,
    ):
        key = (self._normalize_data_id(data_id), str(replica_id))
        now = time.time() if now is None else float(now)
        with self._lock:
            if int(expected_revision) != self._revision:
                self._reject_stale("quarantine proof revision changed")
            record = self._replicas.get(key)
            if (
                record is None
                or record.generation != int(generation)
                or record.state != "quarantined"
                or record.active_leases
                or record.quarantine_until is None
                or now < record.quarantine_until
            ):
                raise ValueError("quarantine cannot be hard deleted")
            return record

    def globally_available(self, data_id):
        return bool(self.candidates(data_id))

    def records_for(self, data_id):
        data_id = self._normalize_data_id(data_id)
        with self._lock:
            return tuple(
                sorted(
                    (
                        self._replicas[key]
                        for key in self._replica_keys_by_data.get(
                            data_id, ()
                        )
                    ),
                    key=lambda record: (
                        record.replica_id,
                        record.generation,
                    ),
                )
            )

    def forget_data(self, data_id, expected_revision):
        """Forget terminal physical history after logical pruning."""
        data_id = self._normalize_data_id(data_id)
        with self._lock:
            if int(expected_revision) != self._revision:
                self._reject_stale("cleanup proof revision changed")
            keys = list(self._replica_keys_by_data.get(data_id, ()))
            if any(
                self._replicas[key].state not in ("invalid", "pruned")
                or self._replicas[key].active_leases
                for key in keys
            ):
                raise ValueError("live replica prevents data cleanup")
            for key in keys:
                del self._replicas[key]
            self._replica_keys_by_data.pop(data_id, None)
            self._latest_attempt.pop(data_id, None)
            if keys:
                self._changed()
            return len(keys)

    def forget_worker(self, worker_id, expected_epoch):
        """Forget an inactive epoch once all of its replicas are cleaned."""
        worker_id = str(worker_id)
        expected_epoch = int(expected_epoch)
        with self._lock:
            worker = self._workers.get(worker_id)
            if (
                worker is None
                or worker.active
                or worker.epoch != expected_epoch
            ):
                raise ValueError("worker epoch is not forgettable")
            if any(
                record.worker_id == worker_id
                for record in self._replicas.values()
            ):
                raise ValueError("worker replicas prevent epoch cleanup")
            del self._workers[worker_id]
            self._changed()
            return worker

    def get_replica(self, data_id, replica_id):
        with self._lock:
            try:
                return self._replicas[
                    (self._normalize_data_id(data_id), str(replica_id))
                ]
            except KeyError:
                raise KeyError("unknown replica") from None

    def snapshot(self):
        with self._lock:
            available_data_ids = {
                record.data_id
                for record in self._replicas.values()
                if (
                    record.state == "available"
                    and record.attempt
                    == self._latest_attempt.get(record.data_id, 0)
                )
            }
            states = {
                state: sum(
                    record.state == state
                    for record in self._replicas.values()
                )
                for state in sorted(REPLICA_STATES)
            }
            return {
                "revision": self._revision,
                "workers": len(self._workers),
                "active_workers": sum(
                    worker.active for worker in self._workers.values()
                ),
                "replicas": len(self._replicas),
                "available_data": len(available_data_ids),
                "available_edata": sum(
                    data_id.startswith("e:")
                    for data_id in available_data_ids
                ),
                "available_idata": sum(
                    data_id.startswith("i:")
                    for data_id in available_data_ids
                ),
                "replica_states": states,
                "replica_high_water": self._replica_high_water,
                "replica_capacity": self._max_replicas,
                "active_leases": len(self._active_leases),
                "active_lease_capacity": self._max_active_leases,
                "completed_lease_tombstones": len(
                    self._completed_leases
                ),
                "completed_lease_capacity": self._max_completed_leases,
                "worker_capacity": self._max_workers,
                "lease_high_water": self._lease_high_water,
                "stale_rejections": self._stale_rejections,
                "peer_transfer_acquires": (
                    self._peer_transfer_acquires
                ),
                "peer_transfer_idempotent": (
                    self._peer_transfer_idempotent
                ),
                "peer_transfer_releases": (
                    self._peer_transfer_releases
                ),
                "peer_transfer_failures": (
                    self._peer_transfer_failures
                ),
                "source_selection_requests": (
                    self._source_selection_requests
                ),
                "source_selection_misses": (
                    self._source_selection_misses
                ),
                "worker_loss_lease_expirations": (
                    self._worker_loss_lease_expirations
                ),
            }
