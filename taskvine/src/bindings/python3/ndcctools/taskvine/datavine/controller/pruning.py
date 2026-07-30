"""Controller-owned lineage proof and physical pruning audit support."""

import collections
import dataclasses
import hashlib
import os
from pathlib import Path

from ..recovery import IncrementalPruner, LineageGraph


@dataclasses.dataclass(frozen=True)
class PruningAudit:
    sequence: int
    action: str
    data_id: int
    reason: str
    graph_revision: int
    state_revision: int
    replica_revision: int
    replica_id: str | None = None
    generation: int | None = None
    path: str | None = None

    def to_dict(self):
        return dataclasses.asdict(self)


class PruningAuthority:
    """Own lineage proof state and filesystem quarantine bookkeeping."""

    def __init__(self, audit_capacity=10000):
        audit_capacity = int(audit_capacity)
        if audit_capacity < 1:
            raise ValueError("pruning audit capacity must be positive")
        self.pruner = IncrementalPruner(LineageGraph())
        self._audit_capacity = audit_capacity
        self._audits = collections.deque(maxlen=audit_capacity)
        self._audit_sequence = 0
        self._persistence_root = None
        self._quarantine_root = None
        self._quarantined_paths = {}
        self._quarantine_high_water = 0

    @property
    def graph_revision(self):
        return self.pruner.graph.revision

    @property
    def state_revision(self):
        return self.pruner.state_revision

    def configure_filesystem(self, persistence_root):
        root = Path(persistence_root).resolve()
        quarantine = root / ".datavine-quarantine"
        quarantine.mkdir(parents=True, exist_ok=True)
        self._persistence_root = root
        self._quarantine_root = quarantine

    def register_task(self, task_id, inputs, outputs):
        return self.pruner.add_task(task_id, inputs, outputs)

    def set_task_state(self, task_id, state):
        return self.pruner.set_task_state(task_id, state)

    def set_data_state(self, data_id, **changes):
        return self.pruner.set_data_state(data_id, **changes)

    def plan(self):
        return self.pruner.assert_matches_reference()

    def validate_revision(self, graph_revision, state_revision):
        if (
            int(graph_revision) != self.graph_revision
            or int(state_revision) != self.state_revision
        ):
            raise ValueError("pruning proof revision changed")
        return self.plan()

    def audit(
        self,
        action,
        data_id,
        reason,
        replica_revision,
        replica_id=None,
        generation=None,
        path=None,
    ):
        self._audit_sequence += 1
        record = PruningAudit(
            sequence=self._audit_sequence,
            action=str(action),
            data_id=int(data_id),
            reason=str(reason),
            graph_revision=self.graph_revision,
            state_revision=self.state_revision,
            replica_revision=int(replica_revision),
            replica_id=(
                str(replica_id) if replica_id is not None else None
            ),
            generation=(
                int(generation) if generation is not None else None
            ),
            path=str(path) if path is not None else None,
        )
        self._audits.append(record)
        return record

    def _validated_durable_path(self, path):
        if self._persistence_root is None:
            raise RuntimeError("pruning filesystem is not configured")
        path = Path(path).resolve()
        if path.parent != self._persistence_root:
            raise ValueError("durable path is outside persistence root")
        return path

    def quarantine_file(self, data_id, replica_id, generation, path):
        source = self._validated_durable_path(path)
        if not source.is_file():
            raise FileNotFoundError(source)
        key = (int(data_id), str(replica_id), int(generation))
        old = self._quarantined_paths.get(key)
        if old is not None:
            return old[1]
        target = self._quarantine_root / (
            f"i-{int(data_id)}-g-{int(generation)}-{source.name}"
        )
        if target.exists():
            raise FileExistsError(target)
        os.replace(source, target)
        self._fsync_directory(source.parent)
        self._fsync_directory(target.parent)
        self._quarantined_paths[key] = (source, target)
        self._quarantine_high_water = max(
            self._quarantine_high_water,
            len(self._quarantined_paths),
        )
        return target

    def restore_file(self, data_id, replica_id, generation):
        key = (int(data_id), str(replica_id), int(generation))
        try:
            source, quarantined = self._quarantined_paths[key]
        except KeyError:
            raise KeyError("unknown quarantined path") from None
        if source.exists():
            raise FileExistsError(source)
        os.replace(quarantined, source)
        self._fsync_directory(quarantined.parent)
        self._fsync_directory(source.parent)
        del self._quarantined_paths[key]
        return source

    def validate_quarantined_file(
        self,
        data_id,
        replica_id,
        generation,
        content_hash,
        size,
    ):
        key = (int(data_id), str(replica_id), int(generation))
        try:
            _, quarantined = self._quarantined_paths[key]
        except KeyError:
            raise KeyError("unknown quarantined path") from None
        payload = quarantined.read_bytes()
        if (
            len(payload) != int(size)
            or hashlib.sha256(payload).hexdigest() != str(content_hash)
        ):
            raise IOError("quarantined replica checksum mismatch")
        return quarantined

    def delete_file(self, data_id, replica_id, generation):
        key = (int(data_id), str(replica_id), int(generation))
        try:
            _, quarantined = self._quarantined_paths[key]
        except KeyError:
            raise KeyError("unknown quarantined path") from None
        quarantined.unlink()
        self._fsync_directory(quarantined.parent)
        del self._quarantined_paths[key]
        return quarantined

    @staticmethod
    def _fsync_directory(path):
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def snapshot(self):
        return {
            "graph_revision": self.graph_revision,
            "state_revision": self.state_revision,
            "audit_records": len(self._audits),
            "audit_capacity": self._audit_capacity,
            "audit_sequence": self._audit_sequence,
            "quarantined_paths": len(self._quarantined_paths),
            "quarantine_high_water": self._quarantine_high_water,
            "audits": [record.to_dict() for record in self._audits],
        }
