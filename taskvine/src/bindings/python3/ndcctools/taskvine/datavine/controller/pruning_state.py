"""Controller pruning, quarantine, and hard-delete state."""

import copy
import dataclasses
import json
from pathlib import Path
import re


PRUNING_OPERATION_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$"
)


class PruningStateMixin:
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
                if data_id in self._deferred_pruning:
                    for item in self._deferred_pruning[data_id]:
                        replica = self.replicas.get_replica(
                            f"i:{data_id}", item["replica_id"]
                        )
                        deferred.append(
                            {
                                **item,
                                "active_leases": (
                                    replica.active_leases
                                ),
                            }
                        )
                    continue
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

    def continue_deferred_pruning(self, operation_id, data_ids=None):
        """Resolve lease-deferred pruning against the newest proof."""
        operation_id = str(operation_id)
        if PRUNING_OPERATION_ID_PATTERN.fullmatch(operation_id) is None:
            raise ValueError("invalid pruning continuation identity")
        selection_key = (
            None
            if data_ids is None
            else tuple(sorted({int(data_id) for data_id in data_ids}))
        )
        with self._lock:
            completed = self._completed_pruning_operations.get(
                operation_id
            )
            if completed is not None:
                if completed["selection"] != selection_key:
                    raise ValueError(
                        "conflicting pruning continuation identity"
                    )
                self._pruning_continuation_idempotent += 1
                return copy.deepcopy(completed["result"])
            selected = (
                set(self._deferred_pruning)
                if selection_key is None
                else set(selection_key)
            )
            unknown = selected - set(self._deferred_pruning)
            if unknown:
                raise ValueError(
                    "IDataIDs have no deferred pruning: "
                    f"{sorted(unknown)}"
                )
            estimated_records = sum(
                len(self.replicas.records_for(f"i:{data_id}"))
                for data_id in selected
            )
            estimated_result_bytes = 4096 + 16384 * (
                len(selected) + estimated_records
            )
            if (
                estimated_result_bytes
                > self._completed_pruning_operation_byte_capacity
            ):
                raise RuntimeError(
                    "pruning continuation response exceeds "
                    "terminal byte capacity"
                )
            applied = []
            deferred = []
            cancelled = []
            for data_id in sorted(selected):
                pending = self._deferred_pruning[data_id]
                current_plan = self.pruning.plan()
                if data_id not in set(current_plan.prunable):
                    for item in pending:
                        restored = self.replicas.cancel_invalidation(
                            f"i:{data_id}",
                            item["replica_id"],
                            item["generation"],
                        )
                        audit = self.pruning.audit(
                            "cancel-retiring-prune",
                            data_id,
                            "pruning-proof-invalidated",
                            self.replicas.revision,
                            restored.replica_id,
                            restored.generation,
                        )
                        cancelled.append(audit.to_dict())
                    del self._deferred_pruning[data_id]
                    self.pruning.set_data_state(
                        data_id,
                        available=self.replicas.globally_available(
                            f"i:{data_id}"
                        ),
                    )
                    continue
                proof = self._pruning_record(current_plan, data_id)
                waiting = False
                for item in pending:
                    replica = self.replicas.get_replica(
                        f"i:{data_id}", item["replica_id"]
                    )
                    if replica.generation != item["generation"]:
                        raise ValueError(
                            "deferred pruning generation changed"
                        )
                    if replica.active_leases:
                        waiting = True
                        deferred.append(
                            {
                                **item,
                                "active_leases": (
                                    replica.active_leases
                                ),
                            }
                        )
                    elif replica.state != "invalid":
                        raise ValueError(
                            "lease-deferred replica did not become invalid"
                        )
                if waiting:
                    continue
                for item in pending:
                    self.replicas.cancel_invalidation(
                        f"i:{data_id}",
                        item["replica_id"],
                        item["generation"],
                    )
                del self._deferred_pruning[data_id]
                result = self._prune_idata_locked(
                    data_id, proof, 0, None
                )
                if result["deferred"]:
                    raise RuntimeError(
                        "deferred pruning reacquired a source lease "
                        "while Controller lock was held"
                    )
                applied.extend(result["applied"])
            result = {
                "operation_id": operation_id,
                "applied": applied,
                "deferred": deferred,
                "cancelled": cancelled,
                "replica_revision": self.replicas.revision,
            }
            result_bytes = len(
                json.dumps(
                    result, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
            )
            if result_bytes > estimated_result_bytes:
                raise RuntimeError(
                    "pruning continuation response exceeded "
                    "admitted bound"
                )
            self._completed_pruning_operations[operation_id] = {
                "selection": selection_key,
                "result": copy.deepcopy(result),
                "bytes": result_bytes,
            }
            self._completed_pruning_operation_bytes += result_bytes
            while (
                len(self._completed_pruning_operations)
                > self._completed_pruning_operation_capacity
                or self._completed_pruning_operation_bytes
                > self._completed_pruning_operation_byte_capacity
            ):
                _, evicted = (
                    self._completed_pruning_operations.popitem(
                        last=False
                    )
                )
                self._completed_pruning_operation_bytes -= evicted[
                    "bytes"
                ]
                self._pruning_continuation_evictions += 1
            self._completed_pruning_operation_bytes_high_water = max(
                self._completed_pruning_operation_bytes_high_water,
                self._completed_pruning_operation_bytes,
            )
            return result

    def _prune_idata_locked(
        self, data_id, proof, grace_seconds, now
    ):
        old = self.get_idata(data_id)
        applied = []
        deferred = []
        available = tuple(
            replica
            for replica in self.replicas.records_for(f"i:{data_id}")
            if replica.state == "available"
        )
        if any(replica.active_leases for replica in available):
            pending = []
            for replica in available:
                if not replica.active_leases:
                    continue
                retiring = self.replicas.invalidate_replica(
                    replica.data_id,
                    replica.replica_id,
                    replica.generation,
                )
                item = {
                    "data_id": data_id,
                    "replica_id": retiring.replica_id,
                    "generation": retiring.generation,
                    "action": "retiring-active-read",
                }
                pending.append(item)
                deferred.append(
                    {
                        **item,
                        "active_leases": retiring.active_leases,
                    }
                )
            self._deferred_pruning[data_id] = pending
            return {"applied": applied, "deferred": deferred}
        sharedfs_id = (
            f"sharedfs-idata-{old.data_id}-attempt-{old.attempt}"
        )
        for replica in self.replicas.records_for(f"i:{data_id}"):
            if replica.state != "available":
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
