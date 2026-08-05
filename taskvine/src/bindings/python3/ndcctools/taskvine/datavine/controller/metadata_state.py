"""Durable Controller metadata synchronization and recovery."""

import contextlib
import dataclasses
import hashlib
from pathlib import Path

from ..models import EDataRecord


class MetadataStateMixin:
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
        if not self._restoring_metadata:
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
                    "origin": (
                        "inline" if record.serialized_bytes is not None
                        else "native" if record.native else "stable"
                    ),
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
            if self._metadata is None or not self._metadata_requires_restore:
                return
            if self._edata or len(self._idata) or len(self._tasks):
                raise RuntimeError("metadata restore requires empty state")
            self._restoring_metadata = True
            reconciled = set()
            try:
                self._restore_edata(native_edata)
                reconciled.update(self._restore_idata())
                self._restore_tasks_and_pruning()
                self._rebuild_persistent_replicas_locked()
                self._rebuild_native_replicas_locked(native_replicas)
            finally:
                self._restoring_metadata = False
            self._mark_idata_metadata(reconciled)
            self._flush_metadata_locked()
            self._metadata_requires_restore = False

    def _restore_edata(self, native_edata):
        for data_id, value in self._metadata.load("edata"):
            payload = None
            if value["origin"] != "stable":
                native = native_edata(data_id)
                if value["origin"] == "inline":
                    payload = native["payload"]
                if (
                    native["content_hash"] != value["content_hash"]
                    or native["serialized_sha256"]
                    != value["serialized_sha256"]
                    or native["serialized_size"]
                    != value["serialized_size"]
                ):
                    raise RuntimeError("native EData metadata mismatch")
            record = EDataRecord(
                data_id,
                value["content_hash"],
                value["serialized_sha256"],
                value["metadata"],
                payload,
                value["stable_path"],
                value["serialized_size"],
                value["origin"] == "native",
            )
            self._edata[data_id] = record
            self._buckets.setdefault(
                (record.metadata, record.content_hash), []
            ).append(data_id)
            if payload is not None or record.native:
                self._edata_bytes += (
                    record.serialized_size
                    if record.native else len(payload)
                )
            else:
                self._edata_bulk_bytes += record.serialized_size
        self._next_edata_id = max(self._edata, default=0) + 1

    def _restore_idata(self):
        reconciled = set()
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
        return reconciled

    def _restore_tasks_and_pruning(self):
        self.register_tasks(
            record for _, record in self._metadata.load("task")
        )
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

    @staticmethod
    def _valid_durable_path(record, path):
        digest = hashlib.sha256()
        size = 0
        try:
            with Path(path).open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
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
                    if record.serialized_bytes is not None or record.native
                    else f"bulk-origin-edata-{record.data_id}"
                ),
                1,
                (
                    "controller-memory"
                    if record.serialized_bytes is not None or record.native
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
            (data_id, {"available": True}) for data_id in available
        )
