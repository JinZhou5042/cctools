"""Controller IData status and task-state transitions."""

import dataclasses
import hashlib


class StatusStateMixin:
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

    def idata_status_batch(self, data_ids):
        with self._lock:
            return tuple(self.idata_status(data_id) for data_id in data_ids)

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

    def set_task_states(self, task_ids, state):
        with self._lock:
            task_ids = tuple(int(task_id) for task_id in task_ids)
            for task_id in task_ids:
                self.get_task(task_id)
            return tuple(
                mutation.to_dict()
                for mutation in self.pruning.set_task_states(
                    task_ids, state
                )
            )

    def set_required_output(self, data_id, required=True):
        with self._lock:
            self.get_idata(data_id)
            return self.pruning.set_data_state(
                data_id, required_output=bool(required)
            ).to_dict()
