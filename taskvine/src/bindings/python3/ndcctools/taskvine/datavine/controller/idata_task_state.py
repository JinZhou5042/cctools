"""Controller IData publication and logical task state."""

import base64
import hashlib

from ..models import IDataRecord, TaskRecord


class IDataTaskStateMixin:
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

    def allocate_idata_batch(self, producer_slots):
        with self._lock:
            return tuple(
                self.allocate_idata(task_id, output_index)
                for task_id, output_index in producer_slots
            )

    def _validate_new_task(self, task):
        if not isinstance(task, TaskRecord):
            raise TypeError("task must be TaskRecord")
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
        normalized_inputs = tuple(sorted(set(task.input_data_ids)))
        if task.input_data_ids != normalized_inputs:
            raise ValueError(
                "TaskRecord IData dependencies must be unique and sorted"
            )
        if not direct_inputs <= set(task.input_data_ids):
            raise ValueError("TaskRecord omits direct IData dependency")

    def register_task(self, task):
        return self.register_tasks((task,))[0]

    def register_tasks(self, tasks):
        with self._lock:
            results = []
            new_tasks = {}
            for task in tasks:
                if not isinstance(task, TaskRecord):
                    raise TypeError("task must be TaskRecord")
                existing = self._tasks.get(task.task_id)
                if existing is None:
                    existing = new_tasks.get(task.task_id)
                if existing is not None:
                    if existing != task:
                        raise ValueError(
                            f"conflicting TaskID {task.task_id}"
                        )
                    results.append(existing)
                    continue
                self._validate_new_task(task)
                new_tasks[task.task_id] = task
                results.append(task)
            self.pruning.register_tasks(
                (
                    task.task_id,
                    task.input_data_ids,
                    task.output_data_ids,
                )
                for task in new_tasks.values()
            )
            self._tasks.update(new_tasks)
            return tuple(results)

    def _validate_binding(self, kind, data_id):
        if kind == "v":
            payload = base64.b64decode(data_id, validate=True)
            if len(payload) > 1024:
                raise ValueError(
                    "inline task value exceeds 1024 bytes"
                )
            return
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
            record, changed = self._publish_idata_locked(
                data_id, attempt, serialized_bytes
            )
            if changed:
                self.pruning.set_data_state(
                    data_id,
                    available=True,
                    durable=False,
                    persistence="none",
                )
            return record

    def _publish_idata_locked(self, data_id, attempt, serialized_bytes):
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
            return old, False
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
            f"controller-idata-{old.data_id}-attempt-{attempt}",
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
        return record, True

    def publish_idata_batch(self, publications):
        with self._lock:
            records = []
            changed_data_ids = []
            for data_id, attempt, payload in publications:
                if not isinstance(payload, bytes):
                    raise TypeError("serialized_bytes must be bytes")
                record, changed = self._publish_idata_locked(
                    data_id, attempt, payload
                )
                records.append(record)
                if changed:
                    changed_data_ids.append(record.data_id)
            self.pruning.set_data_states(
                (
                    (
                        data_id,
                        {
                            "available": True,
                            "durable": False,
                            "persistence": "none",
                        },
                    )
                    for data_id in changed_data_ids
                )
            )
            return tuple(records)

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
