"""Workflow value and task registration."""

import dataclasses
import os
from pathlib import Path
import time
import uuid

from ..models import EDataRecord, TaskRecord
from ..serialization import serialize
from ..workflow import OutputRef, iter_output_refs


class WorkflowRegistrar:
    """Register one validated workflow into the Data Controller."""

    def __init__(
        self,
        controller,
        bulk_origin_dir=None,
        bulk_threshold=8 * 1024 * 1024,
        task_batch_size=4096,
    ):
        self.controller = controller
        self.bulk_origin_dir = (
            Path(bulk_origin_dir).resolve()
            if bulk_origin_dir is not None
            else None
        )
        self.bulk_threshold = int(bulk_threshold)
        self.task_batch_size = int(task_batch_size)
        if self.bulk_threshold < 1:
            raise ValueError("bulk threshold must be positive")
        if self.task_batch_size < 1:
            raise ValueError("task batch size must be positive")
        if self.bulk_origin_dir is not None:
            self.bulk_origin_dir.mkdir(parents=True, exist_ok=True)

    def register(self, workflow, context):
        """Register a validated workflow and return its logical outputs."""

        registration_started = time.monotonic()
        tasks = workflow.tasks
        producer_slots = tuple(
            (task.task_id, output_index)
            for task in tasks
            for output_index in range(task.output_count)
        )
        allocated = iter(self.controller.allocate_idata_batch(producer_slots))
        idata_allocated = time.monotonic()
        for task in tasks:
            output_data_ids = tuple(
                next(allocated) for _ in range(task.output_count)
            )
            context.logical_output_slots[task.task_id] = output_data_ids
            context.logical_outputs[task.task_id] = output_data_ids[0]

        self._register_workflow_values(context, tasks)
        values_registered = time.monotonic()
        records = []
        record_build_seconds = 0.0
        task_registration_seconds = 0.0
        build_started = time.monotonic()
        for task in tasks:
            context.nested_idata_by_task[task.task_id] = set()
            positional = tuple(
                self._binding(context, task.task_id, value)
                for value in task.args
            )
            keyword = tuple(
                (
                    name,
                    self._binding(context, task.task_id, value),
                )
                for name, value in sorted(task.kwargs.items())
            )
            record = TaskRecord(
                task.task_id,
                self._register_value(context, task.function, "function"),
                positional,
                keyword,
                context.logical_output_slots[task.task_id],
                tuple(
                    sorted(
                        {
                            context.logical_output_slots[
                                reference.producer_task_id
                            ][reference.output_index]
                            for value in (
                                *task.args,
                                *task.kwargs.values(),
                            )
                            for reference in iter_output_refs(value)
                        }
                    )
                ),
            )
            context.task_records[task.task_id] = record
            records.append(record)
            if len(records) == self.task_batch_size:
                register_started = time.monotonic()
                record_build_seconds += register_started - build_started
                self.controller.register_tasks(records)
                registered = time.monotonic()
                task_registration_seconds += registered - register_started
                records.clear()
                build_started = registered

        register_started = time.monotonic()
        record_build_seconds += register_started - build_started
        if records:
            self.controller.register_tasks(records)
        tasks_registered = time.monotonic()
        task_registration_seconds += tasks_registered - register_started
        context.registration_timing = {
            "idata_allocation": idata_allocated - registration_started,
            "edata": values_registered - idata_allocated,
            "task_record_build": record_build_seconds,
            "task_registration": task_registration_seconds,
        }
        result = dict(context.logical_outputs)
        context.release_registration_caches()
        return result

    def _register_value(self, context, value, domain="value"):
        cache_key = (str(domain), id(value))
        cached = context.edata_by_object.get(cache_key)
        if cached is not None and cached[0] is value:
            return cached[1]
        metadata, payload = serialize(value)
        metadata = dataclasses.replace(metadata, domain=str(domain))
        context.serialization_count += 1
        if (
            self.bulk_origin_dir is not None
            and len(payload) >= self.bulk_threshold
        ):
            context.bulk_serialization_count += 1
            digest = EDataRecord.digest(metadata, payload)
            path = self.bulk_origin_dir / f"edata-{digest}.pkl"
            if not path.exists():
                temporary = self.bulk_origin_dir / (
                    f".edata-{digest}-{uuid.uuid4().hex}.tmp"
                )
                try:
                    with temporary.open("xb") as stream:
                        stream.write(payload)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, path)
                    os.chmod(path, 0o444)
                    directory = os.open(self.bulk_origin_dir, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                finally:
                    temporary.unlink(missing_ok=True)
            os.chmod(path, 0o444)
            result = self.controller.register_edata_origin(
                metadata, path, digest, len(payload)
            )
        else:
            result = self.controller.register_edata(metadata, payload)
        data_id = int(result["data_id"])
        context.edata_info[data_id] = result
        context.edata_by_object[cache_key] = (value, data_id)
        return data_id

    def _register_workflow_values(self, context, tasks):
        candidates = []
        seen = set()
        for task in tasks:
            values = ((task.function, "function"),)
            values += tuple(
                (
                    value,
                    "container" if tuple(iter_output_refs(value)) else "value",
                )
                for value in (*task.args, *task.kwargs.values())
                if not isinstance(value, OutputRef)
            )
            for value, domain in values:
                cache_key = (domain, id(value))
                if cache_key in seen:
                    continue
                seen.add(cache_key)
                cached = context.edata_by_object.get(cache_key)
                if cached is not None and cached[0] is value:
                    continue
                metadata, payload = serialize(value)
                metadata = dataclasses.replace(metadata, domain=str(domain))
                if (
                    self.bulk_origin_dir is not None
                    and len(payload) >= self.bulk_threshold
                ):
                    self._register_value(context, value, domain)
                    continue
                context.serialization_count += 1
                candidates.append((cache_key, value, metadata, payload))
        if not candidates:
            return
        results = self.controller.register_edata_batch(
            (metadata, payload)
            for _, _, metadata, payload in candidates
        )
        for (cache_key, value, _, payload), result in zip(
            candidates, results
        ):
            data_id = int(result["data_id"])
            context.edata_by_object[cache_key] = (value, data_id)
            context.edata_info[data_id] = result

    def _binding(self, context, task_id, value):
        if isinstance(value, OutputRef):
            return (
                "i",
                context.logical_output_slots[value.producer_task_id][
                    value.output_index
                ],
            )
        references = tuple(iter_output_refs(value))
        if references:
            context.nested_idata_by_task[task_id].update(
                context.logical_output_slots[reference.producer_task_id][
                    reference.output_index
                ]
                for reference in references
            )
            return ("c", self._register_value(context, value, "container"))
        return ("e", self._register_value(context, value))
