"""Event-driven logical-task readiness and cache planning."""

import heapq
from dataclasses import dataclass


@dataclass(frozen=True)
class CachePlan:
    """Cache demand and safe retention limits for one workflow run."""

    task_inputs: dict
    remaining_uses: dict
    known_sizes: dict
    max_task_items: int
    max_known_input_bytes: int
    retention_items: int | None
    retention_bytes: int | None


def build_cache_plan(
    task_ids,
    task_record,
    nested_idata_by_task,
    logical_output_slots,
    size_for_key,
    retention_items=None,
    retention_bytes=None,
    admission_items=None,
    admission_bytes=None,
):
    """Build cache-use accounting and validate admission capacity."""

    task_inputs = {}
    remaining_uses = {}
    for task_id in sorted(task_ids):
        record = task_record(task_id)
        keys = {f"e:{record.function_data_id}"}
        keys.update(
            f"{'e' if kind == 'c' else kind}:{data_id}"
            for kind, data_id in record.positional
            if kind in ("e", "c", "i")
        )
        keys.update(
            f"{'e' if kind == 'c' else kind}:{data_id}"
            for _, (kind, data_id) in record.keyword
            if kind in ("e", "c", "i")
        )
        keys.update(
            f"i:{data_id}"
            for data_id in nested_idata_by_task.get(task_id, ())
        )
        task_inputs[task_id] = keys
        for key in keys:
            remaining_uses[key] = remaining_uses.get(key, 0) + 1

    max_task_items = max(
        (
            len(keys) + len(logical_output_slots[task_id])
            for task_id, keys in task_inputs.items()
        ),
        default=0,
    )
    if admission_items is not None and int(admission_items) < max_task_items:
        raise ValueError(
            "worker disk cache admission capacity "
            f"{admission_items} cannot fit the largest task working set "
            f"of {max_task_items} items"
        )

    known_sizes = {
        key: max(0, int(size_for_key(key) or 0))
        for key in remaining_uses
    }
    max_known_input_bytes = max(
        (
            sum(known_sizes[key] for key in keys)
            for keys in task_inputs.values()
        ),
        default=0,
    )
    if (
        admission_bytes is not None
        and int(admission_bytes) < max_known_input_bytes
    ):
        raise ValueError(
            "worker disk cache admission capacity "
            f"{admission_bytes} bytes cannot fit the largest known task "
            f"input working set of {max_known_input_bytes} bytes"
        )

    if admission_items is not None:
        headroom = max(0, int(admission_items) - max_task_items)
        if retention_items is None or int(retention_items) > headroom:
            retention_items = headroom
    if admission_bytes is not None:
        headroom = max(0, int(admission_bytes) - max_known_input_bytes)
        if retention_bytes is None or int(retention_bytes) > headroom:
            retention_bytes = headroom

    return CachePlan(
        task_inputs=task_inputs,
        remaining_uses=remaining_uses,
        known_sizes=known_sizes,
        max_task_items=max_task_items,
        max_known_input_bytes=max_known_input_bytes,
        retention_items=retention_items,
        retention_bytes=retention_bytes,
    )


class ReadyQueue:
    """Dependency-counter ready queue with O(V+E) workflow progression."""

    def __init__(self, dependencies, dependents, pending, done=()):
        self.dependencies = dependencies
        self.dependents = dependents
        self._remaining = {}
        self._heap = []
        self._queued = set()
        self.rebuild(pending, done)

    def rebuild(self, pending, done):
        """Rebuild after the rare multi-task recovery rollback."""

        done = set(done)
        self._remaining = {
            task_id: len(parent_ids - done)
            for task_id, parent_ids in self.dependencies.items()
        }
        self._heap.clear()
        self._queued.clear()
        for task_id in pending:
            self.mark_pending(task_id)

    def mark_pending(self, task_id):
        if self._remaining[task_id] or task_id in self._queued:
            return
        heapq.heappush(self._heap, task_id)
        self._queued.add(task_id)

    def mark_done(self, task_id, pending):
        for child_id in self.dependents[task_id]:
            remaining = self._remaining[child_id]
            if remaining <= 0:
                continue
            remaining -= 1
            self._remaining[child_id] = remaining
            if remaining == 0 and child_id in pending:
                self.mark_pending(child_id)

    def take(self, pending, eligible, limit=None):
        """Take currently eligible tasks while retaining temporary blocks."""

        if limit is not None and int(limit) < 0:
            raise ValueError("ready-task limit cannot be negative")
        ready = []
        blocked = []
        while self._heap and (limit is None or len(ready) < int(limit)):
            task_id = heapq.heappop(self._heap)
            self._queued.discard(task_id)
            if task_id not in pending or self._remaining[task_id]:
                continue
            if eligible(task_id):
                ready.append(task_id)
            else:
                blocked.append(task_id)
        for task_id in blocked:
            self.mark_pending(task_id)
        return tuple(ready)
