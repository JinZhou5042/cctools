#!/usr/bin/env python3

from ndcctools.taskvine.datavine.scheduler.execution_state import (
    ExecutionState,
    PersistenceState,
    PruningState,
    PublicationState,
)


def main():
    first = PersistenceState()
    second = PersistenceState()
    assert not first.has_work()
    first.pending.append((1.0, 7, {"request_id": "request-1"}))
    first.required.add(7)
    first.retry_counts[(7, 1)] += 1
    first.worker_bytes += 1024
    assert first.has_work()
    assert first.retry_counts[(7, 1)] == 1
    assert first.worker_bytes == 1024
    assert not second.pending
    assert not second.required
    assert second.retry_counts[(7, 1)] == 0
    assert not second.has_work()

    first.pending.clear()
    assert not first.has_work()
    first.controller_pending[7] = 2.0
    assert first.has_work()
    first.controller_pending.clear()
    first.suspended_recovery[7] = {}
    assert first.has_work()

    pruning = PruningState()
    other_pruning = PruningState()
    assert not pruning.has_work()
    pruning.pending[3] = [7]
    assert pruning.has_work()
    pruning.pending.clear()
    pruning.active = {"frontier_task_id": 3}
    pruning.events.append({"frontier_task_id": 2})
    pruning.completions_while_active += 1
    assert pruning.has_work()
    assert not other_pruning.events
    assert not other_pruning.has_work()

    execution = ExecutionState(pending={1, 2})
    other_execution = ExecutionState()
    assert execution.has_work()
    execution.pending.clear()
    assert not execution.has_work()
    execution.running[10] = (1, 2)
    execution.completion_queue.append(1)
    execution.recovery_waves.append([1])
    assert execution.has_work()
    assert not other_execution.running
    assert not other_execution.completion_queue
    assert not other_execution.recovery_waves

    publication = PublicationState()
    other_publication = PublicationState()
    publication.triggered_tasks.add(1)
    publication.cancelled_physical_tasks[10] = {"task_id": 1}
    publication.failures.append({"task_id": 1})
    assert not other_publication.triggered_tasks
    assert not other_publication.cancelled_physical_tasks
    assert not other_publication.failures

    print("DataVine Scheduler execution state contract PASS")


if __name__ == "__main__":
    main()
