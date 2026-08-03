"""Typed mutable state for Scheduler execution subsystems."""

import collections
import dataclasses


@dataclasses.dataclass
class PersistenceState:
    """Mutable state for external and Controller persistence work."""

    pending: list = dataclasses.field(default_factory=list)
    running: dict = dataclasses.field(default_factory=dict)
    controller_pending: dict = dataclasses.field(default_factory=dict)
    required: set = dataclasses.field(default_factory=set)
    requested: set = dataclasses.field(default_factory=set)
    suspended_recovery: dict = dataclasses.field(default_factory=dict)
    retry_counts: dict = dataclasses.field(
        default_factory=lambda: collections.defaultdict(int)
    )
    tasks_completed: int = 0
    controller_tasks_completed: int = 0
    worker_bytes: int = 0
    controller_bytes: int = 0
    cancellations: int = 0
    failures: int = 0
    injected_failures_observed: int = 0
    retries: int = 0
    retry_delay_seconds: float = 0.0
    compute_completions_while_active: int = 0
    global_losses: int = 0
    loss_pruning_plans: list = dataclasses.field(default_factory=list)
    injected_external_failures: int = 0

    def has_work(self):
        return bool(
            self.pending
            or self.running
            or self.controller_pending
            or self.suspended_recovery
        )


@dataclasses.dataclass
class PruningState:
    """Mutable state for asynchronous frontier pruning."""

    events: list = dataclasses.field(default_factory=list)
    applied: set = dataclasses.field(default_factory=set)
    pending: dict = dataclasses.field(default_factory=dict)
    active: dict | None = None
    sharedfs_delete: dict | None = None
    completions_while_active: int = 0

    def has_work(self):
        return bool(self.pending or self.active)


@dataclasses.dataclass
class ExecutionState:
    """Mutable logical execution and recovery state."""

    pending: set = dataclasses.field(default_factory=set)
    running: dict = dataclasses.field(default_factory=dict)
    done: set = dataclasses.field(default_factory=set)
    completed_once: set = dataclasses.field(default_factory=set)
    completion_queue: collections.deque = dataclasses.field(
        default_factory=collections.deque
    )
    deferred_completed_states: list = dataclasses.field(
        default_factory=list
    )
    unbatchable: set = dataclasses.field(default_factory=set)
    physical_batch_metrics: list = dataclasses.field(default_factory=list)
    recovery_waves: list = dataclasses.field(default_factory=list)
    unavailable_input_recoveries: list = dataclasses.field(
        default_factory=list
    )
    worker_loss_events: list = dataclasses.field(default_factory=list)
    recovery_audit_data_ids: set = dataclasses.field(default_factory=set)
    physical_submissions: int = 0
    physical_task_build_seconds: float = 0.0
    physical_task_submit_seconds: float = 0.0
    batch_worker_seconds: float = 0.0
    recovery_reexecutions: int = 0
    local_idata_hits: int = 0
    loss_injected: bool = False
    worker_controller_retries: int = 0
    worker_loss_injections: int = 0

    def has_work(self):
        return bool(self.pending or self.running)


@dataclasses.dataclass
class PublicationState:
    """Mutable state for atomic multi-output publication faults."""

    failures: list = dataclasses.field(default_factory=list)
    triggered_tasks: set = dataclasses.field(default_factory=set)
    cancelled_physical_tasks: dict = dataclasses.field(default_factory=dict)
