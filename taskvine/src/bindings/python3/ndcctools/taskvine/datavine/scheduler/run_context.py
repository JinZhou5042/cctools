"""Mutable state owned by one Scheduler workflow run."""

import dataclasses


@dataclasses.dataclass
class WorkflowRunContext:
    logical_outputs: dict = dataclasses.field(default_factory=dict)
    logical_output_slots: dict = dataclasses.field(default_factory=dict)
    task_records: dict = dataclasses.field(default_factory=dict)
    edata_by_object: dict = dataclasses.field(default_factory=dict)
    edata_info: dict = dataclasses.field(default_factory=dict)
    nested_idata_by_task: dict = dataclasses.field(default_factory=dict)
    attempts: dict = dataclasses.field(default_factory=dict)
    registration_timing: dict = dataclasses.field(default_factory=dict)
    serialization_count: int = 0
    bulk_serialization_count: int = 0

    def release_registration_caches(self):
        """Drop object caches that are not needed after registration."""

        self.edata_by_object.clear()
