# Copyright (C) 2026- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Phase 4 worker-side serialized-data inventory and preparation checks."""

import dataclasses
import os


@dataclasses.dataclass(frozen=True)
class StableDataSource:
    data_kind: str
    data_id: int
    source_kind: str
    locator: str


@dataclasses.dataclass(frozen=True)
class WorkerPreparationReport:
    task_id: int
    assignment: str
    required: tuple
    local_before: tuple
    missing: tuple
    stale: tuple
    resolved: tuple

    def audit_line(self):
        return f"DATAVINE_WORKER_DATA_AGENT PASS {self.assignment}"


class WorkerDataAgent:
    """Track qualified DataIDs already verified in one worker library process."""

    def __init__(self):
        self._inventory = set()

    @property
    def inventory(self):
        return frozenset(self._inventory)

    def seed_inventory(self, references):
        """Test hook for partial, complete, and stale inventory scenarios."""
        self._inventory.update(
            (reference.data_kind, reference.data_id)
            for reference in references
        )

    def prepare(self, controller, workflow, assignment):
        task_id, required = controller.parse_worker_assignment(assignment)
        expected = controller.required_data_references(task_id)
        if required != expected:
            raise ValueError(
                f"TaskID {task_id} worker assignment differs from Controller"
            )

        local_before = []
        stale = []
        missing = []
        resolved = []
        for reference in required:
            key = (reference.data_kind, reference.data_id)
            source = controller.resolve_stable_source(
                workflow, task_id, reference
            )
            available = self._source_is_available(controller, source)
            if key in self._inventory and available:
                local_before.append(reference)
                continue
            if key in self._inventory:
                stale.append(reference)
                self._inventory.remove(key)
            missing.append(reference)
            if not available:
                raise FileNotFoundError(
                    f"TaskID {task_id} has no available stable source for "
                    f"{reference.data_kind}{reference.data_id}: "
                    f"{source.locator}"
                )
            self._inventory.add(key)
            resolved.append(source)

        return WorkerPreparationReport(
            task_id=task_id,
            assignment=assignment,
            required=required,
            local_before=tuple(local_before),
            missing=tuple(missing),
            stale=tuple(stale),
            resolved=tuple(resolved),
        )

    @staticmethod
    def _source_is_available(controller, source):
        if source.source_kind == "controller-context":
            record = controller.edata.get(source.data_id)
            return (
                record is not None
                and isinstance(record.serialized_bytes, bytes)
            )
        if source.source_kind in {
            "legacy-input-file",
            "legacy-parent-output",
            "legacy-produced-file",
        }:
            return os.path.isfile(source.locator)
        raise ValueError(
            f"unsupported stable source kind {source.source_kind!r}"
        )

_WORKER_AGENTS = {}


def worker_data_agent_for(workflow):
    """Return one inventory per Workflow in the long-lived task-runner process."""
    return _WORKER_AGENTS.setdefault(workflow._workflow_id, WorkerDataAgent())
