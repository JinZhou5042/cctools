# Copyright (C) 2026- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Phase 3 authoritative logical Data Controller."""

import dataclasses
from types import MappingProxyType

from .data_identity import DataReference
from .worker_data_agent import StableDataSource


@dataclasses.dataclass(frozen=True)
class ControllerTaskPlan:
    task_id: int
    workflow_key: object
    callable_edata_id: int
    input_bindings: tuple
    output_bindings: tuple
    parent_task_ids: tuple
    input_file_data_ids: tuple
    output_file_data_ids: tuple


@dataclasses.dataclass(frozen=True)
class LegacyMountExpectation:
    task_id: int
    parent_inputs: int
    extra_inputs: int
    extra_outputs: int


class DataController:
    """Own immutable identity/lineage and audit the legacy physical adapter."""

    def __init__(
        self,
        *,
        edata,
        idata,
        tasks,
        workflow_key_to_task_id,
        input_file_data_ids,
        output_file_data_ids,
        edata_availability,
        idata_state,
        comparison,
    ):
        self.edata = MappingProxyType(dict(edata))
        self.idata = MappingProxyType(dict(idata))
        self.tasks = MappingProxyType(dict(tasks))
        self.workflow_key_to_task_id = MappingProxyType(
            dict(workflow_key_to_task_id)
        )
        self.input_file_data_ids = MappingProxyType(
            dict(input_file_data_ids)
        )
        self.output_file_data_ids = MappingProxyType(
            dict(output_file_data_ids)
        )
        self.edata_availability = MappingProxyType(
            dict(edata_availability)
        )
        self.idata_state = MappingProxyType(dict(idata_state))
        self._comparison = _freeze_json_value(comparison)
        self._materialization_audits = {}
        self._worker_preparation_audits = {}

    @classmethod
    def from_workflow(cls, workflow):
        identity = workflow.indexed_data_identity
        shadow = workflow.shadow_data_graph
        if identity is None or shadow is None:
            raise ValueError(
                "data controller requires indexed identity and shadow graph"
            )
        identity.validate()

        tasks = {}
        for task_id, binding in identity.task_bindings.items():
            source_refs = []
            for input_binding in binding.inputs:
                source_refs.append(
                    (input_binding.source_kind, input_binding.data_id)
                )
                source_refs.extend(
                    (reference.data_kind, reference.data_id)
                    for reference in input_binding.references
                )

            parent_task_ids = {
                identity.idata[data_id].producer_task_id
                for data_kind, data_id in source_refs
                if data_kind == "idata"
            }
            input_file_data_ids = {
                data_id
                for data_kind, data_id in source_refs
                if (
                    data_kind == "edata"
                    and identity.edata.get(data_id).metadata.serializer
                    == "raw-file"
                )
                or (
                    data_kind == "idata"
                    and identity.idata[data_id].slot_kind == "file"
                )
            }
            output_file_data_ids = {
                output.data_id
                for output in binding.outputs
                if identity.idata[output.data_id].slot_kind == "file"
            }
            tasks[task_id] = ControllerTaskPlan(
                task_id=task_id,
                workflow_key=binding.workflow_key,
                callable_edata_id=binding.callable_edata_id,
                input_bindings=tuple(binding.inputs),
                output_bindings=tuple(binding.outputs),
                parent_task_ids=tuple(sorted(parent_task_ids)),
                input_file_data_ids=tuple(sorted(input_file_data_ids)),
                output_file_data_ids=tuple(sorted(output_file_data_ids)),
            )

        controller = cls(
            edata=identity.edata.records,
            idata=identity.idata,
            tasks=tasks,
            workflow_key_to_task_id=identity.task_ids,
            input_file_data_ids=identity.input_file_data_ids,
            output_file_data_ids=identity.output_file_data_ids,
            edata_availability={
                data_id: node.availability
                for data_id, node in shadow.edata.items()
            },
            idata_state={
                data_id: node.state
                for data_id, node in shadow.idata.items()
            },
            comparison=shadow.comparison_report(),
        )
        controller.validate()
        return controller

    def validate(self):
        task_ids = set(self.tasks)
        if set(self.workflow_key_to_task_id.values()) != task_ids:
            raise ValueError("Controller workflow key mapping differs from tasks")
        if any(
            plan.task_id != task_id
            for task_id, plan in self.tasks.items()
        ):
            raise ValueError("Controller task plan key mismatch")
        if any(
            parent_id not in task_ids
            for plan in self.tasks.values()
            for parent_id in plan.parent_task_ids
        ):
            raise ValueError("Controller plan references unknown parent TaskID")
        if any(
            plan.callable_edata_id not in self.edata
            for plan in self.tasks.values()
        ):
            raise ValueError("Controller plan references unknown EDataID")
        if set(self.edata_availability) != set(self.edata) or any(
            state != "controller"
            for state in self.edata_availability.values()
        ):
            raise ValueError("Controller EData availability is invalid")
        if set(self.idata_state) != set(self.idata) or any(
            state != "unproduced" for state in self.idata_state.values()
        ):
            raise ValueError("Controller IData state is invalid")
        if self.comparison_report()["mismatches"]:
            raise ValueError("Controller received mismatched shadow graph")
        return True

    def task_id_for(self, workflow_key):
        try:
            return self.workflow_key_to_task_id[workflow_key]
        except KeyError:
            raise KeyError(f"unknown workflow key {workflow_key!r}") from None

    def materialization_plan(self, task_id):
        try:
            return self.tasks[task_id]
        except KeyError:
            raise KeyError(f"unknown TaskID {task_id}") from None

    def required_data_references(self, task_id):
        """Return the compact, qualified inputs a worker must prepare."""
        plan = self.materialization_plan(task_id)
        references = {
            ("edata", plan.callable_edata_id): DataReference(
                data_kind="edata", data_id=plan.callable_edata_id
            )
        }
        for binding in plan.input_bindings:
            source_kind = (
                "idata" if binding.source_kind == "idata" else "edata"
            )
            key = (source_kind, binding.data_id)
            references[key] = DataReference(
                data_kind=source_kind, data_id=binding.data_id
            )
            for reference in binding.references:
                key = (reference.data_kind, reference.data_id)
                references[key] = DataReference(
                    data_kind=reference.data_kind,
                    data_id=reference.data_id,
                )
        return tuple(
            references[key]
            for key in sorted(
                references,
                key=lambda item: (item[0] != "edata", item[1]),
            )
        )

    def worker_assignment(self, task_id):
        encoded = ",".join(
            f"{reference.data_kind[0]}{reference.data_id}"
            for reference in self.required_data_references(task_id)
        )
        return f"T{task_id}|{encoded}"

    def parse_worker_assignment(self, assignment):
        if not isinstance(assignment, str) or "|" not in assignment:
            raise ValueError("invalid worker data assignment")
        task_token, encoded = assignment.split("|", 1)
        if not task_token.startswith("T") or not task_token[1:].isdigit():
            raise ValueError("invalid worker assignment TaskID")
        task_id = int(task_token[1:])
        self.materialization_plan(task_id)
        references = []
        if encoded:
            for token in encoded.split(","):
                if (
                    len(token) < 2
                    or token[0] not in {"e", "i"}
                    or not token[1:].isdigit()
                ):
                    raise ValueError("invalid worker assignment DataID")
                references.append(
                    DataReference(
                        data_kind=(
                            "edata" if token[0] == "e" else "idata"
                        ),
                        data_id=int(token[1:]),
                    )
                )
        return task_id, tuple(references)

    def resolve_stable_source(self, workflow, task_id, reference):
        """Resolve a required DataID to the source supplied by legacy mounts."""
        required = {
            (item.data_kind, item.data_id)
            for item in self.required_data_references(task_id)
        }
        key = (reference.data_kind, reference.data_id)
        if key not in required:
            raise ValueError(
                f"TaskID {task_id} does not require "
                f"{reference.data_kind}{reference.data_id}"
            )

        if reference.data_kind == "edata":
            record = self.edata.get(reference.data_id)
            if record is None:
                raise KeyError(f"unknown EDataID {reference.data_id}")
            if record.metadata.serializer != "raw-file":
                return StableDataSource(
                    "edata",
                    reference.data_id,
                    "controller-context",
                    f"edata:{reference.data_id}",
                )
            file_ids = [
                file_id
                for file_id, data_id in self.input_file_data_ids.items()
                if data_id == reference.data_id
            ]
            if len(file_ids) != 1:
                raise ValueError(
                    f"EDataID {reference.data_id} has no unique input file"
                )
            return StableDataSource(
                "edata",
                reference.data_id,
                "legacy-input-file",
                workflow.file_input_path(file_ids[0]),
            )

        if reference.data_kind != "idata":
            raise ValueError(f"unknown data kind {reference.data_kind!r}")
        record = self.idata.get(reference.data_id)
        if record is None:
            raise KeyError(f"unknown IDataID {reference.data_id}")
        producer_plan = self.materialization_plan(record.producer_task_id)
        if record.slot_kind == "return":
            locator = workflow.outfile_remote_name[
                producer_plan.workflow_key
            ]
            source_kind = "legacy-parent-output"
        elif record.slot_kind == "file":
            file_ids = [
                file_id
                for file_id, data_id in self.output_file_data_ids.items()
                if data_id == reference.data_id
            ]
            if len(file_ids) != 1:
                raise ValueError(
                    f"IDataID {reference.data_id} has no unique output file"
                )
            locator = workflow.file_input_path(file_ids[0])
            source_kind = "legacy-produced-file"
        else:
            raise ValueError(
                f"IDataID {reference.data_id} has unsupported slot kind"
            )
        if not locator:
            raise ValueError(
                f"IDataID {reference.data_id} legacy source is unresolved"
            )
        return StableDataSource(
            "idata", reference.data_id, source_kind, locator
        )

    def legacy_mount_expectation(self, workflow, workflow_key):
        """Fail if legacy topology/file bindings disagree with the plan."""
        task_id = self.task_id_for(workflow_key)
        plan = self.materialization_plan(task_id)

        legacy_parent_ids = {
            self.task_id_for(parent_key)
            for parent_key in workflow.parents_of.get(workflow_key, ())
        }
        if legacy_parent_ids != set(plan.parent_task_ids):
            raise ValueError(
                f"TaskID {task_id} legacy parent bindings differ from Controller"
            )

        legacy_input_file_ids = {
            file_id
            for file_id, consumers in workflow.file_consumers.items()
            if workflow_key in consumers
        }
        try:
            legacy_input_data_ids = {
                (
                    self.input_file_data_ids[file_id]
                    if file_id in self.input_file_data_ids
                    else self.output_file_data_ids[file_id]
                )
                for file_id in legacy_input_file_ids
            }
        except KeyError as exc:
            raise ValueError(
                f"TaskID {task_id} legacy input file has no Controller DataID"
            ) from exc
        if legacy_input_data_ids != set(plan.input_file_data_ids):
            raise ValueError(
                f"TaskID {task_id} legacy file inputs differ from Controller"
            )

        legacy_output_file_ids = set(
            workflow.output_files_by_task.get(workflow_key, {}).values()
        )
        try:
            legacy_output_data_ids = {
                self.output_file_data_ids[file_id]
                for file_id in legacy_output_file_ids
            }
        except KeyError as exc:
            raise ValueError(
                f"TaskID {task_id} legacy output file has no Controller IDataID"
            ) from exc
        if legacy_output_data_ids != set(plan.output_file_data_ids):
            raise ValueError(
                f"TaskID {task_id} legacy file outputs differ from Controller"
            )

        return LegacyMountExpectation(
            task_id=task_id,
            parent_inputs=len(plan.parent_task_ids),
            extra_inputs=len(plan.input_file_data_ids),
            extra_outputs=len(plan.output_file_data_ids),
        )

    def record_materialization_audit(self, task_id, count):
        if task_id not in self.tasks:
            raise KeyError(f"unknown TaskID {task_id}")
        if task_id in self._materialization_audits:
            raise ValueError(f"TaskID {task_id} audit recorded more than once")
        if count != 1:
            raise ValueError(
                f"TaskID {task_id} materialized {count} times; expected once"
            )
        self._materialization_audits[task_id] = count

    def record_worker_preparation_audit(self, task_id, count):
        if task_id not in self.tasks:
            raise KeyError(f"unknown TaskID {task_id}")
        if task_id in self._worker_preparation_audits:
            raise ValueError(
                f"TaskID {task_id} worker audit recorded more than once"
            )
        if count != 1:
            raise ValueError(
                f"TaskID {task_id} worker prepared {count} times; expected once"
            )
        self._worker_preparation_audits[task_id] = count

    def comparison_report(self):
        return _thaw_json_value(self._comparison)

    def audit_report(self):
        raw_counts = {
            task_id: self._materialization_audits.get(task_id, 0)
            for task_id in sorted(self.tasks)
        }
        return {
            "expected_tasks": len(self.tasks),
            "audited_tasks": sum(count == 1 for count in raw_counts.values()),
            "counts": {
                str(task_id): count
                for task_id, count in raw_counts.items()
            },
            "mismatches": [
                f"TaskID {task_id} audit count is {count}, expected 1"
                for task_id, count in raw_counts.items()
                if count != 1
            ],
        }

    def worker_preparation_audit_report(self):
        raw_counts = {
            task_id: self._worker_preparation_audits.get(task_id, 0)
            for task_id in sorted(self.tasks)
        }
        return {
            "expected_tasks": len(self.tasks),
            "audited_tasks": sum(count == 1 for count in raw_counts.values()),
            "counts": {
                str(task_id): count
                for task_id, count in raw_counts.items()
            },
            "mismatches": [
                f"TaskID {task_id} worker audit count is {count}, expected 1"
                for task_id, count in raw_counts.items()
                if count != 1
            ],
        }

    def summary(self):
        return {
            "tasks": len(self.tasks),
            "edata": len(self.edata),
            "idata": len(self.idata),
            "input_bindings": sum(
                len(plan.input_bindings) for plan in self.tasks.values()
            ),
            "output_bindings": sum(
                len(plan.output_bindings) for plan in self.tasks.values()
            ),
        }


def _freeze_json_value(value):
    if isinstance(value, dict):
        return MappingProxyType(
            {key: _freeze_json_value(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json_value(item) for item in value)
    return value


def _thaw_json_value(value):
    if isinstance(value, MappingProxyType):
        return {
            key: _thaw_json_value(item) for key, item in value.items()
        }
    if isinstance(value, tuple):
        return [_thaw_json_value(item) for item in value]
    return value
