# Copyright (C) 2026- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Read-only Phase 2 Data Graph derived from Phase 1 identities."""

import dataclasses


@dataclasses.dataclass(frozen=True, order=True)
class ShadowConsumer:
    task_id: int
    binding_kind: str
    slot_kind: str
    slot: object


@dataclasses.dataclass(frozen=True)
class ShadowEDataNode:
    data_id: int
    content_hash: str
    serialized_size: int
    availability: str
    consumers: tuple


@dataclasses.dataclass(frozen=True)
class ShadowIDataNode:
    data_id: int
    producer_task_id: int
    slot_kind: str
    slot: object
    state: str
    consumers: tuple


@dataclasses.dataclass(frozen=True)
class ShadowTaskNode:
    task_id: int
    edata_inputs: tuple
    idata_inputs: tuple
    idata_outputs: tuple


class ShadowDataGraph:
    """Validated observer view; it has no runtime authority."""

    def __init__(self, edata, idata, tasks, comparison):
        self.edata = edata
        self.idata = idata
        self.tasks = tasks
        self._comparison = comparison

    @classmethod
    def from_workflow(cls, workflow):
        identity = workflow.indexed_data_identity
        if identity is None:
            raise ValueError(
                "shadow data graph requires indexed data identity"
            )
        identity.validate()

        edata_consumers = {data_id: set() for data_id in identity.edata.records}
        idata_consumers = {data_id: set() for data_id in identity.idata}
        tasks = {}

        for task_id in sorted(identity.task_bindings):
            binding = identity.task_bindings[task_id]
            edata_inputs = {binding.callable_edata_id}
            idata_inputs = set()
            edata_consumers[binding.callable_edata_id].add(
                ShadowConsumer(task_id, "callable", "callable", None)
            )

            for input_binding in binding.inputs:
                consumer = ShadowConsumer(
                    task_id,
                    "input",
                    input_binding.slot_kind,
                    input_binding.slot,
                )
                if input_binding.source_kind == "idata":
                    idata_inputs.add(input_binding.data_id)
                    idata_consumers[input_binding.data_id].add(consumer)
                else:
                    edata_inputs.add(input_binding.data_id)
                    edata_consumers[input_binding.data_id].add(consumer)

                for reference in input_binding.references:
                    # Direct bindings repeat their primary source as a
                    # reference. Only structured bindings add a second edge.
                    if (
                        reference.data_kind == input_binding.source_kind
                        and reference.data_id == input_binding.data_id
                    ):
                        continue
                    reference_consumer = ShadowConsumer(
                        task_id,
                        "reference",
                        input_binding.slot_kind,
                        input_binding.slot,
                    )
                    if reference.data_kind == "edata":
                        edata_inputs.add(reference.data_id)
                        edata_consumers[reference.data_id].add(reference_consumer)
                    elif reference.data_kind == "idata":
                        idata_inputs.add(reference.data_id)
                        idata_consumers[reference.data_id].add(reference_consumer)
                    else:
                        raise ValueError(
                            f"TaskID {task_id} has invalid referenced data kind"
                        )

            outputs = tuple(
                output.data_id for output in binding.outputs
            )
            tasks[task_id] = ShadowTaskNode(
                task_id=task_id,
                edata_inputs=tuple(sorted(edata_inputs)),
                idata_inputs=tuple(sorted(idata_inputs)),
                idata_outputs=outputs,
            )

        edata = {}
        for data_id, record in identity.edata.records.items():
            edata[data_id] = ShadowEDataNode(
                data_id=data_id,
                content_hash=record.content_hash,
                serialized_size=len(record.serialized_bytes),
                availability="controller",
                consumers=tuple(sorted(edata_consumers[data_id])),
            )

        idata = {}
        for data_id, record in identity.idata.items():
            idata[data_id] = ShadowIDataNode(
                data_id=data_id,
                producer_task_id=record.producer_task_id,
                slot_kind=record.slot_kind,
                slot=record.slot,
                state="unproduced",
                consumers=tuple(sorted(idata_consumers[data_id])),
            )

        comparison = cls._compare(workflow, identity, edata, idata, tasks)
        if comparison["mismatches"]:
            raise ValueError(
                "shadow data graph mismatch: "
                + "; ".join(comparison["mismatches"])
            )
        return cls(edata, idata, tasks, comparison)

    @staticmethod
    def _compare(workflow, identity, edata, idata, tasks):
        mismatches = []
        if set(edata) != set(identity.edata.records):
            mismatches.append("EDataID set differs from Phase 1")
        if set(idata) != set(identity.idata):
            mismatches.append("IDataID set differs from Phase 1")
        if set(tasks) != set(identity.task_ids.values()):
            mismatches.append("TaskID set differs from Phase 1")

        workflow_dependencies = {
            (
                identity.task_ids[parent_key],
                identity.task_ids[child_key],
            )
            for child_key in workflow.task_dict
            for parent_key in workflow.parents_of.get(child_key, ())
        }
        shadow_dependencies = {
            (idata[data_id].producer_task_id, task_id)
            for task_id, task in tasks.items()
            for data_id in task.idata_inputs
        }
        if shadow_dependencies != workflow_dependencies:
            mismatches.append("task dependency edges differ from Workflow")

        expected_outputs = {
            data_id: record.producer_task_id
            for data_id, record in identity.idata.items()
        }
        actual_outputs = {
            data_id: task_id
            for task_id, task in tasks.items()
            for data_id in task.idata_outputs
        }
        if actual_outputs != expected_outputs:
            mismatches.append("IData producer edges differ from Phase 1")

        if any(node.availability != "controller" for node in edata.values()):
            mismatches.append("initial EData availability is not controller")
        if any(node.state != "unproduced" for node in idata.values()):
            mismatches.append("initial IData state is not unproduced")

        return {
            "mismatches": mismatches,
            "counts": {
                "tasks": len(tasks),
                "edata": len(edata),
                "idata": len(idata),
                "workflow_dependency_edges": len(workflow_dependencies),
                "shadow_dependency_edges": len(shadow_dependencies),
                "producer_edges": len(actual_outputs),
                "consumer_edges": sum(
                    len(node.consumers) for node in edata.values()
                )
                + sum(len(node.consumers) for node in idata.values()),
            },
        }

    def comparison_report(self):
        return {
            "mismatches": list(self._comparison["mismatches"]),
            "counts": dict(self._comparison["counts"]),
        }

    def summary(self):
        return self.comparison_report()["counts"]
