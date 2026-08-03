#!/usr/bin/env python3

from ndcctools.taskvine.datavine.scheduler.registration import (
    WorkflowRegistrar,
)
from ndcctools.taskvine.datavine.scheduler.run_context import (
    WorkflowRunContext,
)
from ndcctools.taskvine.datavine.workflow import Workflow


def identity(value):
    return value


class FakeController:
    def __init__(self):
        self.next_edata_id = 1
        self.task_batches = []

    def allocate_idata_batch(self, producer_slots):
        return tuple(
            101 + index for index, _ in enumerate(producer_slots)
        )

    def _edata_result(self, payload):
        result = {
            "data_id": self.next_edata_id,
            "storage": "controller-memory",
            "serialized_size": len(payload),
        }
        self.next_edata_id += 1
        return result

    def register_edata(self, metadata, payload):
        return self._edata_result(payload)

    def register_edata_batch(self, records):
        return tuple(
            self._edata_result(payload) for _, payload in records
        )

    def register_tasks(self, records):
        self.task_batches.append(tuple(records))


def main():
    controller = FakeController()
    registrar = WorkflowRegistrar(controller, task_batch_size=1)
    context = WorkflowRunContext()
    workflow = Workflow()
    producer = workflow.add_task(identity, 7, output_count=2)
    workflow.add_task(
        identity,
        producer.output(1),
        [producer.output(0), 7],
        named=7,
    )
    workflow.validate()

    outputs = registrar.register(workflow, context)
    assert outputs == {1: 101, 2: 103}
    assert context.logical_output_slots == {1: (101, 102), 2: (103,)}
    assert len(controller.task_batches) == 2
    first = controller.task_batches[0][0]
    second = controller.task_batches[1][0]
    assert first.output_data_ids == (101, 102)
    assert second.output_data_ids == (103,)
    assert second.input_data_ids == (101, 102)
    assert second.positional[0] == ("i", 102)
    assert second.positional[1][0] == "c"
    assert second.keyword[0][0] == "named"
    assert second.keyword[0][1][0] == "e"
    assert all(
        kind in ("e", "c", "i")
        for record in (first, second)
        for kind, _ in (
            *record.positional,
            *(binding for _, binding in record.keyword),
        )
    )
    assert context.serialization_count > 0
    assert context.registration_timing.keys() == {
        "idata_allocation",
        "edata",
        "task_record_build",
        "task_registration",
    }
    assert not context.edata_by_object

    for changes, expected in (
        ({"bulk_threshold": 0}, "bulk threshold"),
        ({"task_batch_size": 0}, "task batch size"),
    ):
        try:
            WorkflowRegistrar(controller, **changes)
        except ValueError as exc:
            assert expected in str(exc)
        else:
            raise AssertionError(f"accepted invalid settings {changes}")

    print("DataVine Scheduler registration contract PASS")


if __name__ == "__main__":
    main()
