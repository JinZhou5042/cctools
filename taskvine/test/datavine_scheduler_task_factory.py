#!/usr/bin/env python3

from ndcctools.taskvine.datavine.scheduler.run_context import (
    WorkflowRunContext,
)
from ndcctools.taskvine.datavine.scheduler.task_factory import TaskFactory
from ndcctools.taskvine.datavine.models import TaskRecord


class FakeManager:
    pass


class FakeController:
    endpoint = "http://127.0.0.1:1234"
    native_endpoint = "tcp://127.0.0.1:1235"
    token = "secret token"


def main():
    manager = FakeManager()
    controller = FakeController()
    context = WorkflowRunContext()
    record = TaskRecord(1, 2, (), (), 1, ())
    factory = TaskFactory(
        manager,
        controller,
        context,
        lambda task_id: record,
        1024,
    )
    task = factory.make_physical_task(1, None, 3)
    assert task.tag == "1"
    assert not task._tracked_inputs
    assert task._invocation

    print("DataVine Scheduler task factory contract PASS")


if __name__ == "__main__":
    main()
