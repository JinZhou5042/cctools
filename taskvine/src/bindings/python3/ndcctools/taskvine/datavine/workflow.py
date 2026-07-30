"""Small logical workflow API for the independent DataVine runtime."""

import dataclasses


@dataclasses.dataclass(frozen=True)
class OutputRef:
    producer_task_id: int
    output_index: int = 0


@dataclasses.dataclass
class WorkflowTask:
    task_id: int
    function: object
    args: tuple
    kwargs: dict
    output_count: int = 1

    def output(self, index=0):
        index = int(index)
        if index < 0 or index >= self.output_count:
            raise IndexError(
                f"TaskID {self.task_id} output index {index} is out of range"
            )
        return OutputRef(self.task_id, index)


class Workflow:
    def __init__(self):
        self._tasks = []

    @property
    def tasks(self):
        return tuple(self._tasks)

    def add_task(self, function, *args, output_count=1, **kwargs):
        if not callable(function):
            raise TypeError("function must be callable")
        output_count = int(output_count)
        if output_count < 1:
            raise ValueError("output_count must be positive")
        task = WorkflowTask(
            task_id=len(self._tasks) + 1,
            function=function,
            args=tuple(args),
            kwargs=dict(kwargs),
            output_count=output_count,
        )
        self._tasks.append(task)
        return task

    def validate(self):
        known = set()
        for task in self._tasks:
            for value in (*task.args, *task.kwargs.values()):
                for reference in iter_output_refs(value):
                    if reference.producer_task_id not in known:
                        raise ValueError(
                            f"TaskID {task.task_id} has a forward or "
                            "unknown dependency"
                        )
                    producer = self._tasks[
                        reference.producer_task_id - 1
                    ]
                    if reference.output_index >= producer.output_count:
                        raise ValueError(
                            f"TaskID {task.task_id} references missing "
                            f"output {reference.output_index} of TaskID "
                            f"{reference.producer_task_id}"
                        )
            known.add(task.task_id)
        return True


def iter_output_refs(value, seen=None):
    if isinstance(value, OutputRef):
        yield value
        return
    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return
    if isinstance(value, (list, tuple, set, frozenset, dict)):
        seen.add(identity)
    if isinstance(value, dict):
        for key, item in value.items():
            yield from iter_output_refs(key, seen)
            yield from iter_output_refs(item, seen)
    elif isinstance(value, (list, tuple, set, frozenset)):
        for item in value:
            yield from iter_output_refs(item, seen)
