"""Small logical workflow API for the independent DataVine runtime."""

import dataclasses


@dataclasses.dataclass(frozen=True)
class OutputRef:
    producer_task_id: int


@dataclasses.dataclass
class WorkflowTask:
    task_id: int
    function: object
    args: tuple
    kwargs: dict

    def output(self):
        return OutputRef(self.task_id)


class Workflow:
    def __init__(self):
        self._tasks = []

    @property
    def tasks(self):
        return tuple(self._tasks)

    def add_task(self, function, *args, **kwargs):
        if not callable(function):
            raise TypeError("function must be callable")
        task = WorkflowTask(
            task_id=len(self._tasks) + 1,
            function=function,
            args=tuple(args),
            kwargs=dict(kwargs),
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
                            f"TaskID {task.task_id} has a forward or unknown dependency"
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
