import contextlib
from collections import OrderedDict

from ..graph import Graph


class _GraphedResources:
    """Small WorkerResources-compatible cache for one graph task."""

    def __init__(self, max_open=128):
        self._handles = OrderedDict()
        self._max_open = max_open

    def open_once(self, uri, opener):
        if uri in self._handles:
            self._handles.move_to_end(uri)
            return self._handles[uri]
        handle = opener(uri)
        self._handles[uri] = handle
        while len(self._handles) > self._max_open:
            _, evicted = self._handles.popitem(last=False)
            self._close_handle(evicted)
        return handle

    def close(self):
        for handle in self._handles.values():
            self._close_handle(handle)
        self._handles.clear()

    @staticmethod
    def _close_handle(handle):
        close = getattr(handle, "close", None)
        if callable(close):
            with contextlib.suppress(Exception):
                close()


def _graphed_empty(empty_ref):
    """Run a graphed Plan empty function."""
    empty = empty_ref[0]
    return empty()


def _graphed_process(process_ref, partition):
    """Run one graphed Plan process task."""
    process = process_ref[0]
    resources = _GraphedResources()
    try:
        return process(partition, resources)
    finally:
        resources.close()


def _graphed_combine(combine_ref, left, right):
    """Run one graphed Plan combine step."""
    combine = combine_ref[0]
    return combine(left, right)


def _validate_static_plan(plan):
    for attr in ("process", "combine", "empty", "tasks"):
        if not hasattr(plan, attr):
            raise TypeError(f"graphed plan is missing required attribute {attr!r}")
    if getattr(plan, "next_tasks", None) is not None:
        raise ValueError("only static graphed plans can be converted")
    if getattr(plan, "stop", None) is not None:
        raise ValueError("graphed plans with a StopCondition cannot be converted yet")


def graphed_plan_to_graph(plan):
    """Convert a static graphed Plan into a Graph, and return the Graph and the Node of the final result."""
    _validate_static_plan(plan)

    graph = Graph()
    tasks = sorted(tuple(plan.tasks), key=lambda task: task.key)

    previous = graph.add(_graphed_empty, [plan.empty])
    for task in tasks:
        process = graph.add(_graphed_process, [plan.process], task.partition)
        previous = graph.add(_graphed_combine, [plan.combine], previous, process)
    return graph, previous
