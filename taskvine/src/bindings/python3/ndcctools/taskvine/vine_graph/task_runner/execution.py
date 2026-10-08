# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


import time
import copy
import dataclasses
import cloudpickle
from collections import deque

from ..workflow import Workflow, TaskOutputWrapper, _TaskOutputAttribute


def _resolve_nested_legacy_tasks(obj, memo=None):
    """Evaluate nested legacy Dask tasks encoded as plain ``(func, *args)`` tuples."""
    if memo is None:
        memo = {}

    if obj is None or isinstance(obj, (str, bytes, bytearray, memoryview, int, float, bool)):
        return obj

    oid = id(obj)
    if oid in memo:
        return memo[oid]

    if type(obj) is tuple and obj and callable(obj[0]):
        func = obj[0]
        args = tuple(_resolve_nested_legacy_tasks(v, memo) for v in obj[1:])
        result = func(*args)
        memo[oid] = result
        return result

    if isinstance(obj, list):
        out = []
        memo[oid] = out
        out.extend(_resolve_nested_legacy_tasks(v, memo) for v in obj)
        return out

    if isinstance(obj, deque):
        out = deque(maxlen=obj.maxlen)
        memo[oid] = out
        out.extend(_resolve_nested_legacy_tasks(v, memo) for v in obj)
        return out

    if type(obj) is tuple:
        out = tuple(_resolve_nested_legacy_tasks(v, memo) for v in obj)
        memo[oid] = out
        return out

    if isinstance(obj, tuple) and hasattr(obj, "_fields"):
        out = obj.__class__(*(_resolve_nested_legacy_tasks(v, memo) for v in obj))
        memo[oid] = out
        return out

    if isinstance(obj, dict):
        try:
            out = copy.copy(obj)
            out.clear()
        except Exception:
            out = {}
        memo[oid] = out
        for key, value in obj.items():
            out[key] = _resolve_nested_legacy_tasks(value, memo)
        return out

    if isinstance(obj, set):
        out = set()
        memo[oid] = out
        out.update(_resolve_nested_legacy_tasks(v, memo) for v in obj)
        return out

    if isinstance(obj, frozenset):
        out = frozenset(_resolve_nested_legacy_tasks(v, memo) for v in obj)
        memo[oid] = out
        return out

    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        out = copy.copy(obj)
        memo[oid] = out
        for field in dataclasses.fields(obj):
            object.__setattr__(out, field.name, _resolve_nested_legacy_tasks(getattr(obj, field.name), memo))
        return out

    return obj


def compute_task(workflow, task_expr):
    """Execute locally using the Manager's Workflow metadata."""
    func_id, args, kwargs = task_expr

    def file_path(handle):
        if handle.workflow_id != workflow._workflow_id:
            raise ValueError("file belongs to a different Workflow")
        return workflow.file_input_path(handle.file_id)

    return _compute_task(workflow.callables[func_id], args, kwargs, workflow.load_task_output, file_path)


def _compute_task(func, args, kwargs, load_output, file_path):
    cache = {}

    def _follow_path(value, path):
        current = value
        for token in path:
            if isinstance(token, _TaskOutputAttribute):
                current = getattr(current, token.name)
            elif isinstance(current, (list, tuple)):
                current = current[token]
            elif isinstance(current, dict):
                current = current[token]
            else:
                current = getattr(current, token)
        return current

    def on_ref(r):
        if r.task_id not in cache:
            x = load_output(r.task_id)
            cache[r.task_id] = x
        else:
            x = cache[r.task_id]
        if r.path:
            return _follow_path(x, r.path)
        return x

    r_args, r_kwargs = Workflow._visit_task_output_refs(
        (args, kwargs), on_ref, rewrite=True, on_file=file_path
    )

    r_args, r_kwargs = _resolve_nested_legacy_tasks((r_args, r_kwargs))

    return func(*r_args, **r_kwargs)


def run_node(node_id):
    """Load one node's execution description, call its function, and write its output."""
    if isinstance(node_id, bool) or not isinstance(node_id, (int, str)):
        raise TypeError("run_node expects one positive node ID")
    node_id = int(node_id)
    if node_id < 1:
        raise ValueError("run_node expects one positive node ID")
    # Pickle preserves arbitrary Python task keys in this node's upstream-reference table.
    with open(f"vine-graph-edata-node-{node_id}.pkl", "rb") as stream:
        manifest = cloudpickle.load(stream)
    objects = {}

    def load(file_id):
        if file_id not in objects:
            with open(f"vine-graph-edata-{file_id}", "rb") as stream:
                objects[file_id] = cloudpickle.load(stream)
        return objects[file_id]

    def load_output(task_key):
        return TaskOutputWrapper.load_from_path(manifest["inputs"][task_key])

    def file_path(handle):
        return manifest["files"][handle.file_id]

    function = load(manifest["callable"])
    args = tuple(load(file_id) for file_id in manifest["args"])
    kwargs = {name: load(file_id) for name, file_id in manifest["kwargs"].items()}
    output = _compute_task(function, args, kwargs, load_output, file_path)
    del function, args, kwargs
    objects.clear()
    time.sleep(manifest["sleep"])
    with open(manifest["output"], "wb") as stream:
        cloudpickle.dump(TaskOutputWrapper(output, extra_size_mb=manifest["output_size_mb"]), stream)
