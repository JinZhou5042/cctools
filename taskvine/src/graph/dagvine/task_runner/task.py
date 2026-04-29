# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


import time

from ndcctools.taskvine.utils import load_variable_from_library


def _resolve_nested_legacy_tasks(obj):
    """Evaluate nested legacy Dask tasks encoded as plain ``(func, *args)`` tuples."""
    if type(obj) is tuple and obj and callable(obj[0]):
        func = obj[0]
        args = tuple(_resolve_nested_legacy_tasks(v) for v in obj[1:])
        return func(*args)

    if isinstance(obj, list):
        return [_resolve_nested_legacy_tasks(v) for v in obj]

    if type(obj) is tuple:
        return tuple(_resolve_nested_legacy_tasks(v) for v in obj)

    if isinstance(obj, dict):
        return {k: _resolve_nested_legacy_tasks(v) for k, v in obj.items()}

    if isinstance(obj, set):
        return {_resolve_nested_legacy_tasks(v) for v in obj}

    if isinstance(obj, frozenset):
        return frozenset(_resolve_nested_legacy_tasks(v) for v in obj)

    return obj


def compute_task(workflow, task_expr):
    func_id, args, kwargs = task_expr
    func = workflow.callables[func_id]
    cache = {}

    def _follow_path(value, path):
        current = value
        for token in path:
            if isinstance(current, (list, tuple)):
                current = current[token]
            elif isinstance(current, dict):
                current = current[token]
            else:
                current = getattr(current, token)
        return current

    def on_ref(r):
        x = cache.get(r.task_key)
        if x is None:
            x = workflow.load_task_output(r.task_key)
            cache[r.task_key] = x
        if r.path:
            return _follow_path(x, r.path)
        return x

    r_args = workflow._visit_task_output_refs(args, on_ref, rewrite=True)
    r_kwargs = workflow._visit_task_output_refs(kwargs, on_ref, rewrite=True)

    r_args = _resolve_nested_legacy_tasks(r_args)
    r_kwargs = _resolve_nested_legacy_tasks(r_kwargs)

    return func(*r_args, **r_kwargs)


def run_task_key(vine_key):
    workflow = load_variable_from_library("graph")

    task_key = workflow.cid2pykey[vine_key]
    task_expr = workflow.task_dict[task_key]

    output = compute_task(workflow, task_expr)

    time.sleep(workflow.extra_task_sleep_time[task_key])

    workflow.save_task_output(task_key, output)
