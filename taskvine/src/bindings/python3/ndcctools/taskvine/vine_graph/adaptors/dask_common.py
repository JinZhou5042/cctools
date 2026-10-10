"""Shared helpers for the Dask adaptor modules."""

from ..graph import _Ref


def identity(value):
    """Return ``value`` unchanged."""
    return value


def build_task_expr(func, args, kwargs):
    """Build the normalized Graph task-expression tuple."""
    return func, tuple(args), dict(kwargs)


def resolve_graph_key_if_task(obj, graph_keys):
    """Return the matching graph key when ``obj`` denotes an existing Dask task."""
    if isinstance(obj, _Ref):
        return None
    try:
        if obj in graph_keys:
            return obj
    except TypeError:
        pass
    if hasattr(obj, "item") and callable(obj.item):
        try:
            item = obj.item()
            if item in graph_keys:
                return item
        except Exception:
            pass
    return None
