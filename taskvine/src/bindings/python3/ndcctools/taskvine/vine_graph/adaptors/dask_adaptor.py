from collections.abc import Mapping

try:
    import dask
except ImportError:
    dask = None

try:
    from dask.base import is_dask_collection
except ImportError:
    is_dask_collection = None

try:
    import importlib

    dts = importlib.import_module("dask._task_spec")
except Exception:
    dts = None

from .legacy_dask_adaptor import call_with_nested_tasks, expand_legacy_subgraph_dsk
from .dask_common import (
    build_task_expr,
    identity,
    resolve_graph_key_if_task,
)
from .dask_task_spec import DaskTaskSpecConverter
from ..graph import Graph, _Ref


def from_dask(dsk, expand_subgraphs=False):
    """Convert a Dask graph, a dictionary of Dask collections or task specifications, into a Graph whose keys are the
    Dask keys."""
    graph = Graph()
    for key, (func, args, kwargs) in _DaskConverter(dsk, expand_subgraphs).tasks.items():
        graph._add(key, func, args, kwargs)
    for key, parents in graph._parents.items():
        for graph_id, parent in parents:
            if graph_id is None and parent not in graph._tasks:
                raise ValueError(f"task {key!r} reads {parent!r}, which is not in the Dask graph")
    return graph


class _DaskConverter:
    """Convert Dask graph forms into Graph task expressions."""

    def __init__(self, task_dict, expand_subgraphs=False):
        self._expand_subgraphs = expand_subgraphs
        normalized = self._normalize_task_dict(task_dict)
        self.tasks = self._convert_to_graph_tasks(normalized)

    def _normalize_task_dict(self, task_dict):
        if self._is_dask_collection_dict(task_dict):
            task_dict = self._dask_collections_to_task_dict(task_dict)
        else:
            # Plain dicts hold task expressions; don't let Dask reinterpret kwargs dicts.
            task_dict = dict(task_dict)

        if self._expand_subgraphs and not dts and task_dict:
            task_dict = expand_legacy_subgraph_dsk(task_dict, dask)
        return task_dict

    def _is_dask_collection_dict(self, task_dict):
        return bool(is_dask_collection and any(is_dask_collection(value) for value in task_dict.values()))

    def _dask_collections_to_task_dict(self, task_dict):
        assert is_dask_collection is not None
        from dask.highlevelgraph import HighLevelGraph, ensure_dict

        if not isinstance(task_dict, dict):
            raise TypeError("Input must be a dict")
        for key, value in task_dict.items():
            if not is_dask_collection(value):
                raise TypeError(f"Input must be a dict of DaskCollection, but found {key} with type {type(value)}")

        if dts:
            hlg = HighLevelGraph.merge(*(value.dask for value in task_dict.values())).to_dict()
        else:
            hlg = dask.base.collections_to_dsk(task_dict.values())
            hlg = hlg.to_dict() if hasattr(hlg, "to_dict") else dict(hlg)
        return ensure_dict(hlg)

    def _convert_to_graph_tasks(self, task_dict):
        if not task_dict:
            return {}

        converted = {}
        graph_keys = set(task_dict.keys())
        task_spec = DaskTaskSpecConverter(dts) if dts else None

        for key, value in task_dict.items():
            if task_spec and task_spec.is_node(value):
                converted[key] = task_spec.convert_node(key, value, graph_keys)
            else:
                converted[key] = self._convert_legacy_task(value, graph_keys)

        if task_spec:
            while True:
                pending = task_spec.pending_nodes(converted)
                if not pending:
                    break
                for key, node in pending:
                    converted[key] = task_spec.convert_node(key, node, graph_keys)

        return converted

    def _convert_legacy_task(self, sexpr, graph_keys):
        try:
            if not isinstance(sexpr, (list, tuple)) and sexpr in graph_keys:
                return build_task_expr(identity, [_Ref(None, sexpr)], {})
        except TypeError:
            pass

        # A legacy task is a tuple that starts with a callable. Any other value, such as a list, is data that may
        # refer to other keys and hold nested tasks.
        if not isinstance(sexpr, tuple) or not sexpr or not callable(sexpr[0]):
            return build_task_expr(call_with_nested_tasks, [identity, self._wrap_dependency(sexpr, graph_keys)], {})

        func = sexpr[0]
        tail = sexpr[1:]
        if tail and isinstance(tail[-1], Mapping):
            raw_args, raw_kwargs = tail[:-1], tail[-1]
        else:
            raw_args, raw_kwargs = tail, {}

        args = tuple(self._wrap_dependency(arg, graph_keys) for arg in raw_args)
        kwargs = {key: self._wrap_dependency(value, graph_keys) for key, value in raw_kwargs.items()}
        return call_with_nested_tasks, (func, *args), kwargs

    def _wrap_dependency(self, obj, graph_keys):
        if isinstance(obj, _Ref):
            return obj

        key = resolve_graph_key_if_task(obj, graph_keys)
        if key is not None:
            return _Ref(None, key)

        if isinstance(obj, list):
            return [self._wrap_dependency(value, graph_keys) for value in obj]
        if isinstance(obj, tuple):
            return tuple(self._wrap_dependency(value, graph_keys) for value in obj)
        if isinstance(obj, Mapping):
            return {key: self._wrap_dependency(value, graph_keys) for key, value in obj.items()}
        if isinstance(obj, set):
            return {self._wrap_dependency(value, graph_keys) for value in obj}
        if isinstance(obj, frozenset):
            return frozenset(self._wrap_dependency(value, graph_keys) for value in obj)
        return obj
