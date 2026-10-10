# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""A task graph describes what to run, independent of where it runs. It holds no runtime state, so it can be pickled,
shared, and run by any Executor any number of times."""

import collections
import collections.abc
import copy
import dataclasses
import os
import uuid


class _Symbol:
    """Base of the objects that stand for data a task reads: task results, selections from them, and files. Task
    arguments may contain symbols anywhere inside lists, tuples, sets, dictionary values, and dataclasses."""

    __slots__ = ()

    def _as_ref(self):
        """Return the lightweight reference stored in task arguments in place of this symbol."""
        raise NotImplementedError


class File(_Symbol):
    """A file on the local filesystem that tasks read. A task receives the path of its copy in the task sandbox, and
    a File given to Executor.submit() as a target returns its own path."""

    __slots__ = ("path",)

    def __init__(self, path):
        path = os.fspath(path)
        if not isinstance(path, str):
            raise TypeError("a File path must be a string or path-like object")
        if "\0" in path:
            raise ValueError("a File path contains a null byte")
        self.path = os.path.abspath(path)

    def _as_ref(self):
        return self

    def __eq__(self, other):
        return isinstance(other, File) and other.path == self.path

    def __hash__(self):
        return hash((File, self.path))

    def __repr__(self):
        return f"File({self.path!r})"


@dataclasses.dataclass(frozen=True)
class _Attribute:
    """A selection step that reads an attribute, as opposed to an item."""

    name: str


class _Selectable(_Symbol):
    """A symbol for a task result, from which items and attributes can be selected."""

    __slots__ = ()

    def __getitem__(self, item):
        """Stand for result[item]. Each [] adds one step, so x["a"]["b"] differs from x[("a", "b")]."""
        return _Selection(self, (item,))

    def attr(self, name):
        """Stand for an attribute of the result."""
        if not isinstance(name, str):
            raise TypeError("an attribute name must be a string")
        return _Selection(self, (_Attribute(name),))


class Node(_Selectable):
    """A task in a Graph. Pass a Node as an argument of another task to give it this task's result."""

    __slots__ = ("graph", "key")

    def __init__(self, graph, key):
        self.graph = graph
        self.key = key

    def file(self, path):
        """Declare a file this task writes at path in its sandbox, and return it. Declaring the same path again
        returns the same file."""
        return self.graph._add_output(self.key, path)

    def _as_ref(self):
        return _Ref(self.graph._id, self.key)

    def __eq__(self, other):
        return isinstance(other, Node) and other.graph is self.graph and other.key == self.key

    def __hash__(self):
        return hash((Node, id(self.graph), self.key))

    def __repr__(self):
        func = self.graph._tasks[self.key][0] if self.key in self.graph._tasks else None
        return f"Node({self.key!r}, {getattr(func, '__name__', type(func).__name__)})"


class NodeFile(_Symbol):
    """A file that a task writes in its sandbox. Pass it to another task to give it the file's path there."""

    __slots__ = ("node", "path")

    def __init__(self, node, path):
        self.node = node
        self.path = path

    def _as_ref(self):
        return _FileRef(self.node.graph._id, self.node.key, self.path)

    def __eq__(self, other):
        return isinstance(other, NodeFile) and other.node == self.node and other.path == self.path

    def __hash__(self):
        return hash((NodeFile, self.node, self.path))

    def __repr__(self):
        return f"{self.node!r}.file({self.path!r})"


class _Selection(_Selectable):
    """An item or attribute selected from a task result."""

    __slots__ = ("source", "steps")

    def __init__(self, source, steps):
        self.source = source
        self.steps = tuple(steps)

    def __getitem__(self, item):
        return _Selection(self.source, self.steps + (item,))

    def attr(self, name):
        if not isinstance(name, str):
            raise TypeError("an attribute name must be a string")
        return _Selection(self.source, self.steps + (_Attribute(name),))

    def _as_ref(self):
        ref = self.source._as_ref()
        return _Ref(ref.graph_id, ref.key, ref.steps + self.steps)

    def __repr__(self):
        return f"{self.source!r}{''.join(f'.attr({s.name!r})' if isinstance(s, _Attribute) else f'[{s!r}]' for s in self.steps)}"


class _Ref(_Symbol):
    """The result of task key in the graph with graph_id, followed by selection steps. A graph_id of None means the
    graph the reference is stored in, which is how a Graph refers to its own tasks. References are what task arguments
    hold, so arguments never carry a graph."""

    __slots__ = ("graph_id", "key", "steps")

    def __init__(self, graph_id, key, steps=()):
        self.graph_id = graph_id
        self.key = key
        self.steps = tuple(steps)

    def _as_ref(self):
        return self


class _FileRef(_Symbol):
    """The file that task key in the graph with graph_id writes at path in its sandbox. A graph_id of None means the
    graph the reference is stored in."""

    __slots__ = ("graph_id", "key", "path")

    def __init__(self, graph_id, key, path):
        self.graph_id = graph_id
        self.key = key
        self.path = path

    def _as_ref(self):
        return self


_LEAF_TYPES = (str, bytes, bytearray, memoryview, int, float, bool, complex, type(None))


def _contains_symbol(value, seen):
    """Return whether value holds a symbol anywhere inside, including inside custom objects."""
    if isinstance(value, _Symbol):
        return True
    if value is None or isinstance(value, _LEAF_TYPES):
        return False
    if id(value) in seen:
        return False
    seen.add(id(value))
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return any(_contains_symbol(getattr(value, f.name), seen) for f in dataclasses.fields(value))
    if isinstance(value, collections.abc.Mapping):
        return any(_contains_symbol(k, seen) or _contains_symbol(v, seen) for k, v in value.items())
    if isinstance(value, (list, tuple, set, frozenset, collections.deque)):
        return any(_contains_symbol(v, seen) for v in value)
    try:
        state = vars(value)
    except TypeError:
        state = None
    if state and any(_contains_symbol(v, seen) for v in state.values()):
        return True
    for slot in getattr(type(value), "__slots__", ()):
        if isinstance(slot, str) and hasattr(value, slot) and _contains_symbol(getattr(value, slot), seen):
            return True
    return False


def replace_symbols(value, replace):
    """Return value with every symbol inside replaced by replace(symbol). A value without symbols is returned as
    is, so arguments keep their identity. Containers that hold symbols are copied with their type, aliases, and
    cycles preserved. Symbols may not be dictionary keys or hide inside other custom objects."""
    if not _contains_symbol(value, set()):
        return value
    memo = {}
    active_immutable = set()

    def visit(x):
        if isinstance(x, _Symbol):
            return replace(x)
        if x is None or isinstance(x, _LEAF_TYPES):
            return x
        oid = id(x)
        if oid in memo:
            return memo[oid]

        if isinstance(x, collections.abc.Mapping):
            if any(_contains_symbol(k, set()) for k in x.keys()):
                raise ValueError("a task result or file cannot be a dictionary key")
            # copy+clear keeps dict subclasses and defaultdict factories, and publishing the empty copy first keeps
            # aliases and cycles.
            try:
                out = copy.copy(x)
                out.clear()
            except Exception:
                out = {}
            memo[oid] = out
            for k, v in x.items():
                out[k] = visit(v)
            return out

        if isinstance(x, list):
            out = []
            memo[oid] = out
            out.extend(visit(v) for v in x)
            return out

        if isinstance(x, collections.deque):
            out = collections.deque(maxlen=x.maxlen)
            memo[oid] = out
            out.extend(visit(v) for v in x)
            return out

        if isinstance(x, set):
            out = set()
            memo[oid] = out
            out.update(visit(v) for v in x)
            return out

        if isinstance(x, (tuple, frozenset)):
            if oid in active_immutable:
                raise ValueError("cycles through tuples or frozensets that hold task results are not supported")
            active_immutable.add(oid)
            try:
                values = [visit(v) for v in x]
            finally:
                active_immutable.remove(oid)
            if hasattr(x, "_fields"):
                out = x.__class__(*values)
            elif isinstance(x, tuple):
                out = tuple(values)
            else:
                out = frozenset(values)
            memo[oid] = out
            return out

        if dataclasses.is_dataclass(x) and not isinstance(x, type):
            out = copy.copy(x)
            memo[oid] = out
            for field in dataclasses.fields(x):
                object.__setattr__(out, field.name, visit(getattr(x, field.name)))
            return out

        if _contains_symbol(x, set()):
            raise TypeError("a task result or file inside a custom object is not supported; use a dataclass or a "
                            "list, tuple, set, or dictionary")
        return x

    return visit(value)


class Graph:
    """Tasks and the data they pass to each other. Graph.add() records a call without running it, and its Node
    stands for the call's result. A Graph is a description: an Executor runs it, and it may be pickled and shared.
    A copy, such as an unpickled Graph, is a Graph of its own, which an Executor runs independently of the original."""

    def __init__(self):
        # Identifies this Graph object while it exists. A Graph refers to its own tasks with graph id None, so the id
        # is not part of its state and every copy gets a new one.
        self._id = uuid.uuid4().hex
        self._next_key = 1
        self._tasks = {}  # key -> (function, args, kwargs), with symbols replaced by references
        # key -> (graph id, key) of every task whose result or file it reads, in first-use order. The graph id is None
        # for a task of this graph.
        self._parents = {}
        self._results = {}  # key -> (graph id, key) of every task whose result it reads, in first-use order
        self._children = collections.defaultdict(set)  # key -> keys in this graph that read its result or files
        self._reads = {}  # key -> File and _FileRef objects the task reads, in first-use order
        self._outputs = collections.defaultdict(dict)  # key -> sandbox path -> NodeFile it writes

    def __getstate__(self):
        state = dict(self.__dict__)
        del state["_id"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._id = uuid.uuid4().hex

    def add(self, func, /, *args, **kwargs):
        """Add a call of func with these arguments and return its Node. Nodes, their selections and files, and
        File objects may appear anywhere inside the arguments."""
        while self._next_key in self._tasks:
            self._next_key += 1
        key = self._next_key
        self._next_key += 1
        return self._add(key, func, args, kwargs)

    def node(self, key):
        """Return the Node of the task with this key, such as a Dask key of a graph from adaptors.from_dask()."""
        if key not in self._tasks:
            raise KeyError(f"no task with key {key!r}")
        return Node(self, key)

    def sinks(self):
        """Return the Nodes that no other task in this graph reads."""
        return [Node(self, key) for key in self._tasks if not self._children.get(key)]

    def __iter__(self):
        return (Node(self, key) for key in self._tasks)

    def __len__(self):
        return len(self._tasks)

    def __contains__(self, node):
        return isinstance(node, Node) and node.graph is self and node.key in self._tasks

    def __repr__(self):
        return f"Graph({len(self._tasks)} tasks)"

    # Construction.

    def _add(self, key, func, args, kwargs, foreign=False, on_symbol=None):
        """Record a task under key. A graph built by an Executor passes foreign=True, so its tasks may read results of
        other graphs and Futures, and on_symbol to see each symbol. A user graph reads only its own tasks."""
        if not callable(func):
            raise TypeError("a task needs a callable followed by its arguments")
        if key in self._tasks:
            raise ValueError(f"a task with key {key!r} already exists")
        parents = {}  # (graph id, key) -> None, as an ordered set
        results = {}  # (graph id, key) of tasks whose result it reads -> None, as an ordered set
        reads = {}  # identity of each file read -> its File or _FileRef

        def replace(symbol):
            if on_symbol is not None:
                on_symbol(symbol)
            # A Graph describes work independent of any run, so it takes Nodes and never Futures.
            if not foreign and hasattr(symbol.source if isinstance(symbol, _Selection) else symbol, "_executor"):
                raise TypeError("a Graph cannot hold a Future; use Executor.submit() for tasks built from Futures")
            ref = symbol._as_ref()
            if isinstance(ref, File):
                reads.setdefault(ref, ref)
                return ref
            graph_id = None if ref.graph_id in (None, self._id) else ref.graph_id
            if graph_id is not None and not foreign:
                raise ValueError("a task reads a Node of another Graph")
            parents.setdefault((graph_id, ref.key), None)
            if isinstance(ref, _FileRef):
                ref = _FileRef(graph_id, ref.key, ref.path)
                reads.setdefault((graph_id, ref.key, ref.path), ref)
                return ref
            results.setdefault((graph_id, ref.key), None)
            return _Ref(graph_id, ref.key, ref.steps)

        # Replacing all arguments at once keeps an object passed as several arguments one object.
        args, kwargs = replace_symbols((tuple(args), dict(kwargs)), replace)
        self._tasks[key] = (func, args, kwargs)
        self._parents[key] = tuple(parents)
        self._results[key] = tuple(results)
        self._reads[key] = tuple(reads.values())
        for graph_id, parent in parents:
            if graph_id is None:
                self._children[parent].add(key)
        return Node(self, key)

    def _add_output(self, key, path):
        if key not in self._tasks:
            raise KeyError(f"no task with key {key!r}")
        path = os.fspath(path)
        if not isinstance(path, str):
            raise TypeError("an output path must be a string or path-like object")
        if "\0" in path:
            raise ValueError("an output path contains a null byte")
        normalized = os.path.normpath(path)
        if not path or normalized in ("", ".") or os.path.isabs(path) or normalized == ".." or normalized.startswith("../"):
            raise ValueError("an output path must be a non-empty relative path inside the task sandbox")
        outputs = self._outputs[key]
        if normalized not in outputs:
            outputs[normalized] = NodeFile(Node(self, key), normalized)
        return outputs[normalized]
