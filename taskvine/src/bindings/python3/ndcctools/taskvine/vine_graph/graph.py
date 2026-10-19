# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""A task graph describes what to run, independent of where it runs. It holds no runtime state, so it can be pickled,
shared, and run by any Executor any number of times."""

import collections
import collections.abc
import copy
import dataclasses
import itertools
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


class Hoisted(_Symbol):
    """An object that the Executor loads once into the library on each Worker, named by the hoist argument of the
    Executor. A task that takes vg.Hoisted(name) as an argument receives that object. Every task gets its own copy, so
    changes a task makes to it stay within that task."""

    __slots__ = ("name",)

    def __init__(self, name):
        if not isinstance(name, str) or not name:
            raise TypeError("a hoisted object is named by a non-empty string")
        self.name = name

    def _as_ref(self):
        return self

    def __eq__(self, other):
        return isinstance(other, Hoisted) and other.name == self.name

    def __hash__(self):
        return hash((Hoisted, self.name))

    def __repr__(self):
        return f"Hoisted({self.name!r})"


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
        self.steps = steps if type(steps) is tuple else tuple(steps)

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
_LEAF_TYPE_SET = frozenset(_LEAF_TYPES)
# The keyword arguments of every task that has none. Stored arguments are never changed.
_NO_KWARGS = {}
# Types of selection steps that selection_key() tells apart by type and value.
_KEY_TYPES = (str, bytes, int, bool, type(None))


def selection_key(steps):
    """Return a hashable key of a chain of selection steps, equal only for chains that select alike, or None for a
    chain with a step of another type. Keys include each step's type, so x[1], x[True], and x["1"] differ."""
    keys = []
    for step in steps:
        kind = type(step)
        if kind is _Attribute:
            keys.append(step)
        elif kind in _KEY_TYPES:
            keys.append((kind, step))
        elif kind is tuple or kind is slice:
            parts = selection_key((step.start, step.stop, step.step) if kind is slice else step)
            if parts is None:
                return None
            keys.append((kind, parts))
        else:
            return None
    return tuple(keys)


# A visit number beyond every object that _scanner() visits.
_NONE_PENDING = float("inf")


def _children(value):
    """Return the objects that a value holds, in which symbols may hide: the fields of a dataclass, the keys and values
    of a mapping, the items of a list, tuple, set, or deque, and otherwise the attributes of an object."""
    kind = type(value)
    if kind is list or kind is tuple:
        return value
    if kind is dict:
        return [item for pair in value.items() for item in pair]
    if isinstance(value, Graph):
        # A Graph passed as an argument is data. The references it holds are to its own tasks, not ones the task reads.
        return ()
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        # A field may be unset, such as one with init=False that the object never assigned.
        return [getattr(value, f.name, None) for f in dataclasses.fields(value)]
    if isinstance(value, collections.abc.Mapping):
        return [item for pair in value.items() for item in pair]
    if isinstance(value, (list, tuple)):
        return value
    if isinstance(value, (set, frozenset, collections.deque)):
        return list(value)
    try:
        state = vars(value)
    except TypeError:
        state = None
    children = list(state.values()) if state else []
    for slot in getattr(type(value), "__slots__", ()):
        if isinstance(slot, str) and hasattr(value, slot):
            children.append(getattr(value, slot))
    return children


def _scanner(known=()):
    """Return a function that tells whether a value holds a symbol anywhere inside, including inside custom objects.
    It scans each object once and remembers the answer, and it does not scan objects whose id is in known, which hold
    none.

    Objects may refer to each other in cycles, such as a tree whose nodes refer to their parents. An object reached
    again while it is being scanned holds a symbol exactly when that object does, which is not known yet. The scan
    therefore finds the strongly connected components of the objects it reaches, as Tarjan's algorithm does, and the
    objects of a component share one answer once its first object is done. A cycle without symbols is left as it is,
    and one with symbols is copied with every symbol replaced. The scan keeps its own stack, so data nested deeper than
    Python's recursion limit is scanned as well."""
    holds = {}  # id -> whether the object holds a symbol, once that is known
    # id -> visit number of each object whose answer is not known yet: one being scanned, or one that reaches an
    # object being scanned and takes its answer
    pending = {}
    waiting = []  # ids of the objects in pending, in visit order
    numbers = itertools.count()

    def scan(value):
        if isinstance(value, _Symbol):
            return True
        if value is None or isinstance(value, _LEAF_TYPES):
            return False
        oid = id(value)
        if oid in known:
            return False
        result = holds.get(oid)
        if result is not None:
            return result
        # A frame is [visit number, mark in waiting, children, next child, result, low] of an object being scanned.
        # The scan of a child is inlined, because arguments may hold many objects.
        number = pending[oid] = next(numbers)
        mark = len(waiting)
        waiting.append(oid)
        stack = [[number, mark, _children(value), 0, False, number]]
        symbol, leaves = _Symbol, _LEAF_TYPES
        while True:
            frame = stack[-1]
            children = frame[2]
            index = frame[3]
            count = len(children)
            child = None
            while index < count:
                child = children[index]
                index += 1
                if isinstance(child, symbol):
                    frame[4] = True
                    continue
                if child is None or isinstance(child, leaves):
                    continue
                oid = id(child)
                if oid in known:
                    continue
                result = holds.get(oid)
                if result is not None:
                    if result:
                        frame[4] = True
                    continue
                number = pending.get(oid)
                if number is not None:
                    if number < frame[5]:
                        frame[5] = number
                    continue
                number = pending[oid] = next(numbers)
                stack.append([number, len(waiting), _children(child), 0, False, number])
                waiting.append(oid)
                break
            else:
                child = None
            frame[3] = index
            if child is not None:
                continue
            # The scan of this object is done.
            stack.pop()
            number, mark, _, _, result, low = frame
            if result or low == number:
                # The objects visited since this one that are still pending are in its cycle, so they share its
                # answer, which is now known: one of them leads to a symbol, or none does.
                for other in waiting[mark:]:
                    holds[other] = result
                    del pending[other]
                del waiting[mark:]
                low = _NONE_PENDING
            if not stack:
                return result
            parent = stack[-1]
            if result:
                parent[4] = True
            elif low < parent[5]:
                parent[5] = low

    return scan


def replace_symbols(value, replace, known=()):
    """Return value with every symbol inside replaced by replace(symbol). Values without symbols are returned as they
    are, at any depth, so arguments and the containers inside them keep their identity. Containers that hold symbols
    are copied with their type, aliases, and cycles preserved. Objects whose id is in known hold no symbol and are not
    scanned. Symbols may not be dictionary keys or hide inside other custom objects. Like the scan, the copy keeps its
    own stack, so it reaches any depth."""
    scan = _scanner(known)
    if not scan(value):
        return value
    memo = {}  # id of a copied container -> its copy
    active_immutable = set()  # ids of tuples and frozensets being copied, which a cycle cannot pass through

    def begin(x):
        """Return a frame [source, copy or None for a tuple or frozenset, items, next item, add, keyed] for a container
        that holds a symbol. A mutable copy is published in memo before its items, which keeps aliases and cycles.
        add(item), or add(key, item) for a keyed frame, puts the copy of an item in place."""
        oid = id(x)
        if isinstance(x, list):
            out = []
            memo[oid] = out
            return [x, out, x, 0, out.append, False]
        if isinstance(x, (tuple, frozenset)):
            if oid in active_immutable:
                raise ValueError("cycles through tuples or frozensets that hold task results are not supported")
            active_immutable.add(oid)
            values = []
            return [x, None, x if isinstance(x, tuple) else list(x), 0, values.append, False, values]
        if isinstance(x, collections.abc.Mapping):
            if any(scan(k) for k in x.keys()):
                raise ValueError("a task result or file cannot be a dictionary key")
            # copy+clear keeps dict subclasses and defaultdict factories.
            try:
                out = copy.copy(x)
                out.clear()
            except Exception:
                out = {}
            memo[oid] = out
            return [x, out, list(x.items()), 0, out.__setitem__, True]
        if isinstance(x, collections.deque):
            out = collections.deque(maxlen=x.maxlen)
            memo[oid] = out
            return [x, out, list(x), 0, out.append, False]
        if isinstance(x, set):
            out = set()
            memo[oid] = out
            return [x, out, list(x), 0, out.add, False]
        if dataclasses.is_dataclass(x) and not isinstance(x, type):
            out = copy.copy(x)
            memo[oid] = out
            # A field may be unset, such as one with init=False that the object never assigned.
            items = [(f.name, getattr(x, f.name)) for f in dataclasses.fields(x) if hasattr(x, f.name)]
            return [x, out, items, 0, lambda name, item: object.__setattr__(out, name, item), True]
        raise TypeError("a task result or file inside a custom object is not supported; use a dataclass or a list, "
                        "tuple, set, or dictionary")

    def end(frame):
        """Return the copy of a frame whose items are all done."""
        x, out = frame[0], frame[1]
        if out is not None:
            return out
        values = frame[6]
        active_immutable.discard(id(x))
        if hasattr(x, "_fields"):
            out = x.__class__(*values)
        elif isinstance(x, tuple):
            out = tuple(values)
        else:
            out = frozenset(values)
        memo[id(x)] = out
        return out

    symbol, leaves = _Symbol, _LEAF_TYPES
    stack = [begin(value)]
    keys = [None]  # for each frame on the stack, the key under which its parent puts its copy
    while True:
        frame = stack[-1]
        items, index, add, keyed = frame[2], frame[3], frame[4], frame[5]
        count = len(items)
        child_frame = None
        while index < count:
            if keyed:
                key, item = items[index]
            else:
                item = items[index]
            index += 1
            if isinstance(item, symbol):
                item = replace(item)
            elif item is not None and not isinstance(item, leaves) and scan(item):
                copied = memo.get(id(item))
                if copied is None:
                    child_frame = begin(item)
                    keys.append(key if keyed else None)
                    break
                item = copied
            if keyed:
                add(key, item)
            else:
                add(item)
        frame[3] = index
        if child_frame is not None:
            stack.append(child_frame)
            continue
        stack.pop()
        out = end(frame)
        key = keys.pop()
        if not stack:
            return out
        parent = stack[-1]
        if parent[5]:
            parent[4](key, out)
        else:
            parent[4](out)


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
        # for a task of this graph. This table and the next two hold only tasks that read something, because most calls
        # submitted in bulk read nothing.
        self._parents = {}
        self._results = {}  # key -> (graph id, key) of every task whose result it reads, in first-use order
        self._children = collections.defaultdict(set)  # key -> keys in this graph that read its result or files
        self._reads = {}  # key -> File and _FileRef objects the task reads, in first-use order
        self._outputs = collections.defaultdict(dict)  # key -> sandbox path -> NodeFile it writes
        self._hoisted = {}  # key -> names of the hoisted objects the task reads, for tasks that read any
        # key -> {(graph id, key): (selection_key(steps), steps)} for each task result that the task reads at exactly
        # one place in its arguments, through a selection, for tasks that read any. Such a task needs only the selected
        # value, and an Executor may have the producer write that value to a file of its own.
        self._selections = {}
        # id -> [object, tasks]: argument objects without symbols, scanned once while tasks hold them. An object passed
        # again is assumed unchanged, as an Executor already assumes when it serializes it once.
        self._plain = {}
        # key -> ((None, key), _Ref(None, key)): the task and the reference that arguments of tasks of this Graph store
        # for the whole result of task key. References never change, so every reader shares them, which spares an
        # allocation for each place a task reads a result.
        self._shared_refs = {}

    def __getstate__(self):
        state = dict(self.__dict__)
        del state["_id"]
        del state["_plain"]
        del state["_shared_refs"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._id = uuid.uuid4().hex
        self._plain = {}
        self._shared_refs = {}

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
        # The keys are copied in one built-in call, so another thread may add tasks meanwhile, as one may while an
        # Executor runs this Graph.
        return [Node(self, key) for key in list(self._tasks) if not self._children.get(key)]

    def __iter__(self):
        """Iterate over the Nodes of the tasks the Graph holds when iteration starts, so the loop may add tasks."""
        return (Node(self, key) for key in list(self._tasks))

    def __len__(self):
        return len(self._tasks)

    def __contains__(self, node):
        return isinstance(node, Node) and node.graph is self and node.key in self._tasks

    def __repr__(self):
        return f"Graph({len(self._tasks)} tasks)"

    # Construction.

    def _add(self, key, func, args, kwargs, foreign=False, on_symbol=None):
        """Record a task under key and return its Node. See _record()."""
        self._record(key, func, args, kwargs, foreign, on_symbol)
        return Node(self, key)

    def _record(self, key, func, args, kwargs, foreign=False, on_symbol=None):
        """Record a task under key. A graph built by an Executor passes foreign=True, so its tasks may read results of
        other graphs and Futures, and on_symbol to see each symbol. A user graph reads only its own tasks."""
        if not callable(func):
            raise TypeError("a task needs a callable followed by its arguments")
        if key in self._tasks:
            raise ValueError(f"a task with key {key!r} already exists")
        # Arguments of leaf types, the common case of calls submitted in bulk, hold no symbol and need no scan. Tasks
        # without keyword arguments share one empty dictionary, which nothing changes, since the garbage collector
        # tracks every dictionary.
        leaf = _LEAF_TYPE_SET
        for value in args:
            if type(value) not in leaf:
                break
        else:
            for value in kwargs.values():
                if type(value) not in leaf:
                    break
            else:
                self._tasks[key] = (func, tuple(args), dict(kwargs) if kwargs else _NO_KWARGS)
                return
        parents = {}  # (graph id, key) -> None, as an ordered set
        results = {}  # (graph id, key) of tasks whose result it reads -> None, as an ordered set
        reads = {}  # identity of each file read -> its File or _FileRef
        hoisted = {}  # names of hoisted objects -> None, as an ordered set
        places = {}  # (graph id, key) of each task result read -> steps of the first place it is read at
        repeated = set()  # (graph id, key) of each task result read at more than one place
        shared_refs = self._shared_refs

        def read_result(task, steps):
            # Replacement visits a container once however often the arguments hold it, and a symbol at each place, as
            # the library does when it runs the task, so repeated holds the results that the task receives, or a
            # selection of, at more than one place.
            if task in places:
                repeated.add(task)
            else:
                parents.setdefault(task, None)
                results[task] = None
                places[task] = steps

        def read_whole_result(key):
            # The whole result of a task of this Graph, the most common argument, takes the shared reference. This is
            # read_result() without its call, because a task may read many results.
            entry = shared_refs.get(key)
            if entry is None:
                entry = shared_refs[key] = ((None, key), _Ref(None, key))
            task = entry[0]
            if task in places:
                repeated.add(task)
            else:
                parents.setdefault(task, None)
                results[task] = None
                places[task] = ()
            return entry[1]

        def replace(symbol):
            if on_symbol is not None:
                on_symbol(symbol)
            if type(symbol) is Node and symbol.graph is self:
                return read_whole_result(symbol.key)
            # A Graph describes work independent of any run, so it takes Nodes and never Futures.
            if not foreign and hasattr(symbol.source if isinstance(symbol, _Selection) else symbol, "_executor"):
                raise TypeError("a Graph cannot hold a Future; use Executor.submit() for tasks built from Futures")
            ref = symbol._as_ref()
            if isinstance(ref, Hoisted):
                hoisted.setdefault(ref.name, None)
                return ref
            if isinstance(ref, File):
                reads.setdefault(ref, ref)
                return ref
            graph_id = None if ref.graph_id in (None, self._id) else ref.graph_id
            if graph_id is not None and not foreign:
                raise ValueError("a task reads a Node of another Graph")
            if isinstance(ref, _FileRef):
                parents.setdefault((graph_id, ref.key), None)
                ref = _FileRef(graph_id, ref.key, ref.path)
                reads.setdefault((graph_id, ref.key, ref.path), ref)
                return ref
            if graph_id is None and not ref.steps:
                return read_whole_result(ref.key)
            read_result((graph_id, ref.key), ref.steps)
            return _Ref(graph_id, ref.key, ref.steps)

        # Replacing all arguments at once keeps an object passed as several arguments one object.
        originals = (*args, *kwargs.values())
        args, kwargs = replace_symbols((tuple(args), dict(kwargs)), replace, self._plain)
        self._tasks[key] = (func, args, kwargs)
        # An argument that replacement kept as it is holds no symbol, so later tasks that pass it skip the scan.
        for original, stored in zip(originals, (*args, *kwargs.values())):
            if stored is original and original is not None and not isinstance(original, _LEAF_TYPES):
                self._plain.setdefault(id(original), [original, 0])[1] += 1
        if parents:
            self._parents[key] = tuple(parents)
        if results:
            self._results[key] = tuple(results)
        if reads:
            self._reads[key] = tuple(reads.values())
        if hoisted:
            self._hoisted[key] = tuple(hoisted)
        selections = {parent: (selection_key(steps), steps) for parent, steps in places.items()
                      if steps and parent not in repeated}
        if selections:
            self._selections[key] = selections
        for graph_id, parent in parents:
            if graph_id is None:
                self._children[parent].add(key)

    def _drop_arguments(self, key):
        """Keep only the function of a call that an Executor submitted to Workers, whose manifest holds its arguments,
        so the call does not hold them for as long as its Future lives. Arguments that later calls may pass again
        without a new scan (_plain) stay. Only an Executor's own Graph does this: a user Graph may be run again."""
        func, args, kwargs = self._tasks[key]
        plain = self._plain
        for value in (*args, *kwargs.values()):
            entry = plain.get(id(value))
            if entry is not None and entry[0] is value:
                return
        # Calls submitted in bulk pass one function, so a call shares the record of the call before it when it can,
        # and millions of calls that their Futures hold cost the garbage collector no record each.
        previous = self._tasks.get(key - 1) if type(key) is int else None
        if previous is not None and previous[0] is func and not previous[1] and previous[2] is _NO_KWARGS:
            self._tasks[key] = previous
        else:
            self._tasks[key] = (func, (), _NO_KWARGS)

    def _forget_plain(self, args, kwargs):
        for value in (*args, *kwargs.values()):
            entry = self._plain.get(id(value))
            if entry is not None and entry[0] is value:
                entry[1] -= 1
                if not entry[1]:
                    del self._plain[id(value)]

    def _remove(self, key):
        """Forget a task that nothing can ask for again, with what it recorded. Only an Executor's own Graph does this,
        for calls whose Futures are gone."""
        _, args, kwargs = self._tasks.pop(key)
        # A call whose Future the Executor held keeps no arguments (_drop_arguments()), and most calls read nothing,
        # so the tables below are usually empty, and the call of a Future that the program drops looks up none of them.
        if args or kwargs:
            self._forget_plain(args, kwargs)
        if self._parents:
            for graph_id, parent in self._parents.pop(key, ()):
                if graph_id is None and parent in self._children:
                    self._children[parent].discard(key)
                    if not self._children[parent]:
                        del self._children[parent]
        for table in (self._results, self._reads, self._children, self._outputs, self._hoisted, self._selections,
                      self._shared_refs):
            if table:
                table.pop(key, None)

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
