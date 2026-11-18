# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""A Graph holds tasks, the data they pass to each other, and the rules that add tasks as others complete. An Executor
(executor.py) runs it on TaskVine Workers, or in this process, and a later run continues it without recomputing what
completed. The tasks that Executor.submit() starts are Tasks too, which the Executor holds in a graph of its own."""

import collections
import collections.abc
import copy
import dataclasses
import itertools
import os
import string
import threading
import weakref


class _Symbol:
    """Base of the objects that stand for data a task reads: task results, selections from them, and files. Task
    arguments may contain symbols anywhere inside lists, tuples, sets, dictionary values, and dataclasses."""

    __slots__ = ()

    def _as_ref(self):
        """Return the lightweight reference stored in task arguments in place of this symbol."""
        raise NotImplementedError


# The lock that a local run holds while a task changes the working directory of the process, which the executor module
# installs, so that a relative path given in another thread meanwhile resolves against the caller's directory.
_working_directory_lock = None


def _absolute(path):
    """Return the absolute path of a path, resolved while no local task changed the working directory."""
    lock = _working_directory_lock
    if lock is None:
        return os.path.abspath(path)
    with lock:
        return os.path.abspath(path)


class File(_Symbol):
    """A file on the local filesystem that tasks read. A task receives the path of its copy in the task sandbox."""

    __slots__ = ("path",)

    def __init__(self, path):
        path = os.fspath(path)
        if not isinstance(path, str):
            raise TypeError("a File path must be a string or path-like object")
        if "\0" in path:
            raise ValueError("a File path contains a null byte")
        self.path = _absolute(path)

    def _as_ref(self):
        return self

    def __eq__(self, other):
        return isinstance(other, File) and other.path == self.path

    def __hash__(self):
        return hash((File, self.path))

    def __repr__(self):
        return f"File({self.path!r})"


class Shared(_Symbol):
    """An object that the library on each Worker loads once, named by the shared argument of the Executor. A task that
    takes vg.Shared(name) as an argument receives that object. Every task gets its own copy, so changes a task makes
    to it stay within that task."""

    __slots__ = ("name",)

    def __init__(self, name):
        if not isinstance(name, str) or not name:
            raise TypeError("a shared object is named by a non-empty string")
        self.name = name

    def _as_ref(self):
        return self

    def __eq__(self, other):
        return isinstance(other, Shared) and other.name == self.name

    def __hash__(self):
        return hash((Shared, self.name))

    def __repr__(self):
        return f"Shared({self.name!r})"


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
        # A Graph passed as an argument is data, and the tasks it holds are not ones the task reads.
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


# Task states. Only the thread of a run changes them while it runs, and never while a rule runs.
_NEW = 0  # not started: never submitted, or returned to not-started by a stopped run
_RUNNING = 1  # submitted and not finished
_COMPLETED = 2
_FAILED = 3
_CANCELLED = 4

# Bytes of standard output that a command task keeps as its result.
COMMAND_RESULT_LIMIT = 1 << 20
# Bytes of a file that TaskFile.content reads at most.
CONTENT_LIMIT = 16 << 20


class TaskError(RuntimeError):
    """A task failed, or it could not run because a task it reads failed. task is the Task, and source the Task whose
    own failure explains it, which is task itself when it failed on its own.

    reason says why the source failed:

    - "function": its function raised an exception, or its command exited with an error status
    - "signal": its function was killed by a signal
    - "workers": Workers kept disconnecting while they ran it, as when it exceeds their memory or crashes them
    - "input": a file it reads is missing at the Manager
    - "retries": more of its attempts failed for other reasons than the setting max-retries allows
    - "submit": it could not be submitted, as when an argument cannot be serialized
    - "result": it completed, but its result cannot be loaded in this process or sent by its Worker

    exception is the exception that the function of the source raised, loaded in this process with the source's
    traceback as a note, or None. exit_code is the exit status of the source's command when it exited with an error
    status, and None otherwise; the message then ends with the end of its standard error."""

    def __init__(self, message, task, source, reason=None, exception=None, exit_code=None):
        super().__init__(message)
        self.task = task
        self.source = source
        self.reason = reason
        self.exception = exception
        self.exit_code = exit_code


class Command:
    """A shell command line that tasks run, as a function is for Python tasks: g.add(command, **values) or
    executor.submit(command, **values) makes a task that runs it in its sandbox, and a Command selects its tasks in
    Graph.on() and Graph.find() as a function does.

    Placeholders such as {name} take the values given by name, shell-quoted: a File, or a file a task writes
    (task.file(name)), becomes its path in the sandbox, a Task becomes str() of its result, and any other value str()
    of the value. Every placeholder needs a value, and every value a placeholder. Write {{ and }} for braces. The text a
    command writes to standard output, at most 1 MiB, is its result, and an exit status other than zero fails it, with
    the end of its standard error in its TaskError. cores is the number of cores it uses on a Worker, where a Python
    task uses one.

    Output files. A file that the command writes in its sandbox is kept only when it was declared: ask for it with
    task.file(name) before the task starts, where name is the relative path the command writes, the same text its
    placeholder gets. A declared file is a required output, so a command that does not write it fails. Pass the
    TaskFile to other tasks, or read it once the task completed with its path, content, or text, whether or not a task
    reads it. A task of a Graph starts when Executor.run() runs the Graph, so its files are declared after Graph.add().
    A task that Executor.submit() returns has started already and has no files: put a command whose files matter in a
    Graph."""

    __slots__ = ("_template", "_cores", "_fields", "__weakref__")

    def __init__(self, template, cores=1):
        if not isinstance(template, str) or not template.strip():
            raise TypeError("a Command takes a command line as a non-empty string")
        if "\0" in template:
            raise ValueError("a command line contains a null byte")
        if type(cores) is not int or cores < 1:
            raise ValueError("cores must be a positive integer")
        fields = {}
        try:
            parsed = list(string.Formatter().parse(template))
        except ValueError as error:
            raise ValueError(f"invalid command line {template!r}: {error}") from None
        for _, field, spec, conversion in parsed:
            if field is None:
                continue
            if not field.isidentifier() or spec or conversion:
                raise ValueError(f"a placeholder of a command line is a name in braces such as {{name}}, not {{{field}}}")
            fields[field] = None
        self._template = template
        self._cores = cores
        self._fields = tuple(fields)

    @property
    def template(self):
        return self._template

    @property
    def cores(self):
        return self._cores

    def __repr__(self):
        return f"Command({self._template!r}, cores={self._cores})"


class TaskFile(_Symbol):
    """A file that a task writes in its sandbox, which other tasks read when it is passed to them. Asking for a file
    of a task before the task starts declares it, and the task fails when it does not write it. Once the task completed,
    path, content, and text read the file, whether or not a task reads it."""

    __slots__ = ("task", "name")

    def __init__(self, task, name):
        self.task = task
        self.name = name

    def _as_ref(self):
        return _FileRef(None, self.task.key, self.name)

    @property
    def exists(self):
        """Whether the task wrote the file: it completed, and the file was declared before it ran."""
        task = self.task
        return task._state == _COMPLETED and task._record is not None and self.name in task._record.outputs

    @property
    def content(self):
        """The bytes of the file, fetched when asked for. A file above 16 MiB raises ValueError: pass it to a task, or
        read TaskFile.path."""
        return self.task._graph._bound(self.task)._file(self, "content")

    @property
    def text(self):
        """The content of the file, decoded as UTF-8."""
        return self.content.decode("utf-8")

    @property
    def path(self):
        """A local path of the file, fetched to the output directory of the Executor when asked for. It stays there
        after the Executor closed."""
        return self.task._graph._bound(self.task)._file(self, "path")

    def __eq__(self, other):
        return isinstance(other, TaskFile) and other.task is self.task and other.name == self.name

    def __hash__(self):
        return hash((TaskFile, id(self.task), self.name))

    def __repr__(self):
        return f"{self.task!r}.file({self.name!r})"


def _readers_of(task):
    """Return the readers of a task as a sequence."""
    readers = task._readers
    if readers is None:
        return ()
    if isinstance(readers, list):
        if task._graph._dropped is not None:
            # A submitted task refers to its readers weakly, and lists those that still exist.
            return [reader for reader in (ref() for ref in readers) if reader is not None]
        return readers
    return (readers,)


def _restore(task, value):
    """Return arguments of a task with their references replaced by the Tasks, selections, and files they stand for."""
    graph = task._graph
    parents = {(graph._graph_id(parent), parent.key): parent for parent in task._parents}

    def restore(ref):
        if type(ref) is _Ref:
            parent = parents[(ref.graph_id, ref.key)]
            return _Selection(parent, ref.steps) if ref.steps else parent
        if type(ref) is _FileRef:
            return parents[(ref.graph_id, ref.key)].file(ref.path)
        return ref

    return replace_symbols(value, restore)


class Task(_Selectable):
    """A call of a function, or a run of a Command, with its arguments. Graph.add() returns the Task of a task that
    runs when an Executor runs the Graph, and Executor.submit() the Task of a task that starts at once. Pass a Task as
    an argument of another task to give that task its result, select from it with task[key] or task.attr(name), and
    name a file it writes with task.file(name).

    result() returns its result, done(), failed(), and cancelled() tell how far it is, and cancel() cancels it. A Task
    keeps the Tasks it reads, which args and kwargs return."""

    __slots__ = ("_graph", "key", "_index", "_func", "_args", "_kwargs", "_parents", "_results", "_reads", "_shared",
                 "_selections", "_outputs", "_readers", "_rules", "_state", "_error", "_record", "_rule_pending",
                 "__weakref__")

    def __init__(self, graph, key, func):
        self._graph = graph
        # The task's key in its Graph: its number, or its Dask key in a Graph from adaptors.from_dask().
        self.key = key
        self._index = 0  # insertion order in the Graph
        self._func = func
        self._args = ()  # arguments, with every symbol replaced by its reference
        self._kwargs = _NO_KWARGS
        self._parents = ()  # Tasks whose results or files it reads, in first-use order
        self._results = None  # Tasks whose results it reads, in first-use order, or None
        self._reads = None  # Files and _FileRefs it reads, or None
        self._shared = None  # names of shared objects it reads, or None
        # {(graph id, key): (selection_key(steps), steps)} of each task result it reads at exactly one place, through a
        # selection, or None. Such a task needs only the selected value, which its producer may write to a file.
        self._selections = None
        self._outputs = None  # {sandbox path: TaskFile} of the files it writes, or None
        # Tasks of the Graph that read it, in the order they were added: None, the one reader, or a list of several,
        # since most tasks have one reader and a list for it would take more memory than the task. A submitted task
        # holds a list of weak references instead, so it does not keep its readers. See _readers_of().
        self._readers = None
        self._rules = None  # rules registered for this task alone, or None
        self._state = _NEW
        # The failure of a failed task: (_FAILED, source Task or None for the task itself, message, reason, exit code).
        # Only a source holds its message, reason, and exit code.
        self._error = None
        self._record = None  # the Executor's record of its latest submission
        self._rule_pending = False  # its rules are due once it completes

    def __del__(self):
        # A submitted task that nothing refers to anymore can be read by nobody, so its Executor lets it go. This only
        # queues the record, because a collection may run anywhere.
        try:
            dropped = self._graph._dropped
            record = self._record
        except AttributeError:
            # An interrupt ended __init__ before it set them, so the task was never submitted.
            return
        if dropped is not None and record is not None:
            dropped.append(record)

    @property
    def func(self):
        """The function or Command the task runs."""
        return self._func

    @property
    def args(self):
        """The positional arguments, with the Tasks, selections, and files they hold."""
        return _restore(self, self._args)

    @property
    def kwargs(self):
        """The keyword arguments, with the Tasks, selections, and files they hold."""
        return dict(_restore(self, self._kwargs))

    @property
    def readers(self):
        """The tasks that read this task's result or files, in the order they were added: tasks of its Graph, or, for
        a submitted task, the submitted tasks that still exist."""
        return list(_readers_of(self))

    def result(self, timeout=None):
        """Return the result of the task: what its function returned, or the text its command wrote to standard
        output. A submitted task that is still running is waited for, at most timeout seconds when one is given, after
        which TimeoutError is raised. A failed task raises its TaskError, and a cancelled one RuntimeError.

        A task of a Graph has a result once an Executor ran the Graph. Before that, and while it did not finish, as in
        a rule that asks for a task that is still running, RuntimeError is raised, because only Executor.run() waits
        for the tasks of a Graph. A result that the Executor released meanwhile is computed again."""
        return self._graph._bound(self)._result(self, timeout)

    def done(self):
        """Return whether the task finished: it completed, failed, or was cancelled."""
        if self._state == _RUNNING:
            self._graph._poll(self)
        return self._state >= _COMPLETED

    def failed(self):
        """Return whether the task failed, or could not run because a task it reads failed."""
        if self._state == _RUNNING:
            self._graph._poll(self)
        return self._state == _FAILED

    def cancelled(self):
        """Return whether cancel() cancelled the task, or a task it reads."""
        if self._state == _RUNNING:
            self._graph._poll(self)
        return self._state == _CANCELLED

    def cancel(self):
        """Cancel the task unless it finished, and the tasks that read it: those not started never run, and running
        ones are stopped. A completed task stays completed, and a cancelled task stays cancelled. A task of a Graph is
        cancelled by a rule, during a run, and a submitted task at any time."""
        self._graph._cancel(self)

    def file(self, name):
        """Return the file this task writes at name, a relative path in its sandbox. Asking for the same name again
        returns the same file. A file is declared by asking for it before the task starts, so a submitted task, which
        started already, has none."""
        name = os.fspath(name)
        if not isinstance(name, str):
            raise TypeError("a file name must be a string or path-like object")
        if "\0" in name:
            raise ValueError("a file name contains a null byte")
        normalized = os.path.normpath(name)
        if not name or normalized in ("", ".") or os.path.isabs(name) or normalized == ".." or normalized.startswith("../"):
            raise ValueError("a file name must be a non-empty relative path inside the task sandbox")
        outputs = self._outputs
        if outputs is None:
            outputs = self._outputs = {}
        file = outputs.get(normalized)
        if file is None:
            file = outputs[normalized] = TaskFile(self, normalized)
        return file

    def _as_ref(self):
        return _Ref(None, self.key)

    def __repr__(self):
        return _label(self)


def _label(task):
    """Return how messages name a task: its key and the name of its function, or the start of its command."""
    return _name_task(task.key, task._func)


def _name_task(key, func):
    if isinstance(func, Command):
        words = func.template.split()
        name = words[0] if words else "command"
    else:
        name = getattr(func, "__name__", None) or type(func).__name__
    return f"Task({key!r}, {name})"


class _Rule:
    """A function that Graph.on() registered, called with each selected task that completes."""

    __slots__ = ("function", "index")

    def __init__(self, function, index):
        self.function = function
        self.index = index


class _FunctionIndex:
    """Values listed by function or Command: by the function itself, which tells bound methods of one object apart from
    others, or by its id when it cannot be hashed."""

    __slots__ = ("hashed", "unhashed")

    def __init__(self):
        self.hashed = {}
        self.unhashed = {}  # id -> (function, values)

    def get(self, func):
        try:
            return self.hashed.get(func)
        except TypeError:
            entry = self.unhashed.get(id(func))
            return None if entry is None else entry[1]

    def add(self, func, value):
        try:
            values = self.hashed.get(func)
            if values is None:
                values = self.hashed[func] = []
        except TypeError:
            entry = self.unhashed.get(id(func))
            if entry is None:
                entry = self.unhashed[id(func)] = (func, [])
            values = entry[1]
        # A value added for two selectors that stand for one function, such as two bound methods, is listed once.
        if not values or values[-1] is not value:
            values.append(value)


# Numbers that tell Graphs apart in the references of tasks that read tasks of another Graph.
_graph_ids = itertools.count(1)


class Graph:
    """Tasks, the data they pass to each other, and rules that add tasks as others complete.

    add() adds a task and returns its Task. on() registers a rule, which a run calls with each selected task that
    completes, one at a time and in completion order, in the thread that called Executor.run(). While a rule runs, task
    states do not change, and the rule may add tasks, which run after it returns, cancel tasks, or stop the run. find()
    returns the selected tasks. Executor.run(graph) blocks until no task runs and no rule is due, and returns the
    results of the completed sinks, the tasks that no task reads. Running the Graph again runs only what did not
    finish, and a stopped run continues.

    A Graph describes work and holds no settings: the Executor decides where it runs. The first Executor that runs a
    Graph holds the results of its tasks, so later runs of the Graph use that Executor."""

    # The Executor that ran the Graph, which holds the results of its tasks, or None before the first run.
    _executor = None
    # The queue of the records of collected tasks, in the graph of an Executor's submitted tasks (_Calls).
    _dropped = None

    def __init__(self):
        self._tasks = {}  # key -> Task, in insertion order
        self._next_key = 1
        self._by_func = _FunctionIndex()  # function or Command -> its Tasks, in insertion order
        self._rule_index = _FunctionIndex()  # function or Command -> rules that select its tasks
        self._rule_count = 0
        # id -> [object, tasks]: argument objects without symbols, scanned once while tasks hold them. An object passed
        # again is assumed unchanged.
        self._plain = {}
        # key -> _Ref(None, key) that arguments of tasks store for the whole result of task key, shared by every
        # reader.
        self._shared_refs = {}
        self._run = None  # the _Run of the run in progress
        # Completed tasks whose rules are due, in completion order. A stopped run leaves those it did not call for the
        # next run.
        self._due = collections.deque()
        self._id = next(_graph_ids)
        self._ref = weakref.ref(self)  # how the records of an Executor refer to the Graph
        # The Executor's records of the tasks submitted, which it lets go when the Graph is collected.
        self._records = []

    def __repr__(self):
        return f"Graph({len(self._tasks)} tasks)"

    def __getstate__(self):
        raise TypeError("a Graph holds rules and the state of its runs, and cannot be pickled")

    # The user interface.

    def add(self, func, /, *args, **kwargs):
        """Add a task that calls func with these arguments, or runs a Command with values for its placeholders, and
        return its Task. Tasks of this Graph, their selections and files, and Files may appear anywhere inside the
        arguments of a function. During a run, only a rule may add tasks, and they run once it returns."""
        self._check_change("add")
        while self._next_key in self._tasks:
            self._next_key += 1
        key = self._next_key
        self._next_key += 1
        return self._add(key, func, args, kwargs)

    def on(self, *selectors):
        """Register the decorated function as a rule: rule(task) is called with each selected task that completes. A
        selector is a function or Command, which selects the tasks of this Graph that run it, including those added
        later, or a Task of this Graph. A task that failed or was cancelled calls no rule, and a rule that raises fails
        the run. Rules are the only callbacks: they select tasks of their Graph, never the tasks that
        Executor.submit() returns, whose results the caller reads itself."""
        if not selectors:
            raise TypeError("on() takes at least one function, Command, or Task")
        for selector in selectors:
            self._check_selector(selector)
        if self._run is not None:
            self._check_change("on")

        def register(function):
            if not callable(function):
                raise TypeError("a rule must be callable")
            self._rule_count += 1
            rule = _Rule(function, self._rule_count)
            for selector in {id(selector): selector for selector in selectors}.values():
                if isinstance(selector, Task):
                    if selector._rules is None:
                        selector._rules = []
                    selector._rules.append(rule)
                    tasks = (selector,)
                else:
                    self._rule_index.add(selector, rule)
                    tasks = self._by_func.get(selector) or ()
                # Running tasks that a rule now selects call it when they complete.
                for task in tasks:
                    if task._state == _RUNNING:
                        task._rule_pending = True
            return function

        return register

    def find(self, *selectors):
        """Return the selected tasks, in the order they were added: the tasks of each function or Command, and each
        Task given, or every task when none is given. The list is a snapshot. During a run, only a rule may call it."""
        run = self._run
        if run is not None and not (run.in_rule and threading.current_thread() is run.thread):
            raise RuntimeError("during a run, only a rule may call find()")
        if not selectors:
            return list(self._tasks.values())
        found = []
        for selector in selectors:
            self._check_selector(selector)
            if isinstance(selector, Task):
                found.append((selector,))
            else:
                found.append(self._by_func.get(selector) or ())
        if len(found) == 1:
            return list(found[0])
        union = {id(task): task for tasks in found for task in tasks}
        return sorted(union.values(), key=_INDEX)

    def stop(self):
        """End the run once the rule that calls this returns: no further rule is called, running tasks are stopped, and
        Executor.run() returns. Tasks that did not finish go back to not started, so a later run continues them. Only a
        rule may stop the run."""
        run = self._check_change("stop", rule_only=True)
        run.stop_requested = True

    # Construction.

    def _check_change(self, what, rule_only=False):
        """Raise unless the Graph may change now: outside a run, or inside a rule in the run's thread. Return the run."""
        run = self._run
        if run is None:
            if rule_only:
                raise RuntimeError(f"{what}() may be called only inside a rule during a run")
            return None
        if not run.in_rule or threading.current_thread() is not run.thread:
            raise RuntimeError(f"during a run, only a rule may call {what}()")
        return run

    def _check_selector(self, selector):
        if isinstance(selector, Task):
            if selector._graph is not self:
                raise ValueError("a rule or find() selects tasks of its own Graph, and this Task is not one of them")
        elif isinstance(selector, _Symbol) or not (callable(selector) or isinstance(selector, Command)):
            raise TypeError("a selector is a function, a Command, or a Task")

    def _add(self, key, func, args, kwargs):
        """Record a task under key and return it."""
        command = isinstance(func, Command)
        if command:
            if args:
                raise TypeError("a Command takes its values by name, as in add(command, name=value)")
            missing = [name for name in func._fields if name not in kwargs]
            extra = [name for name in kwargs if name not in func._fields]
            if missing or extra:
                problems = []
                if missing:
                    problems.append(f"no value for {', '.join(missing)}")
                if extra:
                    problems.append(f"no placeholder for {', '.join(extra)}")
                raise TypeError(f"{func!r}: {'; '.join(problems)}")
        elif isinstance(func, _Symbol) or not callable(func):
            raise TypeError("a task needs a function or a Command followed by its arguments")
        if key in self._tasks:
            raise ValueError(f"a task with key {key!r} already exists")
        task = Task(self, key, func)
        self._record_arguments(task, args, kwargs)
        self._insert(task)
        return task

    def _insert(self, task):
        """Take a new task into the Graph, and tell the tasks it reads and the run in progress."""
        task._index = len(self._tasks)
        self._tasks[task.key] = task
        self._by_func.add(task._func, task)
        for parent in task._parents:
            readers = parent._readers
            if readers is None:
                parent._readers = task
            elif isinstance(readers, list):
                readers.append(task)
            else:
                parent._readers = [readers, task]
        run = self._run
        if run is not None:
            run.added.append(task)
            run.task_added()

    def _graph_id(self, parent):
        """Return the graph id in the references of this Graph's tasks to a task: None for a task of this Graph."""
        return None if parent._graph is self else parent._graph._id

    def _check_parent(self, parent, what):
        """Raise unless tasks of this Graph may read a task: only its own."""
        if parent._graph is not self:
            raise ValueError(f"a task reads {what} of another Graph; a Graph holds every task its tasks read")

    def _ref_to(self, parent):
        """Return the reference that arguments store for the whole result of a task, shared by every reader."""
        ref = self._shared_refs.get(parent.key)
        if ref is None:
            ref = self._shared_refs[parent.key] = _Ref(None, parent.key)
        return ref

    def _record_arguments(self, task, args, kwargs):
        """Store the arguments of a task with every symbol replaced by its reference, and what it reads."""
        if isinstance(task._func, Command):
            for name, value in kwargs.items():
                if isinstance(value, _Selection):
                    raise TypeError(f"the value of {{{name}}} is a selection, which a command cannot read; read it in "
                                    f"a Python task and pass that task")
                if isinstance(value, Shared):
                    raise TypeError(f"the value of {{{name}}} is a shared object, which only Python tasks take")
                if not isinstance(value, _Symbol) and _scanner()(value):
                    raise TypeError(f"the value of {{{name}}} holds tasks or files inside, which a command cannot read; "
                                    f"pass each as a value of its own")
        # Arguments of leaf types, the common case of calls added in bulk, hold no symbol and need no scan. Tasks
        # without keyword arguments share one empty dictionary.
        leaf = _LEAF_TYPE_SET
        for value in args:
            if type(value) not in leaf:
                break
        else:
            for value in kwargs.values():
                if type(value) not in leaf:
                    break
            else:
                task._args = tuple(args)
                task._kwargs = dict(kwargs) if kwargs else _NO_KWARGS
                return
        parents = {}  # Task -> None, as an ordered set
        results = {}  # Tasks whose result it reads -> None, as an ordered set
        reads = {}  # identity of each file read -> its File or _FileRef
        shared = {}  # names of shared objects -> None, as an ordered set
        places = {}  # Task of each result read -> steps of the first place it is read at
        repeated = set()  # Tasks whose result is read at more than one place
        tasks = self._tasks
        graph_id = self._graph_id

        def read_result(parent, steps):
            # Replacement visits a container once however often the arguments hold it, and a symbol at each place, as
            # the library does when it runs the task, so repeated holds the results read at more than one place.
            if parent in places:
                repeated.add(parent)
            else:
                parents.setdefault(parent, None)
                results[parent] = None
                places[parent] = steps

        def parent_of(symbol_key):
            parent = tasks.get(symbol_key)
            if parent is None:
                raise ValueError(f"a task reads task {symbol_key!r}, which is not in this Graph")
            return parent

        def read_file(parent, path):
            parents.setdefault(parent, None)
            ref = _FileRef(graph_id(parent), parent.key, path)
            reads.setdefault((ref.graph_id, parent.key, path), ref)
            return ref

        def replace(symbol):
            kind = type(symbol)
            if kind is Task:
                self._check_parent(symbol, "a Task")
                read_result(symbol, ())
                return self._ref_to(symbol)
            if kind is Shared:
                shared.setdefault(symbol.name, None)
                return symbol
            if kind is File:
                reads.setdefault(symbol, symbol)
                return symbol
            if kind is TaskFile:
                parent = symbol.task
                self._check_parent(parent, "a file of a Task")
                if parent._state != _NEW and (parent._record is None or symbol.name not in parent._record.outputs):
                    raise ValueError(f"the file {symbol.name!r} of {parent!r} was asked for after the task started; "
                                     f"ask for its files before it starts, as when it is added to its Graph")
                return read_file(parent, symbol.name)
            if kind is _FileRef:
                # A reference that an adaptor built, to a task of this Graph.
                parent = parent_of(symbol.key)
                parent.file(symbol.path)
                return read_file(parent, symbol.path)
            if isinstance(symbol, _Selection) or kind is _Ref:
                if isinstance(symbol, _Selection) and isinstance(symbol.source, Task):
                    parent, steps = symbol.source, symbol.steps
                    self._check_parent(parent, "a Task")
                else:
                    # A reference that an adaptor built, or a selection from one, to a task of this Graph.
                    source = symbol._as_ref()
                    parent, steps = parent_of(source.key), source.steps
                read_result(parent, steps)
                if not steps:
                    return self._ref_to(parent)
                return _Ref(graph_id(parent), parent.key, steps)
            raise TypeError(f"{symbol!r} cannot be a task argument")

        # Replacing all arguments at once keeps an object passed as several arguments one object.
        originals = (*args, *kwargs.values())
        plain = self._plain
        args, kwargs = replace_symbols((tuple(args), dict(kwargs)), replace, () if plain is None else plain)
        task._args = args
        task._kwargs = kwargs if kwargs else _NO_KWARGS
        if plain is not None:
            # An argument that replacement kept as it is holds no symbol, so later tasks that pass it skip the scan.
            for original, stored in zip(originals, (*args, *kwargs.values())):
                if stored is original and original is not None and not isinstance(original, _LEAF_TYPES):
                    plain.setdefault(id(original), [original, 0])[1] += 1
        if parents:
            task._parents = tuple(parents)
        if results:
            task._results = tuple(results)
        if reads:
            task._reads = tuple(reads.values())
        if shared:
            task._shared = tuple(shared)
        selections = {(graph_id(parent), parent.key): (selection_key(steps), steps) for parent, steps in places.items()
                      if steps and parent not in repeated}
        if selections:
            task._selections = selections

    # Rules.

    def _has_rules(self, task):
        return task._rules is not None or self._rule_index.get(task._func) is not None

    def _rules_of(self, task):
        """Return the rules that select a task, each once, in the order they were registered."""
        by_func = self._rule_index.get(task._func) or ()
        if task._rules is None:
            return list(by_func)
        if not by_func:
            return list(task._rules)
        return sorted({id(rule): rule for rule in (*by_func, *task._rules)}.values(), key=_INDEX_OF_RULE)

    # What the Tasks of the Graph ask of the Executor that ran it.

    def _bound(self, task):
        """Return the Executor that holds the results of the Graph's tasks, or raise before the first run."""
        executor = self._executor
        if executor is None:
            raise RuntimeError(f"{task!r} has not run: Executor.run() runs its Graph")
        return executor

    def _poll(self, task):
        """Bring the state of a running task up to date. The tasks of a Graph change only during a run."""

    def _cancel(self, task):
        run = self._check_change("cancel", rule_only=True)
        run.cancel((task,))


class _Calls(Graph):
    """The graph of the tasks that an Executor's submit() started. It differs from a Graph that a user builds in what it
    keeps: a submitted task lives while something refers to its Task, so the graph holds its tasks weakly, tasks hold
    their readers weakly, and a collected task queues its record in dropped, from where the Executor lets it go. Its
    tasks may read tasks of the Graphs that the Executor ran, and no rule selects them."""

    def __init__(self, executor, dropped):
        super().__init__()
        self._tasks = weakref.WeakValueDictionary()
        self._plain = None  # arguments are scanned for every task, so the graph keeps none of them
        self._shared_refs = None
        self._owner = weakref.ref(executor)
        self._dropped = dropped

    @property
    def _executor(self):
        return self._owner()

    def _bound(self, task):
        executor = self._owner()
        if executor is None:
            raise RuntimeError(f"the Executor that started {task!r} is gone")
        return executor

    def _insert(self, task):
        task._index = task.key
        self._tasks[task.key] = task
        reader = weakref.ref(task)
        for parent in task._parents:
            if parent._graph is not self:
                continue
            readers = parent._readers
            if readers is None:
                readers = parent._readers = []
            elif len(readers) >= 8 and not len(readers) & (len(readers) - 1):
                # Readers that were collected leave the list whenever it reached twice the size it had after the last
                # time, so a task that many tasks read in turn keeps a list of the size of those that exist.
                readers[:] = [ref for ref in readers if ref() is not None]
            readers.append(reader)

    def _check_parent(self, parent, what):
        """Raise unless a submitted task may read a task: one submitted before it, or one that the Executor ran."""
        graph = parent._graph
        if graph is self:
            return
        if graph._executor is not self._owner():
            raise ValueError(f"a submitted task reads {what} that this Executor neither started nor ran")
        if parent._record is None:
            raise ValueError(f"a submitted task reads {parent!r}, which has not started; Executor.run() runs its Graph")

    def _ref_to(self, parent):
        return _Ref(self._graph_id(parent), parent.key)

    def _poll(self, task):
        executor = self._owner()
        if executor is not None:
            executor._poll()

    def _cancel(self, task):
        self._bound(task)._cancel_call(task)


def _INDEX(task):
    return task._index


def _INDEX_OF_RULE(rule):
    return rule.index
