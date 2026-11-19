# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""A Graph holds tasks and the data they pass to each other. Its tasks are descriptions until an Executor (executor.py)
takes the Graph (Executor.submit(graph) or Executor.run(graph)), and from then on they are live."""

import collections
import collections.abc
import copy
import dataclasses
import itertools
import os
import re
import shutil
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


# The lock that a local runner holds while a task changes the working directory of the process, which the executor
# module installs where a thread cannot have a working directory of its own, so that a relative path given in another
# thread meanwhile resolves against the caller's directory.
_working_directory_lock = None


def _absolute(path):
    """Return the absolute path of a path, resolved while no local task changed the working directory."""
    lock = _working_directory_lock
    if lock is None:
        return os.path.abspath(path)
    with lock:
        return os.path.abspath(path)


# Names that the runtime owns in a task sandbox: the library protocol (infile, outfile), the Worker (.taskvine.*), the
# resource monitor (cctools-monitor*), and vine_graph itself (vine-graph-*). An input never takes one of them.
RESERVED_NAMES = frozenset({"infile", "outfile", ".", ".."})
RESERVED_PREFIXES = (".taskvine.", "vine-graph-", "cctools-monitor")


def reserved_name(name):
    """Return whether a name in a task sandbox belongs to the runtime."""
    return name in RESERVED_NAMES or name.startswith(RESERVED_PREFIXES)


class _Input(_Symbol):
    """Base of the inputs that a task reads under a name of its own in its sandbox, File and URL, which are equal when
    their location and name are."""

    __slots__ = ()

    def _as_ref(self):
        return self

    def __eq__(self, other):
        return isinstance(other, type(self)) and other._location() == self._location() and other.name == self.name

    def __hash__(self):
        return hash((type(self), self._location(), self.name))

    def __repr__(self):
        if self.name is None:
            return f"{type(self).__name__}({self._location()!r})"
        return f"{type(self).__name__}({self._location()!r}, name={self.name!r})"


class File(_Input):
    """A file or directory on the local filesystem that tasks read. A task receives the path of its copy in the task
    sandbox, named like the file when no other input of the task has that name, or name when given, as for a tool that
    reads a fixed name such as File("run7.yaml", name="config.yaml"). A File may be a directory, which the task receives
    with its tree. Never write to an input: a task gets a link to the copy that other tasks on its Worker share, or to
    the file itself in a local Executor."""

    __slots__ = ("path", "name")

    def __init__(self, path, name=None):
        path = os.fspath(path)
        if not isinstance(path, str):
            raise TypeError("a File path must be a string or path-like object")
        if "\0" in path:
            raise ValueError("a File path contains a null byte")
        if _SCHEME.match(path):
            raise ValueError(f"{path!r} is a URL; use vg.URL(...) for a remote input")
        self.path = _absolute(path)
        self.name = _input_name(name, "File")

    def _location(self):
        return self.path


# The start of a URL: a scheme followed by ://.
_SCHEME = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://")
# The schemes of remote inputs that Workers fetch: with curl, and with xrdcp for XRootD.
URL_SCHEMES = ("http", "https", "ftp", "file", "root")


def _input_name(name, what):
    """Return the name that an input is given in a task sandbox, checked, or None."""
    if name is None:
        return None
    name = os.fspath(name)
    if not isinstance(name, str):
        raise TypeError(f"a {what} name must be a string or path-like object")
    if "/" in name or _sandbox_name(name) != name:
        raise ValueError(f"a {what} name is one file name without a directory, not {name!r}")
    if reserved_name(name):
        raise ValueError(f"the name {name!r} is reserved for the runtime in a task sandbox; choose another")
    return name


class URL(_Input):
    """A remote file that tasks read, which each Worker fetches once from its origin: an http, https, ftp, or file URL,
    fetched with curl, or a root:// URL of XRootD, fetched with xrdcp and the X509 proxy that X509_USER_PROXY names. A
    task receives the path of its copy, named like the last part of the URL path when no other input of the task has
    that name, or name when given. A task whose URL keeps failing to be fetched fails with reason "input" and the error
    of its origin. A local Executor downloads it once. Never write to an input."""

    __slots__ = ("url", "name")

    def __init__(self, url, name=None):
        if not isinstance(url, str):
            raise TypeError("a URL is a string")
        if "\0" in url or any(c.isspace() for c in url):
            raise ValueError("a URL holds no spaces or null bytes")
        match = _SCHEME.match(url)
        scheme = match.group(0)[:-3].lower() if match else None
        if scheme not in URL_SCHEMES:
            raise ValueError(f"a URL starts with one of {', '.join(s + '://' for s in URL_SCHEMES)}, not {url!r}")
        self.url = url
        self.name = _input_name(name, "URL")

    @property
    def basename(self):
        """The last part of the URL's path, without a query, which names the task's copy by default."""
        path = self.url.split("://", 1)[1].split("?", 1)[0].split("#", 1)[0]
        return path.rstrip("/").rsplit("/", 1)[-1] if "/" in path else ""

    def _location(self):
        return self.url


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


class _NoSuchItem(LookupError):
    """An item selection, task[key], from a result that has no such item: the result has no items at all, and no
    attribute of that name either, which an item selects from an object without items. The task that reads the
    selection raises it, and the Executor names the two tasks in the message of its TaskError. task is the (graph id,
    key) of the task whose result was selected from, step the text of the key, kind the type of the value, and error
    the class of what the value raised for the key, such as KeyError, or None for a value without items."""

    def __init__(self, step, kind, error=None):
        super().__init__(step, kind, error)
        self.task = None
        self.step = step
        self.kind = kind
        self.error = error

    def __str__(self):
        return f"[{self.step}] selects from {_a(self.kind)}, which has no such item"


def _a(noun):
    """Return a noun with its indefinite article."""
    return f"{'an' if noun[:1].lower() in 'aeiou' else 'a'} {noun}"


class _Selectable(_Symbol):
    """A symbol for a task result, from which items and attributes can be selected."""

    __slots__ = ()

    def __getitem__(self, item):
        """Stand for result[item]. Each [] adds one step, so x["a"]["b"] differs from x[("a", "b")]."""
        return self._then(item)

    def attr(self, name):
        """Stand for an attribute of the result."""
        if not isinstance(name, str):
            raise TypeError("an attribute name must be a string")
        return self._then(_Attribute(name))

    def _then(self, step):
        return _Selection(self, (step,))


class _Selection(_Selectable):
    """An item or attribute selected from a task result."""

    __slots__ = ("source", "steps")

    def __init__(self, source, steps):
        self.source = source
        self.steps = tuple(steps)

    def _then(self, step):
        return _Selection(self.source, self.steps + (step,))

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


# Task states, which change under the lock of the Executor of the task.
_NEW = 0  # not submitted: a task of a Graph that no Executor took yet
_RUNNING = 1  # live and not finished
_COMPLETED = 2
_FAILED = 3
_CANCELLED = 4

# The value of a Task whose result was not returned yet.
_UNREAD = object()

# Bytes of standard output that a command task keeps as its result.
COMMAND_RESULT_LIMIT = 1 << 20
# Bytes of a file that TaskFile.content reads at most.
CONTENT_LIMIT = 16 << 20


class CancelledError(RuntimeError):
    """Raised by result() of a task that was cancelled, and by reading its files. It is a RuntimeError, as before."""


class TaskError(RuntimeError):
    """A task failed, or it could not run because a task it reads failed. task is the Task, and source the Task whose
    own failure explains it, which is task itself when it failed on its own.

    reason says why the source failed:

    - "function": its function raised an exception
    - "command": its command exited with an error status, or did not write a file it declared
    - "signal": its function was killed by a signal
    - "resources": it used more than it requested, as a command that requested memory and exited with status 137, the
      status of a shell whose command was killed by SIGKILL, as the kernel does to a process that runs out of memory
    - "workers": Workers kept disconnecting while they ran it, as when it exceeds their memory or crashes them
    - "input": a file it reads is missing at the Manager, or a URL it reads could not be fetched from its origin
    - "timeout": it ran longer than its time_limit
    - "retries": more of its attempts failed for other reasons than the setting max-retries allows
    - "submit": it could not be submitted, as when an argument cannot be serialized
    - "result": it completed, but its result cannot be loaded in this process or sent by its Worker

    Reasons may grow: compare with these strings, and treat others as failures.

    exception is the exception that the function of the source raised, loaded in this process with the source's
    traceback as a note, or None. exit_code is the exit status of the source's command when it exited with an error
    status, and None otherwise, also when it failed as "resources"; the message then ends with the end of its standard
    error."""

    def __init__(self, message, task, source, reason=None, exception=None, exit_code=None):
        super().__init__(message)
        self.task = task
        self.source = source
        self.reason = reason
        self.exception = exception
        self.exit_code = exit_code


def _sandbox_name(name):
    """Return the normalized form of the name of a file that a task writes, a relative path in its sandbox."""
    name = os.fspath(name)
    if not isinstance(name, str):
        raise TypeError("a file name must be a string or path-like object")
    if "\0" in name:
        raise ValueError("a file name contains a null byte")
    normalized = os.path.normpath(name)
    if not name or normalized in ("", ".") or os.path.isabs(name) or normalized == ".." or normalized.startswith("../"):
        raise ValueError("a file name must be a non-empty relative path inside the task sandbox")
    return normalized


class _Options:
    """What a task requests of its Worker and how it is tried again: cores, memory in MB or None, GPUs, a time limit in
    seconds or None, the retries after failures of the task itself, and retry_on, the tuple of exception classes those
    failures must match. Equal options are one object (_intern_options()), so the tasks of many Functions and Commands
    share one, and the Executor turns each into one profile of the C executor. A record is frozen, and unpickling one
    interns it again."""

    __slots__ = ("cores", "memory", "gpus", "time_limit", "retries", "retry_on")

    def __setattr__(self, name, value):
        raise AttributeError("task options are frozen")

    def _values(self):
        return (self.cores, self.memory, self.gpus, self.time_limit, self.retries, self.retry_on)

    def __reduce__(self):
        return (_intern_options, self._values())

    def __eq__(self, other):
        return type(other) is _Options and self._values() == other._values()

    def __hash__(self):
        return hash(self._values())

    def __repr__(self):
        return (f"_Options(cores={self.cores}, memory={self.memory}, gpus={self.gpus}, time_limit={self.time_limit}, "
                f"retries={self.retries}, retry_on={self.retry_on})")

    def retries_cover(self, error_class):
        """Return whether a failure seen as an exception of error_class is retried, as an except clause of retry_on
        would catch it."""
        return self.retries > 0 and issubclass(error_class, self.retry_on)


# The options as tuples of values -> the one _Options of these values. It holds one record per distinct value that a
# program spells, not one per task.
_OPTIONS = {}


def _intern_options(cores, memory, gpus, time_limit=None, retries=0, retry_on=(Exception,)):
    """Return the one _Options of these values."""
    key = (cores, memory, gpus, time_limit, retries, retry_on)
    options = _OPTIONS.get(key)
    if options is None:
        options = object.__new__(_Options)
        for name, value in zip(_Options.__slots__, key):
            object.__setattr__(options, name, value)
        options = _OPTIONS.setdefault(key, options)
    return options


# The options of a plain function: one core.
_DEFAULT_OPTIONS = _intern_options(1, None, 0)


def _options(cores, memory=None, gpus=0, time_limit=None, retries=0, retry_on=Exception):
    """Check what a task requests and how it is tried again, and return its interned options."""
    if type(cores) is not int or cores < 1:
        raise ValueError("cores must be a positive integer")
    if memory is not None and (type(memory) is not int or memory < 1):
        raise ValueError("memory is in MB and must be a positive integer")
    if type(gpus) is not int or gpus < 0:
        raise ValueError("gpus must be a non-negative integer")
    if time_limit is not None:
        if type(time_limit) not in (int, float) or not time_limit > 0 or time_limit == float("inf"):
            raise ValueError("time_limit is in seconds and must be a positive number")
        # Workers count whole seconds.
        time_limit = max(1, int(-(-time_limit // 1)))
    if type(retries) is not int or not 0 <= retries <= 1000:
        raise ValueError("retries must be an integer from 0 to 1000")
    classes = retry_on if isinstance(retry_on, tuple) else (retry_on,)
    if not classes or not all(isinstance(c, type) and issubclass(c, BaseException) for c in classes):
        raise TypeError("retry_on takes an exception class or a tuple of them, as an except clause does")
    return _intern_options(cores, memory, gpus, time_limit, retries, tuple(dict.fromkeys(classes)))


def _check_cache(cache):
    if type(cache) is not bool:
        raise TypeError("cache must be True or False")
    return cache


# What name= takes: a label of one word, as TaskVine categories are.
_LABEL = re.compile(r"[A-Za-z0-9_.+-]{1,64}")


def _check_label(name):
    """Return name, a label of tasks, or raise for one that is not 1 to 64 letters, digits, or _.+-."""
    if name is not None and (not isinstance(name, str) or not _LABEL.fullmatch(name)):
        raise ValueError("name must be 1 to 64 letters, digits, or _.+-")
    return name


def _fields_of(template, what):
    """Return the names of the placeholders of a template, each once, in order. A placeholder is a name in braces, and
    {{ and }} stand for braces."""
    fields = {}
    try:
        parsed = list(string.Formatter().parse(template))
    except ValueError as error:
        raise ValueError(f"invalid {what} {template!r}: {error}") from None
    for _, field, spec, conversion in parsed:
        if field is None:
            continue
        if not field.isidentifier() or spec or conversion:
            raise ValueError(f"a placeholder of {_a(what)} is a name in braces such as {{name}}, not {{{field}}}")
        fields[field] = None
    return tuple(fields)


def _parse_outputs(outputs):
    """Return (outputs, render) for the outputs of a Function or Command: the names of the files that its tasks write,
    each once, normalized when they have no placeholder and as given otherwise, and None when none has a placeholder,
    or else (templates, names of their placeholders), from which _task_outputs() names the files of each task."""
    if isinstance(outputs, (str, bytes, os.PathLike)):
        raise TypeError("outputs takes a list of file names, such as outputs=[\"out.txt\"]")
    shown = {}  # what outputs returns -> the template
    fields = {}
    for template in outputs:
        template = os.fspath(template)
        if not isinstance(template, str):
            raise TypeError("a file name must be a string or path-like object")
        own = _fields_of(template, "file name")
        if own:
            fields.update(dict.fromkeys(own))
            shown.setdefault(template, template)
        else:
            shown.setdefault(_sandbox_name(template.format()), template)
    if not fields:
        return tuple(shown), None
    return tuple(shown), (tuple(shown.values()), tuple(fields))


def _task_outputs(func, kwargs):
    """Return the sandbox paths of the files that a task of a Function or Command writes, with the placeholders of its
    outputs filled from its keyword arguments, unquoted."""
    render = func._render
    if render is None:
        return func._outputs
    templates, fields = render
    values = {}
    for field in fields:
        if field not in kwargs:
            raise TypeError(f"{func!r}: no value for {field}, which outputs names")
        value = kwargs[field]
        if type(value) is bool or not isinstance(value, (str, int)):
            raise TypeError(f"{field} names a file in outputs, so its value must be a str or int")
        values[field] = value
    return tuple(dict.fromkeys(_sandbox_name(template.format_map(values)) for template in templates))


class Command:
    """A shell command line and the values of its placeholders, which g.add(command, **settings) makes a task of that
    runs it in its sandbox: g.add(vg.Command("sort {f} > {out}", f=vg.File("a.txt"), out="b.txt"), outputs=["b.txt"]).

    Placeholders such as {name} take the values given by name, shell-quoted: a File, or a file a task writes
    (task.file(name)), becomes its path in the sandbox, a Task becomes str() of its result without trailing newlines,
    a selection such as task["mean"] or task.attr("x") str() of the selected value, and any other value str() of the
    value. A list or tuple renders each element, separated by spaces, and an empty one
    adds nothing. For a repeated flag, pass [w for v in vcfs for w in ("-I", v)]. Every placeholder needs a value, and
    every value a placeholder. Write {{ and }} for braces. The text a command writes to standard output, at most 1 MiB,
    is its result, and an exit status other than zero fails it, with the end of its standard error in its TaskError.
    Never write to an input file. The settings of its task, such as cores and outputs, are given to Graph.add(), whose
    outputs may name placeholders, filled unquoted from these values."""

    __slots__ = ("_template", "_placeholders", "_values", "_options", "_fields", "_outputs", "_render", "_name",
                 "_cache", "__weakref__")

    def __init__(self, template, /, **fields):
        if not isinstance(template, str) or not template.strip():
            raise TypeError("a Command takes a command line as a non-empty string")
        if "\0" in template:
            raise ValueError("a command line contains a null byte")
        self._template = template
        self._placeholders = _fields_of(template, "command line")
        self._values = fields
        _default_settings(self)
        self._fields = self._placeholders

    @property
    def template(self):
        return self._template

    @property
    def fields(self):
        """The values of the placeholders, as given."""
        return dict(self._values)

    def __repr__(self):
        return f"Command({self._template!r}, cores={self._options.cores}{_repr_extra(self, False)})"


class Function:
    """A Python function and the arguments to call it with, which g.add(function, **settings) makes a task of:
    g.add(vg.Function(train, data, 1, lr=0.1), cores=4, retries=2). Every keyword argument goes to func, and every
    setting of the task goes to Graph.add(). Every Python task is a Function: Graph.add() takes no bare function."""

    __slots__ = ("_func", "_args", "_kwargs", "_options", "_name", "_outputs", "_render", "_cache", "__weakref__")

    def __init__(self, func, /, *args, **kwargs):
        if isinstance(func, (Command, Function, _Symbol)) or not callable(func):
            raise TypeError("a Function takes a function and its arguments, such as Function(train, data, lr=0.1)")
        self._func = func
        self._args = args
        self._kwargs = kwargs
        _default_settings(self)

    @property
    def func(self):
        return self._func

    def __repr__(self):
        func = getattr(self._func, "__name__", None) or type(self._func).__name__
        return f"Function({func}, cores={self._options.cores}{_repr_extra(self, True)})"


def _default_settings(spec):
    """Give a Function or Command the settings of a task that Graph.add() was given none for."""
    spec._options = _DEFAULT_OPTIONS
    spec._cache = False
    spec._name = None
    spec._outputs = ()
    spec._render = None


def _bind(spec, cores, memory, gpus, time_limit, retries, retry_on, outputs, cache, name):
    """Return (what the task runs, its positional arguments, its keyword arguments) for a Function or Command and the
    settings of Graph.add(). What the task runs is a copy that holds the settings and no arguments, so the arguments
    live only in the task, as references to the tasks they read."""
    bound = object.__new__(type(spec))
    if (cores == 1 and memory is None and gpus == 0 and time_limit is None and retries == 0
            and retry_on is Exception):
        bound._options = _DEFAULT_OPTIONS
    else:
        bound._options = _options(cores, memory, gpus, time_limit, retries, retry_on)
    bound._cache = _check_cache(cache)
    bound._name = _check_label(name)
    bound._outputs, bound._render = _parse_outputs(outputs) if outputs else ((), None)
    if type(spec) is Function:
        bound._func = spec._func
        bound._args = ()
        bound._kwargs = {}
        return bound, spec._args, spec._kwargs
    bound._template = spec._template
    bound._placeholders = spec._placeholders
    bound._values = {}
    # The placeholders of the command line, followed by those only its outputs name.
    if bound._render:
        bound._fields = tuple(dict.fromkeys(spec._placeholders + bound._render[1]))
    else:
        bound._fields = spec._placeholders
    return bound, (), spec._values


def _repr_extra(func, retries_first):
    """Return the part of the repr of a Function or Command after its cores: the settings that differ from the
    defaults. A Function shows its retries before its outputs, and a Command after them."""
    options = func._options
    retries = ""
    if options.time_limit is not None:
        retries += f", time_limit={options.time_limit}"
    if options.retries:
        retries += f", retries={options.retries}"
        if options.retry_on != (Exception,):
            names = [c.__name__ for c in options.retry_on]
            retries += f", retry_on={names[0] if len(names) == 1 else '(' + ', '.join(names) + ')'}"
    outputs = f", outputs={list(func._outputs)!r}" if func._outputs else ""
    extra = ""
    if options.memory is not None:
        extra += f", memory={options.memory}"
    if options.gpus:
        extra += f", gpus={options.gpus}"
    extra += retries + outputs if retries_first else outputs + retries
    if func._name is not None:
        extra += f", name={func._name!r}"
    if func._cache:
        extra += ", cache=True"
    return extra


def _add_common_properties(cls):
    """Give a Function or Command the properties of what its tasks request, write, and are named, of their time limit
    and retries, and of the cache."""
    cls.cores = property(lambda self: self._options.cores, doc="The cores its tasks use.")
    cls.memory = property(lambda self: self._options.memory, doc="The MB of memory its tasks request, or None.")
    cls.gpus = property(lambda self: self._options.gpus, doc="The GPUs its tasks request.")
    cls.outputs = property(lambda self: self._outputs, doc="The files that every task writes, as relative paths in "
                           "the sandbox, and templates for those whose names have placeholders.")
    cls.name = property(lambda self: self._name, doc="The label of its tasks, or None.")
    cls.time_limit = property(lambda self: self._options.time_limit, doc="Seconds its tasks may run, or None.")
    cls.retries = property(lambda self: self._options.retries, doc="New attempts after failures of a task itself.")
    cls.retry_on = property(lambda self: self._options.retry_on,
                            doc="The exception classes that failures must match to be retried.")
    cls.cache = property(lambda self: self._cache, doc="Whether results are kept in the store of the Executor.")
    return cls


_add_common_properties(Command)
_add_common_properties(Function)


def _callable(func):
    """Return what a Python task calls: the function of a Function, or func itself."""
    return func._func if type(func) is Function else func


def _check_task_kind(func, method):
    """Raise TypeError unless func, the first argument of method, is a Function or a Command."""
    if type(func) is Function or isinstance(func, Command):
        return
    name = getattr(func, "__name__", None) if callable(func) and not isinstance(func, _Symbol) else None
    if name:
        raise TypeError(f"{method} takes a vg.Function or vg.Command; wrap {name} as vg.Function({name})")
    raise TypeError(f"{method} takes a vg.Function or vg.Command followed by its arguments, not {func!r}")


class TaskFile(_Symbol):
    """A file that a task writes in its sandbox, which other tasks read when it is passed to them. Asking for a file
    of a task before the task is submitted declares it, as g.add(..., outputs=...) does, and
    the task fails when it does not write it. Once the task completed, path, content, text, and save() read the file,
    whether or not a task reads it. They read through the open Executor, and a path or content that was returned once
    stays readable after it closed."""

    __slots__ = ("task", "name", "_path", "_content")

    def __init__(self, task, name):
        self.task = task
        self.name = name
        self._path = None  # the local path that path returned
        self._content = None  # the bytes that content returned

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
        content = self._content
        if content is None:
            content = self._content = self.task._graph._bound(self.task)._file(self, "content")
        return content

    @property
    def text(self):
        """The content of the file, decoded as UTF-8."""
        return self.content.decode("utf-8")

    @property
    def path(self):
        """A local path of the file, fetched to the output directory of the Executor when asked for. It stays there
        after the Executor closed. save() puts the file at a path of the caller's choice."""
        path = self._path
        # A with block that raised removed the paths it had returned.
        if path is None or not os.path.lexists(path):
            path = self._path = self.task._graph._bound(self.task)._file(self, "path")
        return path

    def save(self, path):
        """Put the file at path, a local path of the caller's choice, and return it as an absolute path. Directories
        on the way are made, and a file already there is replaced. The file is linked where the file system allows
        it and copied otherwise, so write a changed version to another path instead of changing it in place while the
        Executor is open. After the Executor closed, a file whose path or content was read before can still be
        saved."""
        path = os.fspath(path)
        if not isinstance(path, str) or not path:
            raise TypeError("save() takes a local path as a non-empty string or path-like object")
        destination = _absolute(path)
        executor = self.task._graph._executor
        if executor is None or executor._runner.closed:
            # What was read before the Executor closed is what is left of the file.
            if self._path is not None and os.path.lexists(self._path):
                _place(self._path, destination)
                return destination
            if self._content is not None:
                _place(self._content, destination)
                return destination
        self.task._graph._bound(self.task)._file(self, "save", destination)
        return destination

    def __eq__(self, other):
        return isinstance(other, TaskFile) and other.task is self.task and other.name == self.name

    def __hash__(self):
        return hash((TaskFile, id(self.task), self.name))

    def __repr__(self):
        return f"{self.task!r}.file({self.name!r})"


def _link_or_copy(source, destination):
    """Hard-link source to destination, or copy it where links fail, such as across file systems."""
    try:
        os.link(source, destination)
    except OSError:
        shutil.copyfile(source, destination)


def _place(source, destination):
    """Put a file that a task wrote at destination, replacing a file there: source is its bytes, or a local path, which
    is linked where the file system allows it and copied otherwise. A directory is copied to a destination that does
    not exist yet."""
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    if type(source) is not bytes and os.path.isdir(source):
        shutil.copytree(source, destination, symlinks=True, copy_function=_link_or_copy)
        return
    # The file appears at destination whole, under a name of this thread first.
    temporary = f"{destination}.vine-graph-{os.getpid()}-{threading.get_ident()}"
    try:
        if os.path.lexists(temporary):
            os.unlink(temporary)
        if type(source) is bytes:
            with open(temporary, "wb") as stream:
                stream.write(source)
        else:
            _link_or_copy(source, temporary)
        os.replace(temporary, destination)
    finally:
        # A rename onto another link to the same file leaves the name, and so does a failure.
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _readers_of(task):
    """Return the readers of a task as a sequence."""
    readers = task._readers
    if readers is None:
        return ()
    if isinstance(readers, list):
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
    """A call of a function, or a run of a Command, with its arguments. Graph.add() returns a Task that describes a task
    until an Executor takes its Graph (Executor.submit(graph) or Executor.run(graph)), and is live from then on: result() waits for its result, done(), failed(), and
    cancelled() tell how far it is, and cancel() cancels it. Pass a Task as an argument of another task to give that
    task its result, select from it with task[key] or task.attr(name), and name a file it writes with task.file(name).

    A live Task is named Task(n, f), where n is its number among the tasks of its Executor, and a Task not submitted yet
    Task(key, f, not submitted), with its key in its Graph. A Task keeps the Tasks it reads, which args and kwargs
    return, and the result that result() returned, which stays readable after the Executor closed."""

    __slots__ = ("_graph", "key", "_func", "_args", "_kwargs", "_parents", "_results", "_reads", "_shared",
                 "_selections", "_outputs", "_readers", "_state", "_error", "_record", "_value", "__weakref__")

    def __init__(self, graph, key, func):
        self._graph = graph
        # The task's key in its Graph: its number in the Graph, or its Dask key in a Graph from adaptors.from_dask().
        self.key = key
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
        # Tasks of the same Graph that read it, in the order they were added: None, the one reader, or a list of
        # several, since most tasks have one reader and a list for it would take more memory than the task.
        self._readers = None
        self._state = _NEW
        # The failure of a failed task: (_FAILED, source Task or None for the task itself, message, reason, exit code).
        # Only a source holds its message, reason, and exit code.
        self._error = None
        self._record = None  # the Executor's record of the task, from when it is live, which holds its number
        self._value = _UNREAD  # the result, once the Executor returned it

    @property
    def func(self):
        """The Function or Command the task runs, with the settings it was added with (cores, memory, gpus, outputs,
        time_limit, retries, retry_on, cache, name), and without its arguments, which args and kwargs give."""
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
        """The tasks of its Graph that read this task's result or files, in the order they were added."""
        return list(_readers_of(self))

    def result(self, timeout=None):
        """Return the result of the task: what its function returned, or the text its command wrote to standard
        output. A task that is still running is waited for, at most timeout seconds when one is given, after which
        TimeoutError is raised. A failed task raises its TaskError, a cancelled task CancelledError, and one that has
        not been submitted RuntimeError. A result that the Executor released meanwhile is computed again.

        Results are read through the open Executor. The Task keeps a result that was returned once, by result() or by
        Executor.run(), and returns the same object again, also after the Executor closed. A result that was never
        read before the Executor closed is gone, and RuntimeError says so."""
        value = self._value
        if value is _UNREAD:
            value = self._graph._bound(self)._result(self, timeout)
        return value

    def done(self):
        """Return whether the task finished: it completed, failed, or was cancelled."""
        if self._state == _RUNNING:
            self._graph._poll()
        return self._state >= _COMPLETED

    def failed(self):
        """Return whether the task failed, or could not run because a task it reads failed."""
        if self._state == _RUNNING:
            self._graph._poll()
        return self._state == _FAILED

    def cancelled(self):
        """Return whether cancel() cancelled the task, or a task it reads."""
        if self._state == _RUNNING:
            self._graph._poll()
        return self._state == _CANCELLED

    def cancel(self):
        """Cancel the task unless it finished, and the tasks that read it: those not started never run, and running
        ones are stopped. Return True when this call cancelled the task, and False when it had finished or has not been
        submitted: a completed task stays completed, a failed task failed, a cancelled task cancelled, and a task not
        submitted runs once its Graph is submitted."""
        if self._state != _RUNNING:
            return False
        return self._graph._bound(self)._cancel(self)

    def file(self, name):
        """Return the file this task writes at name, a relative path in its sandbox. Asking for the same name again
        returns the same file. A file is declared by asking for it before the task is submitted, or by the outputs of
        Graph.add()."""
        normalized = _sandbox_name(name)
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


def _undeclared(task, name):
    """Return the message for a file of a task that was not declared before the task was submitted."""
    return (f"the file {name!r} of {task!r} was asked for after the task was submitted, so it was not kept; ask for "
            f"the files of a task before its Graph is submitted, or list the files that the task writes "
            f"in g.add(..., outputs=...)")


def _label(task):
    """Return how messages name a task: its number and the name of its function, or the start of its command, or its
    key in its Graph for a task not submitted yet."""
    record = task._record
    if record is None:
        return _name_task(task.key, task._func, ", not submitted")
    return _name_task(record.number, task._func)


def _name_task(number, func, suffix=""):
    kind = type(func)
    if (kind is Command or kind is Function) and func._name is not None:
        name = func._name
    elif kind is Command:
        words = func.template.split()
        name = words[0] if words else "command"
    else:
        func = _callable(func)
        name = getattr(func, "__name__", None) or type(func).__name__
    return f"Task({number!r}, {name}{suffix})"


def _check_command_value(name, value):
    """Raise for a value of a placeholder that a command cannot read."""
    if isinstance(value, Shared):
        raise TypeError(f"the value of {{{name}}} is a shared object, which only Python tasks take")
    if not isinstance(value, _Symbol) and _scanner()(value):
        raise TypeError(f"the value of {{{name}}} holds tasks or files inside, which a command cannot read; pass each "
                        f"as a value of its own")


def _check_input_names(task, outputs):
    """Raise ValueError for a File that a task reads under a name that the task also gives another File it reads, or
    a file it writes."""
    named = {}
    for ref in task._reads:
        if (type(ref) is not File and type(ref) is not URL) or ref.name is None:
            continue
        other = named.setdefault(ref.name, ref)
        if other is not ref:
            raise ValueError(f"{task!r} reads two files named {ref.name!r}: {other!r} and {ref!r}")
        if _clashes_with_output(ref.name, outputs):
            raise ValueError(f"{task!r} reads {ref!r} under the name of a file it writes")


def _clashes_with_output(name, outputs):
    """Return whether a sandbox name is one of outputs or a directory of one."""
    for output in outputs:
        if output == name or output.startswith(name + "/"):
            return True
    return False


# Numbers that tell Graphs apart in the references of tasks that read tasks of another Graph.
_graph_ids = itertools.count(1)


class Graph:
    """Tasks and the data they pass to each other.

    add() adds a task and returns its Task, which describes the task until an Executor takes the Graph:
    Executor.submit(graph) makes every task not submitted yet live and returns at once, and Executor.run(graph) does so
    and waits for them. Tasks added afterwards become live at the next submit() or run() of the Graph. Iterating a Graph
    yields its tasks in the order they were added, and len() counts them.

    A Graph describes work and holds no settings: the Executor decides where it runs. The first Executor that takes a
    Graph holds the results of its tasks, so the Graph belongs to that Executor, and its tasks may read the live tasks
    of that Executor, those of other Graphs it took."""

    # The Executor that the Graph belongs to, which holds the results of its tasks, or None before one took it.
    _executor = None

    def __init__(self):
        self._tasks = {}  # key -> Task, in insertion order
        self._next_key = 1
        # id -> [object, tasks]: argument objects without symbols, scanned once while tasks hold them. An object passed
        # again is assumed unchanged.
        self._plain = {}
        # key -> _Ref(None, key) that arguments of tasks store for the whole result of task key, shared by every
        # reader.
        self._shared_refs = {}
        # Tasks not submitted yet, in the order they were added, in which each comes after the tasks it reads.
        self._new = collections.deque()
        self._unfinished = 0  # live tasks that did not finish, counted under the lock of the Executor
        self._id = next(_graph_ids)
        self._ref = weakref.ref(self)  # how the records of an Executor refer to the Graph
        # The Executor's records of the tasks submitted, which it lets go when the Graph is collected.
        self._records = []

    def __repr__(self):
        return f"Graph({len(self._tasks)} tasks)"

    def __iter__(self):
        """Yield the tasks in the order they were added."""
        return iter(list(self._tasks.values()))

    def __len__(self):
        return len(self._tasks)

    def __getstate__(self):
        raise TypeError("a Graph holds the state of its tasks, and cannot be pickled")

    def add(self, spec, /, *extra, cores=1, memory=None, gpus=0, time_limit=None, retries=0, retry_on=Exception,
            outputs=(), cache=False, name=None):
        """Add a task that calls a Function or runs a Command, with these settings, and return its Task, which becomes
        live when an Executor takes the Graph. Tasks of this Graph and live tasks of its Executor, their selections and
        files, and Files may appear anywhere inside the arguments of a Function.

        cores is the number of cores the task uses on a Worker. memory (MB) and gpus are requests, not limits: a task
        waits for a Worker that can grant them, and sees CORES, MEMORY, GPUS, and CUDA_VISIBLE_DEVICES for what it got.
        A Python task with gpus runs as a standard TaskVine task instead of a call of the library. A task that asks for
        more than the largest connected Worker has waits for a larger Worker, with a notice once for each size.

        outputs lists the files that the task writes in its sandbox, as relative paths, with placeholders filled
        unquoted from the keyword arguments of a Function or the values of a Command. task.file(name) names them for
        other tasks and for reading, and a task that does not write one fails.

        time_limit (seconds) stops a task that runs longer, which fails with reason "timeout". retries=k tries a task up
        to k more times after failures of the task itself that are instances of retry_on (Exception by default): the
        function's own exception, CalledProcessError for a command's exit status, TimeoutError for "timeout", and
        MemoryError for "resources". cache=True keeps its results and files in the store of the Executor
        (Executor(store=path)) for later tasks of the same function, arguments, and input files. name labels the task
        in errors, notices, and repr."""
        _check_task_kind(spec, "g.add()")
        if extra:
            raise TypeError("g.add() takes one Function or Command; pass the arguments inside it, as in "
                            "g.add(vg.Function(f, x, y))")
        while self._next_key in self._tasks:
            self._next_key += 1
        key = self._next_key
        self._next_key += 1
        return self._add(key, spec, (cores, memory, gpus, time_limit, retries, retry_on, outputs, cache, name))

    def _add(self, key, spec, settings=None, method="g.add()"):
        """Record a task of a Function or Command and its settings, as Graph.add() takes them, under key and return
        it."""
        _check_task_kind(spec, method)
        if settings is None:
            settings = (1, None, 0, None, 0, Exception, (), False, None)
        func, args, kwargs = _bind(spec, *settings)
        command = type(func) is Command
        if command:
            missing = [name for name in func._fields if name not in kwargs]
            extra = [name for name in kwargs if name not in func._fields]
            if missing or extra:
                problems = []
                if missing:
                    problems.append(f"no value for {', '.join(missing)}")
                if extra:
                    problems.append(f"no placeholder for {', '.join(extra)}")
                raise TypeError(f"{spec!r}: {'; '.join(problems)}")
        if key in self._tasks:
            raise ValueError(f"a task with key {key!r} already exists")
        outputs = _task_outputs(func, kwargs) if func._outputs else ()
        task = Task(self, key, func)
        self._record_arguments(task, args, kwargs)
        if outputs:
            # The files of a Function or Command are declared for each of its tasks before the task can start.
            task._outputs = {name: TaskFile(task, name) for name in outputs}
        if task._reads:
            _check_input_names(task, outputs)
        self._insert(task)
        return task

    def _insert(self, task):
        """Take a new task into the Graph, tell the tasks of this Graph that it reads, and queue it for submission."""
        self._tasks[task.key] = task
        for parent in task._parents:
            if parent._graph is not self:
                continue
            readers = parent._readers
            if readers is None:
                parent._readers = task
            elif isinstance(readers, list):
                readers.append(task)
            else:
                parent._readers = [readers, task]
        self._new.append(task)

    def _graph_id(self, parent):
        """Return the graph id in the references of this Graph's tasks to a task: None for a task of this Graph."""
        return None if parent._graph is self else parent._graph._id

    def _check_parent(self, parent, what):
        """Raise unless tasks of this Graph may read a task: one of this Graph, or a live task of its Executor. A Graph
        that no Executor took yet belongs from now on to the Executor of the live task."""
        graph = parent._graph
        if graph is self:
            return
        if parent._record is None:
            raise ValueError(f"a task reads {what} {parent!r}, a task of another Graph that has not been submitted; "
                             f"submit or run that Graph first, or add the task to the same Graph")
        executor = graph._executor
        mine = self._executor
        if mine is None:
            executor._bind(self)
        elif mine is not executor:
            raise ValueError(f"a task reads {what} {parent!r}, which belongs to another Executor; the tasks of a Graph "
                             f"read only tasks of the Executor that the Graph belongs to")

    def _ref_to(self, parent):
        """Return the reference that arguments store for the whole result of a task, shared by every reader of a task
        of this Graph."""
        if parent._graph is not self:
            return _Ref(parent._graph._id, parent.key)
        ref = self._shared_refs.get(parent.key)
        if ref is None:
            ref = self._shared_refs[parent.key] = _Ref(None, parent.key)
        return ref

    def _record_arguments(self, task, args, kwargs):
        """Store the arguments of a task with every symbol replaced by its reference, and what it reads."""
        if isinstance(task._func, Command):
            for name, value in kwargs.items():
                if isinstance(value, (set, frozenset)):
                    raise TypeError(f"the value of {{{name}}} is a set, which has no order; pass sorted(...)")
                if type(value) is list or type(value) is tuple:
                    for element in value:
                        if isinstance(element, (list, tuple, set, frozenset, dict)):
                            raise TypeError(f"the value of {{{name}}} is a list that holds a container; a list "
                                            f"renders each of its elements, which are single values")
                        _check_command_value(name, element)
                else:
                    _check_command_value(name, value)
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
                self._check_parent(symbol, "the result of")
                read_result(symbol, ())
                return self._ref_to(symbol)
            if kind is Shared:
                shared.setdefault(symbol.name, None)
                return symbol
            if kind is File or kind is URL:
                reads.setdefault(symbol, symbol)
                return symbol
            if kind is TaskFile:
                parent = symbol.task
                self._check_parent(parent, "a file of")
                if parent._record is not None and symbol.name not in parent._record.outputs:
                    raise ValueError(_undeclared(parent, symbol.name))
                return read_file(parent, symbol.name)
            if kind is _FileRef:
                # A reference that an adaptor built, to a task of this Graph.
                parent = parent_of(symbol.key)
                parent.file(symbol.path)
                return read_file(parent, symbol.path)
            if isinstance(symbol, _Selection) or kind is _Ref:
                if isinstance(symbol, _Selection) and isinstance(symbol.source, Task):
                    parent, steps = symbol.source, symbol.steps
                    self._check_parent(parent, "the result of")
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

    # What the Tasks of the Graph ask of the Executor that took it.

    def _bound(self, task):
        """Return the Executor of a live task, or raise for a task not submitted yet."""
        if task._record is None:
            raise RuntimeError(f"{task!r} has not been submitted; submit or run its Graph")
        return self._executor

    def _poll(self):
        """Bring the states of the live tasks up to date."""
        executor = self._executor
        if executor is not None:
            executor._poll()


