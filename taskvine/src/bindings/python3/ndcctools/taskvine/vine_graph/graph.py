# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Tasks and the data they pass to each other. A task is made by vg.Function, vg.Command, or vg.Expand, and the tasks
among its arguments are the tasks it reads: the graph is this structure of references, and no object holds it. A task
is a description until an Executor (executor.py) takes it (Executor.submit() or Executor.run()), and from then on it is
live."""

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
import warnings


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
    selection raises it, and the Executor names the two tasks in the message of its TaskError. task is the key of the
    task whose result was selected from, step the text of the key, kind the type of the value, and error
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

    # A result is not iterated here: without this, iter() would select task[0], task[1], and so on without end.
    __iter__ = None

    def __getitem__(self, item):
        """Stand for result[item]. Each [] adds one step, so task["a"]["b"] differs from task[("a", "b")]."""
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
        return _Ref(ref.key, ref.steps + self.steps)

    def __repr__(self):
        return f"{self.source!r}{''.join(f'.attr({s.name!r})' if isinstance(s, _Attribute) else f'[{s!r}]' for s in self.steps)}"


class _Ref(_Symbol):
    """The result of the task with a key, followed by selection steps. References are what the stored arguments of a
    task and its manifest hold in place of the tasks it reads, so they never carry a Task."""

    __slots__ = ("key", "steps")

    def __init__(self, key, steps=()):
        self.key = key
        self.steps = steps if type(steps) is tuple else tuple(steps)

    def _as_ref(self):
        return self


class _FileRef(_Symbol):
    """The file that the task with a key writes at path in its sandbox."""

    __slots__ = ("key", "path")

    def __init__(self, key, path):
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
    chain with a step of another type. Keys include each step's type, so task[1], task[True], and task["1"] differ."""
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


# Task states. A task that was not submitted is new and has no record. The state of a live task is that of its record
# (runner._Record.state), which changes under the lock of its Executor.
_NEW = 0  # not submitted
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
    - "expand": the builder of its Expand raised, returned anything but a list of tasks, or returned a task that
      reads the Expand

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
    failures must match. Equal options are one object (_intern_options()), which many tasks share, and the Executor
    turns each into one profile of the C executor. A record is frozen, and unpickling one
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


def _output_names(outputs, kwargs):
    """Return the sandbox paths, each once, of the files that the outputs setting of a task names, with the
    placeholders of a name filled, unquoted, from the keyword arguments of the task."""
    if isinstance(outputs, (str, bytes, os.PathLike)):
        raise TypeError("outputs takes a list of file names, such as outputs=[\"out.txt\"]")
    names = {}
    for template in outputs:
        template = os.fspath(template)
        if not isinstance(template, str):
            raise TypeError("a file name must be a string or path-like object")
        values = {}
        for field in _fields_of(template, "file name"):
            if field not in kwargs:
                raise TypeError(f"no value for {field}, which outputs names; a placeholder of outputs takes the value "
                                f"of a keyword argument of the task")
            value = kwargs[field]
            if type(value) is bool or not isinstance(value, (str, int)):
                raise TypeError(f"{field} names a file in outputs, so its value must be a str or int")
            values[field] = value
        names[_sandbox_name(template.format_map(values))] = None
    return tuple(names)


class TaskFile(_Symbol):
    """A file that a task writes in its sandbox, which other tasks read when it is passed to them. Asking for a file
    of a task before the task is submitted declares it, as task.set(outputs=...) does, and the task fails when it does
    not write it. Once the task completed, path, content, text, and save() read the file, whether or not a task reads
    it. They read through the open Executor, and a path or content that was returned once stays readable after it
    closed."""

    __slots__ = ("task", "name", "_path", "_content")

    def __init__(self, task, name):
        self.task = task
        self.name = name
        self._path = None  # the local path that path returned
        self._content = None  # the bytes that content returned

    def _as_ref(self):
        return _FileRef(self.task.key, self.name)

    @property
    def exists(self):
        """Whether the task wrote the file: it completed, and the file was declared before it ran."""
        record = self.task._record
        return record is not None and record.state == _COMPLETED and self.name in record.outputs

    @property
    def content(self):
        """The bytes of the file, fetched when asked for. A file above 16 MiB raises ValueError: pass it to a task, or
        read TaskFile.path."""
        content = self._content
        if content is None:
            content = self._content = self.task._live()._file(self, "content")
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
            path = self._path = self.task._live()._file(self, "path")
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
        task = self.task
        if task._record is not None and task._executor._runner.closed:
            # What was read before the Executor closed is what is left of the file.
            if self._path is not None and os.path.lexists(self._path):
                _place(self._path, destination)
                return destination
            if self._content is not None:
                _place(self._content, destination)
                return destination
        task._live()._file(self, "save", destination)
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


# The keys of tasks, which tell the tasks of this process apart in the references that arguments and manifests hold.
# next() of a count is one step of the interpreter, so tasks made in several threads take distinct keys without a lock.
_keys = itertools.count(1)

# id -> [object, tasks that hold it]: argument objects without symbols, each scanned once while a task holds it. An
# object passed again is assumed unchanged, and the runner searches no such argument for references. A task that
# registered one (_PLAIN) gives it back when it is collected, so the table holds only arguments of tasks that exist.
_plain = {}

# The settings of a task, which Task.set() takes, in the order of their bits in Task._flags.
_SETTINGS = ("cores", "memory", "retries", "name", "outputs", "gpus", "time_limit", "retry_on", "cache")

# Bits of Task._flags.
_READ = 1  # never warn that the task was not submitted: a task reads it, or it was made where no caller submits it
_CACHE = 2  # the setting cache=True
_PLAIN = 4  # the task registered arguments in _plain
_GUEST = 8  # the Task was made for a record that another Task owns (_revive()), and never lets go of it
# Setting -> the bit of a setting that Task.set() was given, which a task that an Expand expands into keeps as it is.
_GIVEN = {name: 16 << index for index, name in enumerate(_SETTINGS)}
_GIVEN_OPTIONS = sum(_GIVEN[name] for name in _Options.__slots__)

# Builders of Expands that run now. A task made meanwhile never warns that it was not submitted, since the Expand
# submits the tasks that its builder returns.
_building = 0
_building_lock = threading.Lock()


def _builder_runs(change):
    """Count a builder of an Expand that starts (1) or ends (-1)."""
    global _building
    with _building_lock:
        _building += change


def _restore(task, value):
    """Return arguments of a task with their references replaced by the Tasks, selections, and files they stand for."""
    parents = {parent.key: parent for parent in task._parents}

    def restore(ref):
        if type(ref) is _Ref:
            parent = parents[ref.key]
            return _Selection(parent, ref.steps) if ref.steps else parent
        if type(ref) is _FileRef:
            return parents[ref.key].file(ref.path)
        return ref

    return replace_symbols(value, restore)


class Task(_Selectable):
    """A unit of work with its arguments: a call of a function (vg.Function), a run of a shell command (vg.Command), or
    tasks decided from results (vg.Expand). Making one of the three makes the task. A task or a selection from one
    (task[key], task.attr(name)), or a file a task writes (task.file(name)), passed as an argument of another task is
    the dependency between the two, and arguments are fixed when the task is made. set() gives the task its settings.

    A task describes work until an Executor takes it (Executor.submit(task) or Executor.run(task)), with every task it
    reads that is not live yet, and is live from then on: result() waits for its result, done(), failed(), and
    cancelled() tell how far it is, and cancel() cancels it.

    A live Task is named Task(n, f), where n is its number among the tasks of its Executor, and a Task not submitted yet
    Task(key, f, not submitted), with its key among the tasks of this process. A Task keeps the Tasks it reads, which
    args and kwargs return, and the result that result() returned, which stays readable after the Executor closed. The
    Executor lets go of a task once its Task is collected. A Task that is collected without ever being submitted or
    read by another task warns once (RuntimeWarning), as a coroutine that was never awaited does."""

    __slots__ = ("key", "_func", "_args", "_kwargs", "_parents", "_results", "_reads", "_shared", "_selections",
                 "_outputs", "_options", "_name", "_flags", "_ref", "_record", "_executor", "_value", "_owner",
                 "__weakref__")

    def __init__(self, *args, **kwargs):
        raise TypeError("a task is made with vg.Function(func, *args, **kwargs), vg.Command(template, **fields), or "
                        "vg.Expand(builder, *args, **kwargs)")

    def __del__(self):
        try:
            record = self._record
            flags = self._flags
        except AttributeError:
            # Its construction raised.
            return
        if flags & _PLAIN:
            _release_plain(self)
        if record is not None:
            # The Executor lets go of the task, unless an Expand expanded into it, which lets go of it with itself.
            if not record.child and not flags & _GUEST:
                runner = self._executor._runner
                if not runner.closed:
                    runner._dropped.append(record)
        elif not flags & _READ:
            try:
                warnings.warn(f"{_label(self)} was never submitted", RuntimeWarning, stacklevel=2)
            except Exception:
                # The interpreter is shutting down.
                pass

    def __reduce__(self):
        raise TypeError(f"{_label(self)} cannot be serialized; pass a task as an argument of the task that reads it "
                        f"instead of using it inside a function")

    def set(self, **settings):
        """Give the task settings and return it, as in vg.Function(train, data_task).set(cores=4, retries=2). Settings
        are fixed once the task is submitted, and every keyword argument of vg.Function, vg.Command, and vg.Expand
        goes to the function, the command line, or the builder, so this is the only way to give a setting.

        cores is the number of cores the task uses on a Worker. memory (MB) and gpus are requests, not limits: a task
        waits for a Worker that can grant them, and sees CORES, MEMORY, GPUS, and CUDA_VISIBLE_DEVICES for what it got.
        A Python task with gpus runs as a standard TaskVine task instead of a call of the library. A task that asks for
        more than the largest connected Worker has waits for a larger Worker, with a notice once for each size.

        outputs lists files that the task writes in its sandbox, as relative paths, with placeholders filled unquoted
        from the keyword arguments of a Function or the values of a Command. task.file(name) names them for other
        tasks and for reading, and a task that does not write one fails. Files add up: set(outputs=...) declares more
        files and takes none back.

        time_limit (seconds) stops a task that runs longer, which fails with reason "timeout". retries=k tries a task up
        to k more times after failures of the task itself that are instances of retry_on (Exception by default): the
        function's own exception, CalledProcessError for a command's exit status, TimeoutError for "timeout", and
        MemoryError for "resources". cache=True keeps its results and files in the store of the Executor
        (Executor(store=path)) for later tasks of the same function, arguments, and input files. name labels the task
        in errors, notices, and repr.

        A task that an Expand expands into takes the settings of the Expand that it was not given itself."""
        if self._record is not None:
            raise RuntimeError(f"{self!r} was submitted, so its settings are fixed; call set() before the task is "
                               f"submitted")
        given = 0
        for name in settings:
            bit = _GIVEN.get(name)
            if bit is None:
                raise TypeError(f"set() takes no setting named {name!r}; the settings of a task are "
                                f"{', '.join(_SETTINGS)}")
            given |= bit
        # Everything is checked before anything changes.
        options = self._options
        if given & _GIVEN_OPTIONS:
            get = settings.get
            options = _options(get("cores", options.cores), get("memory", options.memory), get("gpus", options.gpus),
                               get("time_limit", options.time_limit), get("retries", options.retries),
                               get("retry_on", options.retry_on))
        name = _check_label(settings["name"]) if "name" in settings else self._name
        flags = self._flags | given
        if "cache" in settings:
            flags = flags | _CACHE if _check_cache(settings["cache"]) else flags & ~_CACHE
        if "outputs" in settings:
            self._declare(settings["outputs"])
        self._options = options
        self._name = name
        self._flags = flags
        return self

    def _declare(self, outputs):
        """Declare the files that the outputs setting names."""
        names = _output_names(outputs, self._kwargs)
        if self._reads:
            _check_input_names(self, names)
        files = self._outputs
        if files is None:
            files = self._outputs = {}
        for name in names:
            if name not in files:
                files[name] = TaskFile(self, name)

    @property
    def cores(self):
        """The cores the task uses."""
        return self._options.cores

    @property
    def memory(self):
        """The MB of memory the task requests, or None."""
        return self._options.memory

    @property
    def gpus(self):
        """The GPUs the task requests."""
        return self._options.gpus

    @property
    def time_limit(self):
        """Seconds the task may run, or None."""
        return self._options.time_limit

    @property
    def retries(self):
        """New attempts after failures of the task itself."""
        return self._options.retries

    @property
    def retry_on(self):
        """The exception classes that failures must match to be retried."""
        return self._options.retry_on

    @property
    def outputs(self):
        """The files the task declared, by set(outputs=...) or file(), as relative paths in its sandbox. A file asked
        for after the task was submitted is not among them."""
        if self._record is not None:
            return self._record.outputs
        return tuple(self._outputs) if self._outputs else ()

    @property
    def cache(self):
        """Whether the result is kept in the store of the Executor."""
        return bool(self._flags & _CACHE)

    @property
    def name(self):
        """The label of the task, or None."""
        return self._name

    @property
    def args(self):
        """The positional arguments, with the Tasks, selections, and files they hold."""
        return _restore(self, self._args) if self._parents else self._args

    @property
    def kwargs(self):
        """The keyword arguments, with the Tasks, selections, and files they hold."""
        return dict(_restore(self, self._kwargs) if self._parents else self._kwargs)

    def result(self, timeout=None):
        """Return the result of the task: what its function returned, the text its command wrote to standard output,
        or the list of the results of the tasks its Expand expanded into. A task that is still running is waited for,
        at most timeout seconds when one is given, after which TimeoutError is raised. A failed task raises its
        TaskError, a cancelled task CancelledError, and one that has not been submitted RuntimeError. A result that the
        Executor released meanwhile is computed again.

        Results are read through the open Executor. The Task keeps a result that was returned once and returns the
        same object again, also after the Executor closed. A result that was never read before the Executor closed is
        gone, and RuntimeError says so."""
        value = self._value
        if value is _UNREAD:
            value = self._live()._result(self, timeout)
        return value

    def done(self):
        """Return whether the task finished: it completed, failed, or was cancelled."""
        return self._poll() >= _COMPLETED

    def failed(self):
        """Return whether the task failed, or could not run because a task it reads failed."""
        return self._poll() == _FAILED

    def cancelled(self):
        """Return whether cancel() cancelled the task, or a task it reads."""
        return self._poll() == _CANCELLED

    def cancel(self):
        """Cancel the task unless it finished, and the tasks that read it: those not started never run, and running
        ones are stopped. Return True when this call cancelled the task, and False when it had finished or has not been
        submitted: a completed task stays completed, a failed task failed, a cancelled task cancelled, and a task not
        submitted runs once it is submitted."""
        record = self._record
        if record is None or record.state != _RUNNING:
            return False
        return self._executor._cancel(self)

    def file(self, name):
        """Return the file this task writes at name, a relative path in its sandbox. Asking for the same name again
        returns the same file. A file is declared by asking for it before the task is submitted, or by
        set(outputs=...)."""
        normalized = _sandbox_name(name)
        outputs = self._outputs
        if outputs is None:
            outputs = self._outputs = {}
        file = outputs.get(normalized)
        if file is None:
            file = outputs[normalized] = TaskFile(self, normalized)
        return file

    def _poll(self):
        """Return the state of the task, brought up to date when it is live and did not finish."""
        record = self._record
        if record is None:
            return _NEW
        if record.state == _RUNNING:
            self._executor._poll()
        return record.state

    def _live(self):
        """Return the Executor of a live task, or raise for a task not submitted yet."""
        if self._record is None:
            raise RuntimeError(f"{self!r} has not been submitted; give it to Executor.submit() or Executor.run()")
        return self._executor

    def _as_ref(self):
        return _Ref(self.key)

    def __repr__(self):
        return _label(self)


def _release_plain(task):
    """Give back the arguments that a task registered in _plain, each once for every place the task holds it at."""
    plain = _plain
    for value in (*task._args, *task._kwargs.values()):
        entry = plain.get(id(value))
        if entry is not None and entry[0] is value:
            if entry[1] > 1:
                entry[1] -= 1
            else:
                plain.pop(id(value), None)


def _whole(parent):
    """Return the reference that arguments store for the whole result of a task, one object for every reader, made
    for the first."""
    ref = parent._ref = _Ref(parent.key)
    parent._flags |= _READ
    return ref


def _make(task, func, args, kwargs):
    """Fill a new task that runs func, a function, a builder, or a command line, with args and kwargs, which belong to
    the task from now on, and return its flags. The caller sets them last, so a task whose construction raised has
    none (Task.__del__())."""
    task.key = next(_keys)
    task._func = func
    task._options = _DEFAULT_OPTIONS
    task._name = None
    task._outputs = None  # {sandbox path: TaskFile} of the files it declared, or None
    # The reference that the arguments of its readers store for its whole result, made for the first of them.
    task._ref = None
    task._record = None  # the Executor's record of the task, from when it is live, which holds its number and state
    task._executor = None  # the Executor that took the task
    task._value = _UNREAD  # the result, once the Executor returned it
    # Arguments of leaf types, the common case of calls made in bulk, hold no symbol and need no scan. Tasks without
    # keyword arguments share one empty dictionary.
    leaf = _LEAF_TYPE_SET
    for value in args:
        if type(value) not in leaf:
            break
    else:
        for value in kwargs.values():
            if type(value) not in leaf:
                break
        else:
            task._args = args  # arguments, with every symbol replaced by its reference
            task._kwargs = kwargs if kwargs else _NO_KWARGS
            task._parents = ()  # Tasks whose results or files it reads, in first-use order
            task._results = None  # Tasks whose results it reads, in first-use order, or None
            task._reads = None  # Files and _FileRefs it reads, or None
            task._shared = None  # names of shared objects it reads, or None
            # {key: (selection_key(steps), steps)} of each task result it reads at exactly one place, through a
            # selection, or None. Such a task needs only the selected value, which its producer may write to a file.
            task._selections = None
            return _READ if _building else 0
    flags = _read_arguments(task, args, kwargs)
    return flags | _READ if _building else flags


def _read_arguments(task, args, kwargs):
    """Store the arguments of a task with every symbol replaced by its reference, and what it reads. Return _PLAIN when
    the task registered an argument in _plain, and 0 otherwise."""
    leaf, tasks = _LEAF_TYPE_SET, _TASK_TYPES
    for value in args:
        kind = type(value)
        if kind not in leaf and kind not in tasks:
            break
    else:
        for value in kwargs.values():
            kind = type(value)
            if kind not in leaf and kind not in tasks:
                break
        else:
            # Values and whole results of tasks, the common case of a task in a graph, need no scan either.
            parents = {}
            stored = list(args)
            for index, value in enumerate(stored):
                if type(value) not in leaf:
                    parents[value] = None
                    stored[index] = value._ref or _whole(value)
            for name, value in kwargs.items():
                if type(value) not in leaf:
                    parents[value] = None
                    kwargs[name] = value._ref or _whole(value)
            task._args = tuple(stored)
            task._kwargs = kwargs if kwargs else _NO_KWARGS
            task._parents = task._results = tuple(parents)
            task._reads = task._shared = task._selections = None
            return 0
    parents = {}  # Task -> None, as an ordered set
    results = {}  # Tasks whose result it reads -> None, as an ordered set
    reads = {}  # identity of each file read -> its File or _FileRef
    shared = {}  # names of shared objects -> None, as an ordered set
    places = {}  # Task of each result read -> steps of the first place it is read at
    repeated = set()  # Tasks whose result is read at more than one place

    def read_result(parent, steps):
        # Replacement visits a container once however often the arguments hold it, and a symbol at each place, as
        # the library does when it runs the task, so repeated holds the results read at more than one place.
        if parent in places:
            repeated.add(parent)
        else:
            parents.setdefault(parent, None)
            results[parent] = None
            places[parent] = steps

    def replace(symbol):
        kind = type(symbol)
        if kind in tasks or isinstance(symbol, Task):
            read_result(symbol, ())
            return symbol._ref or _whole(symbol)
        if kind is Shared:
            shared.setdefault(symbol.name, None)
            return symbol
        if kind is File or kind is URL:
            reads.setdefault(symbol, symbol)
            return symbol
        if kind is TaskFile:
            parent = symbol.task
            record = parent._record
            if record is not None and symbol.name not in record.outputs:
                raise ValueError(_undeclared(parent, symbol.name))
            parent._flags |= _READ
            parents.setdefault(parent, None)
            identity = (parent.key, symbol.name)
            ref = reads.get(identity)
            if ref is None:
                ref = reads[identity] = _FileRef(parent.key, symbol.name)
            return ref
        if kind is _Selection and isinstance(symbol.source, Task):
            parent = symbol.source
            parent._flags |= _READ
            read_result(parent, symbol.steps)
            return _Ref(parent.key, symbol.steps)
        raise TypeError(f"{symbol!r} cannot be a task argument")

    # Replacing all arguments at once keeps an object passed as several arguments one object.
    originals = (*args, *kwargs.values())
    plain = _plain
    args, kwargs = replace_symbols((args, kwargs), replace, plain)
    task._args = args
    task._kwargs = kwargs if kwargs else _NO_KWARGS
    flags = 0
    # An argument that replacement kept as it is holds no symbol, so later tasks that pass it skip the scan.
    for original, stored in zip(originals, (*args, *kwargs.values())):
        if stored is original and original is not None and not isinstance(original, (*_LEAF_TYPES, _Symbol)):
            plain.setdefault(id(original), [original, 0])[1] += 1
            flags = _PLAIN
    task._parents = tuple(parents)
    # Most tasks read the result of every task they read, and keep one tuple for both.
    task._results = (task._parents if len(results) == len(parents) else tuple(results)) if results else None
    task._reads = tuple(reads.values()) if reads else None
    task._shared = tuple(shared) if shared else None
    selections = {parent.key: (selection_key(steps), steps) for parent, steps in places.items()
                  if steps and parent not in repeated}
    task._selections = selections if selections else None
    if reads:
        try:
            _check_input_names(task, ())
        except BaseException:
            if flags:
                _release_plain(task)
            raise
    return flags


class Function(Task):
    """A task that calls a Python function with its arguments: vg.Function(train, data_task, 1, lr=0.1). Tasks, their
    selections and files, and Files and URLs may appear anywhere inside the arguments, in lists, tuples, sets,
    dictionary values, and dataclasses, and the function receives their values and paths. Every keyword argument goes
    to func, and settings go to set(): vg.Function(train, data_task).set(cores=4, retries=2)."""

    __slots__ = ()

    def __init__(self, func, /, *args, **kwargs):
        if not callable(func) or isinstance(func, _Symbol):
            raise TypeError("a Function takes a function and its arguments, such as vg.Function(train, data_task, "
                            "lr=0.1)")
        self._flags = _make(self, func, args, kwargs)

    @property
    def func(self):
        """The function the task calls."""
        return self._func


class Command(Task):
    """A task that runs a shell command line in its sandbox:
    vg.Command("sort {f} > {out}", f=vg.File("a.txt"), out="b.txt").set(outputs=["b.txt"]).

    Placeholders such as {name} take the values given by name, shell-quoted: a File, or a file a task writes
    (task.file(name)), becomes its path in the sandbox, a Task becomes str() of its result without trailing newlines,
    a selection such as task["mean"] or task.attr("x") str() of the selected value, and any other value str() of the
    value. A list or tuple renders each element, separated by spaces, and an empty one adds nothing. For a repeated
    flag, pass [w for v in vcfs for w in ("-I", v)]. Every placeholder needs a value, and every value a placeholder.
    Write {{ and }} for braces. The text a command writes to standard output, at most 1 MiB, is its result, and an exit
    status other than zero fails it, with the end of its standard error in its TaskError. Never write to an input
    file. Settings go to set(), whose outputs may name placeholders, filled unquoted from these values."""

    __slots__ = ()

    def __init__(self, template, /, **fields):
        if not isinstance(template, str) or not template.strip():
            raise TypeError("a Command takes a command line as a non-empty string")
        if "\0" in template:
            raise ValueError("a command line contains a null byte")
        placeholders = _fields_of(template, "command line")
        if len(placeholders) != len(fields) or not all(name in fields for name in placeholders):
            problems = []
            missing = [name for name in placeholders if name not in fields]
            extra = [name for name in fields if name not in placeholders]
            if missing:
                problems.append(f"no value for {', '.join(missing)}")
            if extra:
                problems.append(f"no placeholder for {', '.join(extra)}")
            raise TypeError(f"Command({template!r}): {'; '.join(problems)}")
        for name, value in fields.items():
            kind = type(value)
            if kind in _LEAF_TYPE_SET:
                continue
            if isinstance(value, (set, frozenset)):
                raise TypeError(f"the value of {{{name}}} is a set, which has no order; pass sorted(...)")
            if kind is list or kind is tuple:
                for element in value:
                    if isinstance(element, (list, tuple, set, frozenset, dict)):
                        raise TypeError(f"the value of {{{name}}} is a list that holds a container; a list "
                                        f"renders each of its elements, which are single values")
                    _check_command_value(name, element)
            else:
                _check_command_value(name, value)
        self._flags = _make(self, template, (), fields)

    @property
    def template(self):
        """The command line, with its placeholders."""
        return self._func

    @property
    def fields(self):
        """The values of the placeholders, with the Tasks, selections, and files they hold."""
        return self.kwargs


class Expand(Task):
    """A task whose work is decided from results: once every task that its arguments hold finished,
    builder(*args, **kwargs) runs once, in this process, with their values, and returns a list of tasks, made as
    anywhere else. They run with the tasks they read that are not live yet, and the Expand's result is the list of
    their results, in order, a nested list for a nested Expand. The Executor does this by itself, with no call of
    result() or a wait in between: submitting the Expand is enough.

    Arguments are those of a Function. A task that the expansion makes live takes the settings of the Expand that it
    was not given itself, and an Expand writes no files of its own. A builder that raises, or returns anything but a
    list of tasks, fails the Expand with reason "expand", and so does a task it returns that reads the Expand or a
    task that went live after it. The builder never runs again, also when results are computed again."""

    __slots__ = ()

    def __init__(self, builder, /, *args, **kwargs):
        if not callable(builder) or isinstance(builder, _Symbol):
            raise TypeError("an Expand takes a builder function and its arguments, such as vg.Expand(per_file, "
                            "files_task)")
        flags = _make(self, builder, args, kwargs)
        self._flags = flags | _READ
        if self._shared:
            raise TypeError("the builder of an Expand runs in this process, where vg.Shared objects are not loaded")
        self._flags = flags

    @property
    def builder(self):
        """The function that returns the tasks the Expand expands into."""
        return self._func

    def _declare(self, outputs):
        raise TypeError("an Expand writes no files of its own, so it takes no outputs; give the tasks that its builder "
                        "returns their files")

    def file(self, name):
        raise TypeError("an Expand writes no files of its own; ask the tasks that its builder returns for their files")


# The classes of tasks, which type() tells apart without a walk of base classes.
_TASK_TYPES = frozenset((Function, Command, Expand))


def _kind(task):
    """Return which of Function, Command, and Expand a task is."""
    kind = type(task)
    if kind in _TASK_TYPES:
        return kind
    return Command if isinstance(task, Command) else Expand if isinstance(task, Expand) else Function


def _inherit(task, options, name, cache):
    """Give a task that an Expand expands into the settings of the Expand, options, name, and cache, that Task.set()
    did not give it."""
    flags = task._flags
    given = flags & _GIVEN_OPTIONS
    if not given:
        task._options = options
    elif given != _GIVEN_OPTIONS:
        own = task._options
        task._options = _intern_options(*[getattr(own if flags & _GIVEN[setting] else options, setting)
                                          for setting in _Options.__slots__])
    if not flags & _GIVEN["name"]:
        task._name = name
    if cache and not flags & _GIVEN["cache"]:
        task._flags = flags | _CACHE


def _revive(record, executor, owner=None):
    """Return a Task for the record of a live task whose own Task is not at hand: a task that an Expand expanded into,
    which has a Task only when something asks for one, as the TaskError of a task that it made fail does. It tells how
    the task ended and reads its result, and has none of its arguments."""
    task = object.__new__(record.kind)
    task.key = next(_keys)
    task._func = record.func
    task._args = ()
    task._kwargs = _NO_KWARGS
    task._parents = ()
    task._results = task._reads = task._shared = task._selections = None
    task._outputs = None
    if record.outputs:
        task._outputs = {name: TaskFile(task, name) for name in record.outputs}
    task._options = record.options
    task._name = record.name
    task._ref = None
    task._record = record
    task._executor = executor
    task._value = _UNREAD
    task._flags = _READ | _GUEST
    # The Task that lets go of the record, which lives while this one does.
    task._owner = owner
    return task


def _undeclared(task, name):
    """Return the message for a file of a task that was not declared before the task was submitted."""
    return (f"the file {name!r} of {task!r} was asked for after the task was submitted, so it was not kept; ask for "
            f"the files of a task before it is submitted, or list the files that the task writes in "
            f"task.set(outputs=...)")


def _label(task):
    """Return how messages name a task: its number and the name of its function, or the start of its command, or its
    key for a task not submitted yet."""
    record = task._record
    if record is None:
        return _name_task(task.key, task._func, task._name, ", not submitted")
    return _name_task(record.number, record.func, record.name)


def _name_task(number, func, name=None, suffix=""):
    """Return the name of a task with a number, of func, a function, a builder, or a command line, labelled name."""
    if name is None:
        if type(func) is str:
            words = func.split()
            name = words[0] if words else "command"
        else:
            name = getattr(func, "__name__", None) or type(func).__name__
    return f"Task({number!r}, {name}{suffix})"


def _check_command_value(name, value):
    """Raise for a value of a placeholder that a command cannot read."""
    if type(value) in _LEAF_TYPE_SET:
        return
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
            raise ValueError(f"{_label(task)} reads two files named {ref.name!r}: {other!r} and {ref!r}")
        if _clashes_with_output(ref.name, outputs):
            raise ValueError(f"{_label(task)} reads {ref!r} under the name of a file it writes")


def _clashes_with_output(name, outputs):
    """Return whether a sandbox name is one of outputs or a directory of one."""
    for output in outputs:
        if output == name or output.startswith(name + "/"):
            return True
    return False
