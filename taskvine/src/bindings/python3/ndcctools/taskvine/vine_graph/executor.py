# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Run tasks on TaskVine. An Executor owns a TaskVine Manager, the C graph executor, and the library that runs tasks
on Workers for as long as it is open. Tasks come from Graphs or from Executor.submit(), and each submitted task is
represented by a Future.

A background thread owns the C executor: it is the only thread that calls into C after the Executor starts, apart
from vine_graph_executor_wake(), which ends the thread's wait in C when a call is queued for it. The
caller's thread checks and serializes a submission, then queues the C calls for the background thread, so submit()
returns at once. The background thread runs queued calls, advances the run, and publishes what happened: the tasks
that finished and the result files that became readable. Waits read that published state, so an interrupt only ends a
wait and never interrupts the run."""

import atexit
import collections
import contextlib
import ctypes
import dataclasses
import errno
import functools
import html
import io
import importlib
import logging
import math
import operator
import os
import re
import shutil
import _signal
import _thread
import signal
import sys
import tempfile
import threading
import time
import types
import weakref
from contextlib import ExitStack
from pathlib import Path

import cloudpickle

# SWIG-generated vine_graph_capi.py imports "cvine"; wire that top-level name
# to the real TaskVine Python module before importing the generated bindings.
# Also expose _cvine's C symbols globally so _vine_graph_capi reuses the same
# TaskVine/dttools runtime instead of loading a second copy of those globals.
cvine = importlib.import_module("ndcctools.taskvine.cvine")
ctypes.CDLL(cvine._cvine.__file__, mode=getattr(os, "RTLD_NOW", 0) | getattr(os, "RTLD_GLOBAL", ctypes.RTLD_GLOBAL))
sys.modules.setdefault("cvine", cvine)

from ndcctools.taskvine.manager import Manager  # noqa: E402

from . import vine_graph_capi as capi  # noqa: E402
# The C functions themselves, which the background thread calls for every task without the Python function of the
# generated wrapper around each.
_capi = capi._vine_graph_capi
from .graph import _LEAF_TYPE_SET, File, Graph, Hoisted, Node, NodeFile, _FileRef, _Ref, _Selectable, _Symbol  # noqa: E402
from . import install, library  # noqa: E402

# Manager tuning that suits graph execution. set_params() overrides it.
MANAGER_TUNING = {
    "worker-source-max-transfers": 100,
    "max-retrievals": -1,
    "prefer-dispatch": 1,
    "transient-error-interval": 1,
    "attempt-schedule-depth": 10000,
    "temp-replica-count": 1,
    "enforce-worker-eviction-interval": -1,
    "shift-disk-load": 0,
    "clean-redundant-replicas": 0,
    # Gigabytes of task results the Manager may keep as checkpoints. Zero means unlimited.
    "vault-disk-limit": 1536,
}

# Settings of the C graph executor and their defaults.
EXECUTOR_TUNING = {
    # largest-input-first, depth-first, or fifo.
    "task-priority-mode": "largest-input-first",
    # Failed attempts allowed per task for infrastructure failures. A function that raises fails its task at once.
    "max-retries": 5,
    # 1 draws a progress bar on standard output while tasks run. Notebooks and interactive shells default to 0, because
    # the bar would draw over later cells or the prompt, and use progress() instead.
    "progress-bar": 1,
    "progress-bar-update-interval-sec": 0.1,
}

COMPLETED = "completed"
FAILED = "failed"
# The entry of every task that completed, which the tasks share.
_COMPLETED_ENTRY = (COMPLETED, None, None)
# TaskError.reason of each kind of failure that C reports
_FAILURE_REASONS = {
    capi.VINE_GRAPH_FAILURE_FUNCTION: "function",
    capi.VINE_GRAPH_FAILURE_SIGNAL: "signal",
    capi.VINE_GRAPH_FAILURE_WORKERS: "workers",
    capi.VINE_GRAPH_FAILURE_INPUT: "input",
    capi.VINE_GRAPH_FAILURE_RETRIES: "retries",
    capi.VINE_GRAPH_FAILURE_CANCELLED: "cancelled",
    capi.VINE_GRAPH_FAILURE_SUBMIT: "submit",
}

# Seconds the background thread spends on queued calls before it advances the run one step.
COMMAND_SECONDS = 0.05

# Seconds per step that the background thread spends at least on clean-up after the queued calls: releasing the pins
# of collected Futures, letting go of the tasks of collected Graphs, and removing and forgetting dropped tasks. Clean-up
# also takes what the queued calls left of COMMAND_SECONDS. Clean-up of millions of tasks takes tens of seconds, and a
# call queued meanwhile, such as stats() or a submission, waits at most one step for it.
CLEANUP_SECONDS = 0.01

# A line that the library runtime logs to a function call's standard output (stdout_timed_message()): its progress,
# which a failure message leaves out, or an error, of which it keeps the text after the prefix.
LIBRARY_LOG_LINE = re.compile(r"^\d\d/\d\d/\d\d \d\d:\d\d:\d\d\.\d+ (?:TASK \d+ (?=error:)|.*\n?)", re.MULTILINE)

# Dropped C nodes removed per call into C, between which clean-up checks its time.
REMOVAL_BATCH = 128

# Bytes up to which a serialized function or argument travels inside the manifest of its task. Larger objects become
# staged files, which every task that passes the same object shares. A file of its own takes at least one 4 KiB page
# on disk, so a smaller object is never cheaper as a file. References to other tasks and small values, which are usually
# different in every task, then add no file at all, and a small object that tasks share is copied at most this many
# bytes per task.
INLINE_LIMIT = 4096

# Small objects whose serialization tasks share by identity, like staged files: functions and classes, which many tasks
# pass and cloudpickle serializes by value when they are defined in __main__ (about 30 us for a small function). Every
# other small object is serialized for each task, so a mutable argument changed between submissions reaches later
# tasks. A function or class is serialized when the first task that holds it is submitted.
_SHARED_INLINE_TYPES = (types.FunctionType, type)

# Arguments that stand for data the task reads: references to task results and files, Files, and hoisted objects. The
# manifest holds them inside its own serialization instead of serializing each on its own, which cost about 15 us per
# argument, most of the caller's time per edge of a graph whose tasks pass many results. A reference to a whole result,
# the most common argument, is held as its task (graph id, key), a tuple that the pickler writes without calling
# Python, where a reference object costs a call of cloudpickle's reducer and a lookup of its dispatch table.
_REFERENCE_TYPES = (_Ref, _FileRef, File, Hoisted)

# Immutable values that the manifest holds as they are, inside its own serialization, instead of each being serialized
# on its own: numbers, None, and strings (bytes are serialized, so that the library tells an object it loads from a value
# it takes as it is). A task's numbers and short strings usually differ in every task, and staging each cost a lock, a
# pickle of its own, and a copy, about a fifth of the caller's time per task with two such arguments. Being immutable,
# they need no identity and no copy per task.
_LITERAL_TYPES = frozenset((int, float, bool, complex, type(None), str))

# Bytes of the longest file name, without its directory, that Linux file systems take (NAME_MAX).
_NAME_MAX = 255


# Open Executors. Creating an Executor on the port of an open one, as rerunning a notebook cell does, closes the open
# one first, and every Executor still open is closed before the interpreter tears its modules down.
_open_executors = weakref.WeakSet()


@atexit.register
def _close_open_executors():
    for executor in list(_open_executors):
        try:
            executor.close()
        except Exception:
            pass


class TaskError(RuntimeError):
    """A task failed, or it could not run because a task it depends on failed. task is the Node of the task, and
    source the Node of the task whose own failure explains it, or None when nothing holds that task anymore: its Graph
    is gone, or it was a call without a Future. The message names both tasks either way.

    reason says why the source failed:

    - "function": its function raised an exception or exited with an error status
    - "signal": its function was killed by a signal
    - "workers": Workers kept disconnecting while they ran it, as when it exceeds their memory or crashes them
    - "input": a file it reads is missing at the Manager
    - "retries": more of its attempts failed for other reasons than the setting max-retries allows
    - "cancelled": Future.cancel() stopped it
    - "submit": its task could not be created or submitted
    - "result": it completed, but its result cannot be loaded in this process or sent by its Worker

    exception is the exception that the function of the source raised, loaded in this process with the source's
    traceback as a note, so a caller can handle it by its type. It is None for any other reason, and when the
    exception cannot be serialized or loaded here."""

    def __init__(self, message, task, source, reason=None, exception=None):
        super().__init__(message)
        self.task = task
        self.source = source
        self.reason = reason
        self.exception = exception


@dataclasses.dataclass(frozen=True)
class Progress:
    """How many tasks an Executor was given, and how many of them completed or failed. The others are waiting or
    running. A notebook shows it as a progress bar."""

    total: int
    completed: int
    failed: int

    @property
    def waiting(self):
        return self.total - self.completed - self.failed

    def __str__(self):
        return f"{self.completed}/{self.total} completed, {self.failed} failed, {self.waiting} waiting"

    def _repr_html_(self):
        done = self.completed + self.failed
        return f'<progress value="{done}" max="{max(self.total, 1)}"></progress> {html.escape(str(self))}'


@dataclasses.dataclass(frozen=True)
class Capacity:
    """The Workers connected to an Executor and their cores, which run one task each at a time."""

    workers: int
    cores: int


class Future(_Selectable):
    """The result of a task submitted to an Executor, or the path of a file. While the Future exists, the Executor
    keeps the data it stands for, so tasks submitted later read it without recomputing it. Pass a Future to
    Executor.submit() to give a task its result, or select from it like a Node."""

    __slots__ = ("_executor", "_target", "_held", "__weakref__")

    def __init__(self, executor, target):
        self._executor = executor
        # The Node, NodeFile, or File, or the task (graph id, key) of a call, whose Node target makes on demand, so a
        # Future of a call is one object for the garbage collector to track instead of two.
        self._target = target
        # What the Executor keeps for this Future: the _Task of the task whose result it pins, or the data key (task,
        # sandbox path) of a file the task writes
        self._held = None

    def __del__(self):
        self._release()

    def _release(self):
        """Let the Executor drop the data this Future keeps. Collection may happen in any thread, so the background
        thread lets the data go, as clean-up work that never delays calls queued after it. A closed Executor ignores the
        call, and one collected at interpreter exit may find its modules gone already."""
        # An interrupt may stop __init__() before it set the key.
        key = getattr(self, "_held", None)
        if key is not None:
            executor = self._executor
            try:
                with executor._lock:
                    # The key is cleared next to the call that queues its release, so an interrupt never separates
                    # them, and a pin is released once.
                    if self._held is not None:
                        self._held = None
                        if not executor._closed:
                            # One built-in append, so an interrupt never leaves the release half queued.
                            executor._deferred.append((executor._queued, Executor._unhold, key))
                            executor._wake_locked()
            except Exception:
                pass

    @property
    def target(self):
        """The Node, NodeFile, or File this Future stands for."""
        target = self._target
        if type(target) is tuple:
            return Node(self._executor._dynamic, target[1])
        return target

    def done(self):
        """Return True once the task completed or failed."""
        return self._executor._finished_state(self._task) is not None

    def result(self, timeout=None):
        """Wait for the task and return its Python result, or the local path of a file. A failure raises TaskError,
        and a timeout in seconds that expires raises TimeoutError."""
        return self._executor._result(self, timeout)

    def exception(self, timeout=None):
        """Wait for the task and return its TaskError, or None when it completed."""
        self._executor._wait_for([self], timeout)
        return self._executor._error(self._task)

    def cancel(self):
        """Stop the task if it has not finished, and fail it and every task that depends on it. Return True if the task
        was cancelled, or False if it had already finished or runs in a local Executor, where it finished at
        submission."""
        return self._executor._cancel(self._task)

    def cancelled(self):
        """Return True if cancel() stopped the task."""
        return self._task in self._executor._cancelled

    def add_done_callback(self, fn):
        """Call fn(future) once the task completes or fails, or at once in this thread when it has finished already or
        the Executor is closed. Other calls run in a thread of the Executor, one at a time and in the order the tasks
        finished, so a callback should not wait long. A callback that raises is logged and ignored."""
        self._executor._add_done_callback(self, fn)

    @property
    def _task(self):
        target = self._target
        if type(target) is tuple:
            return target
        if isinstance(target, File):
            return None
        node = target.node if isinstance(target, NodeFile) else target
        return (node.graph._id, node.key)

    def __getitem__(self, item):
        self._check_selectable()
        return super().__getitem__(item)

    def attr(self, name):
        self._check_selectable()
        return super().attr(name)

    def _check_selectable(self):
        if not isinstance(self.target, Node):
            raise TypeError("only a task result can be selected from, and this Future stands for a file")

    def _as_ref(self):
        if self._executor._closed:
            raise RuntimeError("the Executor of this Future is closed")
        target = self._target
        if type(target) is tuple:
            return _Ref(target[0], target[1])
        return target._as_ref()

    def __reduce__(self):
        raise TypeError("a Future belongs to a running Executor and cannot be pickled; pickle the Graph instead")

    def __repr__(self):
        if self._executor._closed:
            state = "closed"
        elif self.cancelled():
            state = "cancelled"
        else:
            state = "done" if self.done() else "pending"
        return f"Future({self.target!r}, {state})"


class _Lock(_thread.RLock):
    """The reentrant lock of an Executor's shared state, which a KeyboardInterrupt never leaves held.

    Python raises an interrupt in the main thread after a call of a built-in function returns, so with the built-in
    lock, the interrupt can come right after a with statement took the lock and before the block whose exit releases
    it starts, and the lock stays held for good: the background thread then stops at its next step, and close() waits
    for it forever. The same can happen right after Condition.wait() released the lock, before the code that takes it
    back starts. This lock gives the lock back when an interrupt stops the call that took it, and takes it back when an
    interrupt stops the call that released it for a wait. Its built-in release stays as it is, because an interrupt
    after a release finds the lock released already. Use it in with statements, not through the Condition, whose own
    __enter__ and __exit__ are Python functions that an interrupt may stop before they reach the lock."""

    __slots__ = ()

    def __enter__(self):
        count = self._recursion_count()
        try:
            self.acquire()
        except BaseException:
            if self._recursion_count() > count:
                self.release()
            raise
        return True

    def _release_save(self):
        count = self._recursion_count()
        try:
            return _thread.RLock._release_save(self)
        except BaseException:
            # Condition.wait() releases the lock before its block that takes it back, and the with block around the
            # wait releases it again.
            if not self._is_owned():
                for _ in range(count):
                    self.acquire()
            raise


# Held while a local Executor runs a task, which changes the working directory of the process.
_LOCAL_TASKS = _Lock()


class _Call:
    """A queued call into C whose caller waits for its outcome."""

    def __init__(self, function):
        self.function = function
        self.finished = False
        self.value = None
        self.error = None

    def __call__(self):
        try:
            self.value = self.function()
        except Exception as error:
            self.error = error
        self.finished = True


class _Task:
    """What an Executor records about one submitted task, from its submission until it forgets the task: one object
    for each task instead of an entry in each of a dozen tables keyed by (graph id, key). A task's clean-up then looks
    up nothing by its key, because the Futures of its result hold this object, and its C node id leads to it.
    Lookups of tuple keys in tables of a million tasks were most of the background thread's Python time per dropped
    Future: each hashes the tuple and misses the processor cache two or three times (BUGS.md).

    The caller's thread sets number, label, and staged when it submits the task, the last under the Executor's lock,
    and the background thread sets the rest. finished is published under the lock."""

    __slots__ = ("task", "number", "label", "staged", "node", "result", "futures", "holds", "finished")

    def __init__(self, task):
        self.task = task  # (graph id, key), the tuple that keys the task in every table that still keys it
        self.number = 0  # the number that names the task's files in sandboxes
        # The name of the task's function, which messages name the task by, and which outlives its Graph
        self.label = None
        # Sandbox names and small function and class ids that the task holds, as a tuple, or the one name or id when
        # it holds only one, as most calls hold only their function. None before the task is queued and once it was
        # let go.
        self.staged = None
        self.node = 0  # C node id, set by the background thread
        self.result = 0  # C file id of the task result
        self.futures = 0  # live Futures that stand for the task, its result or its files
        self.holds = 0  # live Futures that pin the task result
        # (COMPLETED, None, None), or (FAILED, source task, message, reason) once the task finished. Only a source holds
        # its message and reason, which its dependents read from it.
        self.finished = None


# The C file id of a task's result, from its _Task, for map() over the tasks whose results a task mounts.
_RESULT_FILE = operator.attrgetter("result")


class _Submission(list):
    """A queued call that submits a task in the background thread, which is the list of what it declares and mounts
    (Executor._submit_node()). A queued task then costs the garbage collector one object instead of a partial, the
    tuple of its arguments, and a list, and a long queue survives young collections."""

    __slots__ = ("executor",)

    def __call__(self):
        self.executor._submit_node(self)


# Executor._unsubmitted_readers of a submitted task of a user Graph that its Graph does not read yet, a sink, which keeps
# its result until a reader of it is queued or its Graph is gone (Executor._track_readers()).
_NO_READER = object()


class _CallPins:
    """The tasks submitted by earlier calls that tasks of one submission call read. The object itself stands for the
    call among a task's readers in Executor._unsubmitted_readers, so each such task keeps its result until the call
    ends, and readers of it that the call queues in different steps of the background thread find it there.

    Every submit() makes one, and most calls read no task of an earlier call, so the object has no __init__, which
    would cost a Python call, and gets its list of parents with its first parent."""

    parents = ()


class _Serialized:
    """A pickle target that keeps the first limit bytes in memory and moves everything to a file at path only when the
    pickle grows past them."""

    def __init__(self, path, limit):
        self.path = path
        self.limit = limit
        self.buffer = bytearray()
        self.stream = None

    def write(self, data):
        if self.stream is None:
            if len(self.buffer) + len(data) <= self.limit:
                self.buffer += data
                return len(data)
            self.stream = open(self.path, "wb")
            self.stream.write(self.buffer)
            self.buffer = None
        return self.stream.write(data)

    def close(self):
        if self.stream is not None:
            self.stream.close()

    def discard(self):
        if self.stream is not None:
            self.stream.close()
            os.unlink(self.path)


class _Pickler:
    """The cloudpickle Picklers of the thread that creates them, reused for every object that thread serializes,
    because constructing one costs more than pickling a small object such as a manifest. dump() writes to the target
    given to it, and dumps() returns bytes. Each dump starts and ends with an empty memo, so a Pickler keeps no object
    alive. Other threads, and a dump nested in another, use a Pickler of their own.

    A Pickler's memo table keeps the capacity of its largest dump, and clearing it touches every slot: after a
    manifest with 200,000 references, each later dump of a small object took 2.5 ms instead of 1.5 us. A Pickler
    whose dump wrote more than LARGE_DUMP bytes is replaced by a new one. The bound is in bytes because the memo's size
    cannot be read cheaply, and a memoized object writes at least about 4 bytes, so a dump of up to 4 KB leaves at most
    about 1,000 entries, whose clearing costs later dumps about as much as a new Pickler (4 us). A bound of 256 KB let a
    210 KB dump of 30,000 small tuples, as some scenario of a long-lived Executor writes, make every later manifest
    take 300 us instead of 1.6 us: in a marathon the caller spent 76% of its time clearing memos (BUGS.md)."""

    LARGE_DUMP = 1 << 12

    def __init__(self):
        self._thread = threading.get_ident()
        self._target = None
        self._busy = False
        self._written = 0
        self._stream_pickler = cloudpickle.Pickler(self, protocol=cloudpickle.DEFAULT_PROTOCOL)
        self._buffer = io.BytesIO()
        self._bytes_pickler = cloudpickle.Pickler(self._buffer, protocol=cloudpickle.DEFAULT_PROTOCOL)

    def write(self, data):
        self._written += len(data)
        return self._target.write(data)

    def dump(self, value, target):
        if self._busy or threading.get_ident() != self._thread:
            cloudpickle.dump(value, target, protocol=cloudpickle.DEFAULT_PROTOCOL)
            return
        self._target = target
        self._written = 0
        try:
            self._run(self._stream_pickler, value)
        finally:
            self._target = None
            if self._written > self.LARGE_DUMP:
                self._stream_pickler = cloudpickle.Pickler(self, protocol=cloudpickle.DEFAULT_PROTOCOL)

    def dumps(self, value):
        if self._busy or threading.get_ident() != self._thread:
            return cloudpickle.dumps(value, protocol=cloudpickle.DEFAULT_PROTOCOL)
        buffer = self._buffer
        # An interrupt may have ended an earlier dump between its own clean-up steps, so the buffer is emptied first:
        # leftover bytes would otherwise come first in the result, and the reader would load an earlier manifest.
        buffer.seek(0)
        buffer.truncate()
        try:
            self._run(self._bytes_pickler, value)
            return buffer.getvalue()
        finally:
            large = buffer.tell() > self.LARGE_DUMP
            buffer.seek(0)
            buffer.truncate()
            if large:
                self._buffer = io.BytesIO()
                self._bytes_pickler = cloudpickle.Pickler(self._buffer, protocol=cloudpickle.DEFAULT_PROTOCOL)

    def _run(self, pickler, value):
        self._busy = True
        # The memo is cleared before each dump too: an interrupt between the clean-up steps of an earlier dump may have
        # left it filled, and a dump that refers to memo entries of another pickle loads wrong objects or fails.
        pickler.clear_memo()
        pickler.globals_ref = {}
        try:
            pickler.dump(value)
        finally:
            self._busy = False
            pickler.clear_memo()
            # cloudpickle shares the globals of functions it pickles by value within one pickle.
            pickler.globals_ref = {}


class _Watcher:
    """The tasks a caller of as_completed() waits for, and those of them that finished since it last looked."""

    def __init__(self, tasks):
        self.tasks = tasks
        self.finished = []


class Executor:
    """Run tasks on TaskVine Workers that connect to port, or to the Manager named name through the catalog.

    Tasks run when they are submitted: submit(func, *args, **kwargs) runs a call, and submit(node) runs a Graph task
    after the tasks it reads. Arguments may hold Futures and Nodes anywhere, which become dependencies. run(graph)
    runs a Graph and returns its results. An Executor is used like a file: close() it, or use it in a with block, which
    also discards every result when the block raises. A background thread advances the run, so tasks keep running
    while the program does other work, and progress() reports how far they got. Interrupting a wait or a submission
    leaves the Executor usable, so a notebook can continue in the next cell, where a progress bar follows the run.
    The Executor keeps the function and arguments of a task, to run it again when a Worker loses its result, while its
    Graph or a Future holds the task, and lets them go afterwards.

    local=True runs each task in this process when it is submitted, for debugging, without TaskVine. output_dir holds
    the directory of file results that result() returns, which stay after close. params holds settings for
    set_params(), and other keyword arguments go to the TaskVine Manager.

    The library that runs tasks on each Worker prepares itself once when it starts, and every task inherits what it
    prepared. It imports library_modules, given as modules or module names such as "torch", so tasks do not import
    them again, and loads hoist, a dictionary from names to objects. A task that takes vg.Hoisted(name) as an argument
    receives its own copy of that object without loading it. Hoisted objects stay in the library on every Worker until
    the Executor closes, so they suit a few large objects that most tasks use. library_files maps local paths to names
    in the library sandbox.

    edata_cache is a budget in bytes for function and argument objects that the library loads when it starts, so tasks
    do not fetch their files. The first submit() or run() call decides them once: of the objects it stages, the
    smallest go into the library until the next one does not fit, and the library is installed before any of its
    tasks run. Objects that later calls stage are fetched as files. The default, zero, caches nothing.
    """

    def __init__(self, port=cvine.VINE_DEFAULT_PORT, name=None, *, local=False, output_dir="./outputs",
                 library_modules=(), library_files=None, hoist=None, edata_cache=0, params=None, **manager_options):
        self._local = bool(local)
        self._closed = False
        self._manager = None
        self._executor = None  # opaque C executor handle
        self._resources = ExitStack()

        # State the caller's thread owns: what was submitted and how its files are named.
        self._dynamic = Graph()  # tasks submitted as calls
        # graph id -> Graph of submitted tasks, held weakly: a Graph the user drops lets its tasks go.
        self._graphs = weakref.WeakValueDictionary({self._dynamic._id: self._dynamic})
        # (graph id, key) of each submitted task -> its _Task, until the Executor forgets the task
        self._records = {}
        self._submitted = 0  # tasks submitted over the Executor's life, including those it forgot
        # Hoisted objects, each serialized on its own: name -> bytes. A local Executor loads one copy per task, as
        # forked tasks get on Workers, and a Worker library loads them from a file.
        self._hoisted = _serialize_hoisted(hoist)
        library_modules = [install.module_name(module) for module in library_modules]
        if not isinstance(edata_cache, int) or edata_cache < 0:
            raise ValueError("edata_cache takes a number of bytes, zero or more")
        self._edata_cache = edata_cache
        # Until the first submit() or run() decides the cached edata, the library waits to be installed, and calls
        # into C that the caller queues meanwhile are held, so no task reaches C before the library caches its edata.
        self._library_pending = bool(edata_cache) and not self._local
        self._held = None
        self._cached_edata = frozenset()  # sandbox names of staged objects that the library loads
        self._last_number = 0
        # (graph id, key) -> output paths the task declared when it was submitted, for tasks that declared any
        self._declared_outputs = {}
        # (graph id, key) -> {selection key: (steps, sandbox path)} of the selections of its result that the task writes
        # to files of its own, for tasks that write any
        self._selection_outputs = {}
        self._file_numbers = {}  # file identity -> number that names the file in sandboxes
        self._local_paths = {}  # file identity -> local path of a file a local task wrote
        self._local_seconds = 0.0
        self._keep_paths = set()

        # State the background thread owns: the C ids of submitted tasks and files.
        self._tasks = {}  # C node id -> _Task
        self._selection_files = {}  # (graph id, key) -> {selection key: C file id of the selection file}
        self._output_files = {}  # (graph id, key, path) -> C file id of a file the task writes
        self._input_files = {}  # absolute path of a File -> C file id
        self._edata_files = {}  # sandbox name of a staged object -> C file id
        self._released = set()  # tasks whose outputs C released, and which nobody asked for since
        # Tasks of user Graphs that ran again because a late reader read them after their release, and keep their
        # results while their Graphs live (_submit_node()).
        self._reread = set()
        # Tasks of user Graphs that C keeps because they ran again after their release (vine_graph_executor_next_kept()),
        # until their Graphs are gone.
        self._kept = set()

        # State both threads share, guarded by the lock. The background thread notifies the condition when it
        # publishes changes or finishes a call that a caller waits for.
        self._lock = _Lock()
        self._changed = threading.Condition(self._lock)
        # Held by a thread that submits, for the whole submission, and by close() while it marks the Executor closed.
        # Submissions read and change the state that the caller's thread owns, and two threads that submit at once would
        # both submit a task they share, and a local Executor runs a task in a working directory of its own.
        self._submitting = _Lock()
        self._commands = collections.deque()  # calls into C, run in order by the background thread
        # Calls ever queued, counted where they are queued, and calls the background thread ran. Clean-up work waits
        # for the calls queued before it by these counts.
        self._queued = 0
        self._ran = 0
        # Clean-up work: (calls queued before it, function, argument), in that order. The background thread calls
        # function(self, argument) once every call queued before the work ran, within CLEANUP_SECONDS per step, so a
        # large drop never delays calls queued after it. A function that returns False has work left and stays first.
        self._deferred = collections.deque()
        # Whether a caller woke the background thread's wait in C since the thread last found no queued call. Calls
        # queued meanwhile need no wake of their own.
        self._wake_pending = False
        self._counts = {COMPLETED: 0, FAILED: 0}
        # data key -> readable local path of a pinned output, or its bytes when the Manager holds them in memory,
        # published by the background thread
        self._paths = {}
        # Data keys name the data that Futures pin in the tables below: a task (graph id, key) for its result, and (task,
        # sandbox path) for a file the task writes. A Future of a result holds the task's _Task instead of its key.
        # data key (task, sandbox path) of a file -> Futures that pin it, for keys that have any; background thread
        # only. A _Task counts the Futures that pin its result.
        self._holds = {}
        # data keys of pinned outputs that their Workers cannot send, such as a directory that holds an entry its Worker
        # cannot read, published by the background thread until the next fetch of the key
        self._unsendable = set()
        self._fetches = {}  # C file id -> data key of a fetch not yet reported readable; background thread only
        self._cancelled = set()  # (graph id, key) of tasks that cancel() stopped
        # Callers in as_completed(): each watches some tasks and collects those that finish, so a wait wakes up for
        # new results instead of checking every pending Future again.
        self._watchers = set()
        # Serialized objects, shared by identity across tasks: id(object) -> (object, sandbox name, local path) for a
        # staged file, or (object, id(object), bytes) for a function or class small enough to travel inside manifests.
        # Holding the object keeps its id from being reused. An entry lives while a task that holds it may still run.
        self._edata = {}
        self._edata_names = {}  # sandbox name of a staged file -> (id of its object, local path)
        # Sandbox name of a staged file, or id of a small function or class -> tasks that hold it
        self._edata_holders = {}
        # graph id -> weak reference to a Graph whose tasks were submitted, whose callback adds the graph id to
        # _collected_graphs, in any thread, when the Graph is collected. The background thread lets its tasks go.
        self._graph_references = {}
        self._collected_graphs = {}
        self._graph_tasks = collections.defaultdict(set)  # id of a user Graph -> keys of its submitted tasks
        # (graph id, key) of a submitted task of a user Graph -> what its result stays pinned for: keys of the tasks of
        # its Graph that read it and are not submitted yet, or the _CallPins of a submission call that reads it, or
        # _NO_READER while its Graph holds no reader of it. The pin goes when the set is empty or the Graph is gone.
        self._unsubmitted_readers = {}
        self._run_error = None  # why the run stopped as a whole
        # (graph id, key) of an unfinished task -> its done-callbacks as (Future, function), which move to
        # _callbacks_due when it finishes, for the callback thread to run in order
        self._callbacks = {}
        self._callbacks_due = collections.deque()
        self._callback_thread = None
        # Capacity that the background thread reads once a second, from the first call of capacity() on
        self._capacity = None
        self._capacity_read = 0.0
        self._driver = None
        self._driver_ident = None
        self._pickler = _Pickler()
        self._display = None

        self._output_root = os.path.abspath(output_dir)
        # An output directory this Executor creates is removed again if it holds no file result at close.
        self._created_output_root = not os.path.isdir(self._output_root)
        self._run_dir = None
        self._edata_dir = None
        try:
            os.makedirs(self._output_root, exist_ok=True)
            self._run_dir = tempfile.mkdtemp(prefix="vine-graph-run-", dir=self._output_root)
            if not self._local:
                self._close_executor_on(port)
                self._start(port, name, library_modules, library_files, manager_options)
                # Workers load the hoisted objects from the library's file, so this process keeps only their names.
                self._hoisted = dict.fromkeys(self._hoisted)
            _open_executors.add(self)
            self.set_params(params or {})
            if _in_notebook():
                self._display = _ProgressDisplay(self.progress())
            if not self._local:
                self._driver = threading.Thread(target=_drive, args=(weakref.ref(self),), name="vine-graph-executor",
                                                daemon=True)
                self._driver.start()
                self._driver_ident = self._driver.ident
        except BaseException:
            self._close(failed=True)
            raise

    @staticmethod
    def _close_executor_on(port):
        if not isinstance(port, int) or port == 0:
            return
        for other in list(_open_executors):
            if not other._closed and not other._local and other.port == port:
                print(f"vine_graph: closing the open {other!r} to start a new Executor on port {port}", file=sys.stderr)
                other.close()

    def _start(self, port, name, library_modules, library_files, manager_options):
        run_info_path = manager_options.get("run_info_path")
        run_info_template = manager_options.get("run_info_template")
        if run_info_path and run_info_template:
            _remove_files(os.path.join(run_info_path, run_info_template))
        self._manager = Manager(port=port, name=name, **manager_options)
        for setting, value in MANAGER_TUNING.items():
            self._tune_manager(setting, value)
        hoisted_path = None
        if self._hoisted:
            hoisted_path = os.path.join(self._run_dir, library.HOISTED)
            install.save_objects(hoisted_path, self._hoisted)
        self._library_name = install.unique_name()
        self._library_options = (library_modules, library_files, hoisted_path)
        if not self._library_pending:
            self._install_library(None)
        self._executor = capi.vine_graph_executor_create(self._manager._taskvine, self._library_name,
                                                         library.run_node.__name__)
        if not self._executor:
            raise MemoryError("could not create the graph executor")
        for setting, value in EXECUTOR_TUNING.items():
            self._tune_executor(setting, value)
        if _in_notebook() or hasattr(sys, "ps1"):
            self._tune_executor("progress-bar", 0)
        self._edata_dir = self._resources.enter_context(
            tempfile.TemporaryDirectory(prefix="vine-graph-edata-", dir=self._manager.staging_directory))

    def _install_library(self, edata_path):
        """Install the library, which loads the cached edata at edata_path, if any, when it starts on a Worker."""
        modules, files, hoisted_path = self._library_options
        self._resources.enter_context(
            install.installed(self._manager, self._library_name, modules, files, hoisted_path, edata_path))

    @contextlib.contextmanager
    def _first_submission(self):
        """Around the first submit() or run() call of an Executor that caches edata, hold the calls into C that it
        queues. Afterwards, install the library with the smallest objects that the call staged, then release them."""
        if not self._library_pending or self._held is not None:
            yield
            return
        with self._lock:
            self._held = []
        try:
            yield
        finally:
            restore_interrupts = _block_interrupts()
            try:
                self._library_pending = False
                path = None
                try:
                    path = self._save_edata_cache()
                finally:
                    with self._lock:
                        held, self._held = self._held, None
                        if not self._closed:
                            self._queued += 1
                            self._commands.append(lambda: self._install_library(path))
                            self._commands.extend(held)
                            self._wake_pending = True
                            capi.vine_graph_executor_wake(self._executor)
            finally:
                restore_interrupts()

    def _save_edata_cache(self):
        """Write the smallest staged objects that fit in edata_cache bytes to the file the library loads, and return
        its path, or None when no object fits."""
        with self._lock:
            staged = [(name, path) for name, (identity, path) in self._edata_names.items()]
        # Equal sizes keep the order of staging.
        candidates = sorted((os.path.getsize(path), order, name, path) for order, (name, path) in enumerate(staged))
        chosen = {}
        total = 0
        for size, _, name, path in candidates:
            if total + size > self._edata_cache:
                break
            with open(path, "rb") as stream:
                chosen[name] = stream.read()
            total += size
        if not chosen:
            return None
        path = os.path.join(self._run_dir, library.EDATA_CACHE)
        install.save_objects(path, chosen)
        self._cached_edata = frozenset(chosen)
        # Only the library file holds cached objects, so their staged files go now, and the vault never sees them.
        with self._lock:
            for name in chosen:
                os.unlink(self._edata_names[name][1])
        return path

    # Lifetime.

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._close(failed=exc_type is not None)

    def __del__(self):
        try:
            self._close(failed=False)
        except Exception:
            pass

    def close(self):
        """Cancel unfinished tasks and release the Manager, the library, and every task result. File results that
        result() returned stay in output_dir. Closing a closed Executor does nothing."""
        self._close(failed=False)

    def _close(self, failed):
        # A submission in another thread completes first, and later ones find the Executor closed.
        with self._submitting, self._lock:
            if self._closed:
                return
            self._closed = True
            # Callbacks of tasks that will not finish now run, as their Futures report the Executor closed.
            for callbacks in self._callbacks.values():
                self._callbacks_due.extend(callbacks)
            self._callbacks.clear()
            # Waiters in other threads see the Executor closed, and the background thread wakes up to stop.
            self._changed.notify_all()
            if self._driver is not None:
                capi.vine_graph_executor_wake(self._executor)
        _open_executors.discard(self)
        if self._driver is not None and self._driver is not threading.current_thread():
            self._driver.join()
        try:
            try:
                if self._executor is not None:
                    capi.vine_graph_executor_delete(self._executor)
                    self._executor = None
            finally:
                try:
                    self._resources.close()
                finally:
                    if self._manager is not None:
                        self._manager._free()
                        self._manager = None
        except BaseException:
            failed = True
            raise
        finally:
            # A closed Executor that the program still holds keeps none of the staged objects.
            self._edata.clear()
            if self._run_dir is not None:
                self._remove_run_data(remove_all=failed or not self._keep_paths)
                self._run_dir = None
                if self._created_output_root and not os.listdir(self._output_root):
                    os.rmdir(self._output_root)

    def _remove_run_data(self, remove_all):
        if remove_all:
            shutil.rmtree(self._run_dir)
            return
        for directory, dirs, files in os.walk(self._run_dir, topdown=False):
            if self._within_kept_path(directory):
                continue
            for name in files:
                path = os.path.join(directory, name)
                if path not in self._keep_paths:
                    os.unlink(path)
            for name in dirs:
                path = Path(directory) / name
                if path.is_symlink():
                    if str(path) not in self._keep_paths:
                        path.unlink()
                elif str(path) not in self._keep_paths and not any(path.iterdir()):
                    path.rmdir()

    def _within_kept_path(self, path):
        """Return whether path is a directory result that result() returned, or lies inside one."""
        while path != self._run_dir:
            if path in self._keep_paths:
                return True
            path = os.path.dirname(path)
        return False

    def _check_open(self):
        if self._closed:
            raise RuntimeError("the Executor is closed")

    # Settings.

    @property
    def port(self):
        """The port Workers connect to, or None for a local Executor."""
        return None if self._manager is None else self._manager.port

    @property
    def name(self):
        """The name of the Manager in the catalog, or None."""
        return None if self._manager is None else self._manager.name

    def set_params(self, params):
        """Apply settings by name. Graph executor settings are task-priority-mode, max-retries, progress-bar, and
        progress-bar-update-interval-sec, and every other name is TaskVine Manager tuning, such as vault-disk-limit.
        Settings take effect at once, and an unknown name raises ValueError. A local Executor ignores Manager tuning."""
        self._check_open()
        for setting, value in params.items():
            if setting in EXECUTOR_TUNING:
                # The C executor owns the rules, which a local Executor follows too.
                problem = capi.vine_graph_executor_check_setting(setting, str(value))
                if problem:
                    raise ValueError(f"invalid value {value!r} for {setting}: {problem}")
                if not self._local:
                    self._call(lambda setting=setting, value=value: self._tune_executor(setting, value))
            else:
                # A local Executor has no Manager, but it checks the setting as a Worker run does.
                number = _manager_setting(setting, value)
                if not self._local:
                    self._call(lambda setting=setting, number=number: self._tune_manager(setting, number))

    def _tune_manager(self, setting, value):
        if self._manager.tune(setting, _manager_setting(setting, value)) != 0:
            raise ValueError(f"invalid value {value!r} for {setting}")

    def _tune_executor(self, setting, value):
        if capi.vine_graph_executor_tune(self._executor, setting, str(value)) != 0:
            raise ValueError(f"invalid value {value!r} for {setting}")

    def __repr__(self):
        if self._closed:
            return "Executor(closed)"
        where = "local" if self._local else f"port={self.port}"
        return f"Executor({where}, {self.progress()})"

    # Submission, in the caller's thread.

    def submit(self, func, /, *args, **kwargs):
        """Submit a task and return its Future. submit(func, *args, **kwargs) runs a call, whose arguments may hold
        Futures and Nodes. submit(node) runs a Graph task after every task it reads, submit(node.file(path)) stands
        for that file, and submit(File(path)) for a local file. Submitting a task again returns a new Future for
        it, and a task whose result was already released runs again."""
        if self._closed:
            raise RuntimeError("the Executor is closed")
        if isinstance(func, (Node, NodeFile, File)):
            if args or kwargs:
                raise TypeError("submit(node) takes no arguments; give them to Graph.add()")
            return self._submit_targets((func,))[0]
        if isinstance(func, _Symbol):
            raise TypeError("submit() takes a callable, a Node, a NodeFile, or a File")
        if not callable(func):
            raise TypeError("submit() takes a callable followed by its arguments")
        with self._submitting:
            self._check_open()
            dynamic = self._dynamic
            key = dynamic._next_key
            dynamic._next_key += 1
            dynamic._record(key, func, args, kwargs, True, self._register_symbol)
            # A call is submitted by its task, and its Future makes the Node only when asked for it.
            return self._submit_targets(((dynamic._id, key),))[0]

    def _register_symbol(self, symbol):
        """Run the user Graphs whose tasks the arguments of a call read. Arguments may also hold Futures of this
        Executor."""
        if isinstance(symbol, Future) and symbol._executor is not self:
            raise ValueError("a Future of another Executor cannot be an argument")
        source = symbol.source if hasattr(symbol, "source") else symbol
        node = source.node if isinstance(source, NodeFile) else source
        if isinstance(node, Node):
            self._add_graph(node.graph)

    def _submit_targets(self, targets):
        """Submit the tasks of targets after every ancestor that is not submitted yet, and return a Future for each
        target. Targets are Nodes, NodeFiles, Files, and tasks (graph id, key) of calls."""
        with self._submitting:
            self._check_open()
            if self._library_pending and self._held is None:
                with self._first_submission():
                    return self._submit_targets_now(targets)
            return self._submit_targets_now(targets)

    def _submit_targets_now(self, targets):
        """Submit tasks upstream first, each in a call of its own. The background thread advances the run between
        queued calls, and C releases a completed task whose readers and Futures it has not seen yet. So a task that
        tasks of its Graph read pins its result in its own submission while any of them is not submitted, in this call
        or a later one, and the last of them unpins it (_track_readers()). The Future of a target holds its data from
        the call that submits its task, so nothing can release a target before its Future holds it. A task submitted by
        an earlier call keeps its result for the readers that this call submits until the call ends, and a sink keeps
        its result while its Graph holds no reader of it (_track_readers())."""
        futures = [None] * len(targets)
        wanted = {}  # task -> [(index of a target, target)] of the targets of the task
        graphs = {}  # graph id -> Graph of the targets
        dynamic_id = self._dynamic._id
        for index, target in enumerate(targets):
            if type(target) is tuple:
                task = target
                graphs[dynamic_id] = self._dynamic
            elif isinstance(target, File):
                _check_input_file(target.path)
                futures[index] = Future(self, target)
                continue
            else:
                node = target.node if isinstance(target, NodeFile) else target
                graph = node.graph
                if node.key not in graph._tasks:
                    raise KeyError(f"no task with key {node.key!r}")
                if graph._id not in graphs:
                    self._add_graph(graph)
                    graphs[graph._id] = graph
                task = (graph._id, node.key)
            entry = wanted.get(task)
            if entry is None:
                wanted[task] = [(index, target)]
            else:
                entry.append((index, target))
        call = _CallPins()
        try:
            for task, graph in self._unsubmitted_ancestors(wanted, graphs):
                self._submit_task(graph, task, wanted.get(task, ()), futures, call)
            # Futures of tasks that were submitted before hold their data from now on.
            for task, entries in wanted.items():
                for index, target in entries:
                    if futures[index] is None:
                        futures[index] = self._new_future(task, target)
        except BaseException:
            # A queued task stays submitted, and the Futures that the caller never receives let their data go. A task
            # that was not queued, because staging failed or was interrupted, may be submitted again.
            for future in futures:
                if future is not None:
                    future._release()
            raise
        finally:
            if call.parents:
                self._end_call(call)
        return futures

    def _end_call(self, call):
        """Release the pins that the tasks submitted by earlier calls took for the readers that a call submitted, after
        its last reader of each was queued."""
        restore_interrupts = _block_interrupts()
        try:
            with self._lock:
                unsubmitted_readers = self._unsubmitted_readers
                unpins = []
                for parent in call.parents:
                    readers = unsubmitted_readers.get(parent)
                    if type(readers) is set and call in readers:
                        readers.discard(call)
                        if not readers:
                            del unsubmitted_readers[parent]
                            unpins.append(parent)
                call.parents = ()
                if unpins and not self._closed:
                    # The records and the call that queues their unpins are next to each other, so an interrupt never
                    # separates them.
                    self._queued += 1
                    (self._commands if self._held is None else self._held).append(
                        functools.partial(self._unpin_results, unpins))
                    self._wake_locked()
        finally:
            restore_interrupts()

    def _unsubmitted_ancestors(self, tasks, graphs):
        """Return (task, Graph) of the given tasks that are not submitted yet and of their unsubmitted ancestors,
        upstream first."""
        records = self._records
        if len(tasks) == 1:
            # A single task that reads no other task, such as most calls, needs no walk.
            task = next(iter(tasks))
            if task in records:
                return ()
            graph = graphs.get(task[0])
            if graph is not None and task[1] not in graph._parents:
                return ((task, graph),)
        order = []
        visiting = [(task, False) for task in reversed(tuple(tasks))]
        seen = set()
        while visiting:
            current, expanded = visiting.pop()
            if expanded:
                order.append(current)
                continue
            if current in seen or current in records:
                continue
            seen.add(current)
            graph_id, key = current
            graph = graphs.get(graph_id)
            if graph is None:
                graph = graphs[graph_id] = self._graphs.get(graph_id)
            if graph is None or key not in graph._tasks:
                raise KeyError(f"no submitted Graph has a task with key {key!r}")
            visiting.append(((current, graph), True))
            # A task may read many others, so parents that were submitted are skipped here, without a visit each.
            for parent_graph_id, parent_key in graph._parents.get(key, ()):
                parent = (graph_id if parent_graph_id is None else parent_graph_id, parent_key)
                if parent not in records:
                    visiting.append((parent, False))
        return order

    def _new_future(self, task, target):
        """Return a Future for a target whose task was submitted before, which holds its data from now on."""
        if isinstance(target, NodeFile) and target.path not in self._declared_outputs.get(task, ()):
            raise ValueError(f"the file {target.path!r} was declared after its task was submitted")
        future = Future(self, target)
        if not self._local:
            key = (task, target.path) if isinstance(target, NodeFile) else self._records[task]
            restore_interrupts = _block_interrupts()
            try:
                # The Future keeps its data, which is fetched to the Manager as soon as the task completes. Closing the
                # Executor releases every pin, so unpinning at interpreter exit could only reach freed state. The key
                # and the call that queues the pin are next to each other, so an interrupt never separates the pin
                # from the Future that releases it.
                command = functools.partial(self._hold, key)
                with self._lock:
                    if not self._closed:
                        future._held = key
                        self._queued += 1
                        (self._commands if self._held is None else self._held).append(command)
                        self._wake_locked()
            finally:
                restore_interrupts()
        return future

    def _add_graph(self, graph):
        if graph._id not in self._graphs:
            self._graphs[graph._id] = graph
            if not self._local:
                # Collection may happen in any thread, so the background thread lets the Graph's tasks go. The callback
                # is a built-in call, which an interrupt cannot stop halfway as it can a Python finalizer.
                self._graph_references[graph._id] = weakref.ref(
                    graph, functools.partial(self._collected_graphs.__setitem__, graph._id))

    def _submit_task(self, graph, task, targets, futures, call):
        """Check and stage one task, then run it locally or queue it for the background thread. A task interrupted
        before it was queued can be submitted again. targets holds (index, target) of the targets of the task, whose
        Futures go to futures at those indices and hold their data from the call that submits the task."""
        graph_id, key = task
        hoisted = graph._hoisted.get(key) if graph._hoisted else None
        if hoisted:
            for name in hoisted:
                if name not in self._hoisted:
                    raise KeyError(f"no hoisted object named {name!r}; give it to Executor(hoist={{...}})")
        for ref in graph._reads.get(key, ()):
            if isinstance(ref, File):
                _check_input_file(ref.path)
            if not isinstance(ref, File) and ref.path not in self._declared_outputs.get(_resolve(graph_id, ref), ()):
                raise ValueError(f"the file {ref.path!r} was declared after its task was submitted")
        names = []
        submitted = False
        record = _Task(task)
        try:
            # The task is recorded inside the block that forgets it again unless it was queued, because an interrupt
            # may stop any call. The statements below make no call, so an interrupt never comes between them.
            record.number = self._last_number = self._last_number + 1
            self._records[task] = record
            self._submitted += 1
            # Messages name the task by its key and the name of its function, which are recorded now because the task
            # may outlive its Graph. The message is formatted only when a task fails (_label()).
            func = graph._tasks[key][0]
            record.label = getattr(func, "__name__", None) or type(func).__name__
            outputs = graph._outputs.get(key)
            if outputs:
                self._declared_outputs[task] = tuple(outputs)
            if not self._local and graph._selections:
                selections = _selections_read_from(graph, key)
                if selections:
                    self._selection_outputs[task] = selections
            if self._local:
                self._run_local(task)
                submitted = True
                for index, target in targets:
                    futures[index] = Future(self, target)
            else:
                plan = self._plan(graph, record, names)
                restore_interrupts = _block_interrupts()
                try:
                    with self._lock:
                        holds = []
                        for index, target in targets:
                            future = futures[index] = Future(self, target)
                            future._held = (task, target.path) if isinstance(target, NodeFile) else record
                            holds.append(future._held)
                        record.staged = names[0] if len(names) == 1 else tuple(names)
                        if graph is not self._dynamic:
                            self._graph_tasks[graph_id].add(key)
                        command = _Submission(plan)
                        command.executor = self
                        command += self._track_readers(graph, task, call)
                        command.append(holds or ())
                        # Another thread of the program may let an interrupt through while this one holds it back, and
                        # Python raises it after a call returns. The flag and the call that queues the task are
                        # therefore next to each other, so a task is never queued without its records or recorded
                        # without being queued.
                        self._queued += 1
                        submitted = True
                        (self._commands if self._held is None else self._held).append(command)
                        self._wake_locked()
                        if graph is self._dynamic:
                            graph._drop_arguments(key)
                finally:
                    restore_interrupts()
        finally:
            if not submitted:
                if self._records.pop(task, None) is not None:
                    self._submitted -= 1
                self._declared_outputs.pop(task, None)
                self._selection_outputs.pop(task, None)
                for index, _ in targets:
                    # A Future made for a plan that was not queued holds nothing.
                    if futures[index] is not None:
                        futures[index]._held = None
                        futures[index] = None
                with self._lock:
                    record.staged = None
                    self._drop_staged(names)

    def _track_readers(self, graph, task, call):
        """Record a task that is being queued as a reader of its parents, and its own unsubmitted readers. The release
        rule only sees submitted readers, so the result of a task that its Graph reads elsewhere stays pinned until the
        last such reader is queued, whether that reader comes in this call or a later one. Otherwise a completed task
        is released, with its ancestors, between two calls that read it, and runs again for the later one.

        A task added to its Graph after the task it reads was submitted is a late reader. A sink, a task that its Graph
        does not read when it is submitted, keeps its result until a reader of it is queued or its Graph is gone,
        because the next task added to a growing Graph usually reads the latest results: an iterative loop that adds
        a step reading the previous one and runs it otherwise recomputes every earlier step. Any other task that late
        readers read keeps its result, or the result it runs again for, until the call that submits them ends (call),
        so it runs again at most once per call instead of once for each reader that the background thread submits in
        a later step. Return whether the task's result takes a pin, the parents whose pins it releases once it is
        submitted, and the parents it pins for the call. Called with the lock held."""
        graph_id, key = task
        unsubmitted_readers = self._unsubmitted_readers
        unpins = pins = ()
        for parent_graph_id, parent_key in graph._parents.get(key, ()):
            # Readers are tasks of the Graph of the task they read, which refers to its own tasks with graph id None.
            if parent_graph_id is not None:
                continue
            parent = (graph_id, parent_key)
            readers = unsubmitted_readers.get(parent)
            if readers is None:
                # A late reader of a task that an earlier call submitted and nothing keeps for its readers anymore.
                unsubmitted_readers[parent] = {call}
                if call.parents:
                    call.parents.append(parent)
                else:
                    call.parents = [parent]
                if not pins:
                    pins = []
                pins.append(parent)
            elif readers is _NO_READER:
                # The first reader of a sink: the sink's pin now lasts until this call ends.
                unsubmitted_readers[parent] = {call}
                if call.parents:
                    call.parents.append(parent)
                else:
                    call.parents = [parent]
            else:
                # A set is removed once it is empty, so the reader that empties it is the last one.
                readers.discard(key)
                if not readers:
                    del unsubmitted_readers[parent]
                    if not unpins:
                        unpins = []
                    unpins.append(parent)
        # A task is queued only after every task it reads, so none of its readers is queued yet.
        children = graph._children.get(key)
        if children:
            unsubmitted_readers[task] = set(children)
            return True, unpins, pins
        if graph is not self._dynamic:
            unsubmitted_readers[task] = _NO_READER
            return True, unpins, pins
        return False, unpins, pins

    def _plan(self, graph, record, names):
        """Stage the arguments and manifest of the task of a _Task, and return what the background thread declares and
        mounts. The sandbox name of every staged file the task holds is added to names."""
        task = record.task
        graph_id, key = task
        func, args, kwargs = graph._tasks[key]
        # The manifest is keyed by the references as the arguments hold them, and the Executor resolves them.
        files = {}
        for ref in graph._reads.get(key, ()):
            identity = library.file_identity(ref)
            files[identity] = self._file_task_path(_resolve_file(graph_id, identity))
        # The manifest holds each small object serialized inside it, names each larger one by the sandbox path of its
        # staged file, and holds references to the data the task reads as they are (_REFERENCE_TYPES). An object passed
        # as several arguments of the task is one entry, so it stays one object.
        inline = []
        entries = {}  # id of an object -> its manifest entry
        edata = []  # (sandbox name, local path) of each staged file the task mounts
        stage = self._manifest_entry
        # A reference to a whole result, the most common argument, is held without a call of _manifest_entry().
        manifest_args = [(value.graph_id, value.key) if type(value) is _Ref and not value.steps
                         else stage(value, entries, names, inline, edata) for value in args]
        manifest_kwargs = {argument: stage(value, entries, names, inline, edata) for argument, value in kwargs.items()}
        # The library replaces the references in arguments that may hold them: references, and the copies the Graph
        # stored of containers that held symbols. An argument stored as it was passed holds none (Graph._plain), and
        # scanning an object-heavy one took the task longer than loading it.
        plain = graph._plain

        def holds_references(value):
            kind = type(value)
            if kind in _LEAF_TYPE_SET:
                return False
            if kind in _REFERENCE_TYPES:
                return True
            entry = plain.get(id(value))
            return entry is None or entry[0] is not value

        scan = (tuple(index for index, value in enumerate(args) if holds_references(value)),
                tuple(argument for argument, value in kwargs.items() if holds_references(value)))
        # A result that the task reads at exactly one place, through a selection, is selected, and the task mounts
        # the producer's file of that selection when the producer writes one, instead of the whole result.
        # The tasks whose whole results the task mounts and their sandbox paths, vine-graph-result-<number of the
        # task>, are separate lists, which the background thread mounts in one call.
        selected = {}
        selections_read = []  # (task, sandbox path, selection key of the producer's selection file)
        read_once = graph._selections.get(key)
        records = self._records
        results = graph._results.get(key, ())
        # The tasks whose whole results the task mounts are given to the background thread as their _Tasks.
        if not results:
            result_tasks, result_paths, inputs = (), (), {}
        elif read_once is None:
            # A task that reads no result through a selection, the common case, mounts every result it reads.
            result_tasks = [records[parent if parent[0] is not None else (graph_id, parent[1])] for parent in results]
            result_paths = [f"vine-graph-result-{source.number}" for source in result_tasks]
            inputs = dict(zip(results, result_paths))
        else:
            result_tasks, result_paths, inputs = [], [], {}
            for parent in results:
                source = parent if parent[0] is not None else (graph_id, parent[1])
                if parent in read_once:
                    chain = read_once[parent][0]
                    if chain is not None and chain in self._selection_outputs.get(source, ()):
                        selected[parent] = self._selection_task_path(source)
                        selections_read.append((source, selected[parent], chain))
                        continue
                    selected[parent] = None
                source = records[source]
                inputs[parent] = path = f"vine-graph-result-{source.number}"
                result_tasks.append(source)
                result_paths.append(path)
        selections = self._selection_outputs.get(task)
        manifest = {
            "objects": inline,
            "callable": stage(func, entries, names, inline, edata),
            "args": manifest_args,
            "kwargs": manifest_kwargs,
            "scan": scan,
            "inputs": inputs,
            "selected": selected,
            "selections": list(selections.values()) if selections else [],
            "files": files,
        }
        return (
            record,
            self._declared_outputs.get(task, ()),
            [(chain, path) for chain, (_, path) in selections.items()] if selections else (),
            result_tasks,
            result_paths,
            selections_read or (),
            [(_resolve_file(graph_id, identity), name) for identity, name in files.items()] if files else (),
            edata or (),
            self._serialize_manifest(manifest),
        )

    def _serialize_manifest(self, manifest):
        """Return the bytes of a task's manifest, which the background thread mounts as a buffer that its C node owns."""
        return self._pickler.dumps(manifest)

    def _manifest_entry(self, value, entries, names, inline, edata):
        """Return how the manifest of a task refers to its function or an argument value. entries maps the id of each
        object the task passes to its entry, so an object passed as several arguments is staged once."""
        kind = type(value)
        if kind is _Ref and not value.steps:
            return (value.graph_id, value.key)
        if kind in _REFERENCE_TYPES:
            return value
        if kind in _LITERAL_TYPES and (kind is not str or len(value) <= INLINE_LIMIT):
            inline.append(value)
            return len(inline) - 1
        entry = entries.get(id(value))
        if entry is None:
            entry = entries[id(value)] = self._stage_argument(value, names, inline, edata)
        return entry

    def _stage(self, value, inline_limit=0):
        """Serialize one object. Return None and its bytes when they fit in inline_limit, and otherwise write them to
        a new staged file and return its sandbox name and local path. Small objects never touch the disk, and a large
        one is written as it is serialized instead of being held in memory."""
        self._last_number += 1
        name = f"vine-graph-edata-{self._last_number}"
        target = _Serialized(os.path.join(self._edata_dir, name), inline_limit)
        try:
            self._pickler.dump(value, target)
            target.close()
        except BaseException:
            # A long-lived Executor would otherwise keep the partial file of a failed or interrupted submission.
            target.discard()
            raise
        if target.stream is None:
            return None, bytes(target.buffer)
        return name, target.path

    def _stage_argument(self, value, names, inline, edata):
        """Return how the manifest refers to the function or an argument of a task: the index of its bytes in inline
        when it is small, or else the sandbox name of its staged file, whose name and local path are added to edata. A
        staged file, or the bytes of a small function or class, that an earlier task still holds is reused, and its
        key, a sandbox name or the object's id, is added to names, which holds it for the task. Any other small object
        is serialized for every task that passes it."""
        # Share top-level objects by identity, not content. Independent pickle streams do not preserve aliases nested
        # across different objects or callable closures.
        holders = self._edata_holders
        with self._lock:
            entry = self._edata.get(id(value))
            if entry is not None:
                holders[entry[1]] += 1
                names.append(entry[1])
        if entry is None:
            name, staged = self._stage(value, INLINE_LIMIT)
            if name is None and not isinstance(value, _SHARED_INLINE_TYPES):
                inline.append(staged)
                return len(inline) - 1
            # A small function or class is keyed by its id, which never equals a sandbox name.
            entry = (value, id(value) if name is None else name, staged)
            with self._lock:
                self._edata[id(value)] = entry
                if name is not None:
                    self._edata_names[name] = (id(value), staged)
                holders[entry[1]] = 1
                names.append(entry[1])
        if isinstance(entry[1], str):
            edata.append((entry[1], entry[2]))
            return entry[1]
        inline.append(entry[2])
        return len(inline) - 1

    def _enqueue(self, command):
        """Queue a call into C for the background thread. Garbage collection may call this from any thread. A wake
        that the background thread has not answered yet covers every call queued after it, so a caller that queues
        many calls makes one system call."""
        with self._lock:
            if self._closed:
                return
            self._queued += 1
            if self._held is not None:
                self._held.append(command)
                return
            self._commands.append(command)
            self._wake_locked()

    def _wake_locked(self):
        """End the background thread's wait in C so it runs the calls queued for it. Called with the lock held."""
        if self._closed or self._held is not None or self._wake_pending or self._driver is None or \
                threading.get_ident() == self._driver_ident:
            # The background thread runs every queued call before it waits again.
            return
        # Closing deletes the C executor only after the thread stopped, and the lock keeps this call before that.
        self._wake_pending = True
        capi.vine_graph_executor_wake(self._executor)

    def _call(self, function):
        """Run a call into C in the background thread and return its value, or raise its error. Before the thread
        starts, the caller's thread runs it."""
        if self._driver is None:
            return function()
        call = _Call(function)
        self._enqueue(call)
        self._wait_until(lambda: call.finished, None, "the call did not finish")
        if call.error is not None:
            raise call.error
        return call.value

    def _sync(self):
        """Wait until the background thread ran every call queued so far, and the clean-up work queued so far."""
        with self._lock:
            queued = self._queued
        self._call(lambda: None)
        if self._driver is not None:
            self._wait_until(lambda: not self._deferred or self._deferred[0][0] > queued, None,
                             "the clean-up did not finish")

    def _defer_locked(self, function, argument):
        """Queue clean-up work for the background thread, after the calls queued so far. Called with the lock held."""
        self._deferred.append((self._queued, function, argument))
        self._wake_locked()

    # Calls into C, in the background thread.

    def _submit_node(self, plan):
        """Declare a task's outputs, mount its inputs and arguments, submit it, and pin and fetch the data of its new
        Futures. Only the C executor can fail this, which stops the run."""
        (record, outputs, selections, result_tasks, result_paths, selections_read, files, edata, manifest, readers,
         unpins, pins, holds) = plan
        executor = self._executor
        for parent in pins:
            # A parent of a late reader keeps its result until the reader's call ends, which releases this pin. A
            # parent that was released runs again for the reader, which shows that its Graph reads it after its
            # release, so it keeps its result with a second pin while its Graph lives.
            file = self._records[parent].result
            if capi.vine_graph_executor_pin_file(executor, file) != 0:
                raise RuntimeError("failed to pin a result")
            if parent in self._released and parent not in self._reread:
                self._reread.add(parent)
                capi.vine_graph_executor_pin_file(executor, file)
        if record.node:
            # An interrupt between queueing a task and recording it makes the caller queue it again.
            return
        task = record.task
        graph_id, key = task
        node = capi.vine_graph_executor_add_node(executor)
        if not node:
            raise MemoryError(f"could not add a graph node for {graph_id}:{key!r}")
        record.node = node
        self._tasks[node] = record
        record.result = result = self._add_output(node, library.RESULT)
        if selections:
            self._selection_files[task] = {chain: self._add_output(node, path) for chain, path in selections}
        for path in outputs:
            self._output_files[(graph_id, key, path)] = self._add_output(node, path)
        if result_tasks or selections_read:
            # A released task that a new task reads runs again.
            if self._released:
                self._released.difference_update([source.task for source in result_tasks])
                self._released.difference_update(parent for parent, _, _ in selections_read)
            # A task may read many results, which one call mounts without Python code for each.
            mounts = list(zip(map(_RESULT_FILE, result_tasks), result_paths))
            mounts.extend((self._selection_files[parent][chain], name) for parent, name, chain in selections_read)
            self._check(capi.vine_graph_executor_add_inputs(executor, node, mounts), f"failed to mount the inputs of task {key!r}")
        for identity, name in files:
            if identity[0] == "input":
                if identity[1] not in self._input_files:
                    self._input_files[identity[1]] = self._declare_file(self._link_input(identity[1]))
                file = self._input_files[identity[1]]
            else:
                self._released.discard(identity[1:3])
                file = self._output_files[identity[1:]]
            self._add_input(node, file, name)
        # The library holds the cached objects, so tasks neither mount nor fetch their files.
        for name, path in edata:
            if name not in self._cached_edata:
                file = self._edata_files.get(name)
                if file is None:
                    file = self._edata_files[name] = self._declare_file(path, vault=True)
                self._add_input(node, file, name)
        # The node owns its manifest, a buffer that the Manager sends with the task, so a manifest costs no file, vault
        # link, or unlink at the Manager, and the C executor undeclares it when it removes the node.
        if capi.vine_graph_executor_add_input_buffer(executor, node, manifest, library.MANIFEST) != 0:
            raise RuntimeError(f"failed to declare the manifest of task {key!r}")
        if capi.vine_graph_executor_submit_node(executor, node) != 0:
            raise RuntimeError(f"failed to submit task {key!r}")
        if readers and capi.vine_graph_executor_pin_file(executor, result) != 0:
            raise RuntimeError("failed to pin a result")
        for parent in unpins:
            self._unpin_result(parent)
        for data_key in holds:
            self._hold(data_key)

    def _unpin_result(self, task):
        """Release a pin that a task took in its own submission. C tells of a release that the unpin made by its result
        instead of a report, so the task is let go here as a reported one would be."""
        record = self._records[task]
        if _capi.vine_graph_executor_unpin_file(self._executor, record.result) == 1:
            with self._lock:
                if self._let_go(record, True):
                    self._drop(record)

    def _unkeep(self, task):
        """Let go of a task that C kept because it ran again after its release, as _unpin_result() does for a pin."""
        record = self._records[task]
        if _capi.vine_graph_executor_unkeep_node(self._executor, record.node) == 1:
            with self._lock:
                if self._let_go(record, True):
                    self._drop(record)

    def _unpin_results(self, tasks):
        """Release the pins that tasks took for the readers of a call that ended."""
        for task in tasks:
            self._unpin_result(task)

    def _add_output(self, node, task_path):
        file = capi.vine_graph_executor_add_output(self._executor, node, task_path)
        if not file:
            raise RuntimeError(f"failed to declare output {task_path!r}")
        return file

    def _add_input(self, node, file, task_path):
        self._check(capi.vine_graph_executor_add_input(self._executor, node, file, task_path),
                    f"failed to mount {task_path!r}")

    def _declare_file(self, path, vault=False):
        file = capi.vine_graph_executor_declare_file(self._executor, path, int(vault))
        if not file:
            raise RuntimeError(f"failed to declare file {path}")
        return file

    def _check(self, status, message):
        if status != 0:
            raise RuntimeError(message)

    def _data_key(self, target):
        node = target.node if isinstance(target, NodeFile) else target
        task = (node.graph._id, node.key)
        return (task, target.path) if isinstance(target, NodeFile) else task

    def _hold(self, held):
        """Pin the data of a new Future and fetch it: the result of the task of a _Task, or the file of a data key
        (task, sandbox path). A released task that is asked for again runs again. The counts of Futures and holds,
        and the released tasks, belong to the background thread."""
        if type(held) is _Task:
            record = held
            file = record.result
            record.holds += 1
            key = record.task
        else:
            key = held
            task, path = key
            record = self._records[task]
            file = self._output_files[(task[0], task[1], path)]
            self._holds[key] = self._holds.get(key, 0) + 1
        record.futures += 1
        if self._released:
            self._released.discard(record.task)
        self._fetches[file] = key
        # Only this thread changes _unsendable, so it may test it without the lock, which the caller's thread holds
        # while it submits. The key is almost never there.
        if self._unsendable and key in self._unsendable:
            with self._lock:
                self._unsendable.discard(key)
        executor = self._executor
        if _capi.vine_graph_executor_pin_file(executor, file) != 0:
            raise RuntimeError("failed to pin a result")
        if _capi.vine_graph_executor_fetch_file(executor, file) != 0:
            raise RuntimeError("failed to fetch a result")

    def _unhold(self, held):
        """Clean-up work for a Future that was collected: release its pin, given as for _hold(). Called with the lock
        held."""
        if type(held) is _Task:
            record = held
            file = record.result
            released = _capi.vine_graph_executor_unpin_file(self._executor, file) == 1
            record.holds -= 1
            if not record.holds:
                # A result no Future pins may leave the vault, so its path waits for the next fetch.
                self._fetches.pop(file, None)
                self._paths.pop(record.task, None)
                if self._unsendable:
                    self._unsendable.discard(record.task)
        else:
            task, path = held
            record = self._records[task]
            file = self._output_files[(task[0], task[1], path)]
            released = _capi.vine_graph_executor_unpin_file(self._executor, file) == 1
            holds = self._holds[held] - 1
            if holds:
                self._holds[held] = holds
            else:
                del self._holds[held]
                self._fetches.pop(file, None)
                self._paths.pop(held, None)
                self._unsendable.discard(held)
        record.futures -= 1
        if record.futures:
            if released:
                self._released.add(record.task)
        elif self._let_go(record, released):
            self._drop(record)

    def _graph_gone(self, gone):
        """Clean-up work for a collected Graph: unpin the results that its unsubmitted tasks would have read, and let go
        of its tasks, a slice at a time. Return False while tasks are left. Called with the lock held.

        The Graph is gone, so no call queued after it was collected reads its tasks, and the C nodes of the tasks let go
        are dropped at once."""
        graph_id, keys = gone
        monotonic = time.monotonic
        deadline = self._cleanup_deadline
        unsubmitted_readers = self._unsubmitted_readers
        reread = self._reread
        kept = self._kept
        let_go = self._let_go
        records = self._records
        executor = self._executor
        drop_node = _capi.vine_graph_executor_drop_node
        count = 0
        while keys:
            task = (graph_id, keys.pop())
            # Tasks of a dropped Graph are never submitted, so the results they would have read are unpinned.
            if unsubmitted_readers and unsubmitted_readers.pop(task, None):
                self._unpin_result(task)
            if reread and task in reread:
                reread.discard(task)
                self._unpin_result(task)
            if kept and task in kept:
                kept.discard(task)
                self._unkeep(task)
            record = records.get(task)
            if record is not None and let_go(record):
                drop_node(executor, record.node)
            count += 1
            if not count & 63 and monotonic() >= deadline:
                return False if keys else None
        return None

    def _let_go(self, record, released=False):
        """Free the staged files of the task of a _Task when nothing holds the task and it will not run again, and
        return True when it did, so the caller drops its C node. The user holds a task through its Graph or a Future.
        A failed task never runs again, and neither does a released one unless asked for. released is set for a task
        that C has just reported released, which is recorded as released only when it cannot be let go yet. Called
        with the lock held."""
        task = record.task
        names = record.staged
        if names is None or record.futures or task[0] in self._graph_references:
            if released:
                self._released.add(task)
            return False
        if not released:
            if self._released and task in self._released:
                self._released.discard(task)
            else:
                entry = record.finished
                if entry is None or entry[0] != FAILED:
                    return False
        record.staged = None
        if type(names) is tuple:
            self._drop_staged(names)
        else:
            self._drop_staged_one(names)
        if task[0] == self._dynamic._id:
            # Only a Future could reach this call again, so its record goes too.
            self._dynamic._remove(task[1])
        return True

    def _drop(self, record):
        """Drop the C node of a task that was let go, at once when every queued call ran, and otherwise after the calls
        queued so far, which may still read the task. C removes the node once every node that reads its outputs was
        removed, and then this Executor forgets it. Called with the lock held."""
        if self._queued == self._ran:
            _capi.vine_graph_executor_drop_node(self._executor, record.node)
        else:
            self._deferred.append((self._queued, Executor._drop_node, record))

    def _drop_node(self, record):
        _capi.vine_graph_executor_drop_node(self._executor, record.node)

    def _forget_removed(self, deadline):
        """Forget tasks whose C nodes were removed, until the deadline. Their descendants were removed before them, so
        no failure names them. Return True when none is left."""
        executor = self._executor
        next_removed = _capi.vine_graph_executor_next_removed
        monotonic = time.monotonic
        tasks = self._tasks.pop
        records = self._records.pop
        released = self._released
        selection_files = self._selection_files
        selection_outputs = self._selection_outputs
        declared_outputs = self._declared_outputs
        cancelled = self._cancelled
        count = 0
        with self._lock:
            while node := next_removed(executor):
                record = tasks(node, None)
                if record is None:
                    continue
                task = record.task
                records(task, None)
                # Tables of tasks that C released, that wrote selections or declared files, or that were cancelled,
                # are small.
                if released:
                    released.discard(task)
                if selection_files:
                    selection_files.pop(task, None)
                if selection_outputs:
                    selection_outputs.pop(task, None)
                if declared_outputs:
                    for path in declared_outputs.pop(task, ()):
                        self._output_files.pop((task[0], task[1], path), None)
                if cancelled:
                    cancelled.discard(task)
                count += 1
                if not count & 63 and monotonic() >= deadline:
                    return False
        return True

    def _drop_staged(self, names):
        """Let go of the staged files and small functions and classes that a task held, and free each one that no task
        holds anymore. Called with the lock held."""
        for name in names:
            self._drop_staged_one(name)

    def _drop_staged_one(self, name):
        holders = self._edata_holders
        count = holders[name] - 1
        if count:
            holders[name] = count
            return
        del holders[name]
        if not isinstance(name, str):
            # A small function or class lived only in this table.
            del self._edata[name]
            return
        identity, path = self._edata_names.pop(name)
        del self._edata[identity]
        # The C file exists once the background thread declared it, which every queued plan does before this runs.
        self._defer_locked(Executor._free_staged, (name, path))

    def _free_staged(self, staged):
        name, path = staged
        file = self._edata_files.pop(name, None)
        if file is not None:
            capi.vine_graph_executor_undeclare_file(self._executor, file)
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    def _clean_up(self, deadline):
        """Do clean-up work until the deadline: queued work whose calls ran, and the removal of dropped C nodes and the
        forgetting of their tasks, in turns. Return True when work is left."""
        self._cleanup_deadline = deadline
        collected = self._collected_graphs
        if collected:
            with self._lock:
                while collected:
                    graph_id = collected.popitem()[0]
                    self._graph_references.pop(graph_id, None)
                    keys = self._graph_tasks.pop(graph_id, None)
                    if keys:
                        self._defer_locked(Executor._graph_gone, (graph_id, keys))
        executor = self._executor
        # The unlinks that releases and removals send leave in one write per Worker when the slice ends, instead of a
        # system call each.
        capi.vine_graph_executor_hold_sends(executor)
        try:
            return self._clean_up_held(deadline)
        finally:
            capi.vine_graph_executor_release_sends(executor)

    def _clean_up_held(self, deadline):
        """The work of _clean_up(), while the Manager holds what it sends to Workers."""
        monotonic = time.monotonic
        deferred = self._deferred
        executor = self._executor
        remove_dropped = _capi.vine_graph_executor_remove_dropped
        lock = self._lock
        while True:
            # The lock is held for a slice of work instead of being taken for each item.
            with lock:
                # Only this thread runs calls, so the count of calls that ran stays as it is during clean-up.
                ran = self._ran
                count = 0
                while deferred and deferred[0][0] <= ran:
                    _, function, argument = deferred[0]
                    if function(self, argument) is False:
                        break
                    deferred.popleft()
                    count += 1
                    if not count & 31 and monotonic() >= deadline:
                        break
                work_left = deferred and deferred[0][0] <= ran
            removals_left = remove_dropped(executor, REMOVAL_BATCH)
            if not self._forget_removed(deadline):
                return True
            if not removals_left and not work_left:
                return False
            if monotonic() >= deadline:
                return True

    def _advance(self):
        """Run queued calls, do clean-up work, advance the run for about a second, and publish what happened. Return
        False when the run stopped. Only the background thread calls this. Queued calls run for at most
        COMMAND_SECONDS before the run advances one step, so tasks already submitted start while a caller keeps
        submitting more, and clean-up runs after them for the rest of COMMAND_SECONDS, and at least CLEANUP_SECONDS."""
        deadline = time.monotonic() + COMMAND_SECONDS
        monotonic = time.monotonic
        lock = self._lock
        pending = False
        while not self._closed:
            # Calls are taken in batches, so the lock is taken once per batch instead of once per call.
            with lock:
                commands = self._commands
                if not commands:
                    # The next call that the caller queues wakes the wait below.
                    self._wake_pending = False
                    break
                self._commands = collections.deque()
            while commands:
                command = commands.popleft()
                command()
                self._ran += 1
                if type(command) is _Call:
                    with lock:
                        self._changed.notify_all()
                if monotonic() >= deadline:
                    break
            if commands or monotonic() >= deadline:
                with lock:
                    # Calls left over run first in the next step, before calls queued meanwhile.
                    commands.extend(self._commands)
                    self._commands = commands
                    pending = bool(commands)
                break
        if self._closed:
            return False
        # Clean-up takes what the calls left of the step, and at least CLEANUP_SECONDS.
        if self._clean_up(max(deadline, monotonic() + CLEANUP_SECONDS)):
            pending = True
        if pending:
            # A wait returns after one step of the Manager, which dispatches ready tasks, when a wake is pending.
            # A wait of zero seconds would only collect returned tasks.
            capi.vine_graph_executor_wake(self._executor)
        status = capi.vine_graph_executor_wait(self._executor, 1)
        error = capi.vine_graph_executor_get_error(self._executor) if status == capi.VINE_GRAPH_FAILED else None
        self._publish_finished()
        capacity = None
        if self._capacity is not None and monotonic() - self._capacity_read >= 1:
            capacity = self._read_capacity()
        fetched = None
        fetches = self._fetches
        while file := capi.vine_graph_executor_next_fetched(self._executor):
            key = fetches.pop(file, None)
            if not key:
                continue
            # A small result is read from the Manager's memory, so its vault entry is never written to disk.
            data = capi.vine_graph_executor_get_local_data(self._executor, file)
            path = data if data is not None else capi.vine_graph_executor_get_local_path(self._executor, file)
            if path is not None:
                if fetched is None:
                    fetched = {}
                fetched[key] = path
        unsendable = []
        while file := capi.vine_graph_executor_next_fetch_failed(self._executor):
            key = fetches.pop(file, None)
            if key:
                unsendable.append(key)
        with lock:
            if fetched:
                self._paths.update(fetched)
            if unsendable:
                self._unsendable.update(unsendable)
            if error:
                self._run_error = error
            if capacity is not None:
                self._capacity = capacity
            self._changed.notify_all()
            progress = self._progress_locked() if self._display is not None else None
        if progress is not None:
            self._display.update(progress)
        return error is None

    def _publish_finished(self):
        """Publish the tasks that completed or failed since the last call, and let go of tasks that will not run
        again."""
        executor = self._executor
        tasks = self._tasks
        finished = []
        failed = []
        next_finished = _capi.vine_graph_executor_next_finished
        get_node_state = _capi.vine_graph_executor_get_node_state
        while node := next_finished(executor):
            record = tasks[node]
            if get_node_state(executor, node) == capi.VINE_GRAPH_NODE_FAILED:
                source = tasks[capi.vine_graph_executor_get_failure_source(executor, node)]
                # Only the source keeps the message, which its dependents read from it.
                if source is record:
                    entry = (FAILED, source.task, capi.vine_graph_executor_get_node_error(executor, node),
                             _FAILURE_REASONS.get(capi.vine_graph_executor_get_node_failure(executor, node)))
                else:
                    entry = (FAILED, source.task, None, None)
                finished.append((record, entry))
                failed.append(record)
            else:
                finished.append((record, _COMPLETED_ENTRY))
        released = []
        while node := capi.vine_graph_executor_next_released(executor):
            released.append(tasks[node])
        while node := capi.vine_graph_executor_next_kept(executor):
            record = tasks[node]
            task = record.task
            # A task of a live user Graph stays kept while its Graph lives, as one that a late reader read again after
            # its release does (_reread), whose own pin already keeps it. Calls of the Executor have no Graph that could
            # ask again, so C lets them go at once.
            if task[0] != self._dynamic._id and task[0] in self._graph_references and task not in self._reread:
                self._kept.add(task)
            elif capi.vine_graph_executor_unkeep_node(executor, node) == 1:
                released.append(record)
        if not finished and not released:
            return
        with self._lock:
            self._set_finished(finished)
            for record in released:
                if self._let_go(record, True):
                    self._drop(record)
            for record in failed:
                if self._let_go(record):
                    self._drop(record)
            self._changed.notify_all()

    # Published state, shared by both threads.

    def _set_finished(self, finished):
        """Record tasks that completed or failed, given as (_Task, entry) pairs, and hand them to the callers of
        as_completed() that watch them. Called with the lock held."""
        counts = self._counts
        for record, entry in finished:
            previous = record.finished
            if previous is not None:
                counts[previous[0]] -= 1
            record.finished = entry
            counts[entry[0]] += 1
        if self._callbacks:
            callbacks = self._callbacks
            for record, _ in finished:
                due = callbacks.pop(record.task, None)
                if due is not None:
                    self._callbacks_due.extend(due)
        if self._watchers:
            for record, _ in finished:
                task = record.task
                for watcher in self._watchers:
                    if task in watcher.tasks:
                        watcher.finished.append(task)

    def _finished_entry(self, task):
        with self._lock:
            record = self._records.get(task)
            return None if record is None else record.finished

    def _finished_state(self, task):
        if task is None:
            return COMPLETED
        with self._lock:
            self._check_open()
            record = self._records.get(task)
            if record is None:
                raise RuntimeError("the task of this Future was never submitted")
            entry = record.finished
            return None if entry is None else entry[0]

    # Waiting and results.

    def wait(self, futures=None, timeout=None):
        """Wait until the given futures are done, or until every submitted task finished. A timeout in seconds that
        expires raises TimeoutError."""
        self._check_open()
        if futures is not None:
            self._wait_for(futures, timeout)
            return
        self._wait_until(lambda: self._progress_locked().waiting == 0, timeout, "submitted tasks are not done")

    def as_completed(self, futures, timeout=None):
        """Yield the given futures as their tasks finish. More tasks may be submitted between iterations. A timeout
        in seconds that expires raises TimeoutError."""
        deadline = None if timeout is None else time.monotonic() + timeout
        pending = collections.defaultdict(list)  # task -> its futures that were not yielded yet
        for future in futures:
            if future._executor is not self:
                raise ValueError("as_completed() takes Futures of this Executor")
            pending[future._task].append(future)
        watcher = _Watcher(pending)
        try:
            with self._lock:
                # Registered first, so a task that finishes during the scan below is not missed.
                self._watchers.add(watcher)
            ready = [task for task, waiting in pending.items() if waiting[0].done()]
            while True:
                for task in ready:
                    yield from pending.pop(task, ())
                if not pending:
                    return
                remaining = None if deadline is None else deadline - time.monotonic()
                self._wait_until(lambda: watcher.finished, remaining,
                                 lambda: f"{sum(map(len, pending.values()))} futures are not done")
                with self._lock:
                    ready, watcher.finished = watcher.finished, []
        finally:
            with self._lock:
                self._watchers.discard(watcher)

    def capacity(self):
        """Return the Workers connected and their cores, which the background thread reads about once a second. A local
        Executor runs one task at a time."""
        self._check_open()
        if self._local:
            return Capacity(1, 1)
        with self._lock:
            capacity = self._capacity
        if capacity is None:
            # The first call reads it, and the background thread keeps it current from then on.
            capacity = self._call(self._read_capacity)
            with self._lock:
                self._capacity = capacity
        return capacity

    def _read_capacity(self):
        """Read the Workers and cores of the Manager in the background thread, at O(Workers)."""
        capacity = capi.vine_graph_executor_get_capacity(self._executor)
        self._capacity_read = time.monotonic()
        return Capacity(capacity.workers, capacity.cores)

    def _add_done_callback(self, future, fn):
        task = future._task
        with self._lock:
            if not self._closed and task is not None:
                record = self._records.get(task)
                if record is not None and record.finished is None:
                    self._callbacks.setdefault(task, []).append((future, fn))
                    if self._callback_thread is None:
                        self._callback_thread = threading.Thread(target=_call_back, args=(weakref.ref(self),),
                                                                 name="vine-graph-callbacks", daemon=True)
                        self._callback_thread.start()
                    return
        _run_callback(future, fn)

    def run(self, graph, targets=None):
        """Run the targets of a Graph, and every task they read, and return {target: result}. Targets are Nodes,
        NodeFiles, or Files, and default to the tasks no other task reads. A failure raises the TaskError of a task
        that failed on its own, in preference to the tasks that could not run because of it."""
        targets = graph.sinks() if targets is None else list(targets)
        self._check_open()
        # One call submits every task that the targets need, so a task read by another target's task stays, and the
        # Future of each target holds its data from the call that submits its task. Anything else goes to submit(),
        # which explains why it is not a target.
        batch = [index for index, target in enumerate(targets) if isinstance(target, (Node, NodeFile, File))]
        futures = [None] * len(targets)
        for index, future in zip(batch, self._submit_targets([targets[index] for index in batch])):
            futures[index] = future
        for index, target in enumerate(targets):
            if futures[index] is None:
                futures[index] = self.submit(target)
        self.wait(futures)
        errors = [error for error in (future.exception() for future in futures) if error is not None]
        if errors:
            raise next((error for error in errors if error.task == error.source), errors[0])
        return {target: future.result() for target, future in zip(targets, futures)}

    def progress(self):
        """Return how many of the submitted tasks completed, failed, or are still waiting or running."""
        with self._lock:
            self._check_open()
            return self._progress_locked()

    def _progress_locked(self):
        return Progress(self._submitted, self._counts[COMPLETED], self._counts[FAILED])

    def stats(self):
        """Return the makespan in seconds, the number of submitted tasks, and the number of recovery tasks."""
        self._check_open()
        if self._local:
            return {"makespan": self._local_seconds, "tasks": self._submitted, "recovery_tasks": 0}
        return self._call(lambda: {
            "makespan": capi.vine_graph_executor_get_makespan_us(self._executor) / 1e6,
            "tasks": self._submitted,
            "recovery_tasks": capi.vine_graph_executor_get_completed_recovery_tasks(self._executor),
        })

    def _wait_for(self, futures, timeout):
        # A finished task stays finished, so each future is checked until it is done and never again.
        futures = list(futures)
        position = 0

        def ready():
            nonlocal position
            while position < len(futures) and futures[position].done():
                position += 1
            return position == len(futures)

        self._wait_until(ready, timeout, "the task is not done")

    def _wait_until(self, ready, timeout, unfinished):
        """Wait until ready() holds, checking it whenever the background thread publishes changes. An interrupt or a
        timeout only ends the wait."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            while not ready():
                self._check_open()
                if self._run_error is not None:
                    raise RuntimeError(f"vine_graph execution stopped: {self._run_error}")
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError(unfinished() if callable(unfinished) else unfinished)
                self._changed.wait(0.5 if remaining is None else min(remaining, 0.5))

    def _node(self, task):
        """Return the Node of a task, or None when nothing holds the task anymore."""
        graph = self._graphs.get(task[0])
        return Node(graph, task[1]) if graph is not None and task[1] in graph._tasks else None

    def _label(self, task):
        """Return how messages name a task, as repr() of its Node did when it was submitted."""
        return f"Node({task[1]!r}, {self._records[task].label})"

    def _error(self, task):
        if self._finished_state(task) != FAILED:
            return None
        _, source, message, reason = self._finished_entry(task)
        if source != task:
            _, _, message, reason = self._finished_entry(source)
        message, exception = library.load_failure(message or "")
        task_node, source_node = self._node(task), self._node(source)
        task_label, source_label = self._label(task), self._label(source)
        # A function failure ends with its traceback. Lines the library writes before it are not about the function,
        # and neither are its timestamped log lines in a message without a traceback.
        start = message.find("Traceback (most recent call last):")
        if start >= 0:
            message = f"\n{message[start:]}"
            if exception is not None:
                exception.add_note(f"raised by task {source_label}:{message.rstrip()}")
        else:
            message = f" {LIBRARY_LOG_LINE.sub('', message)}"
        if source == task:
            return TaskError(f"task {task_label} failed:{message}", task_node, source_node, reason, exception)
        return TaskError(f"task {task_label} did not run because task {source_label} failed:{message}", task_node,
                         source_node, reason, exception)

    def _cancel(self, task):
        self._check_open()
        if task is None or self._local or self._finished_state(task) is not None:
            return False

        def cancel():
            if capi.vine_graph_executor_cancel_node(self._executor, self._records[task].node) != 0:
                return False
            with self._lock:
                self._cancelled.add(task)
            # The task and its dependents are done when cancel() returns.
            self._publish_finished()
            return True
        return self._call(cancel)

    def _result(self, future, timeout):
        held = future._held
        if type(held) is _Task:
            # A completed task whose result already reached the Manager, the common case once a run finished, is read
            # at once. The Future of a task result holds the task's _Task.
            task = held.task
            with self._lock:
                path = None
                if not self._closed and held.finished is _COMPLETED_ENTRY:
                    path = self._paths.get(task)
            if path is not None:
                return self._load(task, _load_fetched, path)
        # One deadline covers waiting for the task and for its result to reach the Manager.
        deadline = None if timeout is None else time.monotonic() + timeout
        self._wait_for([future], timeout)
        target = future.target
        if isinstance(target, File):
            return target.path
        task = future._task
        error = self._error(task)
        if error is not None:
            raise error
        if isinstance(target, Node):
            if self._local:
                return self._load(task, library.load_result, self._local_result_path(task))
            return self._load(task, _load_fetched, self._fetched_path(task, self._data_key(target), deadline))
        identity = library.file_identity(target._as_ref())
        if self._local:
            path = self._local_paths[identity]
        else:
            # Link or copy the file, or each file of a directory, out of the Manager's vault, which the Executor
            # removes when it closes.
            path = os.path.join(self._run_dir, self._file_task_path(identity))
            if not os.path.exists(path):
                fetched = self._fetched_path(task, self._data_key(target), deadline)
                if isinstance(fetched, bytes):
                    with open(path, "wb") as stream:
                        stream.write(fetched)
                elif os.path.isdir(fetched):
                    shutil.copytree(fetched, path, symlinks=True, copy_function=_link_or_copy)
                else:
                    _link_or_copy(fetched, path)
        path = os.path.abspath(path)
        self._keep_paths.add(path)
        return path

    def _load(self, task, load, fetched):
        """Return the result of a completed task loaded by load(fetched). A result that cannot be loaded in this process,
        such as an exception whose class takes other arguments than it passes to Exception.__init__(), or an object of
        a module this process cannot import, raises a TaskError that names the task instead of the error of pickle."""
        try:
            return load(fetched)
        except Exception as error:
            node = self._node(task)
            raise TaskError(f"task {self._label(task)} completed, but its result cannot be loaded: "
                            f"{type(error).__name__}: {error}", node, node, "result") from error

    def _fetched_path(self, task, key, deadline=None):
        """Wait until a pinned output of a completed task is readable at the Manager, and return its path, or its
        bytes when the Manager holds them in memory. The background thread publishes either when the fetch completes.
        A deadline from time.monotonic() that passes first raises TimeoutError."""
        timeout = None if deadline is None else max(deadline - time.monotonic(), 0)
        self._wait_until(lambda: key in self._paths or key in self._unsendable or self._finished_state(task) == FAILED,
                         timeout, "the result is not available")
        with self._lock:
            path = self._paths.get(key)
            unsendable = key in self._unsendable
        if path is not None:
            return path
        if unsendable:
            path = _split_data_key(key)[1]
            output = "result" if path is None else f"output file {path!r}"
            node = self._node(task)
            raise TaskError(f"task {self._label(task)} completed, but its Worker cannot send its {output}, which is "
                            f"or holds a file that the Worker cannot read, or is a FIFO, socket, or device", node, node,
                            "result")
        raise self._error(task)

    # Local execution.

    def _run_local(self, task):
        """Run one task in this process, for debugging without Workers. Its parents already finished."""
        graph_id, key = task
        graph = self._graphs[graph_id]
        # The function and its arguments are serialized as a Worker run serializes them, so one that cannot be
        # serialized fails the submission alike, and the task gets copies of its arguments, as a task on a Worker does.
        # The function itself runs as it is, with the state of this process, which is what debugging it locally needs.
        func, args, kwargs = graph._tasks[key]
        self._pickler.dumps(func)
        serialized = self._pickler.dumps((args, kwargs))
        for parent in graph._parents.get(key, ()):
            entry = self._records[_resolve(graph_id, parent)].finished
            if entry[0] == FAILED:
                self._finish_local((task, (FAILED, entry[1], None, None)))
                return
        task_dir = os.path.join(self._run_dir, ".tasks", f"task-{self._records[task].number}")
        shutil.rmtree(task_dir, ignore_errors=True)
        os.makedirs(task_dir)
        outputs = graph._outputs.get(key, {})
        for task_path in outputs:
            os.makedirs(os.path.dirname(os.path.join(task_dir, task_path)), exist_ok=True)
        # The working directory belongs to the process, so local tasks of every Executor and thread run one at a
        # time. Two Executors in two threads otherwise changed each other's directory, and a task wrote its files
        # into another task's sandbox or left the caller in one.
        with _LOCAL_TASKS:
            previous_cwd = os.getcwd()
            started = time.time()
            os.chdir(task_dir)
            try:
                args, kwargs = cloudpickle.loads(serialized)
                output = library.run_task(
                    func, args, kwargs,
                    lambda parent: library.load_result(self._local_result_path(_resolve(graph_id, parent))),
                    lambda identity: self._local_file_path(_resolve_file(graph_id, identity)),
                    lambda name: cloudpickle.loads(self._hoisted[name]), graph._selections.get(key, ()))
                for task_path in outputs:
                    local_path = os.path.abspath(os.path.join(task_dir, task_path))
                    if not os.path.exists(local_path):
                        raise FileNotFoundError(f"task {key!r} did not produce its output file {task_path!r}")
                    self._local_paths[("output", graph_id, key, task_path)] = local_path
                library.save_result(self._local_result_path(task), output)
                self._finish_local((task, _COMPLETED_ENTRY))
            except KeyboardInterrupt:
                # An interrupt stops the submission, which can be repeated.
                raise
            except BaseException as error:
                # As on a Worker, anything else the function raises, including an exit, fails the task.
                report = library.failure_report(error, __file__, library.__file__)
                self._finish_local((task, (FAILED, task, report, "function")))
            finally:
                os.chdir(previous_cwd)
                self._local_seconds += time.time() - started
                if self._display is not None:
                    self._display.update(self._progress_locked())

    def _finish_local(self, entry):
        """Record a local task that completed or failed, given as (task, entry), and wake the waits of other threads."""
        task, value = entry
        with self._lock:
            self._set_finished(((self._records[task], value),))
            self._changed.notify_all()

    def _local_result_path(self, task):
        return os.path.join(self._run_dir, f"result-{self._records[task].number}")

    def _local_file_path(self, identity):
        if identity[0] == "input":
            return identity[1]
        return self._local_paths[identity]

    # Names and local paths.

    def _selection_task_path(self, task):
        # A task mounts at most one selection file of each producer.
        return f"vine-graph-selection-{self._records[task].number}"

    def _file_task_path(self, identity):
        """Return the sandbox name of a file a task reads: a number of its own followed by the end of its name, which
        holds its extension, as much of it as fits in the longest file name a file system takes."""
        if identity not in self._file_numbers:
            self._last_number += 1
            self._file_numbers[identity] = self._last_number
        prefix = f"vine-graph-file-{self._file_numbers[identity]}-"
        name = os.path.basename(identity[-1])
        excess = len(os.fsencode(prefix + name)) - _NAME_MAX
        start = 0
        while excess > 0:
            excess -= len(os.fsencode(name[start]))
            start += 1
        return prefix + name[start:]

    def _link_input(self, source):
        """Give this Executor its own native cache identity for a File without copying its bytes.

        A closed Executor can still receive late cache-update messages. A path of its own prevents those messages
        from satisfying the next Executor's declaration after the old replica was unlinked.
        """
        directory = Path(self._run_dir) / ".inputs"
        directory.mkdir(exist_ok=True)
        path = directory / str(len(self._input_files) + 1)
        path.symlink_to(source)
        return str(path)


def _split_data_key(key):
    """Return the task and the sandbox path of a data key, with None for a task result. A task starts with its graph
    id, a string, and the key of a file with its task."""
    return (key, None) if type(key[0]) is str else key


def _load_fetched(fetched):
    """Load a task result fetched to the Manager: its bytes, which the Manager held in memory, or its vault path."""
    if type(fetched) is bytes:
        return cloudpickle.loads(fetched)["Result"]
    return library.load_result(fetched)


def _manager_setting(setting, value):
    """Return the number that TaskVine Manager tuning setting takes for value, or raise ValueError for a name that is
    neither a vine_graph setting nor Manager tuning, and for a value that is not a finite number, which C converts to
    integers and byte counts."""
    if not cvine.vine_tune_known(setting):
        raise ValueError(f"unknown setting {setting!r}: it is neither a vine_graph setting nor TaskVine Manager tuning")
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = math.nan
    if not math.isfinite(number):
        raise ValueError(f"invalid value {value!r} for {setting}: TaskVine Manager tuning takes a finite number")
    return number


def _check_input_file(path):
    """Raise an error that says why a File cannot be read by tasks: it does not exist or is not a regular file."""
    if os.path.isfile(path):
        return
    if os.path.isdir(path):
        raise IsADirectoryError(errno.EISDIR, "a File must be a regular file, not a directory", path)
    raise FileNotFoundError(errno.ENOENT, "a File must name an existing regular file", path)


def _link_or_copy(source, destination):
    """Hard-link source to destination, or copy it where links fail, such as across file systems."""
    try:
        os.link(source, destination)
    except OSError:
        shutil.copyfile(source, destination)


def _serialize_hoisted(hoist):
    """Serialize each hoisted object on its own and return {name: bytes}, so an unpicklable object or a bad name fails
    the Executor before it starts."""
    if hoist is None:
        return {}
    if not isinstance(hoist, dict):
        raise TypeError("hoist takes a dictionary from names to objects")
    hoisted = {}
    for name, value in hoist.items():
        if not isinstance(name, str) or not name:
            raise TypeError("a hoisted object is named by a non-empty string")
        hoisted[name] = cloudpickle.dumps(value)
    return hoisted


def _selections_read_from(graph, key):
    """Return {selection key: (steps, sandbox path)} of the distinct selections of a task's result that tasks of its
    Graph read at exactly one place. The task writes each to a file of its own, which those tasks mount instead of the
    whole result. A task added after the task was submitted, or one of another Graph, mounts such a file when it reads
    the same selection, and the whole result otherwise."""
    selections = {}
    read_once = graph._selections
    # Another thread may add readers of the task meanwhile, which the copy, one built-in call, leaves out.
    for child in tuple(graph._children.get(key, ())):
        entry = read_once.get(child)
        if entry is None:
            continue
        entry = entry.get((None, key))
        if entry is not None and entry[0] is not None and entry[0] not in selections:
            selections[entry[0]] = (entry[1], f"vine-graph-selection-output-{len(selections) + 1}")
    return selections


def _resolve(graph_id, reference):
    """Return the task (graph id, key) of a reference stored in the graph with graph_id, as a (graph id, key) pair or a
    _Ref or _FileRef. A Graph refers to its own tasks with graph id None."""
    reference_graph_id, key = reference if isinstance(reference, tuple) else (reference.graph_id, reference.key)
    return (graph_id if reference_graph_id is None else reference_graph_id, key)


def _resolve_file(graph_id, identity):
    """Return a library.file_identity() whose output file reference is resolved like _resolve()."""
    if identity[0] != "output":
        return identity
    return ("output", *_resolve(graph_id, identity[1:3]), identity[3])


_MAIN_THREAD = threading.main_thread().ident
_INTERRUPT = (signal.SIGINT,)


# A call that changes no signal mask, which _block_interrupts() returns in threads other than the main thread.
_KEEP_INTERRUPTS = functools.partial(_signal.pthread_sigmask, _signal.SIG_BLOCK, ())


def _block_interrupts():
    """Hold back SIGINT while a short change of Executor state completes, so an interrupt never leaves a task queued but
    unrecorded, and return the call that restores the signal mask, which delivers a held interrupt. Other threads keep
    their mask, since only the main thread receives KeyboardInterrupt. The built-in function takes and returns signal
    numbers: signal.pthread_sigmask() converts the returned set to enum members in Python, which cost about 15% of the
    caller's time per submission.

    Python raises an interrupt when a Python function starts and after a built-in function returns. An interrupt that
    is already pending is raised right after SIGINT is blocked, before the caller has the call that restores the mask,
    so the mask is restored here. The returned call is a built-in partial, which a caller makes in a finally block
    without starting a Python function that an interrupt could stop first. It is made before SIGINT is blocked, so
    nothing that could raise runs between blocking and returning: creating it afterwards let an interrupt raised when
    `functools.partial()` returned, which another thread had received, leave the mask blocked (2026-10-18, the second
    interrupts scenario of one Executor received none). Otherwise the main thread would block SIGINT for good, and so
    would every thread it starts later."""
    if threading.get_ident() != _MAIN_THREAD:
        return _KEEP_INTERRUPTS
    restore = functools.partial(_signal.pthread_sigmask, _signal.SIG_SETMASK, _signal.pthread_sigmask(_signal.SIG_BLOCK, ()))
    try:
        _signal.pthread_sigmask(_signal.SIG_BLOCK, _INTERRUPT)
    except BaseException:
        restore()
        raise
    return restore


def _drive(executor_reference):
    """Advance the run of an Executor in the background until it closes. The thread holds the Executor only while it
    advances it, so an Executor nothing else refers to can still be collected and closed.

    The thread blocks SIGINT, and so do the threads that the Manager starts from it. The kernel delivers a signal that
    a thread blocks to another thread, and Python raises the interrupt in the main thread whichever thread received it,
    so a thread that left SIGINT unblocked would let it through while the main thread holds it back
    (_block_interrupts())."""
    _signal.pthread_sigmask(_signal.SIG_BLOCK, _INTERRUPT)
    while True:
        executor = executor_reference()
        if executor is None or executor._closed:
            return
        try:
            if not executor._advance():
                return
        except Exception as error:
            with executor._lock:
                executor._run_error = f"{error}"
                executor._changed.notify_all()
            return
        del executor


def _call_back(executor_reference):
    """Run the done-callbacks of an Executor's Futures as their tasks finish, until it closes and the last of them ran.
    Like the background thread, it holds the Executor only while it takes callbacks, and blocks SIGINT (_drive())."""
    _signal.pthread_sigmask(_signal.SIG_BLOCK, _INTERRUPT)
    while True:
        executor = executor_reference()
        if executor is None:
            return
        with executor._lock:
            if not executor._callbacks_due:
                if executor._closed:
                    return
                executor._changed.wait(0.5)
            due = executor._callbacks_due
            executor._callbacks_due = collections.deque()
        del executor
        for future, fn in due:
            _run_callback(future, fn)
        del due


def _run_callback(future, fn):
    try:
        fn(future)
    except Exception:
        logging.getLogger(__name__).exception("a done-callback of %r raised", future)


def _in_notebook():
    """Return True inside a Jupyter kernel."""
    try:
        from IPython import get_ipython
    except ImportError:
        return False
    return type(get_ipython()).__name__ == "ZMQInteractiveShell"


class _ProgressDisplay:
    """A progress bar in the notebook cell that created an Executor, updated as tasks finish."""

    def __init__(self, progress):
        from IPython.display import display

        self._handle = display(progress, display_id=True)
        self._shown = progress
        self._shown_at = time.monotonic()

    def update(self, progress):
        now = time.monotonic()
        if progress != self._shown and (now - self._shown_at > 0.5 or progress.waiting == 0):
            self._handle.update(progress)
            self._shown, self._shown_at = progress, now


def _remove_files(root):
    """Remove the files under a run-info template directory, keeping its directories."""
    for directory, _, files in os.walk(root):
        for name in files:
            try:
                os.remove(os.path.join(directory, name))
            except FileNotFoundError:
                pass
