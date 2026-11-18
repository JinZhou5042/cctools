# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Run tasks on TaskVine Workers, or in this process. An Executor runs Graphs (run()) and starts single tasks at once
(submit()). It works through one runner: _Workers owns a TaskVine Manager, the C graph executor, and the library that
runs Python tasks on Workers, and _Local runs tasks in this process. _Run is one call of Executor.run(): it submits
tasks, applies what finished, calls rules one at a time in completion order, and ends when nothing runs and no rule is
due.

A background thread owns the C executor: it is the only thread that calls into C after the runner starts, apart from
vine_graph_executor_wake(), which ends the thread's wait in C when a call is queued for it. The thread that submits a
task stages it and queues the C calls for the background thread. The background thread runs queued calls, advances the
run, and publishes what finished and which fetched results became readable. What was published is applied to the Tasks
under the Executor's lock, by the thread of a run between rules, so a rule sees task states that do not change while it
runs, or by a thread that asks for a submitted task."""

import atexit
import collections
import contextlib
import ctypes
import errno
import functools
import importlib
import io
import math
import operator
import os
import re
import shlex
import shutil
import signal
import subprocess
import _signal
import _thread
import sys
import tempfile
import threading
import time
import traceback
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
from .graph import (  # noqa: E402
    _LEAF_TYPE_SET, COMMAND_RESULT_LIMIT, CONTENT_LIMIT, Command, File, Graph, Shared, TaskError, _Calls, _FileRef,
    _Ref, _label, _name_task, _readers_of, _NEW, _RUNNING, _COMPLETED, _FAILED, _CANCELLED,
)
from . import graph as _graph_module, install, library  # noqa: E402

# Manager tuning that suits graph execution. The params of an Executor override it.
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

# Settings of the C graph executor and their defaults. The progress bar of a run is drawn by _Run, so the C executor
# draws none.
EXECUTOR_TUNING = {
    # largest-input-first, depth-first, or fifo.
    "task-priority-mode": "largest-input-first",
    # Failed attempts allowed per task for infrastructure failures. A function that raises fails its task at once.
    "max-retries": 5,
    "progress-bar": 0,
    # 1 releases a result once every task that reads it is durable, which the results that can still be asked for
    # without being computed again are pinned against (Executor._start()). 0 keeps every result.
    "release-outputs": 1,
    # auto anchors every output in the vault while it keeps up, and only the outputs on the boundaries of bands of
    # generations while large outputs complete faster than it anchors them. 0 anchors every output, k fixes the band.
    "checkpoint-cut": "auto",
    # Seconds a large output's checkpoint waits, once, while tasks that read it run, because the Worker that holds it
    # sends the checkpoint through the link its readers fetch it from. 0 starts every checkpoint at once.
    "checkpoint-yield": 10,
}

# Graph executor settings: those above, and shared-directory, which has no default. It names a directory that the
# Manager and every Worker mount at one path, such as a shared file system, where the vault keeps large checkpoints
# that the Workers holding them write, besides those it receives itself (Manager tuning vault-shared-min-bytes).
EXECUTOR_SETTINGS = frozenset(EXECUTOR_TUNING) | {"shared-directory"}

# The setting of the Executor itself: a budget in bytes of function and argument objects of the first submission that
# the library on each Worker loads when it starts, so tasks do not fetch their files. Zero, the default, caches none.
EDATA_CACHE = "edata-cache"

# Why a result is pinned, as bits of _Record.keep: no task that was submitted reads it yet, its rules are due, or its
# task wrote a file that no task reads. While any holds, the release rule leaves the result alone.
_SINK = 1
_RULE = 2
_FINAL = 4

# The entry of every task that completed, which the records share. A failure is (_FAILED, record of the source,
# message, reason, exit code), and only a source holds its message, reason, and exit code.
_COMPLETED_ENTRY = (_COMPLETED, None, None, None, None)
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

# Seconds per step that the background thread spends at least on clean-up after the queued calls: freeing staged files
# and removing the C nodes of tasks that were let go.
CLEANUP_SECONDS = 0.01

# A line that the library runtime logs to a function call's standard output (stdout_timed_message()): its progress,
# which a failure message leaves out, or an error, of which it keeps the text after the prefix.
LIBRARY_LOG_LINE = re.compile(r"^\d\d/\d\d/\d\d \d\d:\d\d:\d\d\.\d+ (?:TASK \d+ (?=error:)|.*\n?)", re.MULTILINE)

# Characters of standard error that the TaskError of a failed command keeps, from its end.
COMMAND_ERROR_TAIL = 4096

# Dropped C nodes removed per call into C, between which clean-up checks its time.
REMOVAL_BATCH = 128

# Bytes up to which a serialized function or argument travels inside the manifest of its task. Larger objects become
# staged files, which every task that passes the same object shares. A file of its own takes at least one 4 KiB page
# on disk, so a smaller object is never cheaper as a file. References to other tasks and small values, which are usually
# different in every task, then add no file at all, and a small object that tasks share is copied at most this many
# bytes per task.
INLINE_LIMIT = 4096

# Small objects whose serialization tasks share by identity, like staged files: functions and classes, which many tasks
# pass and cloudpickle serializes by value when they are defined in __main__. Every other small object is serialized
# for each task, so a mutable argument changed between runs reaches later tasks.
_SHARED_INLINE_TYPES = (types.FunctionType, type)

# Arguments that stand for data the task reads: references to task results and files, Files, and shared objects. The
# manifest holds them inside its own serialization instead of serializing each on its own. A reference to a whole
# result, the most common argument, is held as its task (None, key), a tuple that the pickler writes without calling
# cloudpickle's reducer.
_REFERENCE_TYPES = (_Ref, _FileRef, File, Shared)

# Immutable values that the manifest holds as they are, inside its own serialization, instead of each being serialized
# on its own: numbers, None, and strings (bytes are serialized, so that the library tells an object it loads from a value
# it takes as it is). Being immutable, they need no identity and no copy per task.
_LITERAL_TYPES = frozenset((int, float, bool, complex, type(None), str))

# Bytes of the longest file name, without its directory, that Linux file systems take (NAME_MAX).
_NAME_MAX = 255

# The Python code that the shell of a command task runs to read str() of a Python task's result, which the result file
# holds serialized. Workers run it with the python3 of their environment, and local runs with this interpreter.
_STR_RESULT = 'import sys, cloudpickle; sys.stdout.write(str(cloudpickle.load(open(sys.argv[1], "rb"))["Result"]))'

# The C library, whose standard output the progress bar writes to.
_libc = ctypes.CDLL(None)

# Runners that hold a Manager. A new Executor on the port of an open one, as rerunning a notebook cell makes, closes
# the open one first, and every runner still open is closed before the interpreter tears its modules down.
_open_executors = weakref.WeakSet()


@atexit.register
def _close_open_executors():
    for executor in list(_open_executors):
        try:
            executor.close()
        except Exception:
            pass


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

    A Pickler's memo table keeps the capacity of its largest dump, and clearing it touches every slot, so a Pickler
    whose dump wrote more than LARGE_DUMP bytes is replaced by a new one. The bound is in bytes because the memo's size
    cannot be read cheaply, and a memoized object writes at least about 4 bytes, so a dump of up to 4 KB leaves at most
    about 1,000 entries, whose clearing costs later dumps about as much as a new Pickler."""

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

# Held while a local run executes a task, which changes the working directory of the process, and while a relative path
# is resolved.


_LOCAL_TASKS = _Lock()
_graph_module._working_directory_lock = _LOCAL_TASKS


class _Record:
    """What a runner records about one submission of a task: one object for each submission instead of an entry in
    each of a dozen tables, which the C node id leads to. A task that a stopped run returns to not-started gets a new
    record when it runs again, and its old record is let go.

    The thread that submits the task sets the fields, and the background thread sets node, result, the C file ids, pin,
    and kept. finished is published under the runner's lock."""

    __slots__ = ("graph", "key", "number", "func", "command", "outputs", "staged", "node", "result", "files",
                 "selections", "selection_files", "finished", "abandoned", "stopping", "held", "keep", "pin", "kept")

    def __init__(self, graph, key):
        self.graph = graph  # weak reference to the task's Graph, which leads to the Task by key
        self.key = key  # the task's key in its Graph
        self.number = 0  # the number that names the task's files in sandboxes, and its data in the runner's tables
        self.func = None  # the task's function or Command, which names it in messages (label)
        self.command = False  # the task runs a Command, whose result is the text of its standard output
        self.outputs = ()  # sandbox paths of the files the task declared when it was submitted
        # Sandbox names and small function and class ids that the task holds, as a tuple, or the one name or id when it
        # holds only one. None before the task is queued and once it was let go.
        self.staged = None
        self.node = 0  # C node id, set by the background thread
        self.result = 0  # C file id of the task result
        self.files = None  # {sandbox path: C file id} of the files the task declared, or None
        # {selection key: (steps, sandbox path)} of the selections of its result that the task writes to files of their
        # own, or None, and {selection key: C file id} of those files
        self.selections = None
        self.selection_files = None
        self.finished = None  # the entry of the task once it finished, published by the background thread
        self.abandoned = False  # the runner let the record go, so nothing about it counts anymore
        self.stopping = False  # a stopped run cancelled the task, which then returns to not-started
        self.held = False  # the result is pinned and fetched for reading
        self.keep = 0  # why the result is pinned against release (_SINK, _RULE, _FINAL), set by the submitting side
        self.pin = 0  # pins that the record holds on its result in C
        self.kept = False  # C keeps the node, which ran again after its outputs were released

    @property
    def label(self):
        """How messages name the task. It is built when needed, since only messages use it."""
        return _name_task(self.key, self.func)


def _task_of(record):
    """Return the Task of a record, or None when the task or its Graph was collected."""
    graph = record.graph()
    return None if graph is None else graph._tasks.get(record.key)


def _references(task):
    """Return {(graph id, key): Task} of the tasks a task reads, by the references that its arguments hold."""
    graph = task._graph
    return {(graph._graph_id(parent), parent.key): parent for parent in task._parents}


class _Submission(list):
    """A queued call that submits a task in the background thread, which is the list of what it declares and mounts
    (Executor._submit_node()). A queued task then costs the garbage collector one object instead of a partial, the
    tuple of its arguments, and a list."""

    __slots__ = ("executor",)

    def __call__(self):
        self.executor._submit_node(self)


# The C file id of a task's result, from its _Record, for map() over the tasks whose results a task mounts.
_RESULT_FILE = operator.attrgetter("result")


class _Runner:
    """What both runners share: the records of submitted tasks and how sandboxes name files."""

    def _new_record(self, task):
        record = _Record(task._graph._ref, task.key)
        self._last_number += 1
        record.number = self._last_number
        record.func = task._func
        record.command = isinstance(task._func, Command)
        record.outputs = tuple(task._outputs) if task._outputs else ()
        return record

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

    def _command_line(self, task, python):
        """Return the shell command line of a Command task and what it mounts: (sandbox path of each result it reads,
        record), and (file identity, sandbox path, record of the task that writes it or None) of each file.
        Placeholders take the shell-quoted path of a file in
        the sandbox, the text of a task result, which the shell reads into a variable first, and str() of any other
        value. The command's standard output goes to the result file, which keeps at most COMMAND_RESULT_LIMIT bytes,
        and a command that exits with status zero fails when it did not write a file that other tasks read."""
        command = task._func
        prelude = []
        values = {}
        results = []
        files = []
        parents = _references(task) if task._parents else None
        for name in command._fields:
            value = task._kwargs[name]
            kind = type(value)
            if kind is _Ref:
                record = parents[(value.graph_id, value.key)]._record
                path = f"vine-graph-result-{record.number}"
                variable = f"vine_graph_value_{len(prelude) + 1}"
                if record.command:
                    read = f"cat {path}"
                else:
                    read = f"{python} -c {shlex.quote(_STR_RESULT)} {path}"
                prelude.append(f"{variable}=$({read}) || {{ echo {shlex.quote(f'vine_graph: could not read the result of task {record.label}')} >&2; exit 1; }}")
                results.append((path, record))
                values[name] = f'"${variable}"'
            elif kind is File or kind is _FileRef:
                identity = library.file_identity(value)
                path = self._file_task_path(identity)
                source = parents[(value.graph_id, value.key)]._record if kind is _FileRef else None
                files.append((identity, path, source))
                values[name] = shlex.quote(path)
            else:
                values[name] = shlex.quote(str(value))
        checks = "".join(f'if [ ! -e {shlex.quote(path)} ]; then echo {shlex.quote(f"vine_graph: the command did not write its output {path}")} >&2; vine_graph_status=1; fi\n'
                         for path in (task._outputs or ()))
        line = "\n".join(prelude + [
            "(",
            command.template.format_map(values),
            f") > {library.RESULT}",
            "vine_graph_status=$?",
            f"truncate -s '<{COMMAND_RESULT_LIMIT}' {library.RESULT} 2>/dev/null",
            f"if [ $vine_graph_status -eq 0 ]; then\n{checks}:\nfi",
            "exit $vine_graph_status",
        ])
        return line, results, files


class _Workers(_Runner):
    """The runner of an Executor on TaskVine Workers that connect to port, or to the Manager named name through the
    catalog.

    It owns a Manager, the C graph executor, and the library that runs Python tasks on each Worker, which prepares
    itself once when it starts: it imports library_modules, loads shared, a dictionary from names to objects that tasks
    take as vg.Shared(name), and, within edata_cache bytes, the smallest function and argument objects of the first
    submission, so tasks do not fetch their files. library_files maps local paths to names in the library sandbox.
    Command tasks run as standard TaskVine tasks with the cores they request. output_dir holds the directory of the
    file paths that TaskFile.path returns, which stay after close, and other keyword arguments go to the TaskVine
    Manager."""

    def __init__(self, port=cvine.VINE_DEFAULT_PORT, name=None, *, output_dir="./outputs", library_modules=(),
                 library_files=None, shared=None, edata_cache=0, **manager_options):
        self._closed = False
        self._manager = None
        self._executor = None  # opaque C executor handle
        self._resources = ExitStack()
        self._shared = _serialize_shared(shared)
        library_modules = [install.module_name(module) for module in library_modules]
        self._edata_cache = edata_cache
        # Until the first submission decides the cached edata, the library waits to be installed, and calls into C that
        # are queued meanwhile are held, so no task reaches C before the library caches its edata.
        self._library_pending = bool(edata_cache)
        self._held = None
        self._cached_edata = frozenset()  # sandbox names of staged objects that the library loads
        self._last_number = 0
        self._file_numbers = {}  # file identity -> number that names the file in sandboxes
        self._keep_paths = set()
        # Records of tasks that nothing can ask for anymore: submitted tasks that were collected, and the tasks of
        # collected Graphs. Any thread appends, and the background thread lets them go.
        self._dropped = collections.deque()

        # State the background thread owns: the C ids of submitted tasks and files.
        self._records = {}  # C node id -> _Record
        self._input_files = {}  # absolute path of a File -> C file id
        self._edata_files = {}  # sandbox name of a staged object -> C file id

        # State both threads share, guarded by the lock. The background thread notifies the condition when it
        # publishes changes or finishes a call that a caller waits for.
        self._lock = _Lock()
        self._changed = threading.Condition(self._lock)
        self._commands = collections.deque()  # calls into C, run in order by the background thread
        # Calls ever queued and calls the background thread ran. Clean-up work waits for the calls queued before it.
        self._queued = 0
        self._ran = 0
        # Clean-up work: (calls queued before it, function, argument), in that order. The background thread calls
        # function(self, argument) once every call queued before the work ran.
        self._deferred = collections.deque()
        # Whether a caller woke the background thread's wait in C since the thread last found no queued call.
        self._wake_pending = False
        self._events = []  # (record, entry) of tasks that finished, published for the Executor
        self._unfinished = 0  # tasks queued or submitted that did not finish
        # data key -> readable local path of a held output, or its bytes when the Manager holds them in memory. A data
        # key is the number of a record for its result, and (number, sandbox path) for a file its task writes.
        self._paths = {}
        # data keys of held outputs that their Workers cannot send, such as a directory that holds an entry its Worker
        # cannot read, published by the background thread until the next fetch of the key
        self._unsendable = set()
        # C file id -> (data key, record) of a fetch not yet reported readable; background thread only
        self._fetches = {}
        self._held_files = set()  # data keys (number, sandbox path) of held files
        # Serialized objects, shared by identity across tasks: id(object) -> (object, sandbox name, local path) for a
        # staged file, or (object, id(object), bytes) for a function or class small enough to travel inside manifests.
        # Holding the object keeps its id from being reused. An entry lives while a task that holds it may still run.
        self._edata = {}
        self._edata_names = {}  # sandbox name of a staged file -> (id of its object, local path)
        # Sandbox name of a staged file, or id of a small function or class -> tasks that hold it
        self._edata_holders = {}
        self._run_error = None  # why the run stopped as a whole
        self._capacity = None
        self._capacity_read = 0.0
        self._driver = None
        self._driver_ident = None
        self._pickler = _Pickler()

        self._output_root = _graph_module._absolute(output_dir)
        # An output directory this runner creates is removed again if it holds no file at close.
        self._created_output_root = not os.path.isdir(self._output_root)
        self._run_dir = None
        self._edata_dir = None
        try:
            os.makedirs(self._output_root, exist_ok=True)
            self._run_dir = tempfile.mkdtemp(prefix="vine-graph-run-", dir=self._output_root)
            self._close_executor_on(port)
            self._start(port, name, library_modules, library_files, manager_options)
            # Workers load the shared objects from the library's file, so this process keeps only their names.
            self._shared = dict.fromkeys(self._shared)
            _open_executors.add(self)
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
            if not other._closed and other.port == port:
                print(f"vine_graph: closing the open Executor on port {port}", file=sys.stderr)
                other.close()

    def _start(self, port, name, library_modules, library_files, manager_options):
        run_info_path = manager_options.get("run_info_path")
        run_info_template = manager_options.get("run_info_template")
        if run_info_path and run_info_template:
            _remove_files(os.path.join(run_info_path, run_info_template))
        self._manager = Manager(port=port, name=name, **manager_options)
        for setting, value in MANAGER_TUNING.items():
            self._tune_manager(setting, value)
        shared_path = None
        if self._shared:
            shared_path = os.path.join(self._run_dir, library.SHARED)
            install.save_objects(shared_path, self._shared)
        self._library_name = install.unique_name()
        self._library_options = (library_modules, library_files, shared_path)
        if not self._library_pending:
            self._install_library(None)
        self._executor = capi.vine_graph_executor_create(self._manager._taskvine, self._library_name,
                                                         library.run_node.__name__)
        if not self._executor:
            raise MemoryError("could not create the graph executor")
        for setting, value in EXECUTOR_TUNING.items():
            self._tune_executor(setting, value)
        self._edata_dir = self._resources.enter_context(
            tempfile.TemporaryDirectory(prefix="vine-graph-edata-", dir=self._manager.staging_directory))

    def _install_library(self, edata_path):
        """Install the library, which loads the cached edata at edata_path, if any, when it starts on a Worker."""
        modules, files, shared_path = self._library_options
        self._resources.enter_context(
            install.installed(self._manager, self._library_name, modules, files, shared_path, edata_path))

    @contextlib.contextmanager
    def first_submission(self):
        """Around the first submissions of a runner that caches edata, those of the first run() or submit(), hold the
        calls into C that they queue. Afterwards, install the library with the smallest objects that they staged, then
        release the calls."""
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

    def close(self):
        """Cancel unfinished tasks and release the Manager, the library, and every task result. File paths that
        TaskFile.path returned stay in output_dir. Closing a closed runner does nothing."""
        self._close(failed=False)

    def discard(self):
        """Close, and remove the run directory with the file paths that TaskFile.path returned."""
        self._close(failed=True)

    def _close(self, failed):
        with self._lock:
            if self._closed:
                return
            self._closed = True
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
            self._edata.clear()
            if self._run_dir is not None:
                _remove_run_data(self._run_dir, self._keep_paths, failed or not self._keep_paths)
                self._run_dir = None
                if self._created_output_root:
                    # Removes it only when empty, and it may be gone already.
                    try:
                        os.rmdir(self._output_root)
                    except OSError:
                        pass

    @property
    def closed(self):
        return self._closed

    # Settings.

    @property
    def port(self):
        """The port Workers connect to."""
        return None if self._manager is None else self._manager.port

    @property
    def name(self):
        """The name of the Manager in the catalog, or None."""
        return None if self._manager is None else self._manager.name

    def set_params(self, params):
        """Apply settings by name: graph executor settings (EXECUTOR_SETTINGS), and every other name as TaskVine
        Manager tuning, such as vault-disk-limit. An unknown name or invalid value raises ValueError."""
        _check_params(params)
        for setting, value in params.items():
            if setting == EDATA_CACHE:
                continue
            if setting in EXECUTOR_SETTINGS:
                self._call(lambda setting=setting, value=value: self._tune_executor(setting, value))
            else:
                number = _manager_setting(setting, value)
                self._call(lambda setting=setting, number=number: self._tune_manager(setting, number))

    def _tune_manager(self, setting, value):
        if self._manager.tune(setting, _manager_setting(setting, value)) != 0:
            raise ValueError(f"invalid value {value!r} for {setting}")

    def _tune_executor(self, setting, value):
        if capi.vine_graph_executor_tune(self._executor, setting, str(value)) != 0:
            raise ValueError(f"invalid value {value!r} for {setting}")

    # Submission, in the thread that submits.

    def submit(self, task, keep):
        """Stage one task and queue its submission for the background thread, and return its record. keep says why its
        result is pinned from the start (_SINK, _RULE, _FINAL), or is zero. A staging error, such as an argument that
        cannot be serialized, raises before anything is queued."""
        self._check_open()
        _check_task(task, self._shared)
        record = self._new_record(task)
        record.keep = keep
        if not record.command and task._readers:
            record.selections = _selections_read_from(task) or None
        names = []
        submitted = False
        try:
            plan = self._plan(task, record, names)
            restore_interrupts = _block_interrupts()
            try:
                with self._lock:
                    record.staged = names[0] if len(names) == 1 else tuple(names)
                    command = _Submission(plan)
                    command.executor = self
                    # The flag and the call that queues the task are next to each other, so a task is never queued
                    # without its record or recorded without being queued.
                    self._queued += 1
                    self._unfinished += 1
                    submitted = True
                    (self._commands if self._held is None else self._held).append(command)
                    self._wake_locked()
            finally:
                restore_interrupts()
        finally:
            if not submitted:
                with self._lock:
                    record.staged = None
                    self._drop_staged(names)
        return record

    def _plan(self, task, record, names):
        """Stage the arguments and manifest of a task, or build its command line, and return what the background
        thread declares and mounts. The sandbox name of every staged file the task holds is added to names."""
        graph = task._graph
        if record.command:
            line, results, files = self._command_line(task, "python3")
            result_records = [parent for _, parent in results]
            result_paths = [path for path, _ in results]
            return (record, record.outputs, (), result_records, result_paths, (), files, (), None,
                    (line, task._func.cores))
        func, args, kwargs = task._func, task._args, task._kwargs
        files = {}
        mounts = []  # (file identity, sandbox path, record of the task that writes the file or None)
        if task._reads:
            parents = _references(task)
            for ref in task._reads:
                identity = library.file_identity(ref)
                files[identity] = path = self._file_task_path(identity)
                source = parents[(ref.graph_id, ref.key)]._record if type(ref) is _FileRef else None
                mounts.append((identity, path, source))
        inline = []
        entries = {}  # id of an object -> its manifest entry
        edata = []  # (sandbox name, local path) of each staged file the task mounts
        stage = self._manifest_entry
        # A reference to a whole result, the most common argument, is held without a call of _manifest_entry().
        manifest_args = [(value.graph_id, value.key) if type(value) is _Ref and not value.steps
                         else stage(value, entries, names, inline, edata) for value in args]
        manifest_kwargs = {argument: stage(value, entries, names, inline, edata) for argument, value in kwargs.items()}
        # The library replaces the references in arguments that may hold them: references, and the copies the Graph
        # stored of containers that held symbols. An argument stored as it was passed holds none (Graph._plain).
        plain = graph._plain

        def holds_references(value):
            kind = type(value)
            if kind in _LEAF_TYPE_SET:
                return False
            if kind in _REFERENCE_TYPES or plain is None:
                return True
            entry = plain.get(id(value))
            return entry is None or entry[0] is not value

        scan = (tuple(index for index, value in enumerate(args) if holds_references(value)),
                tuple(argument for argument, value in kwargs.items() if holds_references(value)))
        # A result that the task reads at exactly one place, through a selection, is selected, and the task mounts the
        # producer's file of that selection when the producer writes one, instead of the whole result.
        selected = {}
        selections_read = []  # (producer's record, sandbox path, selection key of the producer's selection file)
        read_once = task._selections
        texts = []  # (graph id, key) of the Command tasks whose results the task reads, which are text
        result_records, result_paths, inputs = [], [], {}
        graph_id = graph._graph_id
        for parent in task._results or ():
            ref = (graph_id(parent), parent.key)
            source = parent._record
            if source.command:
                texts.append(ref)
            if read_once and ref in read_once:
                chain = read_once[ref][0]
                if chain is not None and source.selections and chain in source.selections:
                    selected[ref] = f"vine-graph-selection-{source.number}"
                    selections_read.append((source, selected[ref], chain))
                    continue
                selected[ref] = None
            inputs[ref] = path = f"vine-graph-result-{source.number}"
            result_records.append(source)
            result_paths.append(path)
        selections = record.selections
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
            "texts": texts,
        }
        return (
            record,
            record.outputs,
            [(chain, path) for chain, (_, path) in selections.items()] if selections else (),
            result_records,
            result_paths,
            selections_read or (),
            mounts,
            edata or (),
            self._pickler.dumps(manifest),
            None,
        )

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
        a new staged file and return its sandbox name and local path."""
        self._last_number += 1
        name = f"vine-graph-edata-{self._last_number}"
        target = _Serialized(os.path.join(self._edata_dir, name), inline_limit)
        try:
            self._pickler.dump(value, target)
            target.close()
        except BaseException:
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

    def cancel(self, records):
        """Cancel the C nodes of submitted tasks that did not finish, which report them finished, with every node that
        depends on them."""
        self._enqueue(functools.partial(self._cancel_nodes, list(records)))

    def abandon(self, records):
        """Let go of the records of tasks that a stopped run returned to not-started."""
        self._enqueue(functools.partial(self._abandon_nodes, list(records)))

    def pin(self, record):
        """Pin a result against release (_Record.keep) after its task was submitted."""
        self._enqueue(functools.partial(self._pin, record))

    def unpin(self, record):
        """Take back the pin that kept a result from release (_Record.keep), once nothing keeps it anymore."""
        self._enqueue(functools.partial(self._unpin, record))

    def _enqueue(self, command):
        """Queue a call into C for the background thread. A wake that the background thread has not answered yet covers
        every call queued after it."""
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
            return
        self._wake_pending = True
        capi.vine_graph_executor_wake(self._executor)

    def _call(self, function):
        """Run a call into C in the background thread and return its value, or raise its error."""
        if self._driver is None:
            return function()
        call = _Call(function)
        self._enqueue(call)
        self._wait_until(lambda: call.finished, None, "the call did not finish")
        if call.error is not None:
            raise call.error
        return call.value

    def _check_open(self):
        if self._closed:
            raise RuntimeError("the Executor was closed, as when another Executor took its port")

    # Calls into C, in the background thread.

    def _submit_node(self, plan):
        """Declare a task's outputs, mount its inputs and arguments, and submit it. Only the C executor can fail this,
        which stops the run."""
        record, outputs, selections, result_records, result_paths, selections_read, files, edata, manifest, command = plan
        executor = self._executor
        key = record.key
        node = capi.vine_graph_executor_add_node(executor)
        if not node:
            raise MemoryError(f"could not add a graph node for task {key!r}")
        record.node = node
        self._records[node] = record
        record.result = self._add_output(node, library.RESULT)
        if record.keep:
            # A result that may be asked for later is pinned before its task can complete and be released.
            if _capi.vine_graph_executor_pin_file(executor, record.result) != 0:
                raise RuntimeError(f"failed to pin the result of task {key!r}")
            record.pin = 1
        if selections:
            record.selection_files = {chain: self._add_output(node, path) for chain, path in selections}
        if outputs:
            record.files = {path: self._add_output(node, path) for path in outputs}
        if result_records or selections_read:
            mounts = list(zip(map(_RESULT_FILE, result_records), result_paths))
            mounts.extend((parent.selection_files[chain], name) for parent, name, chain in selections_read)
            self._check(capi.vine_graph_executor_add_inputs(executor, node, mounts), f"failed to mount the inputs of task {key!r}")
        for identity, name, source in files:
            if source is None:
                if identity[1] not in self._input_files:
                    self._input_files[identity[1]] = self._declare_file(self._link_input(identity[1]))
                file = self._input_files[identity[1]]
            else:
                file = source.files[identity[3]]
            self._add_input(node, file, name)
        if command is not None:
            if capi.vine_graph_executor_set_command(executor, node, command[0], command[1]) != 0:
                raise RuntimeError(f"failed to set the command of task {key!r}")
        else:
            # The library holds the cached objects, so tasks neither mount nor fetch their files.
            for name, path in edata:
                if name not in self._cached_edata:
                    file = self._edata_files.get(name)
                    if file is None:
                        file = self._edata_files[name] = self._declare_file(path, vault=True)
                    self._add_input(node, file, name)
            # The node owns its manifest, a buffer that the Manager sends with the task, and the C executor undeclares
            # it when it removes the node.
            if capi.vine_graph_executor_add_input_buffer(executor, node, manifest, library.MANIFEST) != 0:
                raise RuntimeError(f"failed to declare the manifest of task {key!r}")
        if capi.vine_graph_executor_submit_node(executor, node) != 0:
            raise RuntimeError(f"failed to submit task {key!r}")

    def _cancel_nodes(self, records):
        cancel = _capi.vine_graph_executor_cancel_node
        for record in records:
            if record.node:
                cancel(self._executor, record.node)
        # The tasks and their dependents are finished when cancel_node() returns.
        self._publish_finished()

    def _abandon_nodes(self, records):
        with self._lock:
            for record in records:
                self._let_go(record)

    def _pin(self, record):
        if record.node and not record.abandoned:
            if _capi.vine_graph_executor_pin_file(self._executor, record.result) == 0:
                record.pin += 1

    def _unpin(self, record):
        if record.pin > 0 and not record.abandoned:
            record.pin -= 1
            _capi.vine_graph_executor_unpin_file(self._executor, record.result)

    def _let_go(self, record):
        """Let go of a record that nothing can ask for anymore: take back its pins, free what its task staged, and drop
        its C node, which C removes once its task finished and its outputs were released. Called with the lock held,
        after every call queued before the record was let go ran."""
        if record.abandoned:
            return
        record.abandoned = True
        executor = self._executor
        if record.staged is not None:
            names, record.staged = record.staged, None
            self._drop_staged(names if type(names) is tuple else (names,))
        paths = self._paths
        paths.pop(record.number, None)
        for path in record.outputs:
            key = (record.number, path)
            paths.pop(key, None)
            if key in self._held_files:
                self._held_files.discard(key)
                if record.files:
                    _capi.vine_graph_executor_unpin_file(executor, record.files[path])
        if record.node:
            while record.pin > 0:
                record.pin -= 1
                _capi.vine_graph_executor_unpin_file(executor, record.result)
            if record.kept:
                record.kept = False
                _capi.vine_graph_executor_unkeep_node(executor, record.node)
            _capi.vine_graph_executor_drop_node(executor, record.node)

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

    def _hold(self, held):
        """Pin the data of a data key for reading and fetch it: a record for its result, or (record, sandbox path) for a
        file it writes. The pin lasts while the record does."""
        if type(held) is _Record:
            record = held
            file = record.result
            key = record.number
            record.pin += 1
        else:
            record, path = held
            key = (record.number, path)
            file = record.files[path]
        self._fetches[file] = (key, record)
        if self._unsendable and key in self._unsendable:
            with self._lock:
                self._unsendable.discard(key)
        executor = self._executor
        if _capi.vine_graph_executor_pin_file(executor, file) != 0:
            raise RuntimeError("failed to pin a result")
        if _capi.vine_graph_executor_fetch_file(executor, file) != 0:
            raise RuntimeError("failed to fetch a result")

    def _drop_staged(self, names):
        """Let go of the staged files and small functions and classes that a task held, and free each one that no task
        holds anymore. Called with the lock held."""
        holders = self._edata_holders
        for name in names:
            count = holders[name] - 1
            if count:
                holders[name] = count
                continue
            del holders[name]
            if not isinstance(name, str):
                # A small function or class lived only in this table.
                del self._edata[name]
                continue
            identity, path = self._edata_names.pop(name)
            del self._edata[identity]
            self._deferred.append((self._queued, _Workers._free_staged, (name, path)))

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
        """Do clean-up work until the deadline: queued work whose calls ran, and the removal of dropped C nodes. Return
        True when work is left."""
        executor = self._executor
        capi.vine_graph_executor_hold_sends(executor)
        try:
            monotonic = time.monotonic
            deferred = self._deferred
            dropped = self._dropped
            if dropped:
                # A record is let go once every call queued so far ran, among them the submissions of the tasks that
                # read its task, which were queued while the task still existed.
                with self._lock:
                    queued = self._queued
                    let_go = _Workers._let_go
                    while dropped:
                        deferred.append((queued, let_go, dropped.popleft()))
            while True:
                with self._lock:
                    ran = self._ran
                    while deferred and deferred[0][0] <= ran:
                        _, function, argument = deferred.popleft()
                        function(self, argument)
                        if monotonic() >= deadline:
                            break
                    work_left = deferred and deferred[0][0] <= ran
                removals_left = _capi.vine_graph_executor_remove_dropped(executor, REMOVAL_BATCH)
                while node := _capi.vine_graph_executor_next_removed(executor):
                    self._records.pop(node, None)
                if not removals_left and not work_left:
                    return False
                if monotonic() >= deadline:
                    return True
        finally:
            capi.vine_graph_executor_release_sends(executor)

    def _advance(self):
        """Run queued calls, do clean-up work, advance the run for about a second, and publish what happened. Return
        False when the runner closed. Only the background thread calls this."""
        deadline = time.monotonic() + COMMAND_SECONDS
        monotonic = time.monotonic
        lock = self._lock
        if self._run_error is not None:
            # The run stopped as a whole, so queued calls cannot run. The Manager keeps running until close.
            with lock:
                self._commands.clear()
                self._wake_pending = False
            capi.vine_graph_executor_wait(self._executor, 1)
            return not self._closed
        pending = False
        while not self._closed:
            with lock:
                commands = self._commands
                if not commands:
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
                    commands.extend(self._commands)
                    self._commands = commands
                    pending = bool(commands)
                break
        if self._closed:
            return False
        if self._clean_up(max(deadline, monotonic() + CLEANUP_SECONDS)):
            pending = True
        if pending:
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
            key, record = fetches.pop(file, (None, None))
            if record is None or record.abandoned:
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
            key, record = fetches.pop(file, (None, None))
            if record is not None and not record.abandoned:
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
        return True

    def _publish_finished(self):
        """Publish the tasks that completed or failed since the last call."""
        executor = self._executor
        records = self._records
        finished = []
        next_finished = _capi.vine_graph_executor_next_finished
        get_node_state = _capi.vine_graph_executor_get_node_state
        while node := next_finished(executor):
            record = records.get(node)
            if record is None:
                continue
            if get_node_state(executor, node) == capi.VINE_GRAPH_NODE_FAILED:
                source = records.get(capi.vine_graph_executor_get_failure_source(executor, node)) or record
                if source is record:
                    exit_code = capi.vine_graph_executor_get_node_exit_code(executor, node)
                    entry = (_FAILED, record, capi.vine_graph_executor_get_node_error(executor, node),
                             _FAILURE_REASONS.get(capi.vine_graph_executor_get_node_failure(executor, node)),
                             None if exit_code < 0 else exit_code)
                else:
                    # A dependent names its source and the source's reason, and reads the message from the source.
                    entry = (_FAILED, source, None,
                             _FAILURE_REASONS.get(capi.vine_graph_executor_get_node_failure(executor, node)), None)
            else:
                entry = _COMPLETED_ENTRY
            finished.append((record, entry))
        # A released node stays until its record is let go, so releases are only drained. A node that ran again after
        # its release is kept by C until its record is let go.
        while capi.vine_graph_executor_next_released(executor):
            pass
        while node := capi.vine_graph_executor_next_kept(executor):
            record = records.get(node)
            if record is not None:
                record.kept = True
        if finished:
            with self._lock:
                for record, entry in finished:
                    if record.finished is None:
                        self._unfinished -= 1
                    record.finished = entry
                self._events.extend(finished)
                self._changed.notify_all()

    # Published state, read by the threads that use the Executor.

    def wait_events(self, timeout):
        """Wait up to timeout seconds until a task finished whose event was not taken yet, or the runner stopped."""
        deadline = time.monotonic() + timeout
        with self._lock:
            while not self._events and not self._closed and self._run_error is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                self._changed.wait(min(remaining, 0.5))

    def pending(self):
        """Whether asking for events again could make progress on its own, which only a local runner does."""
        return False

    def drain(self):
        """Wait until every task that was submitted finished."""
        self._wait_until(lambda: self._unfinished <= 0, None, "tasks did not finish")

    def take_events(self, timeout=0):
        """Return the (record, entry) of tasks that finished since the last call, waiting up to timeout seconds for
        one. A run that stopped as a whole raises its error."""
        deadline = time.monotonic() + timeout
        with self._lock:
            while not self._events:
                self._check_open()
                if self._run_error is not None:
                    raise RuntimeError(f"vine_graph execution stopped: {self._run_error}")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return []
                self._changed.wait(min(remaining, 0.5))
            events, self._events = self._events, []
            return events

    def hold(self, record, path=None):
        """Pin and fetch a result, or the file at a sandbox path, of a completed task once, so it can be read."""
        if path is None:
            if record.held:
                return
            record.held = True
            self._enqueue(functools.partial(self._hold, record))
            return
        key = (record.number, path)
        if key in self._held_files:
            return
        self._held_files.add(key)
        self._enqueue(functools.partial(self._hold, (record, path)))

    def fetched(self, record, path=None, timeout=None):
        """Wait until a held output is readable at the Manager, and return its path, or its bytes when the Manager holds
        them in memory. A timeout in seconds that expires first raises TimeoutError."""
        self.hold(record, path)
        key = record.number if path is None else (record.number, path)
        # A completed task whose lost output had to be recreated fails when a task it reads failed and that task's
        # output exists nowhere, and C reports it again, as failed.
        self._wait_until(lambda: key in self._paths or key in self._unsendable or _lost(record),
                         timeout, "the result did not reach the Manager")
        with self._lock:
            fetched = self._paths.get(key)
        if fetched is not None:
            return fetched
        if _lost(record):
            source = record.finished[1]
            raise TaskError(f"task {record.label} completed, but its output was lost, and it cannot be recreated because "
                            f"task {source.label} failed", _task_of(record), _task_of(source), record.finished[3])
        output = "result" if path is None else f"output file {path!r}"
        raise TaskError(f"task {record.label} completed, but its Worker cannot send its {output}, which is or holds a "
                        f"file that the Worker cannot read, or is a FIFO, socket, or device", None, None, "result")

    def result(self, record, timeout=None):
        """Return the result of a completed task, fetched to the Manager once."""
        return _load_result(record, self.fetched(record, None, timeout))

    def file_bytes(self, record, path, limit):
        """Return the bytes of a file that a completed task wrote, or raise ValueError above limit bytes."""
        fetched = self.fetched(record, path)
        if isinstance(fetched, bytes):
            data = fetched
        else:
            if os.path.isdir(fetched):
                raise IsADirectoryError(errno.EISDIR, "the task wrote a directory, which has no content", path)
            if os.path.getsize(fetched) > limit:
                raise ValueError(_too_large(path, limit))
            with open(fetched, "rb") as stream:
                data = stream.read()
        if len(data) > limit:
            raise ValueError(_too_large(path, limit))
        return data

    def file_path(self, record, path):
        """Return a local path of a file that a completed task wrote, linked or copied out of the Manager's vault."""
        # The number of the record tells the file apart from those of the other Graphs that the Executor runs.
        identity = ("fetched", record.number, path)
        local = os.path.join(self._run_dir, self._file_task_path(identity))
        if not os.path.exists(local):
            fetched = self.fetched(record, path)
            if isinstance(fetched, bytes):
                with open(local, "wb") as stream:
                    stream.write(fetched)
            elif os.path.isdir(fetched):
                shutil.copytree(fetched, local, symlinks=True, copy_function=_link_or_copy)
            else:
                _link_or_copy(fetched, local)
        local = os.path.abspath(local)
        self._keep_paths.add(local)
        return local

    def capacity(self):
        """Return (Workers, cores) connected, which the background thread reads about once a second."""
        with self._lock:
            capacity = self._capacity
        if capacity is None:
            capacity = self._call(self._read_capacity)
            with self._lock:
                self._capacity = capacity
        return capacity

    def _read_capacity(self):
        capacity = capi.vine_graph_executor_get_capacity(self._executor)
        self._capacity_read = time.monotonic()
        return (capacity.workers, capacity.cores)

    def stats(self):
        """Return the makespan in seconds, the number of recovery tasks, and how far durability lags behind
        completions (see vine_graph_executor_get_durability())."""
        self._check_open()
        return self._call(lambda: {
            "makespan": capi.vine_graph_executor_get_makespan_us(self._executor) / 1e6,
            "recovery_tasks": capi.vine_graph_executor_get_completed_recovery_tasks(self._executor),
            "durability": _durability(capi.vine_graph_executor_get_durability(self._executor)),
        })

    def _wait_until(self, ready, timeout, unfinished):
        """Wait until ready() holds, checking it whenever the background thread publishes changes."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            while not ready():
                self._check_open()
                if self._run_error is not None:
                    raise RuntimeError(f"vine_graph execution stopped: {self._run_error}")
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError(unfinished)
                self._changed.wait(0.5 if remaining is None else min(remaining, 0.5))

    def _link_input(self, source):
        """Give this runner its own native cache identity for a File without copying its bytes.

        A closed runner can still receive late cache-update messages. A path of its own prevents those messages from
        satisfying the next runner's declaration after the old replica was unlinked."""
        directory = Path(self._run_dir) / ".inputs"
        directory.mkdir(exist_ok=True)
        path = directory / str(len(self._input_files) + 1)
        path.symlink_to(source)
        return str(path)


class _Local(_Runner):
    """The runner of an Executor in this process, for debugging without TaskVine. It runs one task at a time when it is
    asked for the next event, in the order the tasks were submitted, so a task runs after the tasks it reads. Each task
    runs in a sandbox directory of its own, and its function and arguments are serialized as on Workers, so it gets
    copies of them. Commands run with /bin/sh in the sandbox, which holds links to the files and results they read."""

    def __init__(self, *, output_dir="./outputs", shared=None):
        self._closed = False
        self._shared = _serialize_shared(shared)
        self._last_number = 0
        self._file_numbers = {}
        self._queue = collections.deque()  # (record, Task) of submitted tasks that did not run yet, in submission order
        self._events = []
        self._dropped = collections.deque()  # records of tasks that nothing can ask for anymore
        self._local_paths = {}  # (number of a record, sandbox path) -> local path of a file its task wrote
        self._results = {}  # number of a record -> local path of its task's result file
        self._seconds = 0.0
        self._keep_paths = set()
        self._pickler = _Pickler()
        self._output_root = _graph_module._absolute(output_dir)
        self._created_output_root = not os.path.isdir(self._output_root)
        os.makedirs(self._output_root, exist_ok=True)
        self._run_dir = tempfile.mkdtemp(prefix="vine-graph-run-", dir=self._output_root)

    @property
    def closed(self):
        return self._closed

    port = None
    name = None

    def close(self):
        self._close(not self._keep_paths)

    def discard(self):
        self._close(True)

    def _close(self, remove_all):
        if self._closed:
            return
        self._closed = True
        _remove_run_data(self._run_dir, self._keep_paths, remove_all)
        if self._created_output_root:
            # Removes it only when empty, and it may be gone already, such as a temporary directory removed at exit.
            try:
                os.rmdir(self._output_root)
            except OSError:
                pass

    def set_params(self, params):
        """Check settings as a runner on Workers does. A local runner has no Manager, so it applies none of them."""
        _check_params(params)

    def first_submission(self):
        return contextlib.nullcontext()

    def submit(self, task, keep):
        if self._closed:
            raise RuntimeError("the Executor was closed")
        _check_task(task, self._shared)
        record = self._new_record(task)
        # The function and its arguments are serialized as a runner on Workers serializes them, so one that cannot be
        # serialized fails alike.
        if not record.command:
            self._pickler.dumps(task._func)
            self._pickler.dumps((task._args, task._kwargs))
        self._queue.append((record, task))
        return record

    def cancel(self, records):
        """Report submitted tasks that did not run as cancelled, which keeps them from running."""
        for record in records:
            if record.finished is None:
                record.finished = (_FAILED, record, "it was cancelled", "cancelled", None)
                self._events.append((record, record.finished))

    def abandon(self, records):
        for record in records:
            self._forget(record)

    def pin(self, record):
        """A local runner keeps every result while its record exists."""

    def unpin(self, record):
        """A local runner keeps every result while its record exists."""

    def hold(self, record, path=None):
        """A local runner reads results where its tasks wrote them."""

    def _forget(self, record):
        """Let go of a record that nothing can ask for anymore, and remove what its task wrote unless a path of it was
        given out."""
        if record.abandoned:
            return
        record.abandoned = True
        result = self._results.pop(record.number, None)
        paths = [self._local_paths.pop((record.number, path), None) for path in record.outputs]
        if result is None or any(path in self._keep_paths for path in paths):
            return
        if not record.command:
            try:
                os.unlink(result)
            except OSError:
                pass
        shutil.rmtree(os.path.join(self._run_dir, ".tasks", f"task-{record.number}"), ignore_errors=True)

    def wait_events(self, timeout):
        """A local runner makes progress only when it is asked for events."""

    def pending(self):
        return bool(self._queue)

    def drain(self):
        """Run every task that was submitted and did not run."""
        while self._queue:
            self.take_events()

    def take_events(self, timeout=0):
        """Return what finished, running the next submitted task when nothing else did."""
        if self._closed:
            raise RuntimeError("the Executor was closed")
        dropped = self._dropped
        while dropped:
            self._forget(dropped.popleft())
        if not self._events:
            while self._queue:
                record, task = self._queue.popleft()
                if record.finished is None and not record.abandoned:
                    self._run(record, task)
                    break
        events, self._events = self._events, []
        return events

    def _run(self, record, task):
        for parent in task._parents:
            entry = parent._record.finished if parent._record is not None else None
            if entry is not None and entry[0] == _FAILED:
                # As in C, a task that reads a failed task fails without running and names its source.
                record.finished = (_FAILED, entry[1], None, entry[3], None)
                self._events.append((record, record.finished))
                return
        task_dir = os.path.join(self._run_dir, ".tasks", f"task-{record.number}")
        shutil.rmtree(task_dir, ignore_errors=True)
        os.makedirs(task_dir)
        # A task makes the directories of the files it writes, as on a Worker.
        started = time.time()
        try:
            if record.command:
                entry = self._run_command(task, record, task_dir)
            else:
                entry = self._run_function(task, record, task_dir)
        finally:
            self._seconds += time.time() - started
        record.finished = entry
        self._events.append((record, entry))

    def _run_function(self, task, record, task_dir):
        func, args, kwargs = task._func, task._args, task._kwargs
        serialized = self._pickler.dumps((args, kwargs))
        sources = {ref: parent._record for ref, parent in _references(task).items()} if task._parents else {}

        def load_parent(ref):
            source = sources[ref]
            path = self._results[source.number]
            return _read_text(path) if source.command else library.load_result(path)

        def file_path(identity):
            if identity[0] == "input":
                return identity[1]
            return self._local_paths[(sources[(identity[1], identity[2])].number, identity[3])]

        # The working directory belongs to the process, so local tasks of every Executor and thread run one at a time.
        with _LOCAL_TASKS:
            previous_cwd = os.getcwd()
            os.chdir(task_dir)
            try:
                args, kwargs = cloudpickle.loads(serialized)
                output = library.run_task(func, args, kwargs, load_parent, file_path,
                                          lambda name: cloudpickle.loads(self._shared[name]), task._selections or ())
                for task_path in record.outputs:
                    local_path = os.path.abspath(os.path.join(task_dir, task_path))
                    if not os.path.exists(local_path):
                        raise FileNotFoundError(f"task {record.label} did not write its output file {task_path!r}")
                    self._local_paths[(record.number, task_path)] = local_path
                result_path = os.path.join(self._run_dir, f"result-{record.number}")
                library.save_result(result_path, output)
                self._results[record.number] = result_path
                return _COMPLETED_ENTRY
            except KeyboardInterrupt:
                raise
            except BaseException as error:
                # As on a Worker, anything else the function raises, including an exit, fails the task.
                return (_FAILED, record, library.failure_report(error, __file__, library.__file__), "function", None)
            finally:
                os.chdir(previous_cwd)

    def _run_command(self, task, record, task_dir):
        line, results, files = self._command_line(task, shlex.quote(sys.executable))
        for path, parent in results:
            os.symlink(self._results[parent.number], os.path.join(task_dir, path))
        for identity, path, source in files:
            local = identity[1] if source is None else self._local_paths[(source.number, identity[3])]
            os.symlink(local, os.path.join(task_dir, path))
        # A Worker tells a command the cores it requested in CORES.
        process = subprocess.run(["/bin/sh", "-c", line], cwd=task_dir, stdin=subprocess.DEVNULL,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                 env=dict(os.environ, CORES=str(task._func.cores)))
        if process.returncode != 0:
            error = process.stderr.decode("utf-8", "replace")
            return (_FAILED, record, f"its command exited with status {process.returncode}:\n{error}", "function",
                    process.returncode)
        for task_path in record.outputs:
            self._local_paths[(record.number, task_path)] = os.path.abspath(os.path.join(task_dir, task_path))
        self._results[record.number] = os.path.join(task_dir, library.RESULT)
        return _COMPLETED_ENTRY

    def result(self, record, timeout=None):
        return _load_result(record, self._results[record.number])

    def file_bytes(self, record, path, limit):
        local = self._local_paths[(record.number, path)]
        if os.path.isdir(local):
            raise IsADirectoryError(errno.EISDIR, "the task wrote a directory, which has no content", path)
        if os.path.getsize(local) > limit:
            raise ValueError(_too_large(path, limit))
        with open(local, "rb") as stream:
            return stream.read()

    def file_path(self, record, path):
        local = self._local_paths[(record.number, path)]
        self._keep_paths.add(local)
        return local

    def capacity(self):
        return (1, 1)

    def stats(self):
        return {"makespan": self._seconds, "recovery_tasks": 0, "durability": None}


# What Executor.capacity() returns.
Capacity = collections.namedtuple("Capacity", ("workers", "cores"))


class Executor:
    """Where tasks run: TaskVine Workers that connect to port, or to the Manager named name through the catalog, or,
    with local=True, this process, one task at a time and with the same semantics, for debugging without Workers.

    run(graph) runs a Graph and returns the results of its sinks. submit(func, *args) starts one task at once and
    returns its Task, whose result() waits for it, for programs that look at a result and then decide what to run. One
    Executor runs any number of Graphs and submitted tasks on its Workers. Use it in a with statement, or close() it:

        with vg.Executor(port=9123) as executor:
            results = executor.run(graph)
            task = executor.submit(load, "more.root")
            if task.result() > 0:
                executor.submit(fit, task)

    Python tasks run in a TaskVine library on the Workers, and commands as standard TaskVine tasks. The library imports
    library_modules when it starts and loads shared, a dictionary from names to objects that tasks take as
    vg.Shared(name). library_files maps local paths to names in the library sandbox. progress draws a progress bar
    while a Graph runs. output_dir holds the directory of the file paths that TaskFile.path returns, which stay after
    the Executor closed. params holds settings by name, applied once: those of the graph executor (EXECUTOR_SETTINGS),
    edata-cache, a budget in bytes of function and argument objects of the first submission that the library loads
    when it starts, and every other name as TaskVine Manager tuning. Other keyword arguments go to the TaskVine Manager.

    Results. The result of a task that can still be asked for without a new task is kept: a sink of a Graph, a
    submitted task that no submitted task reads yet, and every result or file that was read here. Other results are
    released once the tasks that read them are safe, and one of them that is asked for after all, by result(), by a
    file, or by a task added or submitted later, is computed again, which the caller does not see. A submitted task is
    let go when nothing refers to its Task anymore, and the tasks of a Graph when the Graph is collected."""

    def __init__(self, port=cvine.VINE_DEFAULT_PORT, name=None, *, local=False, progress=True, shared=None, params=None,
                 library_modules=(), library_files=None, output_dir="./outputs", **manager_options):
        self._runner = None
        params = dict(params or {})
        # A bad setting raises before a Manager starts for it.
        _check_params(params)
        edata_cache = params.pop(EDATA_CACHE, 0)
        self._progress = bool(progress)
        self._local = bool(local)
        # Held while what finished is applied to Tasks and while a rule runs, so task states change in one thread at a
        # time and never under a rule.
        self._lock = _Lock()
        self._run = None  # the _Run in progress
        self._last_call = 0  # the key of the last submitted task
        if local:
            # A local runner has no Manager, so it takes only what it uses.
            runner = _Local(output_dir=output_dir, shared=shared)
        else:
            runner = _Workers(port, name, output_dir=output_dir, library_modules=library_modules,
                              library_files=library_files, shared=shared, edata_cache=edata_cache, **manager_options)
        try:
            runner.set_params(params)
        except BaseException:
            runner.discard()
            raise
        self._calls = _Calls(self, runner._dropped)
        self._runner = runner

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        runner = self._runner
        if exc_type is not None:
            # A block that raised discards its results.
            runner.discard()
            return
        try:
            # Submitted tasks that still run finish first, as the tasks of a Graph do before run() returns.
            if not runner.closed:
                try:
                    with self._lock:
                        runner.drain()
                        self._apply(runner.take_events())
                except RuntimeError:
                    pass
        finally:
            runner.close()

    def __del__(self):
        runner = self._runner
        if runner is not None:
            try:
                runner.close()
            except Exception:
                pass

    def __repr__(self):
        runner = self._runner
        if runner.closed:
            return "Executor(closed)"
        return "Executor(local)" if self._local else f"Executor(port={runner.port})"

    def close(self):
        """Cancel the tasks that did not finish and release the Workers and every task result. File paths that
        TaskFile.path returned stay in output_dir. Leaving a with block without an exception first waits for the
        submitted tasks that still run, and leaving it with one discards those file paths too. Closing a closed
        Executor does nothing."""
        self._runner.close()

    @property
    def port(self):
        """The port Workers connect to, or None for a local or closed Executor."""
        return self._runner.port

    @property
    def name(self):
        """The name of the Manager in the catalog, or None."""
        return self._runner.name

    def capacity(self):
        """Return the Workers connected and their cores, as a tuple with the fields workers and cores, read about once
        a second. A local Executor has one of each."""
        self._check_open()
        return Capacity(*self._runner.capacity())

    def _check_open(self):
        if self._runner.closed:
            raise RuntimeError("the Executor was closed, as when another Executor took its port")

    # Graphs.

    def run(self, graph):
        """Run the tasks of a Graph that did not finish, and the tasks that its rules add, until no task runs and no
        rule is due, and return {sink: result} of the completed tasks that no task reads. A failure raises the
        TaskError of a task that failed on its own, after the other tasks finished, and a rule that raises fails the
        run with an error that names the rule and the task. A Graph runs on one Executor, which holds its results, any
        number of times, and an Executor runs one Graph at a time."""
        if not isinstance(graph, Graph) or isinstance(graph, _Calls):
            raise TypeError("run() takes a Graph")
        self._check_open()
        if graph._run is not None:
            raise RuntimeError("the Graph is already running; a rule may add tasks, cancel them, or stop the run")
        if self._run is not None:
            raise RuntimeError("the Executor is already running a Graph, and runs one at a time; submit() starts "
                               "single tasks meanwhile")
        bound = graph._executor
        if bound is not self:
            if bound is not None and graph._records:
                if bound._runner.closed:
                    raise RuntimeError("the Executor that ran this Graph was closed, so the results of its completed "
                                       "tasks are gone; build a new Graph to run them again")
                raise ValueError("this Graph ran on another Executor, which holds the results of its tasks; a Graph "
                                 "runs on one Executor")
            # The Executor lets the records of the Graph go when the Graph is collected. The call holds the list of
            # the records and the runner's queue, not the Graph.
            graph._records = []
            graph._executor = self
            weakref.finalize(graph, self._runner._dropped.extend, graph._records).atexit = False
        run = _Run(self, graph, self._progress)
        graph._run = self._run = run
        try:
            return run.execute()
        finally:
            graph._run = self._run = None

    # Single tasks.

    def submit(self, func, /, *args, **kwargs):
        """Start a task that calls func with these arguments, or runs a Command with values for its placeholders, and
        return its Task at once. Files, and Tasks that this Executor started or ran, with their selections and files,
        may appear anywhere inside the arguments of a function. task.result() waits for the task and returns its
        result. A task that cannot start, as when an argument cannot be serialized, fails, and its result() raises."""
        self._check_open()
        with self._lock:
            self._last_call += 1
            task = self._calls._add(self._last_call, func, args, kwargs)
            with self._runner.first_submission():
                self._start(task, None)
            if self._local and not self._in_rule():
                # A local task runs when it is submitted, after the submitted tasks that waited for a rule to return.
                while task._state == _RUNNING and self._runner.pending():
                    self._apply(self._runner.take_events())
        return task

    def _in_rule(self):
        """Whether the calling thread is inside a rule, where task states must not change."""
        run = self._run
        return run is not None and run.in_rule and threading.current_thread() is run.thread

    def _start(self, task, run):
        """Submit a task after the tasks it reads, and record why its result is kept. A task whose parent failed or was
        cancelled fails or is cancelled at once without running, and so does one that cannot be submitted. Called with
        the lock held."""
        if task._state != _NEW:
            return
        parent = next((parent for parent in task._parents if parent._state >= _FAILED), None)
        if parent is not None:
            if parent._state == _CANCELLED:
                task._state = _CANCELLED
            else:
                task._state = _FAILED
                task._error = (_FAILED, parent._error[1] or parent, None, parent._error[3], None)
                if run is not None:
                    run.failed.append(task)
            if run is not None:
                run.count_finished()
            return
        graph = task._graph
        rules = run is not None and graph._has_rules(task)
        # A result that may be asked for without a new task is pinned from the start: nothing reads it yet, a rule gets
        # its task, or its task writes a file that no task reads. Any other result is left to the release rule, and is
        # computed again when it is asked for after all.
        keep = 0 if task._readers else _SINK
        if rules:
            keep |= _RULE
        if task._outputs and _writes_unread_file(task):
            keep |= _FINAL
        try:
            record = self._runner.submit(task, keep)
        except KeyboardInterrupt:
            raise
        except Exception as error:
            # The task cannot run, such as when an argument cannot be serialized, and its readers fail with it.
            message = "".join(traceback.format_exception_only(error)).strip()
            task._state = _FAILED
            task._error = (_FAILED, None, f"it could not be submitted: {message}", "submit", None)
            if run is not None:
                run.failed.append(task)
                run.count_finished()
            return
        task._record = record
        task._state = _RUNNING
        task._rule_pending = rules
        if run is not None:
            graph._records.append(record)
            run.running.add(task)
        # A task that now has a reader is no sink anymore, and the release rule waits for its reader.
        for parent in task._parents:
            source = parent._record
            if source is not None and source.keep & _SINK:
                source.keep &= ~_SINK
                if not source.keep:
                    self._runner.unpin(source)

    def _rules_ran(self, task):
        """Take back the pin that kept the result of a task for its rules, unless the task is still a sink."""
        record = task._record
        if record is None or not record.keep & _RULE:
            return
        keep = record.keep & ~_RULE
        if not keep and not any(reader._record is not None for reader in _readers_of(task)):
            keep = _SINK
        record.keep = keep
        if not keep:
            self._runner.unpin(record)

    # What finished.

    def _apply(self, events):
        """Apply what finished to the Tasks: set their states, tell the run of their Graph, and queue the rules of
        those that completed. Called with the lock held, and never inside a rule."""
        calls = self._calls
        for record, entry in events:
            if record.abandoned:
                continue
            graph = record.graph()
            if graph is None:
                continue
            task = graph._tasks.get(record.key)
            if task is None or task._record is not record or task._state != _RUNNING:
                # A task that was cancelled stays cancelled whatever its submission did.
                continue
            run = graph._run
            if run is not None:
                run.running.discard(task)
            if entry[0] == _FAILED and entry[3] == "cancelled":
                if graph is calls:
                    # A submitted task that reads a cancelled task is cancelled with it.
                    task._state = _CANCELLED
                    continue
                # A task of a Graph that was cancelled ignores its events, so this is the cancel of a stop that an
                # interrupt cut short, of the task or of a task it reads: it starts again.
                self._reset([task])
                if run is not None:
                    run.resubmit = True
                continue
            if entry[0] == _COMPLETED:
                task._state = _COMPLETED
                if task._rule_pending:
                    graph._due.append(task)
            else:
                source = entry[1]
                task._state = _FAILED
                task._error = (_FAILED, None if source is record else _task_of(source), entry[2], entry[3], entry[4])
                if run is not None:
                    run.failed.append(task)
            if run is not None:
                run.count_finished()

    def _reset(self, tasks):
        """Return stopped tasks to not-started and let go of their submissions."""
        records = []
        for task in tasks:
            records.append(task._record)
            task._state = _NEW
            task._record = None
            task._rule_pending = False
        if records:
            self._runner.abandon(records)

    def _poll(self):
        """Apply what finished, without waiting, for a caller that asks how far a submitted task is."""
        if self._runner.closed or self._in_rule():
            return
        try:
            with self._lock:
                self._apply(self._runner.take_events())
        except RuntimeError:
            # A run that stopped as a whole finishes nothing more, which result() reports.
            pass

    def _wait(self, task, deadline):
        """Wait until a submitted task finished, applying what finishes meanwhile."""
        if self._in_rule():
            raise RuntimeError(f"{task!r} is still running, and a rule cannot wait for a task: task states do not "
                               f"change while a rule runs")
        runner = self._runner
        while True:
            with self._lock:
                self._apply(runner.take_events())
            if task._state != _RUNNING:
                return
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(f"{task!r} did not finish in time")
            runner.wait_events(0.5 if remaining is None else min(remaining, 0.5))

    def _cancel_call(self, task):
        """Cancel a submitted task that did not finish. C cancels the submitted tasks that read it, which report it."""
        self._poll()
        with self._lock:
            if task._state == _RUNNING:
                task._state = _CANCELLED
                self._runner.cancel([task._record])

    # Results.

    def _completed_record(self, task, deadline):
        """Return the record of a completed task, waiting for a submitted task that still runs, or raise why the task
        has no result."""
        if task._state == _RUNNING and task._graph is self._calls:
            self._wait(task, deadline)
        if task._state == _FAILED:
            raise task_error(task)
        if task._state != _COMPLETED:
            if task._state == _CANCELLED:
                raise RuntimeError(f"{task!r} was cancelled, so it has no result")
            raise RuntimeError(f"{task!r} did not complete, so it has no result; Executor.run() runs its Graph")
        self._check_open()
        return task._record

    def _result(self, task, timeout):
        deadline = None if timeout is None else time.monotonic() + timeout
        record = self._completed_record(task, deadline)
        try:
            return self._runner.result(record, None if deadline is None else max(deadline - time.monotonic(), 0))
        except TaskError as error:
            if error.task is None:
                error.task = error.source = task
            raise

    def _file(self, file, what):
        task = file.task
        record = self._completed_record(task, None)
        if file.name not in record.outputs:
            raise ValueError(f"the file {file.name!r} of {task!r} was asked for after the task started; ask for its "
                             f"files before it starts, as when it is added to its Graph")
        try:
            if what == "path":
                return self._runner.file_path(record, file.name)
            return self._runner.file_bytes(record, file.name, CONTENT_LIMIT)
        except TaskError as error:
            if error.task is None:
                error.task = error.source = task
            raise


def _writes_unread_file(task):
    """Return whether a task declared a file that no task of its Graph reads, which only the caller can ask for."""
    read = set()
    key = task.key
    for reader in _readers_of(task):
        for ref in reader._reads or ():
            if type(ref) is _FileRef and ref.graph_id is None and ref.key == key:
                read.add(ref.path)
    return len(read) < len(task._outputs)


class _Run:
    """One call of Executor.run(). Its thread submits the tasks of its Graph, applies what finished, and calls rules,
    and only rules called in that thread may add tasks, cancel them, or stop the run."""

    def __init__(self, executor, graph, progress):
        self.executor = executor
        self.runner = executor._runner
        self.graph = graph
        self.thread = threading.current_thread()
        self.in_rule = False
        self.added = []  # tasks that the current rule added, submitted once it returns
        self.running = set()  # tasks submitted and not finished
        self.stop_requested = False
        self.resubmit = False  # tasks went back to not-started during the run and wait to be submitted again
        self.rule_error = None
        self.failed = []  # tasks that failed in this run, in the order they failed
        self.finished = 0  # tasks completed, failed, or cancelled, for the progress bar
        self.bar = None
        self.part = None
        self.progress = progress

    # The progress bar.

    def _start_bar(self):
        if not self.progress:
            return
        tasks = self.graph._tasks
        self.finished = sum(1 for task in tasks.values() if task._state >= _COMPLETED)
        self.bar = capi.progress_bar_init("Executing Tasks")
        capi.progress_bar_set_update_interval(self.bar, 0.1)
        self.part = capi.progress_bar_create_part("Tasks", len(tasks))
        capi.progress_bar_bind_part(self.bar, self.part)
        if self.finished:
            capi.progress_bar_update_part(self.bar, self.part, self.finished)

    def count_finished(self, count=1):
        self.finished += count
        if self.bar is not None:
            capi.progress_bar_update_part(self.bar, self.part, count)

    def task_added(self):
        if self.bar is not None:
            capi.progress_bar_set_part_total(self.bar, self.part, len(self.graph._tasks))

    def _finish_bar(self):
        if self.bar is not None:
            sys.stdout.flush()
            capi.progress_bar_finish(self.bar)
            capi.progress_bar_delete(self.bar)
            self.bar = None
            # The bar writes to the C library's standard output, which Python's own buffer does not flush.
            _libc.fflush(None)

    # The loop.

    def execute(self):
        graph = self.graph
        executor = self.executor
        runner = self.runner
        lock = executor._lock
        self._start_bar()
        interrupted = None
        try:
            with lock:
                # An interrupt inside the stop of an earlier run may have left tasks running. The stop finishes first,
                # so no new task reads a task whose cancel is on its way, and tasks it did not reach are waited for.
                stale = []
                for task in graph._tasks.values():
                    if task._state == _RUNNING:
                        if task._record is None:
                            task._state = _NEW
                        elif task._record.stopping:
                            stale.append(task)
                        else:
                            self.running.add(task)
                if stale:
                    waiting, self.running = self.running, set(stale)
                    self._stop()
                    self.running = waiting
                with runner.first_submission():
                    self._submit([task for task in graph._tasks.values() if task._state == _NEW])
            due = graph._due
            while True:
                with lock:
                    # A local runner executes a task when asked for events, so a stop is noticed first.
                    if self.stop_requested:
                        break
                    executor._apply(runner.take_events())
                    if self.resubmit:
                        # Tasks that a cancel of an interrupted stop reached start again, after the tasks they read.
                        self.resubmit = False
                        self._submit([task for task in graph._tasks.values() if task._state == _NEW])
                    if due:
                        self._call_rules(due.popleft())
                        continue
                    if not self.running:
                        break
                    if runner.pending():
                        continue
                # Other threads apply what finishes for the tasks they submitted while this one waits.
                runner.wait_events(0.5)
        except KeyboardInterrupt as error:
            interrupted = error
        finally:
            try:
                if self.running:
                    # A stop, a rule that raised, or an interrupt returns unfinished tasks to not-started.
                    with lock:
                        self._stop()
            finally:
                self._finish_bar()
        if interrupted is not None:
            raise interrupted
        if self.rule_error is not None:
            raise self.rule_error
        # A run raises for the tasks that failed in it, among them new readers of a task that failed before, with the
        # error of a task that failed on its own in preference to the tasks that failed because of it. A task that
        # failed in an earlier run stays failed, and its error stays on it, but a later run that does not touch it
        # returns its results.
        failed = self.failed
        if failed:
            raise task_error(next((task for task in failed if task._error[1] is None), failed[0]))
        sinks = [task for task in graph._tasks.values() if task._state == _COMPLETED and not task._readers]
        for task in sinks:
            runner.hold(task._record)
        return {task: executor._result(task, None) for task in sinks}

    def _submit(self, tasks):
        """Submit tasks in the order given, each after the tasks it reads."""
        start = self.executor._start
        for task in tasks:
            start(task, self)

    def _call_rules(self, task):
        graph = self.graph
        rules = graph._rules_of(task)
        try:
            for rule in rules:
                self.in_rule = True
                try:
                    rule.function(task)
                except Exception as error:
                    name = getattr(rule.function, "__qualname__", None) or repr(rule.function)
                    failure = RuntimeError(f"rule {name} raised for task {task!r}: {type(error).__name__}: {error}")
                    failure.__cause__ = error
                    self.rule_error = failure
                    self.stop_requested = True
                finally:
                    self.in_rule = False
                    added, self.added = self.added, []
                    if added:
                        self._submit(added)
                if self.stop_requested:
                    break
        finally:
            task._rule_pending = False
            self.executor._rules_ran(task)

    # Changes that rules make.

    def cancel(self, tasks):
        """Cancel tasks and their readers that did not finish: tasks not started never run, and running ones stop."""
        stack = [task for task in tasks]
        cancelled = []
        while stack:
            task = stack.pop()
            if task._state == _NEW:
                task._state = _CANCELLED
            elif task._state == _RUNNING:
                task._state = _CANCELLED
                self.running.discard(task)
                cancelled.append(task._record)
            else:
                continue
            self.count_finished()
            stack.extend(_readers_of(task))
        if cancelled:
            self.runner.cancel(cancelled)

    def _stop(self):
        """Cancel the running tasks and return them to not-started, so a later run continues them. Tasks that
        completed or failed before the cancel arrived keep that outcome. Called with the lock held."""
        records = []
        runner = self.runner
        for task in self.running:
            task._record.stopping = True
            records.append(task._record)
            # A cancelled reader no longer holds back the release of what it read, and the run that continues reads it
            # again, so the completed tasks that a stopped task reads are sinks until it is submitted again.
            for parent in task._parents:
                source = parent._record
                if parent._state == _COMPLETED and source is not None and not source.keep & _SINK:
                    if not source.keep:
                        runner.pin(source)
                    source.keep |= _SINK
        runner.cancel(records)
        stopping = set(self.running)
        executor = self.executor
        reset = []
        while stopping:
            try:
                events = self.runner.take_events(0.5)
            except Exception:
                # A run that failed as a whole reports nothing more, and its unfinished tasks cannot complete.
                for task in stopping:
                    task._record.stopping = True
                    if task._record.finished is None:
                        reset.append(task)
                break
            for record, entry in events:
                task = None if record.abandoned else _task_of(record)
                if task is not None and task in stopping and task._record is record and task._state == _RUNNING:
                    stopping.discard(task)
                    if record.stopping and entry[0] == _FAILED and entry[3] == "cancelled":
                        reset.append(task)
                        continue
                # Anything else finished on its own, among it the tasks of other Graphs and submitted tasks.
                executor._apply(((record, entry),))
        self.running.clear()
        executor._reset(reset)


def task_error(task):
    """Return the TaskError of a failed task, which names the task whose own failure explains it."""
    entry = task._error
    source = entry[1] or task
    source_entry = source._error if source._error is not None else entry
    message, reason, exit_code = source_entry[2], source_entry[3], source_entry[4]
    message, exception = library.load_failure(message or "")
    task_label, source_label = _label(task), _label(source)
    # A function failure ends with its traceback. Lines the library writes before it are not about the function, and
    # neither are its timestamped log lines in a message without a traceback.
    start = message.find("Traceback (most recent call last):")
    if start >= 0:
        message = f"\n{message[start:]}"
        if exception is not None:
            exception.add_note(f"raised by task {source_label}:{message.rstrip()}")
    elif exit_code is not None:
        # A command's message is its standard error, of which the end tells what went wrong.
        head, _, error = message.partition("\n")
        if len(error) > COMMAND_ERROR_TAIL:
            error = "...\n" + error[-COMMAND_ERROR_TAIL:]
        message = f" {head}\n{error}" if error else f" {head}"
    else:
        message = f" {LIBRARY_LOG_LINE.sub('', message)}"
    if source is task:
        return TaskError(f"task {task_label} failed:{message}", task, source, reason, exception, exit_code)
    return TaskError(f"task {task_label} did not run because task {source_label} failed:{message}", task, source,
                     reason, exception, exit_code)


def _lost(record):
    """Return whether C reported a submitted task as failed, such as a completed one whose output cannot be recreated."""
    finished = record.finished
    return finished is not None and finished[0] == _FAILED


def _read_text(path):
    """Return the text of a command task's result file, at most COMMAND_RESULT_LIMIT bytes of its standard output."""
    with open(path, "rb") as stream:
        return stream.read(COMMAND_RESULT_LIMIT).decode("utf-8", "replace")


def _load_result(record, fetched):
    """Load a task result: the text of a command's standard output, or the object a Python task returned, from its
    bytes, which the Manager held in memory, or from a path."""
    if record.command:
        if type(fetched) is bytes:
            return fetched[:COMMAND_RESULT_LIMIT].decode("utf-8", "replace")
        return _read_text(fetched)
    try:
        if type(fetched) is bytes:
            return cloudpickle.loads(fetched)["Result"]
        return library.load_result(fetched)
    except Exception as error:
        # A result that cannot be loaded in this process, such as an object of a module this process cannot import,
        # raises an error that names the task instead of the error of pickle.
        raise TaskError(f"task {record.label} completed, but its result cannot be loaded: {type(error).__name__}: "
                        f"{error}", None, None, "result") from error


def _too_large(path, limit):
    return (f"the file {path!r} holds more than {limit >> 20} MiB, more than TaskFile.content reads; pass the file to a "
            f"task instead, or use TaskFile.path")


def _remove_run_data(run_dir, keep_paths, remove_all):
    """Remove a run directory, keeping the paths that TaskFile.path returned unless remove_all is set."""
    if remove_all:
        shutil.rmtree(run_dir, ignore_errors=True)
        return

    def kept(path):
        while path != run_dir:
            if path in keep_paths:
                return True
            path = os.path.dirname(path)
        return False

    for directory, dirs, files in os.walk(run_dir, topdown=False):
        if kept(directory):
            continue
        for name in files:
            path = os.path.join(directory, name)
            if path not in keep_paths:
                os.unlink(path)
        for name in dirs:
            path = Path(directory) / name
            if path.is_symlink():
                if str(path) not in keep_paths:
                    path.unlink()
            elif str(path) not in keep_paths and not any(path.iterdir()):
                path.rmdir()


def _selections_read_from(task):
    """Return {selection key: (steps, sandbox path)} of the distinct selections of a task's result that its readers
    read at exactly one place. The task writes each to a file of its own, which those readers mount instead of the whole
    result. A reader added after the task was submitted mounts such a file when it reads the same selection, and the
    whole result otherwise."""
    selections = {}
    ref = (None, task.key)
    for reader in _readers_of(task):
        read_once = reader._selections
        if not read_once:
            continue
        entry = read_once.get(ref)
        if entry is not None and entry[0] is not None and entry[0] not in selections:
            selections[entry[0]] = (entry[1], f"vine-graph-selection-output-{len(selections) + 1}")
    return selections


def _check_task(task, shared):
    """Raise the error that keeps a task from running, such as a shared object that its Executor does not hold."""
    for name in task._shared or ():
        if name not in shared:
            raise KeyError(f"no shared object named {name!r}; give it to the Executor as shared={{...}}")
    for ref in task._reads or ():
        if isinstance(ref, File):
            _check_input_file(ref.path)


def _check_params(params):
    """Raise ValueError for a setting that an Executor would refuse."""
    for setting, value in params.items():
        if setting == EDATA_CACHE:
            if type(value) is not int or value < 0:
                raise ValueError(f"invalid value {value!r} for {setting}: it takes a number of bytes, zero or more")
        elif setting in EXECUTOR_SETTINGS:
            problem = capi.vine_graph_executor_check_setting(setting, str(value))
            if problem:
                raise ValueError(f"invalid value {value!r} for {setting}: {problem}")
        else:
            _manager_setting(setting, value)


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


def _durability(durability):
    """Return the fields of a C vine_graph_durability as a dictionary."""
    return {"exposed": durability.exposed, "pending": durability.pending, "output_rate": durability.output_rate,
            "lag": durability.lag, "large_rate": durability.large_rate, "large_capacity": durability.large_capacity,
            "cut": durability.cut}


def _link_or_copy(source, destination):
    """Hard-link source to destination, or copy it where links fail, such as across file systems."""
    try:
        os.link(source, destination)
    except OSError:
        shutil.copyfile(source, destination)


def _serialize_shared(shared):
    """Serialize each shared object on its own and return {name: bytes}, so an unpicklable object or a bad name fails
    the Executor before it starts."""
    if shared is None:
        return {}
    if not isinstance(shared, dict):
        raise TypeError("shared takes a dictionary from names to objects")
    serialized = {}
    for name, value in shared.items():
        if not isinstance(name, str) or not name:
            raise TypeError("a shared object is named by a non-empty string")
        serialized[name] = cloudpickle.dumps(value)
    return serialized


_MAIN_THREAD = threading.main_thread().ident
_INTERRUPT = (signal.SIGINT,)


# A call that changes no signal mask, which _block_interrupts() returns in threads other than the main thread.
_KEEP_INTERRUPTS = functools.partial(_signal.pthread_sigmask, _signal.SIG_BLOCK, ())


def _block_interrupts():
    """Hold back SIGINT while a short change of Executor state completes, so an interrupt never leaves a task queued but
    unrecorded, and return the call that restores the signal mask, which delivers a held interrupt. Other threads keep
    their mask, since only the main thread receives KeyboardInterrupt. The built-in function takes and returns signal
    numbers, while signal.pthread_sigmask() converts the returned set to enum members in Python.

    Python raises an interrupt when a Python function starts and after a built-in function returns. An interrupt that
    is already pending is raised right after SIGINT is blocked, before the caller has the call that restores the mask,
    so the mask is restored here. The returned call is a built-in partial, which a caller makes in a finally block
    without starting a Python function that an interrupt could stop first. It is made before SIGINT is blocked, so
    nothing that could raise runs between blocking and returning. Otherwise an interrupt raised when
    `functools.partial()` returned could leave the main thread blocking SIGINT for good, and so every thread it starts
    later."""
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
            # The run stops as a whole, and the next steps only keep the Manager running until close.
            with executor._lock:
                if executor._run_error is None:
                    executor._run_error = f"{error}"
                executor._changed.notify_all()
        del executor


def _in_notebook():
    """Return True inside a Jupyter kernel."""
    try:
        from IPython import get_ipython
    except ImportError:
        return False
    return type(get_ipython()).__name__ == "ZMQInteractiveShell"


def _remove_files(root):
    """Remove the files under a run-info template directory, keeping its directories."""
    for directory, _, files in os.walk(root):
        for name in files:
            try:
                os.remove(os.path.join(directory, name))
            except FileNotFoundError:
                pass
