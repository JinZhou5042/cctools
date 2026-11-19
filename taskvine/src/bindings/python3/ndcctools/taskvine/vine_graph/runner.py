# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""What both runners of an Executor share: the C bindings, the settings, the record of a live task, serialization,
the lock that interrupts never leave held, and how sandboxes name files and commands. _Workers (workers.py) runs tasks on
TaskVine Workers and _Local (local.py) in this process."""

import collections
import ctypes
import errno
import functools
import importlib
import io
import math
import os
import shlex
import shutil
import signal
import _signal
import _thread
import sys
import tempfile
import threading
import time
from pathlib import Path

import cloudpickle

# SWIG-generated vine_graph_capi.py imports "cvine"; wire that top-level name to the real TaskVine Python module before
# importing the generated bindings. Also expose _cvine's C symbols globally so _vine_graph_capi reuses the same
# TaskVine/dttools runtime instead of loading a second copy of those globals.
cvine = importlib.import_module("ndcctools.taskvine.cvine")
ctypes.CDLL(cvine._cvine.__file__, mode=getattr(os, "RTLD_NOW", 0) | getattr(os, "RTLD_GLOBAL", ctypes.RTLD_GLOBAL))
sys.modules.setdefault("cvine", cvine)

from . import vine_graph_capi as capi  # noqa: E402
from . import graph as _graph_module, library, store as _store_module  # noqa: E402
from .graph import (  # noqa: E402
    COMMAND_RESULT_LIMIT, URL, File, TaskError, _COMPLETED, _FileRef, _Ref, _clashes_with_output, _name_task,
    reserved_name,
)

# The C functions themselves, which the background thread calls for every task without the generated Python wrapper.
_capi = capi._vine_graph_capi

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
    # Library instances in a row that may die before completing a call before the library is removed and the run
    # stops. An instance that dies before it runs a task, as in a Worker without the Python environment of this
    # program, dies again on every Worker, and each death prints a notice.
    "max-library-retries": 3,
    # Gigabytes of task results the Manager may keep as checkpoints. Zero means unlimited.
    "vault-disk-limit": 1536,
    # Failed fetches of a URL from its origin after which the tasks that read it fail with reason "input".
    "max-origin-failures": 3,
}

# Settings of the C graph executor and their defaults. Executor.run() draws the progress bar (_ProgressBar), so the C
# executor draws none.
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

# Why a result is pinned, as bits of _Record.keep: no task that was submitted reads it yet, or its task wrote a file
# that no task reads. While either holds, the release rule leaves the result alone.
_SINK = 1
_FINAL = 2

# The entry of every task that completed, which the records share. A failure is (_FAILED, record of the source,
# message, reason, exit code), and only a source holds its message, reason, and exit code.
_COMPLETED_ENTRY = (_COMPLETED, None, None, None, None)


# Characters of standard error that the TaskError of a failed command keeps, from its end.
COMMAND_ERROR_TAIL = 4096


# Bytes of the longest file name, without its directory, that Linux file systems take (NAME_MAX).
_NAME_MAX = 255

# The Python code that the shell of a command task runs to read str() of a Python task's result, which the result file
# holds serialized. Workers run it with the python3 of their environment, and local runs with this interpreter.
_STR_RESULT = 'import sys, cloudpickle; sys.stdout.write(str(cloudpickle.load(open(sys.argv[1], "rb"))["Result"]))'


class _Lock(_thread.RLock):
    """The reentrant lock of an Executor's shared state, which a KeyboardInterrupt never leaves held.

    Python raises an interrupt in the main thread after a built-in call returns, so with the built-in lock an interrupt
    right after a with statement took the lock, or right after Condition.wait() released it, leaves it held for good,
    and close() waits for the background thread forever. This lock gives the lock back when an interrupt stops the call
    that took it, and takes it back when one stops the release for a wait. Use it in with statements, not through the
    Condition, whose own __enter__ and __exit__ an interrupt may stop before they reach the lock."""

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


def _written_size(data):
    """Return the bytes of a piece that a pickler writes: bytes, or, for a large buffer such as the data of a NumPy
    array or a bytearray, a pickle.PickleBuffer, which has no len()."""
    if type(data) is bytes:
        return len(data)
    with memoryview(data) as view:
        return view.nbytes


class _Pickler:
    """The cloudpickle Picklers of the thread that creates them, reused for every object that thread serializes,
    because constructing one costs more than pickling a small object such as a manifest. dump() writes to a target,
    and dumps() returns bytes. Each dump starts and ends with an empty memo, so a Pickler keeps no object alive. Other
    threads, and a dump nested in another, use a Pickler of their own.

    A memo table keeps the capacity of its largest dump, and clearing it touches every slot, so a Pickler whose dump
    wrote more than LARGE_DUMP bytes, about 1,000 memo entries at most, is replaced by a new one."""

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
        self._written += _written_size(data)
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


# Held while a runner makes its run directory, which threads that ask for file paths may do at once.
_OUTPUT_DIRECTORIES = threading.Lock()


class _Record:
    """What an Executor records about a live task: one object for each task instead of an entry in each of a dozen
    tables, which the C node id leads to.

    The thread that submits the task sets the fields, and the background thread sets node, result, the C file ids, pin,
    and kept. finished is published under the runner's lock."""

    __slots__ = ("graph", "key", "number", "func", "command", "outputs", "staged", "node", "result", "files",
                 "selections", "selection_files", "finished", "abandoned", "held", "keep", "pin", "kept", "store")

    def __init__(self, graph, key):
        self.graph = graph  # weak reference to the task's Graph, which leads to the Task by key
        self.key = key  # the task's key in its Graph
        # The task's number among the tasks of its Executor, which names it in messages, its files in sandboxes, and
        # its data in the runner's tables
        self.number = 0
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
        self.held = False  # the result is pinned and fetched for reading
        self.keep = 0  # why the result is pinned against release (_SINK, _FINAL), set by the submitting side
        self.pin = 0  # pins that the record holds on its result in C
        self.kept = False  # C keeps the node, which ran again after its outputs were released
        # (key, directory of its entry or None) of a task of cache=True that has a key in the store, or None
        self.store = None

    @property
    def label(self):
        """How messages name the task. It is built when needed, since only messages use it."""
        return _name_task(self.number, self.func)


def _task_of(record):
    """Return the Task of a record, or None when the task or its Graph was collected."""
    graph = record.graph()
    return None if graph is None else graph._tasks.get(record.key)


def _references(task):
    """Return {(graph id, key): Task} of the tasks a task reads, by the references that its arguments hold."""
    graph = task._graph
    return {(graph._graph_id(parent), parent.key): parent for parent in task._parents}


class _Runner:
    """What both runners share: their lifetime and run directory, the events they publish, how sandboxes name files,
    and how a closed runner explains itself."""

    # The port that another Executor took, which closed this runner, or None.
    _closed_for_port = None
    # Why the run stopped as a whole, which only a runner on Workers sets.
    _run_error = None

    def __init__(self, output_dir, shared, store):
        self._closed = False
        self._store = store
        self._shared = _serialize_shared(shared)  # name -> serialized shared object
        self._last_number = 0  # the number of the latest file or staged object, which names it in sandboxes
        self._file_numbers = {}  # file identity -> number that names the file in sandboxes
        # The lock of the state that the runner's thread shares with the threads that use the Executor, whose condition
        # the runner's thread notifies when it publishes changes.
        self._lock = _Lock()
        self._changed = threading.Condition(self._lock)
        # (record, entry) of tasks that finished, published for the Executor, which removes each once it applied it
        self._events = collections.deque()
        self._unfinished = 0  # tasks submitted that did not finish
        # Records of tasks that nothing can ask for anymore: submitted tasks that were collected, and the tasks of
        # collected Graphs. Any thread appends, and the runner's thread lets them go.
        self._dropped = collections.deque()
        self._keep_paths = set()  # local paths that TaskFile.path returned, which stay after close
        self._pickler = _Pickler()  # of the thread that creates the runner, which serializes submitted tasks
        # The output directory and the run directory in it, which holds the file paths that TaskFile.path returns, are
        # made when the first path is asked for (_output_directory()).
        self._output_root = _graph_module._absolute(output_dir)
        self._created_output_root = False
        self._run_dir = None

    @property
    def closed(self):
        return self._closed

    def close(self):
        """Cancel unfinished tasks and release every task result. File paths that TaskFile.path returned stay in
        output_dir. Closing a closed runner does nothing."""
        self._close(False)

    def discard(self):
        """Close, and remove the run directory with the file paths that TaskFile.path returned."""
        self._close(True)

    def _check_open(self):
        if self._closed:
            raise RuntimeError(f"the Executor is closed{self.closed_because()}")

    def _check_running(self):
        """Raise when the runner closed, or when the run stopped as a whole. Called with the lock held."""
        self._check_open()
        if self._run_error is not None:
            raise RuntimeError(f"vine_graph execution stopped: {self._run_error}")

    def _remove_run_dir(self, remove_all):
        """Remove the run directory at close, keeping the paths that TaskFile.path returned unless remove_all is set,
        and the output directory when this runner made it and it is empty."""
        if self._run_dir is None:
            return
        _remove_run_data(self._run_dir, self._keep_paths, remove_all or not self._keep_paths)
        self._run_dir = None
        if self._created_output_root:
            # Removes it only when empty, and it may be gone already.
            try:
                os.rmdir(self._output_root)
            except OSError:
                pass

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

    def take_events(self, timeout=0):
        """Return the queue of the (record, entry) of tasks that finished and were not applied yet, waiting up to timeout
        seconds for one. A run that stopped as a whole raises its error."""
        deadline = time.monotonic() + timeout
        with self._lock:
            while not self._events:
                self._check_running()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._changed.wait(min(remaining, 0.5))
            return self._events

    def drain(self):
        """Wait until every task that was submitted finished."""
        self._wait_until(lambda: self._unfinished <= 0, None, "tasks did not finish")

    def _wait_until(self, ready, timeout, unfinished):
        """Wait until ready() holds, checking it whenever the runner publishes changes."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            while not ready():
                self._check_running()
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError(unfinished)
                self._changed.wait(0.5 if remaining is None else min(remaining, 0.5))

    def closed_because(self):
        """Return the end of a message about this closed runner: why it closed, when another Executor closed it."""
        if self._closed_for_port is None:
            return ""
        return f", because another Executor was created on port {self._closed_for_port}"

    def _output_directory(self):
        """Return the run directory of this runner in the output directory, which is made, with the output directory,
        when something is first written there. An output directory that this runner made is removed again when it
        holds nothing at close."""
        with _OUTPUT_DIRECTORIES:
            if self._run_dir is None:
                self._check_open()
                if not os.path.isdir(self._output_root):
                    os.makedirs(self._output_root, exist_ok=True)
                    self._created_output_root = True
                self._run_dir = tempfile.mkdtemp(prefix="vine-graph-run-", dir=self._output_root)
            return self._run_dir

    def _look_up(self, task, record):
        """Give a task of cache=True its key in the store and its entry there, if any (_Record.store). A task without a
        key runs as usual, and the store says why once per function."""
        func = task._func
        if not getattr(func, "_cache", False):
            return
        store = self._store
        parents = {ref: parent._record for ref, parent in _references(task).items()} if task._parents else {}
        try:
            key = store.key(task, parents)
        except _store_module.NotCacheable as error:
            store.notice(func, str(error))
            return
        entry = store.lookup(key, record.outputs)
        if entry is not None and record.selections:
            store.add_selections(entry, record.selections)
        record.store = (key, entry)

    def _file_task_path(self, identity, name):
        """Return a numbered name of a file: a number of its own followed by the end of its name, which holds its
        extension, as much of it as fits in the longest file name a file system takes."""
        if identity not in self._file_numbers:
            self._last_number += 1
            self._file_numbers[identity] = self._last_number
        prefix = f"vine-graph-file-{self._file_numbers[identity]}-"
        excess = len(os.fsencode(prefix + name)) - _NAME_MAX
        start = 0
        while excess > 0:
            excess -= len(os.fsencode(name[start]))
            start += 1
        return prefix + name[start:]

    def _file_task_paths(self, task, identities):
        """Return {file identity: sandbox name} of the files a task reads, given by identity. A File with a name takes
        it. Any other file takes its own name, its basename, when no other input of the task has that name and the name
        is not reserved for the runtime, not a file the task writes, and does not start with "-", so a tool that looks
        for an index file next to its data, such as x.bam.bai next to x.bam, finds it. Every other file, every member of
        a group of files with one name included, takes a numbered name, so the outcome does not depend on the order of
        the arguments. A name that a File was given and that the task writes raises ValueError."""
        outputs = task._outputs or ()
        chosen = []
        counts = {}
        for identity in identities:
            if identity[0] == "input" or identity[0] == "url":
                explicit = identity[2] is not None
                if explicit:
                    name = identity[2]
                else:
                    name = os.path.basename(identity[1]) if identity[0] == "input" else URL(identity[1]).basename
            else:
                explicit = False
                name = os.path.basename(identity[3])
            chosen.append((identity, name, explicit))
            counts[name] = counts.get(name, 0) + 1
        names = {}
        for identity, name, explicit in chosen:
            if explicit:
                if counts[name] > 1 and any(other[2] and other[1] == name and other[0] != identity for other in chosen):
                    raise ValueError(f"{task!r} reads two files named {name!r}")
                if _clashes_with_output(name, outputs):
                    raise ValueError(f"{task!r} reads the file {identity[1]} under the name {name!r} of a file it "
                                     f"writes")
                names[identity] = name
            elif not name or counts[name] > 1 or name.startswith("-") or reserved_name(name) or \
                    (outputs and _clashes_with_output(name, outputs)):
                names[identity] = self._file_task_path(identity, name)
            else:
                names[identity] = name
        return names

    def _command_line(self, task, python):
        """Return the shell command line of a Command task and what it mounts: (sandbox path of each result it reads,
        record), and (file identity, sandbox path, record of the task that writes it or None) of each file.
        Placeholders take the shell-quoted path of a file in the sandbox, the text of a task result, which the shell
        reads into a variable first, without its trailing newlines, and str() of any other value. A list or tuple
        renders each element so, separated by spaces. The command's standard output goes to the result file, which
        keeps at most COMMAND_RESULT_LIMIT bytes, and a command that exits with status zero fails when it did not write
        a file it declared."""
        command = task._func
        kwargs = task._kwargs
        fields = command._fields
        parents = _references(task) if task._parents else None
        # The names of the files depend on all the files of the task, so they are found first.
        identities = {}
        for name in fields:
            value = kwargs[name]
            for element in (value if type(value) is list or type(value) is tuple else (value,)):
                kind = type(element)
                if kind is File or kind is _FileRef or kind is URL:
                    identities.setdefault(library.file_identity(element), None)
        names = self._file_task_paths(task, identities) if identities else None
        prelude = []
        variables = {}  # number of a record whose result the task reads -> the shell variable of its text
        results = {}  # sandbox path of a result the task reads -> its record
        files = {}  # identity -> (identity, sandbox path, record of the task that writes it or None)

        def render(value):
            kind = type(value)
            if kind is _Ref:
                record = parents[(value.graph_id, value.key)]._record
                variable = variables.get(record.number)
                if variable is None:
                    path = f"vine-graph-result-{record.number}"
                    variable = variables[record.number] = f"vine_graph_value_{len(prelude) + 1}"
                    if record.command:
                        read = f"cat {path}"
                    else:
                        read = f"{python} -c {shlex.quote(_STR_RESULT)} {path}"
                    prelude.append(f"{variable}=$({read}) || {{ echo {shlex.quote(f'vine_graph: could not read the result of task {record.label}')} >&2; exit 1; }}")
                    results[path] = record
                return f'"${variable}"'
            if kind is File or kind is _FileRef or kind is URL:
                identity = library.file_identity(value)
                path = names[identity]
                if identity not in files:
                    source = parents[(value.graph_id, value.key)]._record if kind is _FileRef else None
                    files[identity] = (identity, path, source)
                return shlex.quote(path)
            return shlex.quote(str(value))

        values = {}
        for name in fields:
            value = kwargs[name]
            if type(value) is list or type(value) is tuple:
                values[name] = " ".join(render(element) for element in value)
            else:
                values[name] = render(value)
        results = list(results.items())
        files = list(files.values())
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


# What separates the kinds of the earlier failures of a task that was tried again from the error of its last attempt.
_HISTORY_END = "; the last time "


def _load_result(record, fetched):
    """Load a task result: the text of a command's standard output, or the object a Python task returned, from its
    bytes, which the Manager held in memory, or from a path."""
    if record.command:
        if type(fetched) is bytes:
            return fetched[:COMMAND_RESULT_LIMIT].decode("utf-8", "replace")
        return library.load_text_result(fetched)
    try:
        if type(fetched) is bytes:
            return cloudpickle.loads(fetched)["Result"]
        return library.load_result(fetched)
    except Exception as error:
        # A result that cannot be loaded in this process, such as an object of a module this process cannot import,
        # raises an error that names the task instead of the error of pickle.
        raise TaskError(f"task {record.label} completed, but its result cannot be loaded: {type(error).__name__}: "
                        f"{error}", None, None, "result") from error


def _read_file(local, path, limit):
    """Return the bytes of a local file that a task wrote at path, or raise ValueError above limit bytes."""
    if os.path.isdir(local):
        raise IsADirectoryError(errno.EISDIR, "the task wrote a directory, which has no content", path)
    if os.path.getsize(local) > limit:
        raise ValueError(_too_large(path, limit))
    with open(local, "rb") as stream:
        return stream.read()


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
    """Raise an error that says why a File cannot be read by tasks: it does not exist."""
    if not os.path.exists(path):
        raise FileNotFoundError(errno.ENOENT, "a File must name an existing file or directory", path)


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
    their mask, since only the main thread receives KeyboardInterrupt.

    Python raises an interrupt when a Python function starts and after a built-in function returns. The returned call
    is a built-in partial of the built-in _signal function, which a finally block calls without starting a Python
    function. It is made before SIGINT is blocked, so nothing that could raise runs between blocking and returning,
    and an interrupt that was pending when SIGINT was blocked restores the mask here."""
    if threading.get_ident() != _MAIN_THREAD:
        return _KEEP_INTERRUPTS
    restore = functools.partial(_signal.pthread_sigmask, _signal.SIG_SETMASK, _signal.pthread_sigmask(_signal.SIG_BLOCK, ()))
    try:
        _signal.pthread_sigmask(_signal.SIG_BLOCK, _INTERRUPT)
    except BaseException:
        restore()
        raise
    return restore
