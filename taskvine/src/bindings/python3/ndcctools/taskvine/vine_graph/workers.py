# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""The runner of an Executor on TaskVine Workers (_Workers). It owns a Manager, the C graph executor, and the library
that runs Python tasks on Workers.

A background thread owns the C executor: it is the only thread that calls into C after the runner starts, apart from
vine_graph_executor_wake(), which ends the thread's wait in C when a call is queued for it. The thread that submits a
task stages it and queues the C calls for the background thread. The background thread runs queued calls, advances the
run, and publishes what finished and which fetched results became readable, which the Executor applies to the Tasks
under its lock."""

import atexit
import collections
import contextlib
import functools
import operator
import os
import queue
import re
import shutil
import subprocess
import _signal
import sys
import tempfile
import threading
import time
import types
import weakref
from contextlib import ExitStack
from pathlib import Path

import cloudpickle

from ndcctools.taskvine.manager import Manager

from . import install, library, store as _store_module
from .graph import (
    _LEAF_TYPE_SET, Command, File, Function, Shared, TaskError, URL, _DEFAULT_OPTIONS, _FAILED, _FileRef, _Ref,
    _callable, _link_or_copy, _place, _readers_of,
)
from .runner import (
    COMMAND_ERROR_TAIL, EDATA_CACHE, EXECUTOR_SETTINGS, EXECUTOR_TUNING, MANAGER_TUNING, _COMPLETED_ENTRY, _INTERRUPT,
    _Record, _Runner, _block_interrupts, _capi, _check_params, _check_task, _load_result,
    _manager_setting, _read_file, _references, _task_of, _too_large, _written_size, capi, cvine,
)

# TaskError.reason of each kind of failure that C reports. A command that fails as a function does reads "command".
_FAILURE_REASONS = {
    capi.VINE_GRAPH_FAILURE_FUNCTION: "function",
    capi.VINE_GRAPH_FAILURE_SIGNAL: "signal",
    capi.VINE_GRAPH_FAILURE_WORKERS: "workers",
    capi.VINE_GRAPH_FAILURE_INPUT: "input",
    capi.VINE_GRAPH_FAILURE_RETRIES: "retries",
    capi.VINE_GRAPH_FAILURE_CANCELLED: "cancelled",
    capi.VINE_GRAPH_FAILURE_SUBMIT: "submit",
    capi.VINE_GRAPH_FAILURE_RESOURCES: "resources",
    capi.VINE_GRAPH_FAILURE_TIMEOUT: "timeout",
}

# The shell command line of a Python task that runs as a standard task, as a task of a Function with GPUs does
# (library.run_standalone()). The shell does not exec Python, so a Python killed by signal N exits with 128 + N, which C
# tells apart from an exit status.
_STANDALONE_LINE = "python -c 'from ndcctools.taskvine.vine_graph import library; library.run_standalone()'; exit $?"

# Seconds the background thread spends on queued calls before it advances the run one step.
COMMAND_SECONDS = 0.05

# Seconds per step that the background thread spends at least on clean-up: freeing staged files and removing the C
# nodes of tasks that were let go.
CLEANUP_SECONDS = 0.01

# The name of the log of a library instance that the Manager keeps under watch-library-logfiles, with its task id.
LIBRARY_LOG_NAME = re.compile(r"library-task-(\d+)\.log")

# Dropped C nodes removed per call into C, between which clean-up checks its time.
REMOVAL_BATCH = 128

# Bytes up to which a serialized function or argument travels inside the manifest of its task. Larger objects become
# staged files, which every task that passes the same object shares. A file takes at least one 4 KiB page on disk, so a
# smaller object is never cheaper as a file.
INLINE_LIMIT = 4096

# Small objects whose serialization tasks share by identity, like staged files: functions and classes. Every other small
# object is serialized for each task, so a mutable argument changed between runs reaches later tasks.
_SHARED_INLINE_TYPES = (types.FunctionType, type)

# Arguments that stand for data the task reads, which the manifest holds as they are. A reference to a whole result, the
# most common argument, is held as its task (graph id, key), a tuple that the pickler writes without a reducer.
_REFERENCE_TYPES = (_Ref, _FileRef, File, URL, Shared)

# Immutable values that the manifest holds as they are: numbers, None, and strings. Bytes are serialized, so the library
# tells an object it loads from a value it takes as it is.
_LITERAL_TYPES = frozenset((int, float, bool, complex, type(None), str))


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
            size = _written_size(data)
            if len(self.buffer) + size <= self.limit:
                self.buffer += data
                return size
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


class _Submission(list):
    """A queued call that submits a task in the background thread: the list of what it declares and mounts
    (_submit_node()), one object for the garbage collector instead of a partial, a tuple, and a list."""

    __slots__ = ("executor",)

    def __call__(self):
        self.executor._submit_node(self)


# The C file id of a task's result, from its _Record, for map() over the tasks whose results a task mounts.
_RESULT_FILE = operator.attrgetter("result")


class _Workers(_Runner):
    """The runner of an Executor on TaskVine Workers that connect to port, or to the Manager named name through the
    catalog. It owns a Manager, the C graph executor, and the library that runs Python tasks on each Worker, which
    imports library_modules, loads shared and, within edata_cache bytes, the smallest function and argument objects of
    the first submission when it starts. Command tasks run as standard TaskVine tasks. The other arguments are those of
    the Executor."""

    def __init__(self, port=cvine.VINE_DEFAULT_PORT, name=None, *, output_dir="./outputs", library_modules=(),
                 library_files=None, shared=None, edata_cache=0, ssl=None, staging_path=None,
                 run_info_path="vine-run-info", run_info_template=None, store=None):
        super().__init__(output_dir, shared, store)
        # Outputs of tasks of cache=True that missed the store, pinned until the store thread wrote them: number of the
        # record -> (record, {C file id: name in the entry}, {name: bytes or local path} fetched so far, fetching), and
        # C file id -> number of a record whose output it is; background thread only
        self._store_pending = {}
        self._store_fetches = {}
        self._store_queue = None
        self._store_thread = None
        if store is not None:
            self._store_queue = queue.SimpleQueue()
            self._store_thread = threading.Thread(target=_write_entries, args=(store, self._store_queue,
                                                                               weakref.ref(self)),
                                                  name="vine-graph-store", daemon=True)
            self._store_thread.start()
        self._manager = None
        self._executor = None  # opaque C executor handle
        self._resources = ExitStack()
        library_modules = [install.module_name(module) for module in library_modules]
        self._edata_cache = edata_cache
        # Until the first submission decides the cached edata, the library waits to be installed, and calls into C that
        # are queued meanwhile are held, so no task reaches C before the library caches its edata.
        self._library_pending = bool(edata_cache)
        self._held = None
        self._cached_edata = frozenset()  # sandbox names of staged objects that the library loads

        # State the background thread owns: the C ids of submitted tasks and files.
        self._records = {}  # C node id -> _Record
        self._input_files = {}  # absolute path of a File -> C file id
        self._url_files = {}  # URL -> C file id
        self._edata_files = {}  # sandbox name of a staged object -> C file id

        # State both threads share, guarded by the lock.
        self._commands = collections.deque()  # calls into C, run in order by the background thread
        # Calls ever queued and calls the background thread ran. Clean-up work waits for the calls queued before it.
        self._queued = 0
        self._ran = 0
        # Clean-up work: (calls queued before it, function, argument), in that order. The background thread calls
        # function(self, argument) once every call queued before the work ran.
        self._deferred = collections.deque()
        # Whether a caller woke the background thread's wait in C since the thread last found no queued call.
        self._wake_pending = False
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
        self._capacity = None
        self._capacity_read = 0.0
        # Options of the tasks that waited for a larger Worker, each printed once (_notice_unfit())
        self._unfit_sizes = set()
        # _Options -> the id of its profile in the C executor, one per distinct value; background thread only
        self._profiles = {}
        # (C file id, sandbox name) of what Python tasks that run as standard tasks load at start, declared for the
        # first of them (_standalone_inputs()), and the cached edata that the library loads; background thread only
        self._standalone = None
        self._edata_cache_path = None
        self._driver = None
        self._driver_ident = None
        self._work_dir = None  # what the runner writes for itself, in the Manager's staging directory
        self._edata_dir = None
        try:
            self._close_executor_on(port)
            self._start(port, name, library_modules, library_files, ssl, staging_path, run_info_path,
                        run_info_template)
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
                other._closed_for_port = port
                other.close()

    def _start(self, port, name, library_modules, library_files, ssl, staging_path, run_info_path, run_info_template):
        if run_info_path and run_info_template:
            _remove_files(os.path.join(run_info_path, run_info_template))
        self._manager = Manager(port=port, name=name, ssl=ssl, staging_path=staging_path, run_info_path=run_info_path,
                                run_info_template=run_info_template)
        for setting, value in MANAGER_TUNING.items():
            self._tune_manager(setting, value)
        self._work_dir = self._resources.enter_context(
            tempfile.TemporaryDirectory(prefix="vine-graph-work-", dir=self._manager.staging_directory))
        shared_path = None
        if self._shared:
            shared_path = os.path.join(self._work_dir, library.SHARED)
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
        self._edata_cache_path = edata_path
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
        path = os.path.join(self._work_dir, library.EDATA_CACHE)
        install.save_objects(path, chosen)
        self._cached_edata = frozenset(chosen)
        # Only the library file holds cached objects, so their staged files go now, and the vault never sees them.
        with self._lock:
            for name in chosen:
                os.unlink(self._edata_names[name][1])
        return path

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
        if self._store_thread is not None:
            # Entries being written read the vault, which goes with the C executor.
            self._store_queue.put(None)
            self._store_thread.join()
            self._store_thread = None
            print(self._store.summary(), file=sys.stderr, flush=True)
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
            self._remove_run_dir(failed)

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

    def submit(self, task, record, commit):
        """Stage one task and queue its submission for the background thread under its record, whose keep says why its
        result is pinned from the start. commit(task, record) makes the task live right after it was queued, with
        interrupts held back. A staging error, such as an argument that cannot be serialized, raises before anything is
        queued."""
        self._check_open()
        _check_task(task, self._shared)
        if not record.command and task._readers:
            record.selections = _selections_read_from(task) or None
        if self._store is not None:
            self._look_up(task, record)
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
                    commit(task, record)
            finally:
                restore_interrupts()
        finally:
            if not submitted:
                with self._lock:
                    record.staged = None
                    self._drop_staged(names)

    def _plan(self, task, record, names):
        """Stage the arguments and manifest of a task, or build its command line, and return what the background
        thread declares and mounts. The sandbox name of every staged file the task holds is added to names."""
        graph = task._graph
        if record.command:
            line, results, files = self._command_line(task, "python3")
            result_records = [parent for _, parent in results]
            result_paths = [path for path, _ in results]
            return (record, record.outputs, (), result_records, result_paths, (), files, (), None, line)
        func, args, kwargs = _callable(task._func), task._args, task._kwargs
        files = {}
        mounts = []  # (file identity, sandbox path, record of the task that writes the file or None)
        if task._reads:
            parents = _references(task)
            identities = {library.file_identity(ref): ref for ref in task._reads}
            sandbox_names = self._file_task_paths(task, identities)
            for identity, ref in identities.items():
                files[identity] = path = sandbox_names[identity]
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
            # The files that the function writes, which the library checks for after the call.
            "outputs": record.outputs,
        }
        options = task._func._options if type(task._func) is Function else _DEFAULT_OPTIONS
        if options.retries:
            # The exceptions after which the library tells C that the task may be tried again.
            manifest["retry_on"] = options.retry_on
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
        when it is small, or else the sandbox name of its staged file, which is added to edata. A staged file, or the
        bytes of a small function or class, that an earlier task still holds is reused, and its key, a sandbox name or
        the object's id, is added to names, which holds it for the task."""
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
            if identity[0] == "url":
                file = self._url_files.get(identity[1])
                if file is None:
                    file = capi.vine_graph_executor_declare_url(executor, identity[1])
                    if not file:
                        raise RuntimeError(f"failed to declare the URL {identity[1]}")
                    self._url_files[identity[1]] = file
            elif source is None:
                if identity[1] not in self._input_files:
                    self._input_files[identity[1]] = self._declare_file(self._link_input(identity[1]))
                file = self._input_files[identity[1]]
            else:
                file = source.files[identity[3]]
            self._add_input(node, file, name)
        kind = type(record.func)
        standalone = False
        if kind is Function or kind is Command:
            options = record.func._options
            standalone = kind is Function and options.gpus > 0
            if options is not _DEFAULT_OPTIONS:
                if _capi.vine_graph_executor_set_profile(executor, node, self._profile(options)) != 0:
                    raise RuntimeError(f"failed to set what task {key!r} requests")
        if command is not None:
            if capi.vine_graph_executor_set_command(executor, node, command) != 0:
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
            if standalone:
                # The task loads what the library loads when it starts, from files of the same names.
                for file, name in self._standalone_inputs():
                    self._add_input(node, file, name)
                if capi.vine_graph_executor_set_standalone(executor, node, _STANDALONE_LINE) != 0:
                    raise RuntimeError(f"failed to make task {key!r} a standard task")
        if record.store is not None and record.store[1] is not None and self._complete_from_store(record):
            return
        if record.store is not None:
            # A miss keeps its outputs until they are in the store.
            outputs = _store_outputs(record)
            for file in outputs:
                if _capi.vine_graph_executor_pin_file(executor, file) != 0:
                    raise RuntimeError(f"failed to pin an output of task {key!r}")
            self._store_pending[record.number] = (record, outputs, {}, False)
        if capi.vine_graph_executor_submit_node(executor, node) != 0:
            raise RuntimeError(f"failed to submit task {key!r}")

    def _complete_from_store(self, record):
        """Complete the node of a task that hit the store with the entry's files as its outputs, which the vault adopts
        as their checkpoints, and return True, or False when the vault takes none of them, and the task runs."""
        entry = record.store[1]
        executor = self._executor
        for file, name in _store_outputs(record).items():
            if capi.vine_graph_executor_adopt_output(executor, file, os.path.join(entry, name)) != 0:
                return False
        return capi.vine_graph_executor_complete_node(executor, record.node) == 0

    def _fetch_for_store(self, record):
        """Fetch the outputs of a completed task that missed the store, which the store thread writes once all came."""
        pending = self._store_pending.get(record.number)
        if pending is None or pending[3]:
            return
        self._store_pending[record.number] = pending[:3] + (True,)
        for file in pending[1]:
            self._store_fetches[file] = record.number
            if _capi.vine_graph_executor_fetch_file(self._executor, file) != 0:
                self._release_for_store(record)
                return

    def _release_for_store(self, record):
        """Unpin the outputs that a task kept for the store, which is done with them or never gets them."""
        pending = self._store_pending.pop(record.number, None)
        if pending is None:
            return
        for file in pending[1]:
            self._store_fetches.pop(file, None)
            _capi.vine_graph_executor_unpin_file(self._executor, file)

    def _stored(self, record):
        """Called by the store thread once it wrote, or failed to write, the entry of a record."""
        self._enqueue(functools.partial(self._release_for_store, record))

    def _standalone_inputs(self):
        """Return (C file id, sandbox name) of the files that a Python task that runs as a standard task loads at start,
        declared once: what library.run_standalone() prepares from, the shared objects, the cached edata, and the
        library files under their library names."""
        if self._standalone is None:
            modules, files, shared_path = self._library_options
            edata_path = self._edata_cache_path
            context = os.path.join(self._work_dir, library.STANDALONE)
            with open(context, "wb") as stream:
                cloudpickle.dump([modules, library.SHARED if shared_path else None,
                                  library.EDATA_CACHE if edata_path else None, library.runtime_identity()], stream)
            inputs = [(self._declare_file(context, vault=True), library.STANDALONE)]
            if shared_path:
                inputs.append((self._declare_file(shared_path, vault=True), library.SHARED))
            if edata_path:
                inputs.append((self._declare_file(edata_path, vault=True), library.EDATA_CACHE))
            for local, remote in (files or {}).items():
                inputs.append((self._declare_file(local), remote))
            self._standalone = inputs
        return self._standalone

    def _profile(self, options):
        """Return the id of the C profile of options, which the first task of a distinct value adds."""
        profile = self._profiles.get(options)
        if profile is None:
            spec = capi.vine_graph_profile()
            spec.cores = options.cores
            spec.memory = options.memory or 0
            spec.gpus = options.gpus
            spec.time_limit = options.time_limit or 0
            spec.retries = options.retries
            spec.retry = _retry_bits(options)
            profile = capi.vine_graph_executor_add_profile(self._executor, spec)
            if profile < 0:
                raise RuntimeError(f"failed to add the profile {options!r}")
            self._profiles[options] = profile
        return profile

    def _cancel_nodes(self, records):
        cancel = _capi.vine_graph_executor_cancel_node
        for record in records:
            if record.node:
                cancel(self._executor, record.node)
        # The tasks and their dependents are finished when cancel_node() returns.
        self._publish_finished()

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
        if record.number in self._store_pending:
            self._release_for_store(record)
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
                    record = self._records.pop(node, None)
                    if record is not None and record.finished is None:
                        # C does not report a node as finished that it removed first, as a task that was let go
                        # while it ran. It finished all the same, which drain() has to learn.
                        with self._lock:
                            record.finished = _COMPLETED_ENTRY
                            self._unfinished -= 1
                            self._changed.notify_all()
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
        if error and self._library_name in error:
            error = self._with_library_log(error)
        self._publish_finished()
        while node := _capi.vine_graph_executor_next_unfit(self._executor):
            record = self._records.get(node)
            if record is not None and not record.abandoned:
                self._notice_unfit(record)
        capacity = None
        if self._capacity is not None and monotonic() - self._capacity_read >= 1:
            capacity = self._read_capacity()
        fetched = None
        fetches = self._fetches
        while file := capi.vine_graph_executor_next_fetched(self._executor):
            if self._store_fetches:
                self._store_fetched(file)
            key, record = fetches.pop(file, (None, None))
            if record is None or record.abandoned:
                continue
            path = self._local_data(file)
            if path is not None:
                if fetched is None:
                    fetched = {}
                fetched[key] = path
        unsendable = []
        while file := capi.vine_graph_executor_next_fetch_failed(self._executor):
            number = self._store_fetches.get(file)
            if number is not None and number in self._store_pending:
                self._release_for_store(self._store_pending[number][0])
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

    def _store_fetched(self, file):
        """Note an output of a task that missed the store that reached the Manager, and hand the task's outputs to the
        store thread once all came."""
        number = self._store_fetches.pop(file, None)
        pending = self._store_pending.get(number) if number is not None else None
        if pending is None:
            return
        record, outputs, items, _ = pending
        items[outputs[file]] = self._local_data(file)
        if len(items) == len(outputs):
            self._store_queue.put((record, list(items.items())))

    def _local_data(self, file):
        """Return the bytes of a fetched output that the Manager holds in memory, so its vault entry is never written to
        disk, or else its local path, or None."""
        data = capi.vine_graph_executor_get_local_data(self._executor, file)
        return data if data is not None else capi.vine_graph_executor_get_local_path(self._executor, file)

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
                reason = _FAILURE_REASONS.get(capi.vine_graph_executor_get_node_failure(executor, node))
                if reason == "function" and source.command:
                    reason = "command"
                if source is record:
                    exit_code = capi.vine_graph_executor_get_node_exit_code(executor, node)
                    message = capi.vine_graph_executor_get_node_error(executor, node)
                    if reason == "function" and message and library.START_FAILURE_MARK in message:
                        # The task ran in a library that could not start, which no task can change, so the run stops
                        # as a whole with what the Worker reported, and this task and those after it are not published.
                        self._stop_for_library(message)
                        break
                    entry = (_FAILED, record, message, reason, None if exit_code < 0 else exit_code)
                else:
                    # A dependent names its source and the source's reason, and reads the message from the source.
                    entry = (_FAILED, source, None, reason, None)
            else:
                entry = _COMPLETED_ENTRY
            if record.number in self._store_pending:
                if entry is _COMPLETED_ENTRY:
                    self._fetch_for_store(record)
                else:
                    self._release_for_store(record)
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

    def _notice_unfit(self, record):
        """Print once for each kind of request that a task waits because it requests more cores, memory, or GPUs than
        the largest connected Worker has, naming what does not fit, which a larger Worker that joins the pool later can
        run."""
        options = record.func._options
        if options in self._unfit_sizes:
            return
        self._unfit_sizes.add(options)
        capacity = capi.vine_graph_executor_get_capacity(self._executor)
        parts = []
        if options.cores > capacity.largest:
            parts.append(f"requests {options.cores} cores; the largest connected Worker has {capacity.largest}")
        if options.memory is not None and options.memory > capacity.largest_memory:
            parts.append(f"requests {options.memory} MB of memory; the largest connected Worker has "
                         f"{capacity.largest_memory} MB")
        if options.gpus > capacity.largest_gpus:
            parts.append(f"requests {options.gpus} GPUs; the largest connected Worker has {capacity.largest_gpus}")
        if not parts:
            # A larger Worker connected since C looked.
            parts.append("requests more than the largest connected Worker had")
        print(f"vine_graph: {record.label} {'; '.join(parts)}; it waits for a larger Worker", file=sys.stderr,
              flush=True)

    def _with_library_log(self, error):
        """Return the error of a library that TaskVine removed because it kept dying, followed by the end of the log of
        its last instance, which says why it died, when the Manager keeps library logs (watch-library-logfiles)."""
        directory = os.path.join(self._manager.runtime_directory, "library-logs")
        try:
            names = [name for name in os.listdir(directory) if LIBRARY_LOG_NAME.fullmatch(name)]
        except OSError:
            return error
        if not names:
            return error
        path = os.path.join(directory, max(names, key=lambda name: int(LIBRARY_LOG_NAME.fullmatch(name)[1])))
        try:
            with open(path, "rb") as stream:
                stream.seek(0, os.SEEK_END)
                stream.seek(max(stream.tell() - COMMAND_ERROR_TAIL, 0))
                log = stream.read().decode("utf-8", "replace").strip()
        except OSError:
            return error
        return f"{error}\nThe log of its last instance ({path}) ends with:\n{log}"

    def _stop_for_library(self, message):
        """Stop the run as a whole because the library could not start on a Worker, with the error that the Worker
        reported."""
        host, report = library.start_failure(message)
        error = (f"the library that runs Python tasks could not start on the Worker at {host}:\n{report}\n"
                 f"Workers need the Python environment of this program, and reach its local modules and files only "
                 f"through library_files.")
        with self._lock:
            if self._run_error is None:
                self._run_error = error
            self._changed.notify_all()

    # Published state, read by the threads that use the Executor.

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
        data = fetched if isinstance(fetched, bytes) else _read_file(fetched, path, limit)
        if len(data) > limit:
            raise ValueError(_too_large(path, limit))
        return data

    def file_path(self, record, path):
        """Return a local path of a file that a completed task wrote, linked or copied out of the Manager's vault."""
        # The number of the record tells the file apart from those of the other Graphs that the Executor runs.
        identity = ("fetched", record.number, path)
        local = os.path.join(self._output_directory(), self._file_task_path(identity, os.path.basename(path)))
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

    def save_file(self, record, path, destination):
        """Put a file that a completed task wrote at destination, a local path."""
        _place(self.fetched(record, path), destination)

    def capacity(self):
        """Return (Workers, cores, MB of memory, GPUs) connected, which the background thread reads about once a
        second."""
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
        return (capacity.workers, capacity.cores, capacity.memory, capacity.gpus)

    def stats(self):
        """Return the makespan in seconds, the number of recovery tasks, and how far durability lags behind
        completions (see vine_graph_executor_get_durability())."""
        self._check_open()
        return self._call(lambda: {
            "makespan": capi.vine_graph_executor_get_makespan_us(self._executor) / 1e6,
            "recovery_tasks": capi.vine_graph_executor_get_completed_recovery_tasks(self._executor),
            "durability": _durability(capi.vine_graph_executor_get_durability(self._executor)),
        })

    def _link_input(self, source):
        """Give this runner its own cache identity for a File without copying its bytes, a link of its own, so late
        cache-update messages of a closed runner never satisfy the declaration of the next."""
        directory = Path(self._work_dir) / "inputs"
        directory.mkdir(exist_ok=True)
        path = directory / str(len(self._input_files) + 1)
        path.symlink_to(source)
        return str(path)


def _retry_bits(options):
    """Return the VINE_GRAPH_RETRY_* bits of the failures that the retries of options cover, each seen as the exception
    that stands for it."""
    bits = 0
    if options.retries_cover(TimeoutError):
        bits |= capi.VINE_GRAPH_RETRY_TIMEOUT
    if options.retries_cover(MemoryError):
        bits |= capi.VINE_GRAPH_RETRY_RESOURCES
    if options.retries_cover(subprocess.CalledProcessError):
        bits |= capi.VINE_GRAPH_RETRY_COMMAND
    return bits


def _store_outputs(record):
    """Return {C file id: name in a store entry} of the outputs of a task of cache=True: its result, the selections
    that its readers mount, and its declared files."""
    outputs = {record.result: _store_module.RESULT}
    for chain, file in (record.selection_files or {}).items():
        outputs[file] = _store_module.Store.selection_name(chain)
    for path, file in (record.files or {}).items():
        outputs[file] = os.path.join(_store_module.FILES, path)
    return outputs


def _lost(record):
    """Return whether C reported a submitted task as failed, such as a completed one whose output cannot be recreated."""
    finished = record.finished
    return finished is not None and finished[0] == _FAILED


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


def _durability(durability):
    """Return the fields of a C vine_graph_durability as a dictionary."""
    return {"exposed": durability.exposed, "pending": durability.pending, "output_rate": durability.output_rate,
            "lag": durability.lag, "large_rate": durability.large_rate, "large_capacity": durability.large_capacity,
            "cut": durability.cut}


def _write_entries(store, entries, runner_reference):
    """Write the entries of tasks that missed the store, in the store thread, until None arrives, and tell the runner
    when each is done so it unpins the outputs."""
    while True:
        item = entries.get()
        if item is None:
            return
        record, items = item
        try:
            store.write(record.store[0], items)
        except Exception as error:
            print(f"vine_graph: the store could not keep the result of {record.label}: {error}", file=sys.stderr,
                  flush=True)
        runner = runner_reference()
        if runner is not None:
            runner._stored(record)
        del runner


def _drive(executor_reference):
    """Advance the run of a runner in the background until it closes. The thread holds the runner only while it
    advances it, so a runner nothing else refers to can still be collected and closed. The thread blocks SIGINT, and so
    do the threads the Manager starts from it, since Python raises an interrupt in the main thread whichever thread
    received it, which would let it through while the main thread holds it back (_block_interrupts())."""
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


def _remove_files(root):
    """Remove the files under a run-info template directory, keeping its directories."""
    for directory, _, files in os.walk(root):
        for name in files:
            try:
                os.remove(os.path.join(directory, name))
            except FileNotFoundError:
                pass
