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
import html
import importlib
import os
import shutil
import signal
import sys
import tempfile
import threading
import time
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
from .graph import File, Graph, Node, NodeFile, _Selectable, _Symbol  # noqa: E402
from . import library  # noqa: E402

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

# Seconds the background thread spends on queued calls before it advances the run one step.
COMMAND_SECONDS = 0.05

# Bytes up to which a serialized function or argument travels inside the manifest of its task. Larger objects become
# staged files, which every task that passes the same object shares. A file of its own takes at least one 4 KiB page
# on disk, so a smaller object is never cheaper as a file. References to other tasks and small values, which are usually
# different in every task, then add no file at all, and a small object that tasks share is copied at most this many
# bytes per task.
INLINE_LIMIT = 4096


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
    is gone, or it was a call without a Future. The message names both tasks either way."""

    def __init__(self, message, task, source):
        super().__init__(message)
        self.task = task
        self.source = source


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


class Future(_Selectable):
    """The result of a task submitted to an Executor, or the path of a file. While the Future exists, the Executor
    keeps the data it stands for, so tasks submitted later read it without recomputing it. Pass a Future to
    Executor.submit() to give a task its result, or select from it like a Node."""

    __slots__ = ("_executor", "_target", "_held", "__weakref__")

    def __init__(self, executor, target):
        self._executor = executor
        self._target = target
        self._held = None  # the data key whose data the Executor keeps for this Future

    def __del__(self):
        # Collection may happen in any thread, so the background thread lets the data go. A closed Executor ignores
        # the call, and one collected at interpreter exit may find its modules gone already.
        key = self._held
        if key is not None:
            executor = self._executor
            try:
                executor._enqueue(lambda: executor._unhold(key[0], key))
            except Exception:
                pass

    @property
    def target(self):
        """The Node, NodeFile, or File this Future stands for."""
        return self._target

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

    @property
    def _task(self):
        target = self._target
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
        if not isinstance(self._target, Node):
            raise TypeError("only a task result can be selected from, and this Future stands for a file")

    def _as_ref(self):
        if self._executor._closed:
            raise RuntimeError("the Executor of this Future is closed")
        return self._target._as_ref()

    def __reduce__(self):
        raise TypeError("a Future belongs to a running Executor and cannot be pickled; pickle the Graph instead")

    def __repr__(self):
        if self._executor._closed:
            state = "closed"
        elif self.cancelled():
            state = "cancelled"
        else:
            state = "done" if self.done() else "pending"
        return f"Future({self._target!r}, {state})"


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


class _Watcher:
    """The tasks a caller of as_completed() waits for, and those of them that finished since it last looked."""

    def __init__(self, tasks):
        self.tasks = tasks
        self.finished = []


class _PathRequest:
    """A caller's wait for the readable local path of a pinned output."""

    def __init__(self, key):
        self.key = key
        self.path = None


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
        self._numbers = {}  # (graph id, key) of each submitted task -> number that names its files
        self._submitted = 0  # tasks submitted over the Executor's life, including those it forgot
        self._labels = {}  # (graph id, key) -> how messages name the task, which outlives its Graph
        # Hoisted objects, each serialized on its own: name -> bytes. A local Executor loads one copy per task, as
        # forked tasks get on Workers, and a Worker library loads them from a file.
        self._hoisted = _serialize_hoisted(hoist)
        library_modules = [library.module_name(module) for module in library_modules]
        if not isinstance(edata_cache, int) or edata_cache < 0:
            raise ValueError("edata_cache takes a number of bytes, zero or more")
        self._edata_cache = edata_cache
        # Until the first submit() or run() decides the cached edata, the library waits to be installed, and calls
        # into C that the caller queues meanwhile are held, so no task reaches C before the library caches its edata.
        self._library_pending = bool(edata_cache) and not self._local
        self._held = None
        self._cached_edata = frozenset()  # sandbox names of staged objects that the library loads
        self._last_number = 0
        self._declared_outputs = {}  # (graph id, key) -> output paths the task declared when it was submitted
        self._file_numbers = {}  # file identity -> number that names the file in sandboxes
        self._local_paths = {}  # file identity -> local path of a file a local task wrote
        self._local_seconds = 0.0
        self._keep_paths = set()

        # State the background thread owns: the C ids of submitted tasks and files.
        self._nodes = {}  # (graph id, key) -> C node id
        self._tasks = {}  # C node id -> (graph id, key)
        self._result_files = {}  # (graph id, key) -> C file id of the task result
        self._output_files = {}  # (graph id, key, path) -> C file id of a file the task writes
        self._input_files = {}  # absolute path of a File -> C file id
        self._edata_files = {}  # sandbox name of a staged object -> C file id
        self._futures = collections.Counter()  # (graph id, key) -> live Futures that stand for the task
        self._released = set()  # tasks whose outputs C released, and which nobody asked for since

        # State both threads share, guarded by the lock. The background thread notifies the condition when it
        # publishes changes, and the caller's thread notifies it when it queues calls.
        self._lock = threading.RLock()
        self._changed = threading.Condition(self._lock)
        self._commands = collections.deque()  # calls into C, run in order by the background thread
        self._finished = {}  # (graph id, key) -> (COMPLETED, None, None) or (FAILED, source task, message)
        self._counts = {COMPLETED: 0, FAILED: 0}
        self._path_requests = []
        self._paths = {}  # data key -> readable local path of a pinned output, published by the background thread
        self._cancelled = set()  # (graph id, key) of tasks that cancel() stopped
        # Callers in as_completed(): each watches some tasks and collects those that finish, so a wait wakes up for
        # new results instead of checking every pending Future again.
        self._watchers = set()
        # Staged objects, shared by identity across tasks: id(object) -> (object, sandbox name, local path). Holding the
        # object keeps its id from being reused. A staged file lives while a task that holds it may still run.
        self._edata = {}
        self._edata_names = {}  # sandbox name -> (id of its object, or None for a manifest, local path)
        self._edata_holders = collections.Counter()  # sandbox name -> tasks that hold the file
        self._staged = {}  # (graph id, key) -> sandbox names of the staged files the task holds
        self._graph_tasks = collections.defaultdict(set)  # graph id -> keys of its submitted tasks
        self._run_error = None  # why the run stopped as a whole
        self._driver = None
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
            library.save_objects(hoisted_path, self._hoisted)
        self._library_name = library.unique_name()
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
            library.installed(self._manager, self._library_name, modules, files, hoisted_path, edata_path))

    @contextlib.contextmanager
    def _first_submission(self):
        """Around the first submit() or run() call of an Executor that caches edata, hold the calls into C that it
        queues. Afterwards, install the library with the smallest objects that the call staged, then release them."""
        if not self._library_pending or self._held is not None:
            yield
            return
        with self._changed:
            self._held = []
        try:
            yield
        finally:
            with _deferred_interrupts():
                self._library_pending = False
                path = None
                try:
                    path = self._save_edata_cache()
                finally:
                    with self._changed:
                        held, self._held = self._held, None
                        if not self._closed:
                            self._commands.append(lambda: self._install_library(path))
                            self._commands.extend(held)
                            self._changed.notify_all()
                            capi.vine_graph_executor_wake(self._executor)

    def _save_edata_cache(self):
        """Write the smallest staged objects that fit in edata_cache bytes to the file the library loads, and return
        its path, or None when no object fits."""
        with self._lock:
            staged = [(name, path) for name, (identity, path) in self._edata_names.items() if identity is not None]
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
        library.save_objects(path, chosen)
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
        with self._changed:
            if self._closed:
                return
            self._closed = True
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
            for name in files:
                path = os.path.join(directory, name)
                if path not in self._keep_paths:
                    os.unlink(path)
            for name in dirs:
                path = Path(directory) / name
                if path.is_symlink():
                    if str(path) not in self._keep_paths:
                        path.unlink()
                elif not any(path.iterdir()):
                    path.rmdir()

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
            elif not self._local:
                self._call(lambda setting=setting, value=value: self._tune_manager(setting, value))

    def _tune_manager(self, setting, value):
        try:
            status = self._manager.tune(setting, value)
        except Exception:
            status = -1
        if status != 0:
            raise ValueError(f"unknown setting {setting!r}: it is neither a vine_graph setting nor TaskVine Manager tuning")

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
        self._check_open()
        if isinstance(func, (Node, NodeFile, File)):
            if args or kwargs:
                raise TypeError("submit(node) takes no arguments; give them to Graph.add()")
            return self._submit_target(func)
        if isinstance(func, _Symbol):
            raise TypeError("submit() takes a callable, a Node, a NodeFile, or a File")
        if not callable(func):
            raise TypeError("submit() takes a callable followed by its arguments")

        def register(symbol):
            # Arguments may come from user Graphs, whose tasks this Executor then runs, or from Futures of this
            # Executor.
            if isinstance(symbol, Future) and symbol._executor is not self:
                raise ValueError("a Future of another Executor cannot be an argument")
            source = symbol.source if hasattr(symbol, "source") else symbol
            node = source.node if isinstance(source, NodeFile) else source
            if isinstance(node, Node):
                self._add_graph(node.graph)

        key = self._dynamic._next_key
        self._dynamic._next_key += 1
        node = self._dynamic._add(key, func, args, kwargs, foreign=True, on_symbol=register)
        return self._submit_target(node)

    def _submit_target(self, target):
        if isinstance(target, File):
            if not os.path.isfile(target.path):
                raise FileNotFoundError(target.path)
            return Future(self, target)
        node = target.node if isinstance(target, NodeFile) else target
        if node.key not in node.graph._tasks:
            raise KeyError(f"no task with key {node.key!r}")
        self._add_graph(node.graph)
        task = (node.graph._id, node.key)
        with self._first_submission():
            held = self._submit_with_ancestors([task])
            try:
                future = self._new_future(task, target)
            finally:
                self._unpin_results(held)
        return future

    def _new_future(self, task, target):
        if isinstance(target, NodeFile) and target.path not in self._declared_outputs[task]:
            raise ValueError(f"the file {target.path!r} was declared after its task was submitted")
        future = Future(self, target)
        if not self._local:
            key = self._data_key(target)
            with _deferred_interrupts():
                # The Future keeps its data, which is fetched to the Manager as soon as the task completes. Closing the
                # Executor releases every pin, so unpinning at interpreter exit could only reach freed state.
                self._enqueue(lambda: self._hold(task, key))
                future._held = key
        return future

    def _add_graph(self, graph):
        if graph._id not in self._graphs:
            self._graphs[graph._id] = graph
            # Collection may happen in any thread, so the background thread lets the Graph's tasks go.
            weakref.finalize(graph, _graph_collected, weakref.ref(self), graph._id).atexit = False

    def _submit_with_ancestors(self, tasks):
        """Submit tasks after every ancestor that is not submitted yet, upstream first. The background thread advances
        the run between queued calls, and C releases a completed task whose readers and Futures it has not seen yet. So
        a task that a later task of this call reads pins its result in its own submission, and its last such reader
        unpins it. The given tasks pin their results too, and the caller passes the returned list to _unpin_results()
        once their Futures hold them."""
        order = []
        visiting = [(task, False) for task in reversed(tasks)]
        seen = set()
        while visiting:
            current, expanded = visiting.pop()
            if expanded:
                order.append(current)
                continue
            if current in seen or current in self._numbers:
                continue
            seen.add(current)
            graph_id, key = current
            graph = self._graphs.get(graph_id)
            if graph is None or key not in graph._tasks:
                raise KeyError(f"no submitted Graph has a task with key {key!r}")
            visiting.append((current, True))
            visiting.extend((_resolve(graph_id, parent), False) for parent in graph._parents[key])
        batch = set(order)
        last_reader = {}
        for current in order:
            for parent in self._graphs[current[0]]._parents[current[1]]:
                parent = _resolve(current[0], parent)
                if parent in batch:
                    last_reader[parent] = current
        unpins = collections.defaultdict(list)
        for parent, reader in last_reader.items():
            unpins[reader].append(parent)
        targets = set(tasks) & batch
        try:
            for current in order:
                pins = (current in last_reader) + (current in targets)
                self._submit_task(current, pins, unpins.get(current, ()))
        except BaseException:
            # A queued task keeps its number. One whose last reader was not queued, because staging failed or was
            # interrupted, gives up its pin, and so does every queued target.
            if not self._local:
                for parent, reader in last_reader.items():
                    if parent in self._numbers and reader not in self._numbers:
                        self._enqueue(lambda parent=parent: self._unpin_result(parent))
                self._unpin_results([task for task in targets if task in self._numbers])
            raise
        return [] if self._local else [task for task in order if task in targets]

    def _unpin_results(self, tasks):
        """Release the pins that submitted targets took until their Futures hold them."""
        for task in tasks:
            self._enqueue(lambda task=task: self._unpin_result(task))

    def _submit_task(self, task, pins=0, unpins=()):
        """Check and stage one task, then run it locally or queue it for the background thread. A task interrupted
        before it was queued can be submitted again. pins is the number of pins its result takes in its own submission,
        which later calls release, and unpins names the tasks whose pins it releases once it is submitted."""
        graph_id, key = task
        graph = self._graphs[graph_id]
        for name in graph._hoisted.get(key, ()):
            if name not in self._hoisted:
                raise KeyError(f"no hoisted object named {name!r}; give it to Executor(hoist={{...}})")
        for ref in graph._reads[key]:
            if isinstance(ref, File) and not os.path.isfile(ref.path):
                raise FileNotFoundError(ref.path)
            if not isinstance(ref, File) and ref.path not in self._declared_outputs.get(_resolve(graph_id, ref), ()):
                raise ValueError(f"the file {ref.path!r} was declared after its task was submitted")
        self._last_number += 1
        self._numbers[task] = self._last_number
        self._submitted += 1
        self._labels[task] = repr(Node(graph, key))
        self._declared_outputs[task] = tuple(graph._outputs.get(key, {}))
        names = []
        submitted = False
        try:
            if self._local:
                self._run_local(task)
                submitted = True
            else:
                plan = self._plan(task, names)
                plan["pins"] = pins
                plan["unpins"] = list(unpins)
                with _deferred_interrupts(), self._lock:
                    self._staged[task] = names
                    self._graph_tasks[graph_id].add(key)
                    self._enqueue(lambda: self._submit_node(plan))
                    submitted = True
        finally:
            if not submitted:
                del self._numbers[task]
                self._submitted -= 1
                del self._labels[task]
                del self._declared_outputs[task]
                with self._lock:
                    self._staged.pop(task, None)
                    self._drop_staged(names)

    def _plan(self, task, names):
        """Stage a task's arguments and manifest, and return what the background thread declares and mounts. The
        sandbox name of every staged file the task holds is added to names."""
        graph_id, key = task
        graph = self._graphs[graph_id]
        func, args, kwargs = graph._tasks[key]
        # The manifest is keyed by the references as the arguments hold them, and the Executor resolves them.
        files = {library.file_identity(ref): self._file_task_path(_resolve_file(graph_id, library.file_identity(ref)))
                 for ref in graph._reads[key]}
        # The manifest holds each small object serialized inside it, and names each larger one by the sandbox path of
        # its staged file. An object passed as several arguments of the task is one entry, so it stays one object.
        inline = []
        entries = {}

        def stage(value):
            if id(value) not in entries:
                entries[id(value)] = self._stage_argument(value, names, inline)
            return entries[id(value)]

        manifest = {
            "objects": inline,
            "callable": stage(func),
            "args": [stage(value) for value in args],
            "kwargs": {argument: stage(value) for argument, value in kwargs.items()},
            "inputs": {parent: self._result_task_path(_resolve(graph_id, parent)) for parent in graph._results[key]},
            "files": files,
        }
        with self._lock:
            edata = [(name, self._edata_names[name][1]) for name in names]
        return {
            "task": task,
            "outputs": self._declared_outputs[task],
            "results": [(_resolve(graph_id, parent), name) for parent, name in manifest["inputs"].items()],
            "files": [(_resolve_file(graph_id, identity), name) for identity, name in files.items()],
            "edata": edata,
            "manifest": self._stage_manifest(manifest, names),
        }

    def _stage(self, value, inline_limit=0):
        """Serialize one object. Return None and its bytes when they fit in inline_limit, and otherwise write them to
        a new staged file and return its sandbox name and local path. Small objects never touch the disk, and a large
        one is written as it is serialized instead of being held in memory."""
        self._last_number += 1
        name = f"vine-graph-edata-{self._last_number}"
        target = _Serialized(os.path.join(self._edata_dir, name), inline_limit)
        try:
            cloudpickle.dump(value, target)
            target.close()
        except BaseException:
            # A long-lived Executor would otherwise keep the partial file of a failed or interrupted submission.
            target.discard()
            raise
        if target.stream is None:
            return None, bytes(target.buffer)
        return name, target.path

    def _stage_argument(self, value, names, inline):
        """Return how the manifest refers to the function or an argument of a task: the index of its bytes in inline
        when it is small, or else the sandbox name of its staged file, staging it unless it is staged already. The name
        of a staged file is added to names, which holds the file for the task."""
        # Share top-level objects by identity, not content. Independent pickle streams do not preserve aliases nested
        # across different objects or callable closures.
        with self._lock:
            entry = self._edata.get(id(value))
            if entry is not None:
                self._edata_holders[entry[1]] += 1
                names.append(entry[1])
                return entry[1]
        name, staged = self._stage(value, INLINE_LIMIT)
        if name is None:
            inline.append(staged)
            return len(inline) - 1
        with self._lock:
            self._edata[id(value)] = (value, name, staged)
            self._edata_names[name] = (id(value), staged)
            self._edata_holders[name] += 1
        names.append(name)
        return name

    def _stage_manifest(self, manifest, names):
        """Stage the manifest of a task as a file of its own, which the task holds, and return its name and path."""
        name, path = self._stage(manifest)
        with self._lock:
            self._edata_names[name] = (None, path)
            self._edata_holders[name] += 1
        names.append(name)
        return name, path

    def _enqueue(self, command):
        """Queue a call into C for the background thread. Garbage collection may call this from any thread."""
        with self._changed:
            if self._closed:
                return
            if self._held is not None:
                self._held.append(command)
                return
            self._commands.append(command)
            if threading.current_thread() is self._driver:
                # The background thread runs every queued call before it waits again.
                return
            self._changed.notify_all()
            if self._driver is not None:
                # End the background thread's wait in C so it runs the call now. Closing deletes the C executor only
                # after the thread stopped, and the lock keeps this call before that.
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
        """Wait until the background thread ran every call queued so far."""
        self._call(lambda: None)

    # Calls into C, in the background thread.

    def _submit_node(self, plan):
        """Declare a task's outputs, mount its inputs and arguments, and submit it. Only the C executor can fail this,
        which stops the run."""
        task = plan["task"]
        if task in self._nodes:
            # An interrupt between queueing a task and recording it makes the caller queue it again.
            return
        graph_id, key = task
        node = capi.vine_graph_executor_add_node(self._executor)
        if not node:
            raise MemoryError(f"could not add a graph node for {graph_id}:{key!r}")
        self._nodes[task] = node
        self._tasks[node] = task
        self._result_files[task] = self._add_output(node, library.RESULT)
        for path in plan["outputs"]:
            self._output_files[(graph_id, key, path)] = self._add_output(node, path)
        with self._lock:
            # A released task that a new task reads runs again.
            self._released.difference_update(parent for parent, _ in plan["results"])
            self._released.difference_update(identity[1:3] for identity, _ in plan["files"] if identity[0] == "output")
        for parent, name in plan["results"]:
            self._add_input(node, self._result_files[parent], name)
        for identity, name in plan["files"]:
            if identity[0] == "input":
                if identity[1] not in self._input_files:
                    self._input_files[identity[1]] = self._declare_file(self._link_input(identity[1]))
                file = self._input_files[identity[1]]
            else:
                file = self._output_files[identity[1:]]
            self._add_input(node, file, name)
        # The library holds the cached objects, so tasks neither mount nor fetch their files.
        for name, path in [*plan["edata"], plan["manifest"]]:
            if name not in self._edata_files and name not in self._cached_edata:
                self._edata_files[name] = self._declare_file(path, vault=True)
        for name, _ in plan["edata"]:
            if name not in self._cached_edata:
                self._add_input(node, self._edata_files[name], name)
        self._add_input(node, self._edata_files[plan["manifest"][0]], library.MANIFEST)
        self._check(capi.vine_graph_executor_submit_node(self._executor, node), f"failed to submit task {key!r}")
        for _ in range(plan["pins"]):
            self._check(capi.vine_graph_executor_pin_file(self._executor, self._result_files[task]),
                        "failed to pin a result")
        for parent in plan["unpins"]:
            self._unpin_result(parent)

    def _unpin_result(self, task):
        """Release a pin that a task took in its own submission."""
        capi.vine_graph_executor_unpin_file(self._executor, self._result_files[task])

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
        return (task, target.path) if isinstance(target, NodeFile) else (task, None)

    def _file_of(self, key):
        """Return the C file id of a task result, or of a file the task writes."""
        task, path = key
        return self._result_files[task] if path is None else self._output_files[(task[0], task[1], path)]

    def _hold(self, task, key):
        """Pin the data of a new Future and fetch it. A released task that is asked for again runs again."""
        with self._lock:
            self._futures[task] += 1
            self._released.discard(task)
        file = self._file_of(key)
        self._check(capi.vine_graph_executor_pin_file(self._executor, file), "failed to pin a result")
        self._check(capi.vine_graph_executor_fetch_file(self._executor, file), "failed to fetch a result")

    def _unhold(self, task, key):
        capi.vine_graph_executor_unpin_file(self._executor, self._file_of(key))
        with self._lock:
            # An unpinned output may leave the vault, so its path is looked up again next time.
            self._paths.pop(key, None)
            self._futures[task] -= 1
            if not self._futures[task]:
                del self._futures[task]
            self._let_go(task)

    def _graph_gone(self, graph_id):
        with self._lock:
            for key in self._graph_tasks.pop(graph_id, ()):
                self._let_go((graph_id, key))

    def _let_go(self, task):
        """Free the staged files of a task that nothing holds and that will not run again. The user holds a task through
        its Graph or a Future. A failed task never runs again, and neither does a released one unless asked for."""
        graph_id, key = task
        if task not in self._staged or self._futures[task] or graph_id in self._graphs and graph_id != self._dynamic._id:
            return
        entry = self._finished.get(task)
        if task not in self._released and (entry is None or entry[0] != FAILED):
            return
        self._released.discard(task)
        self._drop_staged(self._staged.pop(task))
        if graph_id == self._dynamic._id:
            # Only a Future could reach this call again, so its function and arguments go too.
            self._dynamic._remove(key)
            self._graph_tasks[graph_id].discard(key)
        # C removes the node once every node that reads its outputs was removed, and then this Executor forgets it.
        self._enqueue(lambda task=task: self._drop_node(task))

    def _drop_node(self, task):
        node = self._nodes.get(task)
        if node is not None:
            capi.vine_graph_executor_drop_node(self._executor, node)

    def _forget(self, node):
        """Forget a task whose C node was removed. Its descendants were removed before it, so no failure names it."""
        task = self._tasks.pop(node, None)
        if task is None:
            return
        self._nodes.pop(task, None)
        self._result_files.pop(task, None)
        for path in self._declared_outputs.pop(task, ()):
            self._output_files.pop((task[0], task[1], path), None)
        self._numbers.pop(task, None)
        self._labels.pop(task, None)
        self._finished.pop(task, None)
        self._cancelled.discard(task)

    def _drop_staged(self, names):
        """Let go of staged files that a task held, and free each one that no task holds anymore."""
        for name in names:
            self._edata_holders[name] -= 1
            if self._edata_holders[name]:
                continue
            del self._edata_holders[name]
            identity, path = self._edata_names.pop(name)
            if identity is not None:
                del self._edata[identity]
            # The C file exists once the background thread declared it, which every queued plan does before this runs.
            self._enqueue(lambda name=name, path=path: self._free_staged(name, path))

    def _free_staged(self, name, path):
        file = self._edata_files.pop(name, None)
        if file is not None:
            capi.vine_graph_executor_undeclare_file(self._executor, file)
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    def _advance(self):
        """Run queued calls, advance the run for about a second, and publish what happened. Return False when the run
        stopped. Only the background thread calls this. Queued calls run for at most COMMAND_SECONDS before the run
        advances one step, so tasks already submitted start while a caller keeps submitting more."""
        deadline = time.monotonic() + COMMAND_SECONDS
        pending = False
        while True:
            with self._changed:
                if self._closed or not self._commands:
                    break
                command = self._commands.popleft()
            command()
            if isinstance(command, _Call):
                with self._changed:
                    self._changed.notify_all()
            if time.monotonic() >= deadline:
                with self._changed:
                    pending = bool(self._commands)
                break
        if self._closed:
            return False
        if pending:
            # A wait returns after one step of the Manager, which dispatches ready tasks, when a wake is pending.
            # A wait of zero seconds would only collect returned tasks.
            capi.vine_graph_executor_wake(self._executor)
        status = capi.vine_graph_executor_wait(self._executor, 1)
        error = capi.vine_graph_executor_get_error(self._executor) if status == capi.VINE_GRAPH_FAILED else None
        self._publish_finished()
        with self._changed:
            for request in self._path_requests:
                if request.path is None:
                    request.path = capi.vine_graph_executor_get_local_path(self._executor, self._file_of(request.key))
                    if request.path:
                        self._paths[request.key] = request.path
            if error:
                self._run_error = error
            self._changed.notify_all()
            progress = self._progress_locked()
            if status == capi.VINE_GRAPH_DONE and not self._commands and not self._closed:
                # Every task finished and every requested output is readable, so wait for the caller to queue more.
                self._changed.wait(0.5)
        if self._display is not None:
            self._display.update(progress)
        return error is None

    def _publish_finished(self):
        """Publish the tasks that completed or failed since the last call, and let go of tasks that will not run
        again."""
        finished = []
        while node := capi.vine_graph_executor_next_finished(self._executor):
            task = self._tasks[node]
            if capi.vine_graph_executor_get_node_state(self._executor, node) == capi.VINE_GRAPH_NODE_FAILED:
                source = self._tasks[capi.vine_graph_executor_get_failure_source(self._executor, node)]
                # Only the source keeps the message, which its dependents read from it.
                message = capi.vine_graph_executor_get_node_error(self._executor, node) if source == task else None
                finished.append((task, (FAILED, source, message)))
            else:
                finished.append((task, (COMPLETED, None, None)))
        released = []
        while node := capi.vine_graph_executor_next_released(self._executor):
            released.append(self._tasks[node])
        with self._changed:
            for task, entry in finished:
                self._set_finished(task, entry)
            self._released.update(released)
            for task in [*released, *(task for task, entry in finished if entry[0] == FAILED)]:
                self._let_go(task)
            while node := capi.vine_graph_executor_next_removed(self._executor):
                self._forget(node)
            self._changed.notify_all()

    # Published state, shared by both threads.

    def _set_finished(self, task, entry):
        previous = self._finished.get(task)
        if previous is not None:
            self._counts[previous[0]] -= 1
        self._finished[task] = entry
        self._counts[entry[0]] += 1
        for watcher in self._watchers:
            if task in watcher.tasks:
                watcher.finished.append(task)

    def _finished_entry(self, task):
        with self._lock:
            return self._finished.get(task)

    def _finished_state(self, task):
        if task is None:
            return COMPLETED
        with self._lock:
            self._check_open()
            if task not in self._numbers:
                raise RuntimeError("the task of this Future was never submitted")
            entry = self._finished.get(task)
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

    def run(self, graph, targets=None):
        """Run the targets of a Graph, and every task they read, and return {target: result}. Targets are Nodes,
        NodeFiles, or Files, and default to the tasks no other task reads. A failure raises the TaskError of a task
        that failed on its own, in preference to the tasks that could not run because of it."""
        targets = graph.sinks() if targets is None else list(targets)
        tasks = []
        for target in targets:
            node = target.node if isinstance(target, NodeFile) else target
            if isinstance(node, Node) and node.key in node.graph._tasks:
                self._add_graph(node.graph)
                tasks.append((node.graph._id, node.key))
        with self._first_submission():
            # One call submits every task that the targets need, so a task read by another target's task stays.
            held = self._submit_with_ancestors(tasks)
            try:
                futures = [self.submit(target) for target in targets]
            finally:
                self._unpin_results(held)
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
        with self._changed:
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

    def _error(self, task):
        if self._finished_state(task) != FAILED:
            return None
        _, source, message = self._finished_entry(task)
        if source != task:
            message = self._finished_entry(source)[2]
        message = message or ""
        task_node, source_node = self._node(task), self._node(source)
        task_label, source_label = self._labels[task], self._labels[source]
        # A function failure ends with its traceback. Lines the library writes before it are not about the function.
        start = message.find("Traceback (most recent call last):")
        message = f"\n{message[start:]}" if start >= 0 else f" {message}"
        if source == task:
            return TaskError(f"task {task_label} failed:{message}", task_node, source_node)
        return TaskError(f"task {task_label} did not run because task {source_label} failed:{message}", task_node,
                         source_node)

    def _cancel(self, task):
        self._check_open()
        if task is None or self._local or self._finished_state(task) is not None:
            return False

        def cancel():
            if capi.vine_graph_executor_cancel_node(self._executor, self._nodes[task]) != 0:
                return False
            with self._lock:
                self._cancelled.add(task)
            # The task and its dependents are done when cancel() returns.
            self._publish_finished()
            return True
        return self._call(cancel)

    def _result(self, future, timeout):
        self._wait_for([future], timeout)
        target = future._target
        if isinstance(target, File):
            return target.path
        task = future._task
        error = self._error(task)
        if error is not None:
            raise error
        if isinstance(target, Node):
            if self._local:
                return library.load_result(self._local_result_path(task))
            return library.load_result(self._fetched_path(task, self._data_key(target)))
        identity = library.file_identity(target._as_ref())
        if self._local:
            path = self._local_paths[identity]
        else:
            # Link or copy the file out of the Manager's vault, which the Executor removes when it closes.
            path = os.path.join(self._run_dir, self._file_task_path(identity))
            if not os.path.exists(path):
                fetched = self._fetched_path(task, self._data_key(target))
                try:
                    os.link(fetched, path)
                except OSError:
                    shutil.copyfile(fetched, path)
        path = os.path.abspath(path)
        self._keep_paths.add(path)
        return path

    def _fetched_path(self, task, key):
        """Wait until a pinned output of a completed task is readable at the Manager, and return its path."""
        with self._lock:
            path = self._paths.get(key)
        if path:
            return path
        request = _PathRequest(key)
        try:
            with self._changed:
                self._path_requests.append(request)
                self._changed.notify_all()
                # The background thread leaves its wait in C to look the path up at once.
                capi.vine_graph_executor_wake(self._executor)
            self._wait_until(lambda: request.path or self._finished_state(task) == FAILED, None,
                             "the result is not available")
        finally:
            with self._lock:
                if request in self._path_requests:
                    self._path_requests.remove(request)
        if request.path:
            return request.path
        raise self._error(task)

    # Local execution.

    def _run_local(self, task):
        """Run one task in this process, for debugging without Workers. Its parents already finished."""
        graph_id, key = task
        graph = self._graphs[graph_id]
        for parent in graph._parents[key]:
            entry = self._finished[_resolve(graph_id, parent)]
            if entry[0] == FAILED:
                self._set_finished(task, (FAILED, entry[1], entry[2]))
                return
        task_dir = os.path.join(self._run_dir, ".tasks", f"task-{self._numbers[task]}")
        shutil.rmtree(task_dir, ignore_errors=True)
        os.makedirs(task_dir)
        outputs = graph._outputs.get(key, {})
        for task_path in outputs:
            os.makedirs(os.path.dirname(os.path.join(task_dir, task_path)), exist_ok=True)
        func, args, kwargs = graph._tasks[key]
        previous_cwd = os.getcwd()
        started = time.time()
        os.chdir(task_dir)
        try:
            output = library.run_task(
                func, args, kwargs,
                lambda parent: library.load_result(self._local_result_path(_resolve(graph_id, parent))),
                lambda identity: self._local_file_path(_resolve_file(graph_id, identity)),
                lambda name: cloudpickle.loads(self._hoisted[name]))
            for task_path in outputs:
                local_path = os.path.abspath(os.path.join(task_dir, task_path))
                if not os.path.isfile(local_path):
                    raise FileNotFoundError(f"task {key!r} did not produce its output file {task_path!r}")
                self._local_paths[("output", graph_id, key, task_path)] = local_path
            library.save_result(self._local_result_path(task), output)
            self._set_finished(task, (COMPLETED, None, None))
        except KeyboardInterrupt:
            # An interrupt stops the submission, which can be repeated.
            raise
        except BaseException as error:
            # As on a Worker, anything else the function raises, including an exit, fails the task.
            self._set_finished(task, (FAILED, task, library.format_user_traceback(error, __file__, library.__file__)))
        finally:
            os.chdir(previous_cwd)
            self._local_seconds += time.time() - started
            if self._display is not None:
                self._display.update(self._progress_locked())

    def _local_result_path(self, task):
        return os.path.join(self._run_dir, f"result-{self._numbers[task]}")

    def _local_file_path(self, identity):
        if identity[0] == "input":
            return identity[1]
        return self._local_paths[identity]

    # Names and local paths.

    def _result_task_path(self, task):
        return f"vine-graph-result-{self._numbers[task]}"

    def _file_task_path(self, identity):
        if identity not in self._file_numbers:
            self._last_number += 1
            self._file_numbers[identity] = self._last_number
        return f"vine-graph-file-{self._file_numbers[identity]}-{os.path.basename(identity[-1])}"

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


@contextlib.contextmanager
def _deferred_interrupts():
    """Hold back SIGINT while a short change of Executor state completes, and deliver it afterwards, so an interrupt
    never leaves a task queued but unrecorded. Only the main thread receives SIGINT."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def _graph_collected(executor_reference, graph_id):
    """Let the tasks of a collected Graph go. Collection may happen in any thread, so the background thread does it."""
    executor = executor_reference()
    if executor is not None and not executor._local:
        executor._enqueue(lambda: executor._graph_gone(graph_id))


def _drive(executor_reference):
    """Advance the run of an Executor in the background until it closes. The thread holds the Executor only while it
    advances it, so an Executor nothing else refers to can still be collected and closed."""
    while True:
        executor = executor_reference()
        if executor is None or executor._closed:
            return
        try:
            if not executor._advance():
                return
        except Exception as error:
            with executor._changed:
                executor._run_error = f"{error}"
                executor._changed.notify_all()
            return
        del executor


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
