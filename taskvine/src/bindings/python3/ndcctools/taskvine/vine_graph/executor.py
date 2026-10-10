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
import ctypes
import dataclasses
import html
import importlib
import os
import shutil
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
    source the Node of the task whose own failure explains it."""

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

    __slots__ = ("_executor", "_target", "__weakref__")

    def __init__(self, executor, target):
        self._executor = executor
        self._target = target

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

    local=True runs each task in this process when it is submitted, for debugging, without TaskVine. output_dir holds
    the directory of file results that result() returns, which stay after close. library_modules are imported by the
    library that runs tasks on each Worker, and library_files maps local paths to names in its sandbox. params holds
    settings for set_params(), and other keyword arguments go to the TaskVine Manager.
    """

    def __init__(self, port=cvine.VINE_DEFAULT_PORT, name=None, *, local=False, output_dir="./outputs",
                 library_modules=(), library_files=None, params=None, **manager_options):
        self._local = bool(local)
        self._closed = False
        self._manager = None
        self._executor = None  # opaque C executor handle
        self._resources = ExitStack()

        # State the caller's thread owns: what was submitted and how its files are named and staged.
        self._dynamic = Graph()  # tasks submitted as calls
        self._graphs = {self._dynamic._id: self._dynamic}  # graph id -> Graph of every submitted task
        self._numbers = {}  # (graph id, key) of each submitted task -> number that names its files
        self._last_number = 0
        self._declared_outputs = {}  # (graph id, key) -> output paths the task declared when it was submitted
        self._file_numbers = {}  # file identity -> number that names the file in sandboxes
        # Top-level argument objects staged so far, by identity: id(object) -> (object, sandbox name, local path).
        # Holding the object keeps its id from being reused.
        self._edata = {}
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

        # State both threads share, guarded by the lock. The background thread notifies the condition when it
        # publishes changes, and the caller's thread notifies it when it queues calls.
        self._lock = threading.RLock()
        self._changed = threading.Condition(self._lock)
        self._commands = collections.deque()  # calls into C, run in order by the background thread
        self._finished = {}  # (graph id, key) -> (COMPLETED, None, None) or (FAILED, source task, message)
        self._counts = {COMPLETED: 0, FAILED: 0}
        self._path_requests = []
        self._cancelled = set()  # (graph id, key) of tasks that cancel() stopped
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
        library_name, function_name = self._resources.enter_context(
            library.installed(self._manager, library_modules, library_files))
        self._executor = capi.vine_graph_executor_create(self._manager._taskvine, library_name, function_name)
        if not self._executor:
            raise MemoryError("could not create the graph executor")
        for setting, value in EXECUTOR_TUNING.items():
            self._tune_executor(setting, value)
        if _in_notebook() or hasattr(sys, "ps1"):
            self._tune_executor("progress-bar", 0)
        self._edata_dir = self._resources.enter_context(
            tempfile.TemporaryDirectory(prefix="vine-graph-edata-", dir=self._manager.staging_directory))

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
                if capi.vine_graph_executor_check_setting(setting, str(value)) != 0:
                    raise ValueError(f"invalid value {value!r} for {setting}")
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
            raise ValueError(f"Unrecognized parameter: {setting}")

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
                self._graphs.setdefault(node.graph._id, node.graph)

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
        self._graphs.setdefault(node.graph._id, node.graph)
        task = (node.graph._id, node.key)
        self._submit_with_ancestors(task)
        if isinstance(target, NodeFile) and target.path not in self._declared_outputs[task]:
            raise ValueError(f"the file {target.path!r} was declared after its task was submitted")
        future = Future(self, target)
        if not self._local:
            key = self._data_key(target)
            # The Future keeps its data, which is fetched to the Manager as soon as the task completes.
            self._enqueue(lambda: self._pin(key))
            # Closing the Executor releases every pin, so unpinning at interpreter exit could only reach freed state.
            weakref.finalize(future, self._enqueue, lambda: self._unpin(key)).atexit = False
        return future

    def _submit_with_ancestors(self, task):
        """Submit a task after every ancestor that is not submitted yet, upstream first."""
        order = []
        visiting = [(task, False)]
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
            visiting.extend((parent, False) for parent in graph._parents[key])
        for current in order:
            self._submit_task(current)

    def _submit_task(self, task):
        """Check and stage one task, then run it locally or queue it for the background thread. A task interrupted
        before it was queued can be submitted again."""
        graph_id, key = task
        graph = self._graphs[graph_id]
        for ref in graph._reads[key]:
            if isinstance(ref, File) and not os.path.isfile(ref.path):
                raise FileNotFoundError(ref.path)
            if not isinstance(ref, File) and ref.path not in self._declared_outputs.get((ref.graph_id, ref.key), ()):
                raise ValueError(f"the file {ref.path!r} was declared after its task was submitted")
        self._last_number += 1
        self._numbers[task] = self._last_number
        self._declared_outputs[task] = tuple(graph._outputs.get(key, {}))
        submitted = False
        try:
            if self._local:
                self._run_local(task)
            else:
                plan = self._plan(task)
                self._enqueue(lambda: self._submit_node(plan))
            submitted = True
        finally:
            if not submitted:
                del self._numbers[task]
                del self._declared_outputs[task]

    def _plan(self, task):
        """Stage a task's arguments and manifest, and return what the background thread declares and mounts."""
        graph_id, key = task
        graph = self._graphs[graph_id]
        func, args, kwargs = graph._tasks[key]
        files = {library.file_identity(ref): self._file_task_path(library.file_identity(ref)) for ref in graph._reads[key]}
        staged = {
            "callable": self._stage_object(func),
            "args": [self._stage_object(value) for value in args],
            "kwargs": {name: self._stage_object(value) for name, value in kwargs.items()},
        }
        # The manifest names every file the task reads by its sandbox path.
        manifest = {
            "callable": staged["callable"][0],
            "args": [name for name, _ in staged["args"]],
            "kwargs": {argument: name for argument, (name, _) in staged["kwargs"].items()},
            "inputs": {parent: self._result_task_path(parent) for parent in graph._results[key]},
            "files": files,
        }
        return {
            "task": task,
            "outputs": self._declared_outputs[task],
            "results": [(parent, self._result_task_path(parent)) for parent in graph._results[key]],
            "files": list(files.items()),
            "edata": list(dict.fromkeys([staged["callable"], *staged["args"], *staged["kwargs"].values()])),
            "manifest": self._stage(manifest),
        }

    def _stage(self, value):
        """Serialize one argument object for the vault and return its sandbox name and local path."""
        self._last_number += 1
        name = f"vine-graph-edata-{self._last_number}"
        path = os.path.join(self._edata_dir, name)
        try:
            with open(path, "wb") as stream:
                cloudpickle.dump(value, stream)
        except BaseException:
            # A long-lived Executor would otherwise keep the partial file of a failed or interrupted submission.
            os.unlink(path)
            raise
        return name, path

    def _stage_object(self, value):
        # Share top-level objects by identity, not content. Independent pickle streams do not preserve aliases nested
        # across different objects or callable closures.
        if id(value) not in self._edata:
            self._edata[id(value)] = (value, *self._stage(value))
        return self._edata[id(value)][1:]

    def _enqueue(self, command):
        """Queue a call into C for the background thread. Garbage collection may call this from any thread."""
        with self._changed:
            if self._closed:
                return
            self._commands.append(command)
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
        for name, path in [*plan["edata"], plan["manifest"]]:
            if name not in self._edata_files:
                self._edata_files[name] = self._declare_file(path, vault=True)
        for name, _ in plan["edata"]:
            self._add_input(node, self._edata_files[name], name)
        self._add_input(node, self._edata_files[plan["manifest"][0]], library.MANIFEST)
        self._check(capi.vine_graph_executor_submit_node(self._executor, node), f"failed to submit task {key!r}")

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

    def _pin(self, key):
        file = self._file_of(key)
        self._check(capi.vine_graph_executor_pin_file(self._executor, file), "failed to pin a result")
        self._check(capi.vine_graph_executor_fetch_file(self._executor, file), "failed to fetch a result")

    def _unpin(self, key):
        capi.vine_graph_executor_unpin_file(self._executor, self._file_of(key))

    def _advance(self):
        """Run queued calls, advance the run for about a second, and publish what happened. Return False when the run
        stopped. Only the background thread calls this."""
        while True:
            with self._changed:
                if self._closed or not self._commands:
                    break
                command = self._commands.popleft()
            command()
            if isinstance(command, _Call):
                with self._changed:
                    self._changed.notify_all()
        if self._closed:
            return False
        status = capi.vine_graph_executor_wait(self._executor, 1)
        error = capi.vine_graph_executor_get_error(self._executor) if status == capi.VINE_GRAPH_FAILED else None
        self._publish_finished()
        with self._changed:
            for request in self._path_requests:
                if request.path is None:
                    request.path = capi.vine_graph_executor_get_local_path(self._executor, self._file_of(request.key))
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
        """Publish the tasks that completed or failed since the last call."""
        finished = []
        while node := capi.vine_graph_executor_next_finished(self._executor):
            task = self._tasks[node]
            if capi.vine_graph_executor_get_node_state(self._executor, node) == capi.VINE_GRAPH_NODE_FAILED:
                source = self._tasks[capi.vine_graph_executor_get_failure_source(self._executor, node)]
                finished.append((task, (FAILED, source, capi.vine_graph_executor_get_node_error(self._executor, node))))
            else:
                finished.append((task, (COMPLETED, None, None)))
        with self._changed:
            for task, entry in finished:
                self._set_finished(task, entry)
            self._changed.notify_all()

    # Published state, shared by both threads.

    def _set_finished(self, task, entry):
        previous = self._finished.get(task)
        if previous is not None:
            self._counts[previous[0]] -= 1
        self._finished[task] = entry
        self._counts[entry[0]] += 1

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
        pending = list(futures)
        while pending:
            remaining = None if deadline is None else deadline - time.monotonic()
            self._wait_until(lambda: any(future.done() for future in pending), remaining,
                             f"{len(pending)} futures are not done")
            done = [future.done() for future in pending]
            yield from (future for future, finished in zip(pending, done) if finished)
            pending = [future for future, finished in zip(pending, done) if not finished]

    def run(self, graph, targets=None):
        """Run the targets of a Graph, and every task they read, and return {target: result}. Targets are Nodes,
        NodeFiles, or Files, and default to the tasks no other task reads. A failure raises the TaskError of a task
        that failed on its own, in preference to the tasks that could not run because of it."""
        targets = graph.sinks() if targets is None else list(targets)
        futures = [self.submit(target) for target in targets]
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
        return Progress(len(self._numbers), self._counts[COMPLETED], self._counts[FAILED])

    def stats(self):
        """Return the makespan in seconds, the number of submitted tasks, and the number of recovery tasks."""
        self._check_open()
        if self._local:
            return {"makespan": self._local_seconds, "tasks": len(self._numbers), "recovery_tasks": 0}
        return self._call(lambda: {
            "makespan": capi.vine_graph_executor_get_makespan_us(self._executor) / 1e6,
            "tasks": len(self._numbers),
            "recovery_tasks": capi.vine_graph_executor_get_completed_recovery_tasks(self._executor),
        })

    def _wait_for(self, futures, timeout):
        self._wait_until(lambda: all(future.done() for future in futures), timeout, "the task is not done")

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
                    raise TimeoutError(unfinished)
                self._changed.wait(0.5 if remaining is None else min(remaining, 0.5))

    def _node(self, task):
        return Node(self._graphs[task[0]], task[1])

    def _error(self, task):
        if self._finished_state(task) != FAILED:
            return None
        _, source, message = self._finished_entry(task)
        task_node, source_node = self._node(task), self._node(source)
        # A function failure ends with its traceback. Lines the library writes before it are not about the function.
        start = message.find("Traceback (most recent call last):")
        message = f"\n{message[start:]}" if start >= 0 else f" {message}"
        if source == task:
            return TaskError(f"task {task_node!r} failed:{message}", task_node, source_node)
        return TaskError(f"task {task_node!r} did not run because task {source_node!r} failed:{message}", task_node,
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
        request = _PathRequest(key)
        with self._changed:
            self._path_requests.append(request)
            self._changed.notify_all()
        try:
            self._wait_until(lambda: request.path or self._finished_state(task) == FAILED, None,
                             "the result is not available")
        finally:
            with self._lock:
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
            entry = self._finished[parent]
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
            output = library.run_task(func, args, kwargs,
                                      lambda parent: library.load_result(self._local_result_path(parent)),
                                      self._local_file_path)
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
