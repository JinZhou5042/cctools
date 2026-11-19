# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""The runner of an Executor in this process (_Local), for debugging without TaskVine. A thread of its own runs the
tasks one at a time and publishes what finished as the runner on Workers does."""

import collections
import contextlib
import ctypes
import os
import shlex
import shutil
import signal
import subprocess
import _signal
import sys
import threading
import time
import weakref

import cloudpickle

from . import graph as _graph_module, library, store as _store_module
from .graph import Command, Expand, Function, _DEFAULT_OPTIONS, _FAILED, _callable, _place
from .runner import (
    _COMPLETED_ENTRY, _HISTORY_END, _INTERRUPT, _Lock, _Pickler, _Runner, _block_interrupts, _check_params, _check_task,
    _load_result, _read_file, _references, _expand_state, _expansion,
)

# Held while a local runner executes a task, so the local tasks of every Executor run one at a time. A local task
# changes the working directory of its thread, which has a working directory of its own (_own_working_directory()).
# Where a thread cannot have one, the task changes that of the process, and the lock is also held while a relative path
# is resolved (graph._working_directory_lock).
_LOCAL_TASKS = _Lock()


@contextlib.contextmanager
def _local_tasks_released():
    """Let other local tasks run while a local task waits for a local runner, as a task that runs a Graph on a local
    Executor of its own does, whose thread needs _LOCAL_TASKS to run the tasks it waits for. The waiting task runs no
    code meanwhile, so local tasks still run one at a time, and each restores the working directory it changed. The
    lock is taken back outside the runner's lock, so the two are never waited for in the opposite order."""
    if not _LOCAL_TASKS._is_owned():
        yield
        return
    state = _LOCAL_TASKS._release_save()
    try:
        yield
    finally:
        _LOCAL_TASKS._acquire_restore(state)


class _InputUnavailable(Exception):
    """A local Executor could not fetch an input of a task, which fails the task with reason "input"."""


class _Local(_Runner):
    """The runner of an Executor in this process, for debugging without TaskVine. A thread of its own runs the submitted
    tasks one at a time, in the order they were submitted, so a task runs after the tasks it reads while the program
    goes on, as with Workers. Each task runs in a sandbox directory of its own, and its function and arguments are
    serialized as on Workers, so it gets copies of them. Commands run with /bin/sh in the sandbox, which holds links to
    the files and results they read. The local tasks of every Executor run one at a time (_LOCAL_TASKS)."""

    port = None
    name = None

    def __init__(self, *, output_dir="./outputs", shared=None, store=None):
        super().__init__(output_dir, shared, store)
        self._queue = collections.deque()  # (record, Task) of submitted tasks that did not run yet, in submission order
        self._local_paths = {}  # (number of a record, sandbox path) -> local path of a file its task wrote
        self._results = {}  # number of a record -> local path of its task's result file
        self._downloads = {}  # URL -> local path of its copy in the run directory
        self._seconds = 0.0
        self._task_pickler = None  # of the runner's thread, which serializes the arguments of the tasks it runs
        self._thread = threading.Thread(target=_run_locally, args=(weakref.ref(self),), name="vine-graph-local",
                                        daemon=True)
        self._thread.start()

    def _close(self, remove_all):
        """Close, after the task that runs finished, and remove the run directory, keeping the paths that
        TaskFile.path returned unless remove_all is set."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._changed.notify_all()
        if self._thread is not threading.current_thread():
            self._thread.join()
        if self._store is not None:
            print(self._store.summary(), file=sys.stderr, flush=True)
        self._remove_run_dir(remove_all)

    def set_params(self, params):
        """Check settings as a runner on Workers does. A local runner has no Manager, so it applies none of them."""
        _check_params(params)

    def first_submission(self):
        return contextlib.nullcontext()

    def submit(self, task, record, commit):
        self._check_open()
        _check_task(task, self._shared)
        # The function and its arguments are serialized as a runner on Workers serializes them, so one that cannot be
        # serialized fails alike.
        if type(task._func) is Expand:
            pass
        elif not record.command:
            self._pickler.dumps(_callable(task._func))
            self._pickler.dumps((task._args, task._kwargs))
        # The names of its files are checked as a runner on Workers checks them.
        if task._reads:
            self._file_task_paths(task, {library.file_identity(ref): None for ref in task._reads})
        if self._store is not None and type(task._func) is not Expand:
            self._look_up(task, record)
        restore_interrupts = _block_interrupts()
        try:
            with self._lock:
                self._check_open()
                self._queue.append((record, task))
                self._unfinished += 1
                commit(task, record)
                self._changed.notify_all()
        finally:
            restore_interrupts()

    def cancel(self, records):
        """Report tasks that did not finish as cancelled: one that did not run never runs, and the outcome of one that
        runs is ignored. A task that reads a cancelled task is cancelled when its turn comes."""
        with self._lock:
            for record in records:
                self._finish(record, (_FAILED, record, "it was cancelled", "cancelled", None))

    def pin(self, record):
        """A local runner keeps every result while its record exists."""

    def unpin(self, record):
        """A local runner keeps every result while its record exists."""

    def hold(self, record, path=None):
        """A local runner reads results where its tasks wrote them."""

    def _finish(self, record, entry):
        """Publish that a task finished, unless it did before. Called with the lock held."""
        if record.finished is None:
            record.finished = entry
            self._events.append((record, entry))
            self._unfinished -= 1
            self._changed.notify_all()

    def _forget(self, record):
        """Let go of a record that nothing can ask for anymore, and remove what its task wrote unless a path of it was
        given out. Called by the runner's thread, between tasks, with the lock held."""
        if record.abandoned:
            return
        if record.finished is None:
            # The task did not run, and it never will.
            self._unfinished -= 1
            self._changed.notify_all()
        record.abandoned = True
        result = self._results.pop(record.number, None)
        paths = [self._local_paths.pop((record.number, path), None) for path in record.outputs]
        if result is None or any(path in self._keep_paths for path in paths):
            return
        if record.store is not None and record.store[1] is not None:
            # A hit read its result and files from the store, which keeps them.
            return
        if not record.command:
            try:
                os.unlink(result)
            except OSError:
                pass
        shutil.rmtree(os.path.join(self._run_dir, ".tasks", f"task-{record.number}"), ignore_errors=True)

    def _step(self):
        """Run the next submitted task, or wait up to a second for one. Return False once the runner closed. Only the
        runner's thread calls this."""
        with self._lock:
            dropped = self._dropped
            while dropped:
                self._forget(dropped.popleft())
            while True:
                if self._closed:
                    return False
                if not self._queue:
                    self._changed.wait(1)
                    return True
                record, task = self._queue.popleft()
                if record.finished is None and not record.abandoned:
                    break
        if self._task_pickler is None:
            self._task_pickler = _Pickler()
        entry = self._run(record, task)
        with self._lock:
            self._finish(record, entry)
        return True

    def _run(self, record, task):
        """Run a task, and return its entry."""
        for parent in task._parents:
            entry = parent._record.finished
            if entry is not None and entry[0] == _FAILED:
                # As in C, a task that reads a failed or cancelled task does not run, and names its source.
                return (_FAILED, entry[1], None, entry[3], None)
        if type(task._func) is Expand:
            return self._expand(record, task)
        if record.store is not None and record.store[1] is not None:
            entry = record.store[1]
            self._results[record.number] = os.path.join(entry, _store_module.RESULT)
            for path in record.outputs:
                self._local_paths[(record.number, path)] = os.path.join(entry, _store_module.FILES, path)
            return _COMPLETED_ENTRY
        entry = self._run_attempts(record, task)
        if entry is _COMPLETED_ENTRY and record.store is not None:
            items = [(_store_module.RESULT, self._results[record.number])]
            items.extend((os.path.join(_store_module.FILES, path), self._local_paths[(record.number, path)])
                         for path in record.outputs)
            try:
                self._store.write(record.store[0], items)
            except Exception as error:
                print(f"vine_graph: the store could not keep the result of {record.label}: {error}", file=sys.stderr,
                      flush=True)
        return entry

    def _expand(self, record, task):
        """Run the builder of an Expand, then the tasks it expands into, in order, in this thread, and gather their
        results as the Expand's result, as on Workers. Its tasks read results where they were written."""
        parents = _references(task) if task._parents else {}
        state = _expand_state(task, parents)

        def load_task_result(ref):
            source = state.sources[ref]
            path = self._results[source.number]
            return library.load_text_result(path) if source.command else library.load_result(path)

        def file_path(identity):
            if identity[0] != "output":
                return identity[1]
            return self._local_paths[(state.sources[(identity[1], identity[2])].number, identity[3])]

        with _LOCAL_TASKS:
            outcome = _expansion(self, record, state, load_task_result, file_path)
        if outcome is None:
            return (_FAILED, record, "its Graph was collected", "cancelled", None)
        if isinstance(outcome, str):
            return (_FAILED, record, outcome, "expand", None)
        tasks, gather = outcome
        graph = record.graph()
        with self._lock:
            self._unfinished += len(tasks)
            self._spawned += len(tasks)
            if graph is not None:
                graph._records.extend(child for _, child in tasks)
        del graph
        for child_task, child in tasks:
            try:
                _check_task(child_task, self._shared)
            except Exception as error:
                # As on Workers, a task that cannot be submitted fails the expansion before any of its tasks runs.
                for _, unrun in tasks:
                    with self._lock:
                        self._finish(unrun, (_FAILED, record, "it was cancelled", "cancelled", None))
                return (_FAILED, record, f"a task that its builder returned could not be submitted: {error}", "expand",
                        None)
        for child_task, child in tasks:
            if record.finished is not None:
                # The Expand was cancelled while it ran its tasks.
                entry = (_FAILED, record, "it was cancelled", "cancelled", None)
            else:
                entry = self._run(child, child_task)
            with self._lock:
                self._finish(child, entry)
        values = []
        for parent in gather._parents:
            entry = parent._record.finished
            if entry[0] == _FAILED:
                return (_FAILED, entry[1], None, entry[3], None)
            path = self._results[parent._record.number]
            values.append(library.load_text_result(path) if parent._record.command else library.load_result(path))
        result_path = os.path.join(self._output_directory(), f"result-{record.number}")
        library.save_result(result_path, values)
        self._results[record.number] = result_path
        return _COMPLETED_ENTRY

    def _run_attempts(self, record, task):
        """Run a task until it completes or fails for good, and return its entry."""
        kind = type(task._func)
        options = task._func._options if kind is Function or kind is Command else _DEFAULT_OPTIONS
        # Failures of the task itself are tried again as on Workers, each seen as the exception that stands for it.
        history = []
        while True:
            entry, error_class = self._run_once(record, task, len(history))
            if entry[0] != _FAILED or len(history) >= options.retries or error_class is None or \
                    not options.retries_cover(error_class):
                break
            history.append(entry[3])
        if history and entry[0] == _FAILED:
            history.append(entry[3])
            entry = (_FAILED, record, f"it failed {len(history)} times: {', '.join(history)}{_HISTORY_END}{entry[2]}",
                     *entry[3:])
        return entry

    def _run_once(self, record, task, attempt):
        """Run one attempt of a task in a new sandbox, and return its entry and the class of exception that stands for
        its failure, or None."""
        task_dir = os.path.join(self._output_directory(), ".tasks", f"task-{record.number}")
        if os.path.lexists(task_dir):
            # Moved aside first, since a process that the earlier attempt left may still write to it.
            aside = f"{task_dir}.attempt-{attempt}"
            os.rename(task_dir, aside)
            shutil.rmtree(aside, ignore_errors=True)
        os.makedirs(task_dir)
        # A task makes the directories of the files it writes, as on a Worker.
        started = time.time()
        try:
            if record.command:
                return self._run_command(task, record, task_dir)
            return self._run_function(task, record, task_dir)
        except _InputUnavailable as error:
            return (_FAILED, record, str(error), "input", None), None
        finally:
            self._seconds += time.time() - started

    def _run_function(self, task, record, task_dir):
        # The process runs one call at a time, so a Function's cores change nothing here.
        func, args, kwargs = _callable(task._func), task._args, task._kwargs
        serialized = self._task_pickler.dumps((args, kwargs))
        sources = {ref: parent._record for ref, parent in _references(task).items()} if task._parents else {}

        def load_parent(ref):
            source = sources[ref]
            path = self._results[source.number]
            return library.load_text_result(path) if source.command else library.load_result(path)

        def file_path(identity):
            if identity[0] == "input" or identity[0] == "url":
                local = identity[1] if identity[0] == "input" else self._download(identity[1])
                if identity[2] is None:
                    return local
                # A File with a name is reached under that name, as in a Worker's sandbox.
                link = os.path.join(task_dir, identity[2])
                if not os.path.lexists(link):
                    os.symlink(local, link)
                return link
            return self._local_paths[(sources[(identity[1], identity[2])].number, identity[3])]

        with _LOCAL_TASKS:
            previous_cwd = os.getcwd()
            os.chdir(task_dir)
            try:
                args, kwargs = cloudpickle.loads(serialized)
                output = library.run_task(func, args, kwargs, load_parent, file_path,
                                          lambda name: cloudpickle.loads(self._shared[name]), task._selections or ())
                for task_path in record.outputs:
                    local_path = os.path.abspath(os.path.join(task_dir, task_path))
                    if not os.path.lexists(local_path):
                        raise FileNotFoundError(f"the function did not write its output {task_path!r}")
                    self._local_paths[(record.number, task_path)] = local_path
                result_path = os.path.join(self._run_dir, f"result-{record.number}")
                library.save_result(result_path, output)
                self._results[record.number] = result_path
                return _COMPLETED_ENTRY, None
            except _InputUnavailable:
                raise
            except BaseException as error:
                # As on a Worker, anything the function raises, including an exit, fails the task.
                return (_FAILED, record, library.failure_report(error, __file__, library.__file__), "function",
                        None), type(error)
            finally:
                os.chdir(previous_cwd)

    def _run_command(self, task, record, task_dir):
        line, results, files = self._command_line(task, shlex.quote(sys.executable))
        for path, parent, _ in results:
            os.symlink(self._results[parent.number], os.path.join(task_dir, path))
        for identity, path, source in files:
            if identity[0] == "url":
                local = self._download(identity[1])
            else:
                local = identity[1] if source is None else self._local_paths[(source.number, identity[3])]
            os.symlink(local, os.path.join(task_dir, path))
        # A Worker tells a command the cores and memory it requested in CORES and MEMORY, and a command that requested
        # no GPUs sees none.
        options = task._func._options
        env = dict(os.environ, CORES=str(options.cores))
        if options.memory is not None:
            env["MEMORY"] = str(options.memory)
        if not options.gpus:
            env["CUDA_VISIBLE_DEVICES"] = ""
        # The command runs in a session of its own, so a time limit stops every process it started, as on a Worker.
        process = subprocess.Popen(["/bin/sh", "-c", line], cwd=task_dir, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=env, start_new_session=True)
        try:
            _, stderr = process.communicate(timeout=options.time_limit)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate()
            return (_FAILED, record, f"it ran longer than its time limit of {options.time_limit} s", "timeout",
                    None), TimeoutError
        if process.returncode != 0:
            error = stderr.decode("utf-8", "replace")
            return (_FAILED, record, f"its command exited with status {process.returncode}:\n{error}", "command",
                    process.returncode), subprocess.CalledProcessError
        for task_path in record.outputs:
            self._local_paths[(record.number, task_path)] = os.path.abspath(os.path.join(task_dir, task_path))
        self._results[record.number] = os.path.join(task_dir, library.RESULT)
        return _COMPLETED_ENTRY, None

    def _download(self, url):
        """Return the local path of a URL, downloaded once into the run directory, as a Worker fetches it once. A URL
        that cannot be fetched raises, which fails the task."""
        path = self._downloads.get(url)
        if path is None:
            directory = os.path.join(self._output_directory(), ".urls")
            os.makedirs(directory, exist_ok=True)
            path = os.path.join(directory, str(len(self._downloads) + 1))
            try:
                if url.startswith("root://"):
                    subprocess.run(["xrdcp", "--nopbar", "--force", url, path], check=True,
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                else:
                    import urllib.request
                    with urllib.request.urlopen(url, timeout=60) as response, open(path + ".part", "wb") as stream:
                        shutil.copyfileobj(response, stream)
                    os.rename(path + ".part", path)
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                raise _InputUnavailable(f"its input {url} could not be fetched: {error}") from None
            self._downloads[url] = path
        return path

    # Published state, read by the threads that use the Executor. A local task that waits for a local runner lets
    # other local tasks run meanwhile (_local_tasks_released()).

    def wait_events(self, timeout):
        with _local_tasks_released():
            _Runner.wait_events(self, timeout)

    def take_events(self, timeout=0):
        with _local_tasks_released():
            return _Runner.take_events(self, timeout)

    def drain(self):
        with _local_tasks_released():
            _Runner.drain(self)

    def result(self, record, timeout=None):
        return _load_result(record, self._results[record.number])

    def file_bytes(self, record, path, limit):
        return _read_file(self._local_paths[(record.number, path)], path, limit)

    def file_path(self, record, path):
        local = self._local_paths[(record.number, path)]
        self._keep_paths.add(local)
        return local

    def save_file(self, record, path, destination):
        _place(self._local_paths[(record.number, path)], destination)

    def capacity(self):
        return (1, 1, None, None)

    def stats(self):
        return {"makespan": self._seconds, "recovery_tasks": 0, "durability": None}


# The flag of unshare(2) that gives the calling thread a working directory of its own.
_CLONE_FS = 0x200


def _own_working_directory():
    """Give the calling thread a working directory of its own, so the local tasks it runs change the working directory
    of no other thread. Where the system does not allow it, local tasks change that of the process, and the lock of
    local tasks guards relative paths that the program's threads resolve meanwhile."""
    try:
        if ctypes.CDLL(None, use_errno=True).unshare(_CLONE_FS) == 0:
            return
    except (OSError, AttributeError):
        pass
    _graph_module._working_directory_lock = _LOCAL_TASKS


def _run_locally(runner_reference):
    """Run the tasks of a local runner until it closes. The thread holds the runner only while it runs a step, so a
    runner that nothing else refers to can still be collected and closed. It blocks SIGINT, as the background thread
    of a runner on Workers does (_drive())."""
    _signal.pthread_sigmask(_signal.SIG_BLOCK, _INTERRUPT)
    _own_working_directory()
    while True:
        runner = runner_reference()
        if runner is None or not runner._step():
            return
        del runner
