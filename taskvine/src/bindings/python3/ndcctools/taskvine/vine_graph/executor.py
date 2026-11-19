# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""The public Executor, which makes tasks live: those of a Graph it takes (submit(graph), run(graph)) and single calls
(submit(func, ...)), which all behave alike from then on. It works through one runner, _Workers (workers.py) on
TaskVine Workers or _Local (local.py) in this process, and applies what the runner publishes to the Tasks under its
lock in whichever thread asks how far a task is, waits for one, or iterates as_completed()."""

import collections
import ctypes
import dataclasses
import os
import re
import sys
import time
import traceback
import weakref

from . import library, store as _store_module
from .graph import (
    CONTENT_LIMIT, CancelledError, Command, Graph, Task, TaskError, _CANCELLED, _COMPLETED, _Calls, _FAILED, _FileRef,
    _NEW, _NoSuchItem, _RUNNING, _UNREAD, _a, _label, _readers_of, _undeclared,
)
from .local import _Local
from .runner import (
    COMMAND_ERROR_TAIL, EDATA_CACHE, _FINAL, _HISTORY_END, _SINK, _Lock, _Record, _block_interrupts, _check_params,
    _references, _task_of, capi, cvine,
)
from .workers import _Workers

# A line that the library runtime logs to a function call's standard output (stdout_timed_message()): its progress,
# which a failure message leaves out, or an error, of which it keeps the text after the prefix.
LIBRARY_LOG_LINE = re.compile(r"^\d\d/\d\d/\d\d \d\d:\d\d:\d\d\.\d+ (?:TASK \d+ (?=error:)|.*\n?)", re.MULTILINE)


# The C library, whose standard output the progress bar writes to.
_libc = ctypes.CDLL(None)


class _ProgressBar:
    """The progress bar that Executor.run() draws over the tasks of its Graph while standard output, which the C
    library writes it to, is a terminal."""

    def __init__(self, total, done):
        self.bar = capi.progress_bar_init("Executing Tasks")
        capi.progress_bar_set_update_interval(self.bar, 0.1)
        self.total = total
        self.done = 0
        self.part = capi.progress_bar_create_part("Tasks", total)
        capi.progress_bar_bind_part(self.bar, self.part)
        self.show(total, done)

    def show(self, total, done):
        if total != self.total:
            self.total = total
            capi.progress_bar_set_part_total(self.bar, self.part, total)
        if done > self.done:
            capi.progress_bar_update_part(self.bar, self.part, done - self.done)
            self.done = done

    def finish(self):
        sys.stdout.flush()
        capi.progress_bar_finish(self.bar)
        capi.progress_bar_delete(self.bar)
        # The bar writes to the C library's standard output, which Python's own buffer does not flush.
        _libc.fflush(None)


@dataclasses.dataclass(frozen=True, slots=True)
class Capacity:
    """The Workers connected to an Executor and their resources: Workers, cores, MB of memory, and GPUs, the last two
    None for a local Executor. Fields may be added; read them by name."""

    workers: int
    cores: int
    memory: int
    gpus: int


@dataclasses.dataclass(frozen=True, slots=True)
class Progress:
    """How many tasks this Executor made live and how they ended. Fields may be added; read them by name."""

    total: int
    completed: int
    failed: int
    cancelled: int
    waiting: int


class _Watch:
    """The tasks of one as_completed() call: those that did not finish wait in pending, and the thread that applies the
    outcome of one moves it to ready (Executor._finish()), which costs O(calls that wait for it) per finished task."""

    __slots__ = ("pending", "ready")

    def __init__(self):
        self.pending = set()
        self.ready = collections.deque()

    def finished(self, task):
        """Move a task that finished from pending to ready. Called with the Executor lock held."""
        self.pending.discard(task)
        self.ready.append(task)


class Executor:
    """Where tasks run: TaskVine Workers that connect to port, or to the Manager named name through the catalog, or,
    with local=True, this process, one task at a time in the order they were submitted, for debugging without Workers.

    submit(func, *args) starts one task at once and returns its Task. submit(graph) makes the tasks of a Graph live and
    returns at once, and run(graph) does so, waits for them, and returns the results of the Graph's sinks. From then on
    every Task behaves alike, whatever made it: result() waits for it, and cancel() cancels it. A task that reads a Task
    starts once its input exists, so work whose dependencies are known is submitted up front, and a decision that
    depends on a result is ordinary control flow around result(). as_completed() yields tasks as they finish, for
    reacting to whichever of several running tasks finishes first. One Executor runs any number of
    Graphs and submitted tasks on its Workers. Use it in a with statement, or close() it, and read results before it
    closes:

        with vg.Executor(port=9123) as executor:
            results = executor.run(graph)
            task = executor.submit(load, "more.root")
            if task.result() > 0:
                executor.submit(fit, task)

    Python tasks run in a TaskVine library on the Workers, and commands as standard TaskVine tasks. The library imports
    library_modules when it starts and loads shared, a dictionary from names to objects that tasks take as
    vg.Shared(name). library_files maps local paths to names in the library sandbox, where the library and its tasks
    import them as modules. progress_bar draws a progress bar while run() waits, when standard output is a terminal.
    output_dir holds the directory of the file paths that TaskFile.path returns, which stay after the Executor closed,
    and is made when the first is asked for. params is the escape hatch to TaskVine tuning: settings by name, applied
    once, whose names follow TaskVine and may change with it. They are those of the graph executor (EXECUTOR_SETTINGS),
    edata-cache, a budget in bytes of function and argument objects of the first submission that the library loads
    when it starts, and every other name as TaskVine Manager tuning. ssl, staging_path, run_info_path and
    run_info_template are passed to the TaskVine Manager. Every other Manager setting is a TaskVine tuning name in
    params. All arguments are keyword-only.

    Results. The result of a task that can still be asked for without a new task is kept: a sink of a Graph, a
    submitted task that no submitted task reads yet, and every result or file that was read here. Other results are
    released once the tasks that read them are safe, and one of them that is asked for after all, by result(), by a
    file, or by a task added or submitted later, is computed again, which the caller does not see. A submitted task is
    let go when nothing refers to its Task anymore, and the tasks of a Graph when the Graph is collected."""

    def __init__(self, *, port=cvine.VINE_DEFAULT_PORT, name=None, local=False, progress_bar=True, shared=None,
                 params=None, library_modules=(), library_files=None, output_dir="./outputs", ssl=None,
                 staging_path=None, run_info_path="vine-run-info", run_info_template=None, store=None):
        self._runner = None
        params = dict(params or {})
        # A bad setting raises before a Manager starts for it.
        _check_params(params)
        edata_cache = params.pop(EDATA_CACHE, 0)
        self._progress_bar = bool(progress_bar)
        self._local = bool(local)
        # Held while task states change, which happens in the thread that holds it.
        self._lock = _Lock()
        self._last_task = 0  # the number of the latest record, which numbers the tasks in the order they went live
        # Tasks that this Executor made live, and how many of them completed, failed, and were cancelled, for progress().
        self._started = 0
        self._completed = 0
        self._failed = 0
        self._cancelled = 0
        # record of a live task that did not finish -> the _Watch of each as_completed() that waits for it
        self._watched = {}
        self._store = None if store is None else _store_module.Store(store)
        if local:
            # A local runner has no Manager, so it takes only what it uses.
            runner = _Local(output_dir=output_dir, shared=shared, store=self._store)
        else:
            runner = _Workers(port, name, output_dir=output_dir, library_modules=library_modules,
                              library_files=library_files, shared=shared, edata_cache=edata_cache, ssl=ssl,
                              staging_path=staging_path, run_info_path=run_info_path,
                              run_info_template=run_info_template, store=self._store)
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
            # Tasks that still run finish first.
            if not runner.closed:
                try:
                    runner.drain()
                    with self._lock:
                        self._apply(runner.take_events())
                except RuntimeError:
                    pass
        finally:
            runner.close()

    # A call with a positional argument or an unknown keyword raises before __init__ sets it.
    _runner = None

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
        TaskFile.path returned stay in output_dir. Leaving a with block without an exception first waits for the tasks
        that still run, and leaving it with one discards those file paths too. A local Executor finishes the task that
        runs. Closing a closed Executor does nothing."""
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
        """Return the Workers connected and their resources as a Capacity with the fields workers, cores, memory (MB),
        and gpus, read about once a second. A local Executor has one Worker of one core, and None for memory and
        gpus."""
        self._check_open()
        return Capacity(*self._runner.capacity())

    def progress(self):
        """Return how far the tasks are that this Executor made live, as a Progress with the fields total, completed,
        failed, cancelled, and waiting, the tasks that did not finish yet."""
        self._poll()
        total, completed, failed, cancelled = self._started, self._completed, self._failed, self._cancelled
        return Progress(total, completed, failed, cancelled, max(total - completed - failed - cancelled, 0))

    def _check_open(self):
        self._runner._check_open()

    # Submission.

    def submit(self, func, /, *args, **kwargs):
        """Start a task that calls func, a function or a Function, with these arguments, or runs a Command with values
        for its placeholders, and return its Task at once. Files and live Tasks of this Executor, with their selections
        and files, may appear anywhere inside the arguments of a function. A task that cannot start, as when an
        argument cannot be serialized, fails, and its result() raises.

        submit(graph) makes every task of a Graph that is not submitted yet live, after the tasks it reads, and returns
        the Graph at once. Tasks added to the Graph later become live at its next submit() or run(). A Graph belongs to
        the first Executor that takes it."""
        if isinstance(func, Graph):
            if args or kwargs:
                raise TypeError("submit(graph) takes the Graph alone; Graph.add() adds tasks to it")
            self._submit_graph(func)
            return func
        self._check_open()
        self._check_cache(func)
        with self._lock:
            task = self._calls._add(self._last_task + 1, func, args, kwargs)
            with self._runner.first_submission():
                self._start(task)
        return task

    def run(self, graph):
        """Submit the tasks of a Graph that are not submitted yet, as submit(graph) does, wait until every task of the
        Graph finished, and return {sink: result} of its sinks, the tasks of the Graph that no task of it reads, that
        completed. When a sink failed, it raises the TaskError of the task whose own failure explains it instead, and so
        does every later run of the Graph. An interrupt leaves the tasks running, and a later run waits for them
        again."""
        if not isinstance(graph, Graph) or isinstance(graph, _Calls):
            raise TypeError("run() takes a Graph")
        self._submit_graph(graph)
        self._wait_graph(graph)
        sinks = []
        for task in list(graph._tasks.values()):
            if not task._readers:
                if task._state == _COMPLETED:
                    sinks.append(task)
                elif task._state == _FAILED:
                    # A source that completed, and whose output could not be recomputed, has no error of its own.
                    source = task._error[1]
                    raise task_error(source if source is not None and source._error is not None else task)
        for task in sinks:
            if task._value is _UNREAD:
                self._runner.hold(task._record)
        return {task: task.result() for task in sinks}

    def _submit_graph(self, graph):
        """Make the tasks of a Graph that are not submitted yet live, in the order they were added, which puts each
        after the tasks it reads."""
        self._check_open()
        bound = graph._executor
        if bound is not self:
            if bound is not None:
                if bound._runner.closed:
                    raise RuntimeError("the Executor that this Graph belongs to was closed, so the results of its "
                                       "tasks are gone; build a new Graph to run them again")
                raise ValueError("this Graph belongs to another Executor, which holds the results of its tasks; a "
                                 "Graph runs on one Executor")
            self._bind(graph)
        new = graph._new
        if not new:
            return
        for task in new:
            self._check_cache(task._func)
        with self._lock:
            with self._runner.first_submission():
                while new:
                    # A task leaves the queue only once it is live, so an interrupt leaves it for the next submission.
                    self._start(new[0])
                    new.popleft()

    def _check_cache(self, func):
        """Raise ValueError for a Function or Command of cache=True on an Executor without a store."""
        if getattr(func, "_cache", False) and self._store is None:
            raise ValueError(f"{func!r} has cache=True, which needs a store: vg.Executor(store=path)")

    def _bind(self, graph):
        """Make a Graph belong to this Executor, which lets go of the records of its tasks once the Graph is
        collected. The finalizer holds the list of the records and the runner's queue, not the Graph."""
        graph._executor = self
        weakref.finalize(graph, self._runner._dropped.extend, graph._records).atexit = False

    def _start(self, task):
        """Make a task live: number it, submit it after the tasks it reads, and record why its result is kept. A task
        whose parent failed or was cancelled fails or is cancelled at once without running, and so does one that cannot
        be submitted. Called with the lock held."""
        if task._state != _NEW:
            return
        record = _Record(task._graph._ref, task.key)
        # The number is taken before anything is queued, so a submission that an interrupt stopped after queuing, whose
        # task is submitted again under a new record, never shares its number, by which the runner knows records.
        self._last_task += 1
        record.number = self._last_task
        record.func = task._func
        record.command = isinstance(task._func, Command)
        record.outputs = tuple(task._outputs) if task._outputs else ()
        parent = next((parent for parent in task._parents if parent._state >= _FAILED), None)
        if parent is not None:
            if parent._state == _CANCELLED:
                self._settle(task, record, (_FAILED, parent, "it was cancelled", "cancelled", None))
            else:
                self._settle(task, record, (_FAILED, parent._error[1] or parent, None, parent._error[3], None))
            return
        # A result that may be asked for without a new task is pinned from the start: nothing reads it yet, or its
        # task writes a file that no task reads. Any other result is left to the release rule, and is computed again
        # when it is asked for after all.
        record.keep = 0 if task._readers else _SINK
        if task._outputs and _writes_unread_file(task):
            record.keep |= _FINAL
        try:
            self._runner.submit(task, record, self._commit)
        except KeyboardInterrupt:
            raise
        except Exception as error:
            # The task cannot run, such as when an argument cannot be serialized, and its readers fail with it.
            message = "".join(traceback.format_exception_only(error)).strip()
            self._settle(task, record, (_FAILED, None, f"it could not be submitted: {message}", "submit", None))

    def _commit(self, task, record):
        """Make a task live that its runner queued. The runner calls this with its lock held and interrupts held back,
        right after it queued the task, so a task is never queued without being live."""
        self._started += 1
        task._record = record
        task._state = _RUNNING
        graph = task._graph
        graph._unfinished += 1
        if graph._dropped is None:
            graph._records.append(record)
        # A task that now has a reader is no sink anymore, and the release rule waits for its reader.
        for parent in task._parents:
            source = parent._record
            if source.keep & _SINK:
                source.keep &= ~_SINK
                if not source.keep:
                    self._runner.unpin(source)

    def _settle(self, task, record, error):
        """Make a task live that finished without running: cancelled, when error says so, or failed with error, the
        (_FAILED, source, message, reason, exit code) of Task._error, whose source is None for the task itself. Called
        with the lock held."""
        restore_interrupts = _block_interrupts()
        try:
            self._started += 1
            source = record if error[1] is None else error[1]._record
            record.finished = (_FAILED, source, *error[2:])
            task._record = record
            if error[3] == "cancelled":
                task._state = _CANCELLED
                self._cancelled += 1
            else:
                task._state = _FAILED
                self._failed += 1
                task._error = error
            graph = task._graph
            if graph._dropped is None:
                graph._records.append(record)
            self._finish(task)
        finally:
            restore_interrupts()

    # What finished.

    def _apply(self, events):
        """Apply what finished to the Tasks, from the runner's queue of events, and remove each event once it was
        applied. An interrupt that stops this leaves the events after it for the next call, and one between applying an
        event and removing it is harmless, since an event applies only to a running task. Called with the lock held."""
        while events:
            self._apply_event(*events[0])
            events.popleft()

    def _apply_event(self, record, entry):
        if record.abandoned:
            return
        task = _task_of(record)
        if task is None or task._record is not record or task._state != _RUNNING:
            # A task that was cancelled stays cancelled whatever its submission did.
            return
        if entry[0] == _COMPLETED:
            task._state = _COMPLETED
            self._completed += 1
        elif entry[3] == "cancelled":
            # A task that reads a cancelled task is cancelled with it.
            task._state = _CANCELLED
            self._cancelled += 1
        else:
            source = entry[1]
            task._state = _FAILED
            self._failed += 1
            task._error = (_FAILED, None if source is record else _task_of(source), entry[2], entry[3], entry[4])
        task._graph._unfinished -= 1
        self._finish(task)

    def _finish(self, task):
        """Hand a live task that finished to the as_completed() calls that wait for it. Called with the lock held."""
        waiting = self._watched.pop(task._record, None)
        if waiting:
            for watch in waiting:
                watch.finished(task)

    def _poll(self):
        """Apply what finished, without waiting, for a caller that asks how far a task is."""
        if self._runner.closed:
            return
        try:
            with self._lock:
                self._apply(self._runner.take_events())
        except RuntimeError:
            # A run that stopped as a whole finishes nothing more, which result() reports.
            pass

    def _wait(self, task, deadline):
        """Wait until a live task finished, applying what finishes meanwhile."""
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

    def _wait_graph(self, graph):
        """Wait until every live task of a Graph finished, applying what finishes meanwhile, with a progress bar."""
        runner = self._runner
        bar = None
        try:
            while True:
                with self._lock:
                    self._apply(runner.take_events())
                    unfinished = graph._unfinished
                if unfinished and bar is None and self._progress_bar and _stdout_is_terminal():
                    bar = _ProgressBar(len(graph._tasks), len(graph._tasks) - unfinished)
                elif bar is not None:
                    bar.show(len(graph._tasks), len(graph._tasks) - unfinished)
                if not unfinished:
                    return
                runner.wait_events(0.5)
        finally:
            if bar is not None:
                bar.finish()

    def as_completed(self, tasks, timeout=None):
        """Return an iterator over live tasks of this Executor that yields each as it finishes, whatever its outcome,
        and at once those that finished already, as concurrent.futures.as_completed() does. It is for reacting to
        whichever of several running tasks finishes first. A task that failed or was cancelled is yielded like one that
        completed, and its result() tells. With a timeout in seconds, the iteration raises TimeoutError when tasks are
        still unfinished that long after this call."""
        deadline = None if timeout is None else time.monotonic() + timeout
        tasks = list(dict.fromkeys(tasks))
        for task in tasks:
            if not isinstance(task, Task):
                raise TypeError("as_completed() takes Tasks")
            if task._record is None:
                raise ValueError(f"{task!r} has not been submitted; submit or run its Graph")
            if task._graph._executor is not self:
                raise ValueError(f"{task!r} belongs to another Executor")
        watch = _Watch()
        with self._lock:
            for task in tasks:
                if task._state == _RUNNING:
                    watch.pending.add(task)
                    self._watched.setdefault(task._record, []).append(watch)
                else:
                    watch.ready.append(task)
        return self._as_completed(watch, deadline)

    def _as_completed(self, watch, deadline):
        runner = self._runner
        while True:
            with self._lock:
                if not watch.ready:
                    if not watch.pending:
                        return
                    self._apply(runner.take_events())
                task = watch.ready.popleft() if watch.ready else None
                pending = len(watch.pending)
            if task is not None:
                yield task
                # The iterator does not keep a task it yielded.
                task = None
                continue
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(f"{pending} tasks did not finish in time")
            runner.wait_events(0.5 if remaining is None else min(remaining, 0.5))

    def _cancel(self, task):
        """Cancel a live task that did not finish, and return whether it was cancelled. The runner cancels the live
        tasks that read it, which report it, and a task of a Graph that reads it is cancelled when it is submitted."""
        self._poll()
        with self._lock:
            if task._state != _RUNNING:
                return False
            restore_interrupts = _block_interrupts()
            try:
                task._state = _CANCELLED
                self._cancelled += 1
                task._graph._unfinished -= 1
                self._finish(task)
                self._runner.cancel([task._record])
            finally:
                restore_interrupts()
            return True

    # Results.

    def _completed_record(self, task, deadline, what="result"):
        """Return the record of a completed task, waiting for a task that still runs, or raise why the task has no
        result."""
        runner = self._runner
        if task._state == _RUNNING and not runner.closed:
            self._wait(task, deadline)
        if task._state == _FAILED:
            raise task_error(task)
        if task._state == _CANCELLED:
            raise CancelledError(f"{task!r} was cancelled, so it has no result")
        if runner.closed:
            cause = runner.closed_because()
            raise RuntimeError(f"The Executor that ran {task!r} is closed{cause}, so its {what} is gone. Read results "
                               f"before close() or the end of the with block.")
        return task._record

    def _result(self, task, timeout):
        deadline = None if timeout is None else time.monotonic() + timeout
        record = self._completed_record(task, deadline)
        try:
            value = self._runner.result(record, None if deadline is None else max(deadline - time.monotonic(), 0))
        except TaskError as error:
            if error.task is None:
                error.task = error.source = task
            raise
        # The Task keeps the result it returned, so it stays readable after the Executor closed.
        task._value = value
        return value

    def _file(self, file, what, destination=None):
        task = file.task
        record = self._completed_record(task, None, f"file {file.name!r}")
        if file.name not in record.outputs:
            raise ValueError(_undeclared(task, file.name))
        try:
            if what == "path":
                return self._runner.file_path(record, file.name)
            if what == "save":
                return self._runner.save_file(record, file.name, destination)
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


def task_error(task):
    """Return the TaskError of a failed task, which names the task whose own failure explains it."""
    entry = task._error
    source = entry[1] or task
    source_entry = source._error if source._error is not None else entry
    message, reason, exit_code = source_entry[2], source_entry[3], source_entry[4]
    message, exception = library.load_failure(message or "")
    # A task that was tried again says how its attempts failed before the error of its last one.
    history = None
    if message.startswith("it failed ") and _HISTORY_END in message:
        history, _, message = message.partition(_HISTORY_END)
    task_label, source_label = _label(task), _label(source)
    if isinstance(exception, _NoSuchItem):
        # The message names the two tasks, which the Worker that raised it cannot, and needs no traceback, because
        # the selection is in the arguments of the task and not in its function.
        parent = _references(source).get(exception.task) if source._parents else None
        message = (f"{source_label} selects [{exception.step}] from the result of "
                   f"{'a task' if parent is None else _label(parent)}, {_a(exception.kind)}, which has no such item")
        # A value with items keeps the class of what it raised, such as KeyError, and a value without items raises
        # LookupError.
        error_class = exception.error if isinstance(exception.error, type) and issubclass(exception.error, Exception) \
            else LookupError
        try:
            exception = error_class(message)
        except Exception:
            exception = LookupError(message)
        message = f" {message}"
    else:
        message = _failure_text(message, exception, exit_code, source_label)
        if history is not None:
            message = f" {history}{_HISTORY_END.rstrip()}:{message}"
    if source is task:
        return TaskError(f"task {task_label} failed:{message}", task, source, reason, exception, exit_code)
    return TaskError(f"task {task_label} did not run because task {source_label} failed:{message}", task, source,
                     reason, exception, exit_code)


def _failure_text(message, exception, exit_code, source_label):
    """Return the part of a TaskError's message that tells how its source failed. A function failure ends with its
    traceback. Lines the library writes before it are not about the function, and neither are its timestamped log
    lines in a message without a traceback. A command's message is its standard error, of which the end tells what went
    wrong."""
    start = message.find("Traceback (most recent call last):")
    if start >= 0:
        message = f"\n{message[start:]}"
        if exception is not None:
            exception.add_note(f"raised by task {source_label}:{message.rstrip()}")
    elif exit_code is not None:
        head, _, error = message.partition("\n")
        if len(error) > COMMAND_ERROR_TAIL:
            error = "...\n" + error[-COMMAND_ERROR_TAIL:]
        message = f" {head}\n{error}" if error else f" {head}"
    else:
        message = f" {LIBRARY_LOG_LINE.sub('', message)}"
    note = _missing_module_note(exception)
    if note:
        message = f"{message.rstrip()}\n{note}"
        exception.add_note(note)
    return message


def _missing_module_note(exception):
    """Return advice for a task that failed because its Worker cannot import a module that this process imports from
    a file of its own, outside the installed packages, or None for any other failure."""
    if not isinstance(exception, ModuleNotFoundError) or not exception.name:
        return None
    name = exception.name.partition(".")[0]
    origin = _store_module.own_module_file(name)
    if not origin:
        return None
    if os.path.basename(origin) == "__init__.py":
        origin = os.path.dirname(origin)
    return (f"The module {name!r} is a local file of this program ({origin}), which Workers do not have. Give it to the "
            f"Executor with library_files={{{origin!r}: {os.path.basename(origin)!r}}} and "
            f"library_modules=[{name!r}], or install it in the environment of the Workers.")


def _stdout_is_terminal():
    """Return whether the standard output of the process, which the C library writes the progress bar to, is a
    terminal."""
    try:
        return os.isatty(1)
    except OSError:
        return False
