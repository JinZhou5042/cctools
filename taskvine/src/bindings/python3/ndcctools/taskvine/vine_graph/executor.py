# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""The public Executor, which makes the tasks it takes live (submit(tasks), run(tasks)). It works through one runner,
_Workers (workers.py) on TaskVine Workers or _Local (local.py) in this process, and applies what the runner publishes
to the records of the tasks under its lock in whichever thread asks how far a task is, waits for one, or iterates
as_completed()."""

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
    CONTENT_LIMIT, CancelledError, Task, TaskError, _CACHE, _CANCELLED, _COMPLETED, _FAILED, _FileRef, _NoSuchItem,
    _RUNNING, _Selection, _Symbol, TaskFile, _a, _label, _revive, _undeclared,
)
from .local import _Local
from .runner import (
    COMMAND_ERROR_TAIL, EDATA_CACHE, _FINAL, _HISTORY_END, _ORPHAN, _ORPHAN_NOTE, _SINK, _Lock, _Record,
    _block_interrupts, _check_params, _collect, _references, capi, cvine,
)
from .workers import _Workers

# A line that the library runtime logs to a function call's standard output (stdout_timed_message()): its progress,
# which a failure message leaves out, or an error, of which it keeps the text after the prefix.
LIBRARY_LOG_LINE = re.compile(r"^\d\d/\d\d/\d\d \d\d:\d\d:\d\d\.\d+ (?:TASK \d+ (?=error:)|.*\n?)", re.MULTILINE)


# The C library, whose standard output the progress bar writes to.
_libc = ctypes.CDLL(None)


class _ProgressBar:
    """The progress bar that Executor.run() draws over the tasks that did not finish when it started and those made
    live since, while standard output, which the C library writes it to, is a terminal."""

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


class _Count:
    """The tasks that one Executor.run() waits for: how many of them did not finish. The thread that applies the
    outcome of one counts it (Executor._finish()), which costs O(1) per finished task."""

    __slots__ = ("left",)

    def __init__(self):
        self.left = 0

    def finished(self, record):
        """Count a task that finished. Called with the Executor lock held."""
        self.left -= 1


class _Watch:
    """The tasks of one as_completed() call: those that did not finish wait in pending, by record, and the thread that
    applies the outcome of one moves it to ready (Executor._finish()), which costs O(calls that wait for it) per
    finished task. members holds the tasks of pending and ready by record, so that add() ignores a task it holds in
    O(1)."""

    __slots__ = ("pending", "ready", "members")

    def __init__(self):
        self.pending = set()
        self.ready = collections.deque()
        self.members = {}

    def finished(self, record):
        """Move a task that finished from pending to ready. Called with the Executor lock held."""
        self.pending.discard(record)
        self.ready.append(self.members[record])


class _AsCompleted:
    """The iterator that Executor.as_completed() returns. It yields each of its tasks once as it finishes, and add()
    gives it another task. The iteration ends when no task is left, and an iteration that ended continues after a later
    add(). The timeout of as_completed() covers the whole iteration."""

    __slots__ = ("_executor", "_watch", "_deadline")

    def __init__(self, executor, deadline):
        self._executor = executor
        self._watch = _Watch()
        self._deadline = deadline

    def __iter__(self):
        return self

    def __len__(self):
        """Return the number of tasks not yielded yet."""
        return len(self._watch.members)

    def add(self, task):
        """Add a live task of this Executor, which is yielded once it finished, or next when it finished already. A task
        the iterator holds and did not yield yet is ignored. Safe to call from another thread than the iteration."""
        executor = self._executor
        executor._check_task(task)
        watch = self._watch
        record = task._record
        finished = False
        with executor._lock:
            if record in watch.members:
                return
            watch.members[record] = task
            if record.state == _RUNNING:
                watch.pending.add(record)
                executor._watched.setdefault(record, []).append(watch)
            else:
                watch.ready.append(task)
                finished = True
        if finished:
            # A wait of another thread yields it now.
            executor._runner.poke()

    def update(self, tasks):
        """Add each of tasks, as add() does."""
        for task in tasks:
            self.add(task)

    def __next__(self):
        executor = self._executor
        runner = executor._runner
        watch = self._watch
        while True:
            with executor._lock:
                if not watch.ready:
                    if not watch.pending:
                        raise StopIteration
                    executor._apply(runner.take_events())
                if watch.ready:
                    task = watch.ready.popleft()
                    del watch.members[task._record]
                    return task
                pending = len(watch.pending)
                # An add() of a finished task after this point ends the wait below.
                pokes = runner._pokes
            remaining = None if self._deadline is None else self._deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(f"{pending} tasks did not finish in time")
            runner.wait_events(0.5 if remaining is None else min(remaining, 0.5), pokes)


def _targets(tasks, method):
    """Return (whether one Task was given, the list of the Tasks given) for the argument of Executor.submit() or
    Executor.run(), or raise TypeError for anything but a Task or an iterable of Tasks."""
    if isinstance(tasks, Task):
        return True, [tasks]
    takes = f"Executor.{method}() takes a task or an iterable of tasks, such as vg.Function(f, x) or [a_task, b_task]"
    if isinstance(tasks, _Symbol):
        raise TypeError(f"{takes}, not {tasks!r}; {_give_task(tasks)}")
    if isinstance(tasks, collections.abc.Mapping):
        raise TypeError(f"{takes}, not a dictionary; give it the tasks, such as list(tasks.values())")
    if callable(tasks) and getattr(tasks, "__name__", None):
        raise TypeError(f"{takes}; wrap {tasks.__name__} as vg.Function({tasks.__name__})")
    if isinstance(tasks, (str, bytes)):
        raise TypeError(f"{takes}, not {tasks!r}")
    try:
        targets = list(tasks)
    except TypeError:
        raise TypeError(f"{takes}, not {tasks!r}") from None
    for task in targets:
        if not isinstance(task, Task):
            hint = f"; {_give_task(task)}" if isinstance(task, _Symbol) else ""
            raise TypeError(f"{takes}, not an iterable that holds {task!r}{hint}")
    return False, targets


def _give_task(symbol):
    """Return what to do instead of giving Executor.submit() or Executor.run() a selection or a file of a task."""
    if isinstance(symbol, TaskFile):
        return "give it the task itself, and read the file from task.file(name)"
    if isinstance(symbol, _Selection):
        return "give it the task itself, and select from task.result()"
    return "give it a task"


class Executor:
    """Where tasks run: TaskVine Workers that connect to port, or to the Manager named name through the catalog, or,
    with local=True, this process, one task at a time in the order they were submitted, for debugging without Workers.

    A task is made by vg.Function, vg.Command, or vg.Expand, and the tasks among its arguments are the tasks it reads.
    submit(tasks) makes tasks live with every task they read and returns them at once, and run(tasks) does the same
    and waits until they finished. A Task is a future: given to another task, that task receives its result, and its
    result() gives the program its result, waiting for it. cancel() cancels it. A
    task that reads a Task starts once its input exists, so work whose dependencies are known is made up front, and a
    decision that depends on a result is ordinary control flow around result(): make more tasks and submit them, or
    make a vg.Expand, whose builder returns the tasks to run once its inputs exist, with no wait of the caller.
    as_completed() yields tasks as they finish, for reacting to whichever of several running tasks finishes first.
    One Executor runs any number of tasks on its Workers, and a task belongs to the first Executor that takes it. Use
    it in a with statement, or close() it, and read results before it closes:

        with vg.Executor(port=9123) as executor:
            load_task = vg.Function(load, "more.root")
            if executor.run(load_task).result() > 0:
                fit_task = vg.Function(fit, load_task).set(cores=4)
                print(executor.run(fit_task).result())

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

    Results. The result of a task that can still be asked for without a new task is kept: a task that no task read
    when it was submitted, until a task that reads it is submitted, a task with a declared file that no task reads,
    and every result or file that was read here. Other results are released once the tasks that read them are safe,
    and one of them that is asked for after all, by result(), by a file, or by a task submitted later, is computed
    again, which the caller does not see. A task is let go when its Task is collected, and a Task keeps the Tasks it
    reads."""

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
        # Tasks that this Executor made live, and how many of them completed, failed, and were cancelled, for progress().
        self._started = 0
        self._completed = 0
        self._failed = 0
        self._cancelled = 0
        # record of a live task that did not finish -> the _Watch of each as_completed(), and the _Count of each run(),
        # that waits for it
        self._watched = {}
        # number of a record -> the Task found or made for it when an error named it (_task_of()), while it exists
        self._known = weakref.WeakValueDictionary()
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
        # The tasks that Expands expanded into count as they go live.
        total = self._started + self._runner._spawned
        completed, failed, cancelled = self._completed, self._failed, self._cancelled
        return Progress(total, completed, failed, cancelled, max(total - completed - failed - cancelled, 0))

    def _check_open(self):
        self._runner._check_open()

    # Submission.

    def submit(self, tasks):
        """Make a task, or each of an iterable of tasks, live, with every task it reads, directly or through others,
        that is not live yet, each after the tasks it reads, and return at once what was given: the Task, or the list
        of the Tasks. A task that is live already stays as it is. A task belongs to the first Executor that takes it,
        and reads only tasks of that Executor."""
        single, targets = _targets(tasks, "submit")
        self._submit(targets)
        return targets[0] if single else targets

    def run(self, tasks):
        """Submit a task, or each of an iterable of tasks, as submit(tasks) does, wait until they finished, and return
        what was given, as submit(tasks) does: the Task, or the list of the Tasks, so run(task).result() is the result
        of one task. When one of them failed, it raises the TaskError of the task whose own failure explains it
        instead, and when one was cancelled, CancelledError. An interrupt leaves the tasks running, and a later run
        waits for them again."""
        single, targets = _targets(tasks, "run")
        self._submit(targets)
        self._wait_all(targets)
        cancelled = None
        for task in targets:
            state = task._record.state
            if state == _FAILED:
                # A source that completed, and whose output could not be recomputed, has no error of its own.
                source = self._source_of(task)
                raise task_error(source if source._record.state == _FAILED else task)
            if state == _CANCELLED and cancelled is None:
                cancelled = task
        if cancelled is not None:
            raise CancelledError(f"{cancelled!r} was cancelled, so it has no result")
        return targets[0] if single else targets

    def _submit(self, targets):
        """Make tasks live with the tasks they read that are not, each after the tasks it reads."""
        self._check_open()
        # A with block holds only calls: Python may raise an interrupt at the backward jump of a loop, which the
        # block's clean-up does not cover when the jump follows a branch, and the lock would stay held.
        with self._lock:
            self._submit_locked(targets)

    def _submit_locked(self, targets):
        order, readers = _collect(targets, self)
        if not order:
            return
        if self._store is None:
            for task in order:
                if task._flags & _CACHE:
                    raise ValueError(f"{task!r} has cache=True, which needs a store: vg.Executor(store=path)")
        with self._runner.first_submission():
            self._start_all(order, readers)

    def _start_all(self, order, readers):
        for task in order:
            # A task that an interrupt leaves out is not live, and the next submission takes it.
            self._start(task, readers[task])

    def _start(self, task, readers):
        """Make a task live: number it, submit it after the tasks it reads, and record why its result is kept. readers
        lists the tasks submitted with it that read it, or is None. A task whose parent failed or was cancelled fails
        or is cancelled at once without running, and so does one that cannot be submitted. Called with the lock
        held."""
        if task._record is not None:
            return
        task._executor = self
        # The number is taken before anything is queued, so a submission that an interrupt stopped after queuing, whose
        # task is submitted again under a new record, never shares its number, by which the runner knows records.
        record = _Record(task, self._runner.next_number())
        for parent in task._parents:
            source = parent._record
            if source.child == _ORPHAN:
                self._settle(task, record, (_FAILED, record, f"it reads {parent!r}, {_ORPHAN_NOTE}", "submit", None))
                return
            if source.state >= _FAILED:
                if source.state == _CANCELLED:
                    self._settle(task, record, (_FAILED, source, "it was cancelled", "cancelled", None))
                else:
                    self._settle(task, record, (_FAILED, source.finished[1] or source, None, source.finished[3], None))
                return
        # A result that may be asked for without a new task is pinned from the start: nothing reads it yet, or its
        # task writes a file that no task reads. Any other result is left to the release rule, and is computed again
        # when it is asked for after all.
        record.keep = 0 if readers else _SINK
        if record.outputs and _writes_unread_file(task, readers):
            record.keep |= _FINAL
        try:
            self._runner.submit(task, record, readers, self._commit)
        except KeyboardInterrupt:
            raise
        except Exception as error:
            # The task cannot run, such as when an argument cannot be serialized, and its readers fail with it.
            message = "".join(traceback.format_exception_only(error)).strip()
            self._settle(task, record, (_FAILED, record, f"it could not be submitted: {message}", "submit", None))

    def _commit(self, task, record):
        """Make a task live that its runner queued. The runner calls this with its lock held and interrupts held back,
        right after it queued the task, so a task is never queued without being live."""
        self._started += 1
        task._record = record
        # A task that now has a reader is no sink anymore, and the release rule waits for its reader.
        for parent in task._parents:
            source = parent._record
            if source.keep & _SINK:
                source.keep &= ~_SINK
                if not source.keep:
                    self._runner.unpin(source)

    def _settle(self, task, record, entry):
        """Make a task live that finished without running: cancelled, when entry says so, or failed with entry, the
        (_FAILED, record of the source, message, reason, exit code) that a runner publishes. Called with the lock
        held."""
        restore_interrupts = _block_interrupts()
        try:
            self._started += 1
            record.finished = entry
            if entry[3] == "cancelled":
                record.state = _CANCELLED
                self._cancelled += 1
            else:
                record.state = _FAILED
                self._failed += 1
            task._record = record
        finally:
            restore_interrupts()

    # What finished.

    def _apply(self, events):
        """Apply what finished to the records, from the runner's queue of events, and remove each event once it was
        applied. An interrupt that stops this leaves the events after it for the next call, and one between applying an
        event and removing it is harmless, since an event applies only to a running task. Called with the lock held."""
        while events:
            self._apply_event(*events[0])
            events.popleft()

    def _apply_event(self, record, entry):
        if record.state != _RUNNING:
            # A task that was cancelled stays cancelled whatever its submission did.
            return
        if entry[0] == _COMPLETED:
            record.state = _COMPLETED
            self._completed += 1
        elif entry[3] == "cancelled":
            # A task that reads a cancelled task is cancelled with it.
            record.state = _CANCELLED
            self._cancelled += 1
        else:
            record.state = _FAILED
            self._failed += 1
        self._finish(record)

    def _finish(self, record):
        """Hand a live task that finished to the as_completed() and run() calls that wait for it. Called with the lock
        held."""
        waiting = self._watched.pop(record, None)
        if waiting:
            for watch in waiting:
                watch.finished(record)

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
        record = task._record
        while True:
            with self._lock:
                self._apply(runner.take_events())
            if record.state != _RUNNING:
                return
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(f"{task!r} did not finish in time")
            runner.wait_events(0.5 if remaining is None else min(remaining, 0.5))

    def _wait_all(self, tasks):
        """Wait until every one of live tasks finished, applying what finishes meanwhile, with a progress bar over the
        tasks of the Executor that did not finish when the wait started and those made live since."""
        runner = self._runner
        count = _Count()
        bar = None
        try:
            with self._lock:
                before = self._count(tasks, count)
            while count.left:
                if self._progress_bar and (bar is not None or _stdout_is_terminal()):
                    total = self._started + runner._spawned - before
                    done = self._completed + self._failed + self._cancelled - before
                    if bar is None:
                        bar = _ProgressBar(total, done)
                    else:
                        bar.show(total, done)
                runner.wait_events(0.5)
                with self._lock:
                    self._apply(runner.take_events())
            if bar is not None:
                bar.show(self._started + runner._spawned - before,
                         self._completed + self._failed + self._cancelled - before)
        finally:
            if bar is not None:
                bar.finish()

    def _count(self, tasks, count):
        """Apply what finished, make count wait for the tasks that still run, and return how many tasks finished so
        far. Called with the lock held, as the only statement of its with block (see _submit())."""
        self._apply(self._runner.take_events())
        for record in {task._record for task in tasks}:
            if record.state == _RUNNING:
                count.left += 1
                self._watched.setdefault(record, []).append(count)
        return self._completed + self._failed + self._cancelled

    def as_completed(self, tasks, timeout=None):
        """Return an iterator over live tasks of this Executor that yields each as it finishes, whatever its outcome,
        and at once those that finished already, as concurrent.futures.as_completed() does. It is for reacting to
        whichever of several running tasks finishes first. A task that failed or was cancelled is yielded like one that
        completed, and its result() tells. With a timeout in seconds, the iteration raises TimeoutError when tasks are
        still unfinished that long after this call. The iterator's add(task) gives it another live task, for a
        controller whose next tasks depend on its own state (see _AsCompleted)."""
        deadline = None if timeout is None else time.monotonic() + timeout
        tasks = list(dict.fromkeys(tasks))
        for task in tasks:
            self._check_task(task)
        iterator = _AsCompleted(self, deadline)
        iterator.update(tasks)
        return iterator

    def _check_task(self, task):
        """Raise unless task is a live task of this Executor."""
        if not isinstance(task, Task):
            raise TypeError("as_completed() takes Tasks")
        if task._record is None:
            raise ValueError(f"{task!r} has not been submitted; submit it first")
        if task._executor is not self:
            raise ValueError(f"{task!r} belongs to another Executor")

    def _cancel(self, task):
        """Cancel a live task that did not finish, and return whether it was cancelled. The runner cancels the live
        tasks that read it, which report it, and a task that reads it is cancelled when it is submitted."""
        self._poll()
        record = task._record
        with self._lock:
            if record.state != _RUNNING:
                return False
            restore_interrupts = _block_interrupts()
            try:
                record.state = _CANCELLED
                self._cancelled += 1
                self._finish(record)
                self._runner.cancel([record])
            finally:
                restore_interrupts()
            return True

    # Results.

    def _completed_record(self, task, deadline, what="result"):
        """Return the record of a completed task, waiting for a task that still runs, or raise why the task has no
        result."""
        runner = self._runner
        record = task._record
        if record.state == _RUNNING and not runner.closed:
            self._wait(task, deadline)
        if record.state == _FAILED:
            raise task_error(task)
        if record.state == _CANCELLED:
            raise CancelledError(f"{task!r} was cancelled, so it has no result")
        if runner.closed:
            cause = runner.closed_because()
            raise RuntimeError(f"The Executor that ran {task!r} is closed{cause}, so its {what} is gone. Read results "
                               f"before close() or the end of the with block.")
        if record.child == _ORPHAN:
            raise RuntimeError(f"{task!r} is {_ORPHAN_NOTE}")
        return record

    def _result(self, task, timeout):
        deadline = None if timeout is None else time.monotonic() + timeout
        record = self._completed_record(task, deadline)
        try:
            value = self._runner.result(record, None if deadline is None else max(deadline - time.monotonic(), 0))
        except TaskError as error:
            self._name_tasks(error, task)
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
            self._name_tasks(error, task)
            raise

    def _name_tasks(self, error, task):
        """Give a TaskError that a runner raised about the result of a task its Tasks, which a runner does not hold:
        the task, and as its source the task that failed, when its output was lost and could not be made again."""
        if error.task is None:
            error.task = task
            entry = task._record.finished
            error.source = self._source_of(task) if entry is not None and entry[0] == _FAILED else task

    def _source_of(self, task):
        """Return the Task whose own failure explains that of a task whose record says that it failed: the task
        itself, or the task of the record that its entry names as the source."""
        source = task._record.finished[1]
        if source is None or source is task._record:
            return task
        return self._task_of(source, task)

    def _task_of(self, source, task):
        """Return the Task of source, the record of the task whose failure made a task fail. Records hold no Tasks,
        but a Task holds the Tasks it reads, and each task on the way from the task to the source failed for the same
        source, so the way is found from the task, once for each source, O(tasks on the way and their arguments). The
        Task of a task that an Expand expanded into, which exists only for the Expand, is made from its record."""
        known = self._known.get(source.number)
        if known is not None:
            return known
        found = None
        current = task
        while found is None:
            for parent in current._parents:
                record = parent._record
                if record is source:
                    found = parent
                    break
                entry = record.finished
                if entry is not None and entry[0] == _FAILED and entry[1] is source:
                    current = parent
                    break
            else:
                break
        if found is None:
            # The way ends at the Expand whose expansion the source is a task of, or at a Task made for a task of
            # one, which holds the Task that lets go of the expansion, as the new Task does.
            found = _revive(source, self, getattr(current, "_owner", None) or current)
        return self._known.setdefault(source.number, found)


def _writes_unread_file(task, readers):
    """Return whether a task declared a file that none of readers, the tasks submitted with it that read it, reads,
    which only the caller can ask for."""
    if not readers:
        return True
    read = set()
    key = task.key
    for reader in readers:
        for ref in reader._reads or ():
            if type(ref) is _FileRef and ref.key == key:
                read.add(ref.path)
    return len(read) < len(task._outputs)


def task_error(task):
    """Return the TaskError of a failed task, which names the task whose own failure explains it."""
    record = task._record
    entry = record.finished
    if entry is None or entry[0] != _FAILED:
        entry = (_FAILED, record, "it failed", None, None)
    source = task._executor._source_of(task) if entry[1] is not None and entry[1] is not record else task
    # A source that completed, and whose output could not be recomputed, has no error of its own.
    source_entry = source._record.finished
    if source_entry is None or source_entry[0] != _FAILED:
        source_entry = entry
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
