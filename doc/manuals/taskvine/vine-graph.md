# Vine Graph

Vine Graph runs Python function calls and shell commands on TaskVine, either
as a graph built in advance or as tasks started one at a time while others
run.

```python
import ndcctools.taskvine.vine_graph as vg


def double(value):
    return 2 * value


graph = vg.Graph()
parts = [graph.add(double, value) for value in range(3)]
total = graph.add(sum, parts)

with vg.Executor(local=True) as executor:
    results = executor.run(graph)
    assert results == {total: 6}
    assert parts[1].result() == 2
```

Two things to know from the start:

- `executor.run(graph)` starts the tasks of the Graph, waits for them, and
  returns the results of the **sinks** only, the tasks
  that no task reads. Here that is `total`. The result of any other task is
  `task.result()`.
- Results are read **before the Executor closes**, which is the end of the
  `with` block. A result that was returned once stays on its Task. A result
  that was never read is gone after close, and `result()` says so.

`local=True` runs the tasks in this process, which needs no workers and suits
a first try. Without it the tasks run on TaskVine workers, as described under
[Running on workers](#running-on-workers).

## The programming model

Two objects keep what to run apart from where it runs:

- `vg.Graph` describes tasks and the data they pass to each other.
  `graph.add(func, *args, **kwargs)` records a call without running it and
  returns a `vg.Task`. Iterating over a Graph gives its tasks in the order they
  were added, and `len(graph)` counts them.
- `vg.Executor` is where tasks run. `executor.submit(func, *args, **kwargs)`
  starts one task at once and returns its `vg.Task`. `executor.submit(graph)`
  starts every task of a Graph that did not start yet and returns at once.
  `executor.run(graph)` does the same and then waits for the sinks.

A Task passed as an argument of another task gives that task its result, so
passing Tasks builds the dependencies. Arguments may hold Tasks anywhere
inside lists, tuples, sets, dictionary values, and dataclasses. `task["key"]`
and `task.attr("name")` select an item or an attribute of a result without
adding a task.

A task is a call of a Python function, or a run of a shell command given as a
`vg.Command`. `task.result()` returns what the function returned, or the text
the command wrote to standard output. A task that fails raises `vg.TaskError`.

A Task of a Graph is a description until its Graph is submitted. From then on
it is a live task and behaves exactly like a Task that `submit()` returned:
`result(timeout=None)` waits for it, and `done()`, `failed()`, `cancelled()`,
and `cancel()` tell and change how far it is. `result()` of a task that was
not submitted raises `RuntimeError`, and its `repr()` says `not submitted`. A
live task is numbered by its Executor, which numbers every task it starts in
one sequence, whatever started it, as in `Task(7, double)`.

One Executor runs any number of Graphs and submitted tasks at the same time,
all on one pool of workers. A Graph belongs to the first Executor that takes
it. Tasks added to a Graph after it was submitted start when it is submitted
or run again, and tasks that started already are not started again. A task
may read the live tasks of its Executor, whatever Graph they belong to, but
not the tasks of another Executor, and not the tasks of another Graph that
was not submitted yet.

`task.func`, `task.args`, `task.kwargs`, `task.readers`, and `task.key`
describe a task.

```python
import ndcctools.taskvine.vine_graph as vg


def make_record(value):
    return {"values": [value, value + 1], "metadata": {"count": 2}}


def scaled_sum(values, count, scale=1):
    assert len(values) == count
    return sum(values) * scale


graph = vg.Graph()
record = graph.add(make_record, 20)
answer = graph.add(scaled_sum, record["values"], record["metadata"]["count"], scale=2)

with vg.Executor(local=True) as executor:
    results = executor.run(graph)
    assert record.result()["metadata"] == {"count": 2}

assert results[answer] == 82
assert [task for task in graph if task.func is make_record] == [record]
```

## Files

`vg.File(path)` is an existing local file, and `task.file(name)`, a
`vg.TaskFile`, is a file that a task writes in its sandbox, the directory it
runs in. A task that takes either as an argument receives a local path.

```python
from pathlib import Path

import ndcctools.taskvine.vine_graph as vg


def uppercase(source_path):
    text = Path(source_path).read_text().upper()
    Path("result.txt").write_text(text)
    return len(text)


def verify(result_path, expected_length):
    text = Path(result_path).read_text()
    assert len(text) == expected_length
    return text


Path("input.txt").write_text("vine graph\n")

graph = vg.Graph()
producer = graph.add(uppercase, vg.File("input.txt"))
produced = producer.file("result.txt")
consumer = graph.add(verify, produced, producer)

with vg.Executor(local=True) as executor:
    results = executor.run(graph)
    assert produced.text == "VINE GRAPH\n"
    saved = produced.save("kept/result.txt")

assert results[consumer] == "VINE GRAPH\n"
assert Path(saved).read_text() == "VINE GRAPH\n"
```

An input keeps its base name in the sandbox, so a function finds an index
such as `x.bam.bai` next to `x.bam` when both are inputs. `vg.File(path,
name="config.yaml")` gives an input a name of its own. An input whose base
name another input of the same task has, that a task writes, that starts with
`-`, or that is reserved (`infile`, `outfile`, and names that start with
`.taskvine.`, `vine-graph-`, or `cctools-monitor`) gets a numbered name
instead, and two inputs given the same `name=` raise `ValueError`. A directory
is a File too, and the task receives its tree. Never write to an input: on a
worker it may be a link to the cached copy that other tasks read.

A file name is a relative path inside the sandbox. Asking for a file before
its task starts declares it, so the files of a Graph task are declared after
`graph.add()` and before the Graph is submitted. A declared file is a required
output: a task that does not write it fails.

Once the task completed, the file can be read whether or not a task reads it:

- `file.save(path)` puts the file at a path of your choice and returns the
  absolute path. It makes the directories on the way and replaces a file that
  is there.
- `file.content` returns its bytes, up to 16 MiB, and `file.text` decodes them
  as UTF-8.
- `file.path` returns a local path under `output_dir`, which stays after the
  Executor closes.
- `file.exists` tells whether the task wrote the file.

Like results, files are read before the Executor closes. A path or content
that was returned once stays readable afterwards.

### Remote inputs

`vg.URL(url, name=None)` is a remote file that tasks read. Each worker
fetches it once from its origin and keeps it while the workflow runs: `http`,
`https`, `ftp`, and `file` URLs with curl, and `root://` URLs of XRootD with
xrdcp, which uses the X509 proxy that `X509_USER_PROXY` names. A task receives
the path of its copy, named like the last part of the URL path, or `name`.
An origin that fails three times, as a URL that returns 404 does, fails the
tasks that read it with reason `"input"` and the origin's error (Manager
tuning `max-origin-failures`). `vg.File("https://...")` raises and says to use
`vg.URL`. A local Executor downloads each URL once into its run directory.

```python
page = vg.URL("https://example.org/data.csv")
rows = executor.submit(count_rows, page)
```

## Commands

A `vg.Command(template, *, cores=1, memory=None, gpus=0, outputs=(), name=None)`
is a shell command line that
tasks run, as a function is for Python tasks. `graph.add(command, **values)`
or `executor.submit(command, **values)` makes a task that runs it in its
sandbox, with a value for each placeholder such as `{name}`.

```python
from pathlib import Path

import ndcctools.taskvine.vine_graph as vg


def count_lines(path):
    return len(Path(path).read_text().splitlines())


Path("words.txt").write_text("pear\napple\nfig\n")

sort = vg.Command("sort {source} > sorted.txt", outputs=["sorted.txt"])

graph = vg.Graph()
sorted_file = graph.add(sort, source=vg.File("words.txt")).file("sorted.txt")
first = graph.add(vg.Command("head -n 1 {source}"), source=sorted_file)
lines = graph.add(count_lines, sorted_file)
report = graph.add(vg.Command("echo {count} lines, first is {word}"), count=lines, word=first)

with vg.Executor(local=True) as executor:
    results = executor.run(graph)
    assert sorted_file.text == "apple\nfig\npear\n"
    again = executor.submit(sort, source=vg.File("words.txt"))
    assert again.file("sorted.txt").text == "apple\nfig\npear\n"

assert results[report] == "3 lines, first is apple\n"
```

Values are given by name and inserted shell-quoted. A `vg.File` or a
`TaskFile` becomes its path in the sandbox, a Task becomes `str()` of its
result, and any other value `str()` of the value. Every placeholder needs a
value, and every value a placeholder. Write `{{` and `}}` for braces. A value
cannot be a selection such as `task["key"]`, a `vg.Shared`, a set, a list
that holds containers, or another container that holds tasks or files: read
those in a Python task and pass that task.

A list or a tuple inserts each element, quoted on its own, separated by
spaces, and nothing when it is empty. Its elements may be Files, TaskFiles,
Tasks, and plain values. A flag repeated for each value is a flat list:

```python
import ndcctools.taskvine.vine_graph as vg

includes = ["src", "lib"]
flags = [word for directory in includes for word in ("-I", directory)]
with vg.Executor(local=True) as executor:
    assert executor.submit(vg.Command("echo {flags}"), flags=flags).result() == "-I src -I lib\n"
```

The text a command writes to standard output, at most 1 MiB, is its result. An
exit status other than zero fails the task, and its `TaskError` carries the
status as `exit_code` and ends with the end of the command's standard error.
`cores` is the number of cores the command uses on a worker, where a Python
task uses one unless its function is a `vg.Function`.

`memory` is the memory in MB the command uses on a worker, which the command
sees as `$MEMORY`, and `gpus` the GPUs, which it sees as `$GPUS` and
`$CUDA_VISIBLE_DEVICES`. A task that requests no GPUs sees
`CUDA_VISIBLE_DEVICES` empty, and so do Python tasks, so a library such as
PyTorch does not take a GPU another task was given. A command that requested
memory and was killed for exceeding it, or that exceeded what it requested in
another way, fails with reason `"resources"`. `name="label"` names the tasks of
a command in their `repr()` and errors, as in `Task(3, sort)`, with 1 to 64
letters, digits, or `_.+-`.

A file that a command writes is kept only when it was declared. `outputs`
lists the files that every task of the command writes, and declares them for
each task, whether it comes from `graph.add()` or `executor.submit()`.
`task.file(name)` then names the file. A placeholder in `outputs` takes the
task's value, a str or an int inserted as it is, so
`vg.Command("convert {n}.png {n}.jpg", outputs=["{n}.jpg"])` declares `7.jpg`
for `n=7`. A file whose name differs from task to task in other ways is
declared with `task.file(name)` before the task starts, which only a task of a
Graph allows, because a submitted task has started already.

## Functions of several cores

A `vg.Function(func, *, cores=1, memory=None, gpus=0, time_limit=None,
retries=0, retry_on=Exception, name=None, outputs=(), cache=False)` is a
Python function whose tasks use `cores` cores of a worker, as `cores` does for
a Command. It goes wherever a function does, in `graph.add()` and
`executor.submit()`, and a plain function uses one core. `name` labels its
tasks, and `outputs` declares the files that each task writes in its working
directory, with placeholders filled from keyword arguments as for a Command. A
function that does not write a declared output fails with reason
`"function"` and a `FileNotFoundError`.

```python
import ndcctools.taskvine.vine_graph as vg


def solve(size):
    import numpy

    matrix = numpy.eye(size) * 2
    return float(numpy.linalg.solve(matrix, numpy.ones(size)).sum())


wide_solve = vg.Function(solve, cores=4)

graph = vg.Graph()
results = [graph.add(wide_solve, 100 * n) for n in range(1, 4)]
total = graph.add(sum, results)

with vg.Executor(local=True) as executor:
    assert executor.run(graph)[total] == 300.0
    assert executor.submit(wide_solve, 10).result() == 5.0
```

A worker runs the Python tasks of an Executor in one library with a slot per
core, and a task of `cores=k` occupies `k` slots, so a worker of 8 cores runs
eight tasks of one core, four of two, or two of four at a time. The library
limits the native thread pools (OpenMP, BLAS) of the task's process to `k`
threads. A task that asks for more cores than the largest connected worker
has waits for a larger worker, which a pool that grows may bring, and so does
a task that computes such a task's result again after a worker was lost. Once
two checks a few seconds apart found it so, the Executor prints one notice for
each such request, such as `Task(7, fit) requests 8 cores; the largest connected
Worker has 4; it waits for a larger Worker`. Tasks wait without a notice while
no worker is connected. The same holds for a Command of several cores, or of
more memory or GPUs than any worker has.
`local=True` runs every task in the Executor's own process and ignores
`cores`.

The library shares its worker with other tasks: it holds no resources itself,
and each call holds the cores and the `memory` (MB) it requests while it runs,
and sees them as `CORES` and `MEMORY`. Memory is a request, not a limit.

### Functions with GPUs

A task of `vg.Function(func, gpus=k)` runs as a standard TaskVine task on a
worker with `k` free GPUs, beside the library, instead of as a call of the
library. It sees its GPUs in `CUDA_VISIBLE_DEVICES`, and every other task sees
that variable empty. Each such task starts Python, checks the worker's
environment, imports `library_modules`, and loads the shared objects and the
cached edata, which a library call does once per worker, so `gpus=` suits
tasks that run long compared with that start. Its `library_files` sit in its
sandbox under their library names. An exit status above 128 means that a
signal killed it, which reads `"resources"` when it requested `memory`. To get
an exact amount of memory, request `memory`, since the worker otherwise gives
the task a share of its memory for its share of GPUs.

## Retries and time limits

Functions and Commands take `time_limit`, `retries`, and `retry_on`:

```python
fetch = vg.Function(download, retries=3, retry_on=ConnectionError, time_limit=600)
simulate = vg.Command("run.sh {cfg}", memory=4000, retries=2)
```

- `time_limit` (seconds) stops a task that runs longer. It fails with reason
  `"timeout"`. A local Executor applies it to Commands only.
- `retries=k` runs a task up to `k` more times after failures of the task
  itself. `retry_on` says which, as an `except` clause does: each failure is
  seen as an exception, the function's own, `subprocess.CalledProcessError`
  for a command's non-zero exit, `TimeoutError` for `"timeout"`, and
  `MemoryError` for `"resources"`, and is retried when it is an instance of a
  class of `retry_on`, `Exception` by default.
- After the last attempt the task fails with the reason of that attempt, and
  its message starts with the history, such as `it failed 3 times: timeout,
  timeout, function`.
- Lost workers and infrastructure failures are retried apart from these, by
  `max-retries`. A task is charged at most `max-retries + 1` worker losses
  across all its attempts, so a task that crashes its workers fails with
  reason `"workers"` however its other failures were retried.

## Caching results across runs

`vg.Executor(store=path)` keeps the results and declared files of tasks of
`cache=True` in a directory, and a later task with the same key takes them
from there without running, in this run or a later one:

```python
simulate = vg.Function(run_sim, cache=True)
with vg.Executor(port=9123, store="~/.cache/my-project") as executor:
    executor.run(graph)
```

The key covers the function (its code, the values it captures and the globals
it reaches; for a function of the program's own modules also the path, size,
and modification time of the module file), or the Command's template and
outputs, the arguments serialized, the keys of the tasks it reads with the
selection or file read of each, and the path, size, and modification time of
each `vg.File`, a directory by a walk of its tree. A task that reads a task
without `cache=True`, a `vg.URL`, or a `vg.Shared` object is not cached and
runs as usual, with one notice per function. The key misses files opened by
name inside the function, environment variables, randomness, compiled
extension code, and a File changed without a change of size or modification
time. A serialization that is not stable, such as a set of strings, causes a
miss, never a wrong hit. To forget an entry, delete its directory. `cache=True`
without a store raises `ValueError`. At close the Executor prints a line such
as `vine_graph: store /path: 120 hits, 3 stored, 2 not cacheable`.

## Running on workers

Without `local=True`, the Executor starts a TaskVine manager, and workers run
the tasks:

```python
import ndcctools.taskvine.vine_graph as vg


def square(value):
    return value * value


graph = vg.Graph()
total = graph.add(sum, [graph.add(square, value) for value in range(1, 6)])

with vg.Executor(port=9123, name="vine-graph-example") as executor:
    results = executor.run(graph)

assert results[total] == 55
```

Run the program in one terminal and start a worker in another:

```bash
vine_worker -M vine-graph-example --cores 4
```

Workers find the manager by its name through the TaskVine catalog, or connect
to its port directly (`vine_worker HOST 9123`). The
[TaskVine manual](index.md) describes how to start many workers on a cluster
with `vine_submit_workers` or `vine_factory`. Three things are particular to
Vine Graph:

- **Workers need the Python environment of the program.** Python tasks run in
  a TaskVine library on each worker, started with the worker's `python3`. It
  must be the same Python version as the program's, with the same
  `cloudpickle` and `ndcctools`, and with the packages the tasks import.
  Start workers from the same conda environment, or give the factory a Poncho
  package of it (`poncho_package_create "$CONDA_PREFIX" env.tar.gz`, then
  `vine_factory --poncho-env env.tar.gz`). When the library cannot import a
  module or load a shared object on a worker, the run stops and its error
  shows what the worker reported. When the library dies before that, as with
  another Python, the run stops after a few attempts, and
  `params={"watch-library-logfiles": 1}` adds the end of the library's log
  from the worker to the error.
- **Workers fetch data from a second port.** If a firewall admits only some
  ports, give the Executor a range of two, such as `port=(9123, 9124)`.
- **The manager copies results from workers.** If it cannot reach them, as
  behind a cluster firewall, start the workers with `--connect-back`.

`executor.port` and `executor.name` report the port and the name of the
manager. `executor.capacity()` returns a `Capacity` with the fields `workers`,
`cores`, `memory` (MB), and `gpus` of the connected workers. A local Executor
reports one worker of one core, and `None` for memory and GPUs.

### Progress

While `executor.run()` waits, the Executor draws a progress bar when standard
output is a terminal, which `progress_bar=False` turns off.
`executor.progress()` returns the counts at any time, as a `Progress` with the
fields `total`, `completed`, `failed`, `cancelled`, and `waiting`, over all
tasks the Executor started.

### Local modules

A function defined in the program's main file travels with its task. A
function imported from another local file, such as `helpers.py` next to the
program, is looked up by module name on the worker, which does not have the
file. `library_files` sends local files to the library, and
`library_modules` imports modules once when the library starts:

```python
import ndcctools.taskvine.vine_graph as vg
import helpers

with vg.Executor(port=9123, library_files={"helpers.py": "helpers.py"}, library_modules=["helpers"]) as executor:
    assert executor.submit(helpers.triple, 2).result() == 6
```

`library_files` maps each local path to the name it gets on the worker. A
task that fails because its worker lacks a local module says so and names the
file to send. Packages that are installed in the program's environment belong
in the workers' environment instead.

### Sharing modules and data across tasks

The library on each worker starts each Python task as a copy of itself, so
anything the library loaded once is free for every task on that worker.
`library_modules` names modules to import when the library starts, which
saves the import in every task. `shared` gives the library objects to load
when it starts, and a task argument `vg.Shared(name)` stands for one:

```python
import ndcctools.taskvine.vine_graph as vg


def predict(model, value):
    return model["weight"] * value


model = {"weight": 3}
with vg.Executor(port=9123, library_modules=["json"], shared={"model": model}) as executor:
    tasks = [executor.submit(predict, vg.Shared("model"), value) for value in range(10)]
    assert [task.result() for task in tasks] == [3 * value for value in range(10)]
```

Shared objects are given when the Executor is created and suit a few large
objects that most tasks use, such as a model or a lookup table. Each task gets
its own copy, so a change a task makes stays within that task. Only Python
tasks take shared objects.

### Where `print` output goes

A Python task on a worker runs in another process on another machine, and its
standard output is not shown by the program. Return what you want to see, or
write it to a file and read the file. When a task fails, the traceback is part
of its `TaskError`. A command's standard output is its result, and the end of
its standard error is part of its `TaskError` when it fails. A local Executor
runs Python tasks in the program's process, so there `print` writes to the
terminal.

## Dynamic work

Work that is not known in advance is submitted as results arrive. A program
that looks at a result and then decides what to run calls `executor.submit()`,
and `executor.as_completed()` tells which of several tasks finished first.
Work whose dependencies are known is submitted up front instead.

### Submitting tasks

`executor.submit(func, *args, **kwargs)` starts a call at once and returns its
Task. `task.result()` waits for the task. Arguments may hold Files and the live
Tasks of the same Executor: earlier submitted tasks, and tasks of the Graphs
it took.

```python
import ndcctools.taskvine.vine_graph as vg


def improve(guess, target):
    return (guess + target / guess) / 2


with vg.Executor(local=True) as executor:
    guess = executor.submit(float, 1)
    steps = 0
    while abs(guess.result() ** 2 - 2) > 1e-9:
        guess = executor.submit(improve, guess, 2)
        steps += 1
    assert steps == 4 and round(guess.result(), 6) == 1.414214
```

Live tasks run in the background, so the program can submit many and do other
work meanwhile. `task.done()` tells whether a task finished, and
`result(timeout=seconds)` raises `TimeoutError` when it is still running after
that long. `task.cancel()` stops a live task that did not finish, together
with the tasks that read it, and returns whether it cancelled the task.
`result()` of a cancelled task raises `vg.CancelledError`, a `RuntimeError`. `cancel()` of a task
that finished or was not submitted returns `False`.

A local Executor runs its tasks one at a time on a thread of its own, in the
order they were submitted, so there too `submit()` returns before the task
ran.

Leaving the `with` block without an exception waits for the tasks that still
run, then closes the Executor. Leaving it with an exception discards the
results.

### Dependencies start tasks

Submit everything you can name up front. A task that takes a Task as an
argument starts as soon as that input exists, so work whose dependencies are
known needs no loop. To analyze each simulation as soon as it finishes,
submit both at once:

```python
import ndcctools.taskvine.vine_graph as vg


def simulate(seed):
    return seed * seed % 7


def analyze(sample):
    return sample * 10


with vg.Executor(local=True) as executor:
    analyses = [executor.submit(analyze, executor.submit(simulate, seed)) for seed in range(100)]
    summary = executor.submit(sum, analyses)
    assert summary.result() == sum(10 * (seed * seed % 7) for seed in range(100))
```

A Graph does the same with `graph.add()`. A decision that depends on the
value of a result is ordinary control flow around `result()`, as in the loop
above that improves a guess until it is good enough.

### The first to finish

`executor.as_completed(tasks, timeout=None)` yields the given live tasks as
they finish, whatever their outcome, and at once those that finished
already, like `concurrent.futures.as_completed()`. It is for reacting to
whichever of several running tasks finishes first, such as a parameter sweep
that stops at the first good enough trial and cancels the rest:

```python
import ndcctools.taskvine.vine_graph as vg


def trial(seed):
    return seed * 37 % 101


with vg.Executor(local=True) as executor:
    trials = [executor.submit(trial, seed) for seed in range(50)]
    for task in executor.as_completed(trials):
        if task.result() > 90:
            best = task
            break
    for task in trials:
        task.cancel()
    assert best.result() > 90
```

A task that failed or was cancelled is yielded too, and its `result()` raises.
With a `timeout` in seconds, the iteration raises `TimeoutError` when tasks
are still unfinished that long after the call.

### Graphs that grow

A Graph may grow while its tasks run: add tasks that read its live tasks, and
submit the Graph again to start them.

```python
import ndcctools.taskvine.vine_graph as vg


def step(value):
    return value + 1


graph = vg.Graph()
last = graph.add(step, 0)

with vg.Executor(local=True) as executor:
    executor.submit(graph)
    while last.result() < 10:
        last = graph.add(step, last)
        executor.submit(graph)
    assert [task.result() for task in graph] == list(range(1, 11))
```

## Failures

A task fails when its function raises, when its command exits with an error
status, or when it cannot run after `max-retries` attempts. The tasks that
read a failed task fail too, without running. Tasks that do not depend on it
keep running. Lost workers are handled by TaskVine and are not failures,
unless a task keeps making its workers exit, as a task that exceeds their
memory does.

`executor.run()` raises the `vg.TaskError` of the task whose failure explains
the first failed sink, once the other tasks finished, and `task.result()`
raises the task's own. Its message includes the traceback from
where the function ran. Its fields:

| Field | Meaning |
| --- | --- |
| `task` | The Task that has no result. |
| `source` | The Task whose own failure explains it, which is `task` itself when it failed on its own. |
| `reason` | Why the source failed: `"function"` (its function raised or did not write a declared file), `"command"` (its command exited with an error status or did not write a declared file), `"signal"` (its function was killed by a signal), `"resources"` (it used more than it requested), `"workers"` (workers kept disconnecting while they ran it), `"input"` (a file it reads is missing, or a URL could not be fetched), `"timeout"` (it ran longer than its `time_limit`), `"retries"`, `"submit"` (it could not be submitted, as when an argument cannot be serialized), or `"result"` (it completed, but its result cannot be loaded or sent). More reasons may be added. |
| `exception` | The exception that the source's function raised, or `None`. |
| `exit_code` | The exit status of the source's command, or `None`. |

### Listing the failed tasks

```python
import ndcctools.taskvine.vine_graph as vg


def check(value):
    if value == 2:
        raise ValueError(f"cannot handle {value}")
    return value


graph = vg.Graph()
checks = [graph.add(check, value) for value in range(4)]
total = graph.add(sum, checks)

with vg.Executor(local=True) as executor:
    try:
        executor.run(graph)
    except vg.TaskError as error:
        assert error.task is checks[2] and isinstance(error.exception, ValueError)
    failed = [task for task in graph if task.failed()]
    assert failed == [checks[2], total]
    assert [task.result() for task in checks if not task.failed()] == [0, 1, 3]
```

### Fixing and running again

A failed task stays failed, and a later run of its Graph raises its error
again. In a script, fix the cause and run the script again. In a session that
should keep what completed, submit the fixed call with the arguments of the
failed task, which are still the completed tasks it read, and then the calls
that read it:

```python
import ndcctools.taskvine.vine_graph as vg


def parse(text):
    return int(text)


def parse_better(text):
    return int(text.strip().rstrip("."))


graph = vg.Graph()
numbers = [graph.add(parse, text) for text in ("1", "2.", "3")]
total = graph.add(sum, numbers)

with vg.Executor(local=True) as executor:
    try:
        executor.run(graph)
    except vg.TaskError as error:
        bad = error.source
    fixed = executor.submit(parse_better, *bad.args, **bad.kwargs)
    again = executor.submit(sum, [fixed if task is bad else task for task in numbers])
    assert again.result() == 6
```

## How long results are kept

The Executor keeps every result that can still be asked for without a new
task: a sink of a Graph, a submitted task that no submitted task reads yet,
and every result or file that was read. Other intermediate results are
released from the workers once the tasks that read them are safe. One of them
that is asked for after all, by `result()`, by a file, or by a task added or
submitted later, is computed again, which the caller does not see. The setting
`release-outputs` set to `0` keeps every result.

A submitted task is let go when its Task object is no longer referenced, and
the tasks of a Graph when the Graph is collected. A long-running session
therefore holds only the data its variables still refer to.

If workers are lost while the Executor is open, results that were copied to
the manager in the background are fetched from there, and the others are
computed again. Nothing survives the end of the program except the files it
saved.

## Notebooks

An Executor works like a file: use it in a `with` block, or create it in one
cell, use it in later cells, and close it in the last.

```python
import ndcctools.taskvine.vine_graph as vg

# Cell 1
executor = vg.Executor(port=9123, progress_bar=False)
graph = vg.Graph()

# Cell 2
squares = [graph.add(pow, value, 2) for value in range(8)]
results = executor.run(graph)

# Cell 3, at any later time
assert squares[3].result() == 9
total = graph.add(sum, squares)
assert executor.run(graph)[total] == 140

# Cell 4
more = executor.submit(pow, squares[3], 2)

# Cell 5
assert more.result() == 81 and more.done()

# Last cell
executor.close()
```

A later cell may add tasks to a Graph and run it again, which runs only the
new tasks. Live tasks keep running between cells, so a later cell can check
`task.done()` without waiting.

Interrupting a cell leaves the Executor usable. An interrupted
`executor.run()`, `result()`, or `as_completed()` stops waiting and leaves the
tasks running, so a later cell reads their results. `task.cancel()` stops a
task that is no longer wanted.

Rerunning the cell that creates an Executor on the same fixed port closes the
open Executor first and prints a notice. The tasks of the closed Executor then
report that it is closed, and a Graph that it ran needs to be built again.

Start workers from a cell with `subprocess` and `start_new_session=True`,
because Jupyter interrupts the whole process group of the kernel. While an
Executor is open, a background thread runs, so use `subprocess` instead of
`os.fork()`.

## Dask graphs

`from_dask()` in `ndcctools.taskvine.vine_graph.adaptors` converts a low-level
Dask task dictionary, or a dictionary of Dask collections, into a Graph whose
task keys are the Dask keys:

```python
import ndcctools.taskvine.vine_graph as vg
from ndcctools.taskvine.vine_graph.adaptors import from_dask


def increment(value):
    return value + 1


def multiply(left, right):
    return left * right


dask_graph = {
    "incremented": (increment, 1),
    "answer": (multiply, "incremented", 10),
}

graph = from_dask(dask_graph)
by_key = {task.key: task for task in graph}
with vg.Executor(local=True) as executor:
    results = executor.run(graph)

assert results[by_key["answer"]] == 20
```

## Executor settings

`vg.Executor(*, port, name, local, progress_bar, shared, params,
library_modules, library_files, output_dir, ssl, staging_path, run_info_path,
run_info_template)` takes only keywords:

| Argument | Default | Purpose |
| --- | --- | --- |
| `port` | `9123` | Port workers connect to. `0` picks a free port, which `executor.port` reports. |
| `name` | `None` | Manager name that workers look up in the catalog. |
| `local` | `False` | Run each task in this process, without TaskVine, for debugging. |
| `progress_bar` | `True` | Draw a progress bar while `run()` waits, when standard output is a terminal. |
| `shared` | `None` | Objects the library loads when it starts, as `{name: object}`, read by tasks through `vg.Shared(name)`. |
| `params` | `None` | TaskVine tuning by name, as a dictionary, applied once when the Executor is created. Its names follow TaskVine and may change. See below. |
| `library_modules` | `()` | Modules, or module names, the library imports when it starts on a worker. |
| `library_files` | `None` | Local files sent to the library, as `{local_path: name_on_the_worker}`. |
| `output_dir` | `./outputs` | Where the files whose paths `TaskFile.path` returns are kept. It is created when the first is asked for. A local Executor also keeps its task sandboxes there while it is open. |
| `ssl` | `None` | A `(key, certificate)` pair for TLS between the manager and its workers. |
| `staging_path` | `None` | The manager's staging directory. |
| `run_info_path`, `run_info_template` | `vine-run-info`, `None` | Where the manager writes its logs. |
| `store` | `None` | A directory that keeps the results of tasks of `cache=True` across runs. |

`params` is the escape hatch to TaskVine tuning: its names follow TaskVine and
may change with it. It takes these settings:

| Setting | Default | Purpose |
| --- | --- | --- |
| `task-priority-mode` | `largest-input-first` | Order of ready tasks: `largest-input-first`, `depth-first`, or `fifo`. |
| `max-retries` | `5` | Failed attempts allowed per task for failures other than its own, and worker losses charged to it, less one (at most 62). |
| `release-outputs` | `1` | `1` releases a result once every task that reads it is safe. `0` keeps every result. |
| `shared-directory` | none | A directory that the manager and every worker mount at the same path, such as a shared file system, which then holds the background copies of large results. |
| `edata-cache` | `0` | Bytes of task functions and arguments of the first `submit()` or `run()` call that the library loads when it starts, so tasks do not fetch them. |

Every other name is a TaskVine manager tuning parameter, such as
`wait-for-workers`. A name that is neither, or an invalid value, raises
`ValueError` before the Executor starts. A local Executor checks the settings
and applies none of them.

Closing an Executor keeps only the files that the program saved or whose paths
`TaskFile.path` returned. Leaving a `with` block because of an exception
removes those paths too. Input files are never modified.

## Changes in this version

- `Executor`, `Function`, and `Command` take every argument but the first as a
  keyword, so `vg.Executor(9123)` becomes `vg.Executor(port=9123)`. The
  Executor takes no other manager options than the ones listed above.
- `progress=` is `progress_bar=`.
- `executor.progress()` has the field `cancelled`, and cancelled tasks count
  there. It and `capacity()` are no longer tuples.
- `result()` of a cancelled task raises `vg.CancelledError`.
- On workers, inputs keep their base names in the sandbox, as they did
  locally, and a directory is accepted as a File.
- Tasks that request no GPUs now see `CUDA_VISIBLE_DEVICES` empty.
- `Function` takes `memory`, `gpus`, `time_limit`, `retries`, `retry_on`, and
  `cache`, and `Command` takes `time_limit`, `retries`, `retry_on`, and
  `cache`. A new reason, `"timeout"`, reports a time limit.
- The library shares its worker (TaskVine's pool mode), so commands and GPU
  tasks run beside it and an idle library is no longer removed for them.
- `vg.URL` reads remote inputs, `vg.File` of a URL raises, and
  `Executor(store=)` caches results across runs.
