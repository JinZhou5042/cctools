# Vine Graph

Vine Graph runs Python function calls and shell commands on TaskVine, either
as a graph built in advance or as tasks started one at a time while others
run.

```python
import ndcctools.taskvine.vine_graph as vg

def double(value):
    return 2 * value

graph = vg.Graph()
parts = [graph.add(vg.Function(double), value) for value in range(3)]
total = graph.add(vg.Function(sum), parts)

with vg.Executor(local=True) as executor:
    results = executor.run(graph)
    assert results == {total: 6}
    assert parts[1].result() == 2
```

`executor.run(graph)` returns the results of the **sinks** only, the tasks
that no task reads (here `total`). Any other result is `task.result()`. Read
results **before the Executor closes**: one returned once stays on its Task,
one never read is gone. `local=True` runs tasks in this process without
workers; see [Running on workers](#running-on-workers) for the rest.

## The programming model

- `vg.Graph` describes tasks and the data they pass to each other.
  `graph.add(vg.Function(func), *args, **kwargs)` records a call without
  running it and returns a `vg.Task`. Iterating over a Graph gives its tasks
  in the order they were added, and `len(graph)` counts them.
- `vg.Executor` is where tasks run.
  `executor.submit(vg.Function(func), *args, **kwargs)` starts one task at
  once and returns its `vg.Task`. `executor.submit(graph)` starts every task
  of a Graph that did not start yet and returns at once.
  `executor.run(graph)` does the same and then waits.

The first argument of `add()` and `submit()` is always a `vg.Function` or a
`vg.Command`. A bare callable raises `TypeError`.

A Task passed as an argument of another task gives that task its result, which
builds the dependencies, and the reader starts as soon as its inputs exist.
Tasks may appear anywhere inside lists, tuples, sets, dictionary values, and
dataclasses. `task["key"]` and `task.attr("name")` select an item or an
attribute of a result without adding a task, as in
`graph.add(vg.Function(scale), record["values"])`.

A Task of a Graph is a description until its Graph is submitted, and then
behaves exactly like a Task that `submit()` returned: `result(timeout=None)`
waits for it, and `done()`, `failed()`, `cancelled()`, and `cancel()` tell and
change its state. `result()` of an unsubmitted task raises `RuntimeError`. A
live task is numbered by its Executor, as in `Task(7, double)`. `task.func`,
`task.args`, `task.kwargs`, `task.readers`, and `task.key` describe a task.

One Executor runs any number of Graphs and submitted tasks at once. A Graph
belongs to the first Executor that takes it, and a task may read only tasks of
its own Graph or live tasks of its own Executor.

## Files

`vg.File(path)` is an existing local file or directory. `task.file(name)`, a
`vg.TaskFile`, is a file that a task writes in its sandbox, the directory it
runs in. A task that takes either as an argument receives a local path.

```python
from pathlib import Path

import ndcctools.taskvine.vine_graph as vg

def uppercase(source_path):
    Path("result.txt").write_text(Path(source_path).read_text().upper())

Path("input.txt").write_text("vine graph\n")

graph = vg.Graph()
produced = graph.add(vg.Function(uppercase), vg.File("input.txt")).file("result.txt")
length = graph.add(vg.Function(lambda path: len(Path(path).read_text())), produced)

with vg.Executor(local=True) as executor:
    assert executor.run(graph)[length] == 11
    assert produced.text == "VINE GRAPH\n"
    saved = produced.save("kept/result.txt")
```

An input keeps its base name in the sandbox, so `x.bam.bai` sits next to
`x.bam` when both are inputs. `vg.File(path, name="config.yaml")` gives an
input a name of its own. An input whose name clashes with another input or an
output, starts with `-`, or is reserved (`infile`, `outfile`, and names that
start with `.taskvine.`, `vine-graph-`, or `cctools-monitor`) gets a numbered
name instead. Two inputs given the same `name=` raise `ValueError`. Never write
to an input: it may be a shared cached copy.

A file name is a relative sandbox path. Asking for it before its task starts
declares it as a required output. Once the task completed, the file can be
read:

- `file.save(path)` puts the file at a path of your choice, making
  directories and replacing what is there, and returns the absolute path.
- `file.content` returns its bytes, up to 16 MiB, and `file.text` decodes them
  as UTF-8.
- `file.path` returns a local path under `output_dir`, which stays after the
  Executor closes.
- `file.exists` tells whether the task wrote the file.

### Remote inputs

`vg.URL(url, name=None)` is a remote file that tasks read. Each worker fetches
it once and keeps it while the workflow runs: `http`, `https`, `ftp`, and
`file` URLs with curl, and XRootD `root://` URLs with xrdcp, using the X509
proxy named by `X509_USER_PROXY`. A task receives the path of its copy, named
after the last part of the URL path, or `name`. An origin that fails three
times (Manager tuning `max-origin-failures`) fails the tasks that read it with
reason `"input"`. `vg.File("https://...")` raises and says to use `vg.URL`.

```python
page = vg.URL("https://example.org/data.csv")
rows = executor.submit(vg.Function(count_rows), page)
```

## Commands

A `vg.Command(template, *, cores=1, memory=None, gpus=0, outputs=(), name=None)`
is a shell command line that tasks run. `graph.add(command, **values)` or
`executor.submit(command, **values)` makes a task that runs it in its sandbox,
with a value for each placeholder such as `{name}`.

```python
from pathlib import Path

import ndcctools.taskvine.vine_graph as vg

Path("words.txt").write_text("pear\napple\nfig\n")
sort = vg.Command("sort {source} > sorted.txt", outputs=["sorted.txt"])

graph = vg.Graph()
sorted_file = graph.add(sort, source=vg.File("words.txt")).file("sorted.txt")
first = graph.add(vg.Command("head -n 1 {source}"), source=sorted_file)
report = graph.add(vg.Command("echo {flags} {word}"), flags=["first", "is"], word=first)

with vg.Executor(local=True) as executor:
    assert executor.run(graph)[report] == "first is apple\n"
    assert sorted_file.text == "apple\nfig\npear\n"
```

Values are given by name and inserted shell-quoted. A `vg.File` or a
`TaskFile` becomes its sandbox path, a Task becomes `str()` of its result, and
any other value becomes `str()` of itself. A flat list or tuple inserts each
element quoted on its own, separated by spaces. Every placeholder needs a
value and every value a placeholder. Write `{{` and `}}` for braces. A value
cannot be a selection, a `vg.Shared`, a set, or a nested container: compute
those in a Python task and pass that task.

Standard output, at most 1 MiB, is the result. A non-zero exit status fails
the task, and its `TaskError` carries the status as `exit_code` and ends with
the tail of standard error.

- `cores` is the number of cores the command uses on a worker.
- `memory` is the memory in MB, seen as `$MEMORY`. A command that exceeds it
  fails with reason `"resources"`.
- `gpus` is the number of GPUs, seen as `$GPUS` and `$CUDA_VISIBLE_DEVICES`.
  Tasks that request no GPUs, Python tasks included, see
  `CUDA_VISIBLE_DEVICES` empty.
- `name="label"` names the tasks in `repr()` and errors, as in `Task(3, sort)`,
  with 1 to 64 letters, digits, or `_.+-`.
- `outputs` declares the files every task writes, and only declared files are
  kept. A placeholder in `outputs` takes the task's value, so `vg.Command("convert {n}.png {n}.jpg", outputs=["{n}.jpg"])` declares
  `7.jpg` for `n=7`. Other per-task names are declared with `task.file(name)`
  before the task starts, which only a Graph task allows.

## Functions with resources

A `vg.Function(func, *, cores=1, memory=None, gpus=0, time_limit=None,
retries=0, retry_on=Exception, name=None, outputs=(), cache=False)` wraps a
Python function and goes wherever a function does. A plain function uses one
core. `name` labels its tasks, and `outputs` declares the files each task
writes in its working directory, with placeholders filled from keyword
arguments as for a Command. A missing declared output fails the task with
reason `"function"` and a `FileNotFoundError`.

```python
import ndcctools.taskvine.vine_graph as vg

def solve(size):
    import numpy

    matrix = numpy.eye(size) * 2
    return float(numpy.linalg.solve(matrix, numpy.ones(size)).sum())

wide_solve = vg.Function(solve, cores=4)

graph = vg.Graph()
results = [graph.add(wide_solve, 100 * n) for n in range(1, 4)]
total = graph.add(vg.Function(sum), results)

with vg.Executor(local=True) as executor:
    assert executor.run(graph)[total] == 300.0
    assert executor.submit(wide_solve, 10).result() == 5.0
```

A task of `cores=k` takes `k` of a worker's library slots (one per core) and
limits its OpenMP and BLAS threads to `k`. It sees `CORES` and `MEMORY` (MB).
Memory is a request, not a limit. A task that asks for more than the largest
connected worker has waits for a larger one, with one notice such as `Task(7, fit) requests 8 cores; the largest connected
Worker has 4; it waits for a larger Worker`. `local=True` ignores resources.

### Functions with GPUs

A task of `vg.Function(func, gpus=k)` runs as a standard TaskVine task on a
worker with `k` free GPUs, beside the library, and sees its GPUs in
`CUDA_VISIBLE_DEVICES`. Each such task starts its own Python and loads
`library_modules` and shared objects itself, so `gpus=` suits long tasks. Its
`library_files` sit in its sandbox. Request `memory` to get an exact amount;
otherwise the worker gives it a share of memory for its share of GPUs.

## Retries and time limits

Functions and Commands take `time_limit`, `retries`, and `retry_on`:

```python
fetch = vg.Function(download, retries=3, retry_on=ConnectionError, time_limit=600)
simulate = vg.Command("run.sh {cfg}", memory=4000, retries=2)
```

- `time_limit` (seconds) stops a task that runs longer, with reason
  `"timeout"`. A local Executor applies it to Commands only.
- `retries=k` runs a failed task up to `k` more times when the failure is an
  instance of a class in `retry_on` (`Exception` by default). Failures are
  seen as the function's own exception, `subprocess.CalledProcessError` for a
  command's non-zero exit, `TimeoutError` for `"timeout"`, and `MemoryError`
  for `"resources"`.
- After the last attempt the task fails with the reason of that attempt, and
  its message starts with the history, such as `it failed 3 times: timeout,
  timeout, function`.
- Lost workers are retried separately, by `max-retries`. A task that keeps
  crashing its workers fails with reason `"workers"`.

## Caching results across runs

`vg.Executor(store=path)` keeps the results and declared files of tasks of
`cache=True` in a directory. A later task with the same key, in this run or a
later one, takes them from there without running:

```python
simulate = vg.Function(run_sim, cache=True)
with vg.Executor(port=9123, store="~/.cache/my-project") as executor:
    executor.run(graph)
```

The key covers the function's code, captured values, and globals (or the
Command's template), the arguments, the tasks it reads, and the size and
modification time of each `vg.File`. It does not see files opened by name,
environment variables, randomness, or compiled extensions. A task that reads
an uncached task, a `vg.URL`, or a `vg.Shared` is not cached. To forget an
entry, delete its directory. `cache=True` without a store raises `ValueError`.

## Running on workers

Without `local=True`, the Executor starts a TaskVine manager, and workers run
the tasks:

```python
import ndcctools.taskvine.vine_graph as vg

graph = vg.Graph()
total = graph.add(vg.Function(sum), [graph.add(vg.Function(abs), -value) for value in range(1, 6)])

with vg.Executor(port=9123, name="vine-graph-example") as executor:
    assert executor.run(graph)[total] == 15
```

Run the program in one terminal and start a worker in another:

```bash
vine_worker -M vine-graph-example --cores 4
```

Workers find the manager by name through the TaskVine catalog, or connect to
its port directly (`vine_worker HOST 9123`). The [TaskVine manual](index.md)
describes starting many workers with `vine_submit_workers` or `vine_factory`.
Three things are particular to Vine Graph:

- **Workers need the program's Python environment.** Python tasks run in a
  TaskVine library started with the worker's `python3`, which must have the
  same Python version, `cloudpickle`, and `ndcctools` as the program, and the
  packages the tasks import. Start workers from the same conda environment, or
  give the factory a Poncho package of it (`poncho_package_create
  "$CONDA_PREFIX" env.tar.gz`, then `vine_factory --poncho-env env.tar.gz`). If
  the library fails to start, the run stops with the worker's error, and
  `params={"watch-library-logfiles": 1}` adds the tail of the library's log.
- **Workers fetch data from a second port.** If a firewall admits only some
  ports, give a range of two, such as `port=(9123, 9124)`.
- **The manager copies results from workers.** If it cannot reach them, start
  the workers with `--connect-back`.

`executor.port` and `executor.name` report the manager's port and name.
`executor.capacity()` returns a `Capacity` (`workers`, `cores`, `memory` in MB,
`gpus`) of the connected workers. `executor.progress()` returns a `Progress`
(`total`, `completed`, `failed`, `cancelled`, `waiting`) over all tasks.

### Local modules

A function from the program's main file travels with its task, but one
imported from a local file such as `helpers.py` needs that file on the worker.
`library_files` sends local files to the library, as `{local_path: name_on_the_worker}`, and
`library_modules` imports modules once when the library starts:

```python
import ndcctools.taskvine.vine_graph as vg
import helpers

with vg.Executor(port=9123, library_files={"helpers.py": "helpers.py"}, library_modules=["helpers"]) as executor:
    assert executor.submit(vg.Function(helpers.triple), 2).result() == 6
```

### Sharing modules and data across tasks

The library starts each Python task as a copy of itself, so what it loaded
once is free for every task on its worker. `shared` gives it objects to load at
start, such as a model, and a task argument `vg.Shared(name)` stands for one.
Each task gets its own copy. Only Python tasks take shared objects:

```python
import ndcctools.taskvine.vine_graph as vg

def predict(model, value):
    return model["weight"] * value

model = {"weight": 3}
with vg.Executor(port=9123, library_modules=["json"], shared={"model": model}) as executor:
    tasks = [executor.submit(vg.Function(predict), vg.Shared("model"), value) for value in range(10)]
    assert [task.result() for task in tasks] == [3 * value for value in range(10)]
```

On workers, `print` output of Python tasks is not shown. Return what you want
to see, or write a file. A failed task's traceback is in its `TaskError`.

## Dynamic work

Submit up front what dependencies can describe, and decide on values with
plain control flow around `result()`:

```python
import ndcctools.taskvine.vine_graph as vg

def improve(guess, target):
    return (guess + target / guess) / 2

with vg.Executor(local=True) as executor:
    guess = executor.submit(vg.Function(float), 1)
    while abs(guess.result() ** 2 - 2) > 1e-9:
        guess = executor.submit(vg.Function(improve), guess, 2)
    assert round(guess.result(), 6) == 1.414214
```

Live tasks run in the background, also on a local Executor, which runs them
one at a time in submission order. `task.done()` tells whether a task
finished, and `result(timeout=seconds)` raises `TimeoutError` if it is still
running. `task.cancel()` stops an unfinished task and its readers and returns
`True`, or `False` for a finished or unsubmitted task. `result()` of a
cancelled task raises `vg.CancelledError`, a `RuntimeError`. Leaving the
`with` block normally waits for running tasks. Leaving it with an exception
discards the results.

### The first to finish

`executor.as_completed(tasks, timeout=None)` yields the given live tasks as
they finish, whatever their outcome, like `concurrent.futures.as_completed()`.
A failed or cancelled task is yielded too, and its `result()` raises. The
`timeout` covers the whole iteration:

```python
import ndcctools.taskvine.vine_graph as vg

with vg.Executor(local=True) as executor:
    trials = [executor.submit(vg.Function(lambda s: s * 37 % 101), seed) for seed in range(50)]
    for task in executor.as_completed(trials):
        if task.result() > 90:
            best = task
            break
    for task in trials:
        task.cancel()
    assert best.result() > 90
```

### Graphs that grow

A Graph may grow while its tasks run: add tasks that read its live tasks, and
submit the Graph again to start them.

```python
import ndcctools.taskvine.vine_graph as vg

step = vg.Function(lambda value: value + 1)
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

A task fails when its function raises, its command exits non-zero, or it
cannot run after `max-retries` attempts. The tasks that read it fail too,
without running, while independent tasks keep running. Lost workers are not
failures, unless a task keeps making its workers exit.

`executor.run()` raises the `vg.TaskError` that explains the first failed
sink, once the other tasks finished, and `task.result()` raises the task's
own. Its message includes the traceback. Its fields:

| Field | Meaning |
| --- | --- |
| `task` | The Task that has no result. |
| `source` | The Task whose own failure explains it, which is `task` itself when it failed on its own. |
| `reason` | Why the source failed: `"function"` (its function raised or did not write a declared file), `"command"` (its command exited with an error status or did not write a declared file), `"signal"` (its function was killed by a signal), `"resources"` (it used more than it requested), `"workers"` (workers kept disconnecting while they ran it), `"input"` (a file it reads is missing, or a URL could not be fetched), `"timeout"` (it ran longer than its `time_limit`), `"retries"`, `"submit"` (it could not be submitted, as when an argument cannot be serialized), or `"result"` (it completed, but its result cannot be loaded or sent). More reasons may be added. |
| `exception` | The exception that the source's function raised, or `None`. |
| `exit_code` | The exit status of the source's command, or `None`. |

### Listing, fixing, and running again

A failed task stays failed. In a script, fix the cause and run again. In a
session, submit the fixed call with the failed task's arguments, and then the
calls that read it:

```python
import ndcctools.taskvine.vine_graph as vg

graph = vg.Graph()
numbers = [graph.add(vg.Function(int), text) for text in ("1", "2.", "3")]
total = graph.add(vg.Function(sum), numbers)

with vg.Executor(local=True) as executor:
    try:
        executor.run(graph)
    except vg.TaskError as error:
        bad = error.source
        assert isinstance(error.exception, ValueError) and error.reason == "function"
    assert [task for task in graph if task.failed()] == [numbers[1], total]
    fixed = executor.submit(vg.Function(lambda text: int(text.rstrip("."))), *bad.args)
    again = executor.submit(vg.Function(sum), [fixed if task is bad else task for task in numbers])
    assert again.result() == 6
```

## How long results are kept

The Executor keeps every result that can be asked for without a new task: a
sink, a submitted task that nothing reads yet, and every result or file that
was read. Other intermediate results are released from the workers once their
readers are safe, and are computed again, unseen, if asked for after all. The
setting `release-outputs` set to `0` keeps every result.

A submitted task is let go when its Task is no longer referenced, and the
tasks of a Graph when the Graph is collected, so a long session holds only
what its variables refer to. Results lost with a worker are fetched from the
manager's background copies or computed again. Nothing survives the program
except the files it saved.

## Notebooks

Use an Executor in a `with` block, or create it in one cell, use it in later
cells, and close it in the last:

```python
import ndcctools.taskvine.vine_graph as vg

# Cell 1
executor = vg.Executor(port=9123, progress_bar=False)
graph = vg.Graph()

# Cell 2
squares = [graph.add(vg.Function(pow), value, 2) for value in range(8)]
results = executor.run(graph)

# Cell 3, at any later time
assert squares[3].result() == 9
total = graph.add(vg.Function(sum), squares)
assert executor.run(graph)[total] == 140

# Last cell
executor.close()
```

Running a Graph again runs only its new tasks. Interrupting a cell stops
waiting and leaves tasks running, so a later cell reads their results.
Rerunning the cell that creates an Executor on the same fixed port closes the
open one first, with a notice. Start workers from a cell with `subprocess` and
`start_new_session=True`, since Jupyter interrupts the kernel's whole process
group, and use `subprocess` rather than `os.fork()`.

## Dask graphs

`from_dask()` in `ndcctools.taskvine.vine_graph.adaptors` converts a low-level
Dask task dictionary, or a dictionary of Dask collections, into a Graph whose
task keys are the Dask keys:

```python
import ndcctools.taskvine.vine_graph as vg
from ndcctools.taskvine.vine_graph.adaptors import from_dask

def multiply(left, right):
    return left * right

graph = from_dask({"incremented": (abs, -2), "answer": (multiply, "incremented", 10)})
by_key = {task.key: task for task in graph}
with vg.Executor(local=True) as executor:
    results = executor.run(graph)

assert results[by_key["answer"]] == 20
```

## Executor settings

`vg.Executor` takes only keywords:

| Argument | Default | Purpose |
| --- | --- | --- |
| `port` | `9123` | Port workers connect to, or a range `(low, high)`. `0` picks a free port, which `executor.port` reports. |
| `name` | `None` | Manager name that workers look up in the catalog. |
| `local` | `False` | Run each task in this process, without TaskVine. |
| `progress_bar` | `True` | Draw a progress bar while `run()` waits, when standard output is a terminal. |
| `shared` | `None` | Objects the library loads when it starts, as `{name: object}`, read through `vg.Shared(name)`. |
| `params` | `None` | TaskVine tuning by name, applied once at creation. See below. |
| `library_modules` | `()` | Modules, or module names, the library imports when it starts. |
| `library_files` | `None` | Local files sent to the library, as `{local_path: name_on_the_worker}`. |
| `output_dir` | `./outputs` | Where the paths returned by `TaskFile.path` live. A local Executor also keeps its sandboxes there while open. |
| `ssl` | `None` | A `(key, certificate)` pair for TLS between the manager and its workers. |
| `staging_path` | `None` | The manager's staging directory. |
| `run_info_path`, `run_info_template` | `vine-run-info`, `None` | Where the manager writes its logs. |
| `store` | `None` | A directory that keeps the results of tasks of `cache=True` across runs. |

`params` is the escape hatch to TaskVine tuning. Its names follow TaskVine and
may change with it:

| Setting | Default | Purpose |
| --- | --- | --- |
| `task-priority-mode` | `largest-input-first` | Order of ready tasks: `largest-input-first`, `depth-first`, or `fifo`. |
| `max-retries` | `5` | Attempts allowed per task for failures other than its own, including worker losses charged to it (at most 62). |
| `release-outputs` | `1` | `1` releases a result once every task that reads it is safe. `0` keeps every result. |
| `edata-cache` | `0` | Bytes of task functions and arguments of the first `submit()` or `run()` that the library loads at start, so tasks do not fetch them. |

Any other name is a TaskVine manager tuning parameter, such as
`wait-for-workers`. An unknown name or invalid value raises `ValueError`
before the Executor starts. A local Executor checks the settings and applies
none.

Closing an Executor keeps only the files the program saved or whose paths
`TaskFile.path` returned. Leaving a `with` block because of an exception
removes those too. Input files are never modified.
