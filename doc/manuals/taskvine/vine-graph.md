# Vine Graph

Vine Graph runs Python function calls and shell commands on TaskVine, either
as a graph built in advance or as tasks started one at a time while others
run. Import it as:

```python
import ndcctools.taskvine.vine_graph as vg
```

## The programming model

There are two main objects, which keep what to run apart from where it runs:

- `vg.Graph`: a description of tasks, the data they pass to each other, and
  rules that add tasks as others complete. `graph.add(func, *args, **kwargs)`
  records a call without running it and returns a `vg.Task` that stands for
  the call. A Graph holds no settings and has no way to run itself.
- `vg.Executor`: where tasks run, on TaskVine workers or in this process.
  `executor.run(graph)` runs a Graph and returns the results of its sinks, the
  tasks that no task reads. `executor.submit(func, *args, **kwargs)` starts
  one task at once and returns its `vg.Task`.

A Task passed as an argument of another task gives that task its result, so
passing Tasks builds the dependencies. Arguments may hold them anywhere inside
lists, tuples, sets, dictionary values, and dataclasses. `task["key"]` and
`task.attr("name")` select an item or an attribute of a result without adding
a task. Files are `vg.File(path)` for an existing local file, and
`task.file(name)`, a `vg.TaskFile`, for a file that a task writes in its
sandbox. A task receives either kind as a local path.

A task is a call of a Python function, or a run of a shell command line given
as a `vg.Command`. `task.result()` returns what the function returned, or the
text the command wrote to standard output. A task that fails raises
`vg.TaskError`.

One Executor runs any number of Graphs, one at a time, and any number of
submitted tasks, all on one pool of workers. A Graph runs on one Executor, the
first that runs it, which holds the results of its tasks. Running the Graph
again runs only the tasks that did not finish, including tasks added since.

## First graph: local execution

`vg.Executor(local=True)` runs each task in this process, one at a time and
without TaskVine, with the same semantics as on workers. It is handy for
checking a graph and its functions. Create `vine_graph_local.py`:

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

assert results[answer] == 82
print(results[answer])
```

Run it with:

```bash
python vine_graph_local.py
```

`executor.run(graph)` runs every task of the Graph that did not finish, blocks
until none runs, and returns a dictionary from each completed sink to its
result. While the Executor is open, `task.result()` returns the result of any
other task of the Graph, such as `record.result()` here. Before an Executor
ran the Graph, `result()` raises `RuntimeError`, because only
`executor.run()` runs the tasks of a Graph. `task.done()`, `task.failed()`,
and `task.cancelled()` tell how far a task is, and `task.func`, `task.args`,
`task.kwargs`, `task.readers`, and `task.key` describe it.
`graph.find(*selectors)` returns tasks of a Graph in the order they were
added: the tasks that run each function or Command given, each Task given, or
every task when nothing is given.

## Passing files between tasks

A task reads a `vg.File` or a `TaskFile` given as an argument at a local path.
For example, `vine_graph_files.py` passes a local file to one task and its
output file to another:

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
produced_file = producer.file("result.txt")
consumer = graph.add(verify, produced_file, producer)

with vg.Executor(local=True, output_dir="./vine-graph-file-output") as executor:
    results = executor.run(graph)
    assert produced_file.text == "VINE GRAPH\n"
    produced_path = produced_file.path

assert results[consumer] == "VINE GRAPH\n"
assert Path(produced_path).read_text() == "VINE GRAPH\n"
print(results[consumer], end="")
```

A file name is a non-empty relative path inside the task sandbox, and asking
for the same name again returns the same `TaskFile`. Asking for a file before
its task starts declares it, so the files of a Graph task are declared after
`graph.add()` and before `executor.run()`. A declared file is a required
output: a task that does not write it fails. A task may read only tasks of its
own Graph.

Once the task completed, the file can be read whether or not a task reads it:
`file.content` returns its bytes, up to 16 MiB, `file.text` decodes them as
UTF-8, and `file.path` returns a local path under `output_dir`, which stays
after the Executor closes. `file.exists` tells whether the task wrote the
file.

## Commands

A `vg.Command(template, cores=1)` is a shell command line that tasks run, as a
function is for Python tasks. `graph.add(command, **values)` adds a task that
runs it in its sandbox, with a value for each placeholder such as `{name}`.
Create `vine_graph_commands.py`:

```python
from pathlib import Path

import ndcctools.taskvine.vine_graph as vg


def count_lines(path):
    return len(Path(path).read_text().splitlines())


Path("words.txt").write_text("pear\napple\nfig\n")

graph = vg.Graph()
sort = vg.Command("sort {source} > {output}")
sorted_task = graph.add(sort, source=vg.File("words.txt"), output="sorted.txt")
sorted_file = sorted_task.file("sorted.txt")
first = graph.add(vg.Command("head -n 1 {source}"), source=sorted_file)
lines = graph.add(count_lines, sorted_file)
report = graph.add(vg.Command("echo {count} lines, first is {word}"), count=lines, word=first)

with vg.Executor(local=True) as executor:
    results = executor.run(graph)
    assert sorted_file.text == "apple\nfig\npear\n"

assert results[report] == "3 lines, first is apple\n"
print(results[report], end="")
```

Values are given by name and inserted shell-quoted. A `vg.File` or a
`TaskFile` becomes its path in the sandbox, a Task becomes `str()` of its
result, and any other value `str()` of the value. Every placeholder needs a
value, and every value a placeholder. Write `{{` and `}}` for braces. A value
cannot be a selection such as `task["key"]`, a `vg.Shared`, or a container
that holds tasks or files: read those in a Python task and pass that task.

The text a command writes to standard output, at most 1 MiB, is its result. An
exit status other than zero fails the task, and its `TaskError` carries the
status as `exit_code` and ends with the end of the command's standard error.
`cores` is the number of cores the command uses on a worker, where a Python
task uses one.

A file that a command writes in its sandbox is kept only when it was declared
with `task.file(name)` before the task starts, where `name` is the relative
path the command writes, the same text its placeholder gets. Pass the
`TaskFile` to other tasks, or read it after the run with `path`, `content`, or
`text`. A task that `executor.submit()` returns has started already and has no
declared files, so put a command whose files matter in a Graph.

## Distributed execution with workers

Without `local=True`, the Executor starts a TaskVine manager, and workers run
the tasks. Create `vine_graph_distributed.py`:

```python
import ndcctools.taskvine.vine_graph as vg


def square(value):
    return value * value


def add_all(*values):
    return sum(values)


graph = vg.Graph()
squares = [graph.add(square, value) for value in range(1, 6)]
total = graph.add(add_all, *squares)

with vg.Executor(port=9123, name="vine-graph-example") as executor:
    results = executor.run(graph)

assert results[total] == 55
print(results[total])
```

Run it in one terminal:

```bash
python vine_graph_distributed.py
```

Start a worker in another terminal:

```bash
vine_worker -M vine-graph-example --cores 4
```

Workers find the manager by its name through the TaskVine catalog, or connect
to its port directly. Workers also fetch data from a second port of the
manager. If a firewall admits only some ports, give the Executor a range of
two, such as `port=(9123, 9124)`, so that both ports are known. The manager
also copies results from workers. If it cannot reach them, for example when
the workers run behind a cluster firewall, start them with `--connect-back`,
and each worker opens the connections that bring its results. Python tasks run
in a TaskVine library: each worker runs one instance of it, which takes the
whole worker and runs one task per core, so workers of any size can join.
Commands run as standard TaskVine tasks with the cores they request. Stop the
worker with `Ctrl-C`.

The Executor installs that library once and keeps it for its lifetime.
`library_files` maps local files to names in the library sandbox, such as
Python modules the tasks import. `executor.port` and `executor.name` report
the port and the name of the manager, and `executor.capacity()` returns the
connected workers and their cores as a tuple with the fields `workers` and
`cores`, read about once a second.

While a Graph runs, the Executor draws a progress bar on standard output,
which `progress=False` turns off.

## Sharing modules and data across tasks

The library on each worker starts each Python task as a copy of itself, and
the copy keeps everything the library has already loaded. Anything loaded once
into the library is therefore free for every task on that worker.

`library_modules` names modules the library imports when it starts, as module
objects or names. Tasks then skip the import, which saves seconds for packages
such as `torch`.

`shared` gives the library objects to load when it starts, as a dictionary
from names to objects. A task argument `vg.Shared(name)` stands for that
object, and the task receives it without loading it:

```python
import ndcctools.taskvine.vine_graph as vg


def predict(model, value):
    return model["weight"] * value


model = {"weight": 3}
with vg.Executor(port=9123, library_modules=["numpy"], shared={"model": model}) as executor:
    tasks = [executor.submit(predict, vg.Shared("model"), value) for value in range(100)]
    print([task.result() for task in tasks])
```

A Graph can use `vg.Shared(name)` too, and keeps only the name, so the
Executor that runs it supplies the object. A task that names an object its
Executor does not hold fails. Each task gets its own copy of a shared object,
so a change a task makes stays within that task. Shared objects are given when
the Executor is created and stay in the library on every worker until the
Executor closes. They suit a few large objects that most tasks use, such as a
model or a lookup table. Every worker loads all of them, and a library that
holds many small Python objects starts each task more slowly, so pass data
that only some tasks read as ordinary arguments. Only Python tasks take shared
objects.

The setting `edata-cache` does the same for the functions and arguments of
tasks without naming them. It is a budget in bytes. The first `submit()` or
`run()` call serializes the functions and arguments of the tasks it starts,
takes those larger than 4 KB from smallest to largest until the next one does
not fit, and gives them to the library before any of those tasks runs. Smaller
objects need no cache, because they already travel with each task. Tasks then
read these objects from the library instead of fetching a file each. The
choice is made once: objects of later calls are fetched as files, so put a
whole graph in the first `run()` call to cover all of it.

```python
with vg.Executor(port=9123, params={"edata-cache": 1 << 30}) as executor:
    results = executor.run(graph)
```

## HTCondor workers

Start the program:

```bash
python vine_graph_distributed.py
```

In another shell, submit workers to HTCondor:

```bash
vine_submit_workers -T condor -M vine-graph-example \
  --cores 4 5
```

This submits five workers with four cores each. Use `condor_q` to check their
status. The submit command prints the cluster ID; use `condor_rm CLUSTER_ID` to
stop the workers.

## Using a factory

A factory adds and removes workers as the workload changes. Keep the program
running, then start the factory in another shell:

```bash
vine_factory -T condor -M vine-graph-example \
  --min-workers 1 --max-workers 10 --cores 4
```

To use the same conda environment on the workers, make a Poncho tarball:

```bash
poncho_package_create --ignore-editable-packages \
  "$CONDA_PREFIX" vine-graph-env.tar.gz
```

Pass it to the factory:

```bash
vine_factory -T condor -M vine-graph-example \
  --min-workers 1 --max-workers 10 --cores 4 \
  --poncho-env vine-graph-env.tar.gz
```

Stop the factory with `Ctrl-C` when the program is finished.

## Dynamic work

Work that is not known in advance is written in one of two ways, each for one
case. A program that looks at a result and then decides what to run calls
`executor.submit()`. A Graph that grows as its tasks complete uses rules.

### Submitting tasks one at a time

`executor.submit(func, *args, **kwargs)` starts a call at once, or a Command
with values for its placeholders, and returns its Task. `task.result()` waits
for the task and returns its result, so a program reads a result and submits
tasks built from it:

```python
import ndcctools.taskvine.vine_graph as vg


def score(value):
    return value * value


def refine(value):
    return value + 1


with vg.Executor(port=9123) as executor:
    scores = [executor.submit(score, value) for value in range(8)]
    refined = [executor.submit(refine, task) for task in scores if task.result() > 10]
    print(sorted(task.result() for task in refined))
```

Arguments may hold Files and Tasks that the same Executor started or ran, with
their selections and files: earlier submitted tasks, and tasks of a Graph that
the Executor ran. A task of a Graph that has not run cannot be an argument.

Submitted tasks run in the background from the moment they are submitted, so
the program can do other work meanwhile. `task.done()` tells whether the task
finished, and `task.failed()` and `task.cancelled()` how. `result()` takes an
optional `timeout` in seconds and raises `TimeoutError` when the task is still
running after it. A task that cannot start, as when an argument cannot be
serialized, fails, and its `result()` raises. `task.cancel()` stops a
submitted task that has not finished, together with the submitted tasks that
read it. `result()` of a cancelled task raises `RuntimeError`.

Leaving the `with` block without an exception waits for the submitted tasks
that still run, then closes the Executor. Leaving it with an exception
discards the results.

### Rules

A rule is a function that a run calls with each selected task that completes.
`@graph.on(*selectors)` registers the decorated function as a rule. A selector
is a function or a Command, which selects the tasks of the Graph that run it,
including those added later, or a Task of the Graph. A rule may add tasks to
its Graph, which run once it returns, cancel its tasks with `task.cancel()`,
or end the run with `graph.stop()`:

```python
import ndcctools.taskvine.vine_graph as vg


def step(value):
    return value + 1


graph = vg.Graph()
graph.add(step, 0)


@graph.on(step)
def again(task):
    if task.result() < 10:
        graph.add(step, task)


with vg.Executor(local=True) as executor:
    results = executor.run(graph)

assert list(results.values()) == [10]
assert len(graph.find(step)) == 10
```

Rules are called one at a time and in completion order, in the thread that
called `executor.run()`. While a rule runs, task states do not change. A rule
may read the result and the declared files of the task it is given and of any
other completed task, and `result()` of a task that is still running raises
`RuntimeError`. `executor.run()` returns once no task runs and no rule is due.
During a run, only a rule may add tasks, register rules, or call
`graph.find()`.

A task that failed or was cancelled calls no rule. A rule that raises fails
the run with a `RuntimeError` that names the rule and the task, with the
exception as its cause. `task.cancel()` on a task of a Graph works only inside
a rule: tasks not started never run, running ones are stopped, and the tasks
that read them are cancelled too. A completed task stays completed, and a
cancelled task stays cancelled. A run does not raise for cancelled tasks.

`graph.stop()` ends the run once the rule that calls it returns. No further
rule is called, running tasks are stopped, and `executor.run()` returns.
Tasks that did not finish go back to not started, so a later run continues
them. Rules stay registered across runs, and a task that completed in an
earlier run does not call them again.

Rules are the only callbacks. They select tasks of their own Graph, never the
tasks that `executor.submit()` returns, whose results the caller reads itself.

## How long results are kept

The Executor keeps every result that can still be asked for without a new
task: a sink of a Graph, a submitted task that no submitted task reads yet,
and every result or file that was read. Other intermediate results are
released once the tasks that read them are safe. One of them that is asked for
after all, by `result()`, by a file, or by a task added or submitted later, is
computed again, which the caller does not see. The setting `release-outputs`
set to `0` keeps every result.

A submitted task is let go when its Task object is no longer referenced, and
the tasks of a Graph when the Graph is collected. The Executor then removes
their results, functions, and arguments from the manager and the workers. A
long-running notebook therefore holds only the data its variables still refer
to.

Results are read through an open Executor. Once it closed, `result()` raises
`RuntimeError`, and what stays are the values the program already holds, such
as the dictionary that `executor.run()` returned, and the paths that
`TaskFile.path` returned.

## Notebooks

An Executor works like a file: use it in a `with` block, or create it in one
cell, use it in later cells, and close it in the last.

```python
# Cell 1
executor = vg.Executor(port=9123)
graph = vg.Graph()

# Cell 2
squares = [graph.add(score, value) for value in range(8)]
results = executor.run(graph)

# Cell 3, at any later time
print(squares[3].result())
total = graph.add(sum, squares)
print(executor.run(graph)[total])

# Cell 4
more = executor.submit(refine, squares[3])

# Cell 5
print(more.done(), more.result())

# Last cell
executor.close()
```

A cell that runs a Graph shows a progress bar and returns when the run ends. A
later cell may add tasks to the Graph and run it again, which runs only the
new tasks and any earlier result they read that was released. Submitted tasks
keep running between cells, so a later cell can check `task.done()` without
waiting, and `task.cancel()` stops a task that is no longer needed.

Interrupting a cell leaves the Executor usable. An interrupted
`executor.run()` stops the tasks that did not finish and returns them to not
started, so the next run continues them. An interrupted `result()` of a
submitted task leaves the task running, and the next cell can wait again.

A background thread advances the tasks, in scripts and notebooks alike, for as
long as the Executor is open. It uses no CPU while nothing is running and
stops when the Executor closes, which also happens at interpreter exit. A
local Executor (`local=True`) runs tasks in the calling thread and starts no
thread. While a background thread runs, `os.fork()` and the `fork` start
method of `multiprocessing` can deadlock the child process, as in any
multithreaded Python program. Use `subprocess`, or the `spawn` or `forkserver`
start method, which is the default on Linux since Python 3.14.

Workers started from a cell with `subprocess` should use
`start_new_session=True`. Jupyter interrupts the whole process group of the
kernel, which would otherwise stop the workers too, and a worker in its own
session also survives a kernel restart and reconnects.

Rerunning the cell that creates an Executor on the same fixed port closes the
open Executor first and prints a notice, so the new one can take the port. The
tasks of the closed Executor then report that it is closed, and a Graph that
it ran cannot run on the new one: build a new Graph. Workers that are not
single-shot reconnect to the new Executor on their own.

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
by_key = {task.key: task for task in graph.find()}
answer = by_key["answer"]
with vg.Executor(local=True) as executor:
    results = executor.run(graph)

assert results[answer] == 20
print(results[answer])
```

For Dask collections, pass a dictionary of Dask collection objects. Do not mix
collection and non-collection values in that dictionary.

## Executor settings

`vg.Executor(port, name, *, local, progress, shared, params, library_modules,
library_files, output_dir, **manager_options)` takes:

| Argument | Default | Purpose |
| --- | --- | --- |
| `port` | `9123` | Port workers connect to. `0` picks a free port, which `executor.port` reports. |
| `name` | `None` | Manager name that workers look up in the catalog. |
| `local` | `False` | Run each task in this process, without TaskVine, for debugging. |
| `progress` | `True` | Draw a progress bar on standard output while a Graph runs. |
| `shared` | `None` | Objects the library loads when it starts, as `{name: object}`, read by tasks through `vg.Shared(name)`. |
| `params` | `None` | Settings by name, as a dictionary, applied once when the Executor is created. See below. |
| `library_modules` | `()` | Modules, or module names, the library imports when it starts on a worker. |
| `library_files` | `None` | Local files given to the library, as `{local_path: name}`. |
| `output_dir` | `./outputs` | Parent of the directory that holds the file paths returned by `TaskFile.path`. |

Other keyword arguments, such as `ssl` or `run_info_path`, go to the TaskVine
manager. `params` takes these settings:

| Setting | Default | Purpose |
| --- | --- | --- |
| `task-priority-mode` | `largest-input-first` | Order of ready tasks: `largest-input-first`, `depth-first`, or `fifo`. |
| `max-retries` | `5` | Failed attempts allowed per task for infrastructure failures. See below. |
| `release-outputs` | `1` | `1` releases a result once every task that reads it is safe. `0` keeps every result. |
| `checkpoint-cut` | `auto` | Which large results are checkpointed. `auto` checkpoints every result while the manager keeps up, and only every few generations while large results complete faster than it stores them. `0` checkpoints every result, and `k` the last generation of each band of `k`. |
| `checkpoint-yield` | `10` | Seconds the checkpoint of a large result waits while tasks that read it run, so the worker's network link serves those tasks first. Each result waits at most once, and a result that `result()` waits for never waits. `0` starts every checkpoint at once. |
| `shared-directory` | none | A directory that the manager and every worker mount at the same path, such as a shared file system. Workers then copy large results there themselves as checkpoints, besides those the manager receives. |
| `edata-cache` | `0` | Bytes of task functions and arguments from the first `submit()` or `run()` call that the library loads when it starts, smallest first. |
| `vault-disk-limit` | `1536` | Gigabytes of checkpointed task results the manager may store, in memory or on disk. `0` means unlimited. |
| `vault-memory-limit` | `1` | Gigabytes of small checkpointed results (up to 64 KB each) the manager keeps in memory instead of writing them to disk. Beyond it, the oldest are written to the vault directory. `0` writes every checkpoint to disk. |
| `vault-shared-min-bytes` | `4194304` | Bytes from which a result is copied to `shared-directory` instead of being sent to the manager. |

Every other name is a TaskVine manager tuning parameter, such as
`wait-for-workers`. A name that is neither, or an invalid value, raises
`ValueError` before the Executor starts. A local Executor checks the settings
and applies none of them.

Each Executor creates a `vine-graph-run-*` directory under `output_dir` for
the files whose paths `TaskFile.path` returns, and in local mode for task
sandboxes. Closing it keeps only the files whose paths were returned. Leaving
a `with` block because of an exception removes the whole directory. Input
files are never modified.

## Task failures

A task fails when its function raises an exception, when its command exits
with an error status, when an input file is lost, or when other failures
repeat more than `max-retries` times, such as a declared output file that the
task did not write. The tasks that depend on a failed task fail too, and their
error names the task that failed first. Tasks that do not depend on it keep
running, and the Executor keeps accepting tasks. Lost workers are handled by
TaskVine and do not count as failures, unless a task keeps making its workers
exit, as a task that exceeds their memory does.

`task.result()` raises the task's `TaskError`, whose message includes the
traceback printed where the function ran. `error.task` is the Task and
`error.source` the Task whose own failure explains it, which is the task
itself when it failed on its own. The message names both tasks.
`error.reason` says why the source failed: `"function"` (its function raised,
or its command exited with an error status), `"signal"` (its function was
killed by a signal), `"workers"` (workers kept disconnecting while they ran
it), `"input"` (a file it reads is missing at the manager), `"retries"`,
`"submit"` (it could not be submitted, as when an argument cannot be
serialized), or `"result"` (it completed, but its result cannot be loaded or
sent). `error.exception` is the exception the source's function raised, loaded
in the calling process with the traceback where it ran as a note, so a program
can handle it by its type, for the task and for each task that depends on it.
It is `None` for other reasons, and when the exception cannot be serialized or
loaded where the program runs. `error.exit_code` is the exit status of the
source's command when it exited with an error status, and `None` otherwise.

`executor.run()` raises a `TaskError` once the other tasks finished,
preferring a task that failed on its own. A failed task stays failed. A later
run of the Graph that starts no task reading it returns the results of the
other sinks, and a task added later that reads it fails without running. Fix
the cause, then add the task again. `TaskError` is a `RuntimeError`.

## Checkpoints and worker loss

Every task result is copied from a worker to the manager in the background
after its task completes. Results of longer-running tasks are copied first.
Copies use spare transfer capacity only: workers fetching data from the
manager always come first. The copy of a large result also waits, at most
`checkpoint-yield` seconds, while tasks that read it run, because both leave
its worker through the same network link. When the stored copies reach
`vault-disk-limit`, new copies wait until space is freed, and the manager
prints a notice once. A result is removed from its worker only after the
results of the tasks that read it are copied, so a lost result never needs
more than its producer to run again. While the limit is reached, results
therefore stay on workers longer and worker disk use grows until downstream
tasks finish. Raise `vault-disk-limit` if that happens. A result that is no
longer needed is removed, along with its copy or any copy in progress.

With `shared-directory` set, the worker that holds a large result copies it
into a directory of the manager's own there, in the background, and the
manager receives smaller results itself, so checkpoints use the bandwidth of
both. Tasks never wait for these copies. A copy counts toward
`vault-disk-limit`, is deleted when its result is no longer needed, and the
directory is removed when the Executor closes.

When large results still complete faster than they can be stored,
`checkpoint-cut auto` checkpoints only the last generation of each band of a
few generations, with bands just wide enough for the copies to keep up, so a
lost result recomputes at most its band.

If workers are lost while the Executor is open, new workers can fetch
checkpointed results from the manager instead of rerunning their producers.
Results without checkpoints are recomputed. Checkpoints last only as long as
the Executor; they do not allow a program to resume after it exits.

## How it works

This section describes the parts of Vine Graph and how a task moves through
them. It is meant for people who maintain the code, or who need to reason about
its behavior when workers fail.

### Components

```
Python program
  vg.Graph, vg.Executor, vg.Task, adaptors.from_dask
        |
Python package (taskvine/src/bindings/python3/ndcctools/taskvine/vine_graph)
  graph.py      what to run: tasks, the data they read, and rules
  executor.py   where to run: the manager, the C executor, a background thread
  library.py    how a Python task runs on a worker
        |   vine_graph_executor.h, the only C interface
C executor (taskvine/src/vine_graph)
  vine_graph.c            nodes are tasks, edges are the files between them
  vine_graph_executor.c   submission, failures, releases, checkpoints, fetches
        |   TaskVine API
TaskVine manager
  scheduling, files, recovery tasks, libraries, and a data service
  with its own port and a vault on the manager disk
        |
TaskVine workers
  disk cache, transfers between workers
```

The manager and the workers are general TaskVine code. Everything that knows
about graphs lives in the C executor and the Python package.

### The Python package

`graph.py` replaces the Tasks, selections, and files in a task's arguments with
small references. It replaces all arguments in one pass, so an object passed as
two arguments is still one object when the task runs. A Graph holds its tasks
and its rules. The tasks that `submit()` starts live in a graph that the
Executor holds, which refers to them weakly, so a submitted task lives exactly
as long as the program refers to its Task.

`executor.py` uses a background thread besides the threads of the program. The
thread that calls `submit()` or `run()` checks the arguments, serializes them,
queues the calls into C, and returns or waits. The background thread is the
only one that calls into C. It runs the queued calls in order, advances the
run, and publishes which tasks finished and which results can be read. What it
published is applied to the Tasks by the thread of a run, between rules, so a
rule sees task states that do not change, or by a thread that asks for a
submitted task. `result()` and `done()` only read what was published, so an
interrupt ends a wait without affecting the tasks. Queuing a call wakes the
background thread from its wait in the manager, so the call is handled at once
even while workers are busy.

`library.py` is the TaskVine library that runs the Python tasks. Each Executor
installs one library under a unique name. Every Python task calls its
`run_node()` function, which reads a manifest in the task sandbox, loads the
function and its arguments, calls it, and returns the result for the library
to write. A function that raises prints its traceback and exits with an error
status. A command runs as a standard TaskVine task instead. Its command line
sends its standard output to the result file and fails when the command did
not write a declared file.

A local Executor starts no manager, no C executor, and no background thread. It
runs each task in the calling thread, the tasks of a Graph in the order they
were added and a submitted task when it is submitted, through the same code in
`library.py` that runs tasks on workers, and checks its settings by the same
rules. It runs commands with `/bin/sh`, each in a sandbox directory of its
own.

### Data

Vine Graph moves three kinds of data:

| Kind | Contents | Location | Removed |
| --- | --- | --- | --- |
| Arguments | The function and each argument of a Python task, serialized one by one. Objects up to 4 KB travel inside the task's manifest; larger ones are files, and an object passed to several tasks is stored once. | Manager disk, served to workers from the data port | When the program can no longer ask for any task that uses them |
| Results | The return value of a task or the standard output of its command, and any files it declared | Worker caches, as TaskVine temporary files | When every task that reads them has a durable result, unless the result is kept |
| Checkpoints | Copies of results | The vault: manager memory for results up to 64 KB, within `vault-memory-limit`, and otherwise a directory on the manager disk | With the result they copy |

Workers fetch arguments and checkpoints from the manager's data port, except
that files up to 64 KB, such as manifests, travel with their task. They fetch
results from each other, or from the vault when no worker has a copy.

### The life of a task

1. `submit()` or `run()` serializes the function and arguments, builds a
   manifest that names every file the task reads, and queues the task. For a
   command it builds the command line instead.
2. The background thread declares those files in C, adds a node with its
   inputs and outputs, and submits it. The node's parents are the tasks that
   write the files it reads. A node may read only results of nodes submitted
   before it, so the graph can grow while it runs and never forms a cycle.
3. Once its parents complete, the C executor submits a TaskVine task, which
   runs in the library or as a command. Ready tasks are ordered by
   `task-priority-mode`.
4. A worker fetches the task's inputs and runs it. Its result stays in the
   worker cache.
5. When the task returns, the C executor marks the node completed, queues its
   results for checkpointing, releases results that are no longer needed, and
   submits the tasks it unblocked. A failure is handled as described under
   Task failures.
6. A result that can be asked for without a new task is pinned when its task
   is submitted. `result()`, and `run()` for the sinks, ask for the result to
   be copied into the vault and read that copy.

### Releasing results

A result is removed from the workers once every task that reads it has a
durable result, which means one that is copied into the vault or was itself
removed. A lost result then needs only its own task to run again, never a chain
of earlier tasks. A pinned result is never removed. Removing a result also
removes its checkpoint, cancels a copy in progress, or withdraws it from the
checkpoint queue. One completion can remove results several levels upstream.

A recovery task runs because another task needs its result now. When it
completes, the results it read may be removed, but its own result waits until
the tasks that read it complete. Removing it at once would let worker churn
lose it again before it is read.

A Graph may grow after it ran, as in a notebook, and a task added later may
read a result that was already removed, which then runs again. A task that no
task read when it was submitted keeps its result until a task that reads it is
submitted, so a loop that adds a step reading the previous one and runs the
Graph runs each step once. A task that ran again after its result was removed
keeps the result until the program lets the task go.

When the vault is full, new checkpoints are refused and the rule stays the
same. Results wait on the workers until downstream tasks are done or the vault
has room again, and the manager prints a notice once. Removing results as soon
as their readers complete would keep worker disks smaller, but a lost result
could then need a long chain of tasks to run again, and with workers leaving
all the time that work never catches up. Keeping a few more levels does not
help, because consecutive tasks of a chain tend to run on the same worker.

### Checkpoints

Every result is queued for checkpointing when its task completes, and results
of longer-running tasks are copied first. The data service handles up to 128
workers fetching data and up to 128 checkpoint copies at once, and neither
direction waits for the other. With a shared directory, up to 128 more large
results are copied by their workers at once. A failed copy is queued again
after the manager's `transient-error-interval`.

### Arguments and recovery

The Executor keeps the function and arguments of each task while the program
can still ask for the task, through its Graph or its Task, so it can run the
task again when a worker loses its result or when a released result is asked
for. Once the Graph is collected, or nothing refers to a submitted Task, it
deletes them from the manager and the workers, drops its own reference to the
original objects, and has the C executor remove the task's node once the task
finished and its results were released. An object shared by several tasks
stays until the last of them goes.

### Closing

Closing an Executor stops the background thread, cancels its tasks,
undeclares its files, removes its library, and deletes the data of the run
except the files whose paths `TaskFile.path` returned.
