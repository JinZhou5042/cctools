# Vine Graph

Vine Graph runs Python function calls on TaskVine, either as a graph built in
advance or as tasks submitted one at a time while others run. Import it as:

```python
import ndcctools.taskvine.vine_graph as vg
```

## The programming model

There are two main objects, which keep what to run apart from where it runs:

- `vg.Graph`: a description of tasks and the data they pass to each other.
  `graph.add(func, *args, **kwargs)` records a call without running it and
  returns a `vg.Node` that stands for the call's result. A Graph holds no
  runtime state, so it can be pickled, shared, and run any number of times.
- `vg.Executor`: runs tasks on TaskVine workers. `executor.submit()` runs a
  task and returns a `vg.Future`, and `executor.run(graph)` runs a Graph and
  returns its results.

A Node or a Future passed as an argument of another task gives that task its
result, so passing them builds the dependencies. Arguments may hold them
anywhere inside lists, tuples, sets, dictionary values, and dataclasses.
`node["key"]` and `node.attr("name")` select an item or an attribute of a
result without adding a task, and so do the same operations on a Future.
Files are `vg.File(path)` for an existing local file, and `node.file(path)`,
a `vg.NodeFile`, for a file that a task writes in its sandbox. A task receives
either kind as a local path.

A task that fails raises `vg.TaskError`.

## First graph: local execution

`vg.Executor(local=True)` runs each task in this process, without TaskVine,
which is handy for checking a graph and its functions. Create
`vine_graph_local.py`:

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
    results = executor.run(graph, [answer])

assert results[answer] == 82
print(results[answer])
```

Run it with:

```bash
python vine_graph_local.py
```

`executor.run(graph, targets)` runs the targets and every task they read, and
returns a dictionary from each target to its result. Without targets, it runs
the tasks that no other task reads, which is the whole graph.

## Passing files between tasks

A task reads a `vg.File` or a `NodeFile` given as an argument at a local path.
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
    results = executor.run(graph, [consumer, produced_file])

assert results[consumer] == "VINE GRAPH\n"
assert Path(results[produced_file]).read_text() == "VINE GRAPH\n"
print(results[consumer], end="")
```

An output path is a non-empty relative path inside the task sandbox, and
declaring the same path again returns the same `NodeFile`. A task may read
only Nodes of its own Graph. The result of a `NodeFile` target is a local path
under `output_dir`, which stays after the Executor closes.

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
    results = executor.run(graph, [total])

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
two, such as `port=(9123, 9124)`, so that both ports are known. Each worker runs one instance of the library that runs
tasks, which takes the whole worker and runs one task per core, so workers of
any size can join. Stop the worker with `Ctrl-C`.

The Executor installs that library once and keeps it for its lifetime.
`library_modules` lists modules the library imports when it starts on a
worker, and `library_files` maps local files to names in the library sandbox,
such as Python modules the tasks import.

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

## Submitting tasks while others run

`executor.submit(func, *args, **kwargs)` runs a call as soon as the tasks it
reads are done, and returns a `vg.Future`. A program can read results as tasks
finish and submit tasks built from them. `executor.as_completed()` yields
Futures in the order their tasks finish:

```python
import ndcctools.taskvine.vine_graph as vg


def score(value):
    return value * value


def refine(value):
    return value + 1


with vg.Executor(port=9123) as executor:
    futures = [executor.submit(score, value) for value in range(8)]
    refined = []
    for future in executor.as_completed(futures):
        if future.result() > 10:
            refined.append(executor.submit(refine, future))
    print(sorted(future.result() for future in refined))
```

`executor.submit(node)` runs a Graph task, after every task it reads, and
returns its Future. `submit(node_file)` stands for a file a task writes, and
`submit(vg.File(path))` for a local file. A Node may also be an argument of
`executor.submit(func, ...)`, which then runs it first.

`future.result()` waits for the task and returns its value, or the local path
of a file. `future.exception()` returns the task's `TaskError` instead of
raising it, and `future.done()` tells whether the task finished. Both
`result()` and `exception()` take an optional `timeout` in seconds.
`executor.wait()` waits for every submitted task, or for the given Futures,
and `executor.stats()` reports the makespan and task counts.

Tasks run in the background from the moment they are submitted, so the
program can do other work meanwhile. `executor.progress()` returns how many
submitted tasks completed, failed, or are still waiting, as a `vg.Progress`.
`future.cancel()` stops a task that has not finished, and fails it together
with every task that reads its result. It returns `False` when the task had
already finished, and `future.cancelled()` tells whether `cancel()` stopped it.

While a Future exists, the Executor keeps the data it stands for, so tasks
submitted later read it directly. Data that no Future refers to is removed once
the tasks that read it complete, including the results of a finished
`executor.run()`. Submitting the task again runs it again. A Future cannot be
pickled; pickle the Graph instead.

## Notebooks

An Executor works like a file: use it in a `with` block, or create it in one
cell and close it in another. The result of any task can be requested at any
time through its Node. `executor.submit(node).result()` returns the result of a
task that already ran, waits for one that is running, and runs one that has not
run or whose result was removed.

```python
# Cell 1
executor = vg.Executor(port=9123)
graph = vg.Graph()

# Cell 2
squares = [graph.add(score, value) for value in range(8)]
futures = [executor.submit(node) for node in squares]

# Cell 3, at any later time
print(executor.submit(squares[3]).result())
print(executor.submit(refine, futures[3]).result())

# Last cell
executor.close()
```

The cell that creates the Executor shows a progress bar, which follows the run
while later cells execute. Tasks keep running between cells, so a later cell
can check `future.done()` or `executor.progress()` without waiting, and
`future.cancel()` stops a task that is no longer needed.

Interrupting a cell, for example while `result()` waits or while tasks are
submitted, leaves the Executor usable: the tasks keep running, and the next
cell can wait again or submit the interrupted task again. One Executor can run
any number of Graphs, and tasks of different Graphs may run at the same time.

Workers started from a cell with `subprocess` should use
`start_new_session=True`. Jupyter interrupts the whole process group of the
kernel, which would otherwise stop the workers too, and a worker in its own
session also survives a kernel restart and reconnects.

Rerunning the cell that creates an Executor on the same port closes the open
Executor first and prints a notice, so the new one can take the port. Its
Futures then report that it is closed. Workers that are not single-shot
reconnect to the new Executor on their own.

## Sharing a graph

A Graph is a description, so it can be pickled with `cloudpickle` and run by
anyone with an Executor. Pickle the Nodes you need together with the Graph:

```python
import cloudpickle

with open("analysis.pkl", "wb") as stream:
    cloudpickle.dump((graph, total), stream)

# On another machine
with open("analysis.pkl", "rb") as stream:
    graph, total = cloudpickle.load(stream)
with vg.Executor(port=9123) as executor:
    print(executor.run(graph, [total])[total])
```

`graph.node(key)` returns the Node with a key, `graph.sinks()` the Nodes that
no other task reads, and iterating a Graph yields all its Nodes. A `vg.File`
stands for a path on the machine that runs the Executor.

## Dask graphs

`from_dask()` in `ndcctools.taskvine.vine_graph.adaptors` converts a low-level
Dask task dictionary, or a dictionary of Dask collections, into a Graph whose
keys are the Dask keys:

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
answer = graph.node("answer")
with vg.Executor(local=True) as executor:
    results = executor.run(graph, [answer])

assert results[answer] == 20
print(results[answer])
```

For Dask collections, pass a dictionary of Dask collection objects. Do not mix
collection and non-collection values in that dictionary.

## Executor settings

`vg.Executor(port, name, *, local, output_dir, library_modules, library_files,
params, **manager_options)` takes:

| Argument | Default | Purpose |
| --- | --- | --- |
| `port` | `9123` | Port workers connect to. `0` picks a free port, which `executor.port` reports. |
| `name` | `None` | Manager name that workers look up in the catalog. |
| `local` | `False` | Run each task in this process, without TaskVine, for debugging. |
| `output_dir` | `./outputs` | Parent of the directory that holds file results returned by `result()`. |
| `library_modules` | `()` | Modules the library imports when it starts on a worker. |
| `library_files` | `None` | Local files given to the library, as `{local_path: name}`. |
| `params` | `None` | Settings applied with `set_params()`. |

Other keyword arguments, such as `ssl` or `run_info_path`, go to the TaskVine
manager. `executor.set_params({...})` changes settings by name, and they take
effect at once:

| Setting | Default | Purpose |
| --- | --- | --- |
| `task-priority-mode` | `largest-input-first` | Order of ready tasks: `largest-input-first`, `depth-first`, or `fifo`. |
| `max-retries` | `5` | Failed attempts allowed per task for infrastructure failures. See below. |
| `progress-bar` | `1` | Draw a progress bar on standard output while tasks run; `0` disables it. Notebooks and interactive shells default to `0`: a notebook shows progress in the cell, and `executor.progress()` reports it anywhere. |
| `vault-disk-limit` | `1536` | Gigabytes of checkpointed task results the manager may store. `0` means unlimited. |

Other names are TaskVine manager tuning parameters, such as
`wait-for-workers`. A name the manager does not recognize raises `ValueError`.
A local Executor ignores manager tuning.

Each Executor creates a `vine-graph-run-*` directory under `output_dir` for
file results, and in local mode for task sandboxes. Closing it keeps only the
files whose paths `result()` returned. Leaving a `with` block because of an
exception removes the whole directory. Input files are never modified.

## Task failures

A task fails when its function raises an exception, when an input file is
lost, or when other failures repeat more than `max-retries` times, such as a
declared output file that the task did not write. The tasks that depend on a
failed task fail too, and their error names the task that failed first. Tasks
that do not depend on it keep running, and the Executor keeps accepting tasks.
Lost workers are handled by TaskVine and do not count as failures.

`future.result()` raises the task's `TaskError`, whose message includes the
traceback printed where the function ran. `error.task` is the Node of the task
and `error.source` the Node of the task whose own failure explains it.
`executor.run()` raises a `TaskError` once its targets finished, preferring a
task that failed on its own. A failed task stays failed in its Executor: fix
the cause, then add the task again or run the Graph in a new Executor.
`TaskError` is a `RuntimeError`.

## Checkpoints and worker loss

Every task result is copied from a worker to the manager in the background
after its task completes. Results of longer-running tasks are copied first.
Copies use spare transfer capacity only: workers fetching data from the
manager always come first. When the stored copies reach `vault-disk-limit`, new
copies wait until space is freed. A result that is no longer needed is removed,
along with its copy or any copy in progress.

If workers are lost while the Executor is open, new workers can fetch
checkpointed results from the manager instead of rerunning their producers.
Results without checkpoints are recomputed. Checkpoints last only as long as
the Executor; they do not allow a program to resume after it exits.

## Run the project regression tests

The tests are Python scripts in `taskvine/src/vine_graph/test`. Tests that
need workers start local workers from this repository's build. After building
the repository, use the Python environment selected at configuration time.
From the repository root:

```bash
export PYTHONPATH="$PWD/test_support/python_modules/python3${PYTHONPATH:+:$PYTHONPATH}"
cd taskvine/src/vine_graph/test
python3 vine_graph_workflow_examples.py
python3 vine_graph_dask_adaptor.py
python3 vine_graph_recovery.py
python3 vine_graph_checkpoint.py
```

| Script | Coverage |
| --- | --- |
| `vine_graph_interface.py` | Every public operation at its edges, with a local Executor and with workers. |
| `vine_graph_notebook.py` | A notebook session in a real Jupyter kernel. Requires `jupyter_client` and `ipykernel`. |
| `vine_graph_workflow_examples.py` | Example graphs, argument corner cases, and rejected graphs. |
| `vine_graph_dynamic.py` | Tasks submitted while others run, isolated failures, recomputed results, notebook use, and shared graphs. |
| `vine_graph_dask_adaptor.py` | Dask graph conversion. Requires `dask`. |
| `vine_graph_data.py` | Example graphs on the worker data path, including large graphs. |
| `vine_graph_edata.py` | Per-task serialized callables and arguments. |
| `vine_graph_boundaries.py` | The C interface, and one Graph run by local and worker Executors. |
| `vine_graph_lifecycle.py` | Executor cleanup at each failure point. |
| `vine_graph_recovery.py` | Checkpoint order and the manager disk limit with workers. |
| `vine_graph_checkpoint.py` | Recovery from manager checkpoints after every worker cache is lost. |
| `vine_graph_worker_churn.py` | A three-minute run that replaces a worker every 20 seconds. |
| `vine_graph_scale.py` | A 4,000-node graph under worker churn, total cache loss, and a small manager disk limit. |

`run_all_tests.sh` runs `vine_graph_interface.py`, `vine_graph_data.py`,
`vine_graph_edata.py`, `vine_graph_dynamic.py`, `vine_graph_recovery.py`,
`vine_graph_dask_adaptor.py`, and `vine_graph_notebook.py`.
