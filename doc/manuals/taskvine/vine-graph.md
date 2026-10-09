# Vine Graph

Vine Graph describes a directed acyclic graph (DAG) of Python function calls and
runs it with TaskVine. A `Workflow` records the tasks and their dependencies;
`VineGraph` runs the graph and returns the requested results.

## The programming model

The main objects are:

- `Workflow`: a reusable description of tasks, dependencies, and the handles
  created for them. The same workflow may be run more than once.
- `TaskHandle`: identifies a task. `workflow.add_task()` returns one.
- `TaskOutputHandle`: represents a task's Python return value, or a selected
  part of that value. Obtain one with `task.output()`. Use `[key]` to select
  an item and `.attr(name)` to select an attribute.
- `FileHandle`: represents an existing frontend file or a file produced in a
  task sandbox.
- `VineGraph`: a TaskVine manager that executes a completed workflow.

`VineGraph.run()` is synchronous: it returns after execution finishes. The
returned dictionary is keyed by the target handles supplied by the caller.

## First workflow: local execution

Local execution runs the graph in the manager process and does not need a
worker. It is handy for checking graph construction and task functions. Create
`vine_graph_local.py`:

```python
from ndcctools.taskvine.vine_graph import VineGraph, Workflow


def make_record(value):
    return {"values": [value, value + 1], "metadata": {"count": 2}}


def scaled_sum(values, count, scale=1):
    assert len(values) == count
    return sum(values) * scale


workflow = Workflow()
record = workflow.add_task(make_record, 20)
answer = workflow.add_task(
    scaled_sum,
    record.output()["values"],
    record.output()["metadata"]["count"],
    scale=2,
)

with VineGraph(port=0) as manager:
    results = manager.run(
        workflow,
        targets=[answer],
        params={
            "local-execute": 1,
            "output-dir": "./vine-graph-local-output",
        },
    )

assert results[answer] == 82
print(results[answer])
```

Run it with:

```bash
python vine_graph_local.py
```

Passing `record` directly would be an error because a `TaskHandle` identifies a
task, not its value. Pass `record.output()` instead. Indexing an output handle,
as in `record.output()["values"]`, selects a value inside the consuming task
without adding another graph node.

To request every terminal result, pass `targets=workflow.sink_tasks()`.
`sink_tasks()` returns handles for all tasks with no downstream consumers.

## Passing files between tasks

Use `workflow.file(path)` for an existing frontend file. Use
`task.file(relative_path)` for a file that the task will create inside its
sandbox. A consumer receives either kind of handle as a local path string.

For example, `vine_graph_files.py` passes a frontend file to one task and its
output file to another:

```python
from pathlib import Path

from ndcctools.taskvine.vine_graph import VineGraph, Workflow


def uppercase(source_path):
    text = Path(source_path).read_text().upper()
    Path("result.txt").write_text(text)
    return len(text)


def verify(result_path, expected_length):
    text = Path(result_path).read_text()
    assert len(text) == expected_length
    return text


Path("input.txt").write_text("vine graph\n")

workflow = Workflow()
source = workflow.file("input.txt")
producer = workflow.add_task(uppercase, source)
produced_file = producer.file("result.txt")
consumer = workflow.add_task(verify, produced_file, producer.output())

with VineGraph(port=0) as manager:
    results = manager.run(
        workflow,
        targets=[consumer, produced_file],
        params={
            "local-execute": 1,
            "output-dir": "./vine-graph-file-output",
        },
    )

assert results[consumer] == "VINE GRAPH\n"
assert Path(results[produced_file]).read_text() == "VINE GRAPH\n"
print(results[consumer], end="")
```

Task output paths must be non-empty relative paths that stay inside the task
sandbox. Each output path may be declared only once for a given task. Handles
belong to one `Workflow` and cannot be passed into another workflow.

## Distributed execution with a worker

Distributed mode is the default. The manager creates a TaskVine task-runner
library, and workers execute the graph nodes. Create
`vine_graph_distributed.py`:

```python
from ndcctools.taskvine.vine_graph import VineGraph, Workflow


def square(value):
    return value * value


def add_all(*values):
    return sum(values)


workflow = Workflow()
squares = [workflow.add_task(square, value) for value in range(1, 6)]
total = workflow.add_task(add_all, *(task.output() for task in squares))

with VineGraph(port=0, name="vine-graph-example") as manager:
    results = manager.run(
        workflow,
        targets=[total],
        params={
            "libcores": 4,
            "output-dir": "./vine-graph-distributed-output",
        },
    )

assert results[total] == 55
print(results[total])
```

Run the manager in one terminal:

```bash
python vine_graph_distributed.py
```

Start a worker in another terminal:

```bash
vine_worker -M vine-graph-example --cores 4
```

The manager name is used by remote workers to find the manager through the
TaskVine catalog. Stop the worker with `Ctrl-C`.

`--cores` on `vine_worker`, `vine_submit_workers`, and `vine_factory` must match
the manager's `libcores`. All examples here use 4.

## HTCondor workers

Start the manager:

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

A factory adds and removes workers as the workload changes. Keep the manager
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

Stop the factory with `Ctrl-C` when the workflow is finished.

## Dask graphs

Set `from_dask=True` to convert a Dask-style graph. Low-level task dictionaries
and common Dask collection forms are supported. For example:

```python
from ndcctools.taskvine.vine_graph import VineGraph


def increment(value):
    return value + 1


def multiply(left, right):
    return left * right


dask_graph = {
    "incremented": (increment, 1),
    "answer": (multiply, "incremented", 10),
}

with VineGraph(port=0) as manager:
    results = manager.run(
        dask_graph,
        targets=["answer"],
        from_dask=True,
        params={
            "local-execute": 1,
            "output-dir": "./vine-graph-dask-output",
        },
    )

assert results["answer"] == 20
print(results["answer"])
```

For Dask collections, pass a dictionary of Dask collection objects. Do not mix
collection and non-collection values in that dictionary.

## Execution parameters

Parameters may be supplied through the `params` argument to `run()` or through
`manager.set_params()` before `run()`.

Useful parameters include:

| Parameter | Default | Purpose |
| --- | --- | --- |
| `local-execute` | `0` | Run in process when set to `1`; use TaskVine workers when `0`. |
| `output-dir` | `./outputs` | Parent directory for per-run data. See below. |
| `libcores` | `16` | Number of cores assigned to the task-runner library. |
| `vault-disk-limit` | `1536` | Gigabytes of checkpointed task outputs the manager may store. `0` means unlimited. |

Unrecognized parameter names are passed to the TaskVine manager as tuning
parameters.

Each `run()` creates a new `vine-graph-run-*` directory under `output-dir`.
It holds serialized task results and, in local mode, task sandboxes. After a
successful run, only the paths of `FileHandle` targets returned by `run()` are
kept; everything else in that directory is removed. A failed or interrupted
run removes the whole directory. Original input files are never modified.

The `checkpoint-dir`, `checkpoint-fraction`, and `checkpoint-threshold-sec`
parameters are no longer supported and raise `ValueError`.

## Checkpoints and worker loss

Every intermediate task output is copied from a worker to the manager in the
background after its task completes. Outputs of longer-running tasks are copied
first. Copies use spare transfer capacity only: workers fetching data from the
manager always come first. When the stored copies reach `vault-disk-limit`, new
copies wait until space is freed. An output that is no longer needed is
removed, along with its copy or any copy in progress.

If workers are lost while the manager is still running, new workers can fetch
checkpointed outputs from the manager instead of rerunning their producers.
Outputs without checkpoints are recomputed. Checkpoints last only for the
current run and are removed when it ends; they do not allow a run to resume
after the manager exits.

## Run the project regression tests

The tests are Python scripts in `taskvine/src/vine_graph/test`. Tests that
need workers start local workers from this repository's build. After building the repository, use the Python
environment selected at configuration time. From the repository root:

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
| `vine_graph_workflow_examples.py` | Workflow examples and corner cases. |
| `vine_graph_dask_adaptor.py` | Dask graph conversion. Requires `dask`. |
| `vine_graph_data.py` | Example graphs on the worker data path, including large graphs. |
| `vine_graph_edata.py` | Per-task serialized callables and arguments. |
| `vine_graph_boundaries.py` | Reusing one workflow across local and worker runs. |
| `vine_graph_lifecycle.py` | Run cleanup at each failure point. |
| `vine_graph_recovery.py` | Checkpoint order and the manager disk limit with workers. |
| `vine_graph_checkpoint.py` | Recovery from manager checkpoints after every worker cache is lost. |
| `vine_graph_worker_churn.py` | A three-minute run that replaces a worker every 20 seconds. |
| `vine_graph_scale.py` | A 4,000-node graph under worker churn, total cache loss, and a small manager disk limit. |

`run_all_tests.sh` runs `vine_graph_data.py`, `vine_graph_edata.py`,
`vine_graph_recovery.py`, and `vine_graph_dask_adaptor.py`.
