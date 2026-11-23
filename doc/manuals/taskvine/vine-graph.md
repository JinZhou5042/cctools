# Vine Graph

Vine Graph runs Python functions and shell commands as tasks on TaskVine.
Tasks read each other's results, and the graph they form may grow while it
runs.

## Install

Vine Graph is on the `vine-graph-new` branch of
`https://github.com/JinZhou5042/cctools`. It needs a C compiler, `make`,
`swig`, and Python 3 with its development headers, `cloudpickle`, and
`threadpoolctl`. Build it in an activated conda environment that has them.

```sh
unset PYTHONPATH
git clone -b vine-graph-new https://github.com/JinZhou5042/cctools.git cctools-src
cd cctools-src
./configure --prefix $CONDA_PREFIX --with-base-dir $CONDA_PREFIX
make -j8
make install
```

This installs into the conda environment, replacing any `ndcctools` it had, so
every terminal with the environment activated can run programs and workers.
Check the installation with the example, which ends by printing `vine_graph example: OK`.

```sh
python3 taskvine/src/examples/vine_example_graph.py --local       # in this process
python3 taskvine/src/examples/vine_example_graph.py --port 9200   # as a manager, and
vine_worker localhost 9200                                         # in a second terminal
```

## Quick start

```python
import ndcctools.taskvine.vine_graph as vg

def square(value):
    return value * value

def per_value(count):
    return [vg.Function(square, value) for value in range(int(count))]

count_task = vg.Command("echo {n}", n=4)          # a shell command, whose result is "4\n"
squares_task = vg.Expand(per_value, count_task)  # four Functions, decided from that result
total_task = vg.Function(sum, squares_task)      # reads their results [0, 1, 4, 9]

with vg.Executor(local=True) as ex:
    print(ex.run(total_task).result())  # 14
```

Three rules cover the interface.

1. **Make tasks** with `vg.Function(func, *args, **kwargs)`, which calls
   `func`, `vg.Command(template, **fields)`, which runs a shell command, or
   `vg.Expand(builder, *args, **kwargs)`, which makes more tasks from results.
2. **A task is a future.** Give it to another task and that task receives its
   result. Call `task.result()` and the program receives it. The arguments
   are the graph, and a task starts as soon as the tasks it reads finished.
3. **Run tasks with an Executor.** `ex.submit(tasks)` starts a task or a list
   of tasks, with every task they read, and returns them. `ex.run(tasks)` does
   the same and waits. So `ex.run(task).result()` runs a task and reads it.

`local=True` runs everything in this process, which is enough to try things.
[Running on workers](#running-on-workers) shows the rest. The example
`vine_example_graph.py` uses every part of the interface in 100 lines.

## Tasks

### Functions

`vg.Function` takes any callable that `cloudpickle` can serialize, such as a function,
a lambda, a builtin such as `sum`, a `functools.partial`, a bound method, a
class, or an object with `__call__`. A task can receive a whole result, an item of one (`task["key"]`, or
`task.attr("name")` for an attribute), or tasks inside lists, dictionaries,
and dataclasses. In the program, read an item as `task.result()["key"]`.

```python
stats_task = vg.Function(stats, data_task)
plot_task = vg.Function(plot, stats_task["mean"], [model_task, other_task])
```

A task's arguments are fixed when it is made. A task cannot be used inside a
function, so pass it as an argument.

### Commands

`vg.Command(template, **fields)` runs a command line in its own directory and
returns its standard output (at most 1 MiB). Each `{field}` is filled
shell-quoted. A file becomes its path, a task `str()` of its result, and a list
each of its elements.

```python
import ndcctools.taskvine.vine_graph as vg
from pathlib import Path

Path("words.txt").write_text("pear\napple\nfig\n")

sort_task = vg.Command("sort {source} > sorted.txt", source=vg.File("words.txt")).set(outputs=["sorted.txt"])
first_task = vg.Command("head -n 1 {source}", source=sort_task.file("sorted.txt"))

with vg.Executor(local=True) as ex:
    assert ex.run(first_task).result() == "apple\n"
```

A non-zero exit fails the task. Write `{{` and `}}` for literal braces.

### Files

- `vg.File(path)` is a local file or directory a task reads. A Function
  receives its path, under the same base name.
- `vg.URL(url)` is a remote file (`http`, `https`, `ftp`, `root://`) that each
  worker downloads once.
- `task.file(name)` is a file that the task writes. Declare it before the task
  runs, with `task.file(name)` or `set(outputs=[name])`. Give it to other tasks,
  and once the task finished read it with `.text`, `.content` (bytes, up to 16
  MiB), or `.save(path)`.

```python
import ndcctools.taskvine.vine_graph as vg
from pathlib import Path

def uppercase(path):
    Path("result.txt").write_text(Path(path).read_text().upper())

Path("input.txt").write_text("vine graph\n")
result_file = vg.Function(uppercase, vg.File("input.txt")).file("result.txt")

with vg.Executor(local=True) as ex:
    length_task = ex.run(vg.Function(lambda path: len(Path(path).read_text()), result_file))
    assert length_task.result() == 11
    assert result_file.text == "VINE GRAPH\n"
    result_file.save("kept/result.txt")
```

Never write to an input, since it may be a shared copy.

### Settings

`task.set(**settings)` sets any of these before the task runs and returns the
task. A dictionary such as `big = dict(cores=4, retries=2)` gives many tasks the
same settings with `.set(**big)`.

| Setting | Default | Meaning |
| --- | --- | --- |
| `cores` | `1` | Cores the task uses. It also limits OpenMP and BLAS threads. |
| `memory` | `None` | Memory in MB. A command that uses more fails. |
| `gpus` | `0` | GPUs the task uses. |
| `outputs` | `()` | Files the task writes, such as `["{n}.jpg"]`, with placeholders filled from its arguments. A missing one fails the task. |
| `time_limit` | `None` | Seconds after which the task is stopped. |
| `retries` | `0` | Further attempts after a failure whose exception is an instance of `retry_on`. |
| `retry_on` | `Exception` | Exception classes worth a retry. A failed command counts as `subprocess.CalledProcessError`, a time limit as `TimeoutError`. |
| `cache` | `False` | Reuse the result across runs, given an Executor `store`. See [Caching](#caching-results-across-runs). |
| `name` | `None` | A name for the task in messages and errors. |

Tasks see their resources as `CORES`, `MEMORY`, `GPUS`, and
`CUDA_VISIBLE_DEVICES`. A task that needs more than any connected worker has
waits for a larger worker, with one notice.

## Running tasks

Tasks run in the background once submitted. `task.result(timeout=None)` waits
for one, and `task.done()` tells whether it finished. Decide what to run next
with plain Python around results.

```python
import ndcctools.taskvine.vine_graph as vg

def improve(guess, target):
    return (guess + target / guess) / 2

guess_task = vg.Function(float, 1)
with vg.Executor(local=True) as ex:
    while abs(ex.run(guess_task).result() ** 2 - 2) > 1e-9:
        guess_task = vg.Function(improve, guess_task, 2)
    assert round(guess_task.result(), 6) == 1.414214
```

`ex.as_completed(tasks, timeout=None)` yields tasks as they finish, as
`concurrent.futures.as_completed()` does, and `task.cancel()` stops a task and
the tasks that read it.

```python
import ndcctools.taskvine.vine_graph as vg

trial_tasks = [vg.Function(lambda s: s * 37 % 101, seed) for seed in range(50)]
with vg.Executor(local=True) as ex:
    for task in ex.as_completed(ex.submit(trial_tasks)):
        if task.result() > 90:
            break
    for other in trial_tasks:
        other.cancel()
```

Read results before the Executor closes. Leaving the `with` block waits for
the tasks still running.

### Expanding

When the tasks to run depend on a result, such as one task per file of a
listing, use `vg.Expand(builder, *args)`. Once its inputs finished,
`builder(*values)` runs once in the program and returns a list of tasks. The
Expand's result is the list of their results, so other tasks can read it.

```python
import ndcctools.taskvine.vine_graph as vg

def list_files(dataset):
    return [f"{dataset}/file{index}.root" for index in range(4)]

def process(path):
    return len(path)

def per_file(paths):
    return [vg.Function(process, path) for path in paths]

files_task = vg.Function(list_files, "/data/run7")
counts_task = vg.Expand(per_file, files_task).set(cores=2)
total_task = vg.Function(sum, counts_task)

with vg.Executor(local=True) as ex:
    assert ex.run(total_task).result() == 4 * len("/data/run7/file0.root")
```

The tasks a builder returns take the Expand's settings, and may be Expands
themselves.

## Failures

A task fails when its function raises, its command exits non-zero, or it runs
out of retries. The tasks that read it fail too, and the others keep running.
A lost worker is not a failure, and its tasks run again elsewhere.

`ex.run()` and `task.result()` raise a `vg.TaskError`.

```python
import ndcctools.taskvine.vine_graph as vg

number_tasks = [vg.Function(int, text) for text in ("1", "2.", "3")]
total_task = vg.Function(sum, number_tasks)

with vg.Executor(local=True) as ex:
    try:
        ex.run(total_task)
    except vg.TaskError as error:
        assert error.source is number_tasks[1]
        assert error.reason == "function"
        assert isinstance(error.exception, ValueError)
```

| Field | Meaning |
| --- | --- |
| `task` | The task that has no result. |
| `source` | The task whose own failure caused it, possibly `task` itself. |
| `reason` | Such as `"function"`, `"command"`, `"timeout"`, `"resources"`, `"input"`, or `"expand"`. |
| `exception` | The exception the function raised, or `None`. |
| `exit_code` | The exit status of the command, or `None`. |

A failed task stays failed. Fix the cause and make a new task.

## Running on workers

Without `local=True` the Executor starts a TaskVine manager, and workers run
the tasks.

```python
with vg.Executor(port=9123) as ex:
    print(ex.run(total_task).result())
```

Start workers with `vine_worker localhost 9123`, or many with
`vine_submit_workers` or `vine_factory` (see the [TaskVine manual](index.md)).

- **Workers need the program's Python environment**, meaning the same Python,
  `cloudpickle`, `ndcctools`, and the packages tasks import. Start them from
  the same environment, or give the factory a Poncho package of it
  (`poncho_package_create "$CONDA_PREFIX" env.tar.gz`, then
  `vine_factory --poncho-env env.tar.gz`).
- **Local modules travel explicitly.** A function defined in the main program
  travels with its task. One from a local file such as `helpers.py` needs
  `library_files={"helpers.py": "helpers.py"}` and `library_modules=["helpers"]`.
- **Large shared objects load once per worker.** `shared={"model": model}`
  loads them when the worker starts, and `vg.Shared("model")` as an argument
  stands for one.
- **Ports.** Workers also fetch data from a second port, so behind a firewall
  give a range such as `port=(9123, 9124)`. If the manager cannot reach the
  workers, start them with `--connect-back`.

On workers, `print` in a Python task is not shown. Return what you want to see.
`ex.progress()` counts the tasks, and `ex.capacity()` the connected workers.

## More

### Caching results across runs

With `vg.Executor(store="~/.cache/my-project")`, a task set to `cache=True`
whose function, arguments, and input files are unchanged takes its earlier
result instead of running. Delete the directory to forget the results.

### How long results are kept

Results that were read, and those of tasks nothing reads, are kept while the
program holds their tasks. Other intermediate results are freed once the tasks
that read them finished, and computed again if asked for later.

### Notebooks

Create the Executor in one cell, use it in later ones, and call `ex.close()` in
the last. Interrupting a cell stops the wait but not the tasks.

### Dask graphs

`from_dask()` turns a Dask graph into a dictionary of tasks by Dask key.

```python
import ndcctools.taskvine.vine_graph as vg
from ndcctools.taskvine.vine_graph.adaptors import from_dask

def multiply(left, right):
    return left * right

key_tasks = from_dask({"incremented": (abs, -2), "answer": (multiply, "incremented", 10)})
with vg.Executor(local=True) as ex:
    assert ex.run(key_tasks["answer"]).result() == 20
```

## Executor arguments

| Argument | Default | Meaning |
| --- | --- | --- |
| `port` | `9123` | Port, or `(low, high)` range, workers connect to. `0` picks one, reported by `ex.port`. |
| `name` | `None` | Name workers can find the manager by (`vine_worker -M name`). |
| `local` | `False` | Run tasks in this process, without workers. |
| `progress_bar` | `True` | Show progress while `run()` waits in a terminal. |
| `library_modules` | `()` | Modules workers import once. |
| `library_files` | `None` | Local files sent to workers, as `{local_path: name}`. |
| `shared` | `None` | Objects loaded once per worker, as `{name: object}`. |
| `store` | `None` | Directory for results of `cache=True` tasks. |
| `output_dir` | `./outputs` | Where `task.file(name).path` places files. |
| `params` | `None` | TaskVine tuning, such as `{"max-retries": 5}`. |
| `ssl`, `staging_path`, `run_info_path`, `run_info_template` | | TLS key and certificate, staging directory, and log location of the manager. |
