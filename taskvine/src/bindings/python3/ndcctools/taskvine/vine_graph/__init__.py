# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Build task graphs and run them on TaskVine.

A task is a Python call or a shell command, and the tasks among its arguments are the tasks it reads. Three rules:
vg.Function(func, *args, **kwargs), vg.Command(template, **fields), and vg.Expand(builder, *args, **kwargs), which
decides more tasks from results, make a task; task.set(**settings) gives it settings such as cores, memory, gpus, and
outputs; and an Executor runs tasks with every task they read, where submit(tasks) starts them and run(tasks) also
waits for them, and both return the tasks. A Task is a future: give it to another task and that task receives its
result, call result() on it and your program receives its result. A task that reads a Task starts once its input
exists, and as_completed() yields whichever of several tasks finishes first. Results are read before the Executor
closes:

    import ndcctools.taskvine.vine_graph as vg

    total_task = vg.Function(sum, [vg.Function(abs, -1), vg.Function(abs, -2)])

    with vg.Executor(port=9123) as executor:
        print(executor.run(total_task).result())
        more_tasks = [vg.Function(abs, -value) for value in range(3, 6)]
        again_task = executor.submit(vg.Function(sum, [total_task, *more_tasks]))
        print(again_task.result())
"""

from .graph import URL, CancelledError, Command, Expand, File, Function, Shared, Task, TaskError, TaskFile

__all__ = ["Task", "TaskFile", "File", "URL", "Command", "Function", "Expand", "Shared", "Executor", "TaskError",
           "CancelledError"]


def __dir__():
    # Executor is loaded on first use, so it is listed here before that.
    return sorted(set(globals()) | set(__all__))


def __getattr__(name):
    # The Executor loads the TaskVine bindings, which a Worker's library that only reads task formats does not need.
    if name == "Executor":
        from .executor import Executor

        return Executor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
