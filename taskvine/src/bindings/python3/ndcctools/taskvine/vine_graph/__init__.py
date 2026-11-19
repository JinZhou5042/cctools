# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Build task graphs and run them on TaskVine.

A Graph holds tasks, Python calls or shell commands, and the data they pass to each other. vg.Function(func, *args,
**kwargs) and vg.Command(template, **fields) hold what a task runs, and g.add(spec, cores=k, ...) its settings, such as
the cores, memory, and gpus it uses. g.add() and submit() take a vg.Function or a vg.Command, never a bare function.
An Executor is where tasks run: submit(vg.Function(func, *args)) starts one task at once and returns its Task, submit(graph) makes the
tasks of a Graph live, and run(graph) also waits for them and returns the results of its sinks, the tasks that no task reads. From then on a
Task behaves alike whatever made it: result() waits for it and returns its result. A task that reads a Task starts once
its input exists, and as_completed() yields whichever of several tasks finishes first. Results are read before the
Executor closes:

    import ndcctools.taskvine.vine_graph as vg

    g = vg.Graph()
    total = g.add(vg.Function(sum, [g.add(vg.Function(abs, -1)), g.add(vg.Function(abs, -2))]))

    with vg.Executor(port=9123) as executor:
        print(executor.run(g)[total])
        more = [executor.submit(vg.Function(abs, -value)) for value in range(3, 6)]
        print(executor.submit(vg.Function(sum, [total, *more])).result())
"""

from .graph import URL, CancelledError, Command, File, Function, Graph, Shared, Task, TaskError, TaskFile

__all__ = ["Graph", "Task", "TaskFile", "File", "URL", "Command", "Function", "Shared", "Executor", "TaskError",
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
