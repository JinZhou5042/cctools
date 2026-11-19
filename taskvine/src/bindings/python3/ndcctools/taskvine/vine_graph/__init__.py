# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Build task graphs and run them on TaskVine.

A Graph holds tasks, Python calls or shell commands, and the data they pass to each other. Three rules:
vg.Function(func, *args, **kwargs) and vg.Command(template, **fields) hold what to run and its arguments, and
vg.Expand(builder, *args, **kwargs) decides more of them from results;
g.add(spec, **settings) is the only way to make a task, with settings such as cores, memory, gpus, and outputs; and an
Executor runs Graphs, where run(g) runs and waits, returning the results of its sinks, the tasks that no task reads,
and submit(g) starts. A live Task's result() waits for it and returns its result. A task that reads a Task starts once
its input exists, and as_completed() yields whichever of several tasks finishes first. Results are read before the
Executor closes:

    import ndcctools.taskvine.vine_graph as vg

    g = vg.Graph()
    total = g.add(vg.Function(sum, [g.add(vg.Function(abs, -1)), g.add(vg.Function(abs, -2))]))

    with vg.Executor(port=9123) as executor:
        print(executor.run(g)[total])
        more = [g.add(vg.Function(abs, -value)) for value in range(3, 6)]
        again = g.add(vg.Function(sum, [total, *more]))
        executor.submit(g)
        print(again.result())
"""

from .graph import URL, CancelledError, Command, Expand, File, Function, Graph, Shared, Task, TaskError, TaskFile

__all__ = ["Graph", "Task", "TaskFile", "File", "URL", "Command", "Function", "Expand", "Shared", "Executor",
           "TaskError", "CancelledError"]


def __dir__():
    # Executor is loaded on first use, so it is listed here before that.
    return sorted(set(globals()) | set(__all__))


def __getattr__(name):
    # The Executor loads the TaskVine bindings, which a Worker's library that only reads task formats does not need.
    if name == "Executor":
        from .executor import Executor

        return Executor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
