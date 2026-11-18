# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Build task graphs and run them on TaskVine.

A Graph holds tasks, Python calls or shell commands, and rules that add tasks as others complete. An Executor is where
tasks run: run() executes a Graph and returns the results of its sinks, the tasks that no task reads, and submit()
starts one task at once and returns its Task, whose result() waits for it. task.result() returns the result of any
other task, and results are read before the Executor closes:

    import ndcctools.taskvine.vine_graph as vg

    g = vg.Graph()
    total = g.add(sum, [g.add(abs, -1), g.add(abs, -2)])

    @g.on(abs)
    def report(task):
        print(task, task.result())

    with vg.Executor(port=9123) as executor:
        print(executor.run(g)[total])
        more = executor.submit(abs, -3)
        if more.result() > 2:
            print(executor.submit(sum, [total, more]).result())
"""

from .graph import Command, File, Graph, Shared, Task, TaskError, TaskFile

__all__ = ["Graph", "Task", "TaskFile", "File", "Command", "Shared", "Executor", "TaskError"]


def __dir__():
    # Executor is loaded on first use, so it is listed here before that.
    return sorted(set(globals()) | set(__all__))


def __getattr__(name):
    # The Executor loads the TaskVine bindings, which a Worker's library that only reads task formats does not need.
    if name == "Executor":
        from .executor import Executor

        return Executor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
