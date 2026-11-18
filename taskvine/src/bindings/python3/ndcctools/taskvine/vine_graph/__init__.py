# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Build task graphs and run them on TaskVine.

A Graph holds tasks, Python calls or shell commands, and rules that add tasks as others complete. run() executes it
and returns the results of the tasks that no task reads:

    import ndcctools.taskvine.vine_graph as vg

    g = vg.Graph()
    total = g.add(sum, [g.add(abs, -1), g.add(abs, -2)])

    @g.on(abs)
    def report(task):
        print(task, task.result)

    print(g.run(port=9123)[total])
"""

from .graph import Command, File, Graph, Hoisted, Task, TaskError, TaskFile

__all__ = ["Graph", "Task", "TaskFile", "File", "Command", "Hoisted", "TaskError"]
