# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Build task graphs and run them on TaskVine.

A Graph describes tasks and the data they pass to each other, and can be pickled and shared. An Executor runs tasks
on TaskVine Workers and returns a Future for each submitted task:

    import ndcctools.taskvine.vine_graph as vg

    graph = vg.Graph()
    total = graph.add(sum, [graph.add(abs, -1), graph.add(abs, -2)])
    with vg.Executor(port=9123) as executor:
        print(executor.run(graph, [total])[total])
"""

from .graph import File, Graph, Node, NodeFile
from .executor import Executor, Future, Progress, TaskError

__all__ = ["Graph", "Node", "NodeFile", "File", "Executor", "Future", "Progress", "TaskError"]
