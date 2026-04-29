# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Python wrapper for the C executor graph API."""

from . import graph_capi


class ExecutorGraph:
    """Thin wrapper around the SWIG bindings."""

    def __init__(self, c_taskvine):
        """Create the backing C executor graph."""
        self._c_graph = graph_capi.graph_create(c_taskvine)
        self._key_to_id = {}
        self._id_to_key = {}

    def tune(self, name, value):
        """Forward a tuning parameter to the C graph."""
        graph_capi.graph_tune(self._c_graph, name, value)

    def add_node(self, key, is_target=None):
        """Create a C node and record its Python key."""
        node_id = graph_capi.graph_add_node(self._c_graph)
        self._key_to_id[key] = node_id
        self._id_to_key[node_id] = key
        if is_target is not None and bool(is_target):
            graph_capi.graph_set_target(self._c_graph, node_id)
        return node_id

    def set_target(self, key):
        """Mark a node as a target."""
        node_id = self._key_to_id.get(key)
        if node_id is None:
            raise KeyError(f"Key not found: {key}")
        graph_capi.graph_set_target(self._c_graph, node_id)

    def add_dependency(self, parent_key, child_key):
        """Add an edge between two existing nodes."""
        if parent_key not in self._key_to_id or child_key not in self._key_to_id:
            raise KeyError("parent_key or child_key missing in mapping; call add_node() first")
        graph_capi.graph_add_dependency(
            self._c_graph, self._key_to_id[parent_key], self._key_to_id[child_key]
        )

    def compute_topology_metrics(self):
        """Finalize the C graph and compute topology metrics."""
        graph_capi.graph_finalize(self._c_graph)

    def get_node_outfile_remote_name(self, key):
        """Return the output path assigned by the C graph."""
        if key not in self._key_to_id:
            raise KeyError(f"Key not found: {key}")
        return graph_capi.graph_get_node_outfile_remote_name(
            self._c_graph, self._key_to_id[key]
        )

    def get_task_runner_library_name(self):
        """Return the generated task runner library name."""
        return graph_capi.graph_get_task_runner_library_name(self._c_graph)

    def set_task_runner_function(self, task_runner_function):
        """Set the worker-side task runner entry point."""
        graph_capi.graph_set_task_runner_function_name(
            self._c_graph, task_runner_function.__name__
        )

    def add_task_input(self, task_key, filename):
        """Add an input file to a task."""
        task_id = self._key_to_id.get(task_key)
        if task_id is None:
            raise KeyError(f"Task key not found: {task_key}")
        graph_capi.graph_add_task_input(self._c_graph, task_id, filename)

    def add_task_output(self, task_key, filename):
        """Add an output file to a task."""
        task_id = self._key_to_id.get(task_key)
        if task_id is None:
            raise KeyError(f"Task key not found: {task_key}")
        graph_capi.graph_add_task_output(self._c_graph, task_id, filename)

    def execute(self):
        """Execute the graph."""
        graph_capi.graph_execute(self._c_graph)

    def get_makespan_us(self):
        """Return the graph makespan in microseconds."""
        return graph_capi.graph_get_makespan_us(self._c_graph)

    def get_total_recovery_tasks(self):
        """Return the total number of submitted recovery tasks."""
        return graph_capi.graph_get_total_recovery_tasks(self._c_graph)

    def get_completed_recovery_tasks(self):
        """Return the number of completed recovery tasks."""
        return graph_capi.graph_get_completed_recovery_tasks(self._c_graph)

    def delete(self):
        """Delete the backing C graph."""
        graph_capi.graph_delete(self._c_graph)
