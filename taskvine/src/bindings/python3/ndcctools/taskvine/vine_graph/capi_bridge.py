# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Bridge from Python VineGraph objects to the C vine_graph API."""

import ctypes
import importlib
import os
import sys

# SWIG-generated vine_graph_capi.py imports "cvine"; wire that top-level name
# to the real TaskVine Python module before importing the generated bindings.
# Also expose _cvine's C symbols globally so _vine_graph_capi reuses the same
# TaskVine/dttools runtime instead of loading a second copy of those globals.
cvine = importlib.import_module("ndcctools.taskvine.cvine")
_rtld_now = getattr(os, "RTLD_NOW", 0)
_rtld_global = getattr(os, "RTLD_GLOBAL", ctypes.RTLD_GLOBAL)
ctypes.CDLL(cvine._cvine.__file__, mode=_rtld_now | _rtld_global)
sys.modules.setdefault("cvine", cvine)

from . import vine_graph_capi  # noqa: E402


class VineGraphCapiBridge:
    """Own opaque C handles and Python-key translation; access C state only through API functions."""

    def __init__(self, c_taskvine):
        """Create the backing C vine_graph objects."""
        self._c_graph = vine_graph_capi.vine_graph_executor_create_graph(c_taskvine)
        self._c_executor = vine_graph_capi.vine_graph_executor_create(c_taskvine, self._c_graph)
        self._workflow_key_to_scheduler_key = {}

    def tune(self, name, value):
        """Forward a tuning parameter to the C vine_graph executor."""
        if vine_graph_capi.vine_graph_executor_tune(self._c_executor, name, value) != 0:
            raise RuntimeError(f"Failed to tune executor parameter {name!r}={value!r}")

    def add_node(self, workflow_key):
        """Create a C node and record its workflow key."""
        node_id = vine_graph_capi.vine_graph_executor_add_node(self._c_executor)
        self._workflow_key_to_scheduler_key[workflow_key] = node_id
        return node_id

    def get_node_id(self, workflow_key):
        """Return the C node ID owned by this bridge."""
        try:
            return self._workflow_key_to_scheduler_key[workflow_key]
        except KeyError:
            raise KeyError(f"Workflow key not found: {workflow_key}") from None

    def set_target(self, workflow_key):
        """Mark a node as a target."""
        node_id = self.get_node_id(workflow_key)
        vine_graph_capi.vine_graph_set_target(self._c_graph, node_id)

    def add_dependency(self, parent_workflow_key, child_workflow_key):
        """Add an edge between two existing nodes."""
        wk2sk = self._workflow_key_to_scheduler_key
        if parent_workflow_key not in wk2sk or child_workflow_key not in wk2sk:
            raise KeyError("parent or child workflow_key missing in mapping; call add_node() first")
        vine_graph_capi.vine_graph_add_dependency(
            self._c_graph, wk2sk[parent_workflow_key], wk2sk[child_workflow_key]
        )

    def compute_topology_metrics(self):
        """Finalize the C graph and compute topology metrics."""
        vine_graph_capi.vine_graph_executor_finalize(self._c_executor)

    def get_node_outfile_remote_name(self, workflow_key):
        """Return the output path assigned by the C graph."""
        return vine_graph_capi.vine_graph_get_node_outfile_remote_name(
            self._c_graph, self.get_node_id(workflow_key)
        )

    def get_task_runner_library_name(self):
        """Return the generated task runner library name."""
        return vine_graph_capi.vine_graph_get_task_runner_library_name(self._c_graph)

    def set_task_runner_function(self, task_runner_function):
        """Set the worker-side task runner entry point."""
        vine_graph_capi.vine_graph_set_task_runner_function_name(
            self._c_graph, task_runner_function.__name__
        )

    def declare_input_file(self, file_id, source_path, export=False):
        """Declare one frontend file, optionally publishing it for asynchronous Worker pulls."""
        if vine_graph_capi.vine_graph_executor_declare_input_file(
            self._c_executor, file_id, source_path, export
        ) != 0:
            raise RuntimeError(f"failed to declare input file {file_id}: {source_path}")

    def add_task_input_file(self, workflow_key, file_id, task_path):
        """Mount a declared file into a task."""
        task_id = self.get_node_id(workflow_key)
        if vine_graph_capi.vine_graph_executor_add_task_input_file(
            self._c_executor, task_id, file_id, task_path
        ) != 0:
            raise RuntimeError(f"failed to mount input file {file_id} on task {workflow_key}")

    def add_task_output_file(self, workflow_key, file_id, task_path, is_target=False):
        """Declare and mount a task-produced FileHandle."""
        task_id = self.get_node_id(workflow_key)
        if vine_graph_capi.vine_graph_executor_add_task_output_file(
            self._c_executor, task_id, file_id, task_path, int(bool(is_target))
        ) != 0:
            raise RuntimeError(f"failed to declare output file {file_id} on task {workflow_key}")

    def get_file_target_path(self, file_id):
        """Return the manager-side path of a retrieved output file."""
        path = vine_graph_capi.vine_graph_executor_get_file_target_path(self._c_executor, file_id)
        if not path:
            raise RuntimeError(f"file {file_id} has no manager-side target path")
        return path

    def execute(self):
        """Execute the graph."""
        vine_graph_capi.vine_graph_executor_execute(self._c_executor)

    def get_makespan_us(self):
        """Return the graph makespan in microseconds."""
        return vine_graph_capi.vine_graph_executor_get_makespan_us(self._c_executor)

    def get_total_recovery_tasks(self):
        """Return the total number of submitted recovery tasks."""
        return vine_graph_capi.vine_graph_executor_get_total_recovery_tasks(self._c_executor)

    def get_completed_recovery_tasks(self):
        """Return the number of completed recovery tasks."""
        return vine_graph_capi.vine_graph_executor_get_completed_recovery_tasks(self._c_executor)

    def delete(self):
        """Delete the backing C graph."""
        vine_graph_capi.vine_graph_executor_delete(self._c_executor)
        self._c_executor = None
        vine_graph_capi.vine_graph_delete(self._c_graph)
        self._c_graph = None
