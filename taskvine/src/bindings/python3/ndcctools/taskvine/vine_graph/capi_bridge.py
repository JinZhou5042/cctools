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
    """Own the opaque C executor handle and the Python-key translation. Every C failure becomes an exception."""

    def __init__(self, c_taskvine, task_runner_library_name, task_runner_function_name):
        """Create the backing C executor, whose nodes run one function of one library."""
        self._c_executor = vine_graph_capi.vine_graph_executor_create(
            c_taskvine, task_runner_library_name, task_runner_function_name
        )
        if not self._c_executor:
            raise MemoryError("Could not create graph executor")
        self._workflow_key_to_scheduler_key = {}

    def tune(self, name, value):
        """Forward a tuning parameter to the C vine_graph executor."""
        if vine_graph_capi.vine_graph_executor_tune(self._c_executor, name, value) != 0:
            raise RuntimeError(f"Failed to tune executor parameter {name!r}={value!r}")

    def add_node(self, workflow_key):
        """Create a C node and record its workflow key."""
        node_id = vine_graph_capi.vine_graph_executor_add_node(self._c_executor)
        if not node_id:
            raise MemoryError(f"Could not add graph node for {workflow_key!r}")
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
        if vine_graph_capi.vine_graph_executor_set_target(self._c_executor, self.get_node_id(workflow_key)) != 0:
            raise RuntimeError(f"failed to mark target {workflow_key!r}")

    def add_dependency(self, parent_workflow_key, child_workflow_key):
        """Add an edge between two existing nodes."""
        parent, child = self.get_node_id(parent_workflow_key), self.get_node_id(child_workflow_key)
        if vine_graph_capi.vine_graph_executor_add_dependency(self._c_executor, parent, child) != 0:
            raise RuntimeError(f"failed to add dependency {parent_workflow_key!r} -> {child_workflow_key!r}")

    def finalize(self):
        """Validate the C graph and declare result outputs after the graph is complete."""
        if vine_graph_capi.vine_graph_executor_finalize(self._c_executor) != 0:
            raise RuntimeError("failed to finalize the graph: it has a cycle or an output could not be declared")

    def get_node_outfile_remote_name(self, workflow_key):
        """Return the output path assigned by the C graph."""
        return vine_graph_capi.vine_graph_executor_get_node_outfile_remote_name(
            self._c_executor, self.get_node_id(workflow_key)
        )

    def declare_input_file(self, file_id, source_path, vault=False):
        """Declare one frontend file, optionally placing it in the vault for asynchronous Worker pulls."""
        if vine_graph_capi.vine_graph_executor_declare_input_file(
            self._c_executor, file_id, source_path, vault
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
        """Execute the graph. A run that cannot continue, such as one with a lost edata file, raises."""
        if vine_graph_capi.vine_graph_executor_execute(self._c_executor) != 0:
            raise RuntimeError("vine_graph execution stopped; see the Manager debug log for the cause")

    def get_makespan_us(self):
        """Return the graph makespan in microseconds."""
        return vine_graph_capi.vine_graph_executor_get_makespan_us(self._c_executor)

    def get_completed_recovery_tasks(self):
        """Return the number of completed recovery tasks."""
        return vine_graph_capi.vine_graph_executor_get_completed_recovery_tasks(self._c_executor)

    def delete(self):
        """Delete the backing C executor and its graph."""
        if self._c_executor is not None:
            vine_graph_capi.vine_graph_executor_delete(self._c_executor)
            self._c_executor = None
