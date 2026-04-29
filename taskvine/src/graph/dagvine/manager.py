# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from ndcctools.taskvine import cvine
from ndcctools.taskvine.manager import Manager

from ndcctools.taskvine.dagvine.adaptor import Adaptor
from ndcctools.taskvine.dagvine.proxy_library import ProxyLibrary
from ndcctools.taskvine.dagvine.proxy_functions import compute_single_key, compute_task
from ndcctools.taskvine.dagvine.workflow import Workflow, TaskOutputRef, TaskOutputWrapper
from ndcctools.taskvine.dagvine.executor.graph import ExecutorGraph

from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeRemainingColumn

import cloudpickle
import os
import random
import signal
import time


def context_loader_func(graph_pkl):
    graph = cloudpickle.loads(graph_pkl)

    return {
        "graph": graph,
    }


def delete_all_files(root_dir):
    """Remove files under the run-info template directory."""
    if not os.path.exists(root_dir):
        return
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            try:
                os.remove(file_path)
            except FileNotFoundError:
                print(f"Failed to delete file {file_path}")


def color_text(text, color_code):
    """Return text wrapped in an ANSI color code."""
    return f"\033[{color_code}m{text}\033[0m"


class GraphParams:
    def __init__(self):
        """Store DAGVine tuning parameters."""
        # Passed to Manager.tune() before execution.
        self.vine_manager_tuning_params = {
            "worker-source-max-transfers": 100,
            "max-retrievals": -1,
            "prefer-dispatch": 1,
            "transient-error-interval": 1,
            "attempt-schedule-depth": 10000,
            "temp-replica-count": 1,
            "enforce-worker-eviction-interval": -1,
            "shift-disk-load": 0,
            "clean-redundant-replicas": 0,
        }
        # Passed through ExecutorGraph to the C executor graph.
        self.executor_tuning_params = {
            "failure-injection-step-percent": -1,
            "task-priority-mode": "largest-input-first",
            "prune-depth": 1,
            "output-dir": "./outputs",
            "checkpoint-dir": "./checkpoints",
            "checkpoint-fraction": 0,
            "progress-bar-update-interval-sec": 0.1,
            "time-metrics-filename": 0,
            "enable-debug-log": 1,
            "max-retry-attempts": 15,
            "print-graph-details": 0,
        }
        # Python-only knobs.
        self.other_params = {
            "schedule": "worst",
            "libcores": 16,
            "failure-injection-step-percent": -1,
            "extra-task-output-size-mb": [0, 0],
            "extra-task-sleep-time": [0, 0],
            # 1 = run Workflow in-process (topological order), no workers / no proxy library; stdout stays on frontend.
            "local-execute": 0,
        }

    def update_param(self, param_name, new_value):
        """Update one parameter."""
        if param_name in self.vine_manager_tuning_params:
            self.vine_manager_tuning_params[param_name] = new_value
        elif param_name in self.executor_tuning_params:
            self.executor_tuning_params[param_name] = new_value
        elif param_name in self.other_params:
            self.other_params[param_name] = new_value
        else:
            self.vine_manager_tuning_params[param_name] = new_value

    def get_value_of(self, param_name):
        """Return the current value for a parameter."""
        if param_name in self.vine_manager_tuning_params:
            return self.vine_manager_tuning_params[param_name]
        elif param_name in self.executor_tuning_params:
            return self.executor_tuning_params[param_name]
        elif param_name in self.other_params:
            return self.other_params[param_name]
        else:
            raise ValueError(f"Invalid param name: {param_name}")


class DAGVine(Manager):
    def __init__(self,
                 *args,
                 **kwargs):
        """Create a DAGVine manager."""

        signal.signal(signal.SIGINT, self._on_sigint)

        self.params = GraphParams()

        run_info_path = kwargs.get("run_info_path", None)
        run_info_template = kwargs.get("run_info_template", None)

        self.run_info_template_path = os.path.join(run_info_path, run_info_template)
        if self.run_info_template_path:
            delete_all_files(self.run_info_template_path)

        # Manager lifetime is tied to this object.
        super().__init__(*args, **kwargs)
        self.runtime_directory = cvine.vine_get_runtime_directory(self._taskvine)

        print(f"=== Manager name: {color_text(self.name, 92)}")
        print(f"=== Manager port: {color_text(self.port, 92)}")
        print(f"=== Runtime directory: {color_text(self.runtime_directory, 92)}")
        self._sigint_received = False

    def param(self, param_name):
        """Return a parameter value."""
        return self.params.get_value_of(param_name)

    def update_params(self, new_params):
        """Apply a batch of parameter overrides."""
        assert isinstance(new_params, dict), "new_params must be a dict"
        for k, new_v in new_params.items():
            self.params.update_param(k, new_v)

    def tune_manager(self):
        """Apply manager-side tuning."""
        for k, v in self.params.vine_manager_tuning_params.items():
            try:
                self.tune(k, v)
            except Exception:
                raise ValueError(f"Unrecognized parameter: {k}")

    def tune_executor(self, executor):
        """Apply executor-graph tuning."""
        for k, v in self.params.executor_tuning_params.items():
            executor.tune(k, str(v))

    def _rep_key(self, k, r):
        return k if r == 0 else ("__rep", r, k)

    def _replicate_graph(self, task_dict, target_keys, repeats):
        if repeats <= 1:
            return task_dict, target_keys
        if isinstance(task_dict, Workflow):
            old_workflow = task_dict
            new_workflow = Workflow()
            new_workflow.callables = list(old_workflow.callables)
            new_workflow._callable_index = dict(old_workflow._callable_index)
            for r in range(repeats):
                def rewriter(ref):
                    return TaskOutputRef(self._rep_key(ref.task_key, r), ref.path)

                def _rewrite(obj):
                    return old_workflow._visit_task_output_refs(obj, rewriter, rewrite=True)

                for k, (func_id, args, kwargs) in old_workflow.task_dict.items():
                    new_workflow.add_task(self._rep_key(k, r), old_workflow.callables[func_id], *_rewrite(args), **_rewrite(kwargs))
            new_workflow.finalize()
            executor_targets = list(target_keys)
            for r in range(1, repeats):
                executor_targets.extend(self._rep_key(k, r) for k in target_keys if k in old_workflow.task_dict)
            return new_workflow, executor_targets
        else:
            temp_workflow = Workflow()
            expanded = {}
            for r in range(repeats):
                def rewriter(ref):
                    return TaskOutputRef(self._rep_key(ref.task_key, r), ref.path)

                def _rewrite(obj):
                    return temp_workflow._visit_task_output_refs(obj, rewriter, rewrite=True)

                for k, v in task_dict.items():
                    func, args, kwargs = v
                    expanded[self._rep_key(k, r)] = (func, _rewrite(args), _rewrite(kwargs))
            executor_targets = list(target_keys)
            for r in range(1, repeats):
                executor_targets.extend(self._rep_key(k, r) for k in target_keys if k in task_dict)
            return expanded, executor_targets

    def build_workflow(self, task_dict):
        if isinstance(task_dict, Workflow):
            workflow = task_dict
        else:
            workflow = Workflow()

            for k, v in task_dict.items():
                func, args, kwargs = v
                assert callable(func), f"Task {k} does not have a callable"
                workflow.add_task(k, func, *args, **kwargs)

        workflow.finalize()

        return workflow

    def build_executor(self, py_graph, target_keys):
        """Build the C executor graph from the Python graph."""
        assert py_graph is not None, "Python graph must be built before building the ExecutorGraph"

        executor = ExecutorGraph(self._taskvine)

        executor.set_proxy_function(compute_single_key)

        self.tune_manager()
        self.tune_executor(executor)

        topo_order = py_graph.get_topological_order()

        for k in topo_order:
            node_id = executor.add_node(k)
            py_graph.pykey2cid[k] = node_id
            py_graph.cid2pykey[node_id] = k
            for pk in py_graph.parents_of[k]:
                executor.add_dependency(pk, k)

        for k in target_keys:
            executor.set_target(k)

        executor.compute_topology_metrics()

        return executor

    def build_graphs(self, task_dict, target_keys):
        """Build the Python graph and its C mirror."""
        py_graph = self.build_workflow(task_dict)

        # Ignore requested targets that are not in the graph.
        missing_keys = [k for k in target_keys if k not in py_graph.task_dict]
        if missing_keys:
            print(f"=== Warning: the following target keys are not in the graph: {','.join(map(str, missing_keys))}")
        target_keys = list(set(target_keys) - set(missing_keys))

        executor = self.build_executor(py_graph, target_keys)

        # Save output locations back into the Python graph.
        for k in py_graph.pykey2cid:
            outfile_remote_name = executor.get_node_outfile_remote_name(k)
            py_graph.outfile_remote_name[k] = outfile_remote_name

        # Declare graph-level file dependencies in the C graph.
        for filename in py_graph.producer_of:
            task_key = py_graph.producer_of[filename]
            executor.add_task_output(task_key, filename)
        for filename in py_graph.consumers_of:
            for task_key in py_graph.consumers_of[filename]:
                executor.add_task_input(task_key, filename)

        return py_graph, executor

    def create_proxy_library(self, py_graph, executor, hoisting_modules, env_files):
        """Build the TaskVine proxy library."""
        proxy_library = ProxyLibrary(self)
        proxy_library.add_hoisting_modules(hoisting_modules)
        proxy_library.add_env_files(env_files)
        proxy_library.set_context_loader(context_loader_func, context_loader_args=[cloudpickle.dumps(py_graph)])
        proxy_library.set_libcores(self.param("libcores"))
        proxy_library.set_name(executor.get_proxy_library_name())

        return proxy_library

    def _execute_workflow_local(self, py_graph):
        """Run the workflow locally in topological order."""
        out_dir = os.path.abspath(self.param("output-dir"))
        os.makedirs(out_dir, exist_ok=True)
        prev_cwd = os.getcwd()
        os.chdir(out_dir)
        t0 = time.time()
        try:
            order = py_graph.get_topological_order()
            interval = float(self.param("progress-bar-update-interval-sec"))
            if interval <= 0:
                interval = 0.1
            refresh_per_second = min(30.0, max(1.0, 1.0 / interval))

            n = len(order)
            if n == 0:
                return time.time() - t0

            with Progress(
                TextColumn("[bold]Executing Tasks"),
                TextColumn("•"),
                TextColumn("[cyan]User"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeRemainingColumn(),
                refresh_per_second=refresh_per_second,
                transient=False,
            ) as progress:
                bar_id = progress.add_task("User", total=n)
                for k in order:
                    out = compute_task(py_graph, py_graph.task_dict[k])
                    py_graph.save_task_output(k, out)
                    progress.advance(bar_id)
        finally:
            os.chdir(prev_cwd)
        return time.time() - t0

    def run(self, task_dict, target_keys=[], params={}, hoisting_modules=[], env_files={}, from_dask=False, expand_subgraphs=False, repeats=1):
        """Build the graph, run it, and return the requested results."""
        self.update_params(params)

        if from_dask:
            task_dict = Adaptor(task_dict, expand_subgraphs=expand_subgraphs).converted

        result_keys = list(target_keys)
        task_dict, target_keys = self._replicate_graph(task_dict, target_keys, repeats)

        py_graph, executor = self.build_graphs(task_dict, target_keys)
        # Optional synthetic output size / sleep for testing.
        for k in py_graph.task_dict:
            py_graph.extra_task_output_size_mb[k] = random.uniform(*self.param("extra-task-output-size-mb"))
            py_graph.extra_task_sleep_time[k] = random.uniform(*self.param("extra-task-sleep-time"))

        local_execute = bool(self.param("local-execute"))
        proxy_library = None

        try:
            if local_execute:
                print("=== local-execute: running Workflow in process (no workers)", flush=True)
                makespan_s = self._execute_workflow_local(py_graph)
                completed_recovery_tasks = 0
            else:
                proxy_library = self.create_proxy_library(py_graph, executor, hoisting_modules, env_files)
                proxy_library.install()
                executor.execute()
                makespan_s = round(executor.get_makespan_us() / 1e6, 6)
                completed_recovery_tasks = executor.get_completed_recovery_tasks()

            total_tasks_completed = len(py_graph.task_dict) + completed_recovery_tasks
            throughput_tps = round(total_tasks_completed / makespan_s, 6) if makespan_s > 0 else 0.0
            print(f"=== Makespan: {makespan_s:.6f} seconds")
            print(f"=== Total tasks completed: {total_tasks_completed}")
            print(f"=== Throughput: {throughput_tps:.6f} tasks/s")

            results = {}
            for k in result_keys:
                if k not in py_graph.task_dict:
                    continue
                outfile_path = os.path.join(self.param("output-dir"), py_graph.outfile_remote_name[k])
                results[k] = TaskOutputWrapper.load_from_path(outfile_path)
            return results
        finally:
            try:
                if proxy_library is not None:
                    proxy_library.uninstall()
            finally:
                executor.delete()

    def _on_sigint(self, signum, frame):
        self._sigint_received = True
        raise KeyboardInterrupt
