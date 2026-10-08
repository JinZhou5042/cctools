# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from ndcctools.taskvine.manager import Manager

from .adaptors import VineGraphDaskAdaptor
from .task_runner import TaskRunnerRegistration, compute_task, run_node
from .workflow import FileHandle, Workflow, TaskHandle, TaskOutputHandle
from .capi_bridge import VineGraphCapiBridge
from .run_state import WorkflowRun, file_remote_name
from .utils import color_text, remove_tree_contents

import cloudpickle
import os
import signal
import sys
import tempfile
import time


class VineGraphConfig:
    def __init__(self):
        """Store VineGraph configuration by the layer that consumes it."""
        self.manager_tuning = {
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
        self.executor_tuning = {
            "failure-injection-step-percent": -1,
            "task-priority-mode": "largest-input-first",
            "prune-depth": 1,
            "checkpoint-threshold-sec": 20.0,
            "output-dir": "./outputs",
            "progress-bar-update-interval-sec": 0.1,
            "print-graph-details": 0,
        }
        self.task_runner = {
            "libcores": 16,
        }
        self.execution = {
            "schedule": "worst",
            "extra-task-output-size-mb": [0, 0],
            "extra-task-sleep-time": [0, 0],
            # 1 = run Workflow in-process (topological order), no workers / no task runner library; stdout stays on frontend.
            "local-execute": 0,

        }

    def _sections(self):
        return (
            self.manager_tuning,
            self.executor_tuning,
            self.task_runner,
            self.execution,
        )

    def update_param(self, param_name, new_value):
        """Update one parameter."""
        if param_name in ("task-group", "chain-grouping-enabled"):
            raise ValueError("vine_graph executes one node per task; grouping is no longer supported")
        if param_name in ("checkpoint-dir", "checkpoint-fraction"):
            raise ValueError("vine_graph filesystem checkpoints are no longer supported")
        for section in self._sections():
            if param_name in section:
                section[param_name] = new_value
                return
        # Unknown parameters are assumed to be TaskVine manager tuning knobs.
        self.manager_tuning[param_name] = new_value

    def get_value_of(self, param_name):
        """Return the current value for a parameter."""
        for section in self._sections():
            if param_name in section:
                return section[param_name]
        raise ValueError(f"Invalid param name: {param_name}")


class VineGraph(Manager):
    def __init__(self,
                 *args,
                 **kwargs):
        """Create a VineGraph manager."""

        signal.signal(signal.SIGINT, self._on_sigint)

        self.params = VineGraphConfig()

        run_info_path = kwargs.get("run_info_path", None)
        run_info_template = kwargs.get("run_info_template", None)

        self.run_info_template_path = None
        if run_info_path and run_info_template:
            self.run_info_template_path = os.path.join(run_info_path, run_info_template)
        if self.run_info_template_path:
            remove_tree_contents(self.run_info_template_path)

        # Manager lifetime is tied to this object.
        super().__init__(*args, **kwargs)

        print(f"=== Manager name: {color_text(self.name, 92)}")
        print(f"=== Manager port: {color_text(self.port, 92)}")
        print(f"=== Runtime directory: {color_text(self.runtime_directory, 92)}")
        self._sigint_received = False

    def get_param(self, param_name):
        """Return a parameter value."""
        return self.params.get_value_of(param_name)

    def set_params(self, new_params):
        """Apply a batch of parameter overrides."""
        assert isinstance(new_params, dict), "new_params must be a dict"
        for k, new_v in new_params.items():
            self.params.update_param(k, new_v)

    def tune_manager(self):
        """Apply manager-side tuning."""
        for k, v in self.params.manager_tuning.items():
            try:
                self.tune(k, v)
            except Exception:
                raise ValueError(f"Unrecognized parameter: {k}")

    def tune_capi_bridge(self, bridge):
        """Apply C API bridge tuning."""
        for k, v in self.params.executor_tuning.items():
            bridge.tune(k, str(v))

    def _rep_key(self, k, r):
        return k if r == 0 else ("__rep", r, k)

    def _replicate_graph(self, task_dict, target_keys, repeats):
        if repeats <= 1:
            return task_dict, target_keys
        if isinstance(task_dict, Workflow):
            old_workflow = task_dict
            if old_workflow.input_files or old_workflow.output_files:
                raise ValueError("repeats cannot be combined with FileHandle dependencies")
            new_workflow = Workflow()
            new_workflow.callables = list(old_workflow.callables)
            new_workflow._callable_index = dict(old_workflow._callable_index)
            for r in range(repeats):
                def rewriter(ref):
                    return TaskOutputHandle(
                        self._rep_key(ref.task_id, r),
                        ref.path,
                        workflow_id=new_workflow._workflow_id,
                    )

                def _rewrite(obj):
                    return old_workflow._visit_task_output_refs(obj, rewriter, rewrite=True)

                for k, (func_id, args, kwargs) in old_workflow.task_dict.items():
                    new_args, new_kwargs = _rewrite((args, kwargs))
                    new_workflow._add_task_with_key(
                        self._rep_key(k, r),
                        old_workflow.callables[func_id],
                        *new_args,
                        **new_kwargs,
                    )
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
                    return TaskOutputHandle(self._rep_key(ref.task_id, r), ref.path)

                def _rewrite(obj):
                    return temp_workflow._visit_task_output_refs(obj, rewriter, rewrite=True)

                for k, v in task_dict.items():
                    func, args, kwargs = v
                    new_args, new_kwargs = _rewrite((args, kwargs))
                    expanded[self._rep_key(k, r)] = (func, new_args, new_kwargs)
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
                workflow._add_task_with_key(k, func, *args, **kwargs)

        workflow.finalize()

        return workflow

    def build_capi_bridge(self, py_graph, target_keys):
        """Build the C vine_graph mirror from the Python graph."""
        assert py_graph is not None, "Python graph must be built before building the VineGraphCapiBridge"

        bridge = VineGraphCapiBridge(self._taskvine)

        bridge.set_task_runner_function(run_node)

        self.tune_manager()
        self.tune_capi_bridge(bridge)

        topo_order = py_graph.get_topological_order()

        for k in topo_order:
            bridge.add_node(k)
            for pk in py_graph.parents_of.get(k, ()):
                bridge.add_dependency(pk, k)

        for k in target_keys:
            bridge.set_target(k)

        return bridge

    def build_workflow_and_capi_bridge(self, task_dict, target_keys, file_target_ids=()):
        """Build the Python graph and its C mirror."""
        py_graph = self.build_workflow(task_dict)

        # Ignore requested targets that are not in the graph.
        missing_keys = [k for k in target_keys if k not in py_graph.task_dict]
        if missing_keys:
            print(f"=== Warning: the following target keys are not in the graph: {','.join(map(str, missing_keys))}")
        target_keys = list(set(target_keys) - set(missing_keys))

        bridge = self.build_capi_bridge(py_graph, target_keys)

        # Declare each FileHandle once, then mount it on its producer/consumers.
        for file_id, source_path in py_graph.input_files.items():
            bridge.declare_input_file(file_id, source_path)
        file_target_ids = set(file_target_ids)
        for file_id, (workflow_key, task_path) in py_graph.output_files.items():
            bridge.add_task_output_file(
                workflow_key, file_id, task_path, is_target=file_id in file_target_ids
            )
        for file_id, consumers in py_graph.file_consumers.items():
            task_path = file_remote_name(py_graph, file_id)
            for workflow_key in consumers:
                bridge.add_task_input_file(workflow_key, file_id, task_path)

        bridge.compute_topology_metrics()

        return py_graph, bridge

    def build_task_runner_registration(self, bridge, hoisting_modules, env_files):
        """Build the TaskVine task runner registration."""
        task_runner_registration = TaskRunnerRegistration(self)
        task_runner_registration.add_hoisting_modules(hoisting_modules)
        task_runner_registration.add_env_files(env_files)
        task_runner_registration.set_cores(self.get_param("libcores"))
        task_runner_registration.set_name(bridge.get_task_runner_library_name())

        return task_runner_registration

    def _stage_node_edata(self, run, directory):
        """Stage independent objects and per-node manifests using the existing file declaration interface."""
        py_graph, bridge = run.workflow, run.bridge
        # Share top-level objects by identity, not content. The Workflow keeps them alive throughout staging.
        # Independent pickle streams do not preserve aliases nested across different objects or callable closures.
        # Reserve IDs after user FileHandles so all declarations can use the C graph's existing file table.
        next_file_id = max((*py_graph.input_files, *py_graph.output_files), default=0)
        object_files = {}

        def stage_object(value):
            nonlocal next_file_id
            identity = id(value)
            if identity not in object_files:
                next_file_id += 1
                path = os.path.join(directory, f"vine-graph-edata-{next_file_id}")
                with open(path, "wb") as stream:
                    cloudpickle.dump(value, stream)
                bridge.declare_input_file(next_file_id, path, export=True)
                object_files[identity] = next_file_id
            return object_files[identity]

        files_by_task = {}
        for file_id, consumers in py_graph.file_consumers.items():
            for key in consumers:
                files_by_task.setdefault(key, {})[file_id] = file_remote_name(py_graph, file_id)

        for key in py_graph.task_dict:
            function, args, kwargs = py_graph._task_edata(key)
            manifest = {
                "callable": stage_object(function),
                "args": [stage_object(value) for value in args],
                "kwargs": {name: stage_object(value) for name, value in kwargs.items()},
                "inputs": {parent: bridge.get_node_outfile_remote_name(parent) for parent in py_graph.parents_of.get(key, ())},
                "files": files_by_task.get(key, {}),
                "output": bridge.get_node_outfile_remote_name(key),
                "sleep": run.task_options[key][1],
                "output_size_mb": run.task_options[key][0],
            }
            file_ids = [manifest["callable"], *manifest["args"], *manifest["kwargs"].values()]
            for file_id in dict.fromkeys(file_ids):
                bridge.add_task_input_file(key, file_id, f"vine-graph-edata-{file_id}")

            node_id = bridge.get_node_id(key)
            remote_name = f"vine-graph-edata-node-{node_id}.pkl"
            path = os.path.join(directory, remote_name)
            with open(path, "wb") as stream:
                cloudpickle.dump(manifest, stream)
            next_file_id += 1
            bridge.declare_input_file(next_file_id, path, export=True)
            bridge.add_task_input_file(key, next_file_id, remote_name)

    def _print_local_progress(self, done, total, started_at):
        """Print a simple local-execute progress line without external dependencies."""
        bar_width = 24
        filled = int(bar_width * done / total) if total else bar_width
        bar = "#" * filled + "-" * (bar_width - filled)
        percent = 100.0 * done / total if total else 100.0
        elapsed = time.time() - started_at
        sys.stdout.write(f"\rExecuting Tasks [{bar}] {done}/{total} {percent:5.1f}% elapsed {elapsed:.1f}s")
        if done == total:
            sys.stdout.write("\n")
        sys.stdout.flush()

    def _execute_workflow_local(self, run):
        """Run the workflow locally in topological order."""
        py_graph, bridge = run.workflow, run.bridge
        out_dir = run.output_dir
        os.makedirs(out_dir, exist_ok=True)
        prev_cwd = os.getcwd()
        t0 = time.time()
        try:
            order = py_graph.get_topological_order()
            interval = float(self.get_param("progress-bar-update-interval-sec"))
            if interval <= 0:
                interval = 0.1

            n = len(order)
            if n == 0:
                return time.time() - t0

            self._print_local_progress(0, n, t0)
            last_update = time.time()
            for i, k in enumerate(order, 1):
                task_dir = os.path.join(out_dir, ".vine_graph_tasks", f"task-{bridge.get_node_id(k)}")
                os.makedirs(task_dir, exist_ok=True)
                remove_tree_contents(task_dir)
                for task_path in py_graph.output_files_by_task.get(k, {}):
                    parent = os.path.dirname(os.path.join(task_dir, task_path))
                    os.makedirs(parent, exist_ok=True)
                os.chdir(task_dir)
                try:
                    out = compute_task(run, py_graph.task_dict[k])
                finally:
                    os.chdir(prev_cwd)
                for task_path, file_id in py_graph.output_files_by_task.get(k, {}).items():
                    local_path = os.path.abspath(os.path.join(task_dir, task_path))
                    if not os.path.isfile(local_path):
                        raise FileNotFoundError(
                            f"task {k} did not produce declared output file {task_path!r}"
                        )
                    run.local_file_paths[file_id] = local_path
                run.save_task_output(k, out)
                now = time.time()
                if now - last_update >= interval or i == n:
                    self._print_local_progress(i, n, t0)
                    last_update = now
        finally:
            os.chdir(prev_cwd)
        return time.time() - t0

    def run(
        self,
        task_dict,
        targets=None,
        params=None,
        hoisting_modules=None,
        env_files=None,
        from_dask=False,
        expand_subgraphs=False,
        repeats=1,
    ):
        """Build the graph, run it, and return the requested results."""
        requested_targets = list(targets or [])
        params = {} if params is None else params
        hoisting_modules = [] if hoisting_modules is None else hoisting_modules
        env_files = {} if env_files is None else env_files
        self.set_params(params)

        if from_dask:
            task_dict = VineGraphDaskAdaptor(task_dict, expand_subgraphs=expand_subgraphs).converted

        file_target_ids = set()
        if isinstance(task_dict, Workflow):
            result_items = []
            for target in requested_targets:
                if isinstance(target, TaskHandle):
                    result_items.append(("task", target, task_dict._task_key(target)))
                elif isinstance(target, FileHandle):
                    if target.workflow_id != task_dict._workflow_id:
                        raise ValueError("file target belongs to a different Workflow")
                    if target.file_id not in task_dict.input_files and target.file_id not in task_dict.output_files:
                        raise ValueError("file target does not belong to this Workflow")
                    result_items.append(("file", target, target.file_id))
                    if target.file_id in task_dict.output_files:
                        file_target_ids.add(target.file_id)
                else:
                    raise TypeError("Workflow targets must be TaskHandle or FileHandle objects")
        else:
            if any(isinstance(target, (TaskHandle, FileHandle)) for target in requested_targets):
                raise TypeError("TaskHandle and FileHandle targets require a Workflow")
            result_items = [("task", target, target) for target in requested_targets]

        scheduler_targets = [key for kind, _, key in result_items if kind == "task"]
        task_dict, scheduler_targets = self._replicate_graph(task_dict, scheduler_targets, repeats)

        py_graph, bridge = self.build_workflow_and_capi_bridge(
            task_dict, scheduler_targets, file_target_ids
        )
        local_execute = bool(self.get_param("local-execute"))
        task_runner_registration = None
        edata_directory = None

        try:
            run = WorkflowRun(
                py_graph, bridge, self.get_param("output-dir"),
                self.get_param("extra-task-output-size-mb"), self.get_param("extra-task-sleep-time"),
            )
            if local_execute:
                print("=== local-execute: running Workflow in process (no workers)", flush=True)
                makespan_s = self._execute_workflow_local(run)
                completed_recovery_tasks = 0
            else:
                edata_directory = tempfile.TemporaryDirectory(prefix="vine-graph-edata-", dir=self.staging_directory)
                self._stage_node_edata(run, edata_directory.name)
                task_runner_registration = self.build_task_runner_registration(bridge, hoisting_modules, env_files)
                task_runner_registration.install()
                bridge.execute()
                makespan_s = round(bridge.get_makespan_us() / 1e6, 6)
                completed_recovery_tasks = bridge.get_completed_recovery_tasks()

            total_tasks_completed = len(py_graph.task_dict) + completed_recovery_tasks
            throughput_tps = round(total_tasks_completed / makespan_s, 6) if makespan_s > 0 else 0.0
            print(f"=== Makespan: {makespan_s:.6f} seconds")
            print(f"=== Total tasks completed: {total_tasks_completed}")
            print(f"=== Throughput: {throughput_tps:.6f} tasks/s")

            results = {}
            for kind, public_target, key in result_items:
                if kind == "task":
                    if key not in py_graph.task_dict:
                        continue
                    results[public_target] = run.load_task_output(key)
                elif key in py_graph.input_files:
                    results[public_target] = py_graph.input_files[key]
                elif local_execute:
                    results[public_target] = run.local_file_path(key)
                else:
                    results[public_target] = os.path.abspath(bridge.get_file_target_path(key))
            return results
        finally:
            try:
                if task_runner_registration is not None:
                    task_runner_registration.uninstall()
            finally:
                try:
                    bridge.delete()
                finally:
                    if edata_directory is not None:
                        edata_directory.cleanup()

    def _on_sigint(self, signum, frame):
        self._sigint_received = True
        raise KeyboardInterrupt
