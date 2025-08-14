from ndcctools.taskvine import cvine
from ndcctools.taskvine.manager import Manager
from ndcctools.taskvine.utils import load_variable_from_library, delete_all_files, get_c_constant
from ndcctools.taskvine.graph_definition import GraphKeyResult, TaskGraph, init_task_graph_context, compute_dts_key, compute_sexpr_key, compute_single_key, hash_name, hashable

import cloudpickle
import dask
import os
import collections
import inspect
import types
import signal
import hashlib
import time
import random
import uuid


class GraphExecutor(Manager):
    def __init__(self,
                 *args,
                 libcores=1,
                 hoisting_modules=[],
                 libtask_env_files={},
                 **kwargs):

        signal.signal(signal.SIGINT, self._on_sigint)

        # delete all files in the run info template directory, do this before super().__init__()
        self.run_info_path = kwargs.get('run_info_path')
        self.run_info_template = kwargs.get('run_info_template')
        self.run_info_path_absolute = os.path.join(self.run_info_path, self.run_info_template)
        if self.run_info_path and self.run_info_template:
            delete_all_files(self.run_info_path_absolute)

        # initialize the manager
        super_params = set(inspect.signature(Manager.__init__).parameters)
        super_kwargs = {k: v for k, v in kwargs.items() if k in super_params}

        super().__init__(*args, **super_kwargs)
        print(f"TaskVine manager {self.name} listening on port {self.port}")

        # tune the manager
        leftover_kwargs = {k: v for k, v in kwargs.items() if k not in super_params}
        for key, value in leftover_kwargs.items():
            try:
                self.tune(key.replace("_", "-"), value)
            except Exception as e:
                print(f"Failed to tune {key} with value {value}: {e}")
                exit(1)
        self.tune("worker-source-max-transfers", 1000)
        self.tune("max-retrievals", -1)
        self.tune("prefer-dispatch", 1)
        self.tune("transient-error-interval", 1)
        self.tune("attempt-schedule-depth", 1000)

        # initialize the task graph
        self._task_graph = cvine.vine_task_graph_create(self._taskvine)

        # create library task with specified resources
        self._create_library_task(libcores, hoisting_modules, libtask_env_files)

    def _create_library_task(self, libcores=1, hoisting_modules=[], libtask_env_files={}):
        assert cvine.vine_task_graph_get_function_name(self._task_graph) == compute_single_key.__name__
        hoisting_modules += [os, cloudpickle, GraphKeyResult, TaskGraph, uuid, hashlib, random, types, collections, time, dask, hash_name, hashable,
                             load_variable_from_library, compute_dts_key, compute_sexpr_key, compute_single_key]
        self.libtask = self.create_library_from_functions(
            cvine.vine_task_graph_get_library_name(self._task_graph),
            compute_single_key,
            library_context_info=[init_task_graph_context, [], {}],
            add_env=False,
            infile_load_mode="text",
            hoisting_modules=hoisting_modules
        )
        self.libtask.add_input(self.declare_file("task_graph.pkl"), "task_graph.pkl")
        for local_file_path, remote_file_path in libtask_env_files.items():
            self.libtask.add_input(self.declare_file(local_file_path, cache=True, peer_transfer=True), remote_file_path)
        self.libtask.set_cores(libcores)
        self.libtask.set_function_slots(libcores)
        self.install_library(self.libtask)

    def run(self,
            task_dict,
            expand_dsk=False,
            replica_placement_policy="random",
            priority_mode="largest-input-first",
            extra_task_output_size_mb=[0, 0],
            extra_task_sleep_time=[0, 0],
            prune_depth=1,
            shared_file_system_dir="/project01/ndcms/jzhou24/shared_file_system",
            staging_dir="/project01/ndcms/jzhou24/staging",
            balance_worker_disk_load=0,
            scheduling_mode="files",
            outfile_type={
                "temp": 1.0,
                "shared-file-system": 0.0,
            }):

        if balance_worker_disk_load:
            self.tune("balance-worker-disk-load", balance_worker_disk_load)
            self.set_scheduler("worst")
        else:
            self.set_scheduler(scheduling_mode)

        # create task graph in the python side
        print("Initializing TaskGraph object")
        task_graph = TaskGraph(task_dict,
                               expand_dsk=expand_dsk,
                               extra_task_output_size_mb=extra_task_output_size_mb,
                               extra_task_sleep_time=extra_task_sleep_time)
        topo_order = task_graph.get_topological_order()

        # the sum of the values in outfile_type must be 1.0
        assert sum(list(outfile_type.values())) == 1.0

        # set replica placement policy
        cvine.vine_set_replica_placement_policy(self._taskvine, get_c_constant(f"replica_placement_policy_{replica_placement_policy.replace('-', '_')}"))

        # create task graph in the python side
        print("Initializing task graph in TaskVine")
        for k in topo_order:
            cvine.vine_task_graph_create_node(self._task_graph,
                                              task_graph.vine_key_of[k],
                                              staging_dir,
                                              prune_depth,
                                              get_c_constant(f"task_priority_mode_{priority_mode.replace('-', '_')}"))
            for pk in task_graph.parents_of.get(k, []):
                cvine.vine_task_graph_add_dependency(self._task_graph, task_graph.vine_key_of[pk], task_graph.vine_key_of[k])

        # we must finalize the graph in c side after all nodes and dependencies are added
        # this includes computing various metrics for each node, such as depth, height, heavy score, etc.
        cvine.vine_task_graph_finalize_metrics(self._task_graph)

        # then we can use the heavy score to sort the nodes and specify their outfile remote names
        heavy_scores = {}
        for k in task_graph.task_dict.keys():
            heavy_scores[k] = cvine.vine_task_graph_get_node_heavy_score(self._task_graph, task_graph.vine_key_of[k])

        # keys with larger heavy score should be stored into the shared file system
        os.makedirs(shared_file_system_dir, exist_ok=True)
        sorted_keys = sorted(heavy_scores, key=lambda x: heavy_scores[x], reverse=True)
        shared_file_system_size = round(len(sorted_keys) * outfile_type["shared-file-system"])
        for i, k in enumerate(sorted_keys):
            if i < shared_file_system_size:
                choice = "shared-file-system"
                task_graph.set_outfile_remote_name_of(task_graph.vine_key_of[k], os.path.join(shared_file_system_dir, task_graph.outfile_remote_name[k]))
            else:
                choice = "temp"
            outfile_type_str = f"NODE_OUTFILE_TYPE_{choice.upper().replace('-', '_')}"
            cvine.vine_task_graph_set_node_outfile(self._task_graph,
                                                   task_graph.vine_key_of[k],
                                                   get_c_constant(outfile_type_str),
                                                   task_graph.outfile_remote_name[k])

        # save the task graph to a pickle file, will be sent to the remote workers
        with open("task_graph.pkl", 'wb') as f:
            cloudpickle.dump(task_graph, f)

        # now execute the vine graph
        print(f"Executing task graph, logs will be written into {self.run_info_path_absolute}")
        cvine.vine_task_graph_execute(self._task_graph)

    def _on_sigint(self, signum, frame):
        self.__del__()

    def __del__(self):
        if hasattr(self, '_task_graph') and self._task_graph:
            cvine.vine_task_graph_delete(self._task_graph)
