from ndcctools.taskvine import cvine
from ndcctools.taskvine.manager import Manager
from ndcctools.taskvine.utils import load_variable_from_library, delete_all_files, get_c_constant
from ndcctools.taskvine.graph_definition import GraphKeyResult, TaskGraph, init_task_graph_context, compute_group_keys

import cloudpickle
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
                 prune_depth=1,
                 priority_mode="largest-input-first",
                 scheduling_mode="files",
                 libcores=1,
                 hoisting_modules=[],
                 staging_dir="/project01/ndcms/jzhou24/staging",
                 shared_file_system_dir="/project01/ndcms/jzhou24/shared_file_system",
                 replica_placement_policy="random",
                 balance_worker_disk_load=0,
                 extra_task_sleep_time=[0, 0],
                 extra_task_output_size_mb=[0, 0],
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

        print("Creating taskvine manager")
        super().__init__(*args, **super_kwargs)
        print(f"TaskVine manager listening on port {self.port}")

        # tune the manager
        leftover_kwargs = {k: v for k, v in kwargs.items() if k not in super_params}
        for key, value in leftover_kwargs.items():
            self.tune(key.replace("_", "-"), value)
        self.tune("worker-source-max-transfers", 1000)
        self.tune("max-retrievals", -1)
        self.tune("prefer-dispatch", 1)
        self.tune("transient-error-interval", 1)
        self.tune("attempt-schedule-depth", 1000)

        if balance_worker_disk_load:
            self.tune("balance-worker-disk-load", balance_worker_disk_load)
            self.set_scheduler("worst")
        else:
            self.set_scheduler(scheduling_mode)

        self.priority_mode = get_c_constant(f"task_priority_mode_{priority_mode.replace('-', '_')}")
        self.replica_placement_policy = get_c_constant(f"replica_placement_policy_{replica_placement_policy.replace('-', '_')}")
        cvine.vine_set_replica_placement_policy(self._taskvine, self.replica_placement_policy)

        self.prune_depth = prune_depth
        self.staging_dir = staging_dir

        # initialize the task graph
        self._task_graph = cvine.vine_task_graph_create(self._taskvine)

        # ensure the dir exists
        os.makedirs(shared_file_system_dir, exist_ok=True)
        self.shared_file_system_dir = shared_file_system_dir

        # create library task with specified resources
        self._create_library_task(libcores, hoisting_modules, libtask_env_files)

        self.extra_task_sleep_time = extra_task_sleep_time
        self.extra_task_output_size_mb = extra_task_output_size_mb
        assert isinstance(self.extra_task_sleep_time, list) and len(self.extra_task_sleep_time) == 2 and self.extra_task_sleep_time[0] <= self.extra_task_sleep_time[1]
        assert isinstance(self.extra_task_output_size_mb, list) and len(self.extra_task_output_size_mb) == 2 and self.extra_task_output_size_mb[0] <= self.extra_task_output_size_mb[1]

    def _create_library_task(self, libcores=1, hoisting_modules=[], libtask_env_files={}):
        print("Initializing library task")
        assert cvine.vine_task_graph_get_function_name(self._task_graph) == compute_group_keys.__name__
        hoisting_modules += [os, cloudpickle, GraphKeyResult, TaskGraph, load_variable_from_library, uuid, hashlib, types, collections, time]
        self.libtask = self.create_library_from_functions(
            cvine.vine_task_graph_get_library_name(self._task_graph),
            compute_group_keys,
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
            debug=False,
            outfile_type={
                "temp": 1.0,
                "shared-file-system": 0.0,
            }):

        # create task graph in the python side
        print("Initializing TaskGraph object")
        task_graph = TaskGraph(task_dict, expand_dsk=expand_dsk, debug=debug)
        topo_order = task_graph.get_topological_order()

        # the sum of the values in outfile_type must be 1.0
        assert sum(list(outfile_type.values())) == 1.0

        # create task graph in the python side
        print("Initializing task graph in TaskVine")
        for k in topo_order:
            cvine.vine_task_graph_create_node(self._task_graph,
                                              k,
                                              self.staging_dir,
                                              self.prune_depth,
                                              self.priority_mode)
            for pk in task_graph.parents_of.get(k, []):
                cvine.vine_task_graph_add_dependency(self._task_graph, pk, k)

        # we must finalize the graph in c side after all nodes and dependencies are added
        # this includes computing various metrics for each node, such as depth, height, heavy score, etc.
        cvine.vine_task_graph_finalize_metrics(self._task_graph)

        # then we can use the heavy score to sort the nodes and specify their outfile remote names
        print("Computing heavy scores for each node")
        heavy_scores = {}
        for k in task_graph.task_dict.keys():
            heavy_scores[k] = cvine.vine_task_graph_get_node_heavy_score(self._task_graph, k)
        # keys with larger heavy score are in the front of the list
        sorted_keys = sorted(heavy_scores, key=lambda x: heavy_scores[x], reverse=True)

        # keys with larger heavy score should be stored into the shared file system
        print("Determining outputs to be writen into the shared file system")
        shared_file_system_size = round(len(sorted_keys) * outfile_type["shared-file-system"])
        for i, k in enumerate(sorted_keys):
            if i < shared_file_system_size:
                choice = "shared-file-system"
                task_graph.set_outfile_remote_name_of(k, os.path.join(self.shared_file_system_dir, task_graph.outfile_remote_name[k]))
            else:
                choice = "temp"
            outfile_type_str = f"NODE_OUTFILE_TYPE_{choice.upper().replace('-', '_')}"
            cvine.vine_task_graph_set_node_outfile(self._task_graph,
                                                   k,
                                                   get_c_constant(outfile_type_str),
                                                   task_graph.outfile_remote_name[k])

        # set the extra output file size for each node to monitor storage consumption
        max_depth = max(depth for depth in task_graph.depth_of.values())
        for k in task_graph.task_dict.keys():
            # skip if the node is the final merging node or the output node for each branch
            if task_graph.depth_of[k] == max_depth or task_graph.depth_of[k] == max_depth - 1:
                continue
            task_graph.extra_size_mb_of[k] = random.uniform(self.extra_task_output_size_mb[0], self.extra_task_output_size_mb[1])
            task_graph.extra_sleep_time_of[k] = random.uniform(self.extra_task_sleep_time[0], self.extra_task_sleep_time[1])

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
