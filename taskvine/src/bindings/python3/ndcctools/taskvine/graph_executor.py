from ndcctools.taskvine import cvine
from ndcctools.taskvine.manager import Manager
from ndcctools.taskvine.utils import load_variable_from_library, delete_all_files, get_c_constant
from ndcctools.taskvine.graph_definition import TaskGraph, init_task_graph_context, compute_group_keys

import cloudpickle
import os
import collections
import inspect
import types
import hashlib
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
                 **kwargs):

        # delete all files in the run info template directory, do this before super().__init__()
        self.run_info_path = kwargs.get('run_info_path')
        self.run_info_template = kwargs.get('run_info_template')
        if self.run_info_path and self.run_info_template:
            delete_all_files(os.path.join(self.run_info_path, self.run_info_template))

        # initialize the manager
        super_params = set(inspect.signature(Manager.__init__).parameters)
        super_kwargs = {k: v for k, v in kwargs.items() if k in super_params}
        super().__init__(*args, **super_kwargs)

        # tune the manager
        leftover_kwargs = {k: v for k, v in kwargs.items() if k not in super_params}
        for key, value in leftover_kwargs.items():
            self.tune(key.replace("_", "-"), value)
        self.tune("worker-source-max-transfers", 1000)
        self.tune("max-retrievals", -1)
        self.tune("prefer-dispatch", 1)
        self.tune("transient-error-interval", 1)
        self.tune("attempt-schedule-depth", 1000)

        self.priority_mode = get_c_constant(f"task_priority_mode_{priority_mode.replace('-', '_')}")
        self.prune_depth = prune_depth
        self.staging_dir = staging_dir

        # initialize the task graph
        self._task_graph = cvine.vine_task_graph_create(self._taskvine)

        self.set_scheduler(scheduling_mode)

        # ensure the dir exists
        os.makedirs(shared_file_system_dir, exist_ok=True)
        self.shared_file_system_dir = shared_file_system_dir

        # create library task with specified resources
        self._create_library_task(libcores, hoisting_modules)

    def _create_library_task(self, libcores=1, hoisting_modules=[]):
        assert cvine.vine_task_graph_get_function_name(self._task_graph) == compute_group_keys.__name__
        hoisting_modules += [os, cloudpickle, TaskGraph, load_variable_from_library, uuid, hashlib, types, collections]
        self.libtask = self.create_library_from_functions(
            cvine.vine_task_graph_get_library_name(self._task_graph),
            compute_group_keys,
            library_context_info=[init_task_graph_context, [], {}],
            add_env=False,
            infile_load_mode="text",
            hoisting_modules=hoisting_modules
        )
        self.libtask.add_input(self.declare_file("task_graph.pkl"), "task_graph.pkl")
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
        task_graph = TaskGraph(task_dict, expand_dsk=expand_dsk, debug=debug)
        topo_order = task_graph.get_topological_order()

        # the sum of the values in outfile_type must be 1.0
        assert sum(list(outfile_type.values())) == 1.0

        # create task graph in the python side
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
        heavy_scores = {}
        for k in task_graph.task_dict.keys():
            heavy_scores[k] = cvine.vine_task_graph_get_node_heavy_score(self._task_graph, k)
        # keys with larger heavy score are in the front of the list
        sorted_keys = sorted(heavy_scores, key=lambda x: heavy_scores[x], reverse=True)

        # keys with larger heavy score should be stored into the shared file system
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

        # save the task graph to a pickle file, will be sent to the remote workers
        with open("task_graph.pkl", 'wb') as f:
            cloudpickle.dump(task_graph, f)

        # now execute the vine graph
        cvine.vine_task_graph_execute(self._task_graph)

    def __del__(self):
        cvine.vine_task_graph_delete(self._task_graph)
