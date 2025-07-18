from ndcctools.taskvine import cvine
from ndcctools.taskvine.manager import Manager
from ndcctools.taskvine.utils import load_variable_from_library
from ndcctools.taskvine.utils import delete_all_files, get_c_constant

import cloudpickle
import os
import collections
import random
import dask
import inspect
from collections import deque
import types
import hashlib
import uuid


class TaskGraph:
    def __init__(self, task_dict, expand_dsk=True, debug=False):
        self.task_dict = self._standardize_keys(task_dict)

        if expand_dsk:
            self.task_dict = self._expand_dsk(self.task_dict)

        self.map_id_to_callable = {}  # id -> fn
        self.task_dict = self._create_callable_mapping(self.task_dict)

        self.parents_of = collections.defaultdict(set)
        self.children_of = collections.defaultdict(set)
        self._build_dependencies()

        self.outfile_remote_name = {key: f"{uuid.uuid4()}.pkl" for key in self.task_dict.keys()}
        self.output_vine_file_of = {key: None for key in self.task_dict.keys()}

        if debug:
            self._write_task_dependencies()
            self._visualize_task_dependencies()

    @staticmethod
    def hashable(s):
        try:
            hash(s)
            return True
        except TypeError:
            return False

    @staticmethod
    def hash_name(*args):
        out_str = ""
        for arg in args:
            out_str += str(arg)
        return hashlib.sha256(out_str.encode('utf-8')).hexdigest()[:20]

    def set_outfile_remote_name_of(self, key, outfile_remote_name):
        self.outfile_remote_name[key] = outfile_remote_name

    def _standardize_keys(self, task_dict):
        key_mapping = {k: TaskGraph.hash_name(k) for k in task_dict.keys()}

        def replace_keys(expr):
            if TaskGraph.hashable(expr) and expr in key_mapping:
                return key_mapping[expr]
            elif isinstance(expr, (list, tuple)):
                return type(expr)(replace_keys(e) for e in expr)
            elif isinstance(expr, dict):
                return {replace_keys(k): replace_keys(v) for k, v in expr.items()}
            else:
                return expr

        new_task_dict = {}
        for k, sexpr in task_dict.items():
            new_k = key_mapping[k]
            new_task_dict[new_k] = replace_keys(sexpr)

        return new_task_dict

    def _build_dependencies(self):
        def _find_parents(sexpr):
            if TaskGraph.hashable(sexpr) and sexpr in self.task_dict.keys():
                return {sexpr}
            elif isinstance(sexpr, (list, tuple)):
                deps = set()
                for x in sexpr:
                    deps |= _find_parents(x)
                return deps
            elif isinstance(sexpr, dict):
                deps = set()
                for k, v in sexpr.items():
                    deps |= _find_parents(k)
                    deps |= _find_parents(v)
                return deps
            else:
                return set()

        for key, sexpr in self.task_dict.items():
            self.parents_of[key] = _find_parents(sexpr)

        for key, deps in self.parents_of.items():
            for dep in deps:
                self.children_of[dep].add(key)

    def _write_task_dependencies(self):
        self.key_to_idx = {k: i + 1 for i, k in enumerate(self.task_dict.keys())}
        with open("task_dependencies.txt", "w") as f:
            for key, parents in self.parents_of.items():
                if parents:
                    for parent in parents:
                        if parent != key:
                            f.write(f"{self.key_to_idx[parent]} -> {self.key_to_idx[key]}\n")
                else:
                    f.write(f"None -> {self.key_to_idx[key]}\n")

    def _visualize_task_dependencies(self, input_file="task_dependencies.txt", output_file="task_graph"):
        from graphviz import Digraph
        dot = Digraph(format='svg')
        with open(input_file) as f:
            for line in f:
                parts = line.strip().split("->")
                if len(parts) != 2:
                    continue
                parent = parts[0].strip()
                child = parts[1].strip()
                if parent != "None":
                    dot.edge(parent, child)
                else:
                    dot.node(child)  # ensure orphan nodes appear
        dot.render(output_file, cleanup=True)

    def _expand_dsk(self, task_dict, save_dir="./"):
        assert os.path.exists(save_dir)

        def _convert_expr_to_task_args(dsk_key, task_dict, args, blockwise_args):
            try:
                if args in task_dict:
                    return TaskGraph.hash_name(dsk_key, args)
            except Exception:
                pass
            if isinstance(args, list):
                return [_convert_expr_to_task_args(dsk_key, task_dict, item, blockwise_args) for item in args]
            elif isinstance(args, tuple):
                # nested tuple is not allowed
                return tuple(_convert_expr_to_task_args(dsk_key, task_dict, item, blockwise_args) for item in args)
            else:
                if isinstance(args, str) and args.startswith('__dask_blockwise__'):
                    blockwise_arg_idx = int(args.split('__')[-1])
                    return blockwise_args[blockwise_arg_idx]
                return args

        expanded_task_dict = {}

        for k, sexpr in task_dict.items():
            if isinstance(sexpr, (tuple, list)) and len(sexpr) > 0:
                callable = sexpr[0]
                args = sexpr[1:]

                if isinstance(callable, dask.optimization.SubgraphCallable):
                    expanded_task_dict[k] = TaskGraph.hash_name(k, callable.outkey)
                    for sub_key, sub_sexpr in callable.dsk.items():
                        unique_key = TaskGraph.hash_name(k, sub_key)
                        expanded_task_dict[unique_key] = _convert_expr_to_task_args(k, callable.dsk, sub_sexpr, args)
                elif isinstance(callable, types.FunctionType):
                    expanded_task_dict[k] = sexpr
                else:
                    print(f"ERROR: unexpected type: {type(callable)}")
                    exit(1)
            else:
                expanded_task_dict[k] = sexpr

        return expanded_task_dict

    def _create_callable_mapping(self, task_dict):
        _callable_to_id = {}

        def _get_callable_id(fn):
            if fn in _callable_to_id.keys():
                return _callable_to_id[fn]

            try:
                callable_id = f"{fn.__module__}.{fn.__name__}"
            except AttributeError:
                pickled = cloudpickle.dumps(fn)
                callable_id = "callable_" + hashlib.md5(pickled).hexdigest()

            _callable_to_id[fn] = callable_id
            self.map_id_to_callable[callable_id] = fn
            return callable_id

        def recurse(obj):
            if isinstance(obj, dict):
                return {k: recurse(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [recurse(e) for e in obj]
            elif isinstance(obj, tuple):
                if callable(obj[0]):
                    callable_id = _get_callable_id(obj[0])
                    return (callable_id,) + tuple(recurse(e) for e in obj[1:])
                else:
                    return tuple(recurse(e) for e in obj)
            else:
                return obj

        return {k: recurse(v) for k, v in task_dict.items()}

    def get_input_keys_of_group(self, group_keys):
        group_set = set(group_keys)
        return {
            p for k in group_keys
            for p in self.parents_of[k]
            if p not in group_set
        }

    def get_output_keys_of_group(self, group_keys):
        group_set = set(group_keys)
        return {
            k for k in group_keys
            if any(c not in group_set for c in self.children_of[k])
        }

    def external_input_keys_to_paths(self, group_keys):
        group_keys = set(group_keys)
        external_input_keys = {
            parent
            for key in group_keys
            for parent in self.parents_of[key]
            if parent not in group_keys
        }

        return {
            key: self.outfile_remote_name[key]
            for key in external_input_keys
        }

    def external_output_keys_to_paths(self, group_keys):
        group_keys = set(group_keys)
        external_output_keys = {
            k for k in group_keys
            if len(self.children_of[k]) == 0 or any(c not in group_keys for c in self.children_of[k])
        }

        return {
            key: self.outfile_remote_name[key]
            for key in external_output_keys
        }

    def get_sexpr_of_group(self, group_keys):
        return {k: self.task_dict[k] for k in group_keys}

    def get_topological_order(self):
        in_degree = {key: len(self.parents_of[key]) for key in self.task_dict.keys()}
        queue = deque([key for key, degree in in_degree.items() if degree == 0])
        topo_order = []

        while queue:
            current = queue.popleft()
            topo_order.append(current)

            for child in self.children_of[current]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(topo_order) != len(self.task_dict):
            raise ValueError("Task graph contains cycles - no valid topological order exists")

        return topo_order


def init_task_graph_context():
    with open("task_graph.pkl", 'rb') as f:
        task_graph = cloudpickle.load(f)

    return {
        'task_graph': task_graph,
    }


def compute_group_keys(key):
    keys = [key]
    task_graph = load_variable_from_library('task_graph')

    values = {}

    for k, path in task_graph.external_input_keys_to_paths(keys).items():
        try:
            with open(path, 'rb') as f:
                values[k] = cloudpickle.load(f)
        except Exception as e:
            raise Exception(f"Error loading input {k} from {path}: {e}")

    def rec_call(expr):
        try:
            if expr in values:
                return values[expr]
        except TypeError:
            pass
        if isinstance(expr, list):
            return [rec_call(e) for e in expr]
        if isinstance(expr, tuple) and isinstance(expr[0], str) and expr[0] in task_graph.map_id_to_callable:
            fn = task_graph.map_id_to_callable[expr[0]]
            return fn(*[rec_call(a) for a in expr[1:]])
        return expr

    for k in keys:
        result = rec_call(task_graph.task_dict[k])
        values[k] = result

    result_paths = set()

    for k, path in task_graph.external_output_keys_to_paths(keys).items():
        with open(path, 'wb') as f:
            cloudpickle.dump(values[k], f)
        if not os.path.exists(path):
            raise Exception(f"Output file {path} does not exist after writing")
        if os.stat(path).st_size == 0:
            raise Exception(f"Output file {path} is empty after writing")
        result_paths.add(path)

    return result_paths


class GraphManager(Manager):
    def __init__(self,
                 *args,
                 prune_depth=0,
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

    def execute(self,
                task_dict,
                expand_dsk=False,
                debug=False,
                output_store_location={
                    "temp": 1.0,
                    "checkpoint": 0.0,
                    "staging_dir": 0.0,
                    "shared_file_system": 0.0,
                }
                ):

        # create task graph in the python side
        task_graph = TaskGraph(task_dict, expand_dsk=expand_dsk, debug=debug)
        topo_order = task_graph.get_topological_order()

        # calculate the number of tasks to store to each location
        choices = list(output_store_location.keys())
        weights = list(output_store_location.values())
        assert sum(weights) == 1.0

        # create task graph in the python side
        for k in topo_order:
            choice = random.choices(choices, weights)[0]
            store_output_location_str = f"OUTPUT_STORE_LOCATION_{choice.upper()}"
            if choice == "shared_file_system":
                task_graph.set_outfile_remote_name_of(k, os.path.join(self.shared_file_system_dir, task_graph.outfile_remote_name[k]))
            cvine.vine_task_graph_create_node(self._task_graph, 
                                              k,
                                              task_graph.outfile_remote_name[k],
                                              self.staging_dir,
                                              self.prune_depth,
                                              self.priority_mode, 
                                              get_c_constant(store_output_location_str))
            for pk in task_graph.parents_of.get(k, []):
                cvine.vine_task_graph_add_dependency(self._task_graph, pk, k)

        with open("task_graph.pkl", 'wb') as f:
            cloudpickle.dump(task_graph, f)

        # now execute the vine graph
        cvine.vine_task_graph_execute(self._task_graph)

    def __del__(self):
        cvine.vine_task_graph_delete(self._task_graph)
