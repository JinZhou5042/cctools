from ndcctools.taskvine import cvine
from ndcctools.taskvine.manager import Manager
from ndcctools.taskvine.utils import load_variable_from_library
from ndcctools.taskvine.utils import delete_all_files, get_c_constant

import cloudpickle
import os
import collections
import dask
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

        self.depth_of = collections.defaultdict(int)
        self._compute_depths()

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

    def _compute_depths(self):
        visited = set()

        def dfs(key):
            if key in visited:
                return self.depth_of[key]
            visited.add(key)
            if not self.parents_of[key]:
                self.depth_of[key] = 0
            else:
                self.depth_of[key] = 1 + max(dfs(p) for p in self.parents_of[key])
            return self.depth_of[key]

        for key in self.task_dict:
            dfs(key)

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
            if any(c not in group_keys for c in self.children_of[k])
        }

        return {
            key: self.outfile_remote_name[key]
            for key in external_output_keys
        }

    def get_sexpr_of_group(self, group_keys):
        return {k: self.task_dict[k] for k in group_keys}


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

    for k, path in task_graph.external_output_keys_to_paths(keys).items():
        with open(path, 'wb') as f:
            cloudpickle.dump(values[k], f)

    return True


class GraphManager(Manager):
    def __init__(self, *args, **kwargs):
        # delete all files in the run info template directory, do this before super().__init__()
        self.run_info_path = kwargs.get('run_info_path')
        self.run_info_template = kwargs.get('run_info_template')
        if self.run_info_path and self.run_info_template:
            delete_all_files(os.path.join(self.run_info_path, self.run_info_template))

        super().__init__(*args, **kwargs)

        # tune the manager for vine graph optimization
        self.tune("worker-source-max-transfers", 1000)
        self.tune("max-retrievals", -1)
        self.tune("prefer-dispatch", 1)
        self.tune("transient-error-interval", 1)
        self.tune("attempt-schedule-depth", 1000)

        self._library_name = "task-graph-library"
        self._node_compute_function_name = "compute_group_keys"

    def _create_library_task(self, libcores=1, hoisting_modules=[]):
        hoisting_modules += [os, cloudpickle, TaskGraph, load_variable_from_library, uuid, hashlib, types, collections]
        self.libtask = self.create_library_from_functions(
            self._library_name,
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

    def _create_vine_graph(self, task_dict, expand_dsk=False, debug=False):
        # create task graph in the python side
        task_graph = TaskGraph(task_dict, expand_dsk=expand_dsk, debug=debug)
        with open("task_graph.pkl", 'wb') as f:
            cloudpickle.dump(task_graph, f)

        # create dependenciy mapping in the C side
        for k, pks in task_graph.parents_of.items():
            for pk in pks:
                cvine.vine_task_graph_add_dependency(self._taskvine, k, pk)

        # set output file remote name in the C side
        for k, outfile_remote_name in task_graph.outfile_remote_name.items():
            cvine.vine_task_graph_set_node_outfile_remote_name(self._taskvine, k, outfile_remote_name)

        # finalize the task graph in the C side, building dependencies, creating tasks, etc.
        cvine.vine_task_graph_finalize(self._taskvine, self._library_name, self._node_compute_function_name)

    def execute(self, task_dict,
                expand_dsk=False,
                libcores=1,
                hoisting_modules=[],
                prune_mode="static",
                static_prune_depth=0,
                ):

        # set prune algorithm and static prune depth
        cvine.vine_task_graph_set_static_prune_depth(self._taskvine, static_prune_depth)
        cvine.vine_task_graph_set_prune_algorithm(self._taskvine, get_c_constant(f"prune_algorithm_{prune_mode}"))

        # create library task with specified resources
        self._create_library_task(libcores, hoisting_modules)

        # initialize the vine graph in the C side
        self._create_vine_graph(task_dict, expand_dsk=expand_dsk)

        # now execute the vine graph
        cvine.vine_task_graph_execute(self._taskvine)
