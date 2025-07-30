import os
import hashlib
import types
import dask
import cloudpickle
import collections
import uuid
from ndcctools.taskvine.utils import load_variable_from_library
from collections import deque


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
