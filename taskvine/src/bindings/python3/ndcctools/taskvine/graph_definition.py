import os
import hashlib
import types
import dask
import cloudpickle
import collections
import uuid
import random
from ndcctools.taskvine.utils import load_variable_from_library
from collections import deque


try:
    import dask._task_spec as dts
except Exception:
    dts = None
    pass


def hash_name(*args):
    out_str = ""
    for arg in args:
        out_str += str(arg)
    return hashlib.sha256(out_str.encode('utf-8')).hexdigest()[:32]


def hashable(s):
    try:
        hash(s)
        return True
    except TypeError:
        return False


class GraphKeyResult:
    # extra_size_mb is used to allocate more space for this object in testing mode to evaluate storage consumption
    # and peer transfer performance across all workers.
    def __init__(self, result, extra_size_mb=None):
        self.result = result
        self.extra_obj = bytearray(int(extra_size_mb * 1024 * 1024)) if extra_size_mb and extra_size_mb > 0 else None


class TaskGraph:
    def __init__(self, task_dict, expand_dsk=False, extra_task_output_size_mb=[0, 0], extra_task_sleep_time=[0, 0]):
        self.task_dict = task_dict

        if hasattr(dask, "_task_spec"):
            self.task_dict = dask._task_spec.convert_legacy_graph(self.task_dict)

        if expand_dsk:
            self.task_dict = self._expand_legacy_dsk(self.task_dict)

        if dts:
            for k, v in self.task_dict.items():
                assert isinstance(v, (dts.Alias, dts.Task, dts.DataNode)), f"Unsupported task type for key {k}: {v.__class__}"

        self.parents_of, self.children_of = self._build_dependencies(self.task_dict)
        self.depth_of = self._calculate_depths()

        self.vine_key_of = {k: hash_name(k) for k in task_dict.keys()}
        self.key_of_vine_key = {hash_name(k): k for k in task_dict.keys()}

        self.outfile_remote_name = {key: f"{uuid.uuid4()}.pkl" for key in self.task_dict.keys()}

        # testing params
        self.extra_task_output_size_mb = self._calculate_extra_size_mb_of(extra_task_output_size_mb)
        self.extra_sleep_time_of = self._calculate_extra_sleep_time_of(extra_task_sleep_time)

    def _calculate_extra_size_mb_of(self, extra_task_output_size_mb):
        assert isinstance(extra_task_output_size_mb, list) and len(extra_task_output_size_mb) == 2 and extra_task_output_size_mb[0] <= extra_task_output_size_mb[1]
        max_depth = max(depth for depth in self.depth_of.values())
        extra_size_mb_of = {}
        for k in self.task_dict.keys():
            if self.depth_of[k] == max_depth or self.depth_of[k] == max_depth - 1:
                extra_size_mb_of[k] = 0
                continue
            extra_size_mb_of[k] = random.uniform(extra_task_output_size_mb[0], extra_task_output_size_mb[1])

        return extra_size_mb_of

    def _calculate_extra_sleep_time_of(self, extra_task_sleep_time):
        assert isinstance(extra_task_sleep_time, list) and len(extra_task_sleep_time) == 2 and extra_task_sleep_time[0] <= extra_task_sleep_time[1]
        extra_sleep_time_of = {}
        for k in self.task_dict.keys():
            extra_sleep_time_of[k] = random.uniform(extra_task_sleep_time[0], extra_task_sleep_time[1])

        return extra_sleep_time_of

    def _calculate_depths(self):
        depth_of = {key: 0 for key in self.task_dict.keys()}

        topo_order = self.get_topological_order()
        for key in topo_order:
            if self.parents_of[key]:
                depth_of[key] = max(depth_of[parent] for parent in self.parents_of[key]) + 1
            else:
                depth_of[key] = 0

        return depth_of

    def set_outfile_remote_name_of(self, key, outfile_remote_name):
        self.outfile_remote_name[key] = outfile_remote_name

    def is_dts_key(self, k):
        if not hasattr(dask, "_task_spec"):
            return False
        import dask._task_spec as dts
        return isinstance(self.task_dict[k], (dts.Task, dts.TaskRef, dts.Alias, dts.DataNode, dts.NestedContainer))

    def _build_dependencies(self, task_dict):
        def _find_sexpr_parents(sexpr):
            if hashable(sexpr) and sexpr in task_dict.keys():
                return {sexpr}
            elif isinstance(sexpr, (list, tuple)):
                deps = set()
                for x in sexpr:
                    deps |= _find_sexpr_parents(x)
                return deps
            elif isinstance(sexpr, dict):
                deps = set()
                for k, v in sexpr.items():
                    deps |= _find_sexpr_parents(k)
                    deps |= _find_sexpr_parents(v)
                return deps
            else:
                return set()

        parents_of = collections.defaultdict(set)
        children_of = collections.defaultdict(set)

        for k, value in task_dict.items():
            if self.is_dts_key(k):
                # in the new Dask expression, each value is an object from dask._task_spec, could be
                # a Task, Alias, TaskRef, etc., but they all share the same base class the dependencies
                # field is of type frozenset(), without recursive ancestor dependencies involved
                parents_of[k] = value.dependencies
            else:
                # the value could be a sexpr, e.g., the old Dask representation
                parents_of[k] = _find_sexpr_parents(value)

        for k, deps in parents_of.items():
            for dep in deps:
                children_of[dep].add(k)

        return parents_of, children_of

    def _expand_legacy_dsk(self, task_dict, save_dir="./"):
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

    def save_result_of_key(self, key, result):
        with open(self.outfile_remote_name[key], "wb") as f:
            result_obj = GraphKeyResult(result, extra_size_mb=self.extra_task_output_size_mb[key])
            cloudpickle.dump(result_obj, f)

    def load_result_of_key(self, key):
        try:
            with open(self.outfile_remote_name[key], "rb") as f:
                result_obj = cloudpickle.load(f)
                assert isinstance(result_obj, GraphKeyResult), "Loaded object is not of type GraphKeyResult"
                return result_obj.result
        except FileNotFoundError:
            raise FileNotFoundError(f"Output file for key {key} not found at {self.outfile_remote_name[key]}")

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
            print(f"len(topo_order): {len(topo_order)}")
            print(f"len(self.task_dict): {len(self.task_dict)}")
            raise ValueError("Failed to create topo order, the dependencies may be cyclic or problematic")

        return topo_order


def init_task_graph_context():
    with open("task_graph.pkl", 'rb') as f:
        task_graph = cloudpickle.load(f)

    return {
        'task_graph': task_graph,
    }


def compute_dts_key(task_graph, k, v):
    try:
        import dask._task_spec as dts
    except ImportError:
        raise ImportError("Dask is not installed")

    input_dict = {dep: task_graph.load_result_of_key(dep) for dep in v.dependencies}

    try:
        if isinstance(v, dts.Alias):
            assert len(v.dependencies) == 1, "Expected exactly one dependency"
            return task_graph.load_result_of_key(next(iter(v.dependencies)))
        elif isinstance(v, dts.Task):
            return v(input_dict)
        elif isinstance(v, dts.DataNode):
            return v.value
        else:
            raise TypeError(f"unexpected node type: {type(v)} for key {k}")
    except Exception as e:
        raise Exception(f"Error while executing task {k}: {e}")


def compute_sexpr_key(task_graph, k, v):
    input_dict = {parent: task_graph.load_result_of_key(parent) for parent in task_graph.parents_of[k]}

    def _rec_call(expr):
        try:
            if expr in input_dict.keys():
                return input_dict[expr]
        except TypeError:
            pass
        if isinstance(expr, list):
            return [_rec_call(e) for e in expr]
        if isinstance(expr, tuple) and isinstance(expr[0], str) and callable(expr[0]):
            res = expr[0](*[_rec_call(a) for a in expr[1:]])
            return res
        return expr

    try:
        return _rec_call(v)
    except Exception as e:
        raise Exception(f"Failed to invoke _rec_call(): {e}")


def compute_single_key(vine_key):
    task_graph = load_variable_from_library('task_graph')

    k = task_graph.key_of_vine_key[vine_key]
    v = task_graph.task_dict[k]

    if task_graph.is_dts_key(k):
        result = compute_dts_key(task_graph, k, v)
    else:
        result = compute_sexpr_key(task_graph, k, v)

    task_graph.save_result_of_key(k, result)
    if not os.path.exists(task_graph.outfile_remote_name[k]):
        raise Exception(f"Output file {task_graph.outfile_remote_name[k]} does not exist after writing")
    if os.stat(task_graph.outfile_remote_name[k]).st_size == 0:
        raise Exception(f"Output file {task_graph.outfile_remote_name[k]} is empty after writing")

    return True
