# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""The TaskVine library that runs the tasks of an Executor on Workers.

An Executor installs one library with installed() and removes it when it closes. Every task calls run_node(), the
library's only function, without arguments. The task sandbox holds a manifest at MANIFEST with the function and the
arguments of the task, small ones serialized inside it and larger ones as staged files named by sandbox path, and the
sandbox paths of the results and files the task reads. run_node()
returns the function's result, which the TaskVine library writes to RESULT as {"Result", "Success", "Reason"}. A
function that raises ends the task process with a non-zero status after printing its traceback, because the library
otherwise reports exceptions with status zero. Local execution calls run_task() too, so both run a function alike.

A task that reads a result at exactly one place, through a selection such as node[k], needs only the selected value.
The manifest of the task that produces the result may list selections, which run_node() writes each to a file of its
own with save_selection(), and a task that reads one of them mounts only that file instead of the whole result.

The library prepares itself once when it starts, in its own process: it imports the modules it was given, then loads
the hoisted objects from one file, HOISTED, which maps each name to the object serialized on its own, and the cached
edata from another, EDATA_CACHE, which maps sandbox names of staged objects to their serialized bytes. Every task it
forks inherits all of them, so tasks neither import those modules nor load those objects again, and a task reads a
cached object instead of the file that its manifest names.
"""

import contextlib
import gc
import importlib
import os
import types
import shutil
import sys
import traceback
import uuid
from pathlib import Path

import cloudpickle

from .graph import File, Hoisted, _Attribute, _Ref, replace_symbols

# Sandbox path of the manifest that describes the task.
MANIFEST = "vine-graph-node.pkl"
# Sandbox path where the TaskVine library writes the result of a function call.
RESULT = "outfile"
# Library sandbox path of the hoisted objects.
HOISTED = "vine-graph-hoisted.pkl"
# Library sandbox path of the cached edata.
EDATA_CACHE = "vine-graph-edata-cache.pkl"

# The hoisted objects of this library process, by name, which forked tasks inherit.
_hoisted = {}
# The cached edata of this library process, by the sandbox name of its staged file, which forked tasks inherit.
_edata = {}


def unique_name():
    """Return a new library name. An Executor never reaches a library instance of an earlier Executor that Workers
    have not removed yet."""
    return f"vine-graph-{uuid.uuid4().hex}"


@contextlib.contextmanager
def installed(manager, name, modules=(), files=None, hoisted=None, edata_cache=None):
    """Install the library named name on a Manager, yield the name of the function every task calls, and remove the
    library with its files on exit. When the library starts, it imports modules, given as module objects or names,
    and loads hoisted and edata_cache, local paths of files written by save_objects(). files maps local paths to names
    in the library sandbox."""
    module_names = [module_name(module) for module in modules]
    files = dict(files or {})
    if hoisted:
        files[hoisted] = HOISTED
    if edata_cache:
        files[edata_cache] = EDATA_CACHE
    for local in files:
        if not os.path.exists(local):
            raise FileNotFoundError(f"Local file {local} not found")
    cache_root = Path(manager.cache_directory) / "vine-library-cache"
    before = set(cache_root.iterdir()) if cache_root.exists() else set()
    inputs = []
    installed_library = False
    try:
        try:
            context = [prepare, [module_names, HOISTED if hoisted else None, EDATA_CACHE if edata_cache else None], {}]
            library = manager.create_library_from_functions(name, run_node, add_env=False, library_context_info=context)
        finally:
            # Library construction is synchronous, so new cache directories belong to this library.
            generated = set(cache_root.iterdir()) - before if cache_root.exists() else set()
        for local, remote in files.items():
            inputs.append(manager.declare_file(local, cache=True, peer_transfer=True))
            library.add_input(inputs[-1], remote)
        # Without a resource request, each instance takes its whole Worker with one function slot per core.
        manager.install_library(library)
        installed_library = True
        yield run_node.__name__
    finally:
        try:
            # Removing the library also undeclares the code that the Manager generated for it. A library that failed
            # too often was already removed by the Manager, and removing it here still undeclares that code.
            if installed_library:
                manager.remove_library(name)
        finally:
            try:
                for file in inputs:
                    manager.undeclare_file(file)
            finally:
                for directory in generated:
                    shutil.rmtree(directory)


def save_objects(path, blobs):
    """Write objects, given as {name: serialized object}, to a file the library loads when it starts."""
    with open(path, "wb") as stream:
        cloudpickle.dump(blobs, stream)


def module_name(module):
    """Return the name of a module given as a module object or a name."""
    if isinstance(module, types.ModuleType):
        return module.__name__
    if isinstance(module, str) and module:
        return module
    raise TypeError(f"library_modules takes modules or module names, not {module!r}")


def prepare(module_names, hoisted_path, edata_path=None):
    """Prepare this library process when it starts: import the modules, then load the hoisted objects and the cached
    edata, which may need them. Freezing what was loaded keeps the garbage collector from touching that memory, so
    forked tasks share it instead of copying it."""
    for name in module_names:
        importlib.import_module(name)
    for path, objects in ((hoisted_path, _hoisted), (edata_path, _edata)):
        if path:
            with open(path, "rb") as stream:
                blobs = cloudpickle.load(stream)
            objects.update((name, cloudpickle.loads(blob)) for name, blob in blobs.items())
    gc.freeze()


def save_result(path, value):
    """Write a task result in the format of the TaskVine library."""
    with open(path, "wb") as stream:
        cloudpickle.dump({"Result": value, "Success": True, "Reason": None}, stream)


def load_result(path):
    """Read a task result written by the TaskVine library or save_result()."""
    with open(path, "rb") as stream:
        return cloudpickle.load(stream)["Result"]


def save_selection(path, result, steps):
    """Write what steps select from a task result: the selected value, or the exception that the selection or its
    serialization raises, which load_selection() raises in the task that reads it. The selection never fails the
    task that produced the result, so the file always exists. A value that cannot be serialized on its own, such as
    state that the result's own serialization replaces, is selected again from a serialized copy of the result, which
    is what a task that loads the whole result selects from. The file holds a flag that tells a value from an
    exception, followed by either."""
    with open(path, "wb") as stream:
        try:
            value = _select(result, steps)
        except Exception as error:
            _save_outcome(stream, False, _portable_error(error))
            return
        try:
            _save_outcome(stream, True, value)
            return
        except Exception:
            del value
        try:
            _save_outcome(stream, True, _select(cloudpickle.loads(cloudpickle.dumps(result)), steps))
        except Exception as error:
            _save_outcome(stream, False, _portable_error(error))


def _save_outcome(stream, selected, value):
    stream.seek(0)
    stream.truncate()
    cloudpickle.dump(selected, stream)
    cloudpickle.dump(value, stream)


def load_selection(path):
    """Return the value that a file written by save_selection() holds, or raise the exception it holds."""
    with open(path, "rb") as stream:
        selected = cloudpickle.load(stream)
        value = cloudpickle.load(stream)
    if not selected:
        raise value
    return value


def _portable_error(error):
    """Return error, or a RuntimeError with its message when it cannot be serialized and loaded again."""
    try:
        cloudpickle.loads(cloudpickle.dumps(error))
        return error
    except Exception:
        return RuntimeError("".join(traceback.format_exception_only(error)).strip())


def file_identity(ref):
    """Return the key under which a manifest lists the path of a File or of a task's output file."""
    if isinstance(ref, File):
        return ("input", ref.path)
    return ("output", ref.graph_id, ref.key, ref.path)


def _select(value, steps):
    for step in steps:
        if isinstance(step, _Attribute):
            value = getattr(value, step.name)
        elif isinstance(value, (list, tuple, dict)):
            value = value[step]
        else:
            value = getattr(value, step)
    return value


def run_task(func, args, kwargs, load_task_result, file_path, load_hoisted_object, read_once=(), load_selected=None):
    """Call func after replacing each reference in its arguments: a task result with load_task_result((graph id,
    key)), a file with file_path(file_identity(file)), and a hoisted object with load_hoisted_object(name). Other
    arguments reach func unchanged. A result or hoisted object read twice is one object. A task in read_once is read at
    exactly one place, through a selection, and load_selected(task, steps) returns the selected value, by default
    selected from load_task_result(task), so the whole result does not stay in memory while func runs."""
    results = {}
    hoisted = {}
    if load_selected is None:
        def load_selected(task, steps):
            return _select(load_task_result(task), steps)

    def replace(ref):
        if isinstance(ref, Hoisted):
            if ref.name not in hoisted:
                hoisted[ref.name] = load_hoisted_object(ref.name)
            return hoisted[ref.name]
        if isinstance(ref, _Ref):
            task = (ref.graph_id, ref.key)
            if task in read_once:
                return load_selected(task, ref.steps)
            if task not in results:
                results[task] = load_task_result(task)
            return _select(results[task], ref.steps)
        return file_path(file_identity(ref))

    # Replacing all arguments at once keeps an object passed as several arguments one object.
    args, kwargs = replace_symbols((tuple(args), dict(kwargs)), replace)
    return func(*args, **kwargs)


def format_user_traceback(error, *runner_files):
    """Return the traceback of an exception without the frames of the runner files that called the task function."""
    frames = error.__traceback__
    while frames is not None and frames.tb_frame.f_code.co_filename in runner_files:
        frames = frames.tb_next
    return "".join(traceback.format_exception(type(error), error, frames))


def run_node():
    """Run the task described by the sandbox manifest and return its result for the library to write."""
    try:
        return _run_node()
    except BaseException as error:
        sys.stdout.write(format_user_traceback(error, __file__))
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(1)


def _run_node():
    with open(MANIFEST, "rb") as stream:
        manifest = cloudpickle.load(stream)
    objects = {}

    def load(entry):
        # An entry is the index of an object serialized inside the manifest, or the sandbox path of a staged file,
        # which the library may hold already.
        if entry not in objects:
            if isinstance(entry, int):
                objects[entry] = cloudpickle.loads(manifest["objects"][entry])
            elif entry in _edata:
                objects[entry] = _edata[entry]
            else:
                with open(entry, "rb") as stream:
                    objects[entry] = cloudpickle.load(stream)
        return objects[entry]

    def load_selected(task, steps):
        # A result read only through a selection is either a file of its own or selected from the whole result.
        path = manifest["selected"][task]
        if path is None:
            return _select(load_result(manifest["inputs"][task]), steps)
        return load_selection(path)

    function = load(manifest["callable"])
    args = tuple(load(task_path) for task_path in manifest["args"])
    kwargs = {name: load(task_path) for name, task_path in manifest["kwargs"].items()}
    output = run_task(function, args, kwargs, lambda task: load_result(manifest["inputs"][task]),
                      manifest["files"].__getitem__, _hoisted.__getitem__, manifest["selected"], load_selected)
    del function, args, kwargs
    objects.clear()
    for steps, path in manifest["selections"]:
        save_selection(path, output, steps)
    return output
