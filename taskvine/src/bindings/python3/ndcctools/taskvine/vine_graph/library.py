# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""The TaskVine library that runs the tasks of an Executor on Workers.

An Executor installs one library with installed() and removes it when it closes. Every task calls run_node(), the
library's only function, without arguments. The task sandbox holds a manifest at MANIFEST that names the files of the
function and its arguments by sandbox path, and the sandbox paths of the results and files the task reads. run_node()
returns the function's result, which the TaskVine library writes to RESULT as {"Result", "Success", "Reason"}. A
function that raises ends the task process with a non-zero status after printing its traceback, because the library
otherwise reports exceptions with status zero. Local execution calls run_task() too, so both run a function alike.
"""

import contextlib
import os
import shutil
import sys
import traceback
import uuid
from pathlib import Path

import cloudpickle

from .graph import File, _Attribute, _Ref, replace_symbols

# Sandbox path of the manifest that describes the task.
MANIFEST = "vine-graph-node.pkl"
# Sandbox path where the TaskVine library writes the result of a function call.
RESULT = "outfile"


@contextlib.contextmanager
def installed(manager, modules=(), files=None):
    """Install the library on a Manager, yield its name and the name of the function every task calls, and remove
    the library with its files on exit. The library imports modules when it starts, and files maps local paths to
    names in the library sandbox. The name is unique, so an Executor never reaches a library instance of an earlier
    Executor that Workers have not removed yet."""
    name = f"vine-graph-{uuid.uuid4().hex}"
    files = files or {}
    for local in files:
        if not os.path.exists(local):
            raise FileNotFoundError(f"Local file {local} not found")
    cache_root = Path(manager.cache_directory) / "vine-library-cache"
    before = set(cache_root.iterdir()) if cache_root.exists() else set()
    inputs = []
    try:
        try:
            library = manager.create_library_from_functions(name, run_node, add_env=False, hoisting_modules=list(modules))
        finally:
            # Library construction is synchronous, so new cache directories belong to this library.
            generated = set(cache_root.iterdir()) - before if cache_root.exists() else set()
        for local, remote in files.items():
            inputs.append(manager.declare_file(local, cache=True, peer_transfer=True))
            library.add_input(inputs[-1], remote)
        # Without a resource request, each instance takes its whole Worker with one function slot per core.
        manager.install_library(library)
        yield name, run_node.__name__
    finally:
        try:
            # Removing the library also undeclares the code that the Manager generated for it.
            if manager.check_library_exists(name):
                manager.remove_library(name)
        finally:
            try:
                for file in inputs:
                    manager.undeclare_file(file)
            finally:
                for directory in generated:
                    shutil.rmtree(directory)


def save_result(path, value):
    """Write a task result in the format of the TaskVine library."""
    with open(path, "wb") as stream:
        cloudpickle.dump({"Result": value, "Success": True, "Reason": None}, stream)


def load_result(path):
    """Read a task result written by the TaskVine library or save_result()."""
    with open(path, "rb") as stream:
        return cloudpickle.load(stream)["Result"]


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


def run_task(func, args, kwargs, load_task_result, file_path):
    """Call func after replacing each reference in its arguments: a task result with load_task_result((graph id,
    key)) and a file with file_path(file_identity(file)). Other arguments reach func unchanged."""
    results = {}

    def replace(ref):
        if isinstance(ref, _Ref):
            task = (ref.graph_id, ref.key)
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

    def load(task_path):
        if task_path not in objects:
            with open(task_path, "rb") as stream:
                objects[task_path] = cloudpickle.load(stream)
        return objects[task_path]

    function = load(manifest["callable"])
    args = tuple(load(task_path) for task_path in manifest["args"])
    kwargs = {name: load(task_path) for name, task_path in manifest["kwargs"].items()}
    output = run_task(function, args, kwargs, lambda task: load_result(manifest["inputs"][task]),
                      manifest["files"].__getitem__)
    del function, args, kwargs
    objects.clear()
    return output
