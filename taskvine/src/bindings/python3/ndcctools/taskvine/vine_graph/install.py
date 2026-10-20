# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Install the library of an Executor on its Manager, from the process that runs the Executor.

installed() declares the library that library.py defines, with the modules, files, hoisted objects, and cached edata
it loads when it starts, and removes the library and the code the Manager generated for it when the Executor closes.
"""

import contextlib
import os
import shutil
import types
import uuid

import cloudpickle

from .library import EDATA_CACHE, HOISTED, prepare, run_node, runtime_identity


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
    inputs = []
    installed_library = False
    generated = None
    try:
        context = [prepare, [module_names, HOISTED if hoisted else None, EDATA_CACHE if edata_cache else None,
                             runtime_identity()], {}]
        try:
            library = manager.create_library_from_functions(name, run_node, add_env=False, library_context_info=context)
        except BaseException:
            generated = generated_directory(manager, name, context)
            raise
        # The Manager generates the library's code in a directory of the shared library cache that is named by a hash
        # of the library, and the name is unique, so the directory of the code belongs to this library alone. Other
        # Executors that use the same cache directory, in this process or another, keep theirs.
        generated = os.path.dirname(library._library_files[0].source())
        for local, remote in files.items():
            inputs.append(manager.declare_file(local, cache=True, peer_transfer=True))
            library.add_input(inputs[-1], remote)
        # Without a resource request, each instance takes its whole Worker with one function slot per core.
        manager.install_library(library)
        installed_library = True
        yield run_node.__name__
    finally:
        try:
            # A library that failed too often was already removed by the Manager, and is removed here all the same.
            if installed_library:
                manager.remove_library(name)
        finally:
            # Dropping the library task undeclares the code that the Manager generated for it.
            library = None
            try:
                for file in inputs:
                    manager.undeclare_file(file)
            finally:
                # Only a directory of the library cache is removed, whatever the Manager's files turn out to be.
                if generated and os.path.basename(os.path.dirname(generated)) == "vine-library-cache":
                    shutil.rmtree(generated, ignore_errors=True)


def generated_directory(manager, name, context):
    """Return the directory of the library cache where the Manager generates the code of the library named name, as
    create_library_from_functions() names it, for a construction that failed before it declared the code."""
    from ndcctools.poncho import package_serverize

    library_hash = package_serverize.generate_library_hash(library_name=name, function_list=[run_node], poncho_env=None,
                                                           init_command=None, add_env=False, exec_mode="fork",
                                                           hoisting_modules=None, library_context_info=context)
    return os.path.join(manager.cache_directory, "vine-library-cache", library_hash)


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
