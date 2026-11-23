# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""The store of an Executor: results and files of tasks of cache=True kept in a directory across runs.

A task is known by a key, a SHA-256 digest of what decides its result: its function or command, its arguments
serialized, the keys of the tasks it reads with the selection or file it reads of each, and the path, size, and
modification time of each File it reads, a directory by a walk of its tree. A function that the program defines is
serialized by value, so its code, the values it captures, and the globals it reaches are in the key, and a function of
the program's own modules, which serialization names only, adds the path, size, and modification time of its module
file. A task that reads a task without a key, a URL, or a shared object has no key and runs as usual.

An entry is a directory <store>/<key[:2]>/<key> that holds the result (RESULT), the declared files under files/, and
the selections of the result that readers mount (sel-<digest>). It is written in a directory of its own and renamed
into place, so an entry is complete or absent, and it never changes, except that a selection may be added to it. A
write that a crash interrupted leaves a directory named <key>.tmp-<pid>-<n>, which the next store of a host removes
when that process is gone.
"""

import hashlib
import itertools
import os
import shutil
import sys
import sysconfig
import threading
import types

import cloudpickle

from .graph import URL, Command, File, Shared, _FileRef, _Ref, replace_symbols
from . import library

# What the key covers besides the task: the format of the key, and the Python and cloudpickle that serialize it.
KEY_FORMAT = 1
# The file of an entry that holds the result of its task.
RESULT = "result"
# The directory of an entry that holds the declared files of its task.
FILES = "files"


_PATHS = sysconfig.get_paths()
# The directories of the installed packages, outside which the program's own modules are.
_INSTALLED = tuple(os.path.abspath(_PATHS[kind]) for kind in ("stdlib", "platstdlib", "purelib", "platlib")
                   if kind in _PATHS)


def own_module_file(module_name):
    """Return the file of a module of the program itself, outside the installed packages, or None."""
    module = sys.modules.get(module_name)
    path = getattr(module, "__file__", None)
    if not path or module_name == "__main__":
        return None
    path = os.path.abspath(path)
    if any(os.path.commonpath((path, root)) == root for root in _INSTALLED):
        return None
    return path


def _stat(path):
    info = os.stat(path)
    return (path, info.st_size, info.st_mtime_ns)


def _file_key(path, name):
    """Return what the key takes of a File: its path and name, and the size and modification time of the file, or of
    every entry of a directory's tree."""
    info = os.stat(path)
    if not os.path.isdir(path):
        return ("file", path, name, info.st_size, info.st_mtime_ns)
    tree = []
    for directory, dirs, files in os.walk(path):
        dirs.sort()
        for entry in sorted(files):
            full = os.path.join(directory, entry)
            try:
                entry_info = os.stat(full)
            except OSError:
                continue
            tree.append((os.path.relpath(full, path), entry_info.st_size, entry_info.st_mtime_ns))
    return ("directory", path, name, tuple(tree))


def _module_files(func):
    """Return (path, size, mtime) of the program's own module files that a function's code names, which serialization
    names only: the function's own module and those of the functions, classes, and modules its globals hold."""
    found = {}
    seen = set()

    def visit(value, depth):
        if depth > 8 or id(value) in seen:
            return
        seen.add(id(value))
        if isinstance(value, types.ModuleType):
            path = own_module_file(value.__name__)
            if path:
                found[path] = None
            return
        module = getattr(value, "__module__", None)
        if isinstance(module, str):
            path = own_module_file(module)
            if path:
                found[path] = None
        code = getattr(value, "__code__", None)
        if code is None:
            return
        globals_ = getattr(value, "__globals__", {})
        codes = [code]
        while codes:
            current = codes.pop()
            for name in current.co_names:
                if name in globals_:
                    target = globals_[name]
                    if isinstance(target, (types.FunctionType, types.ModuleType, type)):
                        visit(target, depth + 1)
            codes.extend(const for const in current.co_consts if isinstance(const, types.CodeType))

    visit(func, 0)
    return tuple(_stat(path) for path in sorted(found))


class NotCacheable(Exception):
    """A task of cache=True has no key, for the reason the message gives, and runs as usual."""


class Store:
    """A store directory, which one Executor reads and writes. Keys are made in the thread that submits; entries are
    written by a thread of the store, so a miss costs the run nothing while its outputs are copied."""

    def __init__(self, path):
        self.path = os.path.abspath(os.path.expanduser(os.fspath(path)))
        os.makedirs(self.path, exist_ok=True)
        self.hits = 0
        self.stored = 0
        self.not_cacheable = 0
        self._noticed = set()
        self._numbers = itertools.count(1)
        self._lock = threading.Lock()
        self._remove_interrupted()

    def _remove_interrupted(self):
        """Remove the directories of writes whose process is gone."""
        try:
            prefixes = os.listdir(self.path)
        except OSError:
            return
        for prefix in prefixes:
            directory = os.path.join(self.path, prefix)
            if len(prefix) != 2 or not os.path.isdir(directory):
                continue
            for name in os.listdir(directory):
                head, mark, tail = name.partition(".tmp-")
                if not mark:
                    continue
                try:
                    pid = int(tail.split("-", 1)[0])
                    os.kill(pid, 0)
                    continue
                except (ValueError, ProcessLookupError):
                    pass
                except PermissionError:
                    continue
                shutil.rmtree(os.path.join(directory, name), ignore_errors=True)

    def entry(self, key):
        return os.path.join(self.path, key[:2], key)

    # Keys.

    def key(self, task, parents):
        """Return the key of a task, given {key of a task: record} of the tasks it reads, or raise NotCacheable."""
        func = task._func
        digest = hashlib.sha256()
        digest.update(repr((KEY_FORMAT, sys.version_info[:2], cloudpickle.__version__)).encode())
        if isinstance(task, Command):
            digest.update(repr(("command", func, task.outputs)).encode())
        else:
            digest.update(cloudpickle.dumps(func))
            digest.update(repr(("function", task.outputs, _module_files(func))).encode())

        def replace(ref):
            kind = type(ref)
            if kind is _Ref:
                record = parents[ref.key]
                if record.store is None:
                    raise NotCacheable(f"it reads {record.label}, whose function is not cache=True")
                return ("task", record.store[0], ref.steps)
            if kind is _FileRef:
                record = parents[ref.key]
                if record.store is None:
                    raise NotCacheable(f"it reads a file of {record.label}, whose function is not cache=True")
                return ("task file", record.store[0], ref.path)
            if kind is File:
                return _file_key(ref.path, ref.name)
            if kind is URL:
                raise NotCacheable("it reads a URL, whose content may change")
            if kind is Shared:
                raise NotCacheable("it reads a shared object")
            raise NotCacheable(f"it reads {ref!r}")

        arguments = replace_symbols((tuple(task._args), dict(task._kwargs)), replace)
        try:
            digest.update(cloudpickle.dumps(arguments))
        except Exception as error:
            raise NotCacheable(f"its arguments cannot be serialized: {error}") from None
        return digest.hexdigest()

    def notice(self, task, reason):
        """Count a task of cache=True without a key, and say why once per function or command line."""
        func = task._func
        with self._lock:
            self.not_cacheable += 1
            name = func if type(func) is str else getattr(func, "__name__", None)
            if name in self._noticed:
                return
            self._noticed.add(name)
        print(f"vine_graph: tasks of {name} are not cached: {reason}", file=sys.stderr, flush=True)

    # Entries.

    def lookup(self, key, outputs):
        """Return the directory of the complete entry of a key that holds every declared file, or None."""
        directory = self.entry(key)
        if not os.path.exists(os.path.join(directory, RESULT)):
            return None
        for path in outputs:
            if not os.path.lexists(os.path.join(directory, FILES, path)):
                return None
        with self._lock:
            self.hits += 1
        return directory

    @staticmethod
    def selection_name(chain):
        """Return the name in an entry of the file of a selection of the result."""
        return "sel-" + hashlib.sha256(repr(chain).encode()).hexdigest()[:32]

    def add_selections(self, directory, selections):
        """Write the selections of a hit's result that its readers mount and the entry lacks, from the stored result.
        selections is {selection key: (steps, sandbox path)}. Another process that adds one first wins, since both
        write the same bytes."""
        result = None
        for chain, (steps, _) in selections.items():
            path = os.path.join(directory, self.selection_name(chain))
            if os.path.exists(path):
                continue
            if result is None:
                result = library.load_result(os.path.join(directory, RESULT))
            temporary = f"{path}.tmp-{os.getpid()}-{next(self._numbers)}"
            library.save_selection(temporary, result, steps)
            os.rename(temporary, path)

    def write(self, key, items):
        """Write an entry from items, (name in the entry, bytes or a local path), and rename it into place. An entry
        that another process wrote first is kept."""
        final = self.entry(key)
        os.makedirs(os.path.dirname(final), exist_ok=True)
        temporary = f"{final}.tmp-{os.getpid()}-{next(self._numbers)}"
        try:
            os.makedirs(temporary)
            for name, source in items:
                target = os.path.join(temporary, name)
                os.makedirs(os.path.dirname(target), exist_ok=True)
                if isinstance(source, (bytes, bytearray, memoryview)):
                    with open(target, "wb") as stream:
                        stream.write(source)
                elif os.path.isdir(source):
                    shutil.copytree(source, target, symlinks=False)
                else:
                    shutil.copyfile(source, target)
            try:
                os.rename(temporary, final)
            except OSError:
                # Another Executor stored the entry first, or the entry exists and gains files only as selections.
                shutil.rmtree(temporary, ignore_errors=True)
                return False
        except BaseException:
            shutil.rmtree(temporary, ignore_errors=True)
            raise
        with self._lock:
            self.stored += 1
        return True

    def summary(self):
        return f"vine_graph: store {self.path}: {self.hits} hits, {self.stored} stored, {self.not_cacheable} not cacheable"
