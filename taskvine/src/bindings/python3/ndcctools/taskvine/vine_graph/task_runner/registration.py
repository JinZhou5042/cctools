# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os
import shutil
from pathlib import Path

from .execution import run_node


class TaskRunnerRegistration:
    def __init__(self, vine_graph):
        self.vine_graph = vine_graph

        self.name = None
        self.cores = None

        self.task = None
        self.generated_directories = set()

        self.hoisting_modules = [run_node]
        self.env_files = {}

    def set_cores(self, cores):
        self.cores = cores

    def set_name(self, name):
        self.name = name

    def add_hoisting_modules(self, new_modules):
        assert isinstance(new_modules, list), "new_modules must be a list of modules"
        self.hoisting_modules.extend(new_modules)

    def add_env_files(self, new_env_files):
        assert isinstance(new_env_files, dict), "new_env_files must be a dictionary"
        self.env_files.update(new_env_files)

    def install(self):
        assert self.name is not None, "Task runner name must be set before installing (use set_name method)"
        assert self.cores is not None, "Task runner cores must be set before installing (use set_cores method)"

        cache_root = Path(self.vine_graph.cache_directory) / "vine-library-cache"
        before = set(cache_root.iterdir()) if cache_root.exists() else set()
        try:
            self.task = self.vine_graph.create_library_from_functions(
                self.name,
                run_node,
                add_env=False,
                function_infile_load_mode="json",
                hoisting_modules=self.hoisting_modules,
            )
        finally:
            # Library construction is synchronous; retain only directories created by this registration.
            if cache_root.exists():
                self.generated_directories.update(set(cache_root.iterdir()) - before)
        for local, remote in self.env_files.items():
            if not os.path.exists(local):
                raise FileNotFoundError(f"Local file {local} not found")
            self.task.add_input(self.vine_graph.declare_file(local, cache=True, peer_transfer=True), remote)
        self.task.set_cores(self.cores)
        self.task.set_function_slots(self.cores)
        self.vine_graph.install_library(self.task)

    def uninstall(self):
        try:
            if self.name and self.vine_graph.check_library_exists(self.name):
                self.vine_graph.remove_library(self.name)
        finally:
            try:
                if self.task is not None:
                    for file, _ in self.task._tracked_inputs:
                        self.vine_graph.undeclare_file(file)
                    self.task = None
            finally:
                for directory in self.generated_directories:
                    shutil.rmtree(directory)
                self.generated_directories.clear()
