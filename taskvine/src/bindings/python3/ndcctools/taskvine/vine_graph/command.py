# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""The function that runs the command of a Command task at a Worker. It is serialized by value, so a Worker needs no
copy of this module."""

import os
import subprocess
import sys

import cloudpickle


# The most text of a failed command's standard error that its error message shows.
ERROR_TEXT_LIMIT = 4096


def run_command(command, files, results):
    """Run a command in the task sandbox and return its standard output as text. files maps a sandbox path to the
    path of an input file, which is linked there. results maps a sandbox path to a task result, text or bytes, which
    is written there. A command that exits with a non-zero status raises subprocess.CalledProcessError, which
    carries the status and the text of both output streams, and shows the end of the standard error."""
    for name, path in files.items():
        os.makedirs(os.path.dirname(name) or ".", exist_ok=True)
        os.symlink(path, name)
    for name, result in results.items():
        if not isinstance(result, (str, bytes)):
            raise TypeError(f"the command input {name!r} is a task result of type {type(result).__name__}, not text or bytes")
        os.makedirs(os.path.dirname(name) or ".", exist_ok=True)
        with open(name, "wb") as stream:
            stream.write(result.encode() if isinstance(result, str) else result)
    done = subprocess.run(command, shell=isinstance(command, str), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
    output = done.stdout.decode(errors="replace")
    if done.returncode != 0:
        errors = done.stderr.decode(errors="replace")
        error = subprocess.CalledProcessError(done.returncode, command, output, errors)
        if errors.strip():
            error.add_note(errors.strip()[-ERROR_TEXT_LIMIT:])
        raise error
    return output


cloudpickle.register_pickle_by_value(sys.modules[__name__])
