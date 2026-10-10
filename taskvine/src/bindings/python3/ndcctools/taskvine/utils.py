# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from . import cvine

import contextlib
import os


def get_c_constant(constant):
    """ Returns a TaskVine C constant value from a string. E.g.:
        "result_success" -> VINE_RESULT_SUCCESS
    """
    constant = f"VINE_{constant.upper()}"
    return getattr(cvine, constant)


@contextlib.contextmanager
def port_range(port):
    """ Set the range in which CCTools looks for free ports while the block runs, and restore the previous range
    afterwards, so that processes started later choose their own ports. """
    try:
        low_port, high_port = port
    except Exception:
        raise ValueError("port should be a single integer, or a sequence of two integers")
    if low_port > high_port:
        raise TypeError("high_port {} cannot be smaller than low_port {}".format(high_port, low_port))
    previous = {name: os.environ.get(name) for name in ("TCP_LOW_PORT", "TCP_HIGH_PORT")}
    os.environ["TCP_LOW_PORT"] = str(low_port)
    os.environ["TCP_HIGH_PORT"] = str(high_port)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


# helper function that allows a function call to access a variable from a library's state
def load_variable_from_library(var_name):
    return globals()[var_name]


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
