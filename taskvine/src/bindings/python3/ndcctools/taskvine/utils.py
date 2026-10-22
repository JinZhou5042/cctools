# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from . import cvine

import os


def get_c_constant(constant):
    """ Returns a TaskVine C constant value from a string. E.g.:
        "result_success" -> VINE_RESULT_SUCCESS
    """
    constant = f"VINE_{constant.upper()}"
    return getattr(cvine, constant)


def set_port_range(port):
    """ Sets the range for CCTools to look for free ports. """
    if isinstance(port, int):
        low_port = port
        high_port = port
    else:
        try:
            low_port, high_port = port
        except Exception:
            raise ValueError("port should be a single integer, or a sequence of two integers")

    if low_port > high_port:
        raise TypeError("high_port {} cannot be smaller than low_port {}".format(high_port, low_port))
    os.environ["TCP_LOW_PORT"] = str(low_port)
    os.environ["TCP_HIGH_PORT"] = str(high_port)


# helper function that allows a function call to access a variable from a library's state
def load_variable_from_library(var_name):
    return globals()[var_name]


# Notes that a forked function call left for its library process with note_library(), and the function that the
# library's context installed with set_library_note_handler() to receive them in the library process.
library_notes = []
library_note_handler = None


def note_library(note):
    """ Leave a note for the library process that runs this function call. A forked call sends its notes with the
    exit status it reports when that status is zero, and the library passes them to its note handler after it sent
    the call's result. A note is a string of at most 255 characters without whitespace or commas. Notes that do not fit in the
    report are dropped, so a note is a hint that may not arrive. """
    if not isinstance(note, str) or not 0 < len(note) <= 255 or "," in note or any(c.isspace() for c in note):
        raise ValueError("a library note is a string of 1 to 255 characters without whitespace or commas")
    library_notes.append(note)


def set_library_note_handler(handler):
    """ Install, from a library's context function, the function that the library process calls with the list of
    notes of each forked function call (note_library()) after it sent the call's result. Calls forked later inherit
    what it loads, as they inherit the library's context, and it returns true when it changed what they inherit, which
    makes the library fork the next call instead of running it in a process forked before. The handler is not called
    again once it raises or leaves a thread running, because forked calls would inherit that thread's state
    half-copied. """
    global library_note_handler
    library_note_handler = handler


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
