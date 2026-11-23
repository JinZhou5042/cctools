#!/usr/bin/env python3

# Copyright (C) 2026 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# A short tour of Vine Graph that checks its own results, also useful to check an installation.
#
#   python3 vine_example_graph.py --local        runs every task in this process
#   python3 vine_example_graph.py --port 9200    starts a manager; then run: vine_worker localhost 9200

import argparse
import os
import tempfile
from pathlib import Path

import ndcctools.taskvine.vine_graph as vg


def load(path):
    return [int(word) for word in Path(path).read_text().split()]


def stats(data):
    return {"count": len(data), "mean": sum(data) / len(data)}


def write_squares(data):
    Path("squares.txt").write_text("".join(f"{value * value}\n" for value in data))


def count_lines(path):
    return len(Path(path).read_text().splitlines())


def per_value(data):
    # An Expand's builder: the tasks to run follow from a result.
    return [vg.Function(abs, value) for value in data]


def divide(a, b):
    return a / b


def main():
    parser = argparse.ArgumentParser(description="Vine Graph example.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--local", action="store_true", help="run the tasks in this process, without workers")
    mode.add_argument("--port", type=int, help="run a manager on this port for workers")
    args = parser.parse_args()

    data_path = Path(tempfile.mkdtemp(prefix="vine-example-graph-")) / "data.txt"
    data_path.write_text("3 -1 4 -1 5 -9 2 6\n")

    data_task = vg.Function(load, vg.File(str(data_path)))
    stats_task = vg.Function(stats, data_task)
    squares_task = vg.Function(write_squares, data_task).set(outputs=["squares.txt"])
    lines_task = vg.Function(count_lines, squares_task.file("squares.txt"))
    note_task = vg.Command("echo mean={m} | tee note.txt", m=stats_task["mean"]).set(outputs=["note.txt"])
    abs_task = vg.Expand(per_value, data_task)
    total_task = vg.Function(sum, abs_task)
    bad_task = vg.Function(divide, 1, 0)

    if args.local:
        executor = vg.Executor(local=True)
    else:
        executor = vg.Executor(port=args.port)
        print(f"manager listening on port {executor.port}; start a worker with: vine_worker localhost {executor.port}")

    with executor as ex:
        # run() starts the tasks and the tasks they read, and waits.
        ex.run([lines_task, note_task, total_task])
        assert stats_task.result() == {"count": 8, "mean": 1.125}
        assert lines_task.result() == 8
        assert note_task.result() == "mean=1.125\n"
        assert note_task.file("note.txt").text == "mean=1.125\n"
        assert squares_task.file("squares.txt").text.split()[:3] == ["9", "1", "16"]
        assert abs_task.result() == [3, 1, 4, 1, 5, 9, 2, 6]
        assert total_task.result() == 31

        # submit() returns at once, and as_completed() yields tasks as they finish.
        scaled_tasks = ex.submit([vg.Function(divide, total_task, k) for k in (1, 2, 4)])
        scaled = sorted(task.result() for task in ex.as_completed(scaled_tasks, timeout=300))
        assert scaled == [7.75, 15.5, 31.0]

        # A failure is a vg.TaskError, raised by run() and by result().
        try:
            ex.run(bad_task)
            raise AssertionError("bad_task should have failed")
        except vg.TaskError as error:
            assert error.reason == "function"
            assert isinstance(error.exception, ZeroDivisionError)
        assert bad_task.failed()

    os.unlink(data_path)
    os.rmdir(data_path.parent)
    print("vine_graph example: OK")


if __name__ == "__main__":
    main()

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
