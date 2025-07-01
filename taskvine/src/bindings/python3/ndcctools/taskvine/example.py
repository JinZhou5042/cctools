#!/usr/bin/env python3

from ndcctools.taskvine.graph_manager import GraphManager

from dask.base import collections_to_dsk
import cloudpickle
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-file", type=str, default="tasks.pkl")
    parser.add_argument("--libcores", type=int, default=16)
    parser.add_argument("--expand-dsk", action="store_true", default=False)
    args = parser.parse_args()

    with open(args.task_file, "rb") as f:
        tasks = cloudpickle.load(f)
    task_list = list(tasks.values())
    task_dict = collections_to_dsk(task_list)

    m = GraphManager(
        9124,
        name="graph-optimization",
        run_info_path="/users/jzhou24/afs/taskvine-report-tool/logs",
        run_info_template="graph-test-3",
    )
    m.execute(task_dict, expand_dsk=args.expand_dsk, libcores=args.libcores)

if __name__ == "__main__":
    main()
