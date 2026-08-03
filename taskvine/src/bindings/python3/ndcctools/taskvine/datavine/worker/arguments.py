"""Worker-runner command-line parsing."""

import argparse


def make_parser():
    parser = argparse.ArgumentParser(prog="datavine_worker_runner")
    parser.add_argument("--controller", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--task-id", required=True, type=int)
    parser.add_argument("--attempt", default=1, type=int)
    parser.add_argument("--output-file", action="append", default=[])
    parser.add_argument(
        "--pause-after-output-index", default=-1, type=int
    )
    return parser


PARSER = make_parser()


def parse_worker_arguments(argv=None):
    args = PARSER.parse_args(argv)
    if args.attempt < 1:
        raise ValueError("attempt must be positive")
    return args
