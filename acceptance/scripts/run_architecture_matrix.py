#!/usr/bin/env python3
"""Run a bounded set of architecture benchmark cases reproducibly."""

import argparse
import json
from pathlib import Path
import subprocess
import sys


SCRIPT = Path(__file__).with_name("compare_architectures.py")
DEFAULT_CASES = (
    "128:0:0:1:4",
    "1024:0:0:1:4",
    "256:65536:0:1:4",
    "256:4096:10000:1:4",
)


def parse_case(value):
    pieces = value.split(":")
    if len(pieces) != 5:
        raise argparse.ArgumentTypeError(
            "case must be tasks:payload_bytes:compute_steps:workers:cores"
        )
    try:
        tasks, payload_bytes, compute_steps, workers, cores = (
            int(piece) for piece in pieces
        )
    except ValueError as error:
        raise argparse.ArgumentTypeError("case values must be integers") from error
    if min(tasks, workers, cores) < 1 or min(payload_bytes, compute_steps) < 0:
        raise argparse.ArgumentTypeError("case contains invalid values")
    return {
        "tasks": tasks,
        "payload_bytes": payload_bytes,
        "compute_steps": compute_steps,
        "workers": workers,
        "cores": cores,
    }


def case_name(case):
    return (
        f"t{case['tasks']}-p{case['payload_bytes']}-"
        f"c{case['compute_steps']}-w{case['workers']}-k{case['cores']}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--case",
        action="append",
        type=parse_case,
        dest="cases",
        help="tasks:payload_bytes:compute_steps:workers:cores",
    )
    parser.add_argument(
        "--modes",
        default="datavine,functioncall-direct,functioncall-fork,pythontask",
    )
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--minimum-datavine-ratio", type=float, default=1.0)
    parser.add_argument("--library-batch-size", type=int, default=4096)
    parser.add_argument("--process-sample-interval", type=float, default=0.1)
    args = parser.parse_args()
    if args.repetitions < 1 or args.library_batch_size < 1:
        parser.error("repetitions and library batch size must be positive")
    cases = args.cases or [parse_case(value) for value in DEFAULT_CASES]

    results = []
    for case in cases:
        command = [
            sys.executable,
            str(SCRIPT),
            "--tasks",
            str(case["tasks"]),
            "--payload-bytes",
            str(case["payload_bytes"]),
            "--compute-steps",
            str(case["compute_steps"]),
            "--workers",
            str(case["workers"]),
            "--cores",
            str(case["cores"]),
            "--modes",
            args.modes,
            "--repetitions",
            str(args.repetitions),
            "--minimum-datavine-ratio",
            str(args.minimum_datavine_ratio),
            "--library-batch-size",
            str(args.library_batch_size),
            "--process-sample-interval",
            str(args.process_sample_interval),
        ]
        completed = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            report = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise RuntimeError(
                f"benchmark case {case_name(case)} produced invalid JSON: "
                f"{completed.stderr}"
            ) from error
        results.append(
            {
                "case": case,
                "name": case_name(case),
                "returncode": completed.returncode,
                "report": report,
                "stderr": completed.stderr,
            }
        )

    failed = [
        result
        for result in results
        if result["returncode"] != 0
        or result["report"]["summary"]["status"] != "PASS"
    ]
    report = {
        "benchmark": "datavine-architecture-workload-matrix-v1",
        "status": "PASS" if not failed else "FAIL",
        "case_count": len(results),
        "passed_count": len(results) - len(failed),
        "configuration": {
            "modes": args.modes.split(","),
            "repetitions": args.repetitions,
            "minimum_datavine_ratio": args.minimum_datavine_ratio,
            "library_batch_size": args.library_batch_size,
        },
        "cases": results,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
