#!/usr/bin/env python3

from pathlib import Path
import subprocess
import sys
import time


SCRIPT_DIR = Path(__file__).resolve().parents[2] / "acceptance" / "scripts"
sys.path.insert(0, str(SCRIPT_DIR))

from benchmark_support import (  # noqa: E402
    BoundedLatencySampler,
    ProcessTreeSampler,
    latency_summary,
)
from compare_architectures import (  # noqa: E402
    benchmark_work,
    expected_result,
    make_payload,
)
from run_architecture_matrix import parse_case  # noqa: E402


def main():
    payload = make_payload(1024)
    assert len(payload) == 1024
    assert benchmark_work(7, payload, 16) == expected_result(7, 1024, 16)
    summary = latency_summary((1, 2, 3, 4, 5))
    assert summary["p50_seconds"] == 3
    assert summary["p95_seconds"] == 4.8

    latencies = BoundedLatencySampler(100, capacity=10)
    now = time.monotonic()
    for ordinal in range(100):
        latencies.submitted(ordinal, ordinal + 1, now)
    for ordinal in range(0, 100, 10):
        latencies.completed(ordinal + 1, now + 0.25)
    sampled = latencies.summary()
    assert sampled["samples"] == 10
    assert sampled["sampling_stride"] == 10
    assert sampled["unresolved_samples"] == 0

    sampler = ProcessTreeSampler(interval_seconds=0.01).start()
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import time; payload=bytearray(8*1024*1024); time.sleep(0.15)",
        ]
    )
    child.wait(timeout=5)
    metrics = sampler.stop()
    assert metrics["samples"] >= 2, metrics
    assert metrics["peak_observed_processes"] >= 2, metrics
    assert metrics["peak_tree_rss_bytes"] > 8 * 1024 * 1024, metrics
    assert metrics["sampling_errors"] == 0, metrics
    assert metrics["peak_rss_bytes_by_role"]["driver"] > 0, metrics

    assert parse_case("128:4096:100:2:4") == {
        "tasks": 128,
        "payload_bytes": 4096,
        "compute_steps": 100,
        "workers": 2,
        "cores": 4,
    }

    print("DataVine benchmark support contract PASS")


if __name__ == "__main__":
    main()
