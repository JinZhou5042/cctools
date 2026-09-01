#!/usr/bin/env python3
"""Executable local gate for the scientific benchmark foundation."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def run(command, *, environment=None):
    completed = subprocess.run(
        tuple(str(item) for item in command),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
    )
    if completed.returncode:
        raise RuntimeError({
            "command": tuple(str(item) for item in command),
            "returncode": completed.returncode,
            "stdout": completed.stdout[-8000:],
            "stderr": completed.stderr[-8000:],
        })
    return completed.stdout


def main():
    repository = Path(__file__).resolve().parents[2]
    scripts = repository / "acceptance/scripts"
    generator = scripts / "generate_scientific_data.py"
    sampler = scripts / "sample_remote_resources.py"
    comparison = scripts / "compare_scientific_workflows.py"
    schema = repository / "acceptance/scientific-workflows/schema.json"
    json.loads(schema.read_text())
    environment = dict(os.environ, PYTHONNOUSERSITE="1")
    with tempfile.TemporaryDirectory(
            prefix="datavine-scientific-foundation-") as temporary:
        root = Path(temporary)
        datasets = []
        for name in ("first", "second"):
            output = root / name
            run((
                sys.executable, generator, "generate",
                "--output-dir", output,
                "--shards", 4,
                "--shard-bytes", 256 * 1024,
                "--chunk-bytes", 64 * 1024,
                "--seed", 20260817,
                "--format", "cloudpickle-bytes",
                "--no-fsync",
            ), environment=environment)
            verification = json.loads(run((
                sys.executable, generator, "verify", output,
            ), environment=environment))
            assert verification["status"] == "PASS", verification
            datasets.append(json.loads((output / "manifest.json").read_text()))
        assert datasets[0]["dataset_sha256"] == datasets[1]["dataset_sha256"]
        assert [item["sha256"] for item in datasets[0]["files"]] == [
            item["sha256"] for item in datasets[1]["files"]
        ]
        assert all(
            item["allocated_bytes"] >= item["stored_bytes"]
            for item in datasets[0]["files"]
        )

        corrupt = root / "second" / datasets[1]["files"][0]["path"]
        with corrupt.open("r+b") as stream:
            stream.seek(17)
            original = stream.read(1)
            stream.seek(17)
            stream.write(bytes((original[0] ^ 0xff,)))
        failed = subprocess.run(
            (sys.executable, str(generator), "verify", str(root / "second")),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
        )
        assert failed.returncode == 1, failed
        assert json.loads(failed.stdout)["status"] == "FAIL"

        calibration_path = root / "calibration.json"
        run((
            sys.executable, sampler, "calibrate",
            "--bytes", 2 * 1024 * 1024,
            "--output", calibration_path,
        ), environment=environment)
        calibration = json.loads(calibration_path.read_text())
        assert calibration["status"] == "PASS", calibration
        assert all(calibration["gates"].values()), calibration

        output = root / "run"
        run((
            sys.executable, comparison,
            "--manifest", root / "first",
            "--sampler-calibration", calibration_path,
            "--output-dir", output,
            "--contracts", "TV-native,DV-native,TV-durable-sink",
            "--workers", 1,
            "--cores", 1,
            "--batch-type", "local",
            "--fan-in", 2,
            "--histogram-bins", 64,
        ), environment=environment)
        artifact = json.loads((output / "summary.json").read_text())
        assert artifact["status"] == "PASS", artifact
        assert artifact["scope"] == "local-pilot"
        assert artifact["contracts"] == [
            "TV-native", "DV-native", "TV-durable-sink"
        ]
        assert len(artifact["runs"]) == 3
        assert all(artifact["gates"].values()), artifact["gates"]
        assert {run["counts"]["logical"] for run in artifact["runs"]} == {11}
        assert len({run["results"]["digest"] for run in artifact["runs"]}) == 1
        assert not list(output.rglob("*.part"))
    print(
        "DataVine scientific workflow foundation PASS "
        "schema=1 deterministic-generator=1 nonsparse=1 corruption=fail-closed "
        "resource-calibration=1 HEP-S-contracts=3 exact-counts=11/11 cleanup=1"
    )


if __name__ == "__main__":
    main()
