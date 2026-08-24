#!/usr/bin/env python3
"""Fast contract gate for the million-task, ten-million-file workload."""

import collections
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    repository = Path(__file__).resolve().parents[2]
    scripts = repository / "acceptance" / "scripts"
    sys.path.insert(0, str(scripts))
    from data_intensive_workload import (  # pylint: disable=import-outside-toplevel
        B_REUSE,
        Workload,
        assert_full_contract,
        stage_source,
    )
    from generate_data_intensive_dataset import (  # pylint: disable=import-outside-toplevel
        content,
        write_source,
    )

    full = Workload()
    contract = assert_full_contract(full)
    counts = contract["counts"]
    sizes = contract["bytes"]
    assert counts == {
        "a_tasks": 262_144,
        "b_tasks": 655_360,
        "c_tasks": 131_072,
        "tasks": 1_048_576,
        "source_files": 9_437_184,
        "output_files": 1_048_576,
        "workflow_files": 10_485_760,
        "code_objects": 3,
        "data_records": 10_485_763,
        "external_input_references": 9_437_184,
        "scheduler_edges": 5_898_240,
        "input_references": 15_335_424,
    }, counts
    assert sizes["source"] == 765_393_371_136, sizes
    assert sizes["a_outputs"] == 137_438_953_472, sizes
    assert sizes["b_outputs"] == 171_798_691_840, sizes
    assert sizes["c_outputs"] == 68_719_476_736, sizes
    assert sizes["stored_artifacts"] == 1_143_350_493_184, sizes
    assert sizes["stored_artifacts"] < sizes["limit"]

    # One smallest cohort has exactly the same regular graph invariants.
    small = Workload(cohorts=1, scale=1, size_profile="tiny")
    a_degree = collections.Counter()
    for b_global in range(small.b_tasks):
        inputs = small.b_input_data_ids(b_global)
        assert len(inputs) == len(set(inputs)) == 8
        a_degree.update(inputs)
    assert set(a_degree.values()) == {B_REUSE}, a_degree
    b_degree = collections.Counter()
    for c_global in range(small.c_tasks):
        inputs = small.c_input_data_ids(c_global)
        assert len(inputs) == len(set(inputs)) == 5
        b_degree.update(inputs)
    assert len(b_degree) == small.b_tasks
    assert set(b_degree.values()) == {1}, b_degree

    # Exercise the exact worker payload locally: random pread order, real CPU,
    # deterministic fixed-size output, and no sleep-based simulation.
    with tempfile.TemporaryDirectory(prefix="datavine-kernel-") as temporary:
        root = Path(temporary)
        inputs = []
        for data_id, size in ((41, 65_537), (42, 17_123)):
            path = root / str(data_id)
            path.write_bytes(bytes((index + data_id) % 251 for index in range(size)))
            inputs.append((data_id, path))
        payload = root / "kernel.py"
        payload.write_text(stage_source("A", 32_768))
        environment = dict(os.environ)
        for data_id, path in inputs:
            environment[f"DATAVINE_DATA_{data_id}"] = str(path)
        first = subprocess.run(
            (sys.executable, payload), cwd=root, env=environment,
            check=True, text=True, stdout=subprocess.PIPE,
        )
        metrics = json.loads(first.stdout)
        assert metrics["inputs"] == 2
        assert metrics["read_bytes"] == 82_660
        assert 2 <= metrics["cpu_ms"] <= 5_000
        first_output = (root / "datavine-python-output-0").read_bytes()
        assert len(first_output) == 32_768
        (root / "datavine-python-output-0").unlink()
        subprocess.run(
            (sys.executable, payload), cwd=root, env=environment,
            check=True, text=True, stdout=subprocess.PIPE,
        )
        assert (root / "datavine-python-output-0").read_bytes() == first_output
        assert "sleep" not in payload.read_text()

    generator = scripts / "generate_data_intensive_dataset.py"
    with tempfile.TemporaryDirectory(prefix="datavine-data-intensive-") as temporary:
        root = Path(temporary)
        common = (
            sys.executable,
            generator,
            "--cohorts", "1",
            "--scale", "1",
            "--size-profile", "tiny",
        )
        for part in range(2):
            subprocess.run(
                common + ("generate-part", "--root", root, "--part", str(part), "--parts", "2"),
                check=True,
                stdout=subprocess.PIPE,
                text=True,
            )
        # A restarted part verifies and reuses completed files rather than
        # overwriting them. A partially written temporary file resumes only
        # after its deterministic prefix has been checked byte-for-byte.
        (root / "_parts" / "part-000.json").unlink()
        subprocess.run(
            common + (
                "generate-part", "--root", root, "--part", "0",
                "--parts", "2", "--resume",
            ),
            check=True, stdout=subprocess.PIPE, text=True,
        )
        partial = root / "resume-direct.bin"
        partial.parent.mkdir(parents=True, exist_ok=True)
        partial.with_name(partial.name + ".part").write_bytes(
            content(99, 0, 16) + content(99, 16, 16)
        )
        digest, _ = write_source(partial, 99, 64, 16, resume=True)
        assert partial.read_bytes() == b"".join(
            content(99, offset, min(16, 64 - offset))
            for offset in range(0, 64, 16)
        )
        assert len(digest) == 32
        verification_root = root / "full-hash-parts"
        for part in range(2):
            subprocess.run(
                common + (
                    "verify-part", "--root", root, "--part", str(part),
                    "--parts", "2", "--full-hash", "--output",
                    verification_root / f"part-{part:03d}.json",
                ),
                check=True, stdout=subprocess.PIPE, text=True,
            )
        completed = subprocess.run(
            common + (
                "assemble", "--root", root, "--parts", "2", "--full-hash",
                "--verification-root", verification_root,
            ),
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        )
        manifest = json.loads(completed.stdout)
        assert manifest["status"] == "PASS", manifest
        assert all(manifest["gates"].values()), manifest
        assert manifest["source_files"] == 2_304, manifest
        assert manifest["verification_mode"] == "part-artifacts", manifest
        assert not list(root.rglob("*.part"))

        # A verifier artifact is accepted only while its own digest and its
        # binding to the current generator part manifest both remain intact.
        verification = verification_root / "part-000.json"
        altered = json.loads(verification.read_text())
        altered["logical_bytes"] += 1
        verification.write_text(json.dumps(altered))
        rejected = subprocess.run(
            common + (
                "assemble", "--root", root, "--parts", "2", "--full-hash",
                "--verification-root", verification_root,
            ),
            stdout=subprocess.PIPE, text=True,
        )
        assert rejected.returncode == 1, rejected
        assert "verification artifact digest mismatch" in rejected.stdout

        corrupt = root / small.source_path(0)
        with corrupt.open("r+b") as stream:
            original = stream.read(1)
            stream.seek(0)
            stream.write(bytes((original[0] ^ 0xff,)))
        failed = subprocess.run(
            common + ("verify-part", "--root", root, "--part", "0", "--parts", "2", "--full-hash"),
            stdout=subprocess.PIPE,
            text=True,
        )
        assert failed.returncode == 1, failed
        assert json.loads(failed.stdout)["status"] == "FAIL"

    print(
        "DataVine data-intensive workload PASS "
        "tasks=1048576 files=10485760 data_records=10485763 "
        "stored_bytes=1143350493184 workers=128 cores=16 "
        "a_degree=20 b_degree=1 generator=deterministic corruption=fail-closed"
    )


if __name__ == "__main__":
    main()
