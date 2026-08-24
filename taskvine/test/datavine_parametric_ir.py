#!/usr/bin/env python3

import base64
import collections
import hashlib
import json
from pathlib import Path
import random
import struct
import subprocess
import sys
import tempfile

repository = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repository / "acceptance/scripts"))
from data_intensive_parametric_ir import ParametricWorkload
from data_intensive_workload import (
    SIZE_PROFILES, Workload, canonical_json, full_workload, stage_source,
)


native_profile = repository / "taskvine/src/tools/datavine_parametric_test"
native_validator = repository / "taskvine/src/tools/datavine_workflow"


def check(workload):
    family = ParametricWorkload(workload, Path("/dataset"))
    document = family.validate()
    samples = {1, workload.a_tasks, workload.a_tasks + 1,
               workload.a_tasks + workload.b_tasks, workload.tasks}
    generator = random.Random(20260824)
    samples.update(generator.randrange(1, workload.tasks + 1) for _ in range(4096))
    for task_id in samples:
        task = family.task(task_id)
        if task["stage"] == "A":
            local = task_id - 1
            assert task["inputs"] == workload.a_input_data_ids(local)
            for data_id in task["inputs"][:2]:
                assert family.source(data_id).endswith(str(
                    workload.source_path((local * 36) + (data_id - task["inputs"][0]))))
        elif task["stage"] == "B":
            local = task_id - workload.a_tasks - 1
            assert task["inputs"] == workload.b_input_data_ids(local)
        else:
            local = task_id - workload.a_tasks - workload.b_tasks - 1
            assert task["inputs"] == workload.c_input_data_ids(local)
    return document


def runtime_document(workload, dataset=Path("/dataset")):
    profile = SIZE_PROFILES[workload.size_profile]
    payloads = [
        stage_source(stage, profile[f"{stage.lower()}_output"]).encode()
        for stage in "ABC"
    ]
    return {
        "schema": "datavine.workflow/v1",
        "workflow_id": "parametric-native-test",
        "idempotency_key": "parametric-native-test-v1",
        "mode": "sealed",
        "tasks": [],
        "data": [
            {
                "data_id": index + 1,
                "codec": {"name": "bytes", "version": "1"},
                "origin": {
                    "kind": "inline",
                    "base64": base64.b64encode(payload).decode(),
                },
            }
            for index, payload in enumerate(payloads)
        ],
        "requested_outputs": [],
        "policy": {
            "maximum_tasks": workload.tasks,
            "maximum_edges": workload.scheduler_edges,
        },
        "parametric": {
            "kind": "data-intensive-v1",
            "seed": 20260823,
            "dataset_root": dataset.resolve().as_uri(),
            "cohorts": workload.cohorts,
            "scale": workload.scale,
            "size_profile": workload.size_profile,
            "contract_sha256": workload.contract()["contract_sha256"],
        },
    }


def topology_digest(workload):
    digest = hashlib.sha256()
    family = ParametricWorkload(workload, Path("/dataset"))
    for task_id in range(1, workload.tasks + 1):
        task = family.task(task_id)
        stage = "ABC".index(task["stage"]) + 1
        digest.update(struct.pack(">QBQ", task_id, stage, len(task["inputs"])))
        for value in task["inputs"]:
            digest.update(struct.pack(">Q", value))
        digest.update(struct.pack(">Q", task["output"]))
    return digest.hexdigest()


def run_native(mode, path, *arguments):
    completed = subprocess.run(
        (str(native_profile), mode, str(path), *map(str, arguments)),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


small = Workload(1, 1, "tiny")
small_family = ParametricWorkload(small, Path("/dataset"))
a_inverse = collections.Counter()
for a_global in range(small.a_tasks):
    data_id = small.a_data_id(a_global)
    consumers = small_family.consumers(data_id)
    assert len(consumers) == 20 == len(set(consumers))
    for task_id in consumers:
        assert data_id in small_family.task(task_id)["inputs"]
        a_inverse[task_id] += 1
assert set(a_inverse.values()) == {8}
for b_global in range(small.b_tasks):
    data_id = small.b_data_first + b_global
    consumers = small_family.consumers(data_id)
    assert len(consumers) == 1
    assert data_id in small_family.task(consumers[0])["inputs"]

check(small)
full = full_workload()
document = check(full)
payload = canonical_json(document)
# Even an impossible lower bound of eight bytes per expanded task/data/input
# entry proves the registration reduction without allocating that graph.
explicit_lower_bound = 8 * (full.tasks + full.data_records + full.input_references)
assert len(payload) < 16_384
assert explicit_lower_bound / len(payload) >= 100
assert document["counts"]["tasks"] == 1_048_576
assert document["counts"]["workflow_files"] == 10_485_760

with tempfile.TemporaryDirectory(prefix="datavine-parametric-native-") as root:
    root = Path(root)
    small_path = root / "small.json"
    full_path = root / "full.json"
    small_path.write_bytes(canonical_json(runtime_document(small)))
    full_path.write_bytes(canonical_json(runtime_document(full)))
    validated = subprocess.run(
        (str(native_validator), "validate", str(full_path)),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    assert validated.returncode == 0, validated.stderr
    native_summary = json.loads(validated.stdout)
    assert native_summary["tasks"] == full.tasks
    assert native_summary["data"] == full.data_records
    assert native_summary["edges"] == full.scheduler_edges
    assert native_summary["requested_outputs"] == full.c_tasks
    small_topology_digest = topology_digest(small)
    assert run_native("digest", small_path)["topology_sha256"] == small_topology_digest
    profile = run_native("profile", full_path)
    assert profile["tasks"] == 1_048_576
    assert profile["edges"] == 5_898_240
    assert profile["build_nanoseconds"] <= 60_000_000_000
    assert profile["max_rss_kib"] < 4 * 1024 * 1024
    invalid = runtime_document(small)
    invalid["policy"]["maximum_edges"] -= 1
    rejected = subprocess.run(
        (str(native_validator), "validate", "-"),
        input=canonical_json(invalid), stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert rejected.returncode == 1
    assert json.loads(rejected.stdout)["valid"] is False

print(json.dumps({
    "status": "PASS", "descriptor_bytes": len(payload),
    "explicit_lower_bound_bytes": explicit_lower_bound,
    "minimum_reduction": explicit_lower_bound / len(payload),
    "tasks": full.tasks, "files": full.workflow_files,
    "sampled_tasks": 4096, "inverse_small_graph": "exhaustive",
    "native_full_build_nanoseconds": profile["build_nanoseconds"],
    "native_full_max_rss_kib": profile["max_rss_kib"],
    "native_small_topology_sha256": small_topology_digest,
}, sort_keys=True))
