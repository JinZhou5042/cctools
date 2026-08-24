#!/usr/bin/env python3

import collections
import json
from pathlib import Path
import random
import sys

repository = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repository / "acceptance/scripts"))
from data_intensive_parametric_ir import ParametricWorkload, descriptor
from data_intensive_workload import Workload, canonical_json, full_workload


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
# entry is enough to prove the registration reduction without allocating it.
explicit_lower_bound = 8 * (full.tasks + full.data_records + full.input_references)
assert len(payload) < 16_384
assert explicit_lower_bound / len(payload) >= 100
assert document["counts"]["tasks"] == 1_048_576
assert document["counts"]["workflow_files"] == 10_485_760
print(json.dumps({
    "status": "PASS", "descriptor_bytes": len(payload),
    "explicit_lower_bound_bytes": explicit_lower_bound,
    "minimum_reduction": explicit_lower_bound / len(payload),
    "tasks": full.tasks, "files": full.workflow_files,
    "sampled_tasks": 4096, "inverse_small_graph": "exhaustive",
}, sort_keys=True))
