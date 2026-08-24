#!/usr/bin/env python3
"""Deterministic topology and byte contract for the large I/O benchmark."""

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path


SCHEMA = "datavine.data-intensive-workload/v1"
SEED = 20260823
SOURCE_INPUTS = 36
B_INPUTS = 8
C_INPUTS = 5
B_REUSE = 20
FULL_COHORTS = 64
FULL_SCALE = 64
FULL_WORKERS = 128
FULL_CORES = 16
TIB = 1 << 40

SIZE_PROFILES = {
    "tiny": {
        "source": (64, 256, 1024, 4096),
        "a_output": 4096,
        "b_output": 2048,
        "c_output": 4096,
    },
    "full": {
        "source": (1 << 10, 16 << 10, 128 << 10, 1 << 20),
        "a_output": 512 << 10,
        "b_output": 256 << 10,
        "c_output": 512 << 10,
    },
}


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def stable_u64(*values):
    payload = ":".join(str(value) for value in (SEED,) + values).encode()
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")


def stage_source(stage, output_bytes):
    """Return the shared source-v1 program for one workload stage.

    Every input byte is read with deterministic pseudo-random chunk and file
    ordering.  The busy loop uses process CPU time, never sleep, and assigns a
    deterministic 2 ms to 5 s target from the task's input Data IDs.
    """

    if stage not in {"A", "B", "C"} or output_bytes < 1:
        raise ValueError((stage, output_bytes))
    return f'''import hashlib
import json
import os
from pathlib import Path
import time

STAGE = {stage!r}
OUTPUT_BYTES = {int(output_bytes)}
CHUNK_BYTES = 64 * 1024
MASK = (1 << 64) - 1

def step(value):
    value ^= (value << 13) & MASK
    value ^= value >> 7
    value ^= (value << 17) & MASK
    return value & MASK

inputs = []
program_path = Path(__file__).resolve()
for name, path in os.environ.items():
    if name.startswith("DATAVINE_DATA_") and Path(path).resolve() != program_path:
        inputs.append((int(name[14:]), Path(path)))
inputs.sort()
if not inputs:
    raise RuntimeError("data-intensive task has no inputs")
state = int.from_bytes(hashlib.sha256(
    (STAGE + ":" + ",".join(str(item[0]) for item in inputs)).encode()
).digest()[:8], "big") or 1
ordered = list(inputs)
for index in range(len(ordered) - 1, 0, -1):
    state = step(state)
    target = state % (index + 1)
    ordered[index], ordered[target] = ordered[target], ordered[index]
digest = hashlib.sha256()
read_bytes = 0
for data_id, path in ordered:
    size = path.stat().st_size
    offsets = list(range(0, size, CHUNK_BYTES))
    for index in range(len(offsets) - 1, 0, -1):
        state = step(state)
        target = state % (index + 1)
        offsets[index], offsets[target] = offsets[target], offsets[index]
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
    try:
        for offset in offsets:
            block = os.pread(descriptor, min(CHUNK_BYTES, size - offset), offset)
            if len(block) != min(CHUNK_BYTES, size - offset):
                raise RuntimeError(f"short read data {{data_id}} offset {{offset}}")
            digest.update(data_id.to_bytes(8, "big"))
            digest.update(offset.to_bytes(8, "big"))
            digest.update(block)
            read_bytes += len(block)
    finally:
        os.close(descriptor)
bucket = state % 1000
if bucket < 800:
    cpu_ms = 2 + state % 9
elif bucket < 950:
    cpu_ms = 10 + state % 41
elif bucket < 990:
    cpu_ms = 50 + state % 451
elif bucket < 999:
    cpu_ms = 500 + state % 1501
else:
    cpu_ms = 2000 + state % 3001
deadline = time.process_time_ns() + cpu_ms * 1_000_000
sink = state
while time.process_time_ns() < deadline:
    sink = step(sink)
seed = hashlib.sha256(STAGE.encode() + digest.digest()).digest()
block = hashlib.shake_256(seed).digest(min(OUTPUT_BYTES, 1 << 20))
output = Path("datavine-python-output-0")
with output.open("xb", buffering=0) as stream:
    remaining = OUTPUT_BYTES
    while remaining:
        count = min(len(block), remaining)
        stream.write(block[:count])
        remaining -= count
print(json.dumps({{"stage": STAGE, "inputs": len(inputs), "read_bytes": read_bytes,
                  "output_bytes": OUTPUT_BYTES, "cpu_ms": cpu_ms,
                  "sink": sink & 0xffff}}, sort_keys=True))
'''


@dataclass(frozen=True)
class Workload:
    """A scaled instance of the fixed three-level cohort topology."""

    cohorts: int = FULL_COHORTS
    scale: int = FULL_SCALE
    size_profile: str = "full"

    def __post_init__(self):
        if self.cohorts < 1 or self.scale < 1:
            raise ValueError("cohorts and scale must be positive")
        if self.size_profile not in SIZE_PROFILES:
            raise ValueError(f"unknown size profile: {self.size_profile}")

    @property
    def a_per_cohort(self):
        return 64 * self.scale

    @property
    def b_per_cohort(self):
        return 160 * self.scale

    @property
    def c_per_cohort(self):
        return 32 * self.scale

    @property
    def a_tasks(self):
        return self.cohorts * self.a_per_cohort

    @property
    def b_tasks(self):
        return self.cohorts * self.b_per_cohort

    @property
    def c_tasks(self):
        return self.cohorts * self.c_per_cohort

    @property
    def tasks(self):
        return self.a_tasks + self.b_tasks + self.c_tasks

    @property
    def source_files(self):
        return self.a_tasks * SOURCE_INPUTS

    @property
    def output_files(self):
        return self.tasks

    @property
    def workflow_files(self):
        return self.source_files + self.output_files

    @property
    def source_data_first(self):
        return 4  # Data IDs 1..3 are the shared stage source programs.

    @property
    def a_data_first(self):
        return self.source_data_first + SOURCE_INPUTS

    @property
    def b_data_first(self):
        return self.source_data_first + self.a_tasks * (SOURCE_INPUTS + 1)

    @property
    def c_data_first(self):
        return self.b_data_first + self.b_tasks

    @property
    def data_records(self):
        return 3 + self.workflow_files

    @property
    def input_references(self):
        return (
            self.a_tasks * SOURCE_INPUTS
            + self.b_tasks * B_INPUTS
            + self.c_tasks * C_INPUTS
        )

    @property
    def scheduler_edges(self):
        return self.b_tasks * B_INPUTS + self.c_tasks * C_INPUTS

    def source_size(self, source_index):
        if not 0 <= source_index < self.source_files:
            raise IndexError(source_index)
        sizes = SIZE_PROFILES[self.size_profile]["source"]
        slot = source_index % 64
        if slot < 29:
            return sizes[0]
        if slot < 48:
            return sizes[1]
        if slot < 61:
            return sizes[2]
        return sizes[3]

    @property
    def source_bytes(self):
        cycles, remainder = divmod(self.source_files, 64)
        sizes = SIZE_PROFILES[self.size_profile]["source"]
        cycle_bytes = 29 * sizes[0] + 19 * sizes[1] + 13 * sizes[2] + 3 * sizes[3]
        return cycles * cycle_bytes + sum(self.source_size(i) for i in range(remainder))

    @property
    def a_output_bytes(self):
        return self.a_tasks * SIZE_PROFILES[self.size_profile]["a_output"]

    @property
    def b_output_bytes(self):
        return self.b_tasks * SIZE_PROFILES[self.size_profile]["b_output"]

    @property
    def c_output_bytes(self):
        return self.c_tasks * SIZE_PROFILES[self.size_profile]["c_output"]

    @property
    def stored_bytes(self):
        return self.source_bytes + self.a_output_bytes + self.b_output_bytes + self.c_output_bytes

    @property
    def logical_read_bytes(self):
        return (
            self.source_bytes
            + self.a_output_bytes * B_REUSE
            + self.b_output_bytes
        )

    @property
    def logical_data_path_bytes(self):
        return self.logical_read_bytes + self.a_output_bytes + self.b_output_bytes + self.c_output_bytes

    def source_path(self, source_index):
        if not 0 <= source_index < self.source_files:
            raise IndexError(source_index)
        a_global, slot = divmod(source_index, SOURCE_INPUTS)
        cohort, a_local = divmod(a_global, self.a_per_cohort)
        return Path("sources") / f"c{cohort:02d}" / f"a{a_local:04d}" / f"s{slot:02d}.bin"

    def a_input_data_ids(self, a_global):
        if not 0 <= a_global < self.a_tasks:
            raise IndexError(a_global)
        first = self.source_data_first + a_global * (SOURCE_INPUTS + 1)
        return tuple(range(first, first + SOURCE_INPUTS))

    def source_data_id(self, source_index):
        if not 0 <= source_index < self.source_files:
            raise IndexError(source_index)
        a_global, slot = divmod(source_index, SOURCE_INPUTS)
        return self.source_data_first + a_global * (SOURCE_INPUTS + 1) + slot

    def a_data_id(self, a_global):
        if not 0 <= a_global < self.a_tasks:
            raise IndexError(a_global)
        return self.source_data_first + a_global * (SOURCE_INPUTS + 1) + SOURCE_INPUTS

    def _a_permutation(self, cohort, round_index, position):
        modulus = self.a_per_cohort
        multiplier = stable_u64("a", cohort, round_index, "m") % modulus | 1
        offset = stable_u64("a", cohort, round_index, "o") % modulus
        return (multiplier * position + offset) % modulus

    def b_input_data_ids(self, b_global):
        if not 0 <= b_global < self.b_tasks:
            raise IndexError(b_global)
        cohort, b_local = divmod(b_global, self.b_per_cohort)
        groups = self.a_per_cohort // B_INPUTS
        round_index, group = divmod(b_local, groups)
        if round_index >= B_REUSE:
            raise AssertionError((round_index, B_REUSE))
        a_locals = (
            self._a_permutation(cohort, round_index, group * B_INPUTS + item)
            for item in range(B_INPUTS)
        )
        return tuple(
            self.a_data_id(cohort * self.a_per_cohort + a_local)
            for a_local in a_locals
        )

    def _b_multiplier(self, cohort):
        modulus = self.b_per_cohort
        value = stable_u64("b", cohort, "m") % modulus
        while math.gcd(value, modulus) != 1:
            value = (value + 1) % modulus
        return value

    def c_input_data_ids(self, c_global):
        if not 0 <= c_global < self.c_tasks:
            raise IndexError(c_global)
        cohort, c_local = divmod(c_global, self.c_per_cohort)
        modulus = self.b_per_cohort
        multiplier = self._b_multiplier(cohort)
        offset = stable_u64("b", cohort, "o") % modulus
        return tuple(
            self.b_data_first
            + cohort * modulus
            + (multiplier * (c_local * C_INPUTS + item) + offset) % modulus
            for item in range(C_INPUTS)
        )

    def source_part_bounds(self, part, parts):
        if parts < 1 or not 0 <= part < parts:
            raise ValueError((part, parts))
        if self.a_tasks % parts:
            raise ValueError("parts must divide the number of A tasks")
        a_count = self.a_tasks // parts
        first_a = part * a_count
        return first_a * SOURCE_INPUTS, a_count * SOURCE_INPUTS

    def contract(self):
        profile = SIZE_PROFILES[self.size_profile]
        value = {
            "schema": SCHEMA,
            "seed": SEED,
            "shape": {
                "cohorts": self.cohorts,
                "scale": self.scale,
                "a_per_cohort": self.a_per_cohort,
                "b_per_cohort": self.b_per_cohort,
                "c_per_cohort": self.c_per_cohort,
            },
            "counts": {
                "a_tasks": self.a_tasks,
                "b_tasks": self.b_tasks,
                "c_tasks": self.c_tasks,
                "tasks": self.tasks,
                "source_files": self.source_files,
                "output_files": self.output_files,
                "workflow_files": self.workflow_files,
                "code_objects": 3,
                "data_records": self.data_records,
                "external_input_references": self.source_files,
                "scheduler_edges": self.scheduler_edges,
                "input_references": self.input_references,
            },
            "bytes": {
                "source": self.source_bytes,
                "a_outputs": self.a_output_bytes,
                "b_outputs": self.b_output_bytes,
                "c_outputs": self.c_output_bytes,
                "stored_artifacts": self.stored_bytes,
                "logical_reads": self.logical_read_bytes,
                "logical_data_path": self.logical_data_path_bytes,
                "limit": 2 * TIB,
            },
            "fan": {
                "source_inputs_per_a": SOURCE_INPUTS,
                "a_inputs_per_b": B_INPUTS,
                "a_consumers_per_output": B_REUSE,
                "b_inputs_per_c": C_INPUTS,
                "b_consumers_per_output": 1,
            },
            "sizes": profile,
            "acceptance": {
                "workers": FULL_WORKERS,
                "cores_per_worker": FULL_CORES,
                "total_cores": FULL_WORKERS * FULL_CORES,
                "retained_outputs": self.c_tasks,
                "reclaimable_intermediate_files": self.a_tasks + self.b_tasks,
                "reclaimable_intermediate_bytes": self.a_output_bytes + self.b_output_bytes,
            },
        }
        value["contract_sha256"] = hashlib.sha256(canonical_json(value)).hexdigest()
        return value


def full_workload():
    return Workload(FULL_COHORTS, FULL_SCALE, "full")


def assert_full_contract(workload):
    """Fail closed if an acceptance run has drifted from the promised workload."""

    expected = full_workload().contract()
    actual = workload.contract()
    if actual != expected:
        raise ValueError("acceptance workload differs from the frozen full contract")
    if actual["counts"]["tasks"] != 1_048_576:
        raise AssertionError(actual["counts"])
    if actual["counts"]["workflow_files"] != 10_485_760:
        raise AssertionError(actual["counts"])
    if actual["bytes"]["stored_artifacts"] >= actual["bytes"]["limit"]:
        raise AssertionError(actual["bytes"])
    return actual
