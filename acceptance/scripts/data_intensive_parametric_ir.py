#!/usr/bin/env python3
"""Constant-size parametric IR evaluator for the frozen data-intensive DAG.

This module is deliberately side-effect free: validation and lookup never stat
or open a source file.  A runtime asks for one TaskID at a time and receives
exactly the task/data records needed for that physical execution.  Inverse
consumer lookup lets a scheduler release children without retaining edges.
"""

from dataclasses import dataclass
from pathlib import Path
import math

from data_intensive_workload import (
    B_INPUTS, B_REUSE, C_INPUTS, SCHEMA, SEED, SOURCE_INPUTS,
    SIZE_PROFILES, Workload, stable_u64,
)


PARAMETRIC_SCHEMA = "datavine.parametric-workflow/v1"


def descriptor(workload: Workload, dataset_root: Path):
    """Return the complete, constant-cardinality declaration."""
    contract = workload.contract()
    return {
        "schema": PARAMETRIC_SCHEMA,
        "expanded_schema": SCHEMA,
        "contract_sha256": contract["contract_sha256"],
        "seed": SEED,
        "dataset_root": str(Path(dataset_root).resolve()),
        "shape": contract["shape"],
        "counts": contract["counts"],
        "sizes": contract["sizes"],
        "source_family": {
            "count": workload.source_files,
            "data_id": [workload.source_data_first, SOURCE_INPUTS + 1],
            "path": "sources/c{cohort:02d}/a{a_local:04d}/s{slot:02d}.bin",
            "coordinates": ["source_index div 36", "source_index mod 36"],
        },
        "task_families": [
            {"stage": "A", "first": 1, "count": workload.a_tasks,
             "inputs": SOURCE_INPUTS, "payload_ref": 1},
            {"stage": "B", "first": workload.a_tasks + 1,
             "count": workload.b_tasks, "inputs": B_INPUTS,
             "payload_ref": 2, "reuse": B_REUSE,
             "mapping": "checked-affine-permutation"},
            {"stage": "C", "first": workload.a_tasks + workload.b_tasks + 1,
             "count": workload.c_tasks, "inputs": C_INPUTS,
             "payload_ref": 3, "mapping": "checked-affine-permutation",
             "requested": True},
        ],
        "invariants": {
            "one_logical_one_physical": True,
            "source_payload_reads_at_registration": 0,
            "resident_materialization": "bounded-frontier",
        },
    }


@dataclass(frozen=True)
class ParametricWorkload:
    workload: Workload
    dataset_root: Path

    def __post_init__(self):
        w = self.workload
        if w.a_per_cohort % B_INPUTS or w.b_per_cohort % C_INPUTS:
            raise ValueError("family cardinalities do not divide exactly")
        if w.b_per_cohort != (w.a_per_cohort // B_INPUTS) * B_REUSE:
            raise ValueError("A/B inverse mapping is not total")
        if w.c_per_cohort * C_INPUTS != w.b_per_cohort:
            raise ValueError("B/C inverse mapping is not total")
        if w.tasks > (1 << 63) - 1 or w.data_records > (1 << 63) - 1:
            raise OverflowError("expanded IDs exceed signed runtime bounds")

    def task(self, task_id):
        """Materialize one logical task; no global task/data object is made."""
        w = self.workload
        if not 1 <= task_id <= w.tasks:
            raise IndexError(task_id)
        local = task_id - 1
        if local < w.a_tasks:
            stage = "A"
            inputs = w.a_input_data_ids(local)
            output = w.a_data_id(local)
            payload_ref = 1
        elif local < w.a_tasks + w.b_tasks:
            stage = "B"
            local -= w.a_tasks
            inputs = w.b_input_data_ids(local)
            output = w.b_data_first + local
            payload_ref = 2
        else:
            stage = "C"
            local -= w.a_tasks + w.b_tasks
            inputs = w.c_input_data_ids(local)
            output = w.c_data_first + local
            payload_ref = 3
        return {
            "task_id": task_id, "stage": stage, "payload_ref": payload_ref,
            "inputs": inputs, "output": output, "requested": stage == "C",
        }

    def source(self, data_id):
        """Resolve a source DataID to its URI only when its A task enters."""
        w = self.workload
        relative = data_id - w.source_data_first
        a_global, position = divmod(relative, SOURCE_INPUTS + 1)
        if not (0 <= a_global < w.a_tasks and position < SOURCE_INPUTS):
            raise IndexError(data_id)
        source_index = a_global * SOURCE_INPUTS + position
        return (self.dataset_root / w.source_path(source_index)).resolve().as_uri()

    def consumers(self, data_id):
        """Return the checked inverse family mapping for one generated value."""
        w = self.workload
        # A output -> exactly one B in each of the twenty permutation rounds.
        relative = data_id - w.a_data_first
        if relative >= 0:
            a_global, position = divmod(relative, SOURCE_INPUTS + 1)
            if a_global < w.a_tasks and position == 0:
                cohort, a_local = divmod(a_global, w.a_per_cohort)
                groups = w.a_per_cohort // B_INPUTS
                result = []
                for round_index in range(B_REUSE):
                    multiplier = stable_u64("a", cohort, round_index, "m") % w.a_per_cohort | 1
                    offset = stable_u64("a", cohort, round_index, "o") % w.a_per_cohort
                    inverse = pow(multiplier, -1, w.a_per_cohort)
                    position = (inverse * (a_local - offset)) % w.a_per_cohort
                    group = position // B_INPUTS
                    b_local = round_index * groups + group
                    result.append(w.a_tasks + cohort * w.b_per_cohort + b_local + 1)
                return tuple(result)
        # B output -> exactly one C through the inverse affine permutation.
        b_global = data_id - w.b_data_first
        if 0 <= b_global < w.b_tasks:
            cohort, b_local = divmod(b_global, w.b_per_cohort)
            multiplier = w._b_multiplier(cohort)
            offset = stable_u64("b", cohort, "o") % w.b_per_cohort
            inverse = pow(multiplier, -1, w.b_per_cohort)
            position = (inverse * (b_local - offset)) % w.b_per_cohort
            c_local = position // C_INPUTS
            return (w.a_tasks + w.b_tasks + cohort * w.c_per_cohort + c_local + 1,)
        if w.c_data_first <= data_id < w.c_data_first + w.c_tasks:
            return ()
        raise IndexError(data_id)

    def validate(self):
        w = self.workload
        value = descriptor(w, self.dataset_root)
        counts = value["counts"]
        if counts["tasks"] != w.tasks or counts["workflow_files"] != w.workflow_files:
            raise ValueError("expanded count mismatch")
        if counts["scheduler_edges"] != w.scheduler_edges:
            raise ValueError("expanded edge mismatch")
        return value
