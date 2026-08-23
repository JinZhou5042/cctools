#!/usr/bin/env python3
"""Contract checks for the fixed 1024-core comparison harness."""

import json
from pathlib import Path
import sys
from concurrent.futures import FIRST_COMPLETED, TimeoutError


REPOSITORY = Path(__file__).resolve().parents[2]
SCRIPTS = REPOSITORY / "acceptance/scripts"
sys.path.insert(0, str(SCRIPTS))

import benchmark_1024_workflows as benchmark  # noqa: E402
from compact_1024_workflow_campaign import compact_run  # noqa: E402
from compare_workflows import (  # noqa: E402
    cleanup_taskvine_futures,
    execute_locally,
    timed_work_unit,
    vine,
)


class FakeFuture:
    def __init__(self, ready):
        self._is_submitted = True
        self.ready = ready
        self.timeouts = []

    def result(self, timeout=None):
        self.timeouts.append(timeout)
        if not self.ready:
            raise TimeoutError()
        return "ready"


class CleanupManager:
    def __init__(self):
        self._task_table = {}
        self.undeclared = []

    def empty(self):
        return True

    def undeclare_file(self, file):
        self.undeclared.append(file)


class CleanupTask:
    def __init__(self):
        self._task = object()
        self._input_file = object()
        self._output_file = object()
        self._saved_output = object()
        self._future = object()

    def __del__(self):
        self._task = None


class CleanupFuture:
    def __init__(self, task):
        self._task = task
        self._result = object()


class RecoveryManager:
    def __init__(self):
        self.wait_timeouts = []

    def wait(self, timeout):
        self.wait_timeouts.append(timeout)


class RecoveryPool:
    def __init__(self):
        self.executor = type("RecoveryExecutor", (), {})()
        self.executor.manager = RecoveryManager()


def main():
    manifest_path = REPOSITORY / "acceptance/1024-core-workflows/campaign-v1.json"
    schema_path = REPOSITORY / "acceptance/1024-core-workflows/result-schema-v1.json"
    manifest = json.loads(manifest_path.read_text())
    json.loads(schema_path.read_text())
    assert benchmark.validate_manifest(manifest) == []
    resource = manifest["resource_contract"]
    assert resource["workers"] * resource["cores_per_worker"] == 1024
    cases = benchmark.build_cases(manifest, 1024)
    assert len(cases) == 49
    assert len({item["case_id"] for item in cases}) == len(cases)
    phases = {item["phase"] for item in cases}
    assert phases == {
        "admission", "confirmation", "cpu", "data", "degree", "topology",
        "interaction",
    }
    immediate = next(item for item in cases if item["case_id"] == "admission_immediate")
    assert immediate["graph"]["logical_tasks"] == 1024
    assert immediate["graph"]["logical_edges"] == 0
    assert all(item["iterations"] == 0 for item in immediate["spec"]["nodes"])
    assert all(item["output_bytes"] == 0 for item in immediate["spec"]["nodes"])
    degree64 = next(item for item in cases if item["case_id"] == "degree_64")
    assert degree64["graph"]["max_indegree"] == 64
    assert degree64["graph"]["max_outdegree"] == 64
    assert degree64["graph"]["logical_tasks"] == 2048
    assert degree64["graph"]["logical_edges"] == 65536
    broadcast_control = next(
        item for item in cases
        if item["case_id"] == "confirmation_input_broadcast_0b_w128"
    )
    assert broadcast_control["graph"]["logical_tasks"] == 129
    assert broadcast_control["graph"]["logical_edge_payload_bytes"] == 0
    replica_source = (
        REPOSITORY / "taskvine/src/manager/vine_file_replica_table.c"
    ).read_text()
    manager_source = (
        REPOSITORY / "taskvine/src/manager/vine_manager.c"
    ).read_text()
    runtime_source = (
        REPOSITORY / "taskvine/src/datavine/vine_datavine_workflow_runtime.c"
    ).read_text()
    swig_source = (
        REPOSITORY / "taskvine/src/bindings/python3/taskvine.i"
    ).read_text()
    finalizer_source = (
        REPOSITORY / "acceptance/scripts/finalize_1024_workflow_campaign.py"
    ).read_text()
    benchmark_source = (
        REPOSITORY / "acceptance/scripts/benchmark_1024_workflows.py"
    ).read_text()
    assert "vine_file_replica_table_find_worker_for_manager" in replica_source
    assert "vine_file_replica_table_find_worker_for_manager" in manager_source
    assert "if (!peer->transfer_port_active)" not in replica_source.split(
        "vine_file_replica_table_find_worker_for_manager", 1
    )[1].split("Count number of replicas", 1)[0]
    assert "if (!contents)" in swig_source and "Py_RETURN_NONE" in swig_source
    # Worker detection stays in the Manager, but a FORSAKEN task crosses the
    # boundary exactly once so the DataVine scheduler owns logical retry.
    assert "vine_task_set_max_forsaken(physical, 0)" in runtime_source
    assert "manager_workers_removed=%lld" in runtime_source
    compare_source = (
        REPOSITORY / "acceptance/scripts/compare_workflows.py"
    ).read_text()
    assert 'not stages.get("manager_workers_removed", 0)' in compare_source
    assert 'not counts["workers_removed"]' in compare_source
    assert "if self._result is not None:" in compare_source
    assert '"dynamic-session-roundtrips"' in benchmark_source
    assert 'median_stage(dv_runs, "runtime_invocations")' in benchmark_source
    assert '"dynamic-session-roundtrips", "result-fetch"' in benchmark_source
    assert '"measurement_records_changed": False' in finalizer_source
    compact_source = (
        REPOSITORY / "acceptance/scripts/compact_1024_workflow_campaign.py"
    ).read_text()
    assert '"analysis_provenance": source.get("analysis_provenance")' in compact_source
    assert '"zero_worker_churn_in_accepted_runs"' in finalizer_source
    assert '"warmups_complete"' in finalizer_source
    dynamic = next(item for item in cases if item["case_id"] == "topology_dynamic")
    assert dynamic["spec"]["kind"] == "timed"
    tiny = benchmark.independent_case("test_immediate", "test", 1, 1, 0, 0)
    first = execute_locally(tiny["spec"])
    second = execute_locally(tiny["spec"])
    assert first == second
    assert first[0]["payload_bytes"] == 0
    timed = timed_work_unit("cpu-contract", 10, 7, 1024 * 1024)
    kernel_cpu_ns = timed["cpu_by_task"]["cpu-contract"]
    assert 10_000_000 <= kernel_cpu_ns < 12_000_000
    interval = benchmark.bootstrap_interval([0.8, 0.9, 1.0], 10000, 0.95, 7)
    assert interval == benchmark.bootstrap_interval([0.8, 0.9, 1.0], 10000, 0.95, 7)
    assert interval[0] <= 0.9 <= interval[1]
    assert benchmark.execution_identity("case", "r", 1, 1) == "case__r1a1"
    assert benchmark.execution_identity("case", "r", 1, 2) == "case__r1a2"
    assert benchmark.execution_identity("case", "w", 1, 1) != benchmark.execution_identity(
        "case", "r", 1, 1
    )
    snapshots = iter([
        {"status": "FAIL", "workers": 63, "cores_per_worker": [16] * 63},
        {"status": "PASS", "workers": 64, "cores_per_worker": [16] * 64},
    ])
    original_snapshot = benchmark.inventory_snapshot
    try:
        benchmark.inventory_snapshot = lambda *_args, **_kwargs: next(snapshots)
        recovery_pool = RecoveryPool()
        recovered = benchmark.wait_for_exact_inventory(
            REPOSITORY, recovery_pool, 64, 16, timeout=1, poll_seconds=0
        )
    finally:
        benchmark.inventory_snapshot = original_snapshot
    assert recovered["status"] == "PASS"
    assert recovered["admission_attempts"] == 2
    assert recovery_pool.executor.manager.wait_timeouts == [1]
    compact = benchmark.compact_admission(recovered, 16)
    assert compact["workers"] == 64 and compact["total_cores"] == 1024
    slow = FakeFuture(False)
    ready = FakeFuture(True)
    waited = vine.futures.wait(
        [slow, ready], timeout=0.1, return_when=FIRST_COMPLETED
    )
    assert ready in waited.done and slow in waited.not_done
    assert slow.timeouts == [1] and ready.timeouts == [0]
    manager = CleanupManager()
    retained_library = object()
    completed_task = CleanupTask()
    executor = type("CleanupExecutor", (), {})()
    executor.manager = manager
    executor.task_table = [retained_library, completed_task]
    cleanup_future = CleanupFuture(completed_task)
    cleanup = cleanup_taskvine_futures(executor, [cleanup_future])
    assert cleanup["tasks_released"] == 1
    assert cleanup["files_undeclared"] == 2
    assert cleanup["executor_task_table_before"] == 2
    assert cleanup["executor_task_table_after"] == 1
    assert executor.task_table == [retained_library]
    assert completed_task._task is None and cleanup_future._task is None
    compacted_warmup = compact_run({
        "case_id": "warmup", "phase": "cpu", "warmup_repetition": 1,
        "backend": "TV-native", "result": {"results": ["ok"]},
    })
    assert compacted_warmup["repetition"] is None
    assert compacted_warmup["warmup_repetition"] == 1
    assert len(compacted_warmup["results_sha256"]) == 64
    print("datavine 1024-core benchmark contract: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
