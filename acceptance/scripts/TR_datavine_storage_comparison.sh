#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
RUN_ROOT=$(mktemp -d /tmp/datavine-storage-comparison.XXXXXX)
trap 'rm -rf -- "$RUN_ROOT"' EXIT

run_mode() {
  local mode=$1
  local output=$2
  env \
    DATAVINE_GRAND_TASKS=40 \
    DATAVINE_GRAND_WORKERS=4 \
    DATAVINE_GRAND_WORKER_CORES=2 \
    DATAVINE_GRAND_MEDIUM_BYTES=65536 \
    DATAVINE_GRAND_LARGE_BYTES=262144 \
    DATAVINE_GRAND_LINEAGE_BYTES=262144 \
    DATAVINE_GRAND_CONTROLLER_INLINE_IDATA_BYTES=65536 \
    DATAVINE_GRAND_PRUNING_GRACE_SECONDS=0.1 \
    DATAVINE_GRAND_HARD_DELETE_PRUNED_SHAREDFS=1 \
	DATAVINE_GRAND_STORAGE_FRONTIER_STRIDE=4 \
	DATAVINE_GRAND_STORAGE_BUDGET_BYTES=524288 \
    DATAVINE_GRAND_WORKFLOW_TIMEOUT=300 \
    DATAVINE_GRAND_MODE="$mode" \
    DATAVINE_GRAND_FRONTIER_RECOVERY=1 \
    timeout 360 "$ROOT/acceptance/scripts/TR_datavine_grand_challenge.sh" \
      > "$output"
}

run_mode failures "$RUN_ROOT/pruning-on.json"
run_mode pruning-off "$RUN_ROOT/pruning-off.json"

python3 - "$RUN_ROOT/pruning-on.json" "$RUN_ROOT/pruning-off.json" <<'PY'
import json
import pathlib
import sys

enabled = json.loads(pathlib.Path(sys.argv[1]).read_text())
disabled = json.loads(pathlib.Path(sys.argv[2]).read_text())
for report in (enabled, disabled):
    assert report["status"] == "PASS", report
    assert report["durable_hashes_valid"], report
    assert not report["persistence_temporary_files"], report
    scheduler = report["scheduler_report"]
    assert scheduler["persistence_worker_bytes"] > 0, scheduler
    assert scheduler["persistence_controller_bytes"] == 0, scheduler

assert all(report["chain_rollback_depths"] == [3, 2, 1] for report in (enabled, disabled))

assert enabled["tasks"] == disabled["tasks"]
assert enabled["task_to_data_bindings"] == disabled["task_to_data_bindings"]
enabled_scheduler = enabled["scheduler_report"]
disabled_scheduler = disabled["scheduler_report"]
assert enabled_scheduler["runtime_pruned_data_ids"], enabled_scheduler
assert not disabled_scheduler["runtime_pruned_data_ids"], disabled_scheduler
assert len(enabled_scheduler["sharedfs_hard_delete"]["deleted"]) == 3
assert disabled_scheduler["sharedfs_hard_delete"] is None

enabled_storage = enabled["sharedfs_storage"]
disabled_storage = disabled["sharedfs_storage"]
assert enabled_storage["quarantine_files"] == 0, enabled_storage
assert disabled_storage["quarantine_files"] == 0, disabled_storage
assert enabled_storage["durable_files"] == 1, enabled_storage
assert disabled_storage["durable_files"] == 4, disabled_storage
assert enabled_storage["durable_bytes"] * 4 == disabled_storage[
    "durable_bytes"
], (enabled_storage, disabled_storage)
assert not enabled["storage_budget"]["exceeded"], enabled
assert disabled["storage_budget"]["exceeded"], disabled

print(json.dumps({
    "status": "PASS",
    "tasks": enabled["tasks"],
    "bindings": enabled["task_to_data_bindings"],
    "worker_persistence_bytes": enabled_scheduler[
        "persistence_worker_bytes"
    ],
    "pruning_enabled_retained_bytes": enabled_storage["durable_bytes"],
    "pruning_disabled_retained_bytes": disabled_storage["durable_bytes"],
    "retained_ratio": (
        enabled_storage["durable_bytes"]
        / disabled_storage["durable_bytes"]
    ),
	"storage_budget_bytes": enabled["storage_budget"]["limit_bytes"],
	"pruning_enabled_budget_exceeded": enabled["storage_budget"][
		"exceeded"
	],
	"pruning_disabled_budget_exceeded": disabled["storage_budget"][
		"exceeded"
	],
    "hard_deleted_files": len(
        enabled_scheduler["sharedfs_hard_delete"]["deleted"]
    ),
}, sort_keys=True))
PY
