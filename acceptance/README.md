# DataVine acceptance

Start with [../DATAVINE_MAP.md](../DATAVINE_MAP.md) for source ownership and
[../DATAVINE_PRODUCTION.md](../DATAVINE_PRODUCTION.md) for runtime semantics.
[matrix.md](matrix.md) owns gate status. Historical PASS results describe their
recorded source and environment; they do not replace a fresh runtime gate.

## Build and verify

From the repository root, build the DataVine runtime and its TaskVine Manager,
Worker and Python bindings, then the test-only tools. Keep this build scoped to
TaskVine; a root build also rebuilds unrelated CCTools packages:

```sh
make -C taskvine/src -j8
make -C taskvine/src/tools \
  datavine_parametric_test datavine_replica_benchmark \
  datavine_controller_benchmark -j8
DATAVINE_PYTHON=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin/python
export PYTHONDONTWRITEBYTECODE=1
export PYTHONNOUSERSITE=1
export PYTHONPATH="$PWD/test_support/python_modules/python3${PYTHONPATH:+:$PYTHONPATH}"
"$DATAVINE_PYTHON" -c 'import ndcctools.taskvine.datavine as dv; print(dv.__file__)'
"$DATAVINE_PYTHON" taskvine/test/datavine_module_boundaries.py
PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH \
PYTHONNOUSERSITE=1 \
DATAVINE_TEST_PYTHON="$DATAVINE_PYTHON" \
DATAVINE_GO_BINARY=/users/jzhou24/graph_optimization/factories/datavine_workflow_go-20260811 \
  acceptance/scripts/run_regression.sh
git diff --check
```

The printed import path must resolve into this checkout. The `PYTHONPATH`
above uses the repository's existing test-module links to select the built
bindings rather than an older installed copy.
The regression requires either `DATAVINE_GO_BINARY` or `DATAVINE_GO_COMPILER`.
Run a focused gate as `bash taskvine/test/TR_NAME.sh run`; the full suite now
uses one workflow owner per service. Compilation alone is not acceptance.

## Result index

Each campaign owns its report, machine results and checksum manifest. Existing
paths are retained so scripts and historical provenance remain valid.

| Question | Evidence | Scope |
|---|---|---|
| DataVine paper and fresh experiments | [Paper package](../paper/README.md) | Matched campaigns, recall fairness fix, mechanism and recovery gates; see current ledger |
| Native scheduling ceiling | [Million-task campaign](million-task-throughput-20260903/INDEX.md) | Repeated 1M no-output native tasks; explicit `recovery:none` |
| Larger Worker pool | [Big-pool report](million-task-bigpool-20260903/REPORT.md) | One complete 32x16 point; historical 8x16 comparison, not matched A/B |
| Elastic admission and Controller RPC | [Elastic campaign](adaptive-window-20260903/INDEX.md) | Local oracle comparison, recall/eviction, one remote scale point, DVP3/DVP4 |
| Storage paths | [Storage matrix](storage-matrix-20260901/INDEX.md) | Exact 32x16 Controller, peer and SharedFS measurements |
| File-size routing decision | [Dense crossover](storage-crossover-20260901/INDEX.md) | No stable size threshold; sparse preliminary results are superseded |
| Dynamic data lifecycle | [Dynamic lifecycle](dynamic-data-management-20260831.json) | Restart hydration, late consumers, peer delivery and seal GC |
| Peer versus Controller | [Route-isolated comparison](peer-vs-controller-20260831.json) | Separate workload and timing from the storage matrix |
| Background backup policy | [Default backup](default-idata-backup-20260831.json), [backup throughput](empty-idata-backup-throughput-20260831.json) | Worker-local primary plus asynchronous Controller backup |
| Controller metadata and RPC | [Controller characterization](controller-comprehensive-20260827.json) | Metadata lifecycle and unbatched RPC |
| Persistence cost and failure | [Stage diagnosis](controller-persistence-diagnostic-20260828.json), [fault gate](controller-persistence-faults-20260828.json) | `fsync`, ENOSPC/EIO, restart recovery |
| Remote persistence and network | [Fan-in](controller-remote-fanin-20260828.json), [network ceiling](controller-network-ceiling-20260828.json), [10k files](controller-10k-files-1m-20260828.json) | Distinct storage and transport boundaries |
| Controller-local storage decision | [Storage summary](controller-local-tmp-20260827/summary.json) | `/tmp` provides Worker-loss resilience, not cross-host durability |
| Data-intensive TaskVine comparison | [Fixed A/B](data-intensive-fixed-ab-20260830.json) | Three-pair 2x4 pilot, not full-scale acceptance |
| Dynamic TaskVine comparison | [Dynamic pilot](dynamic-adaptive-fixed-ab-20260830.json), [control-path repair](dynamic-control-root-fix-20260831.json) | Result-driven graph; separate from static speedup |
| Previous production baseline | [Baseline provenance](production-baseline-20260901.json) | Prior package; current elastic/RPC work is not promoted |
| Earlier correctness and tuning | [Rigorous validation](rigorous-validation-20260827/summary.json) | Historical multi-pass evidence; consult matrix for current gates |
| Earlier adaptive prototype | [Prototype index](adaptive-concurrency-20260902/INDEX.md) | Historical local mechanism evidence; final policy is in adaptive-window |

Other dated top-level JSON files retain supporting diagnostics and earlier
optimization measurements. They are not additional current headline results.
The scripts in `scripts/` reproduce benchmarks and plots; `helpers/` contains
supporting executors. Scientific and 1024-core workflow definitions remain in
`scientific-workflows/` and `1024-core-workflows/`.

## Detailed reports and historical scope

The root-level reports retain interpretation not duplicated in the matrix:
[Controller metadata](../CONTROLLER_PERFORMANCE_20260827.md),
[remote fan-in](../CONTROLLER_REMOTE_FANIN_20260828.md),
[persistence diagnosis](../CONTROLLER_PERSISTENCE_DIAGNOSTIC_20260828.md),
[static pilot](../DATAVINE_FIXED_AB_20260830.md), and
[dynamic correction](../DATAVINE_DYNAMIC_CONTROL_20260831.md).
The [full data-intensive workload](../DATAVINE_DATA_INTENSIVE_BENCHMARK.md) is a
planned scale contract, not a completed experiment.

Checksummed campaign Markdown is frozen with its original measurements.
Statements such as “next gate” or “current implementation” inside an old report
refer to that campaign's date. For present work use the
[handoff](../DATAVINE_HANDOFF_20260827.md), not an archived todo list.

## Retention and integrity

- Preserve accepted reports, exact per-run results, supporting logs, figures and
  their manifests. Keep `raw/results/` and `raw/logs/` separate in new campaigns.
- Do not rewrite historical evidence to match a new source tree. Record a new
  campaign and identify which older conclusion it supersedes.
- Keep temporary run directories, Worker sandboxes, copied executables and
  Python caches outside evidence. Use `/tmp` for smoke runs and discard their
  generated directories after retaining the required evidence.
- Compare rates only with matching topology, executor, output lifecycle and
  timer boundaries; exclude incomplete Worker admission from runtime samples.
- Verify each `SHA256SUMS` from its containing directory. To check all campaigns
  from the repository root:

```sh
python3 - <<'PYTHON'
from pathlib import Path
import subprocess

manifests = sorted(Path("acceptance").rglob("SHA256SUMS"))
if not manifests:
	raise SystemExit("No acceptance manifests found; run from the repository root")
for manifest in manifests:
	subprocess.run(
		["sha256sum", "-c", manifest.name], cwd=manifest.parent, check=True
	)
PYTHON
```

See [maintenance-20260906.md](maintenance-20260906.md) for the cleanup inventory
and integrity checks. Runtime binaries and libraries are retained for local use;
object files and compiled examples can be regenerated with `make -j8`.
`make clean` is an explicit build reset, not routine evidence maintenance.
