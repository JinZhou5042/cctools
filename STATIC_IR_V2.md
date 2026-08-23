# DataVine production static Workflow IR

Updated: 2026-08-23

Status: **PASS — IMPLEMENTED AND VALIDATED; DYNAMIC OPTIMIZATION DEFERRED**

This is the single narrative contract for the production static-workflow
lightweighting pass. Raw measurements remain authoritative in the linked JSON
artifacts; this document records architecture, semantics, gates, and operating
boundaries without copying implementation ownership into documentation.

## Contract

Static IR accepts the existing full object records and two compact forms:

```text
task        = [task_id, input_data_ids, output_data_ids]
output data = [data_id, producer_task_id, output_index]
```

Repeated executor, resource, retry, priority, and codec configuration moves to
top-level `task_defaults` and `data_defaults`. A full task may override a
default; compact tasks deliberately cannot carry per-task overrides. Workflow
deltas may define new defaults for their own ascending TaskID range. Runtime
stores one small range entry per delta and uses binary search, rather than one
template index entry per task.

`vine_datavine_ir.c` is the only compact/full record accessor layer used by
validation, Store, Runtime, Scheduler integration, and Data Controller. It is
not a graph owner. The native Runtime remains the graph authority; Scheduler
owns logical state and dependency counters; Data Controller owns DataID
identity, locations, durability, recovery metadata, and lifecycle.

Every logical task is an independent physical TaskVine task on every attempt.
Bounded workflow deltas reduce graph-registration messages but never combine
task execution, resource requests, retries, or completions.

## Readiness and recovery

Scheduler readiness is producer-task completion, not an independently
maintained data-ready bit. A producer becomes DONE only after its physical task
succeeds and its required output publication succeeds. This preserves one
readiness transition and prevents downstream release if a worker disappears
after execution but before publication.

The Data Controller maps the final loss of a cached replica to the affected
DataID. If that loss intersects a queued publication, the publication fails
immediately instead of waiting forever. Runtime retries the producer while it
is still logically RUNNING. For already completed values, recovery invalidates
only lost DataIDs, their producers, and descendants whose required inputs can
no longer be reconstructed. Historical journal events remain audit evidence;
the active scheduler state behaves as though invalidated completions had not
occurred.

## Message path

Graph registration uses one initial submission, bounded append deltas, and one
seal. The 100k/1M-I/O gate used 20 deltas and 26 total RPC requests including
authentication, capability check, terminal wait, and result descriptor fetch.
Execution remains 100,000 independent physical submissions and completions.
No per-edge or per-DataID readiness RPC was added.

Worker input lookup remains local cache, peer replica, then immutable object
fallback. Scheduler and Manager carry DataIDs/file handles, not payload bytes.
Only requested retained outputs require durable public retention; disposable
intermediates remain worker-local/peer-transferable when possible.

## Acceptance results

### Same-graph full versus compact IR

The graph contains 100,000 Python FunctionCall tasks, 1,000,000 input
references, 1,000,000 logical outputs, and 1,000 requested outputs.

| Metric | Full object IR | Production compact IR | Change |
|---|---:|---:|---:|
| Workflow payload | 221.17 MB | 26.98 MB | -87.80% |
| Graph load | 43.58 s | 6.81 s | 6.40x faster |
| Load RSS | 3.451 GB | 1.188 GB | -65.57% |

Execution throughput is not compared between these two artifacts because the
baseline used 16x16 Condor and the compact acceptance gate used 4x16 local workers.

### Fresh 4x16 gates

- One million independent builtin tasks: PASS; exactly 1,000,000 submissions
  and completions; 38.59 MB workflow payload; 10.69 s registration; 234.13 s
  runtime; 4,271 task/s; 1.606 GB peak process-tree RSS.
- 100k tasks / 1M input references / 1M outputs with real Python callables:
  PASS; exactly 100,000 submissions and completions; 6.81 s graph load; 193.8
  task/s; exact sampled results.
- 20k-task, 19k-edge recovery DAG: PASS after four live worker removals; every
  sink matched; 5,232 lost DataIDs caused 1,779 completed-task invalidations
  and 1,844 extra attempts; maximum attempt was three.
- Repository regression: PASS, 13/13.

## Reproduction

Use the verified DataVine environment and current local binaries:

```sh
make -C taskvine/src/datavine -j8
make -C taskvine/src/tools datavine_workflow -j8

conda run -p /groups/dthain/users/jzhou24/miniconda/envs/datavine \
  python acceptance/scripts/benchmark_native_workflow.py \
  --tasks 1000000 --workers 4 --cores 16 --executor builtin \
  --chunk-tasks 10000 --workflow-timeout 1800 --output RESULT.json

conda run -p /groups/dthain/users/jzhou24/miniconda/envs/datavine \
  python acceptance/scripts/benchmark_static_scale.py \
  --tasks 100000 --inputs-per-task 10 --outputs-per-task 10 \
  --chunk-tasks 5000 --sample-tasks 100 --workers 4 --cores 16 \
  --batch-type local --timeout 3600 --output-dir RESULT_DIR

DATAVINE_GO_BINARY=/users/jzhou24/graph_optimization/factories/datavine_workflow_go-20260811 \
DATAVINE_REGRESSION_REPORT=REPORT.json \
conda run -p /groups/dthain/users/jzhou24/miniconda/envs/datavine \
  bash acceptance/scripts/run_regression.sh
```

## Evidence

- Delivery index:
  `/project01/ndcms/jzhou24/datavine-benchmarks/static-ir-v2-delivery-20260823.json`
- Million-task gate:
  `/project01/ndcms/jzhou24/datavine-benchmarks/static-ir-v2-native-1m-4x16-20260823.json`
- 100k/1M-I/O gate:
  `/project01/ndcms/jzhou24/datavine-benchmarks/static-ir-v2-compact-100k-1mio-4x16-20260823/summary.json`
- Live-loss recovery:
  `/project01/ndcms/jzhou24/datavine-benchmarks/static-ir-v2-compact-recovery-fixed-20k-4x16-r4-20260823/summary.json`
- Final regression:
  `/project01/ndcms/jzhou24/datavine-benchmarks/static-ir-v2-final-regression-rerun-20260823.json`

## Explicit OPEN boundaries

- Dynamic-workflow optimization is intentionally deferred.
- Store retains compact raw JX documents for replay and debugging. A packed
  post-seal arena could reduce memory further, but it adds lifecycle and replay
  complexity and is not required by the current gate.
- Compact IR scale and compact IR recovery passed separately. The exact
  combined one-million-task plus 100-worker-removal campaign was not rerun.
- Results are frozen in the production source checkpoint tagged
  `datavine-production-v1-20260823`; exact source hashes remain in the delivery
  index and repository handoff manifest.
