# Architecture Review A — Before Pruning

Date: 2026-07-29
Reviewed commit: `1045fa2dd067078dc5e7fe5bdbeb83cb994e45aa`
Status: **FAIL — CRITICAL FINDINGS OPEN**

This is deliberately a failing review. Phase 8 component acceptance does not
satisfy the Ultimate Acceptance Goal.

## Findings

### A1 — One logical authority exists, but physical truth is incomplete

The independent Controller owns EData/IData/Task records, lineage,
availability, and durability. The Scheduler and workers do not import the
legacy `vine_graph` semantic runtime.

However, the Controller has no physical replica records, worker epochs,
transfer leases, or source invalidation. “Global loss” is currently injected
by deleting Controller bytes rather than derived from replica state. Therefore
sole authority over global availability and pruning is not yet real.

Required correction: add Controller-owned replica/worker/source state with
epochs, validated transitions, cleanup, and explainable availability.

### A2 — Lineage is stable only for the single-output prototype

TaskID and one IDataID remain stable across the tested replay. TaskRecord has
one `output_data_id`; multiple output slots, partial downstream demand, and
per-slot durability/pruning are absent.

Required correction: introduce stable output-slot records and multi-output
bindings without merging byte-identical logical outputs.

### A3 — No legacy recovery-task correctness dependency

The new runtime uses ordinary logical tasks for replay and contains no
`vine_graph` import. This question passes for the independent runtime.

The older package remains frozen reference code and must not regain authority.

### A4 — Workers do not secretly own semantics, but Controller memory does

TaskVine worker files are treated as soft physical cache. Yet the Controller
retains every IData byte and all admitted EData bytes, making central memory a
hidden stable source and preventing a realistic volatile-replica model.

Required correction: separate metadata from bulk bytes, implement bounded
stable origins and large-object bypass, then derive global loss from accepted
replicas and durability.

### A5 — Data movement and scheduling are only partially separated

Workers pull URL inputs and TaskVine can peer-transfer them. The Scheduler,
however, creates one special TaskVine task per prefetch and the single-threaded
Controller HTTP server synchronously serves both control and bytes. Burst
backpressure, bounded transfer concurrency, batching, and source load are
absent.

Required correction: bounded data-plane admission, batch resolution, explicit
source candidates/load, and a reviewed bounded prefetch mechanism.

### A6 — Policy rollback is correct only in small isolated tests

Peer-off, prefetch-off, persistence-off, corruption fallback, and one recovery
case retain exact outputs. There is no proof under cancellation, repeated
recovery, pruning, large data, overload, or reordered events.

Required correction: deterministic combined fault/state-transition harness.

### A7 — Pruning correctness is entirely absent

The independent `recovery/` package is empty and has no reference or
incremental minimum-recoverable-cut algorithm. There are no proof records,
local deletions, SharedFS quarantine, dynamic invalidation, or storage-decline
metrics.

Required correction: complete shadow proof before enabling any deletion.

### A8 — Queues and metadata are not fully bounded

Persistence and Scheduler command queues use unbounded `queue.Queue`.
Controller metadata/history has no workflow cleanup. Controller HTTP serving
has no explicit byte/concurrency admission. Hot paths perform repeated
per-task HTTP lookups and Phase 8 fanout analysis scans all tasks.

Required correction: explicit queue capacities/backpressure, workflow
lifecycle cleanup, batch APIs, incremental indexes, and high-water metrics.

### A9 — Grand Challenge and comparative thesis evidence do not exist

Accepted tests have at most single-digit logical tasks. There is no 10k-task
workflow, Legacy-equivalent driver, eight-mode matrix, three-run statistics,
resource sampler, storage budget failure, or architectural scaling result.

Required correction: build the committed workload and metric/failure harness
after correctness mechanisms exist.

### A10 — Traceability is not yet acceptable

Phases 0–8 were consolidated into one 6,712-line code commit and history
repeats that commit for multiple responsibilities. This violates the final
reviewability requirement even though the working tree and tested package were
clean.

Required correction: all new major responsibilities receive focused commits;
before final acceptance, make history/checkpoints accurate and meaningfully
bisectable, including remediation of the consolidated prototype history.

## Question outcomes

| Review A question | Result |
|---|---|
| Exactly one data authority? | FAIL — physical availability/pruning authority incomplete |
| Lineage stable across retries? | PARTIAL — one output only |
| Legacy recovery task on correctness path? | PASS |
| Worker replica acting as semantic ownership? | PASS, but Controller bytes mask realistic ownership |
| Controller carries unnecessary bulk bytes? | FAIL |
| Data movement genuinely separate? | FAIL |
| Cache/persistence policy independent of correctness? | PARTIAL |
| Module boundaries clear? | PARTIAL — empty recovery/pruning and oversized scheduler |

## Exit criteria

Review A becomes PASS only after A1–A8 have implemented designs, invariant
tests, and boundedness evidence sufficient to begin deletion safely. A9 and A10
may remain tracked for later gates, but cannot be ignored.
