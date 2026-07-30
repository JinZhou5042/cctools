# Architecture Review B — After Shadow Pruning

Date: 2026-07-29
Reviewed through code commit: `fbddcc70d`
Status: **FAIL — SHADOW PROOF PASSES, PHYSICAL DELETION IS UNSAFE**

## Accepted shadow evidence

- A full-scan reference algorithm and event-indexed incremental algorithm made
  identical semantic decisions across 40 deterministic random DAGs, 80 tasks
  per graph, and 6,400 state events.
- The deterministic graph covers fan-out/fan-in, multiple roots, a join,
  multiple logical outputs, mixed volatile/durable branches, two recovery
  cycles, queued persistence cancellation, and dynamic graph growth.
- Every decision contains stable reasons, recovery targets, and the current
  graph/state revision.
- Recovery depth is observable and decreases to zero when a demanded output
  becomes durable.
- An independent test removes every proposed candidate and verifies all live
  obligations can still be reproduced from retained durable anchors or stable
  root lineage.
- The maximum incremental lineage scan was 64 nodes in the accepted 84-IData
  random graphs; no update performed an unconditional full-graph scan.

Machine-readable evidence:
`acceptance/artifacts/phase9-shadow-20260729.json`.

## Critical findings

### B1 — Logical availability is not a physical replica model

The proof consumes one `available` and one `durable` bit per IData. It cannot
distinguish worker DRAM, worker disk, peer, Controller memory, SharedFS,
quarantine, active reads, or replicas from stale worker epochs.

Required correction: Controller-owned physical replica records, epochs,
locations, tiers, leases/readers, and validated loss transitions.

Correction checkpoint `fbddcc70d` connects qualified, tiered, generation- and
epoch-checked physical records to real Controller, Scheduler, and worker
protocol events. TaskVine worker loss invalidates available and preparing
replicas, and late completion fails closed. B1 is satisfied for currently
reported Controller-memory, SharedFS, and worker-disk replicas. DRAM admission
and large-data stable origins remain separately open under B2 and the cache
acceptance rows.

### B2 — Stable-root reproducibility is assumed, not proved by Controller state

The independent safety oracle assumes task metadata and all root EData remain
stable. Current Controller capacity rejection and in-memory EData do not prove
that assumption for bulk data, Controller restart, or eviction.

Required correction: explicit stable-origin records and large-data bypass with
content validation and lifecycle protection.

### B3 — Active and recovery consumers are modeled but not runtime-connected

The algorithms distinguish active direct consumers and recovery anchors.
Scheduler task transitions, late completion, dynamic submission, cancellation,
and retry epochs do not yet drive this model through the Controller protocol.

Required correction: versioned Controller events and fail-closed integration
tests for reordered/duplicate/stale transitions.

Worker incarnation and publication transitions are now runtime-connected at
`fbddcc70d`, but consumer lifecycle, cancellation, and atomic dynamic graph
growth are not. This finding remains open.

### B4 — Persistence cancellation is advisory only

Correction checkpoint `17577b058` adds bounded request generations, queued and
active cancellation, a defined atomic too-late boundary, and stale-completion
rejection. The deterministic direct and HTTP tests pass.

This finding remains open because the shadow pruning plan does not yet invoke
the runtime cancellation API atomically with its proof revision.

Required correction: bounded request IDs/generations, queued cancellation,
defined active cancellation, and stale-completion rejection.

### B5 — No in-flight transfer/read protection

A candidate can be logically prunable while a worker is reading or serving it.
The shadow model has no transfer lease, source load, or concurrent reader
state.

Required correction: source leases or equivalent reference protection tied to
replica epochs, with deterministic race tests.

The authenticated protocol can acquire and release bounded epoch-checked
source leases, but TaskVine's actual byte transfer path does not call it yet.
The component race is necessary but insufficient; this finding remains open.

### B6 — Dynamic invalidation is correct only inside the shadow object

Adding a task updates incremental direct-consumer indexes and invalidates the
old proof. There is no atomic ordering between real graph submission,
Controller lineage mutation, and a pruning executor.

Required correction: Controller revision compare-and-apply for every deletion
or quarantine action.

### B7 — SharedFS quarantine and hard-delete proof do not exist

There is no grace period, recovery from quarantine, audit record, hard-delete
revalidation, or pin/final-output enforcement at the filesystem boundary.

Required correction: quarantine state machine and executor after B1–B6.

## Question outcomes

| Review B question | Result |
|---|---|
| Recoverability model complete? | FAIL — runtime epochs exist; stable bulk origins and transfer coupling remain open |
| Every shadow decision explained? | PASS |
| Active and recovery consumers distinguished? | PASS in shadow, not runtime |
| Durability coverage handles branches/joins? | PASS in shadow |
| Persistence cancellation ordered safely? | FAIL |
| Dynamic growth invalidates proofs? | PASS in shadow, not atomic at runtime |
| Incremental equivalent to reference? | PASS for accepted deterministic suite |

No local or SharedFS deletion may be enabled while B1–B7 remain unresolved.
