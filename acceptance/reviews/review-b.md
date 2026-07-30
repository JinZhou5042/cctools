# Architecture Review B — After Shadow Pruning

Date: 2026-07-29
Reviewed through code commit: `c20db01a1`
Status: **FAIL — SCOPED PHYSICAL DELETION PASSES, CRITICAL GAPS REMAIN**

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

Correction checkpoint `3f993f15b` adds acknowledged physical worker-disk
deletion. UUID operation IDs reject duplicate, stale, reordered, and
wrong-worker acknowledgements; Scheduler confirmation is generation-specific
and fails closed if Controller and TaskVine replica counts disagree. Five
local multi-replica repetitions and one factory E2E pass.

Correction checkpoint `c20db01a1` resolves pending unlink state before a
worker object is freed. Worker disappearance records an explicit failed
deletion, releases the tracker, and does not claim that a keep-workspace cache
was physically removed. Controller-authorized, generation-exact eviction now
keeps only records whose direct-consumer count has reached zero and passes
two-worker bounded-retention and zero-retention workflows. B1 remains open for
strict instantaneous disk admission, worker DRAM, active-demand read
protection, and recovery after eviction.

### B2 — Stable-root reproducibility is assumed, not proved by Controller state

The independent safety oracle assumes task metadata and all root EData remain
stable. Current Controller capacity rejection and in-memory EData do not prove
that assumption for bulk data, Controller restart, or eviction.

Required correction: explicit stable-origin records and large-data bypass with
content validation and lifecycle protection.

Correction checkpoint `d694bef4a` bounds Controller request threads, byte
response concurrency, and in-flight served bytes, and exposes saturation
metrics. The accepted test fails closed for a serialized object larger than
the serving budget. B2 remains open because rejection is not a stable bulk
origin or a usable large-data bypass.

Correction checkpoint `13193c99a` adds an explicit content-addressed stable
origin beneath a configured Controller root. The Controller validates
metadata-aware hash, size, regular-file type, path containment, and canonical
name without retaining the bulk payload. A 4,194,313-byte repeated object
completes through two factory workers while Controller memory and byte serving
are each limited to 1 MiB. B2 is improved but remains open because origin
mutation/replacement, Controller restart reconstruction, and lifecycle
cleanup are not yet proved.

### B3 — Active and recovery consumers are modeled but not runtime-connected

The algorithms distinguish active direct consumers and recovery anchors.
Scheduler task transitions, late completion, dynamic submission, cancellation,
and retry epochs do not yet drive this model through the Controller protocol.

Required correction: versioned Controller events and fail-closed integration
tests for reordered/duplicate/stale transitions.

Worker incarnation and publication transitions are now runtime-connected at
`fbddcc70d`, but consumer lifecycle, cancellation, and atomic dynamic graph
growth are not. This finding remains open.

Correction checkpoint `347f60531` adds complete TaskRecord IData dependency
edges, Scheduler task-state events, and Controller-serialized dynamic graph
growth. The E2E rejects stale proofs after both required-output mutation and a
new consumer. Cancellation semantics beyond the exercised completed/pending
states remain part of the final cross-component matrix, so B3 is improved but
not closed.

### B4 — Persistence cancellation is advisory only

Correction checkpoint `17577b058` adds bounded request generations, queued and
active cancellation, a defined atomic too-late boundary, and stale-completion
rejection. The deterministic direct and HTTP tests pass.

At checkpoint `17577b058` this finding remained open because the shadow plan
did not invoke runtime cancellation atomically with its proof revision.

Required correction: bounded request IDs/generations, queued cancellation,
defined active cancellation, and stale-completion rejection.

Correction checkpoint `347f60531` invokes cancellation inside the
Controller's compare-and-apply critical section. One queued request is
cancelled while another acknowledged writing request remains protected. B4 is
closed for the current fault model.

### B5 — No in-flight transfer/read protection

A candidate can be logically prunable while a worker is reading or serving it.
The shadow model has no transfer lease, source load, or concurrent reader
state.

Required correction: source leases or equivalent reference protection tied to
replica epochs, with deterministic race tests.

Correction checkpoint `ef605c343` binds qualified DataIDs to TaskVine files
and makes the actual peer-transfer path acquire and release bounded,
epoch-checked Controller leases. Unverified prefetch-created TaskVine replicas
are rejected by the Controller and fall back to the stable origin without
compute rollback. Three local repetitions and factory peer-on/off runs balance
every acquisition and release with no active lease leak.

This finding remains open because worker loss during the active byte transfer,
temporary Controller unavailability during release, and pruning concurrent
with those real transfers have not yet passed one deterministic E2E schedule.

### B6 — Dynamic invalidation is correct only inside the shadow object

Adding a task updates incremental direct-consumer indexes and invalidates the
old proof. There is no atomic ordering between real graph submission,
Controller lineage mutation, and a pruning executor.

Required correction: Controller revision compare-and-apply for every deletion
or quarantine action.

Correction checkpoint `347f60531` makes graph/state revisions part of the
authenticated apply request and rechecks each decision before mutation. A
dynamic consumer changes both the proof and hard-delete eligibility. B6 is
closed for the in-process Controller fault model.

### B7 — SharedFS quarantine and hard-delete proof do not exist

There is no grace period, recovery from quarantine, audit record, hard-delete
revalidation, or pin/final-output enforcement at the filesystem boundary.

Required correction: quarantine state machine and executor after B1–B6.

Correction checkpoint `347f60531` performs real owned-file rename, fsync,
source exclusion, checksum-validated restore, grace validation before unlink,
fresh-proof hard deletion, and bounded audit. This finding remains open because
pin coverage and restart-persistent quarantine/audit recovery are absent.

## Question outcomes

| Review B question | Result |
|---|---|
| Recoverability model complete? | FAIL — stable bulk origins and normal transfer coupling exist; mutation/restart and transfer-failure lifecycle remain open |
| Every shadow decision explained? | PASS |
| Active and recovery consumers distinguished? | PASS in shadow, not runtime |
| Durability coverage handles branches/joins? | PASS in shadow |
| Persistence cancellation ordered safely? | PASS in current fault model |
| Dynamic growth invalidates proofs? | PASS in runtime compare-and-apply |
| Incremental equivalent to reference? | PASS for accepted deterministic suite |

The reviewed local and SharedFS deletion paths may remain enabled only within
their proven fail-closed preconditions. No broader eviction, transfer-coupled
pruning, restart recovery, or automatic scale policy may be accepted until
B2, B3, B5, and B7 are closed. Strict disk admission, DRAM ownership,
recovery after eviction, and eviction concurrent with an active demand read
remain additional blockers.
