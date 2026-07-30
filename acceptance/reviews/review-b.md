# Architecture Review B — After Shadow Pruning

Date: 2026-07-29
Reviewed through code commit: `0a5eefbd0`
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

Correction checkpoint `88b7d1a44` adds Manager-coordinated dispatch admission.
The projection accounts for cached files, assigned outputs, the candidate
working set, and unacknowledged physical deletion. Local and factory runs keep
both workers at the six-item limit and reject an impossible five-item limit
before execution. B1 remains open because enforcement is not worker-local,
bytes and DRAM are unbounded, and prefetch/recovery/churn combinations have not
passed.

Correction checkpoint `6d3b77042` adds independent worker-side item/byte
enforcement, reserves output slots before execution, and orders new dispatch
after physical unlink acknowledgement. Combined prefetch, cache pressure, and
one ordinary-task recovery replay pass locally and through two package-only
factory workers without exceeding configured disk limits. B1 remains open
because DRAM is absent, the fault is Manager-triggered worker release rather
than a process kill, and active demand/peer transfers are not yet protected
through eviction.

Self-review also proved that future-used volatile IData cannot safely be
evicted through the current TaskVine temporary-file interface: the experiment
caused TaskVine to submit its special legacy recovery task and exposed a
stale-generation prune acknowledgement. The policy was reverted. Closing B1
therefore requires DataVine-owned IData materialization and normal logical
producer invalidation, not a permissive cache rule.

Correction checkpoint `f1237b8b8` supplies that materialization path. IData
uses an attempt-qualified Controller URL as one writable TaskVine cache
identity and stable fallback; missing physical replicas no longer invoke
TaskVine TEMP recovery. Local and factory cases evict future-used IData and
report zero special recovery tasks. The first factory attempt exposed and
rejected a stale-generation eviction race. The accepted operation validates
the complete observed identity under the Controller lock before invalidating
the current generation.

B1 remains open because the Controller still stores ordinary IData bytes,
DRAM is absent, true worker process loss is untested, and active peer-transfer
eviction has not passed. The current solution establishes authority but does
not yet establish the required volatile worker-local/bulk-data behavior.

Correction checkpoint `53db69f1e` removes ordinary large-IData retention from
the Controller. A configurable inline threshold keeps only small stable
fallbacks; a 2 MiB output publishes immutable attempt/hash/size metadata and
remains in the worker/peer cache identity. Loss of its only worker replica
reuses the original logical task and IDataID, and local plus package-only
factory runs report zero legacy recovery tasks. Controller retained-IData
high-water is 79 bytes under a 128 KiB limit.

B1 still remains open. Worker DRAM is absent, active-transfer loss is not
covered, and large worker-only IData cannot yet enter durable storage or
return as a large final result without a new worker-driven path. Controller
metadata/history cleanup also remains unresolved under `CTRL-BOUND`.

Correction checkpoint `4e8f19f1f` supplies that worker-driven path. Controller
owns a bounded persistence request and target, worker validates and atomically
writes SharedFS, and Controller validates outside its global lock before an
identity-checked durability commit. The resulting source supports downstream
tasks, durability validation after volatile loss, and return of a 2 MiB final
without Controller byte retention.

B1 remains open for worker DRAM, active transfer/cancellation, SharedFS
overload retry, Controller metadata cleanup, and scale/fairness evidence.

Correction checkpoint `c4d5258b6` adds bounded worker-persistence failure
recovery. Two package-worker partial writes fail and clean their temporary
files before a new request reaches durable. One Controller active set now
limits Controller-inline and worker writes together; self-review rejected the
first passing version after it reached global high-water two under capacity
one. Normal compute completes while persistence is active, and Controller
state remains responsive during deliberately blocked stream validation.

B1 remains open for worker DRAM, active peer-transfer loss, persistence
completion concurrent with global loss/pruning, metadata cleanup, and
scale-level I/O/fairness evidence.

Correction checkpoint `aac966a09a` closes one persistence/global-loss/pruning
interleaving in the real runtime. Scheduler protects a writing IData from
pruning, cancels its exact persistence request on global loss, waits for both
that physical task to drain and the old worker-cache prune acknowledgement,
then reuses the original logical task for recomputation. Three local
repetitions and a two-worker package-only factory run complete two recovery
cycles with zero legacy recovery tasks. Failed intermediate designs exposed
and corrected HTTP error-body cache admission and request-ID barrier races.

B1 remains open for DRAM, active peer-transfer loss, metadata cleanup,
minimum-cut/frontier recovery, and scale-level I/O/fairness evidence.

Correction checkpoint `fafead8bde` adds a scoped real minimum-recoverable-cut
path. Controller-selected unique volatile replica owners are shut down as
processes twice. Recovery reuses original tasks in rollback waves of depth four
and three. Once task 5 is durable, IData 2–4 are physically deleted with exact
worker acknowledgements, and the second loss recovers only tasks 6–8. Failed
prototypes exposed and corrected a nested `manager.wait` completion-loss race,
non-persistence frontier gating, lost compute/persistence overlap, reconnecting
release semantics, and failure injection that did not target the replica owner.

B1 remains open. The safe runtime pruning path currently drains all submitted
compute, prefetch, and persistence work before its bounded acknowledgement
loop; this is a correctness barrier, not independent asynchronous pruning.
Worker DRAM, active-transfer loss, multi-branch minimum-cut optimality,
metadata cleanup, and scale-level I/O/fairness evidence remain absent.

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

Correction checkpoint `426ea2195` extends cancellation to the worker-driven
large-IData path. A real package-only factory workflow cancels after the
worker's atomic SharedFS publication and retries to durable. Self-review
rejected the first passing version because it did not cover cancellation
between Controller stream validation and final compare-and-commit. The
accepted deterministic threaded race blocks at that boundary and proves the
cancelled request cannot acknowledge durability, removes its target, releases
admission, and can be retried. B4 remains closed for the current fault model;
SharedFS unavailability/overload and pruning/global-loss combinations remain
separate open race requirements.

Checkpoint `aac966a09a` additionally proves that global loss during worker
persistence cannot let the cancelled request publish durability or let a
different request for the same IDataID release recovery early. The pruning
proof moves from protected `persistence-writing` to non-prunable
`no-accepted-replica`, then ordinary recomputation reaches durable. B4 remains
closed; other transfer/pruning and scale combinations remain open elsewhere.

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
repeated frontier-bounded recovery after eviction, true process loss, and
eviction concurrent with an active demand read remain additional blockers.

Correction checkpoint `a8bd9609c` removes the Scheduler's global
frontier-pruning drain barrier and proves that unrelated compute completes
while generation-exact worker deletions await acknowledgement. It does not
close Review B. A Controller replica with an active source lease transitions
to retiring, but the Scheduler currently fails closed instead of waiting for
lease release, revalidating the proof/generation, and completing deletion.
Dynamic consumer insertion during that wait is also untested. Those are
critical blockers to accepting concurrent active-read pruning.

Correction checkpoint `a1d273444` closes the Controller-level active-lease
ordering identified above. Pruning is deferred without hiding alternate
sources, duplicate apply is idempotent, and continuation revalidates both the
newest proof and replica generation. If an existing output becomes required
during the wait, retirement is cancelled and the replica is restored rather
than deleted. The package-only two-mode factory E2E is recorded in
`../artifacts/pruning-lease-race-a1d273444.json`.

Review B remains **FAIL**. The lease test does not interrupt a real peer byte
transfer, source-worker epoch loss does not yet prove bounded cleanup of every
outstanding lease, and a continuation response lost after successful
processing cannot yet be replayed from a bounded terminal record. Dynamic
registration of a new consumer is also not exercised; the accepted test
invalidates the proof through an existing output becoming required. These
remain critical before transfer-coupled pruning can pass this review.

Correction checkpoint `f737147e7` adds bounded dead-epoch cleanup and terminal
protocol replay. Loss of either the source or destination worker incarnation
fails its leases and releases exact source load; a retiring replica becomes
invalid when the final lease is gone. Pruning continuation uses a stable
operation ID, and an injected response loss after Controller commit is
recovered with one same-ID Scheduler retry. Tombstones have independent item
and byte bounds and do not retain repeated full-graph plans. Evidence is
`../artifacts/lease-epoch-idempotency-f737147e7.json`.

Review B remains **FAIL**. Epoch loss is proven at the Controller protocol
boundary, but no package-only run kills the actual source process while a
throttled peer byte transfer is in flight. The current scoped pruning lease
test also uses one real active worker epoch as both selected source and lease
destination rather than proving two distinct live peers. Transfer fallback,
partial-byte cleanup, and pruning completion after that real process failure
are the next critical correction.

Correction checkpoint `9afe1a64b` kills the actual source worker process group
only after the exact lease destination reports that its transfer child has
forked. Local evidence includes source return code `-9`, no surviving transfer
server/PGID, two concurrent failed/released leases, zero active leases, a
stable-origin fallback read, and the exact oracle. The rebuilt-package
two-worker factory reproduces the same semantic result and removes its
remaining worker at shutdown. Evidence is
`../artifacts/peer-source-loss-9afe1a64b.json`.

Review B remains **FAIL**. Transfer-child start is not proof that a positive
number of bytes crossed the peer connection, and the E2E does not retain and
audit the destination transfer temporary path after interruption. Corrupt
surviving peer fallback and real-transfer failure concurrent with pruning
continuation are also absent. Those must pass, together with the existing
restart/dynamic-consumer blockers, before transfer-coupled pruning can pass
this review.

Correction checkpoint `b5f2ec21c` proves a positive destination-side write
strictly below the complete 48,600,009-byte object before source-process-group
loss. The failed transfer removes its exact temporary path before invalidation;
a TransferID/WorkerID-bound audit record crosses active-lease deletion and is
consumed exactly once. Three local repetitions and a package-only factory run
end with one absent-path cleanup report, zero pending audit records, zero
active leases, stable-origin fallback, and the exact oracle. Evidence is
`../artifacts/peer-partial-loss-b5f2ec21c.json`.

Review B remains **FAIL**. The checkpoint does not corrupt a surviving peer or
prove fallback to a second peer candidate, and it does not run physical
pruning continuation concurrently with the byte-counted failure. Controller
restart and dynamic-consumer invalidation also remain unresolved.

Correction checkpoint `0a5eefbd0` keeps the corrupt peer alive, validates a
raw serialized-byte SHA-256 at the destination before cache publication,
rejects the bad replica, excludes the failed source WorkerID, and completes
from a different READY peer. Three local and three rebuilt-package factory
repetitions have one injected corruption, one rejection, one alternate-source
success, balanced leases, zero worker disconnections, and the exact oracle.
Evidence is `../artifacts/peer-corruption-0a5eefbd0.json`.

Review B remains **FAIL**. Integrity and alternate-source behavior are now
proved independently, but no E2E overlaps a real active transfer lease with a
deferred physical pruning decision and then revalidates the proof after
transfer success or failure. Controller restart, dynamic-consumer invalidation
during that exact window, release timeout, and scale remain unresolved.
