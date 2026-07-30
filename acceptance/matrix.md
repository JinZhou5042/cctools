# Ultimate Acceptance Matrix

Status values are `OPEN`, `PASS`, `FAIL`, or `OUTSIDE-FAULT-MODEL`. A PASS
requires a command, exact commit, and machine-readable artifact. Blank evidence
is never a pass.

| ID | Requirement group | Status | Evidence / blocker |
|---|---|---|---|
| GC-SCALE | 10k tasks, 100k bindings, churn, storage pressure | OPEN | Grand Challenge not implemented |
| GC-SHAPE | All graph/data shapes in one workflow | OPEN | Grand Challenge not implemented |
| GC-MODES | Eight mandatory comparison modes | OPEN | Unified mode runner absent |
| GC-LEGACY | Architectural Legacy limit demonstrated | OPEN | Comparable Legacy driver absent |
| CORRECT | Exact oracle, failures equal normal, safe explicit failure | OPEN | Small Phase 4–8 cases only |
| MULTIOUT | Multiple outputs and partial downstream demand | FAIL | One IData output per TaskRecord |
| EDATA-ID | Independent function/arg/kwarg/file identity and collision checks | OPEN | Function/value/container domains, repeated-reference one-time serialization, bulk hash/path validation pass at `13193c99a`; dependency-file path and explicit collision injection remain |
| IDATA-ID | Stable output-slot identity and complete explainable lineage | OPEN | Runtime lineage includes nested dependencies at `347f60531`; multi-output remains absent |
| LIGHTWEIGHT | Compact dispatch/queues scale with IDs and bindings | OPEN | Small records exist; 100k-binding bound unmeasured |
| SERIAL | Exactly-once serialization and byte-preserving movement | OPEN | A repeated 4 MiB EData object serializes once at `13193c99a`; a 2 MiB IData is serialized/fsynced once per attempt and metadata-published without Controller byte retention at `53db69f1e`; full movement fault proof remains |
| AUTHORITY | Controller sole data/lineage/durability/pruning authority | FAIL | Actual TaskVine peer sources require Controller authorization at `ef605c343`; consumer lifecycle and restart authority remain incomplete |
| STATE | Validated logical/physical/durability/recovery/pruning transitions | OPEN | SharedFS transitions and acknowledged worker-local deletion pass at `3f993f15b`; full races remain |
| CTRL-BOUND | Bounded memory, serving, metadata, queues, cleanup | FAIL | Request/byte admission passes at `d694bef4a`; EData bulk bypass at `13193c99a`; retained IData total/object bounds plus metadata-only large-IData bypass at `53db69f1e`; metadata/history cleanup remains absent |
| CTRL-FAIL | Auth, idempotency, epochs, stale/partial/restart behavior | FAIL | Runtime epochs and stale completion pass at `fbddcc70d`; Controller-owned reconnect claims pass at `643cddd68`; restart/auth-isolation contract remains absent |
| SCHED | Independent data progress, minimal rollback, fairness, termination | OPEN | Future-used IData rematerializes with ordinary logical attempts at `f1237b8b8`; worker-only large IData loss rolls back through the same logical task with zero legacy recovery at `53db69f1e`; repeated/minimal rollback, fairness, and termination combinations remain open |
| WORKER-PREP | Batched validated resolution with direct source fallback | FAIL | Controller returns validated candidates; worker still resolves per object without direct candidate pulls |
| CACHE | Strict DRAM/disk bounds, admission, eviction, zero mode | FAIL | Worker/Manager bounds plus future-used IData eviction/rematerialization remain within six items and the byte limit at `f1237b8b8`; DRAM, active-transfer eviction, true process loss, soft-metadata cleanup, and scale cost remain open |
| PREFETCH | Bounded/cancellable/priority-safe independent progress | OPEN | Byte/item/priority gates pass; unverified prefetched replicas safely fall back at `ef605c343`; concurrency/cancel/final architecture open |
| PUBLISH | Exactly-once staged idempotent publication and cleanup | OPEN | Two-phase worker prepare/Scheduler commit passes; large output publishes attempt/hash/size without byte POST at `53db69f1e`; full publication fault matrix open |
| PLACE | Multi-source, load/epoch, bulk bypass, peer fallback | OPEN | Actual TaskVine peer movement acquires epoch-checked Controller leases and unverified sources fall back at `ef605c343`; transfer-loss/load adaptation remain open |
| PERSIST | Bounded/cancellable/backpressured atomic durability | OPEN | Controller-inline queue/cancel/overload passes at `17577b058`; worker-driven 2 MiB atomic persistence and durable return pass at `4e8f19f1f`; active external cancellation before acknowledgement and during final compare-and-commit passes at `426ea2195`; two partial-write failures, bounded retry/exhaustion, unified global write admission, cleanup, and compute overlap pass at `c4d5258b6`; persistence/global-loss/pruning races and scale I/O limits remain open |
| RECOVERY | Replica-aware repeated minimal recovery from frontier | FAIL | One ordinary replay of a lost worker-only 2 MiB IData completes with stable IDs and zero special TaskVine recovery tasks at `53db69f1e`; repeated loss, minimum rollback, and durability-frontier bounds remain absent |
| PRUNE-SHADOW | Reference/incremental equivalence and proof records | PASS | `artifacts/phase9-shadow-20260729.json`, commit `2108b68a8` |
| PRUNE-LOCAL | Safe DRAM/disk pruning with declining storage | OPEN | Multi-replica deletion plus generation-exact targeted dead-data eviction and pending-ACK worker-loss cleanup pass through `c20db01a1`; active-read races, DRAM pruning, and recovery-after-prune remain |
| PRUNE-SHAREDFS | Quarantine/grace/recovery/hard-delete audit | OPEN | Real revision-safe component path passes at `347f60531`; restart persistence, pins, and scale comparison open |
| MIN-CUT | Observable minimum recoverable cut/frontier/depth | FAIL | Not implemented |
| RACES | Mandatory cross-component race/corner-case matrix | OPEN | Unified deterministic harness absent |
| PERF-MGR | Manager/Controller/serialization/metadata metrics | FAIL | Required independent resource metrics absent |
| PERF-WORKER | Cache/staging/peer/overlap/idle metrics | FAIL | Partial transfer counts only |
| PERF-FS | Bounded read/write/metadata/storage metrics | FAIL | Unified write high-water and worker bytes are recorded at `c4d5258b6`; separate read/write, metadata, peak-storage, and scale metrics remain absent |
| PERF-COMP | Three repetitions, median/variation, scaling cause | FAIL | Not run |
| REVIEW-A | Before-pruning architecture review | FAIL | `reviews/review-a.md` |
| REVIEW-B | After shadow-pruning review | FAIL | `reviews/review-b.md`; physical deletion blocked |
| REVIEW-C | Final architecture review | OPEN | Blocked on all acceptance work |
| QUALITY | Modular ownership, invariants, cleanup, lint/sanitizers | OPEN | Phase 8 lint passes; ultimate implementation not reviewed |
| GIT | Reviewable commits/checkpoints/builds/artifacts/bisection | FAIL | Earlier phases consolidated into one code commit |
| REPRO | One-command build/regression/challenge and clean runs | FAIL | Acceptance entry points absent |
| THESIS | Direct evidence for every paper-thesis clause | FAIL | Grand Challenge comparison absent |

## Mandatory cross-component tests

All remain OPEN until a deterministic committed test and artifact exists:

- cache eviction during resolution;
- worker loss during peer transfer and after local publication;
- persistence completion concurrent with global-loss detection;
- pruning concurrent with persistence and dynamic consumers;
- duplicate publication and late old-attempt completion;
- prefetch racing demand for one DataID;
- multiple destinations fetching one source;
- late source-load updates and Controller eviction during read;
- zero-byte, very large, and over-capacity objects;
- equal IData bytes with different lineage and serialization-domain separation;
- cyclic/aliased graphs, nested OutputRefs, and multi-output partial demand;
- task cancellation, workflow interruption, delayed workers, repeated churn;
- temporary SharedFS/Controller overload and all-optimizations-disabled mode.

Standalone physical-directory evidence at commit `e1843b9bd` now covers two
destinations fetching one source, source invalidation during active reads,
partial publication, stale generations/epochs/attempts, zero-byte replicas,
and metadata-capacity overload. These checklist rows remain OPEN until the
same races pass through the actual Controller/worker/runtime protocol.

Protocol evidence at commit `fbddcc70d` additionally covers authenticated
worker incarnation tracking, real worker-loss reconciliation, two-phase output
replica publication, corrupt logical-identity rejection, stale/late reports,
foreign invalidation, distinct equal-byte IData lineage, and zero-byte data.
Actual peer movement now acquires and releases Controller source leases at
`ef605c343`; three local repetitions and factory peer-on/off runs have balanced
lease counts and no active leaks. Transfer races remain OPEN because worker
loss during an active transfer and release-timeout retry are not yet injected.

Physical pruning evidence at commit `347f60531` covers persistence cancellation
concurrent with an active write, pruning with an active source lease, stale
proof rejection, dynamic-consumer invalidation, corrupt quarantine, validated
restore, grace enforcement, and hard deletion. Cross-component rows remain
OPEN until the real TaskVine transfer and worker-cache deletion paths
participate and the Grand Challenge repeats the schedule at scale.

Worker-local pruning evidence at commit `3f993f15b` covers real TaskVine cache
deletion, multiple replicas of one IData, UUID-correlated duplicate/stale ACK
rejection, exact Controller generation confirmation, cache decline, and
bounded acknowledgement cleanup locally and through factory workers. Worker
cache checkpoint `c20db01a1` additionally covers two-worker dead-data
retention, zero-retention correctness, and pending-unlink worker loss. The
pending tracker is released as an explicit failure without falsely claiming
physical deletion of a possibly retained workspace. Recovery after eviction,
eviction during an active demand read, and strict instantaneous admission
remain OPEN.

Dispatch-admission evidence at `88b7d1a44` closes the scoped instantaneous
item-bound gap for a cooperating DataVine Manager: both local and factory
workers remain at or below six physical items while admission backpressure is
observed, and capacity five is rejected for a six-item task. The overall row
remains FAIL because the worker has no independent hard limit, byte and DRAM
capacities are absent, prefetch and recovery combinations are untested, and
the hot-path projection has not been bounded at Grand Challenge scale.

Worker-enforced capacity evidence at `6d3b77042` adds hard item/byte checks in
the worker, pre-execution output-slot reservation, explicit publication
failure, and unlink-acknowledgement ordering. The installed-path suite passes
15/15 tests. Local and package-only factory recovery workflows combine
prefetch, eviction, worker disconnection, and one ordinary-task replay while
remaining at or below six physical items and the configured byte limit. This
does not close `CACHE`: DRAM is not implemented, worker release is not a
process-kill fault, and active-transfer eviction plus Grand Challenge scale
remain untested. It also does not close `RECOVERY`: an attempted policy for
evicting future-used volatile IData fell into TaskVine's prohibited special
recovery-task path and was reverted.

IData rematerialization evidence at `f1237b8b8` removes that rejected
dependency. An attempt-qualified Controller URL is now the writable physical
cache identity and stable fallback for each IData attempt. Local and
package-only factory runs delete future-used IData, reconstruct exact results,
perform one ordinary logical-task replay, and report zero TaskVine recovery
tasks. The first factory attempt exposed a worker-release/eviction generation
race and was rejected; the accepted Controller-atomic operation validates the
observed attempt, hash, size, WorkerID, and epoch before invalidating the
current generation. `CACHE` remains FAIL because DRAM, active-transfer loss,
true process death, soft-record cleanup, and scale remain open. `RECOVERY`
remains FAIL because only one replay occurs and no durability-frontier bound is
proved. `CTRL-BOUND` remains FAIL because ordinary IData bytes are still held
by the Controller.

Bounded-IData evidence at runtime commit `53db69f1e` and test commit
`e6ef08b16` removes the ordinary-byte-retention blocker without claiming the
whole row. A 2 MiB intermediate exceeds the 64 KiB inline limit and is
metadata-published twice across one worker-loss recovery; Controller IData
high-water remains 79 bytes under a 128 KiB capacity. The local and rebuilt
package factory runs return the exact oracle through three ordinary attempts
and zero TaskVine recovery tasks. `CTRL-BOUND` remains FAIL because logical
metadata and completed history are not reclaimed. `PERSIST` remains OPEN
because metadata-only IData has no worker-driven durable write path.
`RECOVERY` remains FAIL because only one loss cycle is covered and no
durability-frontier bound is proved. Evidence:
`artifacts/idata-capacity-e6ef08b16.json`.

Worker-persistence evidence at `4e8f19f1f` adds a bounded data-operation path
for metadata-only IData. The Controller owns request identity and durability,
the worker validates and atomically publishes directly to SharedFS, and the
Controller stream-validates outside its state lock before compare-and-commit.
The same 2 MiB value is consumed downstream and returned as a durable final
after worker churn, with Controller IData high-water still 79 bytes.
Duplicate identical begin/complete is idempotent. `PERSIST` remains OPEN
because active external cancellation, SharedFS overload/failure retry,
fairness, and the full failure schedule are not proved. Evidence:
`artifacts/worker-persistence-4e8f19f1f.json`.

Controller admission evidence at commit `d694bef4a` covers hard request-thread,
byte-response concurrency, and in-flight-byte capacities; immediate overload;
oversized response rejection; telemetry high-water marks; and cleanup after a
held response. Twenty component repetitions and a rebuilt-package two-worker
factory workflow pass. `CTRL-BOUND` remains FAIL because rejecting a large
object is not the required stable bulk-data bypass.

Stable-origin evidence at commits `13193c99a` and `643cddd68` covers a
4,194,313-byte repeated EData object with 1 MiB Controller memory and serving
limits, one serialization, one EDataID, exact alias reconstruction, hash/path
validation, serialization-domain separation, and a rebuilt-package two-worker
factory run. The first factory recovery exposed and rejected a hard-coded
worker-epoch bug; Controller-owned idempotent incarnation claims then passed
the same recovery schedule. `CTRL-BOUND` remains FAIL because completed
metadata/history cleanup is incomplete, and `PLACE` remains FAIL because
worker-cache admission and the remaining transfer failure races are open.

## Paper-thesis evidence map

| Clause | Status | Required final evidence |
|---|---|---|
| Workflow-owned semantics | OPEN | stable identity/lineage/authority under churn |
| Serialized identity | OPEN | dedup, indexing, preserved bytes, boundary deserialize |
| Distributed movement | OPEN | demand/peer/multi-source/bounded central bytes |
| Distributed caching | FAIL | bulk bypass passes; bounded DRAM/disk admission and eviction remain |
| Controlled persistence | FAIL | admission, retry, backpressure, cancellation |
| Re-realization | OPEN | repeated ordinary-task recovery from frontier |
| Disposable workers | OPEN | churn across execution/transfer/persist/recovery/prune |
| Reduced storage demand | FAIL | frontier progression and pruning comparison |
