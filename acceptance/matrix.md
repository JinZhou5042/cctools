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
| EDATA-ID | Independent function/arg/kwarg/file identity and collision checks | OPEN | Values covered partially; dependency-file path absent |
| IDATA-ID | Stable output-slot identity and complete explainable lineage | OPEN | Single output stable; complete lineage API absent |
| LIGHTWEIGHT | Compact dispatch/queues scale with IDs and bindings | OPEN | Small records exist; 100k-binding bound unmeasured |
| SERIAL | Exactly-once serialization and byte-preserving movement | OPEN | Small tests exist; full movement/fault proof absent |
| AUTHORITY | Controller sole data/lineage/durability/pruning authority | FAIL | Standalone replica authority exists at `e1843b9bd`; runtime integration and pruning authority absent |
| STATE | Validated logical/physical/durability/recovery/pruning transitions | FAIL | Replica state machine passes standalone; combined runtime state is absent |
| CTRL-BOUND | Bounded memory, serving, metadata, queues, cleanup | FAIL | Replica metadata bounded; bulk bytes and HTTP/persistence queues remain unbounded |
| CTRL-FAIL | Auth, idempotency, epochs, stale/partial/restart behavior | FAIL | Replica epochs/idempotency pass standalone; protocol integration/restart contract absent |
| SCHED | Independent data progress, minimal rollback, fairness, termination | OPEN | Basic recovery/prefetch exists; combined cases absent |
| WORKER-PREP | Batched validated resolution with direct source fallback | FAIL | Per-object resolution; no multi-source Controller response |
| CACHE | Strict DRAM/disk bounds, admission, eviction, zero mode | FAIL | TaskVine disk reuse only; DataVine DRAM/admission metrics absent |
| PREFETCH | Bounded/cancellable/priority-safe independent progress | OPEN | Byte/item/priority gates pass; concurrency/cancel/final architecture open |
| PUBLISH | Exactly-once staged idempotent publication and cleanup | OPEN | Basic stage/fsync/publish passes; full fault matrix open |
| PLACE | Multi-source, load/epoch, bulk bypass, peer fallback | FAIL | Standalone multi-source/load/epoch logic passes; runtime still uses Controller URL plus TaskVine peer |
| PERSIST | Bounded/cancellable/backpressured atomic durability | FAIL | Atomic bounded workers exist; queue/cancel/overload missing |
| RECOVERY | Replica-aware repeated minimal recovery from frontier | FAIL | Single manual global-loss replay only |
| PRUNE-SHADOW | Reference/incremental equivalence and proof records | PASS | `artifacts/phase9-shadow-20260729.json`, commit `2108b68a8` |
| PRUNE-LOCAL | Safe DRAM/disk pruning with declining storage | FAIL | Not implemented |
| PRUNE-SHAREDFS | Quarantine/grace/recovery/hard-delete audit | FAIL | Not implemented |
| MIN-CUT | Observable minimum recoverable cut/frontier/depth | FAIL | Not implemented |
| RACES | Mandatory cross-component race/corner-case matrix | OPEN | Unified deterministic harness absent |
| PERF-MGR | Manager/Controller/serialization/metadata metrics | FAIL | Required independent resource metrics absent |
| PERF-WORKER | Cache/staging/peer/overlap/idle metrics | FAIL | Partial transfer counts only |
| PERF-FS | Bounded read/write/metadata/storage metrics | FAIL | Persistence active count only |
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

## Paper-thesis evidence map

| Clause | Status | Required final evidence |
|---|---|---|
| Workflow-owned semantics | OPEN | stable identity/lineage/authority under churn |
| Serialized identity | OPEN | dedup, indexing, preserved bytes, boundary deserialize |
| Distributed movement | OPEN | demand/peer/multi-source/bounded central bytes |
| Distributed caching | FAIL | bounded DRAM/disk, admission, eviction, bulk bypass |
| Controlled persistence | FAIL | admission, retry, backpressure, cancellation |
| Re-realization | OPEN | repeated ordinary-task recovery from frontier |
| Disposable workers | OPEN | churn across execution/transfer/persist/recovery/prune |
| Reduced storage demand | FAIL | frontier progression and pruning comparison |
