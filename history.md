# DataVine History

## 2026-08-01 — Positive-byte transfer loss recovery and pruning-proof retry

- Surfaced unavailable IData inputs when TaskVine internally retries a
  `FORSAKEN` physical attempt, cancelling only attempts still owned by
  DataVine and returning the logical task to ordinary lineage recovery.
- Ignored late completion for cancelled physical attempts by physical TaskID.
- Added fresh-proof retry for Controller pruning revision conflicts; stale
  pruning proofs are never applied.
- Added the deterministic 48,600,009-byte transfer-loss/pruning workflow.
  All 26 local DataVine regression scripts pass; the new workflow records
  positive bytes, `keep` during the probe, source attempt 2, two worker
  prunes, one cleanup report, and zero legacy recovery tasks.
- Rebuilt the factory package with `poncho_package_create`; SHA-256:
  `ed2a93d3e27f8664602fe9b491b81467c19c48c5ae9bf924fdded582cb69d70a`.
  Factory validation is not accepted because workers stayed in
  waiting-connection state. Debug logs show packaged workers reach the catalog
  but the advertised manager is absent (`Connection refused`/`matches 0
  managers`); no distributed claim is made.
- Code commit: `adb0236b0`.
- Evidence: `acceptance/artifacts/transfer-failure-recovery-adb0236b0.json`.
- Self-review: **PASS for this scoped checkpoint, FAIL for Review B and
  Ultimate Acceptance**. Factory connectivity, scale, Legacy comparison,
  Controller restart, and remaining Grand Challenge requirements remain.

## 2026-07-30 — Dynamic pruning invalidation and terminal release drain

- Registered a new Controller lineage consumer while a real IData source
  replica was retiring under a retained peer lease.
- Cancelled the stale pruning proof, restored all replicas, and performed no
  physical deletion of newly required data.
- Fixed Scheduler termination so pending peer releases are bounded terminal
  obligations; permanent failure now raises an explicit timeout.
- Three local and three package-only factory repetitions returned the exact
  oracle. The exact clean build and all 25 regressions pass.
- Rejected premature Scheduler completion, access to a non-public replica
  field, a timing-dependent four-second lease window, and a DataID/TaskID
  identity assumption.
- Code commit: `f60fe7582`; package SHA-256:
  `a3b72d6f08de9abb241163d03b067b412c8721c6cfbd67150818ad3891a1b856`.
- Evidence: `acceptance/artifacts/dynamic-pruning-f60fe7582.json`.
- Self-review: **PASS for the scoped dynamic/termination checkpoint, FAIL for
  Review B and Ultimate Acceptance**. Positive-byte failure in the same
  pruning window, timeout/restart, scale, and Grand Challenge work remain.

## 2026-07-30 — Real IData transfer release/pruning coordination

- Retained failed Controller lease releases for delayed retry and bounded them
  with an explicit capacity plus an exact O(1) pending counter.
- Backpressured missing volatile IData at capacity while preserving safe
  stable-origin fallback for EData.
- Forced an 8,000,132-byte serialized IData across two distinct workers,
  deferred pruning with one active lease, retried release, revalidated the
  proof, and acknowledged deletion of both worker replicas.
- Three local and three package-only factory repetitions returned the exact
  oracle. The exact clean build and all 25 DataVine regressions pass.
- Rejected a timing-dependent no-backpressure run, an O(active-transfers)
  hot-path scan, and an unsafe volatile-IData origin fallback.
- Code commit: `8772071b8`; package SHA-256:
  `a63b506e31417bd1dda2596911a7229f80cdd65edff4cde0313cb26a3f063161`.
- Evidence: `acceptance/artifacts/transfer-pruning-8772071b8.json`.
- Self-review: **PASS for the scoped release/pruning checkpoint, FAIL for
  Review B and Ultimate Acceptance**. Positive-byte failure during the same
  pruning window, dynamic invalidation, restart, scale, and the Grand
  Challenge remain open.

## 2026-07-30 — Surviving corrupt peer and alternate-peer fallback

- Added a raw serialized-byte SHA-256 beside, not instead of, the
  metadata-domain-qualified EData identity. Destination workers validate it
  before cache publication, so corrupt bytes cannot become an available
  replica.
- Added a deterministic default-off corruption injection on a real peer
  transfer. The source stays alive; after rejection, the Manager excludes its
  WorkerID and waits for a different READY peer rather than silently using the
  Controller or SharedFS origin.
- Three local and three package-only factory repetitions each record one
  injection, one rejection, one alternate-peer success, four balanced leases,
  zero disconnections, and the exact oracle.
- Rejected a missing Python binding, short peer-retention window, mixed
  incremental binary, generic peer reselection, and an accidental EData-only
  field access in the IData endpoint. The accepted result follows an exact
  clean rebuild and all 24 regressions.
- Code commit: `0a5eefbd0`; package SHA-256:
  `ba5d58999ee8a5a636d189ebcf03849af3958dd83b36ee2bff958d9410f939cb`.
- Evidence: `acceptance/artifacts/peer-corruption-0a5eefbd0.json`.
- Self-review: **PASS for the scoped integrity/fallback checkpoint, FAIL for
  Review B and Ultimate Acceptance**. Real-transfer/pruning continuation,
  restart/dynamic-consumer races, scale, and the Grand Challenge remain open.

## 2026-07-30 — Byte-counted peer interruption and partial cleanup

- Destination workers now observe a real positive transfer-file byte count;
  the Manager validates exact destination, active lease, nonzero bytes, and
  bytes below the expected complete size before injecting source loss.
- Failed transfer paths are removed before invalidation and audited across
  lease deletion with a one-shot TransferID/WorkerID expectation. Accepted
  runs end with one absent-path report and zero pending audit records.
- Three local repetitions cut a 48,600,009-byte EData item after 40,960 to
  1,077,248 bytes, kill one complete source process group, release every lease,
  fall back to the stable origin, and return the same oracle.
- Rejected a 70 MiB workload stopped by the configured Controller capacity and
  an implementation that lost cleanup evidence after active lease removal.
- Exact clean build/install and all 23 regressions pass. A package-only
  two-worker factory reproduces a 65,536-byte cut, cleanup, fallback, and
  oracle; factory shutdown removes its remaining worker.
- Code commit: `b5f2ec21c`; package SHA-256:
  `180dc4b948dd6c1a85d88c5f177a00b82e509e2e6b3f930d4dd989aa8376d649`.
- Evidence: `acceptance/artifacts/peer-partial-loss-b5f2ec21c.json`.
- Self-review: **PASS for this scoped checkpoint, FAIL for Review B and
  Ultimate Acceptance**. Corrupt alternate-peer fallback and concurrent
  transfer/pruning continuation remain open.

## 2026-07-30 — Actual peer-source process loss after transfer start

- Added a destination-originated event after the real transfer child forks.
  Manager accepts it only from the destination bound to the active DataVine
  lease and records an exact start/fault count.
- Added a default-off deterministic fault that abruptly loses the real source
  worker process group. Normal cleanup fails all concurrent leases, releases
  source load, and leaves zero active leases; the destination retries through
  the stable origin and returns the exact oracle.
- The accepted local run observes source return code `-9`, no surviving
  process group, one worker disconnection, two failed/released leases, and zero
  Legacy recovery tasks. Three repetitions have identical semantic evidence.
- Rejected dispatch-only graceful shutdown, an orphaned transfer-server child,
  a mixed old-manager/new-worker incremental install, and a build invoked from
  the wrong directory.
- The exact clean build/install, all 22 DataVine regressions, lint/diff checks,
  packaged-environment verification, and a two-worker package-only factory
  E2E pass. Factory shutdown removes its remaining worker.
- Code commit: `9afe1a64b`; package SHA-256:
  `ab39c707723854a269702b89ec85fdbf9b2f00e207ad151564fb88b0a42e77d5`.
- Evidence: `acceptance/artifacts/peer-source-loss-9afe1a64b.json`.
- Self-review: **PASS for the scoped process-loss checkpoint, FAIL for
  Ultimate Acceptance**. Positive byte-count interruption, partial-file audit,
  corrupt/alternate peer fallback, concurrent pruning continuation, scale,
  and the Grand Challenge remain open.

## 2026-07-30 — Dead-epoch lease cleanup and bounded continuation replay

- Worker reconciliation now fails leases owned by either a dead source or dead
  destination epoch. Exact replica load reaches zero, retiring sources become
  invalid, late duplicate failure is idempotent, and contradictory success is
  rejected.
- Added stable pruning continuation operation IDs. Scheduler transport retries
  reuse the in-flight ID; Controller terminal tombstones replay matching
  results and reject conflicting reuse.
- Bounded terminal history by both configurable item count and serialized
  bytes, with pre-mutation response admission. Removed the full pruning plan
  from each cached continuation response.
- The Scheduler E2E injects response loss after Controller commit and completes
  with exactly one same-ID retry. The component E2E covers destination loss
  followed by source loss with two active leases.
- Rejected a fake destination-worker test, fresh IDs on retry, and item-only
  tombstones containing repeated full-graph plans.
- Final clean build, all 21 regressions, lint, three identical repetitions,
  package verification, and two-mode package-only factory E2E pass.
- Code commit: `f737147e7`; package SHA-256:
  `51f52957912d9e62fa336e76edd0a45b5cbbb899bd8f93ba7c70945d056ed588`.
- Evidence: `acceptance/artifacts/lease-epoch-idempotency-f737147e7.json`.
- Self-review: **PASS for the scoped checkpoint, FAIL for Ultimate
  Acceptance**. Actual in-flight peer source process loss, broader Controller
  timeout coverage, restart semantics, scale, and the Grand Challenge remain.

## 2026-07-30 — Lease-aware pruning proof revalidation

- Added explicit deferred pruning for actively leased Controller sources.
  Alternate replicas remain readable, and duplicate proof application returns
  the same deferred record without changing other replicas.
- Lease release resumes through the newest proof and exact replica generation.
  A valid proof proceeds to physical deletion; an invalidated proof cancels
  retirement and restores source availability.
- Scheduler now treats safe cancellation as a resolved frontier obligation
  while excluding cancelled IData from the runtime-pruned set.
- The deterministic two-mode E2E proves both `[1,2]` deletion after normal
  release and root restoration with only `[2]` deletion after proof
  invalidation. Unrelated compute progresses during the wait.
- Rejected self-invalidating retirement, deadlocking cancellation, duplicate
  apply that bypassed the deferred record, and nondeterministic short timing.
- Required clean build, all 21 installed regressions, `flake8`, three identical
  repetitions, packaged-environment verification, and a two-manager
  package-only factory run pass. Factory shutdown removes all workers.
- Code commit: `a1d273444`; package SHA-256:
  `e391cf570b5a005cbb5bd95ce5fa7d6ddbccebcce4d815c6d4a30bb1bc035036`.
- Evidence: `acceptance/artifacts/pruning-lease-race-a1d273444.json`.
- Self-review: **PASS for the scoped checkpoint, FAIL for Ultimate
  Acceptance**. Actual peer-transfer source death, source-epoch lease cleanup,
  terminal continuation idempotency, dynamic task insertion, scale, and the
  Grand Challenge remain unresolved.

## 2026-07-30 — Asynchronous frontier pruning and compute overlap

- Replaced the frontier-pruning global drain barrier with an explicit active
  state polled from the Scheduler event loop. Unrelated compute and
  persistence remain dispatchable while worker deletion acknowledgements are
  outstanding.
- Prevented prune start when targeted IData is used by a running task or an
  active persistence request. Controller proof application precedes physical
  deletion; generation-exact confirmations and tracker cleanup precede
  frontier advancement.
- Added a deterministic five-task chain/independent-branch E2E. All five tasks
  run once, IData `[1,2]` is deleted, and one compute completion occurs during
  the controlled pruning-acknowledgement window.
- The required build, all 20 installed regressions, `flake8`, three identical
  local repetitions, packaged-environment verification, and two-worker
  factory run pass. Factory shutdown removes all workers.
- Code commit: `a8bd9609c`; package SHA-256:
  `3b74914b788ed67e665f651ba873eda005765ff0857e85582519acea59a9b51f`.
- Evidence: `acceptance/artifacts/async-pruning-a8bd9609c.json`.
- Self-review status: **PASS for the scoped checkpoint, FAIL for Ultimate
  Acceptance**. Active Controller leases still fail closed instead of
  entering a bounded wait/revalidation state. Dynamic proof invalidation,
  scale storage decline, and the Grand Challenge remain unresolved.

## 2026-07-30 — Branched durability-frontier recovery

- Added a diamond branch and unequal chain joined by fan-in, with durable
  frontiers at tasks 4 and 6 and physical pruning of covered IData
  `[1,2,3,5]`.
- Made Controller-inline persistence a Scheduler-owned obligation, separated
  Controller/worker persistence telemetry, added bounded inline retry, and
  preserved strict shared global write admission through condition
  backpressure.
- Actual shutdown of the unique join-replica WorkerID causes only tasks 8 and
  7 to replay at rollback depth two. The left diamond, both durable frontiers,
  and final target do not replay; 11 physical attempts execute for 9 logical
  tasks with zero Legacy recovery tasks.
- Rejected three intermediate results: an untracked inline write deadlocked
  the frontier gate; temporary shared-capacity contention became terminal;
  and clearing the last obligation in-loop caused a false no-progress error.
- The final prescribed build, all 19 installed regressions, three deterministic
  local repetitions, package verification, and three-worker factory run pass.
  Factory shutdown removes all workers.
- Code commit: `6135c761a`; package SHA-256:
  `d0b5afd42be4b2b6f256873c077cdf0c0e3a1c048fee777959f12c2f2c5d423f`.
- Evidence: `acceptance/artifacts/branched-cut-6135c761a.json`.
- Self-review status: **PASS for the scoped checkpoint, FAIL for Ultimate
  Acceptance**. Asynchronous pruning, dynamic proof invalidation, repeated
  branched churn, scale, and the Grand Challenge remain unresolved.

## 2026-07-30 — Multi-output partial-publication process loss

- Added a coordinated fault after output slot zero publication but before slot
  one. Scheduler reads the Controller-owned preparing replica, cancels the
  physical task, shuts down that exact WorkerID, invalidates the partial
  attempt, and releases the original logical task as attempt two only after
  cancellation completion.
- The accepted run uses three physical attempts for two logical tasks. Stable
  IDataIDs `[1, 2]` advance to attempt two, the consumer runs once, the oracle
  matches, and zero Legacy recovery tasks execute.
- Rejected two designs: killing only the worker parent left the transfer server
  holding the manager socket and timed out; self-killing the process group let
  TaskVine repeatedly reassign the same faulting physical command.
- The final required clean build/install, all 18 installed regressions, and
  three local repetitions pass. Package-only factory
  `datavine-partial-79bcbc832` passes with exact remote WorkerID shutdown and
  removes all remaining workers.
- Code commit: `79bcbc832`; package SHA-256:
  `73205f65cd5e5cf8aac2070ee3c1e27e055652047e990d6d30ee81476e04e615`.
- Evidence: `acceptance/artifacts/partial-publication-79bcbc832.json`.
- Self-review status: **PASS for MULTIOUT and this scoped fault, FAIL for
  Ultimate Acceptance**. Other publication stages, concurrent persistence and
  pruning, branched minimum-cut behavior, scale, and the Grand Challenge remain.

## 2026-07-30 — Stable multi-output identity and partial demand

- Replaced the one-output TaskRecord assumption with ordered output slots.
  Controller-owned IData identity now includes producer TaskID and output
  index, and task registration rejects mismatched producer slots.
- Worker execution stages, serializes, publishes, and prepares every output
  slot. Scheduler validates and commits all expected slots before marking the
  logical task complete. Nested OutputRefs resolve the selected output index.
- Added a deterministic two-output workflow with equal serialized bytes,
  cyclic nested containers, repeated-reference alias identity, partial demand,
  and loss/recovery of only the demanded slot. Retry keeps IDataIDs `[1, 2]`,
  moves both slots to attempt two, and uses zero legacy recovery tasks.
- The required clean build/install and all 18 installed regressions pass.
  Three local repetitions have identical output hashes. Package-only factory
  `datavine-multi-605426341` passes normal and recovery modes and removes both
  workers on shutdown.
- Code commit: `605426341`; package SHA-256:
  `662acb77d961016f4717581728a00adc2d5fe1138c7ad8cf6c2ac591b53a3e9f`.
- Evidence: `acceptance/artifacts/multi-output-605426341.json`.
- Self-review status: **PASS for the scoped checkpoint, FAIL for Ultimate
  Acceptance**. Mid-publication worker death, partial-slot cleanup,
  multi-output persistence/pruning races, scale, and the Grand Challenge
  remain unresolved.

## 2026-07-30 — Persistence/global-loss/pruning recovery barrier

- Added a deterministic runtime interleaving that loses the only volatile
  replica while worker persistence is writing, evaluates pruning immediately
  before and after the loss, drains the cancelled request, confirms physical
  worker-cache deletion, and only then releases the stable logical task for
  ordinary recomputation.
- Rejected three intermediate designs. The first retried persistence while
  IData was unavailable; the second let a prune acknowledgement release a new
  attempt before the old persistence task drained; the third matched that
  drain by DataID instead of exact request ID and allowed another request to
  open the barrier.
- The failed runs exposed that TaskVine curl transfers cached HTTP 409 error
  bodies as successful files. URL transfer now fails on HTTP 4xx/5xx, and
  URL-origin failures no longer falsely report a missing peer source worker
  or penalize a destination worker.
- Final clean build/install and all 16 installed regressions pass. Three
  consecutive local race repetitions pass with identical semantic metrics.
  Package-only factory `datavine-persist-loss-aac966a09` passed with two
  workers, two ordinary recovery reexecutions, four physical attempts for two
  logical tasks, one persistence/global-loss event, one 2,097,161-byte worker
  persistence, zero legacy recovery tasks, no stale completion, and 79 bytes
  of Controller IData high-water. Both workers were removed.
- Code commit: `aac966a09a`; package SHA-256:
  `d9fff8aef1e52a2d8ab574de9903df06e463a3221da370d977c78e498b2a18ce`.
- Evidence:
  `acceptance/artifacts/persistence-loss-race-aac966a09.json`.
- Ultimate Acceptance remains FAIL. This checkpoint closes one required
  persistence/global-loss/pruning ordering, not the full race matrix,
  minimum recoverable cut, scale comparison, or Grand Challenge.

## 2026-07-30 — Bounded worker persistence failure recovery

- Replaced TaskVine-level persistence-task retries with Scheduler-owned,
  request-aware retry. Failed requests become terminal, retries receive new
  request IDs for the same IDataID/attempt, and exhaustion fails explicitly.
- Added independent bounds for retry count, exponential backoff, and maximum
  delay.
- Added deterministic partial-SharedFS-write failure with temporary-file
  cleanup and stable worker failure telemetry.
- Rejected the first passing design after self-review observed global write
  concurrency two under capacity one: Controller-inline and worker writes had
  separate admission checks. Unified both beneath the Controller active set.
- Rejected count-only backoff boundedness and added an explicit maximum delay.
- Required two consecutive transient failures before success, and proved
  ordinary compute completion while persistence remained active.
- The final clean build/install and all 16 installed regressions pass.
  Package-only factory `datavine-persist-fail-c4d5258b6` passed through two
  failed partial writes, two retries, worker churn, and ordinary recovery;
  global persistence high-water stayed one and no temporary file remained.
- Code commit: `c4d5258b6`; package SHA-256:
  `592cc09333d91ff2b36ce94a06d18189de18b874dcedcb202b88dcd00684ac2a`.
- Evidence:
  `acceptance/artifacts/worker-persistence-failure-c4d5258b6.json`.
- Ultimate Acceptance remains FAIL pending persistence/global-loss/pruning
  races, scale I/O limits, remaining architecture rows, and the Grand
  Challenge.

## 2026-07-30 — Active worker-persistence cancellation and commit race

- Added active external-persistence cancellation with explicit `cancelling`
  and terminal `cancelled` behavior. A cancelled target cannot publish a
  durable replica, its admission slot is released, and Scheduler retries with
  a new request for the same IDataID.
- Added a real two-worker workflow injection after local SharedFS publication
  but before acknowledgement. It requires one cancellation, one successful
  worker persistence, one worker-loss recovery, exact output, and zero legacy
  recovery tasks.
- Rejected the first factory PASS at `52f7b8139`: self-review showed it did
  not cover cancellation between Controller stream validation and the final
  durability commit, where cancellation was incorrectly treated as stale.
- Closed that compare-and-commit race and added a deterministic threaded
  regression that cancels at the directory-fsync boundary. Worker output now
  reports cancellation rather than falsely printing `DATAVINE_PERSISTED`.
- The required clean build/install and all 16 installed regressions pass.
  Package-only factory `datavine-cancel-race-426ea2195` passed with two
  workers and removed both. It records one active cancellation, one retry to
  durable, 2,097,161 worker-persisted bytes, one ordinary recovery replay,
  zero legacy recovery tasks, and 79 bytes of Controller IData high-water.
- Code commits: `52f7b8139`, `426ea2195`; archive SHA-256:
  `57b98edc2b5583d9dcfe49fb1698cc3c0f23940ff13fd53f9e5ef625967d54d6`.
- Evidence:
  `acceptance/artifacts/worker-persistence-cancel-426ea2195.json`.
- PERSIST and Ultimate Acceptance remain unaccepted pending SharedFS
  failure/overload retry, fairness/responsiveness bounds, unified races, and
  the Grand Challenge.

## 2026-07-30 — Worker-driven large-IData persistence

- Added Controller-authorized external persistence requests for metadata-only
  IData with bounded admission/concurrency and explicit queued, writing,
  failed/cancelled, and durable state.
- Added a worker persistence entry point that validates the existing local or
  peer-provided bytes, writes a same-directory temporary, fsyncs, atomically
  renames, and acknowledges the exact request.
- Moved Controller durable validation outside the global state lock and added
  a compare-and-commit check after streaming hash/size validation.
- Made duplicate begin/complete of the same durable request idempotent while
  retaining fail-closed attempt/request mismatch behavior.
- Made durable large IData a TaskVine SharedFS source and supported explicit
  large final-result return without Controller byte retention.
- Fixed durable recovery so it validates/re-registers SharedFS rather than
  loading a large durable object into Controller memory.
- Required clean build/install and all 16 regressions pass. Rebuilt-package
  factory test persists and returns a 2 MiB IData after worker churn with one
  ordinary recovery and zero legacy recovery tasks; Controller IData
  high-water is 79 bytes.
- Commit: `4e8f19f1f`; package SHA-256:
  `9eb6c9e6ba2f31b5989cd0e0c35408a6f17423aee6dca8c2d90121558fdb4db2`.
- Evidence: `acceptance/artifacts/worker-persistence-4e8f19f1f.json`.
- PERSIST and Ultimate Acceptance remain FAIL pending active cancellation,
  SharedFS overload/failure retry, fairness/latency bounds, and Grand
  Challenge evidence.

## 2026-07-30 — Bounded Controller IData and large-output bypass

- Added hard total and per-object Controller IData byte capacities with
  high-water metrics and fail-closed over-capacity publication.
- Added metadata-only publication for large IData. Workers serialize and fsync
  once, publish attempt/hash/size, and retain bytes in the worker/peer cache
  path instead of POSTing them to Controller memory.
- Split logical physical availability from stable rematerializability, and
  prevented future-input eviction from treating the last volatile worker
  replica as a stable fallback.
- Added selected-result retrieval so large intermediates need not be fetched
  through the Controller merely to return an unrelated small final result.
- Rejected the first E2E after it exposed stale source visibility during worker
  release. Added input-loss reconciliation and ordinary logical rollback.
- Rejected a later Phase 7 regression that exposed non-atomic deterministic
  loss injection. The accepted scheduler revokes logical completion in the
  same event and does not double-decrement input-use counts on recovery.
- Required clean build/install and all 16 installed-path regressions pass.
- Rebuilt `datavine.tar.gz`, verified it with `poncho_package_run -e`, and
  passed a two-worker package-only factory run with a 2 MiB worker-local
  intermediate, one worker release, exact output, one ordinary recovery, and
  zero legacy recovery tasks. Controller IData high-water was 79 bytes under
  a 128 KiB limit.
- Runtime code commit: `53db69f1e`; factory-test commit: `e6ef08b16`; archive
  SHA-256:
  `f56ea4078e90cebcf58f4f5592c899761544931ad55bc7771e06386b7570ad07`.
- Evidence: `acceptance/artifacts/idata-capacity-e6ef08b16.json`.
- Ultimate Acceptance remains FAIL: metadata/history cleanup, worker-driven
  large-IData persistence/final return, repeated churn, DRAM, and Grand
  Challenge evidence remain absent.

## 2026-07-29 — Ultimate Acceptance reopened

- Adopted `acceptance/README.md` as the binding final contract and created the
  requirement/evidence index in `acceptance/matrix.md`.
- Reclassified Phase 8 as a component checkpoint, not final acceptance.
- Completed Architecture Review A against pushed commit `1045fa2dd0`.
- Review A status is FAIL. Critical gaps include physical replica epochs,
  multi-output identity, large-data bypass, bounded serving/queues,
  persistence cancellation, repeated frontier-aware recovery, pruning, and
  Grand Challenge evidence.
- No runtime source changed in this checkpoint; build/package evidence remains
  the Phase 8 component baseline only.

## 2026-07-29 — Phase 9 recovery-aware shadow pruning

- Added independent reference and incremental pruning proof algorithms in
  `datavine/recovery/pruning.py`; no deletion path was enabled.
- Fixed a self-review failure where set-based recovery sources dropped a
  shared anchor when one of several obligations ended. The accepted algorithm
  uses target reference counts and event-local memoization.
- Added current-revision proof records and observable recovery depth.
- Accepted 40 seeded random DAGs, 80 tasks per graph, 6,400 state events, zero
  reference/incremental mismatches, and zero observed false-positive pruning
  decisions. Maximum per-event incremental scan was 64 of 84 IData nodes.
- Dynamic growth, multiple durability frontiers, mixed volatile/durable
  branches, repeated recovery, and obsolete queued-persistence decisions pass.
- Required clean build/install and local topology plus Phase 4–9 regressions
  pass.
- Architecture Review B remains FAIL and blocks deletion until physical
  replicas/epochs, transfer readers, atomic revisions, persistence
  cancellation, and SharedFS quarantine exist.
- Code commit: `2108b68a8`.

## 2026-07-29 — Phase 9 bounded physical replica directory

- Added qualified `e:<id>` / `i:<id>` physical identities, explicit replica
  tiers and states, worker epochs, logical attempts, source generations, and
  Controller-owned multi-source selection.
- Added transfer leases that protect concurrent readers and make source
  invalidation retire before final invalidation.
- Added explicit capacities for workers, replicas, active leases, and bounded
  completed-lease tombstones, plus revision-checked terminal cleanup.
- Added SharedFS quarantine, restore, grace-period, and revision-checked
  hard-delete transitions. This is state-machine coverage only; filesystem
  deletion remains disabled.
- Required clean build/install passed. The installed component test and 20
  repeated race runs passed at code commit `e1843b9bd`.
- Self-review found and fixed two pre-acceptance defects: numeric EData/IData
  ID collision and unbounded completed lease history.
- The checkpoint remains runtime-disconnected. Review B stays FAIL until
  Controller/protocol/persistence/pruning integration passes.
- Code commit: `e1843b9bd`.

## 2026-07-29 — Phase 9 generation-safe persistence

- Replaced IDataID-only persistence callbacks with bounded immutable requests
  carrying request ID, attempt, content hash, payload, and target identity.
- Added queued cancellation, active cancellation, a defined too-late atomic
  commit boundary, bounded terminal tombstones, and protocol-v1 cancellation.
- Registered Controller-memory and durable SharedFS realizations in the
  physical replica directory; attempt-specific replica names permit an old
  in-flight read to finish without being overwritten by a newer generation.
- Fixed the pre-existing race where a late write for attempt 1 could mark
  attempt 2 durable or overwrite its path. The accepted test commits the old
  write, publishes attempt 2 concurrently, rejects the stale callback, removes
  the old file, and then durably persists only attempt 2.
- Required clean build/install, installed topology and Phase 4–9 regressions,
  and 20 repeated persistence race runs passed.
- Self-review also caught and fixed physical replica ID reuse while an old
  source lease remained active.
- Review B remains FAIL until pruning and real worker/local/SharedFS deletion
  paths use the new states.
- Code commit: `17577b058`.

## 2026-07-29 — Phase 9 worker replica incarnations

- Exposed TaskVine's unique WorkerID to task processes and to manager status
  for both local and factory workers.
- Added authenticated Controller protocol operations for worker join,
  disconnect/reconciliation, replica prepare/commit/report/invalidate,
  candidate lookup, and source-lease acquire/release.
- Connected worker cache observations to qualified Controller replica records.
  Reports must match the Controller's attempt, content hash, and serialized
  size; stale epochs, corrupt metadata, and foreign invalidation fail closed.
- Made output realization two phase. A worker prepares the replica after
  fsync/publication, while the Scheduler commits the exact generation only
  after successful TaskVine completion.
- Fixed two defects found during integration: zero-byte IData was incorrectly
  rejected, and worker loss left an unbounded ghost `preparing` replica.
  Scheduler-to-Controller incarnation reconciliation now invalidates both
  available and preparing state.
- Required clean build/install, installed topology, Phase 4 worker-loss, Phase
  7 recovery, the complete Phase 4-9 local regression, and 20 repeated
  protocol races passed.
- Review B remains FAIL because real peer byte movement does not yet use the
  candidate/lease protocol and pruning still performs no physical deletion.
- Code commit: `fbddcc70d`.

## 2026-07-29 — Phase 9 revision-safe SharedFS pruning

- Made Controller runtime lineage explicit for every TaskRecord, including
  nested IData dependencies, and connected Scheduler task-state transitions
  to the proven incremental/reference pruning model.
- Added authenticated pruning plan, compare-and-apply, restore, and hard-delete
  protocol operations with graph/state/replica revision checks.
- Connected obsolete queued persistence cancellation and active-write
  protection to pruning decisions.
- Added real SharedFS quarantine rename, directory fsync, source exclusion,
  checksum-validated restore, configurable grace, hard delete, and bounded
  machine-readable audit records.
- Added an E2E covering stale proof, persistence concurrency, active source
  read, corrupt quarantine, dynamic consumer, restore, early-delete rejection,
  final hard delete, and retained-output correctness.
- Fixed destructive-order bugs uncovered by the test: unlink-before-grace,
  restore-before-checksum, permanently undeletable quarantines, and
  half-registered lineage.
- Removed premature explicit worker disconnect after a Phase 7 retry exposed a
  race with a still-live TaskVine worker; manager-status reconciliation remains
  the worker-loss truth.
- Required clean build/install, 20 repeated pruning E2Es, five repeated
  worker-loss recoveries, and the installed Phase 4–9 regression passed.
- Review B remains FAIL pending physical worker-cache deletion, transfer-lease
  coupling, stable bulk origins, and persistent quarantine recovery.
- Code commit: `347f60531`.

## 2026-07-29 — Phase 9 acknowledged worker-local pruning

- Added UUID-correlated TaskVine worker cache-unlink acknowledgements. The
  Manager accepts each operation once and only from its intended worker.
- Added bounded acknowledgement lifecycle: completed per-file tracker state is
  explicitly forgotten and verified empty by the Scheduler.
- Unified worker/DataID replica naming so one physical cache file cannot
  appear as two Controller replicas merely because it is later consumed.
- Added fail-closed Controller/TaskVine replica-count comparison and
  generation-specific Controller confirmation after all physical ACKs.
- Added a two-worker, multi-replica fan-out/fan-in E2E. Five local runs and one
  prescribed-factory run passed exact output, request/ACK equality, Controller
  state, and tracker cleanup; local cache decline equalled unlink count.
- Rebuilt and verified `datavine.tar.gz`; SHA-256:
  `af92ca3718fab236366b307d6ad98b4bd30df0b04c72b31ff8c2e41677cfd663`.
- Self-review leaves worker loss during pending unlink, transfer-lease
  coupling, bounded cache policy, and recovery-after-prune open.
- Code commit: `3f993f15b`.

## 2026-07-30 — Phase 9 bounded Controller admission

- Replaced unbounded per-request HTTP threading with a hard request semaphore
  and immediate overload response.
- Added a second admission gate for Controller byte responses, bounded by both
  response concurrency and total in-flight serialized bytes.
- Added observable active/high-water/admitted/rejected/bytes-served metrics.
- Added deterministic held-response tests for request saturation, byte
  concurrency saturation, byte-budget rejection, metadata responsiveness,
  and exact release accounting; 20 repetitions passed.
- Required clean build/install and the complete current DataVine regression
  passed. A rebuilt-package prescribed-factory Phase 4 run used two workers
  and returned exact normal/shared-input results.
- Rebuilt `datavine.tar.gz` SHA-256:
  `9c31fdbe190e223a38708db17bb7064e687eb69b83a9b5056d4641eab9161391`.
- Self-review leaves large-object bulk bypass, stable origins, direct transfer
  lease coupling, cache ownership, restart behavior, and the Grand Challenge
  open. This is not ultimate acceptance.
- Code commit: `d694bef4a`.

## 2026-07-30 — Phase 9 stable bulk origins and worker epoch claims

- Added metadata-aware serialization domains and Scheduler object-identity
  memoization so repeated function/value/container references serialize once
  without cross-domain aliasing.
- Added atomic content-addressed bulk EData origins beneath a configured
  Controller root. Registration rejects symlinks, root escape, wrong names,
  size mismatch, hash mismatch, and byte collisions while keeping bulk bytes
  outside Controller memory and byte serving.
- Added worker fallback validation for stable-origin bytes and a two-worker
  4 MiB repeated-reference E2E whose Controller memory and serving capacities
  are each 1 MiB.
- Required clean build/install, all 14 current DataVine tests, 10 repeated bulk
  workflows, and five repeated worker-loss recoveries passed locally.
- The first rebuilt-package factory bulk workflow passed, but the immediately
  following recovery workflow failed with `stale worker epoch`. This run was
  rejected as acceptance evidence.
- Fixed the root cause by replacing worker hard-coded epoch 1 with an
  idempotent Controller claim. An inactive WorkerID advances to the next
  incarnation; an active duplicate claim returns the same epoch; explicit old
  reports still fail closed.
- Rebuilt and verified `datavine.tar.gz`; SHA-256:
  `857eb5a8d4f586c369ab0755b7f249557a8b1c08e1e3f367a5635dbfef3a5cd6`.
  Factory `datavine-epoch-643cddd68` then passed both the bulk workflow and the
  same deterministic worker-loss recovery and was stopped with two workers
  removed.
- Self-review leaves transfer-lease coupling, bounded cache admission,
  stable-origin mutation/restart recovery, dependency-file identity,
  Controller history cleanup, and the Grand Challenge open.
- Code commits: `13193c99a`, `643cddd68`.

## 2026-07-29 — Phase 0 local baseline

- Created branch `datavine` from freshly fetched `origin/task-graph` at PR
  #4253 head `345dc7fcde3851400bebb81ccfd877c93a93cdca`.
- Studied the Python Workflow/task-runner layers, C compute graph and executor,
  lazy task materialization, `vine_file` movement and replica tracking,
  special-task recovery, checkpoint selection, cut propagation, and
  prune-depth release.
- Added a deterministic three-case Phase 0 suite covering normal execution,
  repeated shared inputs, and loss of the only worker replica.
- Repaired generated task-runner preambles so hoisted Workflow/execution code
  has explicit `collections`, `copy`, and `dataclasses` dependencies.
- Validation: clean build/install PASS; Phase 0 suite PASS; existing 13-task
  corner-case workflow PASS; exact local metrics saved in
  `phase0-artifacts/baseline-local-20260729.json`.
- The prescribed factory worker connected but failed to deserialize the
  manager context because its Python 3.11 archive is incompatible with the
  Python 3.10 manager. This was later identified as use of the wrong
  `dagvine-env.tar.gz`, corrected in the Phase 2 factory validation below.
- Code commit: `ab6b7666d`.

## 2026-07-29 — Phase 9 direct TaskVine transfer authority

- Commit `4367f95ca` added idempotent observed-transfer acquisition to the
  authenticated Controller protocol, with current worker-epoch validation,
  completed-ID tombstones, and bounded lease metrics.
- Commit `11e555bef` attached qualified DataIDs to TaskVine files and wired
  real peer pulls through Controller acquisition/release. Terminal transfer
  state detaches worker pointers before lease-release retry, preventing stale
  worker references after loss.
- The old Phase 5 test was found insufficient: using two workers did not prove
  both consumed the shared EData. It now uses two single-core workers, records
  worker assignment per task, and requires nonzero balanced Controller lease
  telemetry for peer-on and zero acquisitions for peer-off.
- The first full regression after integration was rejected: Phase 8 prefetch
  exposed a TaskVine cache source not yet published to the Controller. The
  Controller correctly refused it, but TaskVine aborted instead of falling
  back. Commit `ef605c343` deletes the unverified substitute and retries the
  stable origin while preserving Controller authority.
- The exact `ef605c343` clean build/install and all 14 local DataVine
  regressions pass. Three repeated local Phase 5 runs each record 5
  acquisitions, 5 releases, zero active leases, and zero peer-off
  acquisitions.
- Rebuilt `datavine.tar.gz` from the active DataVine environment and verified
  it with `poncho_package_run`. SHA-256:
  `fc44eadfc93a207f919279854036701248dafe4b98ba6bc45279a253ba89e110`.
- Factory `datavine-transfer-ef605c343` supplied two workers. Phase 5
  peer-on/off, Phase 8 prefetch/fallback, and Phase 7 worker-loss recovery
  passed. The factory was stopped and both workers were removed.
- This is not Ultimate Acceptance. Active-transfer worker loss,
  release-timeout fault injection, bounded worker-cache admission, and the
  Grand Challenge remain open.
- Evidence: `acceptance/artifacts/transfer-authority-ef605c343.json`.

## 2026-07-29 — Phase 1 indexed serialized identity

- Added feature-flagged TaskID, EDataID, IDataID, and task binding records while
  preserving the legacy execution and data-movement path as authoritative.
- Added fixed-protocol cloudpickle metadata, SHA-256 interning, byte-level
  collision checks, raw input-file identity, stable output-slot identity, and
  snapshot invariant validation.
- Added component tests for equal distinct values, forced hash collisions,
  metadata separation, nested IData/file bindings, file-content deduplication,
  stable re-finalization, and rollback.
- Enabled the deterministic Phase 0 and 13-task corner-case workflows in both
  identity-on and identity-off modes. Normal, repeated-input, exact recovery,
  nested container, alias, cycle, and file cases pass.
- The repeated-input case performed 17 registrations, retained seven unique
  EData records, and assigned one EDataID to all eight uses of the 950,272-byte
  shared payload.
- Final clean build/install and local tests pass. The unique prescribed factory
  worker attached through Condor job `4937.0` but retained the known Python
  3.11 worker versus Python 3.10 manager cloudpickle incompatibility because
  the factory selected the wrong archive. This result was superseded by the
  corrected Phase 2 factory PASS.
- Code commit: `ab6b7666d`.

## 2026-07-29 — Phase 2 read-only shadow data graph

- Added a separately feature-flagged `ShadowDataGraph` derived from the
  validated Phase 1 identity snapshot.
- Added deterministic Task/EData/IData/consumer nodes and comparison reports
  for task IDs, data IDs, dependency edges, producer mappings, controller
  EData availability, and unproduced IData state.
- Made graph construction fail closed on an injected Workflow dependency
  mismatch; the shadow graph has no scheduling, movement, recovery, deletion,
  persistence, or pruning authority.
- Added a focused component suite and extended the deterministic Phase 0 and
  13-task corner-case suites to audit the shadow graph before and after
  execution and recovery.
- Final clean build/install passed. Phase 2-on, Phase 1-only, and full-off
  baseline modes all passed normal execution, repeated shared inputs, and
  exact worker-loss recovery. The Phase 2 comparison reports zero mismatches.
- The first Phase 2 factory attempt used the wrong `dagvine-env.tar.gz` and
  failed before a user task because its Python 3.11 worker could not load the
  Python 3.10 manager cloudpickle context. The job was removed.
- Corrected the factory default to `datavine.tar.gz`, rebuilt that package from
  `/groups/dthain/users/jzhou24/miniconda/envs/datavine` with
  `poncho_package_create`, and verified Python 3.10.20 plus cloudpickle 3.1.2
  inside the archive.
- Condor job `4939.0` then passed the complete Phase 2 normal,
  repeated-shared-input, and worker-loss baseline. The recovery case completed
  four user tasks and three recovery tasks with zero shadow mismatches. The
  accepted machine-readable result is
  `phase0-artifacts/baseline-phase2-factory-fixed-20260729.json`.
- Proposed Phase 3 as a narrow Controller authority step: immutable logical
  registry and materialization-time binding/lineage lookup with legacy
  physical data lifecycle retained behind adapters.
- Code commit: `ab6b7666d`.

## 2026-07-29 — Phase 3 Data Controller logical authority

- Added a separately feature-flagged immutable `DataController` that becomes
  the sole enabled owner of Task/EData/IData records, canonical serialized
  EData, file DataID mappings, lineage, and initial availability state.
- Added compact materialization plans and fail-closed Python checks that the
  legacy parent and declared-file mounts match Controller bindings.
- Added submit-time C materializer expectations and per-node/aggregate audits;
  duplicate materialization or mount-count disagreement is fatal.
- Kept `vine_file` placement, transfer, replica handling, recovery tasks,
  deletion, pruning, and C scheduling unchanged behind the legacy adapter.
  Controller-on task grouping is rejected until grouped physical
  materialization can preserve the exact logical audit contract.
- Added Controller component tests for immutability, cloudpickle round trips,
  exact lookup plans, initial states, prerequisite validation, Python
  disagreement, and direct C disagreement.
- Final clean build/install, the 13-task corner suite, and all four rollback
  modes passed. The final local Controller-on baseline passed normal,
  repeated-shared-input, and one-worker-loss recovery with every logical task
  audited exactly once.
- Rebuilt the required
  `/users/jzhou24/graph_optimization/factories/datavine.tar.gz` using
  `poncho_package_create`; SHA-256 is
  `13d79ae8bdc1644e5c38ba6bca902d5c20ec783c5fea48ad4d6fb54be5118f61`.
- Condor job `4941.0` passed the final factory suite: 4 normal tasks, 5
  shared-input tasks, and 7 recovery-case physical tasks (4 user plus 3
  recovery), with zero Controller audit mismatches. The job and worker were
  removed afterward.
- The first final-factory manager attempt expired at its 240-second global
  alarm because Condor had not yet started the worker. Once the worker was
  available, the complete suite passed under a 600-second bound; no code
  correctness failure was observed.
- Proposed Phase 4 as a narrow Worker Data Agent inventory and stable-source
  resolution contract that continues to use legacy mounts for actual byte
  transport.
- Code commit: `ab6b7666d`.

## 2026-07-29 — Phase 4A Worker Data Agent inventory contract

- Added feature-flagged compact worker assignments carrying one TaskID and
  qualified required EData/IData IDs without serialized payloads.
- Added a worker-process inventory with deterministic empty, partial, complete,
  missing, and stale handling. The Controller resolves required IDs to
  Controller context or the exact legacy file/result source.
- Added fail-closed worker checks for altered assignments, unknown or
  unrequired DataIDs, missing stable sources, and missing Controller
  prerequisites.
- Added C task-runner argument wiring and completion-time stdout audits. Every
  successful logical task must return its exact preparation marker once;
  TaskVine recovery tasks retain legacy semantics and do not double-count the
  logical audit.
- Added `vine_graph_worker_data_agent.py` and
  `TR_vine_graph_worker_data_agent.sh`, and extended the Phase 0 baseline and
  13-task corner suite with the Phase 4A flag and audit reports.
- Final exact clean build/install passed. Phase 4A-on, Controller-only, Phase
  2-only, Phase 1-only, and full-off deterministic baselines passed, including
  loss of the only worker replica.
- Rebuilt
  `/users/jzhou24/graph_optimization/factories/datavine.tar.gz` with
  `poncho_package_create`; SHA-256 is
  `2a3a150a967204cc2f1783dc357b41e5425cbfab9291ce10b6adc965810383a6`.
- Condor job `4942.0` passed the final factory normal,
  repeated-shared-input, and worker-loss suite. The recovery case completed
  four user tasks and three recovery tasks, with every Worker Data Agent and
  Controller audit exactly once and zero mismatches. The job and worker were
  removed.
- Phase 4 is not yet complete: legacy mounts still transport bytes before the
  Worker Data Agent validates them. Proposed the smallest next increment as
  bounded independent demand pull for immutable, content-addressed non-file
  EData only.
- Code commit: `ab6b7666d`.

## 2026-07-29 — Architecture reset and Phase 4 worker demand pull

- Froze `ndcctools.taskvine.vine_graph` as the Phase 4A reference and created
  the independent `ndcctools.taskvine.datavine` system with explicit
  controller, scheduler, worker, placement, persistence, recovery, and legacy
  subsystem boundaries.
- Added the separate installed `datavine_controller` command. Its process owns
  a dedicated single Controller thread; the application process owns a
  dedicated single Task Scheduler thread and TaskVine manager.
- Added protocol-v1 authentication, content-addressed canonical EData,
  lineage-owned IData, TaskID/DataID-only task records, binary demand reads,
  binary output publication, integrity validation, and fail-closed logical
  completion.
- Added an independent DataVine Workflow and worker runner. TaskVine dispatches
  only a compact TaskID and endpoint information; workers fetch and deserialize
  data at execution and publish canonical serialized outputs.
- The initial shared-input test exposed loss of alias identity. Worker
  execution now memoizes deserialized values by qualified DataID. The initial
  failure harness exposed surviving task children; it now kills the complete
  worker process group.
- Exact local normal, repeated-shared-input, and worker-loss workflows pass
  after a clean build/install. The standalone topology component test also
  passes.
- Rebuilt `/users/jzhou24/graph_optimization/factories/datavine.tar.gz` with
  `poncho_package_create`; SHA-256 is
  `45fa55d134567018b7e23031361a8c380b5b22218a0459ef4ecdd7448af5c6b4`.
  Factory `datavine-phase4-20260729` passed normal and shared-input workflows
  and was shut down with its worker removed.
- Phase review records the remaining base64 registration overhead,
  Controller-only stable storage, physical-retry recovery, and missing nested
  binding support; these are explicit future work, not accepted Phase 4
  behavior.
- Code commit: `ab6b7666d`.

## 2026-07-29 — Phase 5 validated peer transfer and bounded caches

- Added a DataVine placement adapter over TaskVine's existing worker cache,
  worker-driven URL transfer, transfer server, and peer-replica machinery.
  Task records remain lightweight; serialized EData is mounted by DataID.
- Added Controller metadata lookup and payload-fetch accounting. Workers verify
  size and canonical metadata-aware hash before deserialization; a corrupt
  cache/peer replica falls back to Controller bytes.
- Added explicit peer-transfer rollback at Scheduler manager creation.
- Added a staged two-worker E2E test. Both workers executed tasks; peer-on
  fetched the 1,769,481-byte shared EData from the Controller once, while
  peer-off fetched it twice. Both returned the exact result.
- Rejected early factory attempts that executed with only one connected worker.
  Corrected the Scheduler-owned connection wait to drive `Manager.wait(1)` and
  verify worker status before submission.
- Final clean build/install, corruption injection, Phase 4 regressions, local
  peer-on/off, and two-worker factory peer-on/off all pass. Factory
  `datavine-phase5-accept-20260729` was stopped and both workers removed.
- Rebuilt `datavine.tar.gz` SHA-256:
  `bd9fd463988ba5dff2244996584a9c61787270ee357eaeaf0b1dfb902d708164`.
- Code commit: `ab6b7666d`.

## 2026-07-29 — Phase 6 controlled, acknowledged persistence

- Added worker-local output staging with file `fsync` before volatile
  publication and cleanup after acknowledgement.
- Added a bounded persistence executor in the standalone Controller process,
  with explicit volatile/queued/writing/durable/failed states, atomic temporary
  writes, file and directory `fsync`, hash validation, and durable paths.
- Added Scheduler durability waiting, idempotent requests, deterministic
  first-write failure injection, and one safe retry. Persistence-disabled
  execution remains the rollback.
- The two-worker seven-output workflow passes locally and in the factory with
  persistence enabled, one injected write failure, and persistence disabled.
  Enabled/retry modes produce seven hash-valid files with maximum concurrency
  one; disabled mode produces none.
- A mistaken root-level clean build was interrupted, then repaired by a full
  repository rebuild/install and the required exact TaskVine clean
  build/install. No source file was lost.
- Factory `datavine-phase6-accept-20260729` was stopped and both workers were
  removed. Rebuilt archive SHA-256:
  `c4b66e32a920c6a4546e33a12e8633bb30d536cc93a2348f683541a929bf5736`.
- Code commit: `ab6b7666d`.

## 2026-07-29 — Phase 7 volatile IData and lineage recovery

- Added TaskVine temporary-file realizations for IData. Workers expose the same
  staged canonical bytes they publish logically; downstream tasks mount,
  validate, and consume those bytes from worker cache/peers.
- Added explicit volatile invalidation and durable restoration behavior to the
  Controller.
- Added Scheduler loss detection that returns the original producer from done
  to pending and reruns it under stable TaskID/IDataID with an incremented
  attempt. No recovery-task class is used.
- The accepted local loss workflow evicts the only producer worker, connects a
  replacement, and completes four logical tasks through five ordinary
  executions with one recovery replay and four local IData hits. The no-loss
  mode has four executions and no replay.
- The factory reproduces the worker disconnection, recovery replay, exact
  output, and no-loss rollback. Factory
  `datavine-phase7-accept-20260729` was stopped and its worker removed.
- Phase 4–6 regressions and the clean build/install pass. Rebuilt archive
  SHA-256:
  `30491e946851aa892c9f0f36f0ae5365dbd0a13f30b95ff5ea3df8f2c690f62a`.
- Code commit: `ab6b7666d`.

## 2026-07-29 — Phase 8 prefetch and adaptive placement

- Added deterministic, bounded prefetch selection from existing graph/DataID
  metadata. Repeated EData inputs are ordered by fanout, score, size, and ID
  under explicit byte/item budgets.
- Added low-priority, zero-core TaskVine prefetch operations. Ready demand work
  retains normal priority and the accepted transaction starts demand task 4
  before prefetch task IDs 1–3.
- Added prefetch completion/failure/overlap/byte reporting, deterministic fault
  injection, and an explicit disabled rollback.
- Removed an invalid first implementation that re-serialized values during
  policy analysis. Final enabled, failure, and disabled modes each perform 21
  registrations rather than 42.
- Added recursive nested `OutputRef` discovery and composite binding. Worker
  reconstruction preserves repeated-reference alias identity through a
  deepcopy memo.
- Added `datavine_phase8_prefetch.py` and
  `TR_datavine_phase8_prefetch.sh`. Local and rebuilt-package two-worker
  factory runs pass enabled, failure, disabled, and nested-binding modes.
- The final local topology and Phase 4–8 combined regression passed. The final
  factory Phase 4–7 combined regression and Phase 8 four-mode acceptance
  passed. After lint cleanup, Phase 8 was accepted again with the newly rebuilt
  package; `datavine-phase8-postlint-20260729` was stopped with both workers
  removed.
- Machine-readable evidence:
  `phase0-artifacts/baseline-datavine-phase8-local-20260729.json` and
  `phase0-artifacts/baseline-datavine-phase8-factory-20260729.json`.
- Final `datavine.tar.gz` SHA-256:
  `4a283955d934c6f6a4123fc877ff9a4c185ca70afd1634dcd5ad6a162e7c85c5`.
- `poncho_package_run` verified that this archive imports the independent
  runtime and reports cloudpickle 3.1.2.
- Phase 8 self-review: demand precedence is proved from transaction order,
  speculation is tested in success/failure/off modes, and no Phase 9 pruning
  behavior is claimed.
- Project-rule flake8 passes after removing one unused import and three
  slice-spacing violations. The cleanup was followed by the complete required
  rebuild and fresh local Phase 4–8 plus package/factory verification.
- Code commit: `ab6b7666d`.

## 2026-07-29 — Phase 9 acknowledged worker-cache retention

- Added `datavine.cache.admission.WorkerCacheAdmission`, preserving Controller
  authority while using TaskVine only for targeted physical unlink.
- Added WorkerID-targeted `vine_prune_file_on_worker`, UUID acknowledgement
  tracking, and safe tracker resolution before worker teardown.
- Worker loss during a pending unlink records one explicit failure, zero
  confirmations, and releases the tracker; it does not falsely claim that a
  keep-workspace cache was removed.
- Rejected an initial one-chain test because it exercised only one worker.
  The accepted workload has two independent chains and a final fan-in and
  requires both workers to execute tasks.
- Rejected an initial eviction rule after it exposed a stale-generation race
  for shared EData with future consumers. The accepted policy evicts only
  Controller records with zero remaining direct consumers.
- At exact code commit `c20db01a1`, the required clean build/install and 15
  installed-path regressions pass. The bounded mode completes 13 logical tasks
  on two workers with 32 acknowledged evictions and six final observed items
  per worker; zero retention completes seven tasks with 22 evictions and an
  empty final cache.
- Rebuilt-package validation passes with Python 3.10.20 and cloudpickle 3.1.2.
  `datavine.tar.gz` SHA-256:
  `852eb4aeaa1d7041046ea1a514aef9d949e74dd48eee28c7cce25193a130091d`.
- Factory `datavine-cache-c20db01a1` reproduced both modes with two workers and
  was stopped with both workers removed.
- Self-review status: **FAIL for strict cache capacity**. The bounded run
  observed a nine-item instantaneous high-water against a six-item target;
  zero retention observed seven items before cleanup. Dispatch admission,
  worker-side hard enforcement, a DRAM tier, active-read eviction races, and
  recovery after eviction remain unresolved.
- Code commits: `2e0d8ebdd`, `c20db01a1`.

## 2026-07-30 — Phase 9 strict cache-item dispatch admission

- Added a Manager worker-selection gate that projects cached files, outputs of
  assigned tasks, candidate inputs/outputs, and acknowledged unlinks still
  occupying physical slots.
- Added per-worker physical item high-water, pending-unlink, and admission
  rejection telemetry.
- Added Scheduler fail-closed validation for a capacity below the largest task
  working set and retained-dead-data headroom.
- Rejected a prototype that deadlocked when retention consumed all admission
  capacity, then rejected a second prototype after eviction raced an active
  root input. The accepted policy protects running-task inputs.
- At exact test commit `88b7d1a44`, the required clean build/install and all
  15 installed regressions pass. Local and factory runs keep two workers at a
  six-item physical high-water, complete 44 acknowledged evictions, and return
  exact results; a five-item capacity rejects a six-item task before execution.
- Rebuilt `datavine.tar.gz` SHA-256:
  `fc8003bb0a5422909214cb62311f1678ea5de37c37a192e0abe0fdcfe75e6e71`.
  Factory `datavine-cache-88b7d1a44` passed and removed both workers.
- Self-review status remains **FAIL for the complete CACHE row**. Worker-side
  hard enforcement, bytes, DRAM, prefetch/recovery/churn combinations, and
  hot-path scale proof remain open.
- Code commits: `9d03dbf4d`, `88b7d1a44`.

## 2026-07-30 — Phase 9 worker-enforced byte/item capacity and recovery

- Made a worker cache insertion fail closed when its configured item or byte
  capacity cannot admit the object; a task whose output cannot be published
  now returns `output missing`.
- Reserved every normal-task output slot before execution and released the
  reservation on cancellation, protocol failure, or failed stageout.
- Extended the Manager projection to account for input bytes, output bytes,
  and unlink operations that have not yet been acknowledged.
- Prevented Scheduler dispatch from using a cache input while its targeted
  prune is pending, and waited for unlink acknowledgement before declaring
  the workflow unable to progress.
- Isolated factory recovery into one uniquely named Manager and raised the
  combined local recovery worker to two cores so data preparation and ordinary
  execution can make concurrent progress.
- Rejected and fully reverted future-used volatile IData eviction after it
  triggered TaskVine special recovery tasks and exposed a stale-generation
  prune race. This is an architecture blocker requiring DataVine-owned
  materialization, not a cache-policy exception.
- Rejected one false clean-build result after its install submake reported
  `Text file busy`; killed the two exact orphan test workers, repeated the
  prescribed clean build/install, and verified source/installed worker hashes
  match.
- All 15 installed regressions and the full local cache workflow pass. A
  unique two-worker package-only factory run passes the combined
  prefetch/cache-pressure/worker-disconnect recovery case and removes both
  workers.
- Rebuilt archive SHA-256:
  `667b2615e28566650e56afca48ab38fc3604ad36e8b1dbbf92b8ed00fde05671`.
- Ultimate Acceptance remains FAIL: DRAM bounds, actual worker process loss,
  active-transfer failure, repeated frontier-bounded recovery, scale, and
  Legacy comparison are not proved.
- Code commits: `3993bd475`, `5d2ef7d60`, `e48fffa4a`, `240e05ebd`,
  `a66f7fae1`, `291a81a95`, `6d3b77042`.

## 2026-07-30 — Phase 9 DataVine-owned IData rematerialization

- Replaced DataVine `VINE_TEMP` outputs with attempt-qualified,
  Controller-backed URL cache identities. Controller-qualified URLs may be
  written as task outputs; ordinary URL outputs remain rejected.
- Kept worker/peer cache reuse while adding an authenticated stable fallback
  for a missing IData realization.
- Added attempt validation to the Controller byte endpoint; an old attempt
  URL receives HTTP 409 after a newer publication.
- Allowed the cache policy to evict future-used IData only while the
  Controller reports it rematerializable.
- Added `legacy_recovery_tasks` telemetry and made zero legacy TaskVine
  recovery tasks an acceptance criterion.
- Rejected the first factory result after worker release raced eviction and
  produced `stale invalidation generation`.
- Added a Controller-atomic observed invalidation operation that verifies
  attempt, hash, size, worker identity, and worker epoch before invalidating
  the current generation. Wrong content and stale incarnations fail closed.
- The required build, 15 regressions, future-IData bounded/zero-cache tests,
  stale-attempt HTTP test, and rematerialized-generation protocol test pass.
- Rebuilt package SHA-256:
  `2bbb11c55b86b08719b4173e32ad0a93b0cdb1961dc68c948ce83342a1cec9f6`.
- A fresh two-worker factory run completes seven logical tasks through eight
  ordinary attempts, evicts future-used IData, performs one DataVine recovery,
  stays within physical disk bounds, reports zero legacy recovery tasks, and
  removes both workers.
- Ultimate Acceptance remains FAIL because ordinary IData bytes are still
  retained centrally, large-IData bypass and DRAM bounds are absent, and real
  process loss/repeated frontier-aware recovery/Grand Challenge comparison
  have not passed.
- Code commits: `997d63acf`, `9512638c5`, `f6c1c712e`, `f1237b8b8`.
# 2026-07-30 — Phase 9 two-frontier minimum recoverable cut

- Added selective durability-frontier scheduling and target-driven recovery
  closure from unfinished consumer and required-result obligations.
- Added deterministic TaskVine process shutdown by exact WorkerID. The accepted
  loss target is selected from the Controller replica directory, not from
  worker ordering; both accepted losses remove the unique volatile source.
- Persisted task 1 on attempt one and task 5 on attempt two. Recovery executes
  tasks 5–2 at depth four, then tasks 8–6 at depth three after the newer
  frontier; 16 physical attempts represent nine logical tasks and seven
  ordinary reexecutions with zero legacy recovery tasks.
- Physically deleted IData 2–4 after task 5 durability. All worker prune
  operations report requested=confirmed=1, failed=0, and release their trackers.
  The later loss still completes from the retained task-5 frontier.
- Rejected timer-dependent worker replacement, a nested pruning wait that
  swallowed compute completion, frontier gating outside persistence mode,
  lost compute/persistence overlap, reconnecting graceful release as process
  loss, and shutdown not tied to the target replica owner.
- The required clean build/install passes. The installed regression suite
  passes 17/17; three final local repetitions complete in 17, 16, and 17
  seconds. Post-clean Phase 7 recovery, replica protocol, DataVine flake8, and
  diff checks pass.
- Rebuilt `/users/jzhou24/graph_optimization/factories/datavine.tar.gz` from the
  active environment. SHA-256:
  `1f1da5b661f61eaa4c532b1df65d09f84a061f96278a729df21c861f2c87761d`.
  `poncho_package_run` verifies cloudpickle 3.1.2, Workflow, and the shutdown
  API.
- Factory `datavine-mincut-fafead8bd` passes with three package-only workers,
  two unique-source process shutdowns, exact oracle output, and zero remaining
  workers after factory stop.
- Code commit: `fafead8bde`.
- Evidence: `acceptance/artifacts/minimum-cut-fafead8bd.json`.
- Self-review status: **FAIL for full MIN-CUT/RECOVERY acceptance**. The
  checkpoint is linear and pruning uses a global drain barrier; branch/join
  optimality, asynchronous pruning, DRAM, active-transfer loss, scale, and the
  Grand Challenge remain open.
