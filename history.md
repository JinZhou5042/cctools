# DataVine History

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
