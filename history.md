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
