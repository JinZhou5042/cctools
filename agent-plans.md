# DataVine active implementation plan

Updated: 2026-08-24

Design filter for every change, in order: **lightweight, efficient, high
performance, maintainable**. A feature that duplicates authority, adds an
unbounded hot-path object, or hides correctness work behind a benchmark is not
acceptable.

This is the active engineering contract. Historical decisions, experiments,
and completed phase narratives are preserved verbatim in
`agent-plans-history-20260810.md`. Current evidence belongs in `progress.md`
and `acceptance/`; do not grow this file into another execution log.

The production static IR is complete and accepted. Compact task/data records, shared
defaults, producer-completion readiness, pending-publication loss handling,
and the 4x16 scale/recovery gates are documented in `STATIC_IR_V2.md`. Dynamic
workflow optimization remains deliberately deferred to the next phase.

The production protocol is frozen at one v1 contract. The active callable path
is object-backed `callable-v1` with `DVP1` tickets and `DVM1` manifests; older
callable ticket and manifest parsers are removed. See `DATAVINE_PRODUCTION.md`.

## Active data-intensive benchmark campaign (2026-08-23)

The implementation is on branch `benchmark/data-intensive-million-file`; the
production tag `datavine-production-v1-20260823` remains unchanged. The frozen
full workload is documented in `DATAVINE_DATA_INTENSIVE_BENCHMARK.md` and has
contract SHA-256
`796255d97815b1dca6ed192931f04d8c97ee086bdfad49b532a14bedddc45ee0`:
1,048,576 tasks, 10,485,760 workflow files, 1.039871 TiB stored artifacts,
3.696121 TiB logical data path, and exactly 128 workers x 16 cores.

Current PASS evidence:

- topology/generator/kernel contract test: exact counts and bytes, regular
  A degree 20, B degree 1, deterministic non-sparse generation, corruption
  rejection, real random `pread`, and 2 ms to 5 s process-CPU work. Full-hash
  validation has 128 independent Condor parts whose signed summaries bind to
  the current generator part manifests before final assembly;
- `source-v1` now uses the worker-local output manifest/direct-durability path
  for indexed output names, while retaining the old 24-byte source ticket for
  custom output-name compatibility;
- local 1x1 paired mechanism pilot: DataVine and TaskVine both 256/256 physical
  tasks, all gates PASS, sampled C result SHA-256 values equal; the canonical
  machine-compared sealed SharedFS-source pair is TaskVine 27.01 s versus DataVine
  26.21 s (1.03x) with 76.38% fewer manager data-plane bytes. This is pilot evidence, not a production
  performance claim;
- the production comparison configures TaskVine's native `declare_url` with
  canonical SharedFS file URIs, matching DataVine's SharedFS
  source transport. The earlier manager-relayed pilot and the stopped
  pre-execution full graph load are diagnostic only and
  cannot support the final manager-byte claim;
- one-use TaskVine source URLs are task-scoped, while A/B intermediates remain
  workflow-scoped. This keeps source retention within the 4 GiB/worker disk
  contract and leaves intermediate movement/GC as the measured pressure;
- 19 superseded DataVine factory packages (about 15 GB) were moved to the
  recoverable `/project01/ndcms/jzhou24/datavine-benchmarks/factory-package-archive-20260823`
  archive with original-path symlinks. Active production and rollback packages
  were not moved.

The full dataset gate is PASS: all 128 atomic part manifests bind exactly
9,437,184 source files and 765,393,371,136 logical/allocated bytes to the
frozen contract. Condor cluster 15870 independently reread and hashed all
712.8 GiB as 128/128 PASS verification parts. Final manifest SHA-256 is
`3a391fea1ff1251015689e640e897c06aafc8e5a3a4a6aea6055ba8d541b5b71`.

OPEN: the user requested one complete TaskVine/DataVine pair at exact 128x16
before implementing the next optimization.  Report it as
`full-scale-single-pair`, not as the five-pair production statistics claim.
The first full attempt is diagnostic only: explicit graph ingestion reached
about 20 GiB Manager RSS and the default 100-task scheduling pass populated
only 100 of 128 FunctionCall libraries.  The scheduling-depth fix and the
parametric/lazy IR response are documented in
`DATAVINE_PARAMETRIC_IR_PLAN.md`.  Do not report the stopped run or pilot ratio
as the full DataVine advantage.

The scheduling-width fix and node-local runtime-info isolation are now live.
The latest full attempt built all 1,048,576 tasks, created 1,048,576 per-task
staging argument files, admitted 128x16, and completed 10,436 tasks before the
operator stopped it.  Three manager worker-read failures match three
HTCondor eviction events to the second; the jobs were well inside memory/disk
requests and were rematched.  Acceptance therefore records opportunistic
churn but requires exact admission, zero exhausted attempts, exact logical and
physical completion, >=90% central active cores, and exact final-pool recovery.
The replacement full pair remains OPEN.  Compact evidence is
`acceptance/data-intensive-large-scale-condor-churn-diagnostic-20260824.json`.

After the pair, implement the parametric IR in this order: checked family
evaluator, inverse dependency mappings, bounded task materialization and
packed state, batched bitmap journal/checkpoints, then generation-checked
cache-update/unlink vectors and family/index FunctionCall arguments.  The
targets are <=60 s graph load, <4 GiB pre-execution RSS, >=100x smaller
registration payload, and no per-task argument inode while retaining one
independent physical execution per logical task attempt.

The fixed 1024-core comparison is complete and PASS. The next measured
optimization order is now evidence-driven:

1. add parallel/streaming multi-DataID result reads; the remaining 32 MiB
   broadcast regression is 0.109 s and directly explained by 0.121 s fetch
   excess;
2. keep one WorkflowSession Runtime lane resident across append/result cycles;
   the current 8-step dynamic case enters the Runtime 9 times;
3. preserve the current high-degree bulk graph path, where DataVine is already
   significantly faster, and add a cold-manager versus long-lived-manager
   diagnostic for TaskVine scheduling growth;
4. run the real scientific workload suite only after its independent remote
   byte-attribution gates pass. Synthetic fixed-core evidence must not be
   relabeled as an application result.

## 1. Mission and boundary

DataVine is a language-neutral, durable, dynamic scientific-workflow layer on
TaskVine. A Python notebook, Python script, shell program, Go program, or a
future Rust adaptor emits the same Workflow IR. Native C owns validation,
durability, scheduling, retries, recovery, result identity, and TaskVine task
declaration. An adaptor must not reproduce those mechanisms.

The stable boundary is:

```text
user language -> thin adaptor -> Workflow IR / RPC
                              -> C workflow store + runtime + data controller
                              -> ordinary TaskVine manager/tasks/workers
```

TaskVine core remains a generic execution substrate. New DataVine policy and
state belong under `taskvine/src/datavine/`; core code may call a narrow,
generic helper but must not own DataVine workflow state.

## 2. Current source layout

- `taskvine/src/datavine/`: journal, validator, store, scheduler, runtime, RPC,
  and the multithreaded file-backed Data Controller.
- `taskvine/src/tools/datavine_workflow.c`: service and language-neutral CLI.
- `taskvine/src/tools/datavine_executor.c`: compact native builtin executor.
- `taskvine/src/tools/datavine_python_executor`: preloaded Python fork
  lifecycle and direct cloudpickle output-file writer.
- `taskvine/src/bindings/python3/ndcctools/taskvine/datavine/workflow.py`:
  optional Python builder and notebook session.
- `workflow_client.py`: thin Python RPC transport.
- `taskvine/examples/`: minimal Python, shell, and Go adaptor examples.
- `taskvine/test/datavine_*workflow*`: supported end-to-end contracts.
- `acceptance/`: durable reports, matrix, scripts, and handoff hashes.

Removed historical Python controller/scheduler/cache/persistence modules and
their tests must not be reintroduced as a second control plane.

## 3. Ownership contract

| Concern | Sole owner |
|---|---|
| User control flow and value serialization | language adaptor |
| Workflow/delta validation and limits | C validator |
| Generation CAS and idempotency | C store |
| Workflow journal and replay | C journal/store |
| Readiness, priority, retries, recovery | C scheduler/runtime |
| Result paths, files, hashes, metadata, retention, fetch, and GC | C Data Controller |
| Task state and resource declarations | C runtime |
| Worker allocation, transfer, execution | TaskVine core |
| Python child-process lifetime and direct pickle-file encoding | executor tool |

Resource ownership must be explicit. Parsed JX roots, buffers, tables,
schedulers, mailboxes, transactions, and TaskVine tasks have one destructor
path. Error exits must converge on those destructors instead of duplicating
partial cleanup.

## 4. Stable external contract

### Workflow IR

- Full schema: `datavine.workflow/v1`.
- Delta schema: `datavine.workflow-delta/v1`.
- IDs are positive integers; workflow/idempotency identifiers are at most 256
  bytes; identifier arrays are canonicalized by ID.
- DataVine executors use the single production v1 values in
  `DATAVINE_PRODUCTION.md`; external codec formats retain their own versions.
- Unknown required schema, executor, codec, field type, or out-of-bound graph
  must fail before mutation.
- Append is immutable and guarded by `expected_generation` CAS.

### Service

The framed RPC service owns the store and runtime. The CLI and adaptors are
clients even when launched on the same host. This process boundary is what
allows notebooks to detach/reconnect, language adaptors to exit, and workflow
state to survive client failure.

Capabilities must be checked before mutation. RPC frames, connections, event
retention, payloads, tasks, edges, and result sizes remain bounded.

### Production compatibility

- Never renumber an existing journal opcode or RPC opcode.
- Historical payload-bearing records within the production v1 journal remain
  replay-only. New workflow-journal
  writes never contain result bytes; Data Controller metadata has its own
  journal and immutable result files.
- A truncated final journal record is recoverable by truncation to the last
  valid record. Invalid magic, sequence, length, checksum, or payload is
  fail-closed.
- Schema or protocol changes require clean rebuild/install, restart replay,
  and all language-adaptor tests.

## 5. Dynamic scientific-workflow semantics

A streaming workflow may append tasks after observing completed values. Each
append is a bounded immutable delta. Quiescence is not completion: an open
workflow may become `open_quiescent`, accept another delta, and resume.

The notebook API supports ordinary Python `if`, loops, `await`, and futures.
Local timeout or kernel interruption stops only the local wait. Durable state
and completed values remain native; `attach` reconstructs the client frontier
without copying scheduler state into Python.

Python callable/source execution runs in managed child processes. The service
must reap failed children, retry according to native policy, survive generator
restart, and leave no leaked Python executor after terminal workflow state.

## 6. TaskVine-core isolation

Allowed core changes are small generic primitives reusable outside DataVine,
currently including task-frame ownership, function-call support, scheduler
helpers, millisecond wait, and manager hooks. DataVine journal, RPC,
directory/index, scheduler,
and workflow policy must not live in `taskvine/src/manager/`.

For every new feature, decide in this order:

1. Implement it entirely in `taskvine/src/datavine/`.
2. If core capability is missing, add a generic helper module and call it from
   core through a narrow interface.
3. Modify existing core logic only when neither option is possible; record the
   exact diff and regression evidence.

## 7. Performance and scale invariants

- Graph construction, readiness updates, completion, and replay must be
  amortized O(tasks + edges), never repeated whole-graph scans.
- Hot paths use integer IDs and indexed tables; serialized documents remain a
  boundary format, not runtime scheduling state.
- Journal writes use batching/group commit with bounded queued bytes.
- A benchmark is valid only if exact requested, submitted, completed, failed,
  and requested-output counts agree after process exit.
- Direct/noop throughput is control-plane evidence, not CPU/fork evidence.
- CPU claims require real persistent `FunctionCall-fork` work, executor
  lifetime evidence, GIL avoidance, utilization, and output validation.

## 8. Required acceptance gates

No PASS may be inferred from a build or success-looking stdout alone.

### Every code change

1. `make -C taskvine/src/datavine format`
2. `make -C taskvine/src/datavine -j8`
3. `make -C taskvine/src/tools datavine_workflow datavine_executor -j8`
4. `python taskvine/test/datavine_module_boundaries.py`
5. Run the directly affected test(s).
6. `git diff --check`

### Release/handoff change

1. Clean build and install from this checkout.
2. Nine supported contracts pass: module boundary, IR, service, execution,
   lifecycle, dynamic, notebook, shell, and Go.
3. Strict native 1M task benchmark records exact counts and throughput.
4. Rebuild `datavine.tar.gz` from the active DataVine conda environment.
5. Verify the package with `poncho_package_run`, then run one packed workflow.
6. Update `acceptance/matrix.md`, `progress.md`, and SHA-256 manifests.
7. Verify every manifest with `sha256sum -c`.

The authoritative commands and latest artifacts are listed in `progress.md`.

## 9. Maintenance rules

- Search before adding code; reuse the existing owner rather than create a
  parallel abstraction.
- Keep public, runtime-only, and replay-only APIs visibly separated.
- Centralize protocol constants and schema names per language boundary.
- Preserve user changes in a dirty tree; never use destructive reset/clean.
- Delete generated `__pycache__`, `vine-run-info`, temporary journals, and
  benchmark scratch state before handoff.
- Archive superseded long-form reports instead of mixing history into active
  contracts.
- Add a test before deleting compatibility code whose producer no longer
  exists.

## 10. Current architecture/performance execution

The 2026-08-10 round below is retained as historical evidence; its old result
path and performance artifacts were superseded on 2026-08-11:

- [x] Remove the grouped-noop physical-task shortcut and restore one logical
  task per TaskVine lease, resource request, retry, and completion.
- [x] Remove duplicate callable output files; transport callable results once and
  materialize only retained downstream buffers.
- [x] Remove the redundant ticket payload ID, unused scheduler state, and unused
  public delta-validation wrapper.
- [x] Route service runtime information into self-cleaning temporary paths.
- [x] Delete historical acceptance snapshots, duplicate reports, stale test
  binaries, run directories, and superseded factory packages.
- [x] Re-run 9/9 contracts, exact independent 100k/1M, and fair 1/4/16-core
  CPU comparisons.
- [x] Rebuild, verify, and promote the post-review packed environment.

- [x] Centralize workflow ID, schema, and event-batch constants.
- [x] Split journal replay into bounded opcode-specific handlers.
- [x] Centralize store transaction ownership and destruction.
- [x] Centralize runtime execution resources and cleanup.
- [x] Label service-facing, runtime-only, and replay-only APIs.
- [x] Add corrupt-checksum journal rejection coverage.
- [x] Remove generated test/build residue.
- [x] Pass clean build and all nine contracts.
- [x] Pass strict 1M and packed-environment workflow gates.
- [x] Refresh acceptance artifacts and verified handoff hashes.
- [x] Move Python callable execution to a persistent preloader plus isolated
  fork children with bounded framed transport.
- [x] Split Python callable registration from invocation: one content-addressed
  callable per builder, compact per-task calls, and a bounded raw-byte executor
  cache that never deserializes user code in the parent.
- [x] Prove cancel and wall-time process-group cleanup with no leaked children.
- [x] Fix append/quiescence with generation CAS.
- [x] Reuse the validated JX root as the durable delta transaction.
- [x] Make scale gates fail closed unless logical task count equals physical
  TaskVine submission and completion counts.
- [x] Run fair 1/4/16-core CPU fork comparison and retain all results.
- [x] Remove the unconditional 10 ms post-completion runtime sleep and close
  single-worker 1/4/16-core parity with three retained 16-core samples.
- [x] Rebuild, verify, promote, and packed-run the active conda package.

The 2026-08-11 result-plane redesign is the active gate:

- [x] Add one multithreaded C Data Controller with immutable DataID/attempt
  files, SHA-256 identity, a private metadata journal, recovery, retention, GC,
  direct fetch, and ordinary TaskVine file restoration.
- [x] Make Python fork children write cloudpickle output files directly.
- [x] Delete historical base64/stdout result parsing and remove the workflow store's
  current result-writer API; retain old payload records as replay-only reads.
- [x] Queue result publication asynchronously so Runtime keeps harvesting and
  dispatching while Data Controller threads hash and commit completed outputs.
- [x] Mark a task complete only after atomic Data Controller metadata commit.
- [x] Prove live pre-terminal fetch, restart fetch, selective pruning,
  multi-output publication, payload-free workflow journaling, and corruption
  rejection in the native execution contract.
- [x] Resolve the notebook cross-workflow lane gate by creating declared
  placeholder output files before callable execution; failed attempts are not
  published, and unrelated workflows no longer incur TaskVine recovery delay.
- [x] Rerun exact 100k/1M with 100% physical count agreement; Runtime reaches
  4,360 and 4,292 tasks/s respectively.
- [x] Obtain 9/9 with isolated Go 1.22.5, retain a hash-verified static
  prebuilt adaptor, and revalidate the Go contract through that binary.
- [x] Rebuild and hash-verify a packed candidate; packed 1x2x10k passes exact.
  Do not promote over production until the Go environment gate is available.
- [x] Rerun 32 MiB reuse and wide multi-output in both backend orders with ten
  samples per backend. Correctness passes, but performance regresses to 0.311x
  and 0.181x versus TaskVine because file materialization/return now dominates.
- [x] Replace that per-output manager return with the smallest native
  data-plane contract: non-requested callable intermediates are `VINE_TEMP`
  files retained in worker cache and peer-transferred by TaskVine; only
  requested public values use Data-Controller-owned output paths.
- [x] Stream SHA-256 during the sole cloudpickle write and return only a compact
  production DVM1 size/hash manifest; retain zero base64 or result parsing in Runtime.
- [x] Preserve dynamic quiescence by reusing active temp files in the same
  service lifetime, and recompute producers after service restart when their
  non-durable worker-local outputs are gone.
- [x] Add a forced service-restart regression that proves an unrequested
  intermediate producer executes again and the requested final value remains
  correct.
- [x] Re-run exact 100k/1M: 100% physical count agreement at 4,303 and 4,122
  Runtime tasks/s respectively.
- [x] Re-run resident 10x16 n=5 data-heavy gates. Final-order DV/TV is 0.326x
  for 32 MiB reuse and 0.361x for wide multi-output; cross-order corroboration
  is 0.437x and 0.392x. No large intermediate is persisted to the Controller.
  This closes architecture correctness, but performance parity remains OPEN.
- [x] Build and hash-verify the worker-local candidate package and pass packed
  1x2x10k at exactly 10,000/10,000 and 3,365 Runtime tasks/s. Promote that
  exact archive only after the 9/9 gate; retain the previous production hard
  link as rollback. The active production path then passed 10,000/10,000 at
  3,707 Runtime tasks/s.
- [x] Establish Git implementation checkpoint `d64387542` containing the worker-local source,
  focused tests, compact acceptance evidence, current documentation, and no
  Factory/journal/debug residue; the post-commit runner reproduced 9/9 PASS.
- [x] Add cumulative output-publication stage metrics without returning result
  payloads through Runtime. The 1x1 profile isolated repeated Python input
  decode (923 ms) rather than Controller publication (about 27 ms) as the 32
  MiB reuse bottleneck.
- [x] Stream worker-local cloudpickle inputs directly from files. The
  three-repetition local median reached 0.905x TaskVine for 32 MiB reuse while
  the complete source regression remained 9/9 PASS. This is a pilot; the
  resident 10x16 performance gate remains OPEN.
- [x] Profile the residual wide fixed cost. Manager-lock wait was about 19 ms;
  an active-only 1 ms Manager idle-poll tune reduced it to 1.6 ms and raised
  the five-repetition local wide rate from 0.675x to 0.824x TaskVine without
  changing TaskVine Core. A 32 MiB reuse check reached 0.961x and 9/9 passed.
- [x] Rebuild and hash-verify the output-heavy candidate; packed 1x2x10k passed
  exactly at 3,349 Runtime tasks/s. The clean resident 10x16 crossed-order gate
  passed 40 backend-runs/12,820 physical tasks. Rates were 0.650x/0.691x for
  32 MiB reuse and 0.411x/0.420x for wide multi-output.
- [x] Fail closed on a transient TaskVine reference error in the first reverse
  attempt, prove DataVine's 320 persisted results match stable TaskVine r2-r5,
  add same-backend repetition validation, and pass a clean reverse rerun.
- [x] Promote the exact verified candidate atomically after the active-path
  1x2x10k smoke passed 10,000/10,000 at 3,370 Runtime tasks/s. Retain the old
  worker-local archive as exact rollback.
- [x] Batch concurrent durable publications inside the existing Controller
  with a bounded 1 ms leader window and one journal barrier per ready group.
  Preserve post-durability visibility and replay; add no agent, RPC, or
  TaskVine Core logic. Final wide median records 16 barriers for 320 durable
  outputs.
- [x] Add compact retained/durable output policy. The production freeze removes
  historical ticket compatibility.
  Skip unconsumed output serialization and recomputable VINE_TEMP/manifest
  fsync while preserving requested-output fsync; focused contract PASS.
- [x] Cache one callable snapshot per Workflow object identity and avoid the
  redundant private deep copy on synchronous submit/append. The 480-task
  profile performs one function cloudpickle and builds in 24.4 ms.
- [x] Pass the post-commit regression 10/10, exact packed 1x2x10k at 3,382
  Runtime tasks/s, and final 10x16 n=5 exactness. Rates improve to 0.814x for
  32 MiB reuse and 0.599x for wide; retain an explicit performance limitation.
- [x] Promote the exact hash-verified candidate atomically and pass the active
  production-path 1x2x10k smoke at 10,000/10,000 and 3,332 Runtime tasks/s;
  retain the preceding production package as rollback.
- [ ] Close the remaining output-heavy execution/fetch gap. Do not optimize
  Manager polling or journal barriers again without new evidence; next profile
  must separate per-fork executor overhead from strict requested-result
  publication/fetch and compare equal durability semantics.
- [x] Design the next data-intensive scientific workflow suite. The frozen
  plan defines partitioned HEP scan/reduce, shared calibration reuse, genomics
  shuffle/merge, climate tiled iteration, adaptive search and ensemble
  checkpoint workloads; cold/warm/peer/cache-pressure placement; strong/weak
  scaling; equal-sink versus native TaskVine contracts; remote byte/resource
  accounting; fault recovery; paired statistics; and explicit promotion gates.
  See `SC_SCIENTIFIC_WORKFLOW_BENCHMARK_PLAN.md`.
- [x] Implement and locally validate the suite foundation: versioned artifact
  schema, deterministic non-sparse/cloudpickle shard generator with corruption
  verification, Linux process/network/disk sampler with known-byte calibration,
  and HEP-S TV-native/DV-native/TV-durable-sink driver. The retained 1x1 pilot
  passed 6/6 runs at 19/19 physical tasks with one exact digest; full regression
  passed 11/11 using the hash-verified Go binary.
- [ ] Integrate sampler snapshots with actual producer/consumer worker identity
  and per-worker transfer attribution, then implement CAL-small cold/warm/
  peer-required gates. Do not launch 10x16 or make a performance claim until
  those byte-accounting gates pass.

## 11. Ordered next work

### SC workflow-characterization campaign

The next active experiment compares DataVine with native TaskVine using one
shared workload model and ordinary one-task/one-lease execution. It must cover
independent maps, chains, fan-out, fan-in, diamonds, staged pipelines,
heavy-tailed tasks, reusable large intermediates, selective multi-output, and
result-driven dynamic workflows.

Fairness gates:

1. Both backends execute the same Python kernels with `FunctionCall-fork`
   semantics, inputs, outputs, task resources, DAG, worker count, and cores.
2. Workflow construction, submission, execution, and requested-result fetch
   are reported separately and end to end; startup and one explicit executor
   warmup are excluded for both backends.
3. Every requested result is content-validated. Exact logical, submitted,
   completed, failed, and retry counts are retained; semantic batching is
   forbidden.
4. Record useful child CPU, wall time, throughput, Manager/runtime CPU and RSS,
   transferred bytes where available, environment identity, raw repetitions,
   median, spread, and limitations.
5. A local/cgroup-limited campaign is a pilot, not a distributed SC result.
   Multi-worker cluster repetitions and statistical confidence remain OPEN
   until run on dedicated hardware.

Deliverables are a reusable comparison driver, raw JSON artifacts, a concise
machine-readable summary, and `SC_WORKFLOW_COMPARISON_REPORT.md` containing
methodology, correctness evidence, results, architecture interpretation,
threats to validity, and exact reproduction commands.

Campaign outcome (2026-08-10):

- [x] Implement one shared two-backend driver with exact result and physical
  task validation and no semantic batching.
- [x] Keep each 10-worker x 16-core Factory pool resident across its backend
  run and fail closed until all 10 workers/160 cores are connected.
- [x] Complete five-repetition primary and feature campaigns plus a
  three-repetition crossed-order campaign: 54,154 successful physical
  executions and zero count mismatch.
- [x] Cover independent, serial, fan-out/in, diamond, pipeline, heavy-tail,
  reusable-data, selective multi-output, duration, and dynamic features.
- [x] Diagnose the wide multi-output `publish_failed` result as legal short
  pipe writes, fix full-frame transport, add forced-partial-write regression,
  and pass five fixed 10x16 repetitions.
- [x] Publish raw JSON and `SC_WORKFLOW_COMPARISON_REPORT.md` with negative
  results and explicit threats to validity.
- [ ] Repeat on paired homogeneous reserved nodes with at least ten repetitions,
  confidence intervals, remote-worker resource/network counters, and real
  scientific applications before an SC causal performance claim.

- Multi-manager/Foreman partitioning, sharded metadata, and distributed
  placement remain research scope, not current production claims.
- Repeat the fair CPU gate at 1/8/16 workers and add chain, fan-out, reusable
  large-input, peer, and SharedFS cases.
- Add a combined child/worker/source/publication/preloader fault matrix and
  retain exact DataID/attempt/result evidence.
- Historical protocol migration is outside the frozen production-v1 scope;
  removed executor, ticket and manifest generations fail closed.
- Security beyond token authentication (TLS, rotation, authorization domains)
  remains OPEN.

These items must stay visibly OPEN; they do not block the existing single
service language-neutral dynamic-workflow contract.
