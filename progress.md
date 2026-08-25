# DataVine current checkpoint

Updated: 2026-08-24

## Current checkpoint — Runtime v2 local acceptance (2026-08-24)

The first native full-scale launch reached exact 128x16 admission and held all
2,048 slots, but exposed a Worker source-ingest multiplier: every one-use
`file:///` origin forked curl and made a redundant cache copy. With ten blocked
curl processes per worker, the final 120-second window completed only about
2.76 tasks/s and could not fit the 24-hour worker lifetime. The run was stopped
at 430 sampled completions and remains a FAIL diagnostic. A direct-symlink
follow-up made random task reads hit SharedFS and was slower. Worker Data Agent
now performs an in-process sequential copy of each local SharedFS source
directly into its task sandbox, with no curl process or cache record. Real
source bytes, intermediate peer movement, and GC are unchanged. The initial
synchronous loop starved worker heartbeats when it copied all 36 inputs in one
turn; the corrected loop groups fast files within a 25-ms budget and returns
`WAIT_DATA` after crossing it. A consumed Worker Agent hint also reduces the
next poll timeout from 5 seconds to 1 ms while data waits remain. A replacement
exact 128x16 run is required after the revised regression gates pass.

A later explicit-`LOCAL_FILE` 128x16 attempt exposed a second scale-only
failure. Dense FunctionCall completion delivered `SIGCHLD` while the Worker
Data Agent was synchronously reading SharedFS sources; `read(2)` returned
`EINTR`, and the old loop incorrectly converted that retryable interruption
into permanent input loss and `FORSAKEN`. The attempt submitted 14,467 physical
tasks, completed 10,371 as `FORSAKEN`, and emitted zero successful task reports.
The copy loop now retries `EINTR`, preserves the failing operation and errno,
and limits failure diagnostics to 32 per worker. A 16x16 Condor probe then
completed 2,336 tasks with zero failed inputs. The installed/runtime binary was
also synchronized before the final 17/17 regression and ordinary TaskVine
smoke passed. Evidence is retained in
`acceptance/data-intensive-parametric-eintr-diagnostic-20260824.json`.

The next 128x16 attempt proved the EINTR repair under scale: it passed 82,745
physical completions before any failure. It then exposed Manager-lane
starvation because the 4,096-task in-flight bound was also used as one
materialize-and-submit lock batch. Worker admission advanced only from 85 to
113 between factory samples, status connections timed out, and 13 tasks were
eventually reclaimed as `FORSAKEN`. Runtime now keeps the 4,096-task window but
limits each Manager-lock submission and completion batch to 128. `FORSAKEN` is
now a separately bounded infrastructure retry (maximum 64 physical attempts)
that does not consume the workflow's application retry budget; ordinary task
failure semantics remain unchanged. The lifecycle worker-loss test now proves
recovery with `maximum_attempts: 1`.

A final admission diagnostic showed that bounded Runtime turns alone could not
absorb 128 simultaneous worker connects because the shared `link` listener
still hard-coded a backlog of five. The generic listener now requests
`SOMAXCONN` (4096 on the acceptance host), so pending worker and status
connections remain queued while Runtime briefly owns the Manager lane. This is
a transport-capacity correction shared by DataVine and the unchanged TaskVine
execution semantics. Post-change DataVine regression is 17/17 and ordinary
`TR_vine_single` passes.

Runtime v2 now separates compute completion from data admission. TaskVine
Manager owns physical dispatch/completion and transports only a generic opaque
auxiliary frame. DataVine Scheduler marks a successful physical task DONE and
releases children immediately. Worker Data Agent and Controller independently
own DataID resolve, replica sessions, peer movement, persistence, failure, and
generation-checked GC; DataVine cache transitions and output payloads do not
enter the Manager data plane. This supersedes the older publication-gated
readiness description below, which remains historical production-v1 context.

The local source/build gate is PASS with 17/17 DataVine tests plus ordinary
TaskVine `TR_vine_single`. It covers HMAC Worker HELLO, atomic batch admission,
digest conflict rejection, late-publication tombstones, reconnect inventory,
Owner restart, Worker loss, direct durable results, dynamic append, Shell,
Notebook, Go, and the scientific foundation. The regression report was written
to `/tmp/datavine-eintr-regression-20260824-r3.json` during this checkout and is
ephemeral; source tests are the durable reproduction mechanism.

The full data-intensive parametric descriptor is 1,344 bytes and expands to
exactly 1,048,576 tasks and 10,485,760 files. Native Store/Scheduler/Runtime
integration is complete: full graph construction takes about 2.59 seconds and
172 MiB RSS, the exhaustive topology digest matches the independent Python
oracle, and Runtime retains at most a 4,096-task materialization window. A
current-code 4x4 E2E passed all gates with 256/256 independent physical tasks,
exact reported reads, requested-only durability, zero Manager output payload,
and final Worker Agent GC state. Owner plus all workers were also SIGKILLed and
successfully recovered from the same journal with empty caches without rolling
logical DONE backward. The exact 128x16 execution is still OPEN, so no
full-scale performance advantage is claimed from this checkpoint.

## Current checkpoint — production v1 freeze (2026-08-23)

All active DataVine-owned boundaries now use one production v1 contract.
The source, tests, documentation and retained evidence are frozen by annotated
Git tag `datavine-production-v1-20260823`.
Python callable execution is object-backed `callable-v1` with a `DVP1` worker
ticket and `DVM1` output manifest. Historical callable tickets and manifest
parsers were removed rather than retained as a second execution path. The
authoritative boundary table and compatibility policy are in
`DATAVINE_PRODUCTION.md`.

The post-freeze source regression is 13/13 PASS, including IR validation,
Python callable execution, output retention, data-plane object pulls, notebook
fork lifecycle, dynamic workflows, Shell, Go and the scientific foundation.
The retained report is `acceptance/production-v1-regression-20260823.json`.

## Current checkpoint — production static IR (2026-08-23)

Static workflows now use optional `task_defaults` and `data_defaults` plus
compact task and produced-data records. One logical task remains one physical
TaskVine task per attempt; graph-registration deltas are transport framing, not
task batching. Scheduler readiness is exclusively producer-task completion.
The Runtime marks that producer complete only after physical success and
successful output publication, so a lost last replica during publication
causes the producer to retry without releasing downstream tasks.

The fresh 4-worker x 16-core gates pass at one million independent tasks and at
100,000 real Python tasks with one million input references and one million
logical outputs. On the latter identical graph, compact IR reduced workflow
payload 87.80%, graph-load time 84.38% (6.40x), and load RSS 65.57% versus the
full-object IR baseline. A separate live-loss gate removed four workers and
validated minimal producer/descendant recomputation with exact sink results.

The design, record forms, ownership rules, reproduction commands, measurements,
artifact paths, and explicit OPEN boundaries are in `STATIC_IR_V2.md`. The
machine-readable delivery index is
`/project01/ndcms/jzhou24/datavine-benchmarks/static-ir-v2-delivery-20260823.json`.

## Current checkpoint — production decoupled data plane (2026-08-18)

The input data path is now decoupled from scheduling. Workflow IR and the
scheduler retain logical DataIDs and content identities only. The native Data
Controller owns the persistent SHA-256 object store, current location
resolution, digest-scoped worker tickets, stable TaskVine file identities,
publication, recovery, and lifecycle. Workers actively pull `datavine://`
objects, stream them into cache, and verify SHA-256 before installation.

Python emits production callable-v1/DVP1, serializes functions and structurally identical
invocations once, hashes remaining immutable bytes, and suppresses duplicate
PUTs. Function and invocation objects are not TaskVine inputs: the persistent
worker-local executor pulls them directly over a reused, digest-scoped
connection. The Controller independently verifies and atomically deduplicates
every object. The durable IR contains neither payload base64, service address,
worker ticket, nor workflow token. One-object-per-file v1 advertises its
67,108,800-byte limit; chunking remains an isolated future object-store
extension.

Final validation passed 13/13 repository contracts. A 64-task shared 4 MiB
test produced four object records, three unique PUTs, one local deduplication,
one ordinary worker-cache pull, and direct executor object pulls. A one-million
repeated-eData build produced 1,000,002 Data records in 22.249 s (44,945.2
tasks/s) while serializing the invocation once. Orthogonal eight-core runs
reported `scheduler_delay` for 256 immediate tasks, `data_stage_in` for 16
unique 4 MiB inputs, and `python_function` for 64 tasks at 50 ms fixed CPU
each. Every service writes a default journal-adjacent profile including direct
executor pull time.

The exact contract, limitations, final measurements, complete regression
report, and checksums are retained in `DATAVINE_DATA_PLANE_V2.md` and
`acceptance/data-plane-v2/`.

## Outcome

The post-upgrade fixed 1024-core DataVine-versus-TaskVine campaign is **PASS**.
It used two exact 64-worker x 16-core resident pools, 49 workflow cases, 98
accepted warmup backend runs and 980 accepted measured backend runs. Ten paired
repetitions, exact result hashes and physical-task counts, zero churn in every
accepted measurement and complete regression attribution all pass. Twenty-eight
runs overlapping explicit Condor eviction were retained as invalidated evidence
and excluded. Paired 95% intervals classify 46 cases as DataVine-faster, 2 as
DataVine-slower and 1 as inconclusive.

Representative throughput is 208.9 versus 111.5 tasks/s for 4096 immediate
tasks, 71.6 versus 62.3 tasks/s at fixed 10 s CPU, and 232.8 versus 119.4
tasks/s for fan-in 16 (DataVine versus TaskVine). The 32 MiB requested-output
case now favors DataVine, 11.224 s versus 13.008 s. The two confirmed
regressions are bounded and fully attributed: 32 MiB broadcast is 1.371 s
versus 1.262 s because DataVine result fetch adds 0.121335 s of direct
critical-path excess; eight-step serial dynamic append is 1.007 s versus 0.186
s because it enters the Runtime nine times. The improvement interfaces are
parallel/streaming multi-DataID reads and a resident WorkflowSession runtime
lane, respectively.

Fresh one-object-per-file SharedFS profiling gives 83.4 cold objects/s with one
writer and 752.9 objects/s with 16 bounded writers for 4096 distinct 256-byte
objects; warm deduplicated throughput is 3162.2 objects/s. The durable compact
campaign, generated report, CSV and ingest profile are under
`acceptance/1024-core-workflows/data-plane-v2-current-final-20260821/`.

The supported single-service, language-neutral dynamic-workflow architecture
is internally consistent and passes its current source gates. C is the sole
runtime authority; Python is an optional builder/notebook adaptor and managed
fork executor; Shell and Go use the same IR/RPC boundary.

Each logical task owns an independent TaskVine task, lease, resource request,
retry, and completion. Scale gates now fail unless the native runtime reports
exactly one physical submission and completion for every logical task.

The result plane is now separate from the control plane. Python callables write
cloudpickle once while streaming SHA-256 and return only compact metadata.
Producer completion reaches the scheduler after required output publication.
The worker Data Agent first
announces local availability for peer-first consumption, then lazily persists
the same object to its hash-derived SharedFS path and sends
`RESULT_PERSISTED`. Runtime and Controller memory never carry result payloads;
the shared journal contains metadata only. The Controller owns location,
persistence, pruning, GC and permanent-loss recovery, while the TaskVine
manager alone owns running-task loss and resubmission.

## Cleanup and semantic fixes

- Deleted the grouped-noop executor path, physical groups, projected results,
  projection metrics, and its executor operation.
- Removed the historical base64/stdout result transport. Callable and source executors
  write direct output files; ordinary TaskVine transfer or SharedFS moves those
  files without routing payload bytes through Runtime.
- Replaced manager-side persistence with worker-local availability followed by
  asynchronous Data-Agent-to-SharedFS durability. Requested and intermediate
  bytes no longer route through Runtime or Controller memory.
- Added fail-closed restart semantics: completed producers are recovered only
  when their retained outputs are still active or durable; lost worker-local
  values cause producer recomputation.
- Reduced the previous tickets from 64 to 56 fixed bytes by deleting the redundant
  payload ID.
- Removed an unused scheduler enum and unused public JSON delta-validation
  wrapper; the parsed validator remains internal to the store.
- Added `DATAVINE_RUNTIME_INFO_PATH` to the service and made regression/scale
  drivers use self-cleaning temporary paths.
- Deleted 441 MB of generated runtime directories, 23 MB of duplicate source
  snapshots, 117 stale JSON/log reports, six retired test binaries, and eight
  superseded factory archives. The current package and one rollback package
  were preserved.

## Current evidence

The worker-local implementation checkpoint is Git commit `d64387542`. The
post-commit contract artifacts record the implementation/validation commits
directly; compact evidence and handoff hashes are retained in subsequent
documentation commits.

The worker-local Data Controller redesign passes warning-clean builds, module
boundaries, and its expanded execution contract: live result fetch before
workflow termination, restart fetch, selective pruning, multi-output atomicity,
2 MiB payload exclusion from the workflow journal, corruption rejection, and
forced service restart with recomputation of a non-durable intermediate.
The full runner is 9/9 PASS, including notebook cross-workflow concurrency and
the Go direct-protocol/dynamic-delta contract. Go was compiled in an isolated
Go 1.22.5 prefix, then the same contract passed using the retained static
prebuilt adaptor. Fresh strict scale runs pass at exactly 100k and 1M physical
submissions/completions.

A post-redesign candidate package was rebuilt from the active conda
environment and verified with `poncho_package_run`: cloudpickle is 3.1.2 and
the package-local service, native executor, Python executor, and worker hashes
match this build. Packed 1x2x10k passed exactly at 3,403 runtime tasks/s and
74.7 MB peak RSS. Candidate SHA-256 is `d9427cec...115821d`; it is retained as
`datavine.data-controller-candidate-20260811.tar.gz`. This superseded package
was not promoted.

The final worker-local candidate is
`datavine.worker-local-candidate-20260811.tar.gz`, SHA-256
`67c202c328b38f59d5da68a68ade5a05abdc076bdb1b57f49f15ad00a5b6f0bf`.
`poncho_package_run` confirms cloudpickle 3.1.2 and exact hashes for the
service, native executor, Python executor, and worker. Its packed 1x2x10k gate
passes exactly at 3,365 Runtime tasks/s and 75.3 MB peak RSS. After the 9/9 Go
gate, this exact archive was promoted atomically to production.

| Gate | Result |
|---|---|
| Warning-clean C build | PASS |
| Python static/module boundary | PASS |
| Supported executable contracts | PASS, 9/9 |
| Exact 100k independent tasks | PASS, 28.63 s E2E, 4,303 runtime tasks/s, 447 MB RSS |
| Exact 1M independent tasks | PASS, 295.64 s E2E, 4,122 runtime tasks/s, 4.04 GB RSS, 44 FDs, 5 processes |
| CPU fork, 1 core | PASS, DV/FC 1.003, 97.56% useful CPU |

The no-grouping guard was revalidated after the cleanup with a live 1x1 smoke:
1,000 logical tasks produced exactly 1,000 TaskVine submissions and 1,000
TaskVine completions. The scale driver now records these counts and fails on
any mismatch. The post-checkpoint regression run passed all 9/9 contracts.
| CPU fork, 4 cores | PASS, DV/FC 1.003, 97.07% useful CPU |
| CPU fork, 16 cores | PASS, DV/FC 0.966, 92.01% useful CPU |
| Package rebuild and packed E2E | PASS, 10,000/10,000; 3,365 Runtime tasks/s, 2,786 tasks/s end-to-end |
| Promoted active-package smoke | PASS, 10,000/10,000; 3,707 Runtime tasks/s, 3,033 tasks/s end-to-end |

Artifacts:

- `acceptance/native-data-controller-regression.json`
- `acceptance/native-data-controller-1x1x100k.json`
- `acceptance/native-data-controller-1x1x1m.json`
- `acceptance/native-data-controller-packed-1x2x10k.json`
- `acceptance/native-worker-local-final-regression.json`
- `acceptance/native-worker-local-hybrid-1x1x100k.json`
- `acceptance/native-worker-local-hybrid-1x1x1m.json`
- `acceptance/native-worker-local-packed-1x2x10k.json`
- `acceptance/native-worker-local-production-smoke-1x2x10k.json`
- `acceptance/worker-local-data-plane-20260811.json`
- `acceptance/go-adaptor-20260811.json`
- `acceptance/sc-workflow-worker-local-final-20260811-10x16-n5/summary.json`
- `acceptance/native-regression-deep-review.json`
- `acceptance/native-workflow-one-lease-1x1x100k.json`
- `acceptance/native-workflow-one-lease-1x1x1m.json`
- `acceptance/cpu-fork-deep-review-1x{1,4,16}.json`
- `acceptance/native-workflow-deep-review-packed-1x2x10k.json`

Promoted package:

- active: `/users/jzhou24/graph_optimization/factories/datavine.tar.gz`
- active production-v1 SHA-256:
  `6019adc524f86bf4d14b984e8a6f07928cf08964ec2a6a19031e36829f50adcd`
- production-v1 candidate hard link:
  `datavine.production-v1-candidate-20260823.tar.gz`
- immediate rollback: `datavine.pre-production-v1-rollback-20260823.tar.gz`,
  SHA-256 `32e1361324df69be2db88258565f3ad393d2eb16ff9228e99387b895d9abba6d`
- Go adaptor: `datavine_workflow_go-20260811`, SHA-256
  `9a52d76a35474cc8e7f5da5b26633428c0f929753ef218d9e7aaa7e9d5ddffd4`

`poncho_package_run` imported cloudpickle 3.1.2 and the DataVine API;
package-local hashes matched the installed `datavine_workflow`,
`datavine_executor`, `datavine_python_executor`, and `vine_worker`. Packed
candidate and promoted active-path 1x2x10k runs both completed exactly at
10,000/10,000. The active path reached 3,820.6 Runtime tasks/s, 50.1 MB peak
RSS, 42 FDs and six processes. Evidence is retained in
`acceptance/production-v1-packed-1x2x10k-20260823.json` and
`acceptance/production-v1-active-1x2x10k-20260823.json`.

## Reproduction

```sh
make -C taskvine/src/datavine format
make -C taskvine/src/datavine -B -j8
make -C taskvine/src/tools datavine_workflow datavine_executor -B -j8
make -C taskvine/src/bindings/python3 -B -j8
conda run -n datavine python taskvine/test/datavine_module_boundaries.py
DATAVINE_REGRESSION_REPORT=/tmp/datavine-regression.json \
  conda run -n datavine bash acceptance/scripts/run_regression.sh

PATH="$PWD/taskvine/src/tools:$PWD/taskvine/src/worker:$PATH" \
conda run -n datavine python acceptance/scripts/benchmark_native_workflow.py \
  --tasks 1000000 --workers 1 --cores 1 --executor builtin \
  --chunk-tasks 10000 --workflow-timeout 1200 \
  --output /tmp/datavine-1m.json
```

## Open research

Foreman partitioning, peer-transfer traffic counters and failure matrices,
SharedFS comparisons, incompatible-version migration, and TLS/authorization
remain OPEN. They are not claimed by the supported single-service contract.

## SC workflow characterization

### Worker-local data-plane rerun

The active design no longer returns large non-requested callable outputs to the
manager. In the final 10x16 n=5 run, the 32 MiB reusable root remained in worker
cache and was peer-served to consumers. Across all ten measured DataVine
workflows plus warmup, the Controller retained exactly 2,401 requested files
(556 KB total, maximum 236 bytes); no large intermediate entered its data
directory.

- Final-order 32 MiB reuse: TaskVine 0.815 s, DataVine 2.500 s, DV/TV 0.326x.
- Final-order wide multi-output: TaskVine 1.543 s, DataVine 4.273 s, DV/TV
  0.361x.
- Cross-order hybrid corroboration: 0.437x and 0.392x respectively.
- All runs used exact 10x16 resident pools and had zero task-count or result
  mismatch.

This is a large improvement for wide multi-output over the old 0.181x result,
and it proves the desired data ownership. It is still slower than TaskVine for
these output-heavy shapes, so it is not a performance-promotion claim. The
remaining fixed cost is per-task FunctionCall/materialization plus durable
requested-output handling, not large intermediate transfer through Runtime.
See `acceptance/worker-local-data-plane-20260811.json`.

### Output-heavy stage profile and first optimization

Low-overhead cumulative stage metrics now separate Data Controller queue and
commit work from Python input decode, user-function execution, serialization,
and fsync. Runtime still consumes task state only; the optional timing values
travel in the compact DVM1 manifest and no result payload is returned through
stdout.

The 1x1 local pilot showed that the Controller was not the 32 MiB reuse
bottleneck: publication queue/commit/prepare totaled about 27 ms while repeated
Python input decode totaled 923 ms. Replacing `read_bytes()` plus
`cloudpickle.loads()` with streaming `cloudpickle.load()` reduced the
three-repetition median decode total to 431 ms. The resulting 32 MiB reuse rate
was 0.905x TaskVine (DataVine 2.409 s, TaskVine 2.181 s), versus 0.753x in the
single pre-change diagnostic. Wide multi-output remained 0.670x; its decode
cost was only 2.5 ms, so its remaining gap is a separate per-task/output path.

That follow-up path exposed about 19 ms median Manager-lock wait caused by the
10 ms completion-pump poll across dependency waves. Runtime now uses TaskVine's
existing `idle-poll-milliseconds` tune at 1 ms only while workflows execute and
restores 10 ms while idle; TaskVine Core is unchanged. In a five-repetition
local run, wide multi-output improved from the untuned 0.675x to 0.824x
TaskVine (DataVine 170 ms, TaskVine 140 ms), and median lock wait fell to 1.6
ms. A three-repetition 32 MiB reuse check reached 0.961x and showed no local
regression.

The candidate package is
`datavine.output-heavy-candidate-20260811.tar.gz`, SHA-256
`9c1c8372c3cbc4baf43317257408213d07867938c6e0f767fb1009f018c8b9ca`.
Package-local cloudpickle/API and all four executable hashes match the installed
build. Its packed 1x2x10k gate passed exactly at 3,349 Runtime tasks/s and 74.9
MB peak RSS. Production was not overwritten during this gate.

The resident 10x16 crossed-order campaign then passed 40 valid backend-runs
and 12,820 exact physical tasks. DataVine-first rates were 0.650x for 32 MiB
reuse and 0.411x for wide multi-output; TaskVine-first rates were 0.691x and
0.420x. A discarded first reverse-order attempt fail-closed when TaskVine r1
duplicated `select-0` into two `select-2` results. TaskVine r2-r5 and the 320
persisted DataVine values agreed exactly; a clean reverse rerun passed. The
driver now checks same-backend consistency across repetitions so this class of
reference nondeterminism is reported at its source.

At cluster scale, Manager-lock time is negligible for wide multi-output while
cumulative Controller publication queue and Python fsync are high. Therefore
the next optimization target is bounded concurrent publication/durability,
not further poll tuning or any new control plane.

That bottleneck round is now complete at source commit `d9f0c9e4c`. The
existing four Data Controller workers prepare publications concurrently; a
bounded 1 ms leader window coalesces ready metadata records and uses journal
enqueue followed by one commit barrier. Results remain invisible until the
barrier succeeds, replay retains the existing `DATA_READY_BATCH` format, and
there is still one Controller owner with no new service, RPC, or TaskVine Core
logic. In the final 10x16 n=5 wide run, 320 durable outputs required a median
16 journal barriers rather than one synchronous barrier per output.

The production Python ticket also carries retained/durable output policy.
Unconsumed outputs are not serialized, recomputable `VINE_TEMP` outputs and
the worker-local DVM1 manifest are not fsynced, and requested outputs keep
their durability fsync. Historical tickets are not part of the production
contract. A focused executable
contract passes `retained=2 skipped=1 remote-fsync=0 durable-fsync=1`.
Repeated use of one callable object now fixes and reuses one cloudpickle
snapshot per Workflow: a 480-task profile performs one function dump and
builds in 24.4 ms instead of the prior approximately 114 ms. Public
`document()`/`delta_document()` still return isolated copies; synchronous
submit/append avoid a redundant private whole-graph copy.

The post-commit regression is 10/10 PASS, including the retained Go adaptor.
The final exact 10x16 n=5 comparison passed 20 backend runs:

- 32 MiB reuse: DataVine 0.609 s, TaskVine 0.496 s, rate 0.814x;
- wide multi-output: DataVine 0.679 s, TaskVine 0.407 s, rate 0.599x;
- wide DataVine wall time fell 28.8% from the preceding clean 0.954 s result.

This is a material improvement, not performance parity. The remaining wide
gap is now the 480-task one-fork execution plus strict requested-result
publication/fetch path, not Manager locking or per-output journal barriers.
The verified but unpromoted candidate is
`datavine.bottleneck-candidate-20260811.tar.gz`, SHA-256
`32e1361324df69be2db88258565f3ad393d2eb16ff9228e99387b895d9abba6d`.
Its packed 1x2x10k gate passed exactly at 3,382 Runtime tasks/s. This exact
archive was then promoted atomically; the active-path 1x2x10k smoke passed
10,000/10,000 at 3,332 Runtime tasks/s. The prior production archive remains
available as `datavine.output-heavy-candidate-20260811.tar.gz`, SHA-256
`9c1c8372c3cbc4baf43317257408213d07867938c6e0f767fb1009f018c8b9ca`.
Full evidence is in
`acceptance/bottleneck-optimization-20260811.json`.

The next data-intensive campaign is now designed in
`SC_SCIENTIFIC_WORKFLOW_BENCHMARK_PLAN.md`. It separates TV-native performance,
DV-native durability, and an optional TaskVine durable-sink diagnostic instead
of claiming false semantic equivalence. Controlled families cover partitioned
HEP scan/reduce, shared calibration reuse, genomics shuffle/merge, climate tile
iteration, adaptive parameter search, and ensemble checkpoint/reduce. The plan
defines cold/warm/peer/cache-pressure placement, theoretical byte lower bounds,
strong and weak scaling through paired reserved nodes, exact storage/recovery
gates, statistics, resource caps, and a staged real-application ladder. The
design is complete; generator, artifact schema, remote sampler and HEP-S driver
remain OPEN.

The complete source regression passed 10/10, including the focused output
retention/durability contract and the prebuilt Go adaptor.
This is a local performance pilot, not a replacement for the resident 10x16
campaign or a distributed parity claim. See
`acceptance/output-heavy-stage-20260811.json`.

### Historical post-Data-Controller rerun

The 2026-08-11 crossed-order rerun used two backend orders, five repetitions
per order, and resident 10x16 Condor pools that reached all 160 cores before
dispatch. All 40 backend-runs passed exact counts and result comparison.

- 32 MiB reuse: TaskVine 0.791 s, DataVine 2.542 s, DV/TV 0.311x.
- Wide multi-output: TaskVine 1.593 s, DataVine 8.783 s, DV/TV 0.181x.
- Exact completions per backend: 1,610 and 4,800 respectively.
- DataVine median publication wait fell to 0.195 s and 0.000046 s, but
  materialization rose to 0.475 s and 1.769 s. Direct per-output TaskVine file
  declaration/return and controller hashing are now the dominant path.

This is a correctness PASS and a performance regression, not a promotion
result. See `acceptance/data-controller-experiment-20260811.json`.

The 10-worker x 16-core HTCondor pilot is complete. Each backend used one
persistent Factory pool; the harness waited for exactly 10 connected workers
and at least 160 connected cores before warmup or measured dispatch. It then
kept the pool resident for all workflows and repetitions.

- Primary five-repetition campaign: PASS, 100 backend-runs, 25,840 exact
  physical task executions.
- Feature five-repetition campaign: PASS, 40 backend-runs, 12,810 exact
  physical task executions.
- Crossed-order three-repetition campaign: PASS, 60 backend-runs, 15,504 exact
  physical task executions.
- Total: 54,154 successful physical executions, zero logical/physical count
  mismatch, and identical requested-result hashes across backends.
- DataVine favors wide maps/DAGs by 2.68x to 4.78x in the primary campaign.
  Tiny serial chain, dynamic, and selective cases favor TaskVine.
- Large-output results are mixed: current 32 MiB reuse is 0.576x and wide
  multi-output is 0.424x. Wide result publication consumes a median 3.896 s of
  4.091 s native runtime.

The wide multi-output campaign exposed a real short-pipe-write bug in
`datavine_python_executor`. `send_frame()` now drains every frame byte, and the
execution regression forces seven-byte partial writes. All five fixed Condor
repetitions pass at exactly 480 submissions/completions per backend.

Final source checks passed the warning-clean build, module boundary, affected
native execution test, benchmark-driver syntax/help, and `git diff --check`.
The complete contract runner passed 8/9; Go was not run because this environment
has neither `DATAVINE_GO_BINARY`, `DATAVINE_GO_COMPILER`, nor a Go compiler.
This is an environment-blocked gate and is not reported as 9/9.

See `SC_WORKFLOW_COMPARISON_REPORT.md` for exact medians, methodology, threats,
raw artifact paths, reproduction commands, and remaining SC publication gates.
These opportunistic heterogeneous-cluster results are a pilot, not a final
causal hardware claim.

## Current checkpoint — scientific workflow foundation (2026-08-17)

The first staged scientific-workflow implementation slice is now locally
executable. It adds a strict v1 raw artifact schema, deterministic non-sparse
raw/cloudpickle shard generation and verification, Linux process/network/disk
sampling with known-byte calibration, and an HEP scan/calibrate/tree-reduce
driver with explicit `TV-native`, `DV-native`, and `TV-durable-sink` contracts.
The runtime and Data Controller implementation were not changed.

The repository test
`taskvine/test/TR_datavine_scientific_workflow_foundation.sh` passes deterministic
double generation, full shard hashes, allocation checks, deliberate corruption
failure, 2 MiB resource calibration, three backend contracts, 11/11 physical
task counts, exact cross-backend digest, and process/partial-file cleanup.

The retained acceptance pilot used eight 1 MiB shards, fan-in four, 64 bins,
one local Worker/core, and two repetitions per contract. All six runs passed
19/19 exact logical/submitted/completed counts and produced digest
`6513a1363c2490c06c54ca304e913c2c0640e5d1525bd16e9a124d5c3ccfcea5`.
The generator dataset identity is
`9707452643482f701b4e1b8b531405beca1e84d8c3d835fd5a198afe053e7a3e`.
The 8 MiB sampler calibration passed with process-write/payload 1.0 and
loopback RX/TX payload ratios 1.001353. Raw retained evidence is under
`acceptance/scientific-workflows/`.

Validation commands:

```sh
make -C taskvine/src/datavine -B -j8
make -C taskvine/src/tools datavine_workflow datavine_executor -B -j8
make -C taskvine/src/bindings/python3 -B -j8
PYTHONNOUSERSITE=1 bash taskvine/test/TR_datavine_scientific_workflow_foundation.sh run
PYTHONNOUSERSITE=1 DATAVINE_GO_BINARY=/users/jzhou24/graph_optimization/factories/datavine_workflow_go-20260811 bash acceptance/scripts/run_regression.sh
```

The first regression attempt passed 10/11 and failed only because neither the
Go binary variable nor a compiler was supplied. The retained Go binary matched
`acceptance/current-handoff.sha256`; the complete rerun then passed 11/11.
This local pilot is not performance evidence: per-worker producer/consumer
identity and byte deltas are still null, TaskVine Futures combine terminal wait
and fetch timing, and TaskVine shard decode is inside the kernel while DataVine
decode is an executor stage. End-to-end includes both decode paths, but useful
CPU is not directly comparable. CAL-small, 10x16 and reserved-node runs remain
OPEN.

## Current checkpoint — million-task data-intensive benchmark (2026-08-23)

Branch `benchmark/data-intensive-million-file` now contains the executable
workload contract, resumable 128-part dataset generator, DataVine runner,
TaskVine FunctionCall-fork runner, fail-closed paired comparator, tests, and
`DATAVINE_DATA_INTENSIVE_BENCHMARK.md`. The frozen acceptance contract is
1,048,576 tasks, 10,485,760 workflow payload files, 1.039871 TiB stored
artifacts, 3.696121 TiB logical data path, and exactly 128 workers x 16 cores.

The workload contract test passes exact task/file/data/edge/byte counts,
regular graph degree, random-pread kernel execution, deterministic output,
non-sparse generation, completed-file resume, partial-file resume, and
deliberate corruption rejection. Full-hash verification can run as 128 Condor
parts; final assembly validates each verification digest and its binding to
the current generator part manifest. Directly affected module-boundary,
Python-output-policy, workflow-execution, workflow-lifecycle, and data-plane-v2
tests also pass.

The first pilot exposed that `source-v1` retained outputs still returned
through the Manager. Indexed `source-v1` outputs now use an extended `DVP1`
ticket, `DVM1` manifest, worker-local temporary file, and direct requested
output persistence. The legacy 24-byte ticket remains for custom output names,
and executors with an environment override retain their non-fork path. In the
accepted 256-task 1x1 DataVine pilot, all 256 outputs were worker-local, only
32 requested C outputs were durable, and Manager task-output payload bytes
were zero.

Canonical paired pilot artifacts are:

- DataVine: `/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/pilot-c1-s1-ir3-20260823/datavine-local-1x1-final/summary.json`
- TaskVine: `/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/pilot-c1-s1-ir3-20260823/taskvine-local-1x1-sharedfs-taskcache-sealed-final/summary.json`
- Comparison: `/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/pilot-c1-s1-ir3-20260823/comparison-pilot-sharedfs-taskcache-sealed-final.json`

Both backends passed 256/256 exact physical counts with matching sampled C
SHA-256 values. The fair sealed SharedFS-source pilot reports TaskVine 27.01 s versus
DataVine 26.21 s (1.03x) and 76.38% fewer Manager data-plane bytes. TaskVine's
one-second sampler observed 15 central-window samples, all at the required
one active core; the full runner uses the same 90% active-core gate as
DataVine without retaining one record per task. Its scope is
explicitly `pilot`; it is not the full performance claim.

The first full TaskVine graph-load attempt exposed an unfair baseline setting:
with local-file declarations, TaskVine individually statted every
source and relayed source payload through the Manager while DataVine used
SharedFS. That pre-execution attempt was stopped and retained under
`campaign-diagnostic-manager-source-20260823`; no task had executed. The final
runner uses TaskVine's native canonical file-URL source transport so manager-byte and
execution differences isolate intermediate/result movement and GC behavior.
The next scale probe showed that admitting workers before a long static graph
load can starve Manager keepalives. The final runner now builds the sealed graph
before starting the factory and uses an unreachable `wait-for-workers=129`
gate during admission; scheduling opens only after exact 128x16 observation.
The first 20k-task scale execution also exposed that workflow-caching one-use
source URLs would exceed 4 GiB/worker. The accepted runner uses task-scoped
source URLs and workflow-scoped A/B temporaries, so source files are released
after use while intermediate movement and GC remain under test.

The full source dataset was generated under
`/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/full-v1-20260823`.
Condor cluster 15830 owns the original 128 parts and cluster 15831 owns three
resume jobs after evictions. Part manifests are atomic, and job arguments are
resume-safe. The generation gate is PASS at 128/128 manifests, 9,437,184
files, and 765,393,371,136 logical/allocated bytes with exact contract and
manifest digests. Cluster 15870 independently reread all 712.8 GiB and produced
128/128 bound, digest-checked, full-hash PASS proofs. The final dataset manifest
is PASS with file SHA-256
`3a391fea1ff1251015689e640e897c06aafc8e5a3a4a6aea6055ba8d541b5b71`;
compact evidence is in
`acceptance/data-intensive-large-scale-dataset-20260823.json`. The remaining
OPEN gate is five alternating exact 128x16 backend pairs and their production
comparison.

Separately, 19 superseded DataVine factory tarballs totaling about 15 GB moved
to `/project01/ndcms/jzhou24/datavine-benchmarks/factory-package-archive-20260823`.
Original paths are symlinks, so historical checksum references remain usable.
The active production package, its hard-linked production-v1 candidate, the
rollback package, the Go binary, and all non-DataVine packages were left in
place. `/users` utilization fell from approximately 99-100% to 76-80%.

## Current checkpoint — full-scale control-plane diagnostic (2026-08-24)

The first final-contract TaskVine run sealed all 1,048,576 tasks and admitted
exactly 128 Condor workers x 16 cores.  It exposed two independent scale
limits before it could become performance evidence.

During graph load, Python/TaskVine expanded 9,437,184 source declarations and
15,335,424 input bindings one object at a time.  At 1,007,504 loaded tasks the
Manager used 20,101,136 KiB RSS, had read zero source payload bytes, and had
already generated 6,290,751,488 bytes of filesystem writes through 3,270,731
write calls.  This is a control-plane graph/catalog bottleneck, not worker
payload execution.

After admission, the default `attempt-schedule-depth=100` caused lazy
FunctionCall library placement on exactly 100 workers.  All 128 workers stayed
connected with no failures or removals, but only 1,600/2,048 cores were active
and 28 workers remained idle while more than one million tasks were ready.
The run was intentionally stopped at 30,000 successful tasks because the
central-window gate requires at least 1,844 active cores and could not pass.
No `summary.json` or PASS result was produced.  The recoverable diagnostic is
under
`/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/full-v1-20260823/campaign-diagnostic-attempt-depth-100-20260824`,
and the compact evidence record is
`acceptance/data-intensive-large-scale-control-plane-diagnostic-20260824.json`.

The TaskVine baseline now sets scheduling depth to at least the requested
worker count so the first dispatch pass covers all 128 workers.  The campaign
and comparator also support an explicit one-pair full-scale scope without
mislabeling it as a five-pair production statistics result.  The user-requested
next run is one complete pair.  The proposed orders-of-magnitude control-plane
reduction—parametric source/task/dependency families, checked inverse mappings,
bounded lazy physical-task materialization, packed task state, and batched
bitmap journaling—is specified in `DATAVINE_PARAMETRIC_IR_PLAN.md` and remains
OPEN until its gates pass.

The narrow scheduling correction then passed a real 128x16 placement gate on
a 512-task / 5,120-file C2-S1 workload.  It completed 512/512 physical tasks,
lost or removed no worker, and produced 185 central-window samples with
minimum and maximum active cores both exactly 2,048 (required: 1,844).  The
summary SHA-256 is
`a9297d6ea2d3831829b54c90a8435f3980c63a93eb05858a6bee1dd028df1bb9` and its
compact repository artifact is
`acceptance/data-intensive-large-scale-placement-20260824.json`.
The placement cleanup exposed a separate lifecycle issue: the generic
30-second process-group grace period could SIGKILL `vine_factory` while it was
serially removing 128 Condor jobs.  Both full runners now give factory cleanup
five minutes; service processes retain the 30-second default.  The two tail
jobs from the diagnostic placement run were already marked for exact removal
by owner and factory `Iwd` and were not reused as evidence.

The next full attempt used the corrected 128-depth runner and sealed the entire
million-task graph.  It admitted exactly 128x16, then lost one worker during
the first 4,690 completed tasks.  The observed state was 127 connected workers,
2,032 active cores, one failed attempt, and one removed worker.  Because those
facts permanently violate the all-success, no-removal, and exact-pool gates,
the run was stopped rather than allowed to produce a misleading comparison.
All 128 Condor jobs were removed through the extended graceful factory path.
The recoverable run directory is
`/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/full-v1-20260823/campaign-diagnostic-worker-loss-20260824`
and the compact record is
`acceptance/data-intensive-large-scale-worker-loss-diagnostic-20260824.json`.

A repeated run localized those losses to the control-plane runtime-info path.
At roughly 44k completed tasks, NFS already held 1,937,468,956 debug bytes,
1,945,624,576 taskgraph bytes, 469,999,982 transaction bytes, and 1,628,679
performance bytes.  The two removals were immediately preceded by manager
`Failed to read from worker` records while it processed the same high-rate
cache-update/unlink stream.  This is not source/intermediate/result data and
therefore polluted the intended data-plane measurement.

TaskVine and DataVine full runners now direct underlying runtime-info to
node-local `/tmp`.  Dataset reads, DataVine journal durability, requested C
outputs, samples, summaries, and hashes remain on shared campaign storage.
Successful runs remove the temporary logs; failures record their local path.
The stopped shared-log run is recoverable under
`/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/full-v1-20260823/campaign-diagnostic-shared-run-info-20260824`
and compact evidence is in
`acceptance/data-intensive-large-scale-shared-run-info-diagnostic-20260824.json`.

## Current checkpoint — Condor churn semantics and per-task inode amplification (2026-08-24)

The first full attempt after runtime-info isolation completed the entire
explicit graph, admitted exactly 128 workers x 16 cores, and began execution.
Graph construction created exactly 1,048,576 separate 51-byte staging argument
files in addition to the 1.83 GB taskgraph stream; Manager RSS was about
20,535,708 KiB near execution.  This confirms that per-task filesystem objects,
not source payload reads, are another major graph-load/cleanup multiplier.

At 14:13:01, 14:13:43, and 14:16:48 UTC, manager `Failed to read from worker`
events matched HTCondor eviction events for clusters 16827, 16856, and 16781
to the second.  The largest evicted process used 469 MiB of a requested 2 GiB
and 245,998 KiB of a requested 4 GiB disk, and each job was rematched.  This
distinguishes site-scheduler churn from the earlier shared-NFS logging
interference and from task failure.  The operator stopped the run after the
runner reported 10,000 completions (the final manager sample contained 10,436
successful completions, three failed attempts, and zero exhausted attempts),
so it remains diagnostic and cannot support a performance claim.

The corrected acceptance contract no longer requires an opportunistic Condor
job to be never evicted over a multi-hour run.  It requires exact 128x16
admission and final recovery, explicit churn/failed-attempt counts, zero
exhausted attempts, exact logical and physical successful completions, and the
existing >=1,844-active-core central-window gate.  Both runners and the paired
comparator implement this rule.  The raw diagnostic is retained under
`/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/full-v1-20260823/campaign-diagnostic-condor-evictions-20260824`;
compact evidence is
`acceptance/data-intensive-large-scale-condor-churn-diagnostic-20260824.json`.
The replacement one-pair full-scale campaign remains OPEN.

## Current checkpoint — Controller-owned recovery and strict benchmark admission (2026-08-25)

Full-scale worker churn exposed four coupled recovery bugs that small clean
runs could not exercise. A generic peer transfer refusal was being reported as
proof that the source replica was lost; one stale generation could reject an
otherwise valid `DATA_READY_BATCH`; successful producer replays had no
single-instance state and could be queued again before their output metadata
arrived; and Runtime tested the Manager's legacy durable-result table for
availability even though Worker Agent intermediates exist only in the Data
Controller replica table.

Runtime recovery is now `NONE -> QUEUED -> RUNNING -> AWAIT_ADMISSION -> NONE`
per logical producer. Queue compaction and the per-task state prevent duplicate
concurrent replay. A successful recovery waits only in physical bookkeeping
for authoritative Controller admission; the Scheduler remains `DONE` and
children remain released. Admission timeout is bounded and measured, stale or
already-GC'd replica records are per-record idempotent no-ops, and a generic
peer timeout uses local 100-ms to 1.6-s backoff without revoking the source.
Only an authenticated exact `(DataID, worker, session, token)` source error, a
worker-session disconnect, or a local integrity/I/O failure can invalidate a
replica.

The double-disconnect gate under
`/tmp/datavine-fault-gate-c4-s8-run-r15-controller-availability-double-loss-20260825`
completed 8,192 logical tasks with 9,295 physical submissions and completions:
8,192 normal tasks + 1,096 recovery replays + 7 infrastructure retries. It
restored the 4-worker pool twice, recorded two recovery epochs, zero recovery
admission timeouts, zero non-infrastructure failures, 8,192 normal task reports
exactly once, and no repeated completion. The final post-change regression is
17/17 PASS.

The next full run revealed a measurement error rather than a Runtime failure.
The old DataVine runner started the factory, submitted the million-task family,
and only then waited for exact pool admission. Thus the r22 diagnostic began
executing on 111-115 workers and mixed early work into admission; it was
stopped and is not performance evidence. The corrected runner admits exactly
128 workers before submitting or sealing any workflow. Its execution timer and
throughput start immediately before sealed submission because Runtime can take
the workflow before the RPC response returns. The TaskVine baseline starts its execution
timer only after opening the same exact-pool scheduling gate. Both retain graph
load and admission as separate measurements, and admission uses the explicit
run timeout rather than a brittle one-hour constant. A 1x1 end-to-end gate
completed 256/256 with every acceptance gate after the ordering correction.
The matching 1x1 TaskVine gate also completed 256/256 with all gates true and
recorded graph load 0.190 s, admission 0.905 s, and admission-free execution
24.835 s as separate intervals. Ordinary `TR_vine_single` remains PASS.

The r23 exact-pool run under
`/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/datavine-native-parametric-r23-617a61836-20260825`
was the first run with strict ordering. It reached 116/128 real 16-core Workers
while its journal and service log remained empty and zero tasks executed. The
methodology audit then proved that a sealed workflow can become runnable before
its submit RPC response returns, so the old `RPC return -> terminal` interval
could omit native setup and early tasks. r23 was stopped before admission and
is diagnostic only. The corrected r24 timer starts before submission, includes
native setup, and uses the 24-hour admission timeout. r21-r23 and all earlier
interrupted artifacts remain excluded from the performance claim.

## Current checkpoint — bounded recovery closure ordering (2026-08-25)

The r24 execution then exposed a scale-only recovery ordering bug after a
Worker disconnect. One completed B producer was dispatched for replay before
one of its lost A inputs had entered the bounded recovery reserve. Because the
Controller still described that retired input as `DEAD`, Worker Agents rejected
the B replay before executor start and it exhausted infrastructure retries.
The first correction arms the entire transitive recovery closure in the Data
Controller before any physical replay: unavailable ancestors therefore resolve
as `PENDING`, while Scheduler state remains `DONE` and Manager remains unaware
of DataIDs.

That correction made a second bounded-window hazard visible in an injected
loss gate. Arbitrary Controller loss-callback order can interleave independently
lost consumers and ancestors; simple queue reversal is not a topological sort.
Waiting consumers could fill the replay reserve and exclude their producers.
The fixed parametric family already assigns all A TaskIDs before B and all B
before C, so Runtime now partitions each new recovery slice A -> B -> C in
O(n) time and constant auxiliary space, then arms the complete slice before
dispatch. No sort, edge expansion, Manager lookup, or Scheduler rollback is
required.

The shared-filesystem injection run at
`/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/recovery-closure-gate-c4s8-r4-1337a5f8f-20260825`
removed one 8-core Worker after 1,988 physical completions with mixed A/B data
resident. It terminated with exact conservation: 8,192 logical tasks + 488
replays + 8 disconnect retries = 8,688 submissions = 8,688 completions. All
488 replays published, recovery failures and admission timeouts were zero,
non-infrastructure failures were zero, all results and GC/count gates passed,
and the pool returned to four Workers. Its summary SHA-256 is
`98644ba3cb53ea04f6695a31d63a102304a5ce2e5a987c508a5d61d1e273b976`.
The generic runner status is `FAIL` only because the deliberately delayed
replacement reduced the central active-core fraction; this artifact is a
recovery-correctness gate, not performance evidence. Post-fix regression is
17/17 PASS. The exact 128x16 performance comparison remains OPEN pending the
terminal DataVine and TaskVine artifacts.

## Current checkpoint — bounded origin recovery under replacement churn (2026-08-25)

The r25 exact-pool run reached all 128 Workers and 2,048 active cores, but a
batch of site evictions exposed a Worker-local cache state bug. A replacement
Worker could leave a failed shared code-object transfer in `LOCAL_FETCHING` and
immediately return the task to Manager. At the diagnostic stop r25 had 704,941
physical submissions, 698,909 completions, 236 removed Workers, 1,409 recovery
epochs, 532,104 invalidated producers, 174,578 successful recovery reports,
and 492 recovery-admission timeouts. The completion rate had fallen from
hundreds per second to single digits while all 2,048 cores remained assigned.
This is replay-thrash diagnostic evidence and is excluded from performance
claims. Its recoverable path is
`/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/datavine-native-parametric-r25-185c0d8bb-20260825`.

Worker Data Agent now owns one bounded origin-fetch transition. A failed cache
record, failed transfer creation, or failed sandbox link removes that exact
local cache entry and retries on the same Worker at 100 ms through 1.6 s, for
at most eight retries. Only exhaustion returns the physical task to Manager
for placement elsewhere. This does not revoke a Controller replica, does not
change Scheduler state, and introduces no DataID knowledge into Manager.

The replacement-churn gate under
`/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/origin-retry-gate-c4s8-w8x8-a4183a448-20260825`
removed four of eight 8-core Workers after 1,772 physical completions. It
completed exact conservation: 8,192 logical tasks + 943 successful recovery
reports + 32 disconnect retries = 9,167 submissions = 9,167 completions. It
recorded zero recovery failure, zero admission timeout, zero
non-infrastructure failure, no duplicate normal task report, and restored the
complete 8-Worker pool. Summary SHA-256 is
`5ac151db62c71c327398ed06dff7a8cccf1b211b07cd172ef2106f1c914202e5`.
The generic status is `FAIL` only because the deliberate half-pool interval
failed the performance-only active-core sampling gate; every correctness,
data, GC, count, output, ready-frontier, and final-pool gate passed. Post-fix
regression is 17/17 PASS and ordinary `TR_vine_single` remains PASS. The r26
exact 128x16 terminal measurement is running; it remains OPEN until its signed
summary exists.

## Current checkpoint — r26 exhausted-origin poison diagnosis (2026-08-25)

r26 admitted exactly 128x16 and sustained 2,048 active cores, but failed closed
after 595,393 physical completions. It produced 491,427 normal task reports and
85,682 successful recovery reports while recovering 236,416 lost DataIDs
across 4,424 coalesced epochs. Twenty-six site Worker removals occurred before
terminal state. The PASS-only supervisor correctly did not start TaskVine.

The terminal failure was narrower than general recovery: 676 B producers
generated 18,102 recovery `FORSAKEN` results, and logical TaskID 310766 reached
63 such failures before the 64-attempt infrastructure bound failed closed.
After eight origin retries, Worker Agent failed the current placement but left
`fetch_failures` at its terminal value. Manager intentionally has no DataID
placement state, so later B tasks could repeatedly land on the same permanently
poisoned replacement Worker and fail immediately.

Commit `a018c2973` preserves bounded placement failure but resets the local
retry cycle and applies a 1.6-second cooldown. It changes no Controller replica
truth, Scheduler state, or Manager boundary. Post-fix regression is 17/17 PASS
and ordinary TaskVine smoke is PASS. The compact diagnosis is
`acceptance/data-intensive-parametric-origin-poison-diagnostic-20260825.json`.
r26 remains excluded from performance claims; r27 is required.

## Current checkpoint — ordinary explicit workflow recovery (2026-08-25)

The million-task workload is no longer used as the inner development loop.
An ordinary explicit-IR workflow with 8,192 logical tasks, 73,728 source files,
and eight 8-core Workers now exercises the same Worker Agent and Controller
data plane in about six minutes. A live Worker removal lost 428 DataIDs and
invalidated a 1,815-task transitive recovery closure. The run completed exact
conservation at 10,015 physical submissions and 10,015 completions, with eight
infrastructure `FORSAKEN` results, zero non-infrastructure failure, zero
recovery-admission timeout, and a restored 8-Worker pool.

The explicit path now follows the same strict data state boundary as the
parametric path. Runtime first moves every lost output from Controller `DEAD`
to `PENDING`, keeps Scheduler `DONE` immutable, orders the closure
producer-first, and submits it through a bounded high-priority replay lane.
Concurrent loss slices are deduplicated by a compact per-TaskID state and
rotated ahead of older waiting consumers. Ordinary Scheduler dispatch pauses
only while physical recovery or Controller admission is outstanding; logical
children are still released immediately by task completion. A recovery
`COMPLETED` event is journaled only after Controller admission, preventing a
retry or restart from binding an old generation.

The compact acceptance artifact is
`acceptance/data-intensive-explicit-ordinary-recovery-20260825.json`. The
generic benchmark summary says `FAIL` because it deliberately retains the
128x16, ten-million-file, IO-dominance, and full-scale-GC promotion gates.
Those gates are out of scope for this ordinary correctness workflow; its
workflow state and all applicable count, output, churn, and Manager-boundary
gates pass. Full-scale performance comparison remains OPEN.
