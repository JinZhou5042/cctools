# DataVine current checkpoint

Updated: 2026-08-23

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
- TaskVine: `/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/pilot-c1-s1-ir3-20260823/taskvine-local-1x1-parallelism-final/summary.json`
- Comparison: `/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/pilot-c1-s1-ir3-20260823/comparison-pilot-parallelism-final.json`

Both backends passed 256/256 exact physical counts with matching sampled C
SHA-256 values. The paired pilot reports TaskVine 37.98 s versus DataVine
26.21 s (1.45x) and 97.00% fewer Manager data-plane bytes. TaskVine's
one-second sampler observed 21 central-window samples, all at the required
one active core; the full runner uses the same 90% active-core gate as
DataVine without retaining one record per task. Its scope is
explicitly `pilot`; it is not the full performance claim.

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
