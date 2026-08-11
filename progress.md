# DataVine current checkpoint

Updated: 2026-08-11

## Outcome

The supported single-service, language-neutral dynamic-workflow architecture
is internally consistent and passes its current source gates. C is the sole
runtime authority; Python is an optional builder/notebook adaptor and managed
fork executor; Shell and Go use the same IR/RPC boundary.

Each logical task owns an independent TaskVine task, lease, resource request,
retry, and completion. Scale gates now fail unless the native runtime reports
exactly one physical submission and completion for every logical task.

The result plane is now separate from the control plane. Python callables write
cloudpickle once while streaming SHA-256 and return only a compact DVM1
manifest. Non-requested intermediates are TaskVine temporary files: they stay
in worker cache and move peer-to-peer. Requested public values alone use
Data-Controller-owned output paths and durable metadata. The four-thread Data
Controller validates and commits them; Runtime never reads result payloads or
parses result metadata. The Data Controller reads only the compact DVM1
size/hash manifest from TaskVine stdout. The workflow journal receives no new
result payloads; its old result records are replay-only compatibility.

## Cleanup and semantic fixes

- Deleted the grouped-noop executor path, physical groups, projected results,
  projection metrics, and its executor operation.
- Removed DVP2/base64/stdout result transport. Callable and source executors
  write direct output files; ordinary TaskVine transfer or SharedFS moves those
  files without routing payload bytes through Runtime.
- Replaced manager-side persistence of every retained callable output with a
  hybrid native path: `VINE_TEMP` plus peer transfer for intermediates, ordinary
  TaskVine output transfer only for requested values. No new worker daemon or
  data protocol was added.
- Added fail-closed restart semantics: completed producers are recovered only
  when their retained outputs are still active or durable; lost worker-local
  values cause producer recomputation.
- Reduced DVP3 tickets from 64 to 56 fixed bytes by deleting the redundant
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
post-commit contract artifact records that commit directly; compact evidence
and handoff hashes are retained in the following documentation-only commit.

The worker-local Data Controller redesign passes warning-clean builds, module
boundaries, and its expanded execution contract: live result fetch before
workflow termination, restart fetch, selective pruning, multi-output atomicity,
2 MiB payload exclusion from the workflow journal, corruption rejection, and
forced service restart with recomputation of a non-durable intermediate.
The full runner is 8/9: every executable contract passes, including notebook
cross-workflow concurrency after failed-output cleanup was fixed; Go alone is
environment-blocked because no Go compiler/binary exists. Fresh strict scale
runs pass at exactly 100k and 1M physical submissions/completions.

A post-redesign candidate package was rebuilt from the active conda
environment and verified with `poncho_package_run`: cloudpickle is 3.1.2 and
the package-local service, native executor, Python executor, and worker hashes
match this build. Packed 1x2x10k passed exactly at 3,403 runtime tasks/s and
74.7 MB peak RSS. Candidate SHA-256 is `d9427cec...115821d`; it is retained as
`datavine.data-controller-candidate-20260811.tar.gz`. The active production
package was deliberately not overwritten while the Go contract is
environment-blocked.

The final worker-local candidate is
`datavine.worker-local-candidate-20260811.tar.gz`, SHA-256
`67c202c328b38f59d5da68a68ade5a05abdc076bdb1b57f49f15ad00a5b6f0bf`.
`poncho_package_run` confirms cloudpickle 3.1.2 and exact hashes for the
service, native executor, Python executor, and worker. Its packed 1x2x10k gate
passes exactly at 3,365 Runtime tasks/s and 75.3 MB peak RSS. Production remains
unchanged pending the Go environment gate.

| Gate | Result |
|---|---|
| Warning-clean C build | PASS |
| Python static/module boundary | PASS |
| Supported executable contracts | PASS, 8/8; Go environment-blocked |
| Exact 100k independent tasks | PASS, 28.63 s E2E, 4,303 runtime tasks/s, 447 MB RSS |
| Exact 1M independent tasks | PASS, 295.64 s E2E, 4,122 runtime tasks/s, 4.04 GB RSS, 44 FDs, 5 processes |
| CPU fork, 1 core | PASS, DV/FC 1.003, 97.56% useful CPU |

The no-grouping guard was revalidated after the cleanup with a live 1x1 smoke:
1,000 logical tasks produced exactly 1,000 TaskVine submissions and 1,000
TaskVine completions. The scale driver now records these counts and fails on
any mismatch. The post-change regression run passed 8/9 contracts; the Go
adaptor contract was not run because neither `DATAVINE_GO_BINARY` nor a Go
compiler is installed in the active environment.
| CPU fork, 4 cores | PASS, DV/FC 1.003, 97.07% useful CPU |
| CPU fork, 16 cores | PASS, DV/FC 0.966, 92.01% useful CPU |
| Package rebuild and packed E2E | PASS, 10,000/10,000; 3,365 Runtime tasks/s, 2,786 tasks/s end-to-end |

Artifacts:

- `acceptance/native-data-controller-regression.json`
- `acceptance/native-data-controller-1x1x100k.json`
- `acceptance/native-data-controller-1x1x1m.json`
- `acceptance/native-data-controller-packed-1x2x10k.json`
- `acceptance/native-worker-local-final-regression.json`
- `acceptance/native-worker-local-hybrid-1x1x100k.json`
- `acceptance/native-worker-local-hybrid-1x1x1m.json`
- `acceptance/native-worker-local-packed-1x2x10k.json`
- `acceptance/worker-local-data-plane-20260811.json`
- `acceptance/sc-workflow-worker-local-final-20260811-10x16-n5/summary.json`
- `acceptance/native-regression-deep-review.json`
- `acceptance/native-workflow-one-lease-1x1x100k.json`
- `acceptance/native-workflow-one-lease-1x1x1m.json`
- `acceptance/cpu-fork-deep-review-1x{1,4,16}.json`
- `acceptance/native-workflow-deep-review-packed-1x2x10k.json`

Promoted package:

- active: `/users/jzhou24/graph_optimization/factories/datavine.tar.gz`
- dated hard link: `datavine.deep-review-20260810.tar.gz`
- SHA-256: `8933446d6583ada6f8d23033012eb618f700b7b1a25b102a93c13661ca19c9e9`

`poncho_package_run` imported cloudpickle 3.1.2 and the DataVine API;
package-local hashes matched the installed `datavine_workflow`,
`datavine_executor`, `datavine_python_executor`, and `vine_worker`. Packed
1x2x10k completed exactly with 75.3 MB peak RSS, 43 FDs, and six processes.

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
