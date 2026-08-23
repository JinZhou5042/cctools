# Current decoupled-data-plane fixed 1024-core result

Status: **PASS** (2026-08-21).

This is the canonical DataVine-versus-native-TaskVine campaign for the
worker-local/peer-first, lazy-SharedFS-persistence architecture. Each backend
used an exact resident pool of 64 workers x 16 cores = 1024 cores. The campaign
contains 49 workflow cases, 98 accepted warmup runs and 980 accepted measured
runs (10 paired repetitions). All result-digest, physical-task-count,
admission, accepted-run churn and regression-attribution gates pass.

Paired bootstrap 95% intervals classify 46 cases as DataVine-faster, 2 as
DataVine-slower and 1 as inconclusive. Representative medians:

- 4096 immediate tasks: 208.9 DataVine tasks/s versus 111.5 TaskVine tasks/s;
- fan-in 16: 232.8 versus 119.4 tasks/s;
- 4096 tasks at fixed 10 s CPU: 71.6 versus 62.3 tasks/s;
- 128 x 32 MiB requested outputs: DataVine 11.224 s versus TaskVine 13.008 s.

The two statistically confirmed regressions are fully attributed. A 32 MiB
broadcast is slower by 0.109198 s because separated result fetch adds 0.121335
s; the interface-level fix is parallel/streaming multi-DataID reads. Eight-step
serial dynamic append is slower by 0.821531 s because it causes nine Runtime
invocations; the fix is a resident WorkflowSession runtime lane.

`compact-summary.json` retains every accepted and invalidated measurement,
profiles, resource gates and source provenance without large result values.
`report.md` is the generated human-readable table and attribution report;
`results.csv` is the flat 49-case table. `regression.json` records the final
13/13 functional regression pass from this exact source state.
`object-ingest-profile.json` records
the fresh one-file-per-object SharedFS microbenchmark (752.9 cold objects/s and
3162.2 warm deduplicated objects/s with 16 writers for 4096 x 256-byte objects).

The raw campaign, service/factory logs and per-attempt records are retained at
`/project01/ndcms/jzhou24/datavine-benchmarks/final-screen-v9-1024-20260820/`.
