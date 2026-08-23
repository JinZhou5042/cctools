# Current-code 1024-core campaign

This is the canonical DVP6 DataVine-versus-TaskVine synthetic workflow
campaign for 2026-08-18.

The runner admitted 64 workers with 16 cores each for each backend, then ran
49 cases with one paired warmup and ten paired measured repetitions. All 98
accepted warmup backend runs and all 980 accepted measured backend runs passed
exact task-count, result-hash, resource-inventory, and no-worker-churn gates.
Six warmup and six measured backend records affected by worker churn were
invalidated and excluded before automatic retry.

The first long-lived TaskVine pool stopped making progress in centralized
`RETRIEVE` state during an excluded warmup. The checkpoint was preserved and
the campaign resumed with fresh exact pools; no partial attempt entered the
statistics. This lifecycle event is evidence, not a measured performance
sample.

Reproduction command:

```bash
PYTHONNOUSERSITE=1 python3 acceptance/scripts/benchmark_1024_workflows.py \
  --output-dir /tmp/datavine-data-plane-v2-current-final-20260818 \
  --repetitions 10
```

The durable `compact-summary.json` contains accepted per-run counters,
profiles, result digests, the manifest, gates, and final paired statistics.
`report.md` is the readable attribution report and `results.csv` is the
49-case table. A ratio above one favors DataVine.

Final classification: 5 DataVine-faster, 23 DataVine-slower, and 21
inconclusive at paired 95% confidence. The main demonstrated scaling advantage
is the zero-CPU degree-64 interaction family. The main remaining limitations
are post-completion fetch for 32 MiB requested outputs and fixed control cost
for tiny dynamic workflows.
