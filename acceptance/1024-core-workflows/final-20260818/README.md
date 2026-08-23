# Fixed 1024-core final artifact

Status: **PASS** (2026-08-18).

This directory is the compact, repository-retained form of the final
DataVine-versus-TaskVine campaign. Both backends used exactly 64 resident Condor
workers x 16 cores = 1024 cores. The campaign contains 49 cases, 98 unmeasured
warmups and 980 accepted backend runs (ten paired repetitions per case), with
zero invalidated runs and zero worker removals in accepted runs.

## Files

- `compact-summary.json`: gates, environment/source hashes, admissions, all
  case summaries, and compact per-run/warmup timings, counters and result
  digests.
- `results.csv`: 49-row median/rate/CI/attribution table.
- `report.md`: generated full table plus evidence for every significant
  DataVine regression.
- `regression.json`: final 12/12 repository regression result.
- `SHA256SUMS`: hashes for the retained deliverables and the immutable raw
  source artifact.

The immutable raw JSON remains at
`/tmp/datavine-taskvine-1024-final-v2-20260818/summary.json` on the campaign
host. It is 345,844,983 bytes with SHA-256
`80c2dedc46bab760d51d34915bdd1b305728ae2274be314e6562c84c2fbe17c7`.
Re-running the finalizer produced a byte-identical file.

## Headline result

The paired 95% intervals classify 12 cases as significantly faster for
DataVine, 24 as significantly slower and 13 as inconclusive. The strongest
DataVine regression is 128 x 32 MiB requested outputs (TV 8.171 s, DV 18.601
s, rate 0.429); 9.261 s of its 10.430 s gap is measured in result fetch. The
strongest stable DataVine advantage is high-degree graph execution (degree-64
rate 1.486; selected interactions 1.159--1.654). Tiny dynamic append remains a
fixed-control weakness (TV 0.131 s, DV 0.481 s).

## Reproduction

```sh
python3 acceptance/scripts/finalize_1024_workflow_campaign.py \
  /tmp/datavine-taskvine-1024-final-v2-20260818/summary.json \
  --output /tmp/finalized-summary.json

python3 acceptance/scripts/compact_1024_workflow_campaign.py \
  /tmp/finalized-summary.json \
  --output acceptance/1024-core-workflows/final-20260818/compact-summary.json

python3 acceptance/scripts/render_1024_workflow_report.py \
  acceptance/1024-core-workflows/final-20260818/compact-summary.json \
  --markdown acceptance/1024-core-workflows/final-20260818/report.md \
  --csv acceptance/1024-core-workflows/final-20260818/results.csv
```

The original measurement command was:

```sh
python3 acceptance/scripts/benchmark_1024_workflows.py \
  --output-dir /tmp/datavine-taskvine-1024-final-v2-20260818 \
  --repetitions 10
```
