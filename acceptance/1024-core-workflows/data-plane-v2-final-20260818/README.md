# Data plane v2 pre-fetch-optimization campaign

This directory is the complete compact form of the first DVP6 fixed-core
campaign. It is retained as the before baseline for the final result-fetch and
Controller-concurrency changes.

- exact resources: 64 workers x 16 cores = 1024 cores per backend
- matrix: 49 cases, 98 accepted warmup runs, 980 accepted measured runs
- statistics: 10 paired repetitions and paired bootstrap 95% intervals
- correctness: exact results, task counts, useful-CPU gates, and zero worker
  churn in accepted runs
- status: PASS, with every statistically established regression attributed

The measured code already used callable-v2/DVP6 direct executor object pulls,
but fetched requested results sequentially and used four Controller/RPC
workers. The subsequent exact 1024-core confirmations are in
`acceptance/data-plane-v2/fetch-confirmation-1024.json` and
`fetch-confirmation-1024-threads8.json`. A second complete campaign measures
the final implementation.

Reproduction:

```sh
python3 acceptance/scripts/benchmark_1024_workflows.py \
  --output-dir /tmp/datavine-data-plane-v2-final-20260818 \
  --repetitions 10
```
