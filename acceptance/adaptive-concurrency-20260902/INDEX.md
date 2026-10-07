# Adaptive concurrency evidence

Status: **PASS_LOCAL**. This directory validates the implementation locally;
it is not a multi-node factory promotion.

- `report.md`: interpretation, protocol invariants and limitations.
- `summary.json`: compact accepted numbers.
- `adaptive_concurrency.png`: median A/B throughput ratios.
- `raw/results/results.json`: two randomized A/B repetitions across noop,
  5-ms, random disk I/O, 250-ms sleep I/O, CPU, mixed and late-Worker cases.
- `raw/results/cpu-rss.json`: three-repetition CPU control after replacing
  expensive PSS reads with conservative RSS accounting.
- `raw/results/memory.json`: declared-memory and RSS admission test.
- `raw/results/eviction.json`: late receiver eviction and recomputation test.
- `raw/results/native-10k-8x16.json`: independent 10,000-task native scale.
- `raw/results/native-10k-layered-8x16.json`: 10,000-task, 18,976-edge DAG.
- `raw/logs/`: benchmark streams and final regression output.
- `SHA256SUMS`: recursive integrity manifest.

Verify from this directory with `sha256sum -c SHA256SUMS`.
