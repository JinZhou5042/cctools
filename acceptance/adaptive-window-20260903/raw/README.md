# Retained raw evidence

Only final, decision-relevant artifacts are retained. Per-trial sandboxes,
journals, logs, failed prototypes, and superseded reruns were removed after the
accepted results were summarized.

- `final-oracle.json`: randomized CPU/I/O/mixed fixed-oracle comparison.
- `final-memory.json`: repeated 80% memory-bound admission test.
- `final-recall.json`: repeated rebalance and Worker-eviction recovery.
- `final-noop.json`, `final-short.json`, `final-disk.json`: focused fast-task and
  random-I/O comparisons.
- `condor-8x16-rpc-final.json`: final 8-Worker x 16-core remote scale point.
- `controller-rpc-local.json`, `controller-rpc-condor.json`: DVP3/DVP4 RPC data
  path validation.
- `controller-object-ingest-256b.json`,
  `controller-object-ingest-128k.json`: cold/deduplicated connection sweeps.
- `regression-final-v2.json`: final 21/21 DataVine regression.
- `generic-taskvine-serverless.stdout`: generic TaskVine compatibility gate.

