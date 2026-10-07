# Dense storage-crossover index

This directory contains the 32-Worker x 16-core Controller `/tmp` versus
Worker-direct SharedFS size sweep. The accepted dense grid is 32--1024 KiB in
uniform 32-KiB steps. It supersedes the preliminary sparse crossover estimate
for conclusions about file-size routing; the sparse inputs remain retained as
provenance.

## Files

- `report.md`: methodology, results, theory, limitations and production
  decision.
- `summary.json`: preliminary sparse campaign and its historical model.
- `dense-summary.json`: validated machine summary of all dense campaigns.
- `small_file_crossover_dense_32x16.png`: every raw point, per-size medians,
  ranges and paired path ratios.
- `storage_crossover_phase_stability_32x16.png`: size-normalized throughput in
  execution order, exposing storage-state and external-load drift.
- `small_file_crossover_32x16.png`: preliminary sparse figure retained for
  provenance, not the primary conclusion.
- `raw/results/`: immutable measurement JSON.
- `raw/logs/`: factory, service and Condor diagnostics.
- `SHA256SUMS`: integrity manifest for all retained evidence.

## Reproduce derived artifacts

From this directory:

```sh
python3 ../scripts/summarize_storage_crossover_dense.py --root .
python3 ../scripts/render_storage_benchmarks.py \
  --matrix-root ../storage-matrix-20260901 --crossover-root .
sha256sum -c SHA256SUMS
```

The raw campaign commands and exact resource contract are in `report.md`.
SharedFS behavior depends on concurrent cluster load, and Controller results in
the streaming campaign include its real cumulative durable-file population.
Neither path is a synthetic isolated-device microbenchmark.
