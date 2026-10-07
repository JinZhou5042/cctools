# Storage-path matrix index

Status: 18/18 measured rounds PASS at 32 Workers x 16 cores.

This campaign separates empty-file lifecycle throughput from 32-GiB bulk
movement for Controller `/tmp`, unrestricted Worker-direct SharedFS and pure
peer transfer. Read `report.md` for method and interpretation, or
`summary.json` for compact machine-readable values.

## Files

- `report.md`: accepted methodology, results, limitations and decision.
- `summary.json`: compact accepted values and exact-count gates.
- `storage_matrix_32x16.png`: three-path visual summary.
- `raw/results/`: immutable per-run JSON measurements.
- `raw/logs/`: factory, service and diagnostic Condor logs supporting those
  measurements.
- `SHA256SUMS`: integrity manifest for every retained artifact above.

## Reproduce and verify

The individual harnesses are `../scripts/benchmark_native_workflow.py`,
`../scripts/benchmark_sharedfs_direct.py` and
`../scripts/benchmark_peer_vs_controller.py`. Plot both storage campaigns with:

```sh
python3 ../scripts/render_storage_benchmarks.py \
  --matrix-root . --crossover-root ../storage-crossover-20260901
sha256sum -c SHA256SUMS
```

Absolute rates apply only to the recorded Worker layout, executor, durability
contract and timer boundary. They must not be combined with earlier route
pilots as if they were matched repetitions.
