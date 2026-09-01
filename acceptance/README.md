# DataVine acceptance

This directory contains executable gates and compact machine evidence. Generated
run trees, Worker sandboxes, debug logs and copied binaries do not belong here.

## Normal source gate

```sh
make -C taskvine/src/datavine -j8
make -C taskvine/src/tools \
  datavine_workflow datavine_executor \
  datavine_parametric_test datavine_replica_benchmark \
  datavine_controller_benchmark -j8
python -m py_compile acceptance/scripts/*.py taskvine/test/datavine_*.py
python taskvine/test/datavine_module_boundaries.py
bash acceptance/scripts/run_regression.sh
git diff --check
```

The regression requires the DataVine Python environment,
`PYTHONNOUSERSITE=1`, and either `DATAVINE_GO_BINARY` or
`DATAVINE_GO_COMPILER`.

Run focused tests directly as `bash taskvine/test/TR_NAME.sh run`. Use
`TR_datavine_scheduler.sh` for the production scheduling smoke.
`TR_datavine_persistence_faults.sh` builds its Linux-only preload shim in a
temporary directory and validates `ENOSPC` and `EIO`; it adds no production
fault hook or retained binary.

The production process accepts exactly one workflow ID. The old aggregate
suite still contains tests that submit several workflow IDs to one service;
those cases are historical until split into one process and journal per
workflow. Current singleton metadata evidence and gate status are in
`controller-slimming-20260827.json`.

## Evidence policy

- `matrix.md` contains only current gates.
- JSON artifacts must state exact counts, source identity and pass/fail status.
- Historical diagnostics do not promote a current gate.
- Compilation is not runtime acceptance.
- Scale results from different worker/core configurations are not ratios.
- Preserve raw evidence only when it supports a current decision; otherwise
  store it outside the repository.

Current storage decision evidence is `controller-local-tmp-20260827/`. Current
post-cleanup correctness, I/O, movement and throughput evidence is
`rigorous-validation-20260827/`. Run each checksum manifest from its containing
directory. Current requested-output stage attribution, `fsync` contract and
rejected persistence optimization screen are in
`controller-persistence-diagnostic-20260828.json`. The deterministic fail-closed
and restart gate is `controller-persistence-faults-20260828.json`. The isolated
remote inbound TCP ceiling is `controller-network-ceiling-20260828.json`.
The exact 10,000-file, 1-MiB remote persistence measurement is
`controller-10k-files-1m-20260828.json`.
The current fixed-topology DataVine versus TaskVine data-intensive pilot is
`data-intensive-fixed-ab-20260830.json`; it is a three-pair 2x4 result and must
not be presented as the full 128x16 acceptance contract.
The result-driven dynamic-workflow pilot is
`dynamic-adaptive-fixed-ab-20260830.json`. Its diagnosis is superseded by
`dynamic-control-root-fix-20260831.json`: tiny invocation control records now
use DVP2 task frames, and Controller-admitted results use one durable stream.
The dynamic result must not be combined with the static data-intensive speedup.
The production iData policy and its explicit volatile opt-out are recorded in
`default-idata-backup-20260831.json`. The route-isolated 1,000-file pilot and
two reverse-order 10,000-file Condor comparisons are in
`peer-vs-controller-20260831.json`; they establish local-first, peer-first,
Controller-fallback as the production source order.
The restart/late-consumer repair and focused dynamic validation are in
`dynamic-data-management-20260831.json`. This gate covers lazy durable-identity
hydration, zero producer replay, forced-peer dynamic append, post-seal GC and
an exact 477-task/445-edge adaptive run. It does not replace the larger remote
peer/controller throughput evidence.
