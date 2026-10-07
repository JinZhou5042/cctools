#!/bin/bash
set -euo pipefail
ROOT=/users/jzhou24/cctools_repo/datavine
PAPER="$ROOT/papers"
cd "$ROOT"
export PYTHONPATH="$PAPER/.deps/research-python3.10:$PAPER/.deps/python3.10:$ROOT/test_support/python_modules/python3"
export PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
exec python "$PAPER/scripts/diagnose_executor_paths.py" --backend datavine --variant diagnostic \
  --workload noop --width 8 --levels 8 --bytes 1024 --workers 2 --cores 4 --timeout 300 \
  --output "$PAPER/results/upgrade-path-diagnostic-v1"
