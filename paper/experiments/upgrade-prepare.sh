#!/bin/bash
set -euo pipefail
ROOT=/users/jzhou24/cctools_repo/datavine
PAPER="$ROOT/papers"
cd "$ROOT"
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
export PYTHONPATH="$PAPER/.deps/research-python3.10:$PAPER/.deps/python3.10:$ROOT/test_support/python_modules/python3"
exec /groups/dthain/users/jzhou24/miniconda/envs/datavine/bin/python "$PAPER/scripts/prepare_atlas.py" \
  --source /groups/dthain/users/jzhou24/atlas-agent/notebooks-collection-opendata/13-TeV-examples/uproot_python/taskvine_executor \
  --output "$PAPER/results/atlas-inputs-v1"
