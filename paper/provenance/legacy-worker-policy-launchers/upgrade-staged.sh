#!/bin/bash
set -euo pipefail
ROOT=/users/jzhou24/cctools_repo/datavine
PAPER="$ROOT/papers"
cd "$ROOT"
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
export PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH
export PYTHONPATH="$PAPER/.deps/research-python3.10:$PAPER/.deps/python3.10:$ROOT/test_support/python_modules/python3"
trial_cpus=$(python -c 'import os; print(",".join(map(str,sorted(os.sched_getaffinity(0))[:8])))')
exec taskset -c "$trial_cpus" python "$PAPER/scripts/stage_application.py" \
  --mode "${1:?mode required}" --output "$PAPER/results/${2:?version required}"
