#!/bin/bash
set -euo pipefail
ROOT=/users/jzhou24/cctools_repo/datavine
PAPER="$ROOT/papers"
cd "$ROOT"
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1
export PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH
export PYTHONPATH="$PAPER/.deps/python3.10:$ROOT/test_support/python_modules/python3"
task_cpu_list=$(python -c 'import os; print(",".join(map(str,sorted(os.sched_getaffinity(0))[:8])))')
taskset -c "$task_cpu_list" python "$PAPER/scripts/run_preloaded_campaign.py" \
 --output "$PAPER/results/preloaded-campaign-v1" --workloads spectral,histogram,quadrature,phase \
 --variants dv-fixed1,dv-elastic,tv-fixed1,tv-fixed4 \
 --workers 2 --cores 4 --width 32 --levels 4 --bytes 262144 \
 --repetitions 3 --timeout 360
