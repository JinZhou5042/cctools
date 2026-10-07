#!/bin/bash
set -euo pipefail
ROOT=/users/jzhou24/cctools_repo/datavine
cd "$ROOT"
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1
export PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH
task_cpu_list=$(python -c 'import os; print(",".join(map(str,sorted(os.sched_getaffinity(0))[:8])))')
for repetition in 1 2 3 4; do
 taskset -c "$task_cpu_list" python paper/scripts/diagnose_process_waits.py \
 --backend datavine --workload quadrature --width 32 --levels 4 --bytes 262144 \
 --workers 2 --cores 4 --policy fixed --window 1 --trace --timeout 120 \
 --output "paper/results/process-waits-r$repetition"
done
