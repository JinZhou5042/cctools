#!/bin/bash
set -euo pipefail
ROOT=/users/jzhou24/cctools_repo/datavine
PAPER="$ROOT/paper"
cd "$ROOT"
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1
export PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH
export PYTHONPATH="$ROOT/test_support/python_modules/python3"
task_cpu_list=$(python -c 'import os; print(",".join(map(str,sorted(os.sched_getaffinity(0))[:2])))')
taskset -c "$task_cpu_list" python acceptance/scripts/benchmark_adaptive_concurrency.py \
 --cases memory,eviction --tasks 32 --cores 2 --worker-memory 2048 \
 --duration .5 --rebalance-tasks 16 --rebalance-duration 2 --late-worker-delay .2 \
 --multipliers 8 \
 --repetitions 3 --cpu-affinity "$task_cpu_list" --timeout 180 \
 --output "$PAPER/results/pressure-elastic.json" --artifacts "$PAPER/results/pressure-elastic"
