#!/bin/bash
set -euo pipefail
ROOT=/users/jzhou24/cctools_repo/datavine
PAPER="$ROOT/paper"
cd "$ROOT"
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1
export PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH
export PYTHONPATH="$PAPER/.deps/python3.10:$ROOT/test_support/python_modules/python3"
task_cpu_list=$(python -c 'import os; print(",".join(map(str,sorted(os.sched_getaffinity(0))[:8])))')
# The kernel's "noop" branch omits extra numerical work, but still hashes every
# parent payload and generates every output with SHAKE256. This is a hash-data
# pipeline, NOT a zero-work/native no-op benchmark.
taskset -c "$task_cpu_list" python "$PAPER/scripts/run_campaign.py" \
 --output "$PAPER/results/data-pipeline-v1" --workloads noop \
 --variants dv-elastic,tv-fixed1,tv-fixed4 \
 --workers 2 --cores 4 --width 128 --levels 8 --bytes 1048576 \
 --repetitions 3 --timeout 600
