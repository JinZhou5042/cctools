#!/bin/bash
set -euo pipefail
ROOT=/users/jzhou24/cctools_repo/datavine
PAPER="$ROOT/papers"
cd "$ROOT"
export PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH
trial_cpus=$(python -c 'import os; print(",".join(map(str,sorted(os.sched_getaffinity(0))[:8])))')
exec taskset -c "$trial_cpus" python "$PAPER/scripts/run_upgrade_campaign.py" \
  --kind application --repetitions 1 --output "$PAPER/results/${1:-upgrade-current-application-smoke}" \
  --manifest "${2:-$PAPER/results/atlas-inputs-v1/manifest.json}"
