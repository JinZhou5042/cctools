#!/bin/bash
set -euo pipefail
ROOT=/users/jzhou24/cctools_repo/datavine
PAPER="$ROOT/papers"
cd "$ROOT"
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
export PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH
export PYTHONPATH="$PAPER/.deps/research-python3.10:$PAPER/.deps/python3.10:$ROOT/test_support/python_modules/python3"
trial_cpus=$(python -c 'import os; print(",".join(map(str,sorted(os.sched_getaffinity(0))[:8])))')
mode="${1:?campaign mode required}"
destination="${2:?versioned result directory required}"
if [[ "$mode" == atlas-smoke ]]; then
  for backend in taskvine datavine; do
    taskset -c "$trial_cpus" python "$PAPER/scripts/research_trial.py" \
      --backend "$backend" --variant "$backend" --workload atlas \
      --manifest "$PAPER/results/atlas-inputs-v1/manifest.json" \
      --width 1 --bytes 20000 --workers 2 --cores 4 --timeout 180 \
      --output "$PAPER/results/$destination-$backend"
  done
elif [[ "$mode" == application ]]; then
  taskset -c "$trial_cpus" python "$PAPER/scripts/research_campaign.py" \
    --mode application --variants dv-fixed,dv-elastic,tv-stock,tv-group \
    --manifest "$PAPER/results/atlas-inputs-v1/manifest.json" \
    --timeout 900 --output "$PAPER/results/$destination"
elif [[ "$mode" == attribution ]]; then
  taskset -c "$trial_cpus" python "$PAPER/scripts/research_campaign.py" \
    --mode attribution --variants dv-fixed,dv-profile,tv-stock,tv-profile,dv-cold,tv-cold \
    --output "$PAPER/results/$destination"
else
  taskset -c "$trial_cpus" python "$PAPER/scripts/research_campaign.py" \
    --mode controls --output "$PAPER/results/$destination"
fi
