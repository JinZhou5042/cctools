#!/bin/bash
set -euo pipefail
ROOT=/users/jzhou24/cctools_repo/datavine
PAPER="$ROOT/paper"
cd "$ROOT"
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1
export PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH
export PYTHONPATH="$PAPER/.deps/python3.10:$ROOT/test_support/python_modules/python3"
python - <<'PY'
import json,os,platform
from pathlib import Path
p=Path('paper/results/compute-environment-v4.json')
p.write_text(json.dumps(dict(host=platform.node(),affinity=sorted(os.sched_getaffinity(0)),condor_ad=os.environ.get('_CONDOR_JOB_AD')),indent=2)+'\n')
PY
task_cpu_list=$(python -c 'import os; print(",".join(map(str,sorted(os.sched_getaffinity(0))[:8])))')
taskset -c "$task_cpu_list" python "$PAPER/scripts/run_campaign.py" --output "$PAPER/results/compute-campaign-v4" --workers 2 --cores 4 --width 32 --levels 4 --bytes 262144 --cpu-ms 100 --repetitions 3 --timeout 360
