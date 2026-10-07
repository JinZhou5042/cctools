#!/bin/bash
set -euo pipefail
ROOT=/users/jzhou24/cctools_repo/datavine
PAPER="$ROOT/papers"
export PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH
exec python "$PAPER/scripts/stage_oracle_on_host.py" --output "$PAPER/results/upgrade-intel-oracle-v2.json"
