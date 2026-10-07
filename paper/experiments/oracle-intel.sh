#!/bin/bash
set -euo pipefail
ROOT=/users/jzhou24/cctools_repo/datavine
PAPER="$ROOT/papers"
export PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH
export PYTHONPATH="$PAPER/.deps/research-python3.10:$PAPER/.deps/python3.10"
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
exec python "$PAPER/scripts/oracle_on_host.py" --output "$PAPER/results/upgrade-intel-oracle.json"
