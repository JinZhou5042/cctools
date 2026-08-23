#!/bin/sh

set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
PYTHONNOUSERSITE=1 python "$repo/taskvine/test/datavine_1024_benchmark_contract.py"
