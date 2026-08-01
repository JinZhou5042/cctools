#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TASKS=${DATAVINE_GRAND_TASKS:-100}
MEDIUM_BYTES=${DATAVINE_GRAND_MEDIUM_BYTES:-65536}
LARGE_BYTES=${DATAVINE_GRAND_LARGE_BYTES:-0}
FAILURE_ARGS=()
if [[ "${DATAVINE_GRAND_WORKER_LOSS:-0}" == "1" ]]; then
  FAILURE_ARGS+=(--worker-loss)
fi
cd "$ROOT/taskvine/test"
exec python "$ROOT/acceptance/scripts/datavine_grand_challenge.py" \
  --tasks "$TASKS" --medium-bytes "$MEDIUM_BYTES" \
  --large-bytes "$LARGE_BYTES" --json "${FAILURE_ARGS[@]}"
