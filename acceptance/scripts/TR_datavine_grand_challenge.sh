#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TASKS=${DATAVINE_GRAND_TASKS:-100}
cd "$ROOT/taskvine/test"
exec python "$ROOT/acceptance/scripts/datavine_grand_challenge.py" \
  --tasks "$TASKS" --json
