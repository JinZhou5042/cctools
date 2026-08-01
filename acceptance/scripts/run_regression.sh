#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TEST_DIR="$ROOT/taskvine/test"
TIMEOUT_SECONDS=${DATAVINE_TEST_TIMEOUT:-180}
REPORT=${DATAVINE_REGRESSION_REPORT:-"$ROOT/acceptance/artifacts/regression-latest.json"}

mkdir -p "$(dirname "$REPORT")"
export ROOT TEST_DIR TIMEOUT_SECONDS REPORT
python - <<'PY'
import json
import os
import pathlib
import subprocess
import time

test_dir = pathlib.Path(os.environ["TEST_DIR"])
timeout = int(os.environ["TIMEOUT_SECONDS"])
commit = subprocess.check_output(
    ["git", "-C", str(test_dir.parent.parent), "rev-parse", "HEAD"],
    text=True,
).strip()
results = []
for script in sorted(test_dir.glob("TR_datavine_*.sh")):
    started = time.monotonic()
    proc = subprocess.run(
        ["timeout", str(timeout), "bash", str(script), "run"],
        cwd=test_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    results.append({
        "test": script.name,
        "returncode": proc.returncode,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "passed": proc.returncode == 0,
    })
    if proc.returncode:
        print(proc.stdout, end="")

report = {
    "artifact_type": "datavine-regression-suite",
    "commit": commit,
    "status": "PASS" if all(item["passed"] for item in results) else "FAIL",
    "test_count": len(results),
    "passed_count": sum(item["passed"] for item in results),
    "results": results,
}
path = pathlib.Path(os.environ["REPORT"])
path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
print(json.dumps({k: report[k] for k in ("status", "test_count", "passed_count")}))
if report["status"] != "PASS":
    raise SystemExit(1)
PY
