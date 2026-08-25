#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TEST_DIR="$ROOT/taskvine/test"
TIMEOUT_SECONDS=${DATAVINE_TEST_TIMEOUT:-180}
REPORT=${DATAVINE_REGRESSION_REPORT:-"${TMPDIR:-/tmp}/datavine-regression-latest.json"}
CONFIGURED_PYTHON=$(sed -n 's/^CCTOOLS_PYTHON_TEST_EXEC=//p' "$ROOT/config.mk")
REGRESSION_PYTHON=${DATAVINE_TEST_PYTHON:-$CONFIGURED_PYTHON}
PARAMETRIC_TEST="$ROOT/taskvine/src/tools/datavine_parametric_test"
BUILT_PARAMETRIC_TEST=0

cleanup_generated_test_tool()
{
    if [ "$BUILT_PARAMETRIC_TEST" -eq 1 ] && [ -e "$PARAMETRIC_TEST" ]; then
        unlink "$PARAMETRIC_TEST"
    fi
}
trap cleanup_generated_test_tool EXIT INT TERM

if [ -z "$REGRESSION_PYTHON" ] || [ ! -x "$REGRESSION_PYTHON" ]; then
    echo "DataVine regression Python is not executable: $REGRESSION_PYTHON" >&2
    echo "set DATAVINE_TEST_PYTHON or reconfigure config.mk" >&2
    exit 2
fi
if ! PYTHONNOUSERSITE=1 "$REGRESSION_PYTHON" -c \
        'import ndcctools.taskvine.datavine' >/dev/null 2>&1; then
    echo "DataVine Python bindings are unavailable in $REGRESSION_PYTHON" >&2
    echo "install the current tree or set DATAVINE_TEST_PYTHON" >&2
    exit 2
fi

# TR scripts consistently invoke `python`; put the configured DataVine Python
# first so the suite cannot silently inherit an unrelated active environment.
PATH=$(dirname "$REGRESSION_PYTHON"):$PATH
export PATH

if [ -n "${DATAVINE_GO_BINARY:-}" ]; then
    if [ ! -x "$DATAVINE_GO_BINARY" ]; then
        echo "DATAVINE_GO_BINARY is not executable: $DATAVINE_GO_BINARY" >&2
        exit 2
    fi
elif ! command -v "${DATAVINE_GO_COMPILER:-go}" >/dev/null 2>&1; then
    echo "set DATAVINE_GO_BINARY or DATAVINE_GO_COMPILER before regression" >&2
    exit 2
fi

# The regression runner invokes each TR script's run phase directly.  Build the
# one test-only executable that is not part of the normal TaskVine install so a
# clean checkout has the same behavior as an already-used developer tree.
if [ ! -x "$PARAMETRIC_TEST" ]; then
    make -C "$ROOT/taskvine/src/tools" datavine_parametric_test -j8
    BUILT_PARAMETRIC_TEST=1
fi

mkdir -p "$(dirname "$REPORT")"
export ROOT TEST_DIR TIMEOUT_SECONDS REPORT
"$REGRESSION_PYTHON" - <<'PY'
import json
import os
import pathlib
import signal
import subprocess
import tempfile
import time

test_dir = pathlib.Path(os.environ["TEST_DIR"])
timeout = int(os.environ["TIMEOUT_SECONDS"])
commit = subprocess.check_output(
    ["git", "-C", str(test_dir.parent.parent), "rev-parse", "HEAD"],
    text=True,
).strip()
results = []
with tempfile.TemporaryDirectory(prefix="datavine-regression-") as run_info:
    for script in sorted(test_dir.glob("TR_datavine_*.sh")):
        runtime_path = pathlib.Path(run_info) / script.stem
        runtime_path.mkdir()
        environment = dict(
            os.environ,
            DATAVINE_RUNTIME_INFO_PATH=str(runtime_path),
        )
        started = time.monotonic()
        proc = subprocess.Popen(
            ["bash", str(script), "run"],
            cwd=test_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
            env=environment,
        )
        try:
            output, _ = proc.communicate(timeout=timeout)
            returncode = proc.returncode
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                output, _ = proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                output, _ = proc.communicate()
            returncode = 124
        results.append({
            "test": script.name,
            "returncode": returncode,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "passed": returncode == 0,
        })
        if returncode:
            print(output, end="")

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
