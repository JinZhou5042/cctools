#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH
export PATH=$(dirname "${CCTOOLS_PYTHON_TEST_EXEC}"):$PATH

STATUS_FILE=vine_graph_phase0_baseline.status
PORT_FILE=vine_graph_phase0_baseline.port
RESULT_FILE=vine_graph_phase0_baseline.result.json
WORK_ROOT=vine_graph_phase0_baseline.work
INDEXED_DATA_IDENTITY=${DATAVINE_INDEXED_DATA_IDENTITY:-0}
SHADOW_DATA_GRAPH=${DATAVINE_SHADOW_DATA_GRAPH:-0}
DATA_CONTROLLER=${DATAVINE_DATA_CONTROLLER:-0}
WORKER_DATA_AGENT=${DATAVINE_WORKER_DATA_AGENT:-0}

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
	"${CCTOOLS_PYTHON_TEST_EXEC}" -c "import cloudpickle" || return 1
	return 0
}

prepare()
{
	rm -f "$STATUS_FILE" "$PORT_FILE" "$RESULT_FILE" worker.phase0.1.log worker.phase0.2.log
	rm -rf "$WORK_ROOT"
	return 0
}

run()
{
	(
		DATAVINE_GIT_COMMIT=$(git rev-parse HEAD) \
		"${CCTOOLS_PYTHON_TEST_EXEC}" vine_graph_phase0_baseline.py \
				--port-file "$PORT_FILE" \
				--work-root "$WORK_ROOT" \
				--result-file "$RESULT_FILE" \
				--indexed-data-identity "$INDEXED_DATA_IDENTITY" \
				--shadow-data-graph "$SHADOW_DATA_GRAPH" \
				--data-controller "$DATA_CONTROLLER" \
				--worker-data-agent "$WORKER_DATA_AGENT"
		echo $? > "$STATUS_FILE"
	) &

	wait_for_file_creation "$PORT_FILE" 15

	cores=2
	memory=3000
	disk=3000
	run_taskvine_worker "$PORT_FILE" worker.phase0.1.log
	run_taskvine_worker "$PORT_FILE" worker.phase0.2.log

	wait_for_file_creation "$STATUS_FILE" 120
	test "$(cat "$STATUS_FILE")" -eq 0
	test -s "$RESULT_FILE"
	"${CCTOOLS_PYTHON_TEST_EXEC}" -c \
		'import json,sys; report=json.load(open(sys.argv[1])); assert report["acceptance"] == "PASS"; assert report["indexed_data_identity"] == int(sys.argv[2]); assert report["shadow_data_graph"] == int(sys.argv[3]); assert report["data_controller"] == int(sys.argv[4]); assert report["worker_data_agent"] == int(sys.argv[5]); assert report["cases"]["worker-loss"]["manager_stats_delta"]["tasks_recovery"] >= 1' \
		"$RESULT_FILE" "$INDEXED_DATA_IDENTITY" "$SHADOW_DATA_GRAPH" "$DATA_CONTROLLER" "$WORKER_DATA_AGENT"
	exit 0
}

clean()
{
	rm -f "$STATUS_FILE" "$PORT_FILE" "$RESULT_FILE" worker.phase0.1.log worker.phase0.2.log
	rm -rf "$WORK_ROOT"
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
