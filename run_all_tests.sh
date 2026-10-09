#!/bin/sh

if [ X$1 != X ]
then
	CCTOOLS_PACKAGES_TEST=$1
fi

if [ ! -r config.mk ]; then
	echo "Please run ./configure && make before executing the test script"
	exit 1
fi

if [ -z "$CCTOOLS_PACKAGES_TEST" ]
then
	CCTOOLS_PACKAGES_TEST=$(grep CCTOOLS_PACKAGES config.mk | cut -d = -f 2)
	if [ -n "${CCTOOLS_DOCKER_GITHUB}" ]
	then
		if ! parrot/src/parrot_run /bin/ls > /dev/null 2>&1
		then
			echo "Skipping parrot tests inside docker build."
			export PARROT_SKIP_TEST=yes
		fi
	fi
fi

if [ -z "$CCTOOLS_TEST_LOG" ]; then
	CCTOOLS_TEST_LOG="./cctools.test.log"
fi

#absolute path?
if [ -z "$(echo $CCTOOLS_TEST_LOG | sed -n 's:^/.*$:x:p')" ]; then
	CCTOOLS_TEST_LOG="$(pwd)/${CCTOOLS_TEST_LOG}"
fi
export CCTOOLS_TEST_LOG
export CCTOOLS_TEST_FAIL=${CCTOOLS_TEST_LOG%.log}.fail
export CCTOOLS_TEST_TMP=${CCTOOLS_TEST_LOG%.log}.tmp

echo "[$(date)] Testing on $(uname -a)." > "$CCTOOLS_TEST_LOG"
rm -f "${CCTOOLS_TEST_FAIL}"

# we need resource_monitor in the path.
PATH="$(pwd)/resource_monitor/src:$PATH"
export PATH

SUCCESS=0
FAILURE=0
SKIP=0
START_TIME=$(date +%s)
test_root=$(pwd)
test_directories=""
for package in ${CCTOOLS_PACKAGES_TEST}; do
	test_directories="$test_directories ${package}/test"
	if [ "$package" = taskvine ]; then
		test_directories="$test_directories taskvine/src/vine_graph/test"
	fi
done
for test_directory in ${test_directories}; do
	if [ -d "$test_directory" ]; then
		cd "$test_root/$test_directory"
		scripts="TR_*"
		if [ "$test_directory" = taskvine/src/vine_graph/test ]; then
			scripts="vine_graph_data.py vine_graph_edata.py vine_graph_recovery.py vine_graph_dask_adaptor.py"
			graph_python=$(sed -n 's/^CCTOOLS_PYTHON_TEST_EXEC=//p' "$test_root/config.mk")
			graph_python_dir=$(sed -n 's/^CCTOOLS_PYTHON_TEST_DIR=//p' "$test_root/config.mk")
		fi
		for script in $scripts; do
			if [ -x "$script" ] || [ "${script##*.}" = py ]; then
				printf "%-66s" "--- Testing ${test_directory}/${script} ... "
				TEST_START_TIME=$(date +%s)
				(
					if [ "${script##*.}" = py ]; then
						[ -n "$graph_python" ] || exit 1
						"$graph_python" -c 'import cloudpickle' || exit 1
						if [ "$script" = vine_graph_dask_adaptor.py ]; then
							"$graph_python" -c 'import dask' || exit 1
						fi
					else
						"./${script}" check_needed
					fi
				) >> "$CCTOOLS_TEST_LOG" 2>&1
				result=$?
				if [ "$result" -ne 0 ]; then
					skip=1
				else
					skip=0
					(
						if [ "${script##*.}" = py ]; then
							export PYTHONPATH="$test_root/test_support/python_modules/$graph_python_dir:${PYTHONPATH:-}"
							export PATH="$(dirname "$graph_python"):$PATH"
							exec "$graph_python" "$script"
						fi
						echo "======== ${script} PREPARE ========"
						"./${script}" prepare
						result=$?
						if [ $result = 0 ]; then
							echo "======== ${script} RUN ========"
							"./${script}" run
							result=$?
						fi
						echo "======== ${script} CLEAN ========"
						"./${script}" clean
						exit $result
					) > "$CCTOOLS_TEST_TMP" 2>&1
					result=$?
					cat "$CCTOOLS_TEST_TMP" >> "$CCTOOLS_TEST_LOG"
					if [ "$result" -ne 0 ]; then
						cat "$CCTOOLS_TEST_TMP" >> "$CCTOOLS_TEST_FAIL"
					fi
				fi
				TEST_STOP_TIME=$(date +%s)
				TEST_ELAPSED=$(($TEST_STOP_TIME-$TEST_START_TIME))
				if [ "$skip" -eq 1 ]; then
					SKIP=$((SKIP+1))
					echo "skipped ${TEST_ELAPSED}s"
					echo "=== Test ${test_directory}/${script}: skipped." >> $CCTOOLS_TEST_LOG
				elif [ "$result" -eq 0 ]; then
					SUCCESS=$((SUCCESS+1))
					echo "success ${TEST_ELAPSED}s"
					echo "=== Test ${test_directory}/${script}: success." >> $CCTOOLS_TEST_LOG
				else
					FAILURE=$((FAILURE+1))
					echo "failure ${TEST_ELAPSED}s"
					echo "=== Test ${test_directory}/${script}: failure." >> $CCTOOLS_TEST_LOG
				fi
			fi
		done
		cd "$test_root"
	fi
done
STOP_TIME=$(date +%s)

TOTAL=$((SUCCESS+FAILURE-SKIP))
ELAPSED=$((STOP_TIME-START_TIME))

echo ""
echo "Test Results: ${FAILURE} of ${TOTAL} tests failed (${SKIP} skipped) in ${ELAPSED} seconds."
echo ""

if [ "$FAILURE" -eq 0 ]; then
	exit 0
else
	exit 1
fi

# vim: set noexpandtab tabstop=4:
