#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH
export PATH=$(dirname "${CCTOOLS_PYTHON_TEST_EXEC}"):$PATH

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
	"${CCTOOLS_PYTHON_TEST_EXEC}" -c "import cloudpickle" || return 1
	return 0
}

prepare()
{
	return 0
}

run()
{
	"${CCTOOLS_PYTHON_TEST_EXEC}" vine_graph_shadow_data_graph.py
}

clean()
{
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
