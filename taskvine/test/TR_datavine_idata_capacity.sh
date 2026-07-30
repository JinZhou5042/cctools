#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	return 0
}

run()
{
	python3 datavine_idata_capacity.py
}

clean()
{
	return 0
}

dispatch "$@"
