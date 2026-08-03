#!/bin/sh

. "$(dirname "$0")/../../dttools/test/test_runner_common.sh"

prepare()
{
    return 0
}

run()
{
    python "$(dirname "$0")/datavine_library_single_task.py"
}

clean()
{
    return 0
}

dispatch "$@"
