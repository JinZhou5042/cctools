#!/bin/sh

set -eu

case "${1:-run}" in
	prepare)
		;;
	run)
		python "$(dirname "$0")/datavine_python_output_policy.py"
		;;
	clean)
		;;
	*)
		echo "usage: $0 prepare|run|clean" >&2
		exit 2
		;;
esac
