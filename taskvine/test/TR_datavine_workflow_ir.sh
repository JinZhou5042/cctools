#!/bin/sh

set -eu

case "${1:-run}" in
	prepare)
		make -C "$(dirname "$0")/../src/manager" -j8
		make -C "$(dirname "$0")/../src/tools" datavine_workflow -j8
		;;
	run)
		python "$(dirname "$0")/datavine_workflow_ir.py"
		;;
	clean)
		;;
	*)
		echo "usage: $0 prepare|run|clean" >&2
		exit 2
		;;
esac
