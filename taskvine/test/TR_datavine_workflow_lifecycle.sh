#!/bin/sh

set -eu

case "${1:-run}" in
	prepare)
		make -C "$(dirname "$0")/../src" -j8
		make -C "$(dirname "$0")/../src" install
		;;
	run)
		python "$(dirname "$0")/datavine_workflow_lifecycle.py"
		;;
	clean)
		;;
	*)
		echo "usage: $0 prepare|run|clean" >&2
		exit 2
		;;
esac
