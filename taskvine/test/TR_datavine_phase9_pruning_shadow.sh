#!/bin/sh

set -eu

case "${1:-run}" in
	prepare)
		;;
	run)
		python "$(dirname "$0")/datavine_phase9_pruning_shadow.py"
		;;
	clean)
		;;
	*)
		echo "usage: $0 prepare|run|clean" >&2
		exit 2
		;;
esac
