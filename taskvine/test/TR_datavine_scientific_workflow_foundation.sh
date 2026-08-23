#!/bin/sh

set -eu

case "${1:-run}" in
	prepare)
		make -C "$(dirname "$0")/../src" -j8
		make -C "$(dirname "$0")/../src" install
		;;
	run)
		PYTHONNOUSERSITE=1 python "$(dirname "$0")/datavine_scientific_workflow_foundation.py"
		;;
	clean)
		;;
	*)
		echo "usage: $0 prepare|run|clean" >&2
		exit 2
		;;
esac
