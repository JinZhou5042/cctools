#!/bin/sh

set -eu

case "${1:-run}" in
	prepare)
		make -C "$(dirname "$0")/../src" -j8
		;;
	run)
		sh "$(dirname "$0")/datavine_go_workflow.sh"
		;;
	clean)
		;;
	*)
		echo "usage: $0 prepare|run|clean" >&2
		exit 2
		;;
esac
