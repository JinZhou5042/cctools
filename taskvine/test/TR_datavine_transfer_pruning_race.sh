#!/bin/sh
set -eu
case "${1:-run}" in
	prepare|clean) ;;
	run) python "$(dirname "$0")/datavine_transfer_pruning_race.py" ;;
	*) echo "usage: $0 prepare|run|clean" >&2; exit 2 ;;
esac
