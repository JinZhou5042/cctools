#!/bin/sh

case "$1" in
	run)
		python3 "$(dirname "$0")/datavine_pruning_lease_race.py"
		;;
	prepare|clean)
		;;
	*)
		echo "usage: $0 prepare|run|clean" >&2
		exit 1
		;;
esac
