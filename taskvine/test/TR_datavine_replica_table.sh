#!/bin/sh

set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
binary="${TMPDIR:-/tmp}/datavine-replica-table-test-$$"
trap 'rm -f "$binary"' EXIT INT TERM

case "${1:-run}" in
	prepare)
		;;
	run)
		${CC:-cc} -std=c11 -Wall -Wextra -Werror -O2 \
			-I "$root/taskvine/src/datavine" \
			"$root/taskvine/test/datavine_replica_table.c" \
			"$root/taskvine/src/datavine/vine_datavine_replica_table.c" \
			-o "$binary"
		"$binary"
		;;
	clean)
		;;
	*)
		echo "usage: $0 prepare|run|clean" >&2
		exit 2
		;;
esac
