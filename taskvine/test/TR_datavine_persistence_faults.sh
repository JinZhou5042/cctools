#!/bin/sh

set -eu

case "${1:-run}" in
	prepare)
		make -C "$(dirname "$0")/../src" -j8
		make -C "$(dirname "$0")/../src" install
		;;
	run)
		if [ "$(uname -s)" != Linux ] || [ ! -d /proc/self/fd ]; then
			echo "DataVine persistence faults SKIP requires=linux-procfs"
			exit 0
		fi
		repo=$(cd "$(dirname "$0")/../.." && pwd)
		runtime=$(mktemp -d "${TMPDIR:-/tmp}/datavine-persistence-faults.XXXXXX")
		trap 'rm -rf "$runtime"' EXIT INT TERM
		compiler=$(sed -n 's/^CC=//p' "$repo/config.mk")
		if [ -z "$compiler" ]; then
			compiler=cc
		fi
		"$compiler" -std=c11 -Wall -Wextra -Werror -fPIC -shared \
			"$repo/taskvine/test/datavine_fsync_fault_preload.c" \
			-o "$runtime/datavine_fsync_fault_preload.so"
		DATAVINE_TEST_FSYNC_PRELOAD="$runtime/datavine_fsync_fault_preload.so" \
			python "$repo/taskvine/test/datavine_persistence_faults.py"
		;;
	clean)
		;;
	*)
		echo "usage: $0 prepare|run|clean" >&2
		exit 2
		;;
esac
