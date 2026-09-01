#!/bin/sh

set -eu

case "${1:-run}" in
	prepare)
		make -C "$(dirname "$0")/../src" -j8
		make -C "$(dirname "$0")/../src" install
		;;
	run)
		repo=$(cd "$(dirname "$0")/../.." && pwd)
		output="${TMPDIR:-/tmp}/datavine-scheduler-$$.json"
		trap 'rm -f "$output" "$output.service.log" "$output.factory.log"' EXIT INT TERM
		python "$repo/acceptance/scripts/benchmark_native_workflow.py" \
			--tasks 300 \
			--workers 1 \
			--cores 16 \
			--memory 2048 \
			--disk 4096 \
			--executor builtin \
			--no-requested-output \
			--registration sealed \
			--worker-timeout 30 \
			--workflow-timeout 90 \
			--output "$output" >/dev/null
		;;
	clean)
		;;
	*)
		echo "usage: $0 prepare|run|clean" >&2
		exit 2
		;;
esac
