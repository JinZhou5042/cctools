#!/bin/sh
set -eu
case "${1:-run}" in
	prepare|clean) ;;
	run) python "$(dirname "$0")/datavine_phase5_peer_cache.py" ;;
	*) echo "usage: $0 prepare|run|clean" >&2; exit 2 ;;
esac
