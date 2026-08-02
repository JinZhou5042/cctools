#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

case "${1:-run}" in
  prepare)
    ;;
  run)
    exec "$ROOT/acceptance/scripts/TR_datavine_storage_comparison.sh"
    ;;
  clean)
    ;;
  *)
    echo "usage: $0 {prepare|run|clean}" >&2
    exit 2
    ;;
esac
