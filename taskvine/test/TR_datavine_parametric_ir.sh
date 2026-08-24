#!/bin/sh
set -eu
case "${1:-run}" in
  prepare)
    make -C "$(dirname "$0")/../src/tools" datavine_workflow datavine_parametric_test -j8
    ;;
  clean) ;;
  run) python "$(dirname "$0")/datavine_parametric_ir.py" ;;
  *) echo "usage: $0 prepare|run|clean" >&2; exit 2 ;;
esac
