#!/bin/sh

# Minimal POSIX-shell adaptor: declare Workflow IR and submit it.  The script
# may exit immediately after submit; the native runtime owns all later work.
set -eu

if [ "$#" -ne 2 ]; then
	echo "usage: $0 ENDPOINT TOKEN" >&2
	exit 2
fi

endpoint=$1
token=$2
workflow_file=$(mktemp "${TMPDIR:-/tmp}/datavine-shell-example.XXXXXX")
trap 'rm -f "$workflow_file"' EXIT HUP INT TERM

cat >"$workflow_file" <<'EOF'
{
  "schema": "datavine.workflow/v1",
  "workflow_id": "shell-example",
  "idempotency_key": "shell-example-v1",
  "mode": "sealed",
  "tasks": [{
    "task_id": 1,
    "executor": {
      "kind": "command",
      "version": "1",
      "argv": ["/usr/bin/printf", "hello from POSIX shell\n"]
    },
    "inputs": [],
    "output_data_ids": [1]
  }],
  "data": [{
    "data_id": 1,
    "codec": {"name": "text/utf-8", "version": "1"},
    "origin": {"kind": "output", "task_id": 1, "output_index": 0}
  }],
  "requested_outputs": [1],
  "policy": {"maximum_tasks": 1, "maximum_edges": 0}
}
EOF

datavine workflow validate "$workflow_file" >/dev/null
datavine workflow submit "$endpoint" "$token" "$workflow_file"
