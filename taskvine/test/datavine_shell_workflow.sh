#!/bin/sh

set -eu

repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cli="$repository/taskvine/src/tools/datavine_workflow"
worker_binary="$repository/taskvine/src/worker/vine_worker"
root=$(mktemp -d "${TMPDIR:-/tmp}/datavine-shell-workflow.XXXXXX")
service_pid=
worker_pid=

cleanup()
{
	if [ -n "$worker_pid" ]; then
		kill "$worker_pid" 2>/dev/null || true
		wait "$worker_pid" 2>/dev/null || true
	fi
	if [ -n "$service_pid" ]; then
		kill "$service_pid" 2>/dev/null || true
		wait "$service_pid" 2>/dev/null || true
	fi
	rm -rf "$root"
}
trap cleanup EXIT HUP INT TERM

wait_completed()
{
	workflow_id=$1
	for unused in $(seq 1 300); do
		"$cli" workflow status "$endpoint" "$token" "$workflow_id" \
			>"$root/status-$workflow_id.json"
		grep -q '"state":"completed"' "$root/status-$workflow_id.json" && return 0
		grep -q '"state":"failed"' "$root/status-$workflow_id.json" && {
			cat "$root/service.err" >&2
			return 1
		}
		sleep 0.1
	done
	return 1
}

cat >"$root/workflow.json" <<'EOF'
{
  "schema": "datavine.workflow/v1",
  "workflow_id": "shell-command-workflow",
  "idempotency_key": "shell-command-workflow-v1",
  "mode": "sealed",
  "tasks": [
    {
      "task_id": 1,
      "executor": {
        "kind": "command",
        "version": "1",
        "argv": ["/usr/bin/printf", "hello-shell\\n"]
      },
      "inputs": [],
      "output_data_ids": [1]
    }
  ],
  "data": [
    {
      "data_id": 1,
      "codec": {"name": "text/utf-8", "version": "1"},
      "origin": {"kind": "output", "task_id": 1, "output_index": 0}
    }
  ],
  "requested_outputs": [1],
  "policy": {"maximum_tasks": 1, "maximum_edges": 0}
}
EOF

token=shell-workflow-token
"$cli" serve "$root/journal" "$token" >"$root/contact.json" 2>"$root/service.err" &
service_pid=$!
for unused in $(seq 1 100); do
	[ -s "$root/contact.json" ] && break
	sleep 0.1
done
[ -s "$root/contact.json" ]
endpoint=$(sed -n 's/.*"endpoint":"\([^"]*\)".*/\1/p' "$root/contact.json")
manager_port=$(sed -n 's/.*"manager_port":\([0-9]*\).*/\1/p' "$root/contact.json")
[ -n "$endpoint" ]
[ -n "$manager_port" ]

"$worker_binary" --cores=1 --memory=256 --disk=256 --idle-timeout=15 \
	-d all -o "$root/worker.debug" \
	localhost "$manager_port" >"$root/worker.out" 2>"$root/worker.err" &
worker_pid=$!

"$cli" workflow validate "$root/workflow.json" >"$root/validated.json"
"$cli" workflow submit "$endpoint" "$token" "$root/workflow.json" \
	>"$root/submitted.json"

for unused in $(seq 1 300); do
	"$cli" workflow status "$endpoint" "$token" shell-command-workflow \
		>"$root/status.json"
	grep -q '"state":"completed"' "$root/status.json" && break
	grep -q '"state":"failed"' "$root/status.json" && {
		cat "$root/service.err" >&2
		exit 1
	}
	sleep 0.1
done
grep -q '"state":"completed"' "$root/status.json"
"$cli" workflow result "$endpoint" "$token" shell-command-workflow 1 \
	>"$root/result.json"
grep -q '"base64":"aGVsbG8tc2hlbGwK"' "$root/result.json"
"$cli" workflow result-info "$endpoint" "$token" shell-command-workflow 1 \
	>"$root/result-info.json"
grep -q '"requested":true' "$root/result-info.json"
grep -q '"producer_task_id":1' "$root/result-info.json"

printf seed >"$root/artifact"
cat >"$root/diamond.json" <<EOF
{
  "schema":"datavine.workflow/v1",
  "workflow_id":"shell-diamond",
  "idempotency_key":"shell-diamond-v1",
  "mode":"sealed",
  "tasks":[
    {"task_id":1,"executor":{"kind":"command","version":"1","argv":["/bin/cat","{{data:1}}"]},"inputs":[{"position":0,"data_id":1}],"output_data_ids":[2]},
    {"task_id":2,"executor":{"kind":"command","version":"1","argv":["/usr/bin/sed","y/abcdefghijklmnopqrstuvwxyz/ABCDEFGHIJKLMNOPQRSTUVWXYZ/","{{data:2}}"]},"inputs":[{"position":0,"data_id":2}],"output_data_ids":[3]},
    {"task_id":3,"executor":{"kind":"command","version":"1","argv":["/usr/bin/sed","s/seed/branch/","{{data:2}}"]},"inputs":[{"position":0,"data_id":2}],"output_data_ids":[4]},
    {"task_id":4,"executor":{"kind":"command","version":"1","argv":["/bin/cat","{{data:3}}","{{data:4}}","{{data:3}}"]},"inputs":[{"position":0,"data_id":3},{"position":1,"data_id":4},{"position":2,"data_id":3}],"output_data_ids":[5]}
  ],
  "data":[
    {"data_id":1,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"uri","uri":"file://$root/artifact"}},
    {"data_id":2,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":1,"output_index":0}},
    {"data_id":3,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":2,"output_index":0}},
    {"data_id":4,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":3,"output_index":0}},
    {"data_id":5,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":4,"output_index":0}}
  ],
  "requested_outputs":[3,5],
  "policy":{"maximum_tasks":4,"maximum_edges":5}
}
EOF
"$cli" workflow submit "$endpoint" "$token" "$root/diamond.json" \
	>"$root/diamond-submit.json"
wait_completed shell-diamond
"$cli" workflow result "$endpoint" "$token" shell-diamond 3 \
	>"$root/diamond-branch.json"
"$cli" workflow result "$endpoint" "$token" shell-diamond 5 \
	>"$root/diamond-result.json"
grep -q '"base64":"U0VFRA=="' "$root/diamond-branch.json"
grep -q '"base64":"U0VFRGJyYW5jaFNFRUQ="' "$root/diamond-result.json"
if grep -Fq "cache: transferring file://$root/artifact" "$root/worker.debug"; then
	echo "SharedFS file URI was copied through worker cache" >&2
	exit 1
fi
if "$cli" workflow result "$endpoint" "$token" shell-diamond 2 \
	>"$root/pruned.json" 2>"$root/pruned.err"; then
	echo "non-requested Shell intermediate was not pruned" >&2
	exit 1
fi

retry_marker=$root/retry-marker
cat >"$root/retry.json" <<EOF
{"schema":"datavine.workflow/v1","workflow_id":"shell-retry","idempotency_key":"shell-retry-v1","mode":"sealed","tasks":[{"task_id":1,"executor":{"kind":"command","version":"1","argv":["/bin/sh","-c","if [ ! -e '$retry_marker' ]; then : > '$retry_marker'; exit 7; else printf retried; fi"]},"inputs":[],"output_data_ids":[1],"retry":{"maximum_attempts":2}}],"data":[{"data_id":1,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":1,"output_index":0}}],"requested_outputs":[1],"policy":{"maximum_tasks":1,"maximum_edges":0}}
EOF
"$cli" workflow submit "$endpoint" "$token" "$root/retry.json" \
	>"$root/retry-submit.json"
wait_completed shell-retry
"$cli" workflow watch "$endpoint" "$token" shell-retry >"$root/retry-events.json"
grep -q '"type":11' "$root/retry-events.json"
"$cli" workflow result "$endpoint" "$token" shell-retry 1 >"$root/retry-result.json"
grep -q '"base64":"cmV0cmllZA=="' "$root/retry-result.json"

cat >"$root/dynamic-open.json" <<'EOF'
{"schema":"datavine.workflow/v1","workflow_id":"shell-dynamic","idempotency_key":"shell-dynamic-open-v1","mode":"streaming","tasks":[{"task_id":1,"executor":{"kind":"command","version":"1","argv":["/usr/bin/printf","dynamic"]},"inputs":[],"output_data_ids":[1]}],"data":[{"data_id":1,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":1,"output_index":0}}],"requested_outputs":[],"policy":{"maximum_tasks":2,"maximum_edges":1}}
EOF
cat >"$root/dynamic-append.json" <<'EOF'
{"schema":"datavine.workflow-delta/v1","workflow_id":"shell-dynamic","idempotency_key":"shell-dynamic-append-v1","tasks":[{"task_id":2,"executor":{"kind":"command","version":"1","argv":["/bin/sh","-c","printf '%s-next' \"$(cat \"$1\")\"","shell-dynamic","{{data:1}}"]},"inputs":[{"position":0,"data_id":1}],"output_data_ids":[2]}],"data":[{"data_id":2,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":2,"output_index":0}}],"requested_outputs":[2]}
EOF
"$cli" workflow submit "$endpoint" "$token" "$root/dynamic-open.json" \
	>"$root/dynamic-submit.json"
"$cli" workflow append "$endpoint" "$token" shell-dynamic 1 \
	"$root/dynamic-append.json" >"$root/dynamic-append-result.json"
"$cli" workflow seal "$endpoint" "$token" shell-dynamic 2 \
	>"$root/dynamic-seal.json"
wait_completed shell-dynamic
"$cli" workflow result "$endpoint" "$token" shell-dynamic 2 \
	>"$root/dynamic-result.json"
grep -q '"base64":"ZHluYW1pYy1uZXh0"' "$root/dynamic-result.json"

kill "$worker_pid"
wait "$worker_pid" 2>/dev/null || true
worker_pid=
kill "$service_pid"
wait "$service_pid"
service_pid=

"$cli" serve "$root/journal" "$token" >"$root/restarted.json" 2>>"$root/service.err" &
service_pid=$!
for unused in $(seq 1 100); do
	[ -s "$root/restarted.json" ] && break
	sleep 0.1
done
endpoint=$(sed -n 's/.*"endpoint":"\([^"]*\)".*/\1/p' "$root/restarted.json")
"$cli" workflow result "$endpoint" "$token" shell-command-workflow 1 \
	>"$root/restarted-result.json"
grep -q '"base64":"aGVsbG8tc2hlbGwK"' "$root/restarted-result.json"

echo "DataVine Shell adaptor PASS python=0 linear=1 fan-out=1 diamond=1 repeated-input=1 dynamic-append=1 retry=1 requested-output=1 file-artifact=1 submitter-detached=1 durable-result=1"
