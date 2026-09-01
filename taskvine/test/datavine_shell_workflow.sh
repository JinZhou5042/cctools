#!/bin/sh

set -eu

repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cli="$repository/taskvine/src/tools/datavine_workflow"
worker_binary="$repository/taskvine/src/worker/vine_worker"
root=$(mktemp -d "${TMPDIR:-/tmp}/datavine-shell-workflow.XXXXXX")
service_pid=
worker_pid=
endpoint=
manager_port=
service_error=
worker_debug=

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

stop_runtime()
{
	if [ -n "$worker_pid" ]; then
		kill "$worker_pid" 2>/dev/null || true
		wait "$worker_pid" 2>/dev/null || true
		worker_pid=
	fi
	if [ -n "$service_pid" ]; then
		kill "$service_pid" 2>/dev/null || true
		wait "$service_pid" 2>/dev/null || true
		service_pid=
	fi
}

start_runtime()
{
	case_name=$1
	stop_runtime
	contact="$root/contact-$case_name.json"
	service_error="$root/service-$case_name.err"
	worker_debug="$root/worker-$case_name.debug"
	DATAVINE_WORKFLOW_METRICS=1 "$cli" serve \
		"$root/$case_name.journal" "$token" >"$contact" 2>"$service_error" &
	service_pid=$!
	for unused in $(seq 1 100); do
		[ -s "$contact" ] && break
		sleep 0.1
	done
	[ -s "$contact" ]
	endpoint=$(sed -n 's/.*"endpoint":"\([^"]*\)".*/\1/p' "$contact")
	manager_port=$(sed -n 's/.*"manager_port":\([0-9]*\).*/\1/p' "$contact")
	[ -n "$endpoint" ] && [ -n "$manager_port" ]
	"$worker_binary" --cores=1 --memory=256 --disk=256 --idle-timeout=120 \
		-d all -o "$worker_debug" localhost "$manager_port" \
		>"$root/worker-$case_name.out" 2>"$root/worker-$case_name.err" &
	worker_pid=$!
}

wait_completed()
{
	workflow_id=$1
	for unused in $(seq 1 600); do
		"$cli" workflow status "$endpoint" "$token" "$workflow_id" \
			>"$root/status-$workflow_id.json"
		grep -q '"state":"completed"' "$root/status-$workflow_id.json" && return 0
		grep -q '"state":"failed"' "$root/status-$workflow_id.json" && {
			cat "$service_error" >&2
			return 1
		}
		sleep 0.1
	done
	return 1
}

wait_quiescent()
{
	workflow_id=$1
	for unused in $(seq 1 600); do
		"$cli" workflow status "$endpoint" "$token" "$workflow_id" \
			>"$root/status-$workflow_id.json"
		grep -q '"state":"open_quiescent"' \
			"$root/status-$workflow_id.json" && return 0
		grep -q '"state":"failed"' "$root/status-$workflow_id.json" && {
			cat "$service_error" >&2
			return 1
		}
		sleep 0.1
	done
	return 1
}

wait_submitted()
{
	workflow_id=$1
	task_id=$2
	for unused in $(seq 1 600); do
		"$cli" workflow watch "$endpoint" "$token" "$workflow_id" \
			>"$root/events-$workflow_id.json"
		grep -q "\"type\":9,\"task_id\":$task_id" \
			"$root/events-$workflow_id.json" && return 0
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
start_runtime command
"$cli" workflow validate "$root/workflow.json" >"$root/validated.json"
"$cli" workflow submit "$endpoint" "$token" "$root/workflow.json" \
	>"$root/submitted.json"

for unused in $(seq 1 300); do
	"$cli" workflow status "$endpoint" "$token" shell-command-workflow \
		>"$root/status.json"
	grep -q '"state":"completed"' "$root/status.json" && break
	grep -q '"state":"failed"' "$root/status.json" && {
		cat "$service_error" >&2
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
start_runtime diamond
"$cli" workflow submit "$endpoint" "$token" "$root/diamond.json" \
	>"$root/diamond-submit.json"
wait_completed shell-diamond
"$cli" workflow result "$endpoint" "$token" shell-diamond 3 \
	>"$root/diamond-branch.json"
"$cli" workflow result "$endpoint" "$token" shell-diamond 5 \
	>"$root/diamond-result.json"
grep -q '"base64":"U0VFRA=="' "$root/diamond-branch.json"
grep -q '"base64":"U0VFRGJyYW5jaFNFRUQ="' "$root/diamond-result.json"
if grep -Fq "cache: transferring file://$root/artifact" "$worker_debug"; then
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
start_runtime retry
"$cli" workflow submit "$endpoint" "$token" "$root/retry.json" \
	>"$root/retry-submit.json"
wait_completed shell-retry
"$cli" workflow watch "$endpoint" "$token" shell-retry >"$root/retry-events.json"
grep -q '"type":11' "$root/retry-events.json"
"$cli" workflow result "$endpoint" "$token" shell-retry 1 >"$root/retry-result.json"
grep -q '"base64":"cmV0cmllZA=="' "$root/retry-result.json"

cat >"$root/dynamic-open.json" <<'EOF'
{"schema":"datavine.workflow/v1","workflow_id":"shell-dynamic","idempotency_key":"shell-dynamic-open-v1","mode":"streaming","tasks":[{"task_id":1,"executor":{"kind":"command","version":"1","argv":["/usr/bin/printf","dynamic"]},"inputs":[],"output_data_ids":[1]}],"data":[{"data_id":1,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":1,"output_index":0}}],"requested_outputs":[],"policy":{"maximum_tasks":2,"maximum_edges":1,"idata_backup":"worker-local"}}
EOF
cat >"$root/dynamic-append.json" <<'EOF'
{"schema":"datavine.workflow-delta/v1","workflow_id":"shell-dynamic","idempotency_key":"shell-dynamic-append-v1","tasks":[{"task_id":2,"executor":{"kind":"command","version":"1","argv":["/bin/sh","-c","printf '%s-next' \"$(cat \"$1\")\"","shell-dynamic","{{data:1}}"]},"inputs":[{"position":0,"data_id":1}],"output_data_ids":[2]}],"data":[{"data_id":2,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":2,"output_index":0}}],"requested_outputs":[2]}
EOF
start_runtime dynamic
"$cli" workflow submit "$endpoint" "$token" "$root/dynamic-open.json" \
	>"$root/dynamic-submit.json"
# Let task 1 finish before its first consumer exists.  Its output must remain
# a volatile Worker-local replica across quiescence rather than being GC'd.
wait_quiescent shell-dynamic
# This case explicitly opts out of the default background backup. Volatile
# means the Controller owns only metadata. Lose the sole replica
# before the late consumer is appended; the new reference must lazily replay
# task 1 on the replacement Worker instead of hanging or eagerly recovering it
# while no consumer exists.
# Simulate an abrupt Worker loss. The Controller must invalidate the Agent
# session from the TCP disconnect; no graceful Worker shutdown is involved.
kill -KILL "$worker_pid"
wait "$worker_pid" 2>/dev/null || true
worker_pid=
"$worker_binary" --cores=1 --memory=256 --disk=256 --idle-timeout=120 \
	-d all -o "$root/replacement-worker.debug" \
	localhost "$manager_port" >"$root/replacement-worker.out" \
	2>"$root/replacement-worker.err" &
worker_pid=$!
"$cli" workflow append "$endpoint" "$token" shell-dynamic 1 \
	"$root/dynamic-append.json" >"$root/dynamic-append-result.json"
"$cli" workflow seal "$endpoint" "$token" shell-dynamic 2 \
	>"$root/dynamic-seal.json"
wait_completed shell-dynamic
"$cli" workflow result "$endpoint" "$token" shell-dynamic 2 \
	>"$root/dynamic-result.json"
grep -q '"base64":"ZHluYW1pYy1uZXh0"' "$root/dynamic-result.json"
"$cli" workflow watch "$endpoint" "$token" shell-dynamic \
	>"$root/dynamic-events.json"
[ "$(grep -o '"type":9,"task_id":1' "$root/dynamic-events.json" | wc -l)" -ge 2 ]

cat >"$root/promote-open.json" <<'EOF'
{"schema":"datavine.workflow/v1","workflow_id":"shell-late-request","idempotency_key":"shell-late-request-open-v1","mode":"streaming","tasks":[{"task_id":1,"executor":{"kind":"command","version":"1","argv":["/usr/bin/printf","late-request"]},"inputs":[],"output_data_ids":[1]}],"data":[{"data_id":1,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":1,"output_index":0}}],"requested_outputs":[],"policy":{"maximum_tasks":2,"maximum_edges":0}}
EOF
cat >"$root/promote-delta.json" <<'EOF'
{"schema":"datavine.workflow-delta/v1","workflow_id":"shell-late-request","idempotency_key":"shell-late-request-delta-v1","tasks":[{"task_id":2,"executor":{"kind":"command","version":"1","argv":["/bin/true"]},"inputs":[],"output_data_ids":[2]}],"data":[{"data_id":2,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":2,"output_index":0}}],"requested_outputs":[1]}
EOF
start_runtime late-request
"$cli" workflow submit "$endpoint" "$token" "$root/promote-open.json" \
	>"$root/promote-submit.json"
wait_quiescent shell-late-request
"$cli" workflow append "$endpoint" "$token" shell-late-request 1 \
	"$root/promote-delta.json" >"$root/promote-append.json"
"$cli" workflow seal "$endpoint" "$token" shell-late-request 2 \
	>"$root/promote-seal.json"
wait_completed shell-late-request
"$cli" workflow result "$endpoint" "$token" shell-late-request 1 \
	>"$root/promote-result.json"
grep -q '"base64":"bGF0ZS1yZXF1ZXN0"' "$root/promote-result.json"
"$cli" workflow watch "$endpoint" "$token" shell-late-request \
	>"$root/promote-events.json"
[ "$(grep -o '"type":9,"task_id":1' "$root/promote-events.json" | wc -l)" -eq 1 ]
grep 'datavine workflow shell-late-request ' "$service_error" |
	grep -q 'agent_active_data=1 agent_active_replicas=1 '

cat >"$root/inflight-open.json" <<'EOF'
{"schema":"datavine.workflow/v1","workflow_id":"shell-inflight-request","idempotency_key":"shell-inflight-request-open-v1","mode":"streaming","tasks":[{"task_id":1,"executor":{"kind":"command","version":"1","argv":["/bin/sh","-c","sleep 1; printf inflight-request"]},"inputs":[],"output_data_ids":[1]}],"data":[{"data_id":1,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":1,"output_index":0}}],"requested_outputs":[],"policy":{"maximum_tasks":2,"maximum_edges":0}}
EOF
cat >"$root/inflight-delta.json" <<'EOF'
{"schema":"datavine.workflow-delta/v1","workflow_id":"shell-inflight-request","idempotency_key":"shell-inflight-request-delta-v1","tasks":[{"task_id":2,"executor":{"kind":"command","version":"1","argv":["/bin/true"]},"inputs":[],"output_data_ids":[2]}],"data":[{"data_id":2,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":2,"output_index":0}}],"requested_outputs":[1]}
EOF
start_runtime inflight-request
"$cli" workflow submit "$endpoint" "$token" "$root/inflight-open.json" \
	>"$root/inflight-submit.json"
wait_submitted shell-inflight-request 1
"$cli" workflow append "$endpoint" "$token" shell-inflight-request 1 \
	"$root/inflight-delta.json" >"$root/inflight-append.json"
"$cli" workflow seal "$endpoint" "$token" shell-inflight-request 2 \
	>"$root/inflight-seal.json"
wait_completed shell-inflight-request
"$cli" workflow result "$endpoint" "$token" shell-inflight-request 1 \
	>"$root/inflight-result.json"
grep -q '"base64":"aW5mbGlnaHQtcmVxdWVzdA=="' \
	"$root/inflight-result.json"

stop_runtime
"$cli" serve "$root/command.journal" "$token" >"$root/restarted.json" 2>>"$root/service-command.err" &
service_pid=$!
for unused in $(seq 1 100); do
	[ -s "$root/restarted.json" ] && break
	sleep 0.1
done
endpoint=$(sed -n 's/.*"endpoint":"\([^"]*\)".*/\1/p' "$root/restarted.json")
"$cli" workflow result "$endpoint" "$token" shell-command-workflow 1 \
	>"$root/restarted-result.json"
grep -q '"base64":"aGVsbG8tc2hlbGwK"' "$root/restarted-result.json"

echo "DataVine Shell adaptor PASS python=0 linear=1 fan-out=1 diamond=1 repeated-input=1 late-dynamic-consumer=1 volatile-loss-replay=1 late-request-promotion=1 inflight-request-promotion=1 retry=1 requested-output=1 file-artifact=1 submitter-detached=1 durable-result=1"
