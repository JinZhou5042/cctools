#!/bin/sh

set -eu

repository=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
go_client=${DATAVINE_GO_BINARY:-}
go_compiler=${DATAVINE_GO_COMPILER:-go}
root=$(mktemp -d "${TMPDIR:-/tmp}/datavine-go-workflow.XXXXXX")
service_pid=
worker_pid=
endpoint=
manager_port=

cleanup()
{
	[ -z "$worker_pid" ] || kill "$worker_pid" 2>/dev/null || true
	[ -z "$worker_pid" ] || wait "$worker_pid" 2>/dev/null || true
	[ -z "$service_pid" ] || kill "$service_pid" 2>/dev/null || true
	[ -z "$service_pid" ] || wait "$service_pid" 2>/dev/null || true
	rm -rf "$root"
}
trap cleanup EXIT HUP INT TERM

stop_runtime()
{
	[ -z "$worker_pid" ] || kill "$worker_pid" 2>/dev/null || true
	[ -z "$worker_pid" ] || wait "$worker_pid" 2>/dev/null || true
	worker_pid=
	[ -z "$service_pid" ] || kill "$service_pid" 2>/dev/null || true
	[ -z "$service_pid" ] || wait "$service_pid" 2>/dev/null || true
	service_pid=
}

start_runtime()
{
	case_name=$1
	stop_runtime
	contact="$root/contact-$case_name.json"
	service_error="$root/service-$case_name.err"
	"$repository/taskvine/src/tools/datavine_workflow" serve \
		"$root/$case_name.journal" "$token" >"$contact" 2>"$service_error" &
	service_pid=$!
	for unused in $(seq 1 100); do
		[ -s "$contact" ] && break
		sleep 0.1
	done
	endpoint=$(sed -n 's/.*"endpoint":"\([^"]*\)".*/\1/p' "$contact")
	manager_port=$(sed -n 's/.*"manager_port":\([0-9]*\).*/\1/p' "$contact")
	[ -n "$endpoint" ] && [ -n "$manager_port" ]
	"$repository/taskvine/src/worker/vine_worker" --cores=1 --memory=256 \
		--disk=256 --idle-timeout=15 localhost "$manager_port" \
		>"$root/worker-$case_name.out" 2>"$root/worker-$case_name.err" &
	worker_pid=$!
}

if [ -z "$go_client" ]; then
    command -v "$go_compiler" >/dev/null 2>&1 || {
        echo "DATAVINE_GO_BINARY or DATAVINE_GO_COMPILER is required" >&2
        exit 2
    }
    "$go_compiler" build -o "$root/datavine_workflow_go" \
        "$repository/taskvine/examples/datavine_workflow_go.go"
    go_client="$root/datavine_workflow_go"
fi

cat >"$root/workflow.json" <<'EOF'
{"schema":"datavine.workflow/v1","workflow_id":"go-direct-workflow","idempotency_key":"go-direct-workflow-v1","mode":"sealed","tasks":[{"task_id":1,"executor":{"kind":"command","version":"1","argv":["/usr/bin/printf","go-adaptor"]},"inputs":[],"output_data_ids":[1]}],"data":[{"data_id":1,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":1,"output_index":0}}],"requested_outputs":[1],"policy":{"maximum_tasks":1,"maximum_edges":0}}
EOF
cat >"$root/dynamic-open.json" <<'EOF'
{"schema":"datavine.workflow/v1","workflow_id":"go-dynamic-workflow","idempotency_key":"go-dynamic-open-v1","mode":"streaming","tasks":[{"task_id":1,"executor":{"kind":"command","version":"1","argv":["/usr/bin/printf","go"]},"inputs":[],"output_data_ids":[1]}],"data":[{"data_id":1,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":1,"output_index":0}}],"requested_outputs":[],"policy":{"maximum_tasks":2,"maximum_edges":1}}
EOF
cat >"$root/dynamic-delta.json" <<'EOF'
{"schema":"datavine.workflow-delta/v1","workflow_id":"go-dynamic-workflow","idempotency_key":"go-dynamic-delta-v1","tasks":[{"task_id":2,"executor":{"kind":"command","version":"1","argv":["/bin/sh","-c","printf '%s-delta' \"$(cat \"$1\")\"","go","{{data:1}}"]},"inputs":[{"position":0,"data_id":1}],"output_data_ids":[2]}],"data":[{"data_id":2,"codec":{"name":"bytes","version":"1"},"origin":{"kind":"output","task_id":2,"output_index":0}}],"requested_outputs":[2]}
EOF

token=go-workflow-token
start_runtime shared-fixture
jq '.valid[] | select(.name == "sealed-command-chain") | .document' \
	"$repository/taskvine/test/datavine_workflow_ir_fixtures.json" \
	>"$root/shared-fixture.json"
"$go_client" submit "$endpoint" "$token" "$root/shared-fixture.json" \
	>"$root/shared-fixture-result.json"
grep -q '"digest":"51c21c319241ac51e316eece2870a4f2e2b7640b"' \
	"$root/shared-fixture-result.json"
for unused in $(seq 1 300); do
    "$go_client" status "$endpoint" "$token" fixture-command-chain \
		>"$root/shared-fixture-status.json"
	grep -q '"state":"completed"' "$root/shared-fixture-status.json" && break
	grep -q '"state":"failed"' "$root/shared-fixture-status.json" && exit 1
	sleep 0.1
done
grep -q '"state":"completed"' "$root/shared-fixture-status.json"
"$go_client" result "$endpoint" "$token" fixture-command-chain 3 \
	>"$root/shared-fixture-output.json"
grep -q '"base64":"SEVMTE8="' "$root/shared-fixture-output.json"

start_runtime direct
"$go_client" submit "$endpoint" "$token" "$root/workflow.json" \
	>"$root/submitted.json"
grep -q '"workflow_id":"go-direct-workflow"' "$root/submitted.json"
for unused in $(seq 1 300); do
    "$go_client" status "$endpoint" "$token" go-direct-workflow \
		>"$root/status.json"
	grep -q '"state":"completed"' "$root/status.json" && break
	sleep 0.1
done
grep -q '"state":"completed"' "$root/status.json"
"$go_client" result "$endpoint" "$token" go-direct-workflow 1 \
	>"$root/result.json"
grep -q '"base64":"Z28tYWRhcHRvcg=="' "$root/result.json"

start_runtime dynamic
"$go_client" submit "$endpoint" "$token" "$root/dynamic-open.json" \
	>"$root/dynamic-submit.json"
"$go_client" append "$endpoint" "$token" go-dynamic-workflow 1 \
	"$root/dynamic-delta.json" >"$root/dynamic-append.json"
"$go_client" seal "$endpoint" "$token" go-dynamic-workflow 2 \
	>"$root/dynamic-seal.json"
for unused in $(seq 1 300); do
	"$go_client" status "$endpoint" "$token" go-dynamic-workflow \
		>"$root/dynamic-status.json"
	grep -q '"state":"completed"' "$root/dynamic-status.json" && break
	sleep 0.1
done
grep -q '"state":"completed"' "$root/dynamic-status.json"
"$go_client" result "$endpoint" "$token" go-dynamic-workflow 2 \
	>"$root/dynamic-result.json"
grep -q '"base64":"Z28tZGVsdGE="' "$root/dynamic-result.json"

echo "DataVine Go adaptor PASS direct-protocol=1 shared-fixture-executed=1 python=0 exact-result=1 dynamic-delta=1"
