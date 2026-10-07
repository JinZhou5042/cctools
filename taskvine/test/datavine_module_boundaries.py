#!/usr/bin/env python3

from pathlib import Path
import subprocess

import ndcctools.taskvine.datavine as datavine


root = Path(datavine.__file__).parent
expected = {"__init__.py", "workflow.py", "workflow_client.py"}
actual = {path.name for path in root.glob("*.py")}
assert actual == expected, (actual, expected)
for removed in ("cache", "controller", "persistence", "recovery", "scheduler", "worker"):
    assert not (root / removed).exists(), removed
assert datavine.WorkflowClient.__module__.endswith(".workflow_client")
assert not hasattr(datavine, "WorkflowDriver")
assert not hasattr(datavine, "LegacyWorkflow")
assert not hasattr(datavine, "TaskRecord")
assert datavine.Workflow.__module__.endswith(".workflow")
adaptor_source = (root / "workflow.py").read_text()
adaptor_source += (root / "workflow_client.py").read_text()
for forbidden in (
    ".scheduler",
    ".controller",
    ".worker",
    "LegacyWorkflow",
    "TaskFactory",
    "WorkflowDriver",
    "vine_submit",
    "vine_wait",
):
    assert forbidden not in adaptor_source, forbidden

source = Path(__file__).resolve().parents[1] / "src"
manager = source / "manager"
worker = source / "worker"
native = source / "datavine"
assert native.is_dir()
assert not list(manager.glob("vine_datavine_*"))
expected_native_sources = {
    "vine_datavine_data_controller",
    "vine_datavine_ir",
    "vine_datavine_journal",
    "vine_datavine_object_store",
    "vine_datavine_parametric",
    "vine_datavine_replica_table",
    "vine_datavine_rpc",
    "vine_datavine_scheduler",
    "vine_datavine_workflow",
    "vine_datavine_workflow_runtime",
    "vine_datavine_workflow_store",
}
assert {path.stem for path in native.glob("vine_datavine_*.c")} == expected_native_sources
expected_native_headers = expected_native_sources - {"vine_datavine_workflow_runtime"}
expected_native_headers.add("vine_datavine_protocol")
assert {path.stem for path in native.glob("vine_datavine_*.h")} == expected_native_headers
assert not list(native.glob("vine_datavine_index.*"))
assert not list(native.glob("vine_datavine_directory.*"))
protocol = (native / "vine_datavine_protocol.h").read_text()
assert "VINE_DATAVINE_RPC_WORKFLOW_RESULT_DESCRIPTORS" in protocol
assert "VINE_DATAVINE_RPC_WORKFLOW_WAIT_TERMINAL" in protocol
assert "VINE_DATAVINE_TASK_INPUT_LOCAL_FILE" in protocol
for removed in (
    "ALLOCATE_BATCH",
    "REPORT_REPLICA",
    "REGISTER_EDATA",
    "REGISTER_TASKS",
    "RPC_PING",
    "working_directory",
    "AGENT_PERSISTED",
    "RESULT_PERSISTED",
    "Runtime-v2",
    "datavine-v2-",
):
    assert removed not in protocol, removed
assert "workflow_result_descriptors" in adaptor_source
assert "wait_workflow" in adaptor_source
worker_data_plane = {
    worker / "vine_datavine_transfer.c",
    worker / "vine_datavine_transfer.h",
    worker / "vine_datavine_agent.c",
    worker / "vine_datavine_agent.h",
    worker / "vine_cache.c",
    worker / "vine_worker.c",
}
for core_file in (
    *manager.glob("*.c"), *manager.glob("*.h"),
    *(path for path in worker.glob("*.c") if path not in worker_data_plane),
    *(path for path in worker.glob("*.h") if path not in worker_data_plane),
):
    assert "datavine" not in core_file.read_text().lower(), core_file
worker_transfer_source = (worker / "vine_datavine_transfer.c").read_text()
assert "VINE_DATAVINE_RPC_OBJECT_GET" in worker_transfer_source
assert "EVP_DigestUpdate" in worker_transfer_source
for forbidden in ("scheduler", "workflow_store", "data_controller"):
    assert forbidden not in worker_transfer_source, forbidden
worker_agent_source = (worker / "vine_datavine_agent.c").read_text()
assert "VINE_DATAVINE_RPC_AGENT_DATA_READY" in worker_agent_source
assert "VINE_DATAVINE_RPC_AGENT_RESOLVE" in worker_agent_source
assert "VINE_DATAVINE_RPC_AGENT_HEARTBEAT" in worker_agent_source
for forbidden in (
    "vine_manager_send",
    "vine_manager_cache_update",
    "vine_manager_cache_invalid",
    "vine_task_add_input",
    "vine_task_add_output",
    "persistence_enqueue",
    "durable_offset",
):
    assert forbidden not in worker_agent_source, forbidden

# The Manager carries one generic opaque extension frame.  It does not know
# DataIDs, replicas, cache transitions, hashes, paths, GC, or Controller RPCs.
manager_core = "\n".join(
    path.read_text() for path in (*manager.glob("*.c"), *manager.glob("*.h"))
)
assert "auxiliary_payload" in manager_core
for forbidden in (
    "datavine",
    "object_token",
    "agent_data_ready",
    "agent_resolve",
    "agent_heartbeat",
):
    assert forbidden not in manager_core.lower(), forbidden

taskvine_members = subprocess.check_output(
    ("ar", "t", manager / "libtaskvine.a"), text=True
).splitlines()
datavine_members = subprocess.check_output(
    ("ar", "t", native / "libdatavine.a"), text=True
).splitlines()
assert not any("datavine" in member for member in taskvine_members)
assert {Path(member).stem for member in datavine_members} == expected_native_sources
assert all(member.startswith("vine_datavine_") for member in datavine_members)

runtime_source = (native / "vine_datavine_workflow_runtime.c").read_text()
store_source = (native / "vine_datavine_workflow_store.c").read_text()
controller_source = (native / "vine_datavine_data_controller.c").read_text()
for forbidden in (
    "stdout-base64-v1",
    "workflow_store_publish_results",
    "vine_task_get_stdout",
    "EVP_Digest",
):
    assert forbidden not in runtime_source, forbidden
assert "vine_datavine_workflow_store_publish_results" not in store_source
assert "vine_task_get_stdout" in controller_source
assert "pthread_create" in controller_source
assert "vine_datavine_journal" in controller_source
assert "vine_declare_temp" not in controller_source
assert "vine_fetch_file" not in controller_source
assert "vine_fetch_file" not in runtime_source

for removed in (
    "pthread_create",
    "execution_mailbox",
    "completion_owners",
    "manager_request_head",
    "manager_owner_call",
    "DATAVINE_WORKFLOW_RUNTIME_LANES",
):
    assert removed not in runtime_source, removed
assert "runtime_main(runtime);" in runtime_source

python_executor = (source / "tools/datavine_executor").read_text()
assert "HashingWriter" in python_executor
assert "DVM1" in python_executor
assert "DVP1" in python_executor
assert "DVP3" in python_executor
assert "DVP4" in python_executor
for removed in ("DVM2", "DVP2", "DVP5", "DVP6", "DVP7", "DVP8", "DVP9", "urllib.parse"):
    assert removed not in python_executor, removed
assert "class ObjectPuller" not in python_executor
assert "base64" not in python_executor
for removed in (
    "DurabilityNotifier",
    "persistence_agent",
    "DATAVINE_PERSIST_CONTEXT_V1",
    "RESULT_PERSISTED",
):
    assert removed not in python_executor, removed

for removed in (
    "workflow_store_legacy",
    "DATAVINE_SERVICE_THREADS",
    "DATAVINE_WORKER_FIRST",
    "DATAVINE_RECOVERY_CACHE",
    "__attribute__((unused))",
):
    assert removed not in runtime_source + store_source, removed

swig = (source / "bindings/python3/taskvine.i").read_text().lower()
assert "vine_datavine" not in swig
core = "\n".join(path.read_text() for path in manager.glob("*.c"))
for removed in (
    "vine_submit_async",
    "submit_inbox",
    "vine_wait_many_ids",
    "vine_task_create_datavine_ticket",
):
    assert removed not in core, removed

print("DataVine module boundaries PASS")
