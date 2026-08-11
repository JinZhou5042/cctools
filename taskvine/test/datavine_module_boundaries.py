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
assert len(list(native.glob("vine_datavine_*.c"))) == 7
assert len(list(native.glob("vine_datavine_*.h"))) == 7
assert not list(native.glob("vine_datavine_index.*"))
assert not list(native.glob("vine_datavine_directory.*"))
protocol = (native / "vine_datavine_protocol.h").read_text()
for removed in (
    "ALLOCATE_BATCH",
    "REPORT_REPLICA",
    "REGISTER_EDATA",
    "REGISTER_TASKS",
    "RPC_PING",
    "working_directory",
):
    assert removed not in protocol, removed
for core_file in (*manager.glob("*.c"), *manager.glob("*.h"),
                  *worker.glob("*.c"), *worker.glob("*.h")):
    assert "datavine" not in core_file.read_text().lower(), core_file

taskvine_members = subprocess.check_output(
    ("ar", "t", manager / "libtaskvine.a"), text=True
).splitlines()
datavine_members = subprocess.check_output(
    ("ar", "t", native / "libdatavine.a"), text=True
).splitlines()
assert not any("datavine" in member for member in taskvine_members)
assert len(datavine_members) == 7
assert all(member.startswith("vine_datavine_") for member in datavine_members)

runtime_source = (native / "vine_datavine_workflow_runtime.c").read_text()
store_source = (native / "vine_datavine_workflow_store.c").read_text()
controller_source = (native / "vine_datavine_data_controller.c").read_text()
for forbidden in (
    "DVP2",
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
assert "vine_declare_temp" in controller_source
assert "vine_fetch_file" not in controller_source
assert "vine_fetch_file" not in runtime_source

python_executor = (source / "tools/datavine_python_executor").read_text()
assert "HashingWriter" in python_executor
assert "DVM1" in python_executor
assert "base64" not in python_executor

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
