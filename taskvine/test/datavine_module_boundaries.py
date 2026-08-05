#!/usr/bin/env python3

from pathlib import Path

import ndcctools.taskvine.datavine as datavine
from ndcctools.taskvine import cvine
from ndcctools.taskvine.datavine import native


root = Path(datavine.__file__).parent
controller_sources = "\n".join(
    path.read_text() for path in (root / "controller").glob("*.py")
)
worker_sources = "\n".join(
    path.read_text() for path in (root / "worker").glob("*.py")
)

for forbidden in (
    "cloudpickle",
    "..scheduler",
    "..serialization",
    "..worker",
    "..workflow",
    "from ndcctools.taskvine import cvine",
    "_native_client",
    "_native_server",
):
    assert forbidden not in controller_sources, forbidden

assert "..scheduler" not in worker_sources
assert not (root / "scheduler" / "client.py").exists()
assert not (root / "scheduler" / "thread.py").exists()
assert datavine.ControllerClient.__module__.endswith(".client")
assert datavine.WorkflowDriver.__module__.endswith(".scheduler.driver")

for name in (
    "AUTH",
    "ALLOCATE_BATCH",
    "PUBLISH_BATCH",
    "CLAIM_WORKER",
    "PUBLISH_OUTPUTS",
    "DISCONNECT_WORKER",
    "REPORT_REPLICA",
    "RESOLVE_SOURCE",
    "RELEASE_SOURCE",
    "REGISTER_EDATA",
    "GET_EDATA",
    "INVALIDATE_REPLICA",
    "RESTORE_REPLICA",
    "CONFIRM_REPLICA_PRUNED",
    "MARK_EDATA_SHARED",
):
    assert getattr(native, f"_{name}") == getattr(
        cvine, f"VINE_DATAVINE_RPC_{name}"
    )

print("DataVine module boundaries PASS")
