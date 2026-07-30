#!/usr/bin/env python3

import argparse
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
from unittest import mock

from datavine_phase4_demand_pull import run_case
from ndcctools.taskvine.datavine import TaskRecord, Workflow
from ndcctools.taskvine.datavine.controller.state import ControllerState
from ndcctools.taskvine.datavine.serialization import serialize


LARGE_SIZE = 2 * 1024 * 1024
INLINE_LIMIT = 64 * 1024
CONTROLLER_IDATA_LIMIT = 128 * 1024


def state_capacity_contract():
    state = ControllerState(
        max_idata_bytes=8,
        max_inline_idata_bytes=6,
    )
    metadata, function_payload = serialize(bytes)
    function = state.register_edata(metadata, function_payload)
    first = state.allocate_idata(1)
    second = state.allocate_idata(2)
    state.register_task(
        TaskRecord(1, function.data_id, (), (), first.data_id, ())
    )
    state.register_task(
        TaskRecord(2, function.data_id, (), (), second.data_id, ())
    )
    state.publish_idata(first.data_id, 1, b"123456")
    try:
        state.publish_idata(second.data_id, 1, b"abc")
    except MemoryError:
        pass
    else:
        raise AssertionError("Controller accepted IData beyond total capacity")
    try:
        state.publish_idata(second.data_id, 1, b"1234567")
    except MemoryError:
        pass
    else:
        raise AssertionError("Controller accepted oversized inline IData")
    metadata = state.publish_idata_metadata(
        second.data_id,
        1,
        hashlib.sha256(b"1234567").hexdigest(),
        7,
    )
    assert metadata.serialized_bytes is None
    snapshot = state.snapshot()
    assert snapshot["idata_bytes"] == 6
    assert snapshot["idata_bytes_high_water"] == 6
    assert snapshot["idata_metadata_records"] == 1
    return snapshot


def external_persistence_idempotency_contract():
    with tempfile.TemporaryDirectory(
        prefix="datavine-external-persistence-contract-"
    ) as root:
        state = ControllerState(
            max_idata_bytes=8,
            max_inline_idata_bytes=6,
        )
        state.configure_persistence(root)
        try:
            metadata, function_payload = serialize(bytes)
            function = state.register_edata(
                metadata, function_payload
            )
            record = state.allocate_idata(1)
            state.register_task(
                TaskRecord(
                    1,
                    function.data_id,
                    (),
                    (),
                    record.data_id,
                    (),
                )
            )
            payload = b"1234567"
            state.publish_idata_metadata(
                record.data_id,
                1,
                hashlib.sha256(payload).hexdigest(),
                len(payload),
            )
            state.join_worker("persistence-source", 1)
            state.report_worker_replica(
                f"i:{record.data_id}",
                "persistence-source-replica",
                1,
                "worker-disk",
                hashlib.sha256(payload).hexdigest(),
                len(payload),
                "persistence-source",
                1,
            )
            state.request_persistence(record.data_id)
            request = state.idata_status(record.data_id)[
                "persistence_request"
            ]
            state.begin_external_persistence(
                record.data_id, request["request_id"]
            )
            Path(request["target_path"]).write_bytes(payload)
            first = state.complete_external_persistence(
                record.data_id, request["request_id"]
            )
            second = state.complete_external_persistence(
                record.data_id, request["request_id"]
            )
            repeated_begin = state.begin_external_persistence(
                record.data_id, request["request_id"]
            )
            assert first == second
            assert repeated_begin["state"] == "durable"
            return state.snapshot()
        finally:
            state.stop()


def external_persistence_cancel_retry_contract():
    with tempfile.TemporaryDirectory(
        prefix="datavine-external-persistence-cancel-"
    ) as root:
        state = ControllerState(
            max_idata_bytes=8,
            max_inline_idata_bytes=6,
        )
        state.configure_persistence(root)
        try:
            metadata, function_payload = serialize(bytes)
            function = state.register_edata(
                metadata, function_payload
            )
            record = state.allocate_idata(1)
            state.register_task(
                TaskRecord(
                    1,
                    function.data_id,
                    (),
                    (),
                    record.data_id,
                    (),
                )
            )
            payload = b"1234567"
            content_hash = hashlib.sha256(payload).hexdigest()
            state.publish_idata_metadata(
                record.data_id, 1, content_hash, len(payload)
            )
            state.join_worker("cancel-source", 1)
            state.report_worker_replica(
                f"i:{record.data_id}",
                "cancel-source-replica",
                1,
                "worker-disk",
                content_hash,
                len(payload),
                "cancel-source",
                1,
            )
            state.request_persistence(record.data_id)
            cancelled = state.idata_status(record.data_id)[
                "persistence_request"
            ]
            state.begin_external_persistence(
                record.data_id, cancelled["request_id"]
            )
            Path(cancelled["target_path"]).write_bytes(payload)
            validation_started = threading.Event()
            allow_validation = threading.Event()
            original_path_open = Path.open
            result = {}
            failure = {}

            class BlockingReader:
                def __init__(self, stream):
                    self.stream = stream

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    return self.stream.__exit__(*args)

                def read(self, *args, **kwargs):
                    validation_started.set()
                    if not allow_validation.wait(5):
                        raise TimeoutError(
                            "test did not release persistence validation"
                        )
                    return self.stream.read(*args, **kwargs)

            def block_target_read(path, *args, **kwargs):
                stream = original_path_open(path, *args, **kwargs)
                mode = (
                    args[0]
                    if args
                    else kwargs.get("mode", "r")
                )
                if (
                    Path(path) == Path(cancelled["target_path"])
                    and "r" in mode
                    and "b" in mode
                ):
                    return BlockingReader(stream)
                return stream

            def complete_persistence():
                try:
                    result["record"] = (
                        state.complete_external_persistence(
                            record.data_id,
                            cancelled["request_id"],
                        )
                    )
                except Exception as exc:
                    failure["error"] = exc

            with mock.patch.object(Path, "open", block_target_read):
                completion_thread = threading.Thread(
                    target=complete_persistence
                )
                completion_thread.start()
                assert validation_started.wait(5)
                status_started = time.monotonic()
                assert state.get_idata(record.data_id).durability == (
                    "writing"
                )
                validation_status_latency = (
                    time.monotonic() - status_started
                )
                assert validation_status_latency < 0.5
                assert state.cancel_persistence(
                    record.data_id, "test-active-cancel"
                ) == "cancelling"
                allow_validation.set()
                completion_thread.join(5)
            assert not completion_thread.is_alive()
            assert "error" not in failure, failure
            completion = result["record"]
            assert completion.durability == "cancelled"
            assert not Path(cancelled["target_path"]).exists()
            assert state.snapshot()["persistence_active"] == 0

            state.request_persistence(record.data_id)
            retry = state.idata_status(record.data_id)[
                "persistence_request"
            ]
            assert retry["request_id"] != cancelled["request_id"]
            state.begin_external_persistence(
                record.data_id, retry["request_id"]
            )
            Path(retry["target_path"]).write_bytes(b"corrupt")
            try:
                state.complete_external_persistence(
                    record.data_id, retry["request_id"]
                )
            except IOError:
                pass
            else:
                raise AssertionError(
                    "corrupt external persistence was acknowledged"
                )
            assert state.fail_external_persistence(
                record.data_id,
                retry["request_id"],
                "corrupt retry",
            ) == "failed"

            state.request_persistence(record.data_id)
            final = state.idata_status(record.data_id)[
                "persistence_request"
            ]
            state.begin_external_persistence(
                record.data_id, final["request_id"]
            )
            Path(final["target_path"]).write_bytes(payload)
            durable = state.complete_external_persistence(
                record.data_id, final["request_id"]
            )
            assert durable.durability == "durable"
            snapshot = state.snapshot()
            snapshot["validation_status_latency_seconds"] = (
                validation_status_latency
            )
            return snapshot
        finally:
            state.stop()


def make_large_payload(size):
    return bytes(index % 251 for index in range(size))


def payload_digest(payload):
    return hashlib.sha256(payload).hexdigest()


def run_bounded_case(factory_manager=None, persistence_mode="cancel"):
    if persistence_mode not in ("cancel", "failure", "loss-race"):
        raise ValueError("unknown persistence test mode")
    workflow = Workflow()
    large = workflow.add_task(make_large_payload, LARGE_SIZE)
    target = workflow.add_task(payload_digest, large.output())
    oracle = hashlib.sha256(
        bytes(index % 251 for index in range(LARGE_SIZE))
    ).hexdigest()
    snapshot = run_case(
        f"idata-capacity-{persistence_mode}",
        workflow,
        target.task_id,
        oracle,
        factory_manager=factory_manager,
        worker_count=1,
        worker_cores=1,
        prefetch=False,
        persistence=True,
        validate_durable_recovery=True,
        max_idata_bytes=CONTROLLER_IDATA_LIMIT,
        max_inline_idata_bytes=INLINE_LIMIT,
        inject_worker_loss_after=large.task_id,
        replacement_worker_delay=None if factory_manager else 1,
        persistence_parent=(
            "/groups/dthain/users/jzhou24/factory-scratch"
            if factory_manager
            else None
        ),
        additional_result_task_ids=(large.task_id,),
        inject_external_persistence_cancel=(
            persistence_mode == "cancel"
        ),
        inject_external_persistence_failures=(
            2 if persistence_mode == "failure" else 0
        ),
        inject_global_loss_during_persistence=(
            persistence_mode == "loss-race"
        ),
        external_persistence_max_retries=2,
        external_persistence_retry_base_seconds=0.25,
        external_persistence_retry_max_seconds=0.5,
        external_persistence_failure_delay=2,
    )
    report = snapshot["scheduler_report"]
    assert snapshot["idata_capacity_bytes"] == CONTROLLER_IDATA_LIMIT
    assert (
        snapshot["idata_inline_object_capacity_bytes"] == INLINE_LIMIT
    )
    assert snapshot["idata_bytes"] <= CONTROLLER_IDATA_LIMIT
    assert snapshot["idata_bytes_high_water"] <= CONTROLLER_IDATA_LIMIT
    assert snapshot["idata_bytes_after_durable_recovery"] == (
        snapshot["idata_bytes"]
    )
    assert snapshot["idata_metadata_records"] >= 1
    assert snapshot["idata_metadata_publications"] >= 1
    assert snapshot["idata_inline_records"] >= 1
    assert snapshot["external_persistence_requests"] == 1
    assert snapshot["external_persistence_durable"] == 1
    assert snapshot["durable_hashes_valid"]
    assert snapshot["persistence_temporary_files"] == []
    assert (
        snapshot["durable_recovery_actions"]["1"]
        == "validated-durable"
    )
    assert len(snapshot["durable_files"]) == 2
    if persistence_mode == "loss-race":
        assert report["persistence_tasks_completed"] >= 1
    else:
        assert report["persistence_tasks_completed"] == 1
    if persistence_mode == "cancel":
        assert report["persistence_cancellations"] == 1
        assert report["persistence_failures"] == 0
        assert report["persistence_retries"] == 0
    elif persistence_mode == "failure":
        assert report["persistence_cancellations"] == 0
        assert report["persistence_failures"] == 2
        assert report["persistence_injected_failures_observed"] == 2
        assert report["persistence_retries"] == 2
        assert report["persistence_retry_delay_seconds"] == 0.75
        assert (
            report["compute_completions_while_persistence_active"]
            >= 1
        ), report
        assert snapshot["persistence_max_active"] == 1
    else:
        assert report["persistence_global_losses"] == 1
        assert report["recovery_reexecutions"] >= 2, report
        assert len(report["persistence_loss_pruning_plans"]) == 1
        pruning = report["persistence_loss_pruning_plans"][0]
        assert pruning["before"]["decision"] == "keep"
        assert "persistence-writing" in pruning["before"]["reasons"]
        assert pruning["after"]["decision"] == "absent"
        assert "no-accepted-replica" in pruning["after"]["reasons"]
        assert snapshot["persistence_stale_completions"] == 0
    assert report["persistence_worker_bytes"] > LARGE_SIZE
    assert (
        snapshot["result_summaries"][str(large.task_id)][
            "serialized_size"
        ]
        > LARGE_SIZE
    )
    assert report["local_idata_hits"] >= 1, report
    assert report["legacy_recovery_tasks"] == 0, report
    assert report["worker_loss_injected"], report
    assert report["recovery_reexecutions"] >= 1, report
    assert report["physical_attempts"] > report["logical_tasks"], report
    return snapshot


def permanent_persistence_failure_contract():
    workflow = Workflow()
    large = workflow.add_task(make_large_payload, LARGE_SIZE)
    target = workflow.add_task(payload_digest, large.output())
    oracle = hashlib.sha256(
        bytes(index % 251 for index in range(LARGE_SIZE))
    ).hexdigest()
    try:
        run_case(
            "idata-persistence-permanent-failure",
            workflow,
            target.task_id,
            oracle,
            worker_count=1,
            worker_cores=1,
            prefetch=False,
            persistence=True,
            max_idata_bytes=CONTROLLER_IDATA_LIMIT,
            max_inline_idata_bytes=INLINE_LIMIT,
            additional_result_task_ids=(large.task_id,),
            inject_external_persistence_failures=1,
            external_persistence_max_retries=0,
            external_persistence_failure_delay=0,
        )
    except RuntimeError as exc:
        message = str(exc)
        assert "persistence exhausted 0 retries" in message
        return message
    raise AssertionError("permanent persistence failure was not visible")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    parser.add_argument(
        "--persistence-mode",
        choices=("cancel", "failure", "loss-race", "both"),
        default="both",
    )
    args = parser.parse_args()
    contract = state_capacity_contract()
    idempotency = external_persistence_idempotency_contract()
    cancel_retry = external_persistence_cancel_retry_contract()
    permanent_failure = (
        permanent_persistence_failure_contract()
        if args.factory_manager is None
        else "covered by local installed-path contract"
    )
    modes = (
        ("cancel", "failure", "loss-race")
        if args.persistence_mode == "both"
        else (args.persistence_mode,)
    )
    snapshots = {
        mode: run_bounded_case(args.factory_manager, mode)
        for mode in modes
    }
    snapshot = snapshots[modes[0]]
    print(
        json.dumps(
            {
                "controller_idata": {
                    key: snapshot[key]
                    for key in (
                        "idata_bytes",
                        "idata_bytes_high_water",
                        "idata_capacity_bytes",
                        "idata_inline_object_capacity_bytes",
                        "idata_inline_records",
                        "idata_metadata_records",
                        "idata_metadata_publications",
                    )
                },
                "state_capacity_contract": {
                    key: contract[key]
                    for key in (
                        "idata_bytes",
                        "idata_bytes_high_water",
                        "idata_capacity_bytes",
                        "idata_inline_object_capacity_bytes",
                        "idata_metadata_records",
                    )
                },
                "external_persistence_idempotency": {
                    "durable": idempotency[
                        "external_persistence_durable"
                    ],
                    "stale_completions": idempotency[
                        "persistence_stale_completions"
                    ],
                },
                "external_persistence_cancel_retry": {
                    "durable": cancel_retry[
                        "external_persistence_durable"
                    ],
                    "persistence_requests": cancel_retry[
                        "persistence_requests"
                    ],
                    "active": cancel_retry[
                        "persistence_active"
                    ],
                    "validation_status_latency_seconds": (
                        cancel_retry[
                            "validation_status_latency_seconds"
                        ]
                    ),
                },
                "permanent_persistence_failure": permanent_failure,
                "scheduler_report": snapshot["scheduler_report"],
                "workflow_modes": {
                    mode: {
                        "controller_idata_bytes_high_water": value[
                            "idata_bytes_high_water"
                        ],
                        "persistence_max_active": value[
                            "persistence_max_active"
                        ],
                        "temporary_files": value[
                            "persistence_temporary_files"
                        ],
                        "scheduler_report": value[
                            "scheduler_report"
                        ],
                    }
                    for mode, value in snapshots.items()
                },
                "status": "PASS",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
