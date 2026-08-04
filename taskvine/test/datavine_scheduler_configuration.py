#!/usr/bin/env python3

from ndcctools.taskvine.datavine.scheduler.configuration import (
    configure_runtime,
)
from ndcctools.taskvine.datavine.scheduler.run_context import (
    WorkflowRunContext,
)


def configure(**changes):
    values = {
        "worker_disk_cache_admission_items": None,
        "worker_disk_cache_admission_bytes": None,
        "peer_source_losses": 0,
        "peer_source_loss_after_bytes": 0,
        "defer_peer_source_loss_after_bytes": False,
        "peer_corruptions": 0,
        "idata_release_failures": 0,
        "peer_release_retry_seconds": 0.1,
        "peer_release_capacity": 1024,
    }
    values.update(changes)
    return configure_runtime(**values)


def expect_error(error_type, text, **changes):
    try:
        configure(**changes)
    except error_type as exc:
        assert text in str(exc)
    else:
        raise AssertionError(f"accepted invalid configuration {changes}")


def main():
    first = WorkflowRunContext()
    second = WorkflowRunContext()
    first.logical_outputs[1] = 10
    first.edata_by_object[("value", 1)] = (object(), 1)
    first.release_registration_caches()
    assert first.logical_outputs == {1: 10}
    assert not first.edata_by_object
    assert not second.logical_outputs

    result = configure(
        worker_disk_cache_admission_items="7",
        worker_disk_cache_admission_bytes="4096",
        peer_source_losses="2",
        peer_source_loss_after_bytes="1024",
        defer_peer_source_loss_after_bytes=True,
        peer_corruptions="3",
        idata_release_failures="4",
        peer_release_retry_seconds="0.25",
        peer_release_capacity="8",
    )
    assert result.peer_source_losses == 2
    assert result.peer_source_loss_after_bytes == 1024
    assert result.defer_peer_source_loss_after_bytes
    assert result.peer_corruptions == 3
    assert result.idata_release_failures == 4
    assert result.peer_release_retry_seconds == 0.25
    assert result.peer_release_capacity == 8
    expect_error(
        ValueError,
        "positive byte threshold",
        defer_peer_source_loss_after_bytes=True,
    )
    expect_error(ValueError, "below one", peer_release_capacity=0)
    expect_error(
        ValueError,
        "item capacity is negative",
        worker_disk_cache_admission_items=-1,
    )
    print("DataVine Scheduler configuration contract PASS")


if __name__ == "__main__":
    main()
