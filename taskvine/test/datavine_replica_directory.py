#!/usr/bin/env python3

import hashlib
import json
import threading

from ndcctools.taskvine.datavine.controller.replicas import (
    ReplicaDirectory,
)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def expect_error(fragment, function, *args, **kwargs):
    try:
        function(*args, **kwargs)
    except (KeyError, RuntimeError, ValueError) as exc:
        assert fragment in str(exc), (fragment, str(exc))
    else:
        raise AssertionError(f"expected failure containing {fragment!r}")


def main():
    assert (
        ReplicaDirectory().snapshot()["completed_lease_capacity"]
        == 65536
    )
    directory = ReplicaDirectory(max_completed_leases=2)
    directory.join_worker("w1", 1)
    directory.join_worker("w2", 1)
    payload = b"replica-data"
    content_hash = digest(payload)

    first = directory.prepare_replica(
        "i:1", "w1-disk", 1, "worker-disk", content_hash, len(payload), "w1", 1
    )
    # Preparing/partial data is never a candidate.
    assert directory.candidates("i:1") == ()
    assert directory.prepare_replica(
        "i:1", "w1-disk", 1, "worker-disk", content_hash, len(payload), "w1", 1
    ) == first
    first = directory.commit_replica(
        "i:1", "w1-disk", first.generation, 1, content_hash, len(payload)
    )
    assert directory.commit_replica(
        "i:1", "w1-disk", first.generation, 1, content_hash, len(payload)
    ) == first

    directory.report_bytes(
        "i:1", "w2-dram", 1, "worker-dram", payload, "w2", 1
    )
    assert [value.replica_id for value in directory.candidates("i:1")] == [
        "w2-dram",
        "w1-disk",
    ]

    # Selection is advisory. Acquire revalidates generation and epoch.
    selected = directory.candidates("i:1")[0]
    directory.invalidate_replica(
        "i:1", selected.replica_id, selected.generation
    )
    expect_error(
        "source disappeared",
        directory.acquire_source,
        "i:1",
        selected.replica_id,
        selected.generation,
        "w1",
        1,
    )

    # A lease protects an active read while invalidation retires the source.
    lease = directory.acquire_source(
        "i:1", first.replica_id, first.generation, "w2", 1
    )
    retiring = directory.invalidate_replica(
        "i:1", first.replica_id, first.generation
    )
    assert retiring.state == "retiring"
    assert not directory.globally_available("i:1")
    directory.release_source(lease.lease_id, True)
    assert directory.get_replica("i:1", first.replica_id).state == "invalid"
    assert directory.release_source(lease.lease_id, True).success
    expect_error(
        "conflicting duplicate",
        directory.release_source,
        lease.lease_id,
        False,
    )

    # A new worker epoch invalidates old replicas and rejects stale reports.
    directory.join_worker("w1", 2)
    expect_error(
        "stale worker epoch",
        directory.join_worker,
        "w1",
        1,
    )
    expect_error(
        "stale worker epoch",
        directory.prepare_replica,
        "i:1",
        "stale",
        1,
        "worker-disk",
        content_hash,
        len(payload),
        "w1",
        1,
    )

    # A newer recovery attempt rejects completion from the older attempt.
    stale = directory.prepare_replica(
        "i:2", "attempt-one", 1, "worker-disk", content_hash, len(payload), "w1", 2
    )
    directory.advance_attempt("i:2", 2)
    expect_error(
        "stale replica completion attempt",
        directory.commit_replica,
        "i:2",
        "attempt-one",
        stale.generation,
        1,
        content_hash,
        len(payload),
    )

    # Corrupt/altered metadata cannot become available.
    corrupt = directory.prepare_replica(
        "i:3", "corrupt", 1, "worker-disk", content_hash, len(payload), "w1", 2
    )
    expect_error(
        "content metadata mismatch",
        directory.commit_replica,
        "i:3",
        "corrupt",
        corrupt.generation,
        1,
        digest(b"wrong"),
        len(payload),
    )

    # One replica loss is not global loss; all volatile loss is.
    directory.report_bytes(
        "i:4", "w1-copy", 1, "worker-disk", payload, "w1", 2
    )
    w2_copy = directory.report_bytes(
        "i:4", "w2-copy", 1, "worker-disk", payload, "w2", 1
    )
    directory.disconnect_worker("w2", 1)
    assert directory.globally_available("i:4")
    assert not directory.disconnect_worker("w2", 1).active
    directory.join_worker("w2", 2)
    expect_error(
        "stale worker disconnect",
        directory.disconnect_worker,
        "w2",
        1,
    )
    w1_copy = directory.candidates("i:4")[0]
    directory.invalidate_replica(
        "i:4", w1_copy.replica_id, w1_copy.generation
    )
    assert not directory.globally_available("i:4")
    assert directory.get_replica("i:4", w2_copy.replica_id).state == "invalid"

    # SharedFS quarantine is excluded from sources, reversible during grace,
    # and hard deletion requires an unchanged proof revision.
    durable = directory.report_bytes(
        "i:5", "sharedfs-5", 1, "sharedfs", payload
    )
    revision = directory.revision
    quarantined = directory.quarantine(
        "i:5", durable.replica_id, durable.generation, 10, revision, now=100
    )
    assert quarantined.state == "quarantined"
    assert not directory.globally_available("i:5")
    directory.restore_quarantine(
        "i:5", durable.replica_id, durable.generation
    )
    assert directory.globally_available("i:5")
    revision = directory.revision
    directory.quarantine(
        "i:5", durable.replica_id, durable.generation, 10, revision, now=100
    )
    expect_error(
        "proof revision changed",
        directory.hard_delete_quarantine,
        "i:5",
        durable.replica_id,
        durable.generation,
        revision,
        111,
    )
    revision = directory.revision
    expect_error(
        "cannot be hard deleted",
        directory.hard_delete_quarantine,
        "i:5",
        durable.replica_id,
        durable.generation,
        revision,
        109,
    )
    pruned = directory.hard_delete_quarantine(
        "i:5",
        durable.replica_id,
        durable.generation,
        revision,
        now=111,
    )
    assert pruned.state == "pruned"

    # Zero-byte replicas are valid when their hash is exact.
    zero = directory.report_bytes(
        "e:6", "external-zero", 1, "external", b""
    )
    assert zero.size == 0
    assert directory.globally_available("e:6")

    # EData and IData numeric IDs cannot alias in the physical directory.
    directory.report_bytes(
        "i:6", "idata-zero", 1, "external", b""
    )
    assert directory.candidates("e:6")[0].data_id == "e:6"
    assert directory.candidates("i:6")[0].data_id == "i:6"
    expect_error("qualified", directory.candidates, 6)

    # Completed-lease idempotency is bounded rather than growing forever.
    for data_id in ("e:6", "i:6", "e:6"):
        source = directory.candidates(data_id)[0]
        current = directory.acquire_source(
            data_id,
            source.replica_id,
            source.generation,
            "w1",
            2,
        )
        directory.release_source(current.lease_id, True)
    assert directory.snapshot()["completed_lease_tombstones"] == 2

    # Concurrent readers hold independent leases; invalidation retires the
    # source until both readers release it.
    concurrent = ReplicaDirectory(max_active_leases=2)
    concurrent.join_worker("d1", 1)
    concurrent.join_worker("d2", 1)
    source = concurrent.report_bytes(
        "e:1", "controller-hot", 1, "controller-memory", payload
    )
    acquired = threading.Barrier(3)
    release = threading.Barrier(3)
    failures = []

    def reader(worker_id):
        try:
            current = concurrent.acquire_source(
                "e:1",
                source.replica_id,
                source.generation,
                worker_id,
                1,
            )
            acquired.wait()
            release.wait()
            concurrent.release_source(current.lease_id, True)
        except BaseException as exc:
            failures.append(exc)

    threads = [
        threading.Thread(target=reader, args=(worker_id,))
        for worker_id in ("d1", "d2")
    ]
    for thread in threads:
        thread.start()
    acquired.wait()
    assert concurrent.snapshot()["active_leases"] == 2
    retiring = concurrent.invalidate_replica(
        "e:1", source.replica_id, source.generation
    )
    assert retiring.state == "retiring"
    release.wait()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()
    assert not failures
    assert concurrent.get_replica("e:1", source.replica_id).state == "invalid"

    # A newer attempt may publish while an old-generation read is active.
    # Attempt-specific replica identities keep the old lease releasable.
    generations = ReplicaDirectory()
    generations.join_worker("reader", 1)
    old_source = generations.report_bytes(
        "i:1",
        "controller-i1-attempt-1",
        1,
        "controller-memory",
        b"old",
    )
    old_lease = generations.acquire_source(
        "i:1",
        old_source.replica_id,
        old_source.generation,
        "reader",
        1,
    )
    generations.report_bytes(
        "i:1",
        "controller-i1-attempt-2",
        2,
        "controller-memory",
        b"new",
    )
    assert generations.get_replica(
        "i:1", old_source.replica_id
    ).state == "retiring"
    generations.release_source(old_lease.lease_id, True)
    assert generations.get_replica(
        "i:1", old_source.replica_id
    ).state == "invalid"
    assert [
        value.replica_id for value in generations.candidates("i:1")
    ] == ["controller-i1-attempt-2"]

    # Each metadata collection has an explicit admission bound and terminal
    # records can be forgotten after revision-checked logical pruning.
    bounded = ReplicaDirectory(
        max_replicas=1,
        max_workers=1,
        max_active_leases=1,
        max_completed_leases=1,
    )
    bounded.join_worker("only", 1)
    expect_error(
        "worker directory capacity",
        bounded.join_worker,
        "overflow",
        1,
    )
    only = bounded.report_bytes(
        "i:1", "only-copy", 1, "worker-disk", payload, "only", 1
    )
    expect_error(
        "replica directory capacity",
        bounded.report_bytes,
        "i:2",
        "overflow-copy",
        1,
        "external",
        payload,
    )
    current = bounded.acquire_source(
        "i:1", only.replica_id, only.generation, "only", 1
    )
    expect_error(
        "lease admission capacity",
        bounded.acquire_source,
        "i:1",
        only.replica_id,
        only.generation,
        "only",
        1,
    )
    bounded.invalidate_replica("i:1", only.replica_id, only.generation)
    expect_error(
        "live replica",
        bounded.forget_data,
        "i:1",
        bounded.revision,
    )
    bounded.release_source(current.lease_id, True)
    revision = bounded.revision
    assert bounded.forget_data("i:1", revision) == 1
    bounded.disconnect_worker("only", 1)
    bounded.forget_worker("only", 1)
    assert bounded.snapshot()["replicas"] == 0
    assert bounded.snapshot()["workers"] == 0

    snapshot = directory.snapshot()
    assert snapshot["stale_rejections"] >= 5
    assert snapshot["lease_high_water"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["completed_lease_capacity"] == 2
    print(json.dumps(snapshot, sort_keys=True))
    print("DataVine Controller replica directory component test PASS")


if __name__ == "__main__":
    main()
