#!/usr/bin/env python3

import hashlib
import json

from ndcctools.taskvine.datavine.controller.service import ControllerService
from ndcctools.taskvine.datavine.controller.state import ControllerState
from ndcctools.taskvine.datavine.models import TaskRecord
from ndcctools.taskvine.datavine.protocol import DataVineRemoteError
from ndcctools.taskvine.datavine.client import ControllerClient
from ndcctools.taskvine.datavine.serialization import serialize


def expect_remote_error(fragment, function, *args, **kwargs):
    try:
        function(*args, **kwargs)
    except DataVineRemoteError as exc:
        assert fragment in str(exc), (
            fragment,
            str(exc),
        )
    else:
        raise AssertionError(
            f"expected remote failure containing {fragment!r}"
        )


def main():
    state = ControllerState()
    service = ControllerService("127.0.0.1", 0, "replica-token", state)
    host, port = service.start()
    client = ControllerClient(
        f"http://{host}:{port}",
        "replica-token",
        native_endpoint=f"tcp://127.0.0.1:{service.native_address[1]}",
    )
    metadata, edata_payload = serialize({"shared": [1, 2, 3]})
    registered = client.register_edata(metadata, edata_payload)
    edata = state.get_edata(registered["data_id"])
    idata = state.allocate_idata(1)
    idata_payload = b"same-logical-bytes"
    state.register_task(
        TaskRecord(1, edata.data_id, (), (), idata.data_id, ())
    )
    state.publish_idata(idata.data_id, 1, idata_payload)
    same_bytes = state.allocate_idata(2)
    state.register_task(
        TaskRecord(
            2, edata.data_id, (), (), same_bytes.data_id, ()
        )
    )
    state.publish_idata(same_bytes.data_id, 1, idata_payload)
    zero = state.allocate_idata(3)
    state.register_task(
        TaskRecord(3, edata.data_id, (), (), zero.data_id, ())
    )
    state.publish_idata(zero.data_id, 1, b"")
    client.native.allocate_idata((
        (idata.data_id, 1, 0),
        (same_bytes.data_id, 2, 0),
        (zero.data_id, 3, 0),
    ))
    client.native.publish_idata_metadata((
        (
            idata.data_id,
            1,
            hashlib.sha256(idata_payload).hexdigest(),
            len(idata_payload),
        ),
        (
            same_bytes.data_id,
            1,
            hashlib.sha256(idata_payload).hexdigest(),
            len(idata_payload),
        ),
        (zero.data_id, 1, hashlib.sha256(b"").hexdigest(), 0),
    ))
    client.finalize_native_edata((edata.data_id,))

    def report_replica(*args):
        replica = client.report_replica(*args)
        client.project_data_events((), replicas=(replica,))
        return replica

    try:
        client.claim_worker("w1", "http://127.0.0.1:1/w1")
        client.claim_worker("w2", "http://127.0.0.1:1/w2")
        edata_hash = edata.content_hash
        first = report_replica(
            f"e:{edata.data_id}",
            "w1-edata",
            1,
            "worker-disk",
            edata_hash,
            len(edata_payload),
            "w1",
            1,
        )
        second = report_replica(
            f"e:{edata.data_id}",
            "w2-edata",
            1,
            "worker-dram",
            edata_hash,
            len(edata_payload),
            "w2",
            1,
        )
        sources = client.replica_sources(f"e:{edata.data_id}")["sources"]
        assert [source["replica_id"] for source in sources[:2]] == [
            "w2-edata",
            "w1-edata",
        ]
        resolved = client.resolve_worker_source(
            f"e:{edata.data_id}",
            "w1",
            "taskvine:resolved-contract-1",
        )
        assert resolved["source"]["worker_id"] == "w2"
        assert resolved["lease"]["lease_id"] == (
            "taskvine:resolved-contract-1"
        )
        assert client.resolve_worker_source(
            f"e:{edata.data_id}",
            "w1",
            "taskvine:resolved-contract-1",
        ) == resolved
        client.release_replica(
            resolved["lease"]["lease_id"], True
        )
        expect_remote_error(
            "no available worker source",
            client.resolve_worker_source,
            f"e:{edata.data_id}",
            "w1",
            "taskvine:resolved-contract-excluded",
            ("w2",),
        )

        transfer_id = "taskvine:transfer-contract-1"
        client.resolve_worker_source(
            f"e:{edata.data_id}", "w1", transfer_id
        )
        retiring_source = client.invalidate_replica(
            f"e:{edata.data_id}",
            "w2-edata",
            1,
            "w2",
            1,
        )
        assert retiring_source["state"] == "retiring"
        released_transfer = client.release_replica(
            transfer_id, False
        )
        assert not released_transfer["active"]
        assert released_transfer["success"] is False
        assert client.release_replica(
            transfer_id, False
        ) == released_transfer
        expect_remote_error(
            "transfer identity already completed",
            client.resolve_worker_source,
            f"e:{edata.data_id}",
            "w1",
            transfer_id,
        )

        lease = client.acquire_replica(
            f"e:{edata.data_id}",
            first["replica_id"],
            first["generation"],
            "w2",
            1,
        )
        retiring = client.invalidate_replica(
            f"e:{edata.data_id}",
            first["replica_id"],
            first["generation"],
            "w1",
            1,
        )
        assert retiring["state"] == "retiring"
        assert retiring["load"] == 1
        assert first["replica_id"] not in {
            source["replica_id"]
            for source in client.replica_sources(
                f"e:{edata.data_id}"
            )["sources"]
        }
        client.release_replica(lease["lease_id"], True)

        expect_remote_error(
            "foreign replica",
            client.invalidate_replica,
            f"e:{edata.data_id}",
            "w2-edata",
            1,
            "w1",
            1,
        )
        expect_remote_error(
            "logical data identity",
            client.report_replica,
            f"i:{idata.data_id}",
            "corrupt",
            1,
            "worker-disk",
            hashlib.sha256(b"wrong").hexdigest(),
            len(idata_payload),
            "w1",
            1,
        )

        output_hash = hashlib.sha256(idata_payload).hexdigest()
        prepared = client.prepare_replica(
            f"i:{idata.data_id}",
            "w1-output",
            1,
            "worker-disk",
            output_hash,
            len(idata_payload),
            "w1",
            1,
        )
        reconciled = client.reconcile_workers(["w2"])
        assert reconciled["disconnected"][0]["worker_id"] == "w1"
        expect_remote_error(
            "state invalid",
            client.commit_replica,
            f"i:{idata.data_id}",
            prepared["replica_id"],
            prepared["generation"],
            1,
            output_hash,
            len(idata_payload),
        )
        claimed = client.claim_worker("w1", "http://127.0.0.1:1/w1")
        assert claimed["epoch"] == 2
        assert (
            client.claim_worker("w1", "http://127.0.0.1:1/w1")
            == claimed
        )
        expect_remote_error(
            "stale worker epoch",
            client.report_replica,
            f"i:{idata.data_id}",
            "stale-epoch",
            1,
            "worker-disk",
            output_hash,
            len(idata_payload),
            "w1",
            1,
        )

        first_idata = report_replica(
            f"i:{idata.data_id}",
            "w2-idata-one",
            1,
            "worker-disk",
            output_hash,
            len(idata_payload),
            "w2",
            1,
        )
        invalid_idata = client.invalidate_replica(
            f"i:{idata.data_id}",
            first_idata["replica_id"],
            first_idata["generation"],
            "w2",
            1,
        )
        assert invalid_idata["state"] == "invalid"
        client.confirm_replica_pruned(
            f"i:{idata.data_id}",
            first_idata["replica_id"],
            first_idata["generation"],
        )
        rematerialized_idata = report_replica(
            f"i:{idata.data_id}",
            "w2-idata-one",
            1,
            "worker-disk",
            output_hash,
            len(idata_payload),
            "w2",
            1,
        )
        assert (
            rematerialized_idata["generation"]
            == first_idata["generation"] + 1
        )
        refreshed_invalidation = client.invalidate_observed_replica(
            f"i:{idata.data_id}",
            first_idata["replica_id"],
            first_idata["attempt"],
            first_idata["content_hash"],
            first_idata["size"],
            "w2",
            1,
        )
        assert refreshed_invalidation["state"] == "invalid"
        assert (
            refreshed_invalidation["generation"]
            == rematerialized_idata["generation"]
        )
        expect_remote_error(
            "no longer current",
            client.invalidate_observed_replica,
            f"i:{idata.data_id}",
            first_idata["replica_id"],
            1,
            hashlib.sha256(b"wrong").hexdigest(),
            first_idata["size"],
            "w2",
            1,
        )
        report_replica(
            f"i:{idata.data_id}",
            "global-loss-w1",
            1,
            "worker-disk",
            output_hash,
            len(idata_payload),
            "w1",
            2,
        )
        report_replica(
            f"i:{idata.data_id}",
            "global-loss-w2",
            1,
            "worker-dram",
            output_hash,
            len(idata_payload),
            "w2",
            1,
        )
        assert len(
            client.replica_sources(f"i:{idata.data_id}")["sources"]
        ) == 3
        invalid_before = client.snapshot()["replica_directory"][
            "replica_states"
        ]["invalid"]
        assert (
            client.invalidate_idata(idata.data_id)["action"]
            == "globally-lost"
        )
        assert client.replica_sources(
            f"i:{idata.data_id}"
        )["sources"] == []
        assert client.snapshot()["replica_directory"][
            "replica_states"
        ]["invalid"] >= invalid_before + 3
        report_replica(
            f"i:{same_bytes.data_id}",
            "w2-idata-two",
            1,
            "worker-disk",
            output_hash,
            len(idata_payload),
            "w2",
            1,
        )
        zero_source = report_replica(
            f"i:{zero.data_id}",
            "w2-zero",
            1,
            "worker-disk",
            hashlib.sha256(b"").hexdigest(),
            0,
            "w2",
            1,
        )
        assert client.replica_sources(
            f"i:{idata.data_id}"
        )["data_id"] != client.replica_sources(
            f"i:{same_bytes.data_id}"
        )["data_id"]
        assert any(
            source["size"] == 0
            for source in client.replica_sources(
                f"i:{zero.data_id}"
            )["sources"]
        )

        client.claim_worker("w3", "http://127.0.0.1:1/w3")
        client.claim_worker("w4", "http://127.0.0.1:1/w4")
        source = zero_source
        destination_loss_lease = client.acquire_replica(
            f"i:{zero.data_id}",
            source["replica_id"],
            source["generation"],
            "w3",
            1,
        )
        source_loss_lease = client.acquire_replica(
            f"i:{zero.data_id}",
            source["replica_id"],
            source["generation"],
            "w4",
            1,
        )
        retiring_loss_source = client.invalidate_replica(
            f"i:{zero.data_id}",
            source["replica_id"],
            source["generation"],
            "w2",
            1,
        )
        assert retiring_loss_source["state"] == "retiring"
        assert retiring_loss_source["load"] == 2
        client.disconnect_worker("w3", 1)
        after_destination_loss = state.replicas.get_replica(
            f"i:{zero.data_id}", source["replica_id"]
        )
        assert after_destination_loss.state == "retiring"
        assert after_destination_loss.active_leases == 1
        assert client.release_replica(
            destination_loss_lease["lease_id"], False
        )["success"] is False
        client.disconnect_worker("w2", 1)
        after_source_loss = state.replicas.get_replica(
            f"i:{zero.data_id}", source["replica_id"]
        )
        assert after_source_loss.state == "invalid"
        assert after_source_loss.active_leases == 0
        assert client.release_replica(
            source_loss_lease["lease_id"], False
        )["success"] is False
        expect_remote_error(
            "conflicting duplicate lease release",
            client.release_replica,
            source_loss_lease["lease_id"],
            True,
        )
        snapshot = client.snapshot()["replica_directory"]
        assert snapshot["stale_rejections"] >= 1
        assert snapshot["peer_transfer_acquires"] >= 2
        assert snapshot["peer_transfer_idempotent"] >= 1
        assert snapshot["peer_transfer_releases"] >= 2
        assert snapshot["source_selection_requests"] >= 3
        assert snapshot["source_selection_misses"] >= 1
        assert snapshot["active_leases"] == 0
        print(json.dumps(snapshot, sort_keys=True))
        print("DataVine worker replica protocol component test PASS")
    finally:
        service.stop()


if __name__ == "__main__":
    main()
