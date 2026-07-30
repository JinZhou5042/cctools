#!/usr/bin/env python3

import hashlib
import json

from ndcctools.taskvine.datavine.controller.service import ControllerService
from ndcctools.taskvine.datavine.controller.state import ControllerState
from ndcctools.taskvine.datavine.protocol import DataVineRemoteError
from ndcctools.taskvine.datavine.scheduler.client import ControllerClient
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
    metadata, edata_payload = serialize({"shared": [1, 2, 3]})
    edata = state.register_edata(metadata, edata_payload)
    idata = state.allocate_idata(1)
    idata_payload = b"same-logical-bytes"
    state.publish_idata(idata.data_id, 1, idata_payload)
    same_bytes = state.allocate_idata(2)
    state.publish_idata(same_bytes.data_id, 1, idata_payload)
    zero = state.allocate_idata(3)
    state.publish_idata(zero.data_id, 1, b"")

    service = ControllerService("127.0.0.1", 0, "replica-token", state)
    host, port = service.start()
    client = ControllerClient(f"http://{host}:{port}", "replica-token")
    try:
        client.join_worker("w1", 1)
        client.join_worker("w2", 1)
        edata_hash = edata.content_hash
        first = client.report_replica(
            f"e:{edata.data_id}",
            "w1-edata",
            1,
            "worker-disk",
            edata_hash,
            len(edata_payload),
            "w1",
            1,
        )
        client.report_replica(
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
        client.join_worker("w1", 2)
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

        client.report_replica(
            f"i:{idata.data_id}",
            "w2-idata-one",
            1,
            "worker-disk",
            output_hash,
            len(idata_payload),
            "w2",
            1,
        )
        client.report_replica(
            f"i:{same_bytes.data_id}",
            "w2-idata-two",
            1,
            "worker-disk",
            output_hash,
            len(idata_payload),
            "w2",
            1,
        )
        client.report_replica(
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
        snapshot = client.snapshot()["replica_directory"]
        assert snapshot["stale_rejections"] >= 1
        assert snapshot["lease_high_water"] == 1
        print(json.dumps(snapshot, sort_keys=True))
        print("DataVine worker replica protocol component test PASS")
    finally:
        service.stop()


if __name__ == "__main__":
    main()
