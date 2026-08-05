"""Standalone Data Controller HTTP service running on its own thread."""

import threading

from ..native import NativeControllerCore, NativeControllerError
from .admission import ByteServingAdmission, BoundedThreadingHTTPServer
from .handler import ControllerHandlerFactory
from .faults import TransferFaults
from .state import ControllerState


class ControllerService:
    def __init__(
        self,
        host,
        port,
        token,
        state=None,
        max_request_concurrency=32,
        max_serving_concurrency=8,
        max_serving_bytes=64 * 1024 * 1024,
        serving_hook=None,
        native=None,
    ):
        if not token:
            raise ValueError("Controller token is required")
        self.host = host
        self.port = int(port)
        self.token = token
        self.state = state or ControllerState()
        self.max_request_concurrency = int(max_request_concurrency)
        self.byte_serving = ByteServingAdmission(
            max_serving_concurrency, max_serving_bytes
        )
        self.serving_hook = serving_hook
        self.transfer_faults = TransferFaults()
        self._server = None
        self._thread = None
        self.native = native
        self.native_address = None

    def snapshot(self):
        value = self.state.snapshot()
        if self.native is not None and self.native.running:
            native = self.native.metrics()
            directory = value["replica_directory"]
            additions = {
                "active_leases": native["active_leases"],
                "peer_transfer_acquires": (
                    native["source_selections"]
                    - native["source_misses"]
                ),
                "peer_transfer_failures": native["release_failures"],
                "peer_transfer_idempotent": native[
                    "idempotent_releases"
                ],
                "peer_transfer_releases": native["releases"],
                "source_selection_misses": native["source_misses"],
                "source_selection_requests": native[
                    "source_selections"
                ],
                "stale_rejections": native["stale_rejections"],
                "restorations": native["restorations"],
                "prunes": native["prunes"],
            }
            for key, count in additions.items():
                directory[key] = directory.get(key, 0) + count
            directory["native"] = native
            value["native_journal"] = self.native.journal_metrics()
        value["byte_serving"] = self.byte_serving.snapshot()
        value["transfer_faults"] = self.transfer_faults.snapshot()
        value["request_admission"] = (
            self._server.admission_snapshot()
            if self._server is not None
            else None
        )
        return value

    def sync_native_leases(self, data_ids, kind="i"):
        if self.native is None or not self.native.running:
            return
        for data_id in data_ids:
            key = f"{kind}:{int(data_id)}"
            native = {
                record["replica_id"]: record
                for record in self.worker_replicas(key)
            }
            for replica in self.state.replicas.records_for(
                key
            ):
                if replica.tier not in ("worker-dram", "worker-disk"):
                    continue
                current = native.get(replica.replica_id)
                count = int(
                    current["load"]
                    if current is not None
                    and current["generation"] == replica.generation
                    else 0
                )
                self.state.replicas.synchronize_active_leases(
                    key, replica.replica_id, count
                )

    def sync_all_native_leases(self):
        for data_id in self.state.replicas.data_ids():
            kind, token = data_id.split(":", 1)
            self.sync_native_leases((token,), kind)

    def disconnect_native_worker(self, worker_id, epoch):
        if self.native is not None and self.native.running:
            self.native.client.disconnect_worker(worker_id, epoch)

    def worker_replicas(self, data_id):
        if self.native is None or not self.native.running:
            return []
        return self.native.replicas(data_id)

    def get_native_edata(self, data_id, **options):
        if self.native is None or not self.native.running:
            raise RuntimeError("native Controller is not running")
        return self.native.client.get_edata(data_id, **options)

    def invalidate_native_data(self, data_id):
        if self.native is None or not self.native.running:
            return
        for replica in self.worker_replicas(data_id):
            if replica["state"] != "available":
                continue
            try:
                self.native.client.invalidate_replica(
                    data_id, replica["replica_id"]
                )
            except NativeControllerError as exc:
                if exc.status != 5:
                    raise

    def replica_records(self, data_id):
        records = self.worker_replicas(data_id)
        native_ids = {record["replica_id"] for record in records}
        records.extend(
            replica.source_dict()
            for replica in self.state.replicas.records_for(data_id)
            if replica.replica_id not in native_ids
        )
        return records

    def replica_sources(self, data_id):
        kind, token = str(data_id).split(":", 1)
        native_records = self.worker_replicas(data_id)
        records = [
            record
            for record in native_records
            if record["state"] == "available"
        ]
        native_ids = {record["replica_id"] for record in native_records}
        for record in records:
            digest = (
                self.state.get_edata(int(token)).serialized_sha256
                if kind == "e"
                else record["content_hash"]
            )
            record["source_url"] = (
                f"{record['source_endpoint']}/data/{kind}/{int(token)}"
                f"?sha256={digest}&size={record['size']}"
            )
        records.extend(
            source
            for source in self.state.replica_sources(data_id)
            if source["replica_id"] not in native_ids
        )
        return records

    def apply_native_pruning(self, result):
        if self.native is None or not self.native.running:
            return
        for record in (*result.get("applied", ()), *result.get("deferred", ())):
            if record.get("action") not in (
                "invalidate-worker-pending-delete",
                "retiring-active-read",
            ):
                continue
            try:
                self.native.client.invalidate_replica(
                    f"i:{int(record['data_id'])}", record["replica_id"]
                )
            except NativeControllerError as exc:
                if exc.status != 5:
                    raise
        for record in result.get("cancelled", ()):
            replica_id = record.get("replica_id")
            if replica_id is None:
                continue
            try:
                self.native.client.restore_replica(
                    f"i:{int(record['data_id'])}", replica_id
                )
            except NativeControllerError as exc:
                if exc.status != 5:
                    raise

    def start(self):
        if self.native is None:
            self.native = NativeControllerCore(
                self.host,
                self.token,
                self.state.max_replicas,
                self.state.max_edata_bytes,
                self.state.native_journal_path,
            )
        self.native_address = self.native.start()
        self.state.restore_metadata(
            lambda data_id: self.native.client.get_edata(
                data_id, allow_shared=True, metadata_only=True
            ),
            self.worker_replicas,
        )
        self.native.client.mark_edata_shared(
            self.state.shared_edata_ids()
        )
        Handler = ControllerHandlerFactory.create(self)

        self._server = BoundedThreadingHTTPServer(
            (self.host, self.port),
            Handler,
            self.max_request_concurrency,
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="datavine-controller",
            daemon=False,
        )
        self._thread.start()
        return self._server.server_address

    @property
    def thread_ident(self):
        return self._thread.ident if self._thread else None

    def stop(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                raise RuntimeError("Controller thread did not stop")
        self.state.stop()
        if self.native is not None:
            self.native.stop()
        self.native_address = None
        self._server = None
        self._thread = None
