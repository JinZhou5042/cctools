"""Standalone Data Controller HTTP service running on its own thread."""

import threading

from ndcctools.taskvine import cvine

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
        self._native_server = None
        self.native_address = None

    def snapshot(self):
        value = self.state.snapshot()
        if self._native_server is not None:
            native = cvine.vine_datavine_rpc_server_metrics_as_dict(
                self._native_server
            )
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
            }
            for key, count in additions.items():
                directory[key] += count
            directory["native"] = native
            value["native_journal"] = (
                cvine.vine_datavine_rpc_server_journal_metrics_as_dict(
                    self._native_server
                )
            )
        value["byte_serving"] = self.byte_serving.snapshot()
        value["transfer_faults"] = self.transfer_faults.snapshot()
        value["request_admission"] = (
            self._server.admission_snapshot()
            if self._server is not None
            else None
        )
        return value

    def sync_native_leases(self, data_ids):
        if self._native_server is None:
            return
        for data_id in data_ids:
            for replica in self.state.replicas.records_for(
                f"i:{int(data_id)}"
            ):
                if replica.tier not in ("worker-dram", "worker-disk"):
                    continue
                count = cvine.vine_datavine_rpc_server_replica_active_leases(
                    self._native_server,
                    "i",
                    int(data_id),
                    replica.replica_id,
                )
                if count >= 0:
                    self.state.replicas.synchronize_active_leases(
                        f"i:{int(data_id)}", replica.replica_id, count
                    )

    def start(self):
        self._native_server = cvine.vine_datavine_rpc_server_create(
            self.host,
            0,
            self.token,
            8,
            self.state.max_replicas,
            self.state.native_journal_path,
        )
        if self._native_server is None:
            raise RuntimeError("could not start native Data Controller")
        self.native_address = (
            self.host,
            cvine.vine_datavine_rpc_server_port(self._native_server),
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
        if self._native_server is not None:
            cvine.vine_datavine_rpc_server_delete(self._native_server)
            self._native_server = None
        self.native_address = None
        self._server = None
        self._thread = None
