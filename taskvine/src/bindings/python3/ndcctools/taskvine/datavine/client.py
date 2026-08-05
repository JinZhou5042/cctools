"""Shared client for the Data Controller control plane."""

import base64
import http.client
import json
import re
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import hashlib

from .codec import (
    TASK_RECORD_COMPACT_FORMAT,
    decode_compact_task_record,
    decode_serialization_metadata,
    decode_task_record,
    encode_compact_task_record,
)
from .models import EDataRecord, TaskRecord
from .native import NativeControllerClient, NativeControllerError
from .protocol import (
    API_PREFIX,
    DataVineRemoteError,
    TOKEN_HEADER,
)


class ControllerClient:
    def __init__(
        self,
        endpoint,
        token,
        timeout=30,
        transient_retries=0,
        idempotent_transient_retries=8,
        retry_base_seconds=0.01,
        retry_max_seconds=0.25,
        native_endpoint=None,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.transient_retries = int(transient_retries)
        self.idempotent_transient_retries = int(
            idempotent_transient_retries
        )
        self.retry_base_seconds = float(retry_base_seconds)
        self.retry_max_seconds = float(retry_max_seconds)
        self.native_endpoint = native_endpoint
        self.native = (
            NativeControllerClient(native_endpoint, token, timeout)
            if native_endpoint
            else None
        )
        self._native_edata_metadata = {}
        if (
            self.transient_retries < 0
            or self.idempotent_transient_retries < 0
        ):
            raise ValueError("transient retries cannot be negative")
        if self.retry_base_seconds < 0 or self.retry_max_seconds < 0:
            raise ValueError("retry delays cannot be negative")
        self._metrics_lock = threading.Lock()
        self._request_metrics = {}
        self._transient_retry_count = 0
        self._retry_local = threading.local()
        endpoint_parts = urllib.parse.urlsplit(self.endpoint)
        if endpoint_parts.scheme not in ("http", "https"):
            raise ValueError("Controller endpoint must use HTTP or HTTPS")
        self._connection_type = (
            http.client.HTTPSConnection
            if endpoint_parts.scheme == "https"
            else http.client.HTTPConnection
        )
        self._connection_host = endpoint_parts.hostname
        self._connection_port = endpoint_parts.port
        self._connection_local = threading.local()

    @property
    def transient_retry_count(self):
        with self._metrics_lock:
            return self._transient_retry_count

    @property
    def thread_transient_retry_count(self):
        return int(getattr(self._retry_local, "count", 0))

    def _open(
        self, request_factory, transient_retries=None, timeout=None
    ):
        retry_limit = (
            self.transient_retries
            if transient_retries is None
            else int(transient_retries)
        )
        for retry in range(retry_limit + 1):
            try:
                return urllib.request.urlopen(
                    request_factory(),
                    timeout=self.timeout if timeout is None else timeout,
                )
            except urllib.error.HTTPError as exc:
                if (
                    exc.code not in (429, 503)
                    or retry >= retry_limit
                ):
                    raise
                exc.close()
            except (
                urllib.error.URLError,
                ConnectionError,
                TimeoutError,
            ):
                if retry >= retry_limit:
                    raise
            with self._metrics_lock:
                self._transient_retry_count += 1
            self._retry_local.count = (
                self.thread_transient_retry_count + 1
            )
            delay = min(
                self.retry_base_seconds * (2 ** min(retry, 30)),
                self.retry_max_seconds,
            )
            if delay:
                time.sleep(delay)
        raise AssertionError("unreachable Controller retry state")

    def _request(self, method, path, value=None, idempotent=None):
        started = time.monotonic()
        data = None
        response_bytes = 0
        headers = {TOKEN_HEADER: self.token}
        if value is not None:
            data = json.dumps(value, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if idempotent is None:
            idempotent = method in ("GET", "HEAD")
        retry_limit = (
            max(
                self.transient_retries,
                self.idempotent_transient_retries,
            )
            if idempotent
            else self.transient_retries
        )
        try:
            for retry in range(retry_limit + 1):
                connection = getattr(
                    self._connection_local, "connection", None
                )
                if connection is None:
                    connection = self._connection_type(
                        self._connection_host,
                        self._connection_port,
                        timeout=self.timeout,
                    )
                    self._connection_local.connection = connection
                try:
                    connection.request(method, path, body=data, headers=headers)
                    response = connection.getresponse()
                    body = response.read()
                    status = response.status
                    result = body, response.headers
                    response_bytes = len(body)
                    if status < 400:
                        break
                    connection.close()
                    self._connection_local.connection = None
                    if status not in (429, 503) or retry >= retry_limit:
                        raise DataVineRemoteError.from_http(
                            status, body.decode("utf-8", "replace")
                        )
                except DataVineRemoteError:
                    raise
                except (
                    http.client.HTTPException,
                    OSError,
                    ConnectionError,
                    TimeoutError,
                ):
                    connection.close()
                    self._connection_local.connection = None
                    if retry >= retry_limit:
                        raise
                with self._metrics_lock:
                    self._transient_retry_count += 1
                self._retry_local.count = (
                    self.thread_transient_retry_count + 1
                )
                delay = min(
                    self.retry_base_seconds * (2 ** min(retry, 30)),
                    self.retry_max_seconds,
                )
                if delay:
                    time.sleep(delay)
            else:
                raise AssertionError("unreachable Controller retry state")
        finally:
            elapsed = time.monotonic() - started
            route = re.sub(r"/\d+(?=/|$)", "/{id}", path)
            key = f"{method} {route}"
            with self._metrics_lock:
                record = self._request_metrics.setdefault(
                    key,
                    {
                        "count": 0,
                        "seconds": 0.0,
                        "request_bytes": 0,
                        "response_bytes": 0,
                    },
                )
                record["count"] += 1
                record["seconds"] += elapsed
                record["request_bytes"] += len(data or b"")
                record["response_bytes"] += response_bytes
        return result

    def request_metrics(self):
        with self._metrics_lock:
            return {
                key: {
                    "count": value["count"],
                    "seconds": round(value["seconds"], 6),
                    "request_bytes": value["request_bytes"],
                    "response_bytes": value["response_bytes"],
                }
                for key, value in sorted(self._request_metrics.items())
            }

    def health(self):
        payload, _ = self._request("GET", f"{API_PREFIX}/health")
        return json.loads(payload)

    def snapshot(self):
        payload, _ = self._request("GET", f"{API_PREFIX}/snapshot")
        return json.loads(payload)

    def join_worker(self, worker_id, epoch=1):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/workers/join",
            {"worker_id": str(worker_id), "epoch": int(epoch)},
        )
        return json.loads(payload)

    def claim_worker(self, worker_id, endpoint=None):
        request = {"worker_id": str(worker_id)}
        if endpoint is not None:
            request["endpoint"] = str(endpoint)
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/workers/claim",
            request,
        )
        worker = json.loads(payload)
        if self.native is not None and worker.get("endpoint"):
            self.native.remember_worker(
                worker_id, worker["endpoint"], worker["epoch"]
            )
        return worker

    def configure_transfer_faults(self, **configuration):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/faults/configure",
            configuration,
        )
        return json.loads(payload)

    def transfer_fault_stats(self):
        payload, _ = self._request("GET", f"{API_PREFIX}/faults")
        return json.loads(payload)

    def claim_transfer_fault(self, transfer_id, size):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/faults/claim-transfer",
            {"transfer_id": str(transfer_id), "size": int(size)},
        )
        return json.loads(payload)

    def record_transfer_progress(
        self, transfer_id, byte_count, deferred=False
    ):
        self._request(
            "POST",
            f"{API_PREFIX}/faults/progress",
            {
                "transfer_id": str(transfer_id),
                "bytes": int(byte_count),
                "deferred": bool(deferred),
            },
        )

    def trigger_deferred_source_loss(self):
        payload, _ = self._request(
            "POST", f"{API_PREFIX}/faults/trigger", {}
        )
        return json.loads(payload)["triggered"]

    def wait_deferred_source_loss(self, transfer_id, timeout=30):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/faults/wait-trigger",
            {"transfer_id": str(transfer_id), "timeout": float(timeout)},
        )
        return json.loads(payload)["triggered"]

    def record_transfer_fault_event(self, name):
        self._request(
            "POST",
            f"{API_PREFIX}/faults/event",
            {"name": str(name)},
        )

    def claim_release_failure(self):
        payload, _ = self._request(
            "POST", f"{API_PREFIX}/faults/claim-release", {}
        )
        return json.loads(payload)["inject"]

    def complete_release_retry(self):
        self._request(
            "POST", f"{API_PREFIX}/faults/complete-release", {}
        )

    def disconnect_worker(self, worker_id, epoch=1):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/workers/disconnect",
            {"worker_id": str(worker_id), "epoch": int(epoch)},
        )
        worker = json.loads(payload)
        return worker

    def reconcile_workers(self, active_worker_ids):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/workers/reconcile",
            {
                "active_worker_ids": sorted(
                    str(worker_id) for worker_id in active_worker_ids
                )
            },
        )
        return json.loads(payload)

    def report_replica(
        self,
        data_id,
        replica_id,
        attempt,
        tier,
        content_hash,
        size,
        worker_id,
        worker_epoch=1,
    ):
        native_generation = None
        if self.native is not None:
            try:
                native_generation = self.native.report_replica(
                    data_id,
                    replica_id,
                    attempt,
                    tier,
                    content_hash,
                    size,
                    worker_id,
                    worker_epoch,
                )
            except NativeControllerError as exc:
                if exc.status != 3:
                    raise
                known_epoch = self.native.worker_epoch(worker_id)
                if known_epoch == int(worker_epoch):
                    raise DataVineRemoteError(
                        "logical data identity conflicts with replica"
                    ) from None
                if known_epoch is not None:
                    raise DataVineRemoteError("stale worker epoch") from None
                worker_epoch = int(
                    self.claim_worker(
                        worker_id,
                        self.native.worker_endpoint(worker_id),
                    )["epoch"]
                )
                native_generation = self.native.report_replica(
                    data_id,
                    replica_id,
                    attempt,
                    tier,
                    content_hash,
                    size,
                    worker_id,
                    worker_epoch,
                )
            return {
                "data_id": str(data_id),
                "replica_id": str(replica_id),
                "generation": native_generation,
                "attempt": int(attempt),
                "tier": str(tier),
                "content_hash": str(content_hash),
                "size": int(size),
                "state": "available",
                "load": 0,
                "worker_id": str(worker_id),
                "worker_epoch": int(worker_epoch),
                "source_endpoint": self.native.worker_endpoint(worker_id),
            }
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/report",
            {
                "data_id": str(data_id),
                "replica_id": str(replica_id),
                "attempt": int(attempt),
                "tier": str(tier),
                "content_hash": str(content_hash),
                "size": int(size),
                "worker_id": str(worker_id),
                "worker_epoch": int(worker_epoch),
                "source_endpoint": (
                    self.native.worker_endpoint(worker_id)
                    if self.native is not None
                    else None
                ),
            },
        )
        replica = json.loads(payload)
        return replica

    def prepare_replica(
        self,
        data_id,
        replica_id,
        attempt,
        tier,
        content_hash,
        size,
        worker_id,
        worker_epoch=1,
    ):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/prepare",
            {
                "data_id": str(data_id),
                "replica_id": str(replica_id),
                "attempt": int(attempt),
                "tier": str(tier),
                "content_hash": str(content_hash),
                "size": int(size),
                "worker_id": str(worker_id),
                "worker_epoch": int(worker_epoch),
            },
        )
        return json.loads(payload)

    def commit_replica(
        self,
        data_id,
        replica_id,
        generation,
        attempt,
        content_hash,
        size,
    ):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/commit",
            {
                "data_id": str(data_id),
                "replica_id": str(replica_id),
                "generation": int(generation),
                "attempt": int(attempt),
                "content_hash": str(content_hash),
                "size": int(size),
            },
            idempotent=True,
        )
        return json.loads(payload)

    def prepare_outputs(self, worker_id, worker_epoch, outputs):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/prepare-outputs",
            {
                "worker_id": str(worker_id),
                "worker_epoch": int(worker_epoch),
                "outputs": list(outputs),
            },
        )
        return json.loads(payload)

    def publish_outputs(
        self,
        worker_id,
        worker_epoch,
        outputs,
    ):
        if self.native is not None:
            try:
                return self.native.publish_outputs(
                    worker_id, worker_epoch, outputs
                )
            except NativeControllerError as exc:
                if exc.status != 3:
                    raise
                worker_epoch = int(
                    self.claim_worker(
                        worker_id,
                        self.native.worker_endpoint(worker_id),
                    )["epoch"]
                )
                for output in outputs:
                    output["worker_epoch"] = worker_epoch
                return self.native.publish_outputs(
                    worker_id, worker_epoch, outputs
                )
        return self.project_outputs(worker_id, worker_epoch, outputs)

    def project_outputs(self, worker_id, worker_epoch, outputs):
        return self.project_data_events(
            ((worker_id, worker_epoch, outputs),)
        )

    def project_data_events(self, output_batches, replicas=()):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/project-events",
            {
                "batches": [
                    {
                        "worker_id": str(worker_id),
                        "worker_epoch": int(worker_epoch),
                        "outputs": [
                            {
                                key: (
                                    int(str(value).split(":", 1)[-1])
                                    if key == "data_id"
                                    else value
                                )
                                for key, value in output.items()
                                if key != "payload"
                            }
                            for output in outputs
                        ],
                    }
                    for worker_id, worker_epoch, outputs in output_batches
                ],
                "replicas": list(replicas),
            },
            idempotent=True,
        )
        return json.loads(payload)

    def commit_outputs(self, outputs):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/commit-outputs",
            {"outputs": list(outputs)},
            idempotent=True,
        )
        return json.loads(payload)

    def invalidate_replica(
        self,
        data_id,
        replica_id,
        generation,
        worker_id,
        worker_epoch=1,
    ):
        if self.native is not None:
            try:
                self.native.invalidate_replica(data_id, replica_id)
            except NativeControllerError as exc:
                if exc.status != 5:
                    raise
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/invalidate",
            {
                "data_id": str(data_id),
                "replica_id": str(replica_id),
                "generation": int(generation),
                "worker_id": str(worker_id),
                "worker_epoch": int(worker_epoch),
            },
        )
        return json.loads(payload)

    def invalidate_observed_replica(
        self,
        data_id,
        replica_id,
        attempt,
        content_hash,
        size,
        worker_id,
        worker_epoch=1,
    ):
        if self.native is not None:
            try:
                self.native.invalidate_replica(data_id, replica_id)
            except NativeControllerError as exc:
                if exc.status != 5:
                    raise
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/invalidate-observed",
            {
                "data_id": str(data_id),
                "replica_id": str(replica_id),
                "attempt": int(attempt),
                "content_hash": str(content_hash),
                "size": int(size),
                "worker_id": str(worker_id),
                "worker_epoch": int(worker_epoch),
            },
        )
        return json.loads(payload)

    def replica_sources(self, data_id):
        kind, token = str(data_id).split(":", 1)
        payload, _ = self._request(
            "GET",
            f"{API_PREFIX}/replicas/{kind}/{int(token)}/sources",
        )
        return json.loads(payload)

    def replica_records(self, data_id):
        kind, token = str(data_id).split(":", 1)
        payload, _ = self._request(
            "GET",
            f"{API_PREFIX}/replicas/{kind}/{int(token)}/records",
        )
        return json.loads(payload)

    def resolve_worker_source(
        self,
        data_id,
        destination_worker_id,
        transfer_id,
        excluded_worker_ids=(),
        allow_local_source=False,
        destination_worker_epoch=1,
    ):
        if self.native is not None and not allow_local_source:
            try:
                resolved = self.native.resolve_source(
                    data_id,
                    destination_worker_id,
                    destination_worker_epoch,
                    transfer_id,
                    next(iter(excluded_worker_ids), None),
                )
            except NativeControllerError as exc:
                if exc.status == 5:
                    raise DataVineRemoteError(
                        "no available worker source"
                    ) from None
                if exc.status == 3:
                    raise DataVineRemoteError(
                        "transfer identity already completed or source "
                        "request rejected"
                    ) from None
                raise
            return resolved
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/resolve-source",
            {
                "data_id": str(data_id),
                "destination_worker_id": str(destination_worker_id),
                "transfer_id": str(transfer_id),
                "excluded_worker_ids": [
                    str(worker_id)
                    for worker_id in excluded_worker_ids
                ],
                "allow_local_source": bool(allow_local_source),
            },
        )
        return json.loads(payload)

    def acquire_replica(
        self,
        data_id,
        replica_id,
        generation,
        destination_worker_id,
        destination_worker_epoch=1,
    ):
        if self.native is not None:
            transfer_id = f"taskvine:acquire-{uuid.uuid4().hex}"
            resolved = self.native.resolve_source(
                data_id,
                destination_worker_id,
                destination_worker_epoch,
                transfer_id,
            )
            source = resolved["source"]
            if (
                source["replica_id"] != str(replica_id)
                or source["generation"] != int(generation)
            ):
                self.native.release_source(transfer_id, False)
                raise DataVineRemoteError(
                    "native Controller selected a different source"
                )
            return resolved["lease"]
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/acquire",
            {
                "data_id": str(data_id),
                "replica_id": str(replica_id),
                "generation": int(generation),
                "destination_worker_id": str(destination_worker_id),
                "destination_worker_epoch": int(
                    destination_worker_epoch
                ),
            },
        )
        return json.loads(payload)

    def release_replica(self, lease_id, success):
        if self.native is not None:
            try:
                self.native.release_source(lease_id, success)
                return {
                    "lease_id": str(lease_id),
                    "active": False,
                    "success": bool(success),
                }
            except NativeControllerError as exc:
                if exc.status == 3:
                    raise DataVineRemoteError(
                        "conflicting duplicate lease release"
                    ) from None
                if exc.status != 5:
                    raise
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/release",
            {"lease_id": str(lease_id), "success": bool(success)},
        )
        return json.loads(payload)

    def release_native_source(self, lease_id, success):
        if self.native is None:
            raise RuntimeError("native Controller is required")
        self.native.release_source(lease_id, success)

    def confirm_replica_pruned(
        self, data_id, replica_id, generation
    ):
        if self.native is not None:
            try:
                self.native.confirm_replica_pruned(data_id, replica_id)
            except NativeControllerError as exc:
                if exc.status != 5:
                    raise
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/pruned",
            {
                "data_id": str(data_id),
                "replica_id": str(replica_id),
                "generation": int(generation),
            },
        )
        return json.loads(payload)

    def set_task_state(self, task_id, state):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/pruning/task-state",
            {"task_id": int(task_id), "state": str(state)},
        )
        return json.loads(payload)

    def set_task_states(self, task_ids, state):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/pruning/task-states",
            {
                "task_ids": [int(task_id) for task_id in task_ids],
                "state": str(state),
            },
        )
        return json.loads(payload)

    def set_required_output(self, data_id, required=True):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/pruning/required-output",
            {"data_id": int(data_id), "required": bool(required)},
        )
        return json.loads(payload)

    def pruning_plan(self):
        payload, _ = self._request(
            "GET", f"{API_PREFIX}/pruning/plan"
        )
        return json.loads(payload)

    def apply_pruning(
        self,
        graph_revision,
        state_revision,
        grace_seconds=60,
        data_ids=None,
        now=None,
    ):
        request = {
            "graph_revision": int(graph_revision),
            "state_revision": int(state_revision),
            "grace_seconds": float(grace_seconds),
        }
        if data_ids is not None:
            request["data_ids"] = [
                int(data_id) for data_id in data_ids
            ]
        if now is not None:
            request["now"] = float(now)
        payload, _ = self._request(
            "POST", f"{API_PREFIX}/pruning/apply", request
        )
        return json.loads(payload)

    def continue_deferred_pruning(self, operation_id, data_ids=None):
        request = {"operation_id": str(operation_id)}
        if data_ids is not None:
            request["data_ids"] = [
                int(data_id) for data_id in data_ids
            ]
        payload, _ = self._request(
            "POST", f"{API_PREFIX}/pruning/continue", request
        )
        return json.loads(payload)

    def restore_quarantined(self, data_id):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/pruning/restore",
            {"data_id": int(data_id)},
        )
        return json.loads(payload)

    def hard_delete_quarantined(
        self, graph_revision, state_revision, now=None
    ):
        request = {
            "graph_revision": int(graph_revision),
            "state_revision": int(state_revision),
        }
        if now is not None:
            request["now"] = float(now)
        payload, _ = self._request(
            "POST", f"{API_PREFIX}/pruning/hard-delete", request
        )
        return json.loads(payload)

    def register_edata(self, metadata, serialized_bytes):
        return self.register_edata_batch(((metadata, serialized_bytes),))[0]

    def register_edata_batch(self, values):
        values = tuple(values)
        if self.native is None:
            raise RuntimeError("native Data Controller is required")
        prepared = tuple(
            (
                metadata,
                bytes(value),
                EDataRecord.digest(metadata, value),
                hashlib.sha256(value).hexdigest(),
            )
            for metadata, value in values
        )
        data_ids = self.native.register_edata(
            (
                metadata.to_dict(),
                content_hash,
                serialized_sha256,
                value,
                len(value),
            )
            for metadata, value, content_hash, serialized_sha256 in prepared
        )
        metadata_table = tuple(dict.fromkeys(
            metadata for metadata, _, _, _ in prepared
        ))
        metadata_index = {
            metadata: index
            for index, metadata in enumerate(metadata_table)
        }
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/edata/project-batch",
            {
                "metadata": [
                    metadata.to_dict() for metadata in metadata_table
                ],
                "values": [
                    {
                        "data_id": data_id,
                        "metadata": metadata_index[metadata],
                        "content_hash": content_hash,
                        "serialized_sha256": serialized_sha256,
                        "size": len(value),
                    }
                    for data_id, (
                        metadata,
                        value,
                        content_hash,
                        serialized_sha256,
                    ) in zip(data_ids, prepared)
                ]
            },
        )
        acknowledgement = json.loads(payload)
        if acknowledgement.get("registered") != len(prepared):
            raise DataVineRemoteError(
                "Controller returned incomplete EData projection"
            )
        return tuple(
            {
                "data_id": int(data_id),
                "content_hash": content_hash,
                "serialized_sha256": serialized_sha256,
                "size": len(value),
                "storage": "native-memory",
            }
            for data_id, (
                _, value, content_hash, serialized_sha256
            ) in zip(data_ids, prepared)
        )

    def finalize_native_edata(self, shared_data_ids=()):
        if self.native is not None:
            self.native.mark_edata_shared(shared_data_ids)

    def register_edata_origin(
        self,
        metadata,
        origin_path,
        content_hash,
        serialized_sha256,
        size,
    ):
        if self.native is None:
            raise RuntimeError("native Data Controller is required")
        data_id = self.native.register_edata(((
            metadata.to_dict(),
            str(content_hash),
            serialized_sha256,
            None,
            int(size),
        ),))[0]
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/edata/register-origin",
            {
                "data_id": data_id,
                "metadata": metadata.to_dict(),
                "origin_path": str(origin_path),
                "content_hash": str(content_hash),
                "serialized_sha256": serialized_sha256,
                "size": int(size),
            },
        )
        return json.loads(payload)

    def fetch_edata(self, data_id, metadata):
        payload, headers = self._request(
            "GET", f"{API_PREFIX}/edata/{int(data_id)}"
        )
        actual = EDataRecord.digest(metadata, payload)
        expected = headers.get("X-DataVine-SHA256")
        if actual != expected:
            raise DataVineRemoteError(
                f"EDataID {data_id} checksum mismatch"
            )
        return payload

    def fetch_edata_record(self, data_id):
        payload, headers = self._request(
            "GET", f"{API_PREFIX}/edata/{int(data_id)}"
        )
        try:
            metadata = json.loads(
                base64.urlsafe_b64decode(
                    headers["X-DataVine-Metadata"]
                ).decode("utf-8")
            )
            metadata = decode_serialization_metadata(metadata)
        except Exception as exc:
            raise DataVineRemoteError(
                f"EDataID {data_id} has invalid metadata"
            ) from exc
        actual = EDataRecord.digest(metadata, payload)
        if actual != headers.get("X-DataVine-SHA256"):
            raise DataVineRemoteError(
                f"EDataID {data_id} checksum mismatch"
            )
        return metadata, payload

    def get_edata_metadata(self, data_id):
        payload, _ = self._request(
            "GET", f"{API_PREFIX}/edata/{int(data_id)}/metadata"
        )
        value = json.loads(payload)
        value["metadata"] = decode_serialization_metadata(
            value["metadata"]
        )
        return value

    def resolve_edata_source(
        self,
        data_id,
        destination_worker_id,
        transfer_id,
        excluded_worker_ids=(),
        allow_peer_transfer=True,
    ):
        payload, headers = self._request(
            "POST",
            f"{API_PREFIX}/edata/resolve-source",
            {
                "data_id": int(data_id),
                "destination_worker_id": str(destination_worker_id),
                "transfer_id": str(transfer_id),
                "excluded_worker_ids": [
                    str(worker_id) for worker_id in excluded_worker_ids
                ],
                "allow_peer_transfer": bool(allow_peer_transfer),
            },
        )
        if headers.get_content_type() == "application/octet-stream":
            try:
                value = {
                    "data_id": int(headers["X-DataVine-Data-ID"]),
                    "content_hash": headers[
                        "X-DataVine-Content-SHA256"
                    ],
                    "serialized_sha256": headers[
                        "X-DataVine-Serialized-SHA256"
                    ],
                    "size": len(payload),
                    "metadata": decode_serialization_metadata(
                        json.loads(
                            base64.urlsafe_b64decode(
                                headers["X-DataVine-Metadata"]
                            )
                        )
                    ),
                    "cache_globally": (
                        headers["X-DataVine-Cache-Globally"] == "1"
                    ),
                    "source_type": "controller-memory",
                    "payload": payload,
                }
            except Exception as exc:
                raise DataVineRemoteError(
                    f"EDataID {data_id} has invalid source metadata"
                ) from exc
            return value
        value = json.loads(payload)
        value["metadata"] = decode_serialization_metadata(
            value["metadata"]
        )
        return value

    def resolve_edata_sources(self, requests):
        requests = tuple(requests)
        if self.native is None:
            return self._resolve_edata_sources_http(requests)
        results = [None] * len(requests)
        misses = []
        for index, request in enumerate(requests):
            try:
                edata = self.native.get_edata(request["data_id"])
            except NativeControllerError as exc:
                if exc.status != 5:
                    raise
            else:
                results[index] = {
                    "data_id": int(request["data_id"]),
                    "content_hash": edata["content_hash"],
                    "serialized_sha256": edata["serialized_sha256"],
                    "size": len(edata["payload"]),
                    "metadata": None,
                    "cache_globally": False,
                    "source_type": "controller-memory",
                    "payload": edata["payload"],
                }
                continue
            if not request.get("allow_peer_transfer", True):
                misses.append((index, request))
                continue
            try:
                resolved = self.native.resolve_source(
                    f"e:{int(request['data_id'])}",
                    request["destination_worker_id"],
                    request["destination_worker_epoch"],
                    request["transfer_id"],
                    next(iter(request.get("excluded_worker_ids", ())), None),
                )
            except NativeControllerError as exc:
                if exc.status == 5:
                    misses.append((index, request))
                    continue
                if exc.status != 3:
                    raise
                epoch = int(
                    self.claim_worker(
                        request["destination_worker_id"],
                        self.native.worker_endpoint(
                            request["destination_worker_id"]
                        ),
                    )["epoch"]
                )
                try:
                    resolved = self.native.resolve_source(
                        f"e:{int(request['data_id'])}",
                        request["destination_worker_id"],
                        epoch,
                        request["transfer_id"],
                        next(
                            iter(request.get("excluded_worker_ids", ())),
                            None,
                        ),
                    )
                except NativeControllerError as retry:
                    if retry.status == 5:
                        misses.append((index, request))
                        continue
                    if retry.status != 3:
                        raise
                    misses.append((index, request))
                    continue
            data_id = int(request["data_id"])
            with self._metrics_lock:
                info = self._native_edata_metadata.get(data_id)
            if info is None:
                info = self.get_edata_metadata(data_id)
                with self._metrics_lock:
                    self._native_edata_metadata[data_id] = info
            source = resolved["source"]
            parsed = urllib.parse.urlsplit(source["source_url"])
            query = urllib.parse.parse_qs(parsed.query)
            query["sha256"] = [info["serialized_sha256"]]
            source["source_url"] = urllib.parse.urlunsplit(
                parsed._replace(
                    query=urllib.parse.urlencode(query, doseq=True)
                )
            )
            results[index] = {
                "data_id": data_id,
                "content_hash": info["content_hash"],
                "serialized_sha256": info["serialized_sha256"],
                "size": info["size"],
                "metadata": info["metadata"],
                "cache_globally": True,
                "source_type": "peer",
                **resolved,
            }
        if misses:
            fallback = self._resolve_edata_sources_http(
                request for _, request in misses
            )
            for (index, _), value in zip(misses, fallback):
                results[index] = value
        return results

    def _resolve_edata_sources_http(self, requests):
        requests = tuple(requests)
        payload, headers = self._request(
            "POST",
            f"{API_PREFIX}/edata/resolve-sources",
            {"requests": list(requests)},
        )
        if (
            headers.get_content_type()
            != "application/x-datavine-resolve-batch"
            or len(payload) < 4
        ):
            raise DataVineRemoteError("invalid EData batch response")
        header_length = struct.unpack("!I", payload[:4])[0]
        header_end = 4 + header_length
        try:
            values = json.loads(payload[4:header_end])
        except Exception as exc:
            raise DataVineRemoteError(
                "invalid EData batch metadata"
            ) from exc
        offset = header_end
        for value in values:
            value["metadata"] = decode_serialization_metadata(
                value["metadata"]
            )
            length = int(value.pop("payload_length"))
            if length:
                value["payload"] = payload[offset:offset + length]
                offset += length
        if offset != len(payload) or len(values) != len(requests):
            raise DataVineRemoteError("invalid EData batch framing")
        return values

    def allocate_idata(self, producer_task_id, producer_output_index=0):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/allocate",
            {
                "producer_task_id": int(producer_task_id),
                "producer_output_index": int(producer_output_index),
            },
        )
        data_id = json.loads(payload)["data_id"]
        if self.native is not None:
            self.native.allocate_idata(
                ((data_id, producer_task_id, producer_output_index),)
            )
        return data_id

    def allocate_idata_batch(self, producer_slots):
        producer_slots = tuple(producer_slots)
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/allocate-batch",
            {
                "producer_slots": [
                    [int(task_id), int(output_index)]
                    for task_id, output_index in producer_slots
                ]
            },
        )
        data_ids = tuple(json.loads(payload)["data_ids"])
        if self.native is not None:
            self.native.allocate_idata(
                (
                    (data_id, task_id, output_index)
                    for data_id, (task_id, output_index) in zip(
                        data_ids, producer_slots
                    )
                )
            )
        return data_ids

    def register_task(self, task):
        if not isinstance(task, TaskRecord):
            raise TypeError("task must be TaskRecord")
        payload, _ = self._request(
            "POST", f"{API_PREFIX}/tasks/register", task.to_dict()
        )
        return decode_task_record(json.loads(payload))

    def register_tasks(self, tasks):
        tasks = tuple(tasks)
        if any(not isinstance(task, TaskRecord) for task in tasks):
            raise TypeError("tasks must contain TaskRecord values")
        request = {
            "task_record_format": TASK_RECORD_COMPACT_FORMAT,
            "tasks": [encode_compact_task_record(task) for task in tasks],
            "bounded_acknowledgement": True,
        }
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/tasks/register-batch",
            request,
        )
        acknowledgement = json.loads(payload)
        if acknowledgement.get("registered") != len(tasks):
            raise DataVineRemoteError(
                "Controller returned an invalid task registration "
                "acknowledgement"
            )
        return tasks

    def get_task(self, task_id):
        payload, _ = self._request(
            "GET", f"{API_PREFIX}/tasks/{int(task_id)}"
        )
        return decode_task_record(json.loads(payload))

    def get_tasks(self, task_ids):
        return self._get_tasks(task_ids, False)[0]

    def get_execution_bundle(self, task_ids):
        return self._get_tasks(task_ids, True)

    def _get_tasks(self, task_ids, include_cache_values):
        task_ids = tuple(int(task_id) for task_id in task_ids)
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/tasks/get-batch",
            {
                "task_ids": task_ids,
                "include_cache_values": bool(include_cache_values),
            },
            idempotent=True,
        )
        response = json.loads(payload)
        if response.get("task_record_format") != TASK_RECORD_COMPACT_FORMAT:
            raise DataVineRemoteError("invalid task record batch format")
        records = tuple(
            decode_compact_task_record(value)
            for value in response["tasks"]
        )
        if tuple(record.task_id for record in records) != task_ids:
            raise DataVineRemoteError("mismatched task record batch")
        return records, response.get("cache_values", {})

    def fetch_idata(self, data_id):
        payload, headers = self._request(
            "GET", f"{API_PREFIX}/idata/{int(data_id)}"
        )
        expected = headers.get("X-DataVine-SHA256")
        if hashlib.sha256(payload).hexdigest() != expected:
            raise DataVineRemoteError(
                f"IDataID {data_id} checksum mismatch"
            )
        return payload

    def fetch_idata_stream(self, data_id):
        request = urllib.request.Request(
            self.endpoint + f"{API_PREFIX}/idata/{int(data_id)}",
            headers={TOKEN_HEADER: self.token},
            method="GET",
        )
        return self._open(lambda: request)

    def fetch_source(self, source_url, content_hash, size):
        request = urllib.request.Request(str(source_url), method="GET")
        with self._open(
            lambda: request,
            transient_retries=0,
            timeout=min(self.timeout, 2),
        ) as response:
            payload = response.read()
        if (
            len(payload) != int(size)
            or hashlib.sha256(payload).hexdigest() != str(content_hash)
        ):
            raise DataVineRemoteError("peer source checksum mismatch")
        return payload

    def open_source(self, source_url):
        request = urllib.request.Request(str(source_url), method="GET")
        return self._open(
            lambda: request,
            transient_retries=0,
            timeout=min(self.timeout, 2),
        )

    def prune_source(self, source_url):
        request = urllib.request.Request(str(source_url), method="DELETE")
        try:
            with self._open(
                lambda: request,
                transient_retries=0,
                timeout=min(self.timeout, 2),
            ) as response:
                return response.status == 204
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return False
            raise

    def kill_source(self, source_url):
        parsed = urllib.parse.urlsplit(str(source_url))
        prefix, separator, _ = parsed.path.rpartition("/data/")
        if not separator:
            raise ValueError("worker source URL lacks a data path")
        target = urllib.parse.urlunsplit(
            parsed._replace(path=f"{prefix}/kill", query="")
        )
        request = urllib.request.Request(target, method="DELETE")
        with self._open(
            lambda: request,
            transient_retries=0,
            timeout=min(self.timeout, 2),
        ) as response:
            if response.status != 204:
                raise DataVineRemoteError("worker source kill was rejected")
        time.sleep(0.05)

    def publish_idata(self, data_id, attempt, serialized_bytes):
        headers = {
            TOKEN_HEADER: self.token,
            "Content-Type": "application/octet-stream",
            "X-DataVine-Attempt": str(int(attempt)),
        }
        try:
            with self._open(
                lambda: urllib.request.Request(
                    self.endpoint
                    + f"{API_PREFIX}/idata/{int(data_id)}/publish",
                    data=serialized_bytes,
                    headers=headers,
                    method="POST",
                )
            ) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise DataVineRemoteError.from_http(exc.code, body) from exc

    def publish_idata_batch(self, publications):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/publish-batch",
            {
                "publications": [
                    {
                        "data_id": int(data_id),
                        "attempt": int(attempt),
                        "payload": base64.b64encode(serialized).decode(
                            "ascii"
                        ),
                    }
                    for data_id, attempt, serialized in publications
                ]
            },
        )
        return json.loads(payload)

    def publish_idata_metadata(
        self, data_id, attempt, content_hash, serialized_size
    ):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/{int(data_id)}/publish-metadata",
            {
                "attempt": int(attempt),
                "content_hash": str(content_hash),
                "size": int(serialized_size),
            },
        )
        return json.loads(payload)

    def persist_idata(self, data_id):
        payload, _ = self._request(
            "POST", f"{API_PREFIX}/idata/{int(data_id)}/persist"
        )
        return json.loads(payload)

    def begin_external_persistence(self, data_id, request_id):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/{int(data_id)}/persist/begin",
            {"request_id": str(request_id)},
        )
        return json.loads(payload)

    def complete_external_persistence(self, data_id, request_id):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/{int(data_id)}/persist/complete",
            {"request_id": str(request_id)},
        )
        return json.loads(payload)

    def fail_external_persistence(
        self, data_id, request_id, error
    ):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/{int(data_id)}/persist/fail",
            {
                "request_id": str(request_id),
                "error": str(error),
            },
        )
        return json.loads(payload)

    def cancel_persistence(self, data_id, reason="obsolete"):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/{int(data_id)}/persist/cancel",
            {"reason": str(reason)},
        )
        return json.loads(payload)

    def idata_status(self, data_id):
        payload, _ = self._request(
            "GET", f"{API_PREFIX}/idata/{int(data_id)}/status"
        )
        return json.loads(payload)

    def idata_status_batch(self, data_ids):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/status-batch",
            {"data_ids": [int(data_id) for data_id in data_ids]},
        )
        return json.loads(payload)

    def invalidate_idata(self, data_id):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/{int(data_id)}/invalidate",
        )
        return json.loads(payload)
