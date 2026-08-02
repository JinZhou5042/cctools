"""Scheduler-side client for the standalone Data Controller."""

import base64
import json
import re
import threading
import time
import urllib.error
import urllib.request

import hashlib

from ..models import EDataRecord, SerializationMetadata, TaskRecord
from ..protocol import (
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
        retry_base_seconds=0.01,
        retry_max_seconds=0.25,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.transient_retries = int(transient_retries)
        self.retry_base_seconds = float(retry_base_seconds)
        self.retry_max_seconds = float(retry_max_seconds)
        if self.transient_retries < 0:
            raise ValueError("transient retries cannot be negative")
        if self.retry_base_seconds < 0 or self.retry_max_seconds < 0:
            raise ValueError("retry delays cannot be negative")
        self._metrics_lock = threading.Lock()
        self._request_metrics = {}
        self._transient_retry_count = 0
        self._retry_local = threading.local()

    @property
    def transient_retry_count(self):
        with self._metrics_lock:
            return self._transient_retry_count

    @property
    def thread_transient_retry_count(self):
        return int(getattr(self._retry_local, "count", 0))

    def _open(self, request_factory):
        for retry in range(self.transient_retries + 1):
            try:
                return urllib.request.urlopen(
                    request_factory(), timeout=self.timeout
                )
            except urllib.error.HTTPError as exc:
                if (
                    exc.code not in (429, 503)
                    or retry >= self.transient_retries
                ):
                    raise
                exc.close()
            except (
                urllib.error.URLError,
                ConnectionError,
                TimeoutError,
            ):
                if retry >= self.transient_retries:
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

    def _request(self, method, path, value=None):
        started = time.monotonic()
        data = None
        headers = {TOKEN_HEADER: self.token}
        if value is not None:
            data = json.dumps(value).encode("utf-8")
            headers["Content-Type"] = "application/json"
        try:
            with self._open(
                lambda: urllib.request.Request(
                    self.endpoint + path,
                    data=data,
                    headers=headers,
                    method=method,
                )
            ) as response:
                result = response.read(), response.headers
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise DataVineRemoteError(
                f"Controller HTTP {exc.code}: {body}"
            ) from exc
        finally:
            elapsed = time.monotonic() - started
            route = re.sub(r"/\d+(?=/|$)", "/{id}", path)
            key = f"{method} {route}"
            with self._metrics_lock:
                record = self._request_metrics.setdefault(
                    key, {"count": 0, "seconds": 0.0}
                )
                record["count"] += 1
                record["seconds"] += elapsed
        return result

    def request_metrics(self):
        with self._metrics_lock:
            return {
                key: {
                    "count": value["count"],
                    "seconds": round(value["seconds"], 6),
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

    def claim_worker(self, worker_id):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/workers/claim",
            {"worker_id": str(worker_id)},
        )
        return json.loads(payload)

    def disconnect_worker(self, worker_id, epoch=1):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/workers/disconnect",
            {"worker_id": str(worker_id), "epoch": int(epoch)},
        )
        return json.loads(payload)

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
            },
        )
        return json.loads(payload)

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

    def acquire_replica(
        self,
        data_id,
        replica_id,
        generation,
        destination_worker_id,
        destination_worker_epoch=1,
    ):
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

    def acquire_observed_transfer(
        self,
        data_id,
        source_worker_id,
        destination_worker_id,
        transfer_id,
    ):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/acquire-observed",
            {
                "data_id": str(data_id),
                "source_worker_id": str(source_worker_id),
                "destination_worker_id": str(
                    destination_worker_id
                ),
                "transfer_id": str(transfer_id),
            },
        )
        return json.loads(payload)

    def release_replica(self, lease_id, success):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/replicas/release",
            {"lease_id": str(lease_id), "success": bool(success)},
        )
        return json.loads(payload)

    def confirm_replica_pruned(
        self, data_id, replica_id, generation
    ):
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
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/edata/register",
            {
                "metadata": metadata.to_dict(),
                "serialized_bytes": base64.b64encode(
                    serialized_bytes
                ).decode("ascii"),
            },
        )
        return json.loads(payload)

    def register_edata_origin(
        self, metadata, origin_path, content_hash, size
    ):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/edata/register-origin",
            {
                "metadata": metadata.to_dict(),
                "origin_path": str(origin_path),
                "content_hash": str(content_hash),
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
            metadata = SerializationMetadata.from_dict(metadata)
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
        value["metadata"] = SerializationMetadata.from_dict(
            value["metadata"]
        )
        return value

    def allocate_idata(self, producer_task_id, producer_output_index=0):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/allocate",
            {
                "producer_task_id": int(producer_task_id),
                "producer_output_index": int(producer_output_index),
            },
        )
        return json.loads(payload)["data_id"]

    def register_task(self, task):
        if not isinstance(task, TaskRecord):
            raise TypeError("task must be TaskRecord")
        payload, _ = self._request(
            "POST", f"{API_PREFIX}/tasks/register", task.to_dict()
        )
        return TaskRecord.from_dict(json.loads(payload))

    def get_task(self, task_id):
        payload, _ = self._request(
            "GET", f"{API_PREFIX}/tasks/{int(task_id)}"
        )
        return TaskRecord.from_dict(json.loads(payload))

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
            raise DataVineRemoteError(
                f"Controller HTTP {exc.code}: {body}"
            ) from exc

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
            "POST", f"{API_PREFIX}/idata/{int(data_id)}/persist", {}
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

    def invalidate_idata(self, data_id):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/{int(data_id)}/invalidate",
            {},
        )
        return json.loads(payload)
