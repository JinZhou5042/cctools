"""Worker-side EData/IData fetching and binding resolution."""

import cloudpickle
import copy
import hashlib
import json
from pathlib import Path
import time
import urllib.parse
import uuid

from ..models import EDataRecord
from ..workflow import iter_output_refs


class _InjectedCorruption(IOError):
    pass


class InputResolver:
    def __init__(
        self,
        controller,
        token,
        client,
        reporter,
        process_cache,
        emit,
        source_resolver,
        cache_values=None,
        allow_peer_transfer=True,
        transfer_faults=False,
    ):
        self.controller = controller
        self.token = token
        self.client = client
        self.reporter = reporter
        self.process_cache = process_cache
        self.emit = emit
        self.source_resolver = source_resolver
        self.objects = {}
        self.cache_values = cache_values or {}
        self.allow_peer_transfer = bool(allow_peer_transfer)
        self.transfer_faults = bool(transfer_faults)
        self._corrupt_fallback_pending = False

    def _fetch_source(self, source_url, content_hash, size, transfer_id):
        fault = (
            self.client.claim_transfer_fault(transfer_id, size)
            if self.transfer_faults
            else {"action": "none"}
        )
        action = fault["action"]
        try:
            if action == "source-loss":
                self.client.kill_source(source_url)
                raise IOError("injected worker-direct source loss")
            if action == "source-loss-after-bytes":
                threshold = int(fault["bytes"])
                received = 0
                with self.client.open_source(source_url) as response:
                    while received < threshold:
                        chunk = response.read(threshold - received)
                        if not chunk:
                            break
                        received += len(chunk)
                    if received < threshold or received >= int(size):
                        raise IOError(
                            "source cannot reach partial-loss threshold"
                        )
                    self.client.record_transfer_progress(
                        transfer_id,
                        received,
                        fault.get("deferred", False),
                    )
                    if fault.get("deferred") and not (
                        self.client.wait_deferred_source_loss(transfer_id)
                    ):
                        raise TimeoutError(
                            "deferred worker source loss was not triggered"
                        )
                    self.client.kill_source(source_url)
                raise IOError("injected partial worker-direct source loss")
            if action == "corrupt":
                parsed = urllib.parse.urlsplit(str(source_url))
                query = urllib.parse.parse_qs(parsed.query)
                query["fault"] = ["corrupt"]
                source_url = urllib.parse.urlunsplit(
                    parsed._replace(
                        query=urllib.parse.urlencode(query, doseq=True)
                    )
                )
            payload = self.client.fetch_source(
                source_url, content_hash, size
            )
        except Exception as exc:
            if self.transfer_faults and action == "corrupt":
                self.client.record_transfer_fault_event(
                    "corruption-rejected"
                )
                self._corrupt_fallback_pending = True
                raise _InjectedCorruption(
                    "injected worker-direct transfer corruption"
                ) from exc
            if (
                self.transfer_faults
                and action == "source-loss-after-bytes"
            ):
                self.client.record_transfer_fault_event("partial-cleanup")
            raise
        if self._corrupt_fallback_pending:
            self.client.record_transfer_fault_event(
                "alternate-source-fallback"
            )
            self._corrupt_fallback_pending = False
        return payload

    def _record_fallback(self):
        if not self._corrupt_fallback_pending:
            return
        self.client.record_transfer_fault_event(
            "alternate-source-fallback"
        )
        self._corrupt_fallback_pending = False

    def _release(self, lease_id, success):
        if (
            success
            and self.transfer_faults
            and self.client.claim_release_failure()
        ):
            self.client.defer_native_release(lease_id)
            self.emit(
                "DATAVINE_PEER_RELEASE_PENDING "
                + json.dumps(
                    {"lease_id": str(lease_id)},
                    separators=(",", ":"),
                )
            )
            return
        self.client.release_replica(lease_id, success)

    def fetch_edata(self, data_id):
        data_id = int(data_id)
        with self.process_cache.fetch_lock(
            (self.controller, self.token, "e", data_id)
        ):
            return self._fetch_edata_locked(data_id)

    def _fetch_edata_locked(self, data_id):
        data_key = f"e:{data_id}"
        with self.process_cache.lock:
            payload = self.process_cache.data.get_local_data(
                self.controller,
                self.token,
                data_key,
            )
            if payload is None:
                payload = self.process_cache.disk.get_local_data(
                    self.controller, self.token, data_key
                )
        if payload is not None:
            self.emit(f"DATAVINE_LOCAL_HIT e{data_id}")
            return payload
        excluded = []
        while True:
            resolved = self.source_resolver.resolve(
                {
                    "data_id": data_id,
                    "destination_worker_id": self.reporter.worker_id,
                    "destination_worker_epoch": self.reporter.worker_epoch,
                    "transfer_id": f"taskvine:{uuid.uuid4().hex}",
                    "excluded_worker_ids": list(excluded),
                    "allow_peer_transfer": self.allow_peer_transfer,
                }
            )
            source_type = resolved["source_type"]
            if source_type != "peer":
                break
            source = resolved["source"]
            lease_id = resolved["lease"]["lease_id"]
            try:
                payload = self._fetch_source(
                    source["source_url"],
                    resolved["serialized_sha256"],
                    resolved["size"],
                    lease_id,
                )
            except Exception as exc:
                self._release(lease_id, False)
                if not isinstance(exc, _InjectedCorruption):
                    try:
                        self.client.invalidate_observed_replica(
                            source["data_id"],
                            source["replica_id"],
                            source["attempt"],
                            source["content_hash"],
                            source["size"],
                            source["worker_id"],
                            source["worker_epoch"],
                        )
                    except Exception:
                        pass
                excluded.append(source["worker_id"])
                continue
            self._release(lease_id, True)
            self.emit(
                f"DATAVINE_PEER_FETCH {data_key} "
                f"source={source['worker_id']}"
            )
            break
        if source_type == "peer":
            pass
        elif source_type == "controller-memory":
            payload = resolved["payload"]
        elif source_type == "sharedfs":
            payload = Path(resolved["origin_path"]).read_bytes()
            self.emit(f"DATAVINE_BULK_ORIGIN e{data_id}")
        else:
            raise RuntimeError(f"invalid EData source type {source_type}")
        self._record_fallback()
        if (
            len(payload) != resolved["size"]
            or hashlib.sha256(payload).hexdigest()
            != resolved["serialized_sha256"]
            or EDataRecord.digest(resolved["metadata"], payload)
            != resolved["content_hash"]
        ):
            raise RuntimeError(f"EDataID {data_id} checksum mismatch")
        self.process_cache.edata_metadata[
            (self.controller, self.token, data_id)
        ] = resolved
        hint = self.cache_values.get(data_key)
        with self.process_cache.lock:
            self.process_cache.data.put_data(
                self.controller,
                self.token,
                data_key,
                resolved["serialized_sha256"],
                payload,
                hint["score"] if hint is not None else None,
            )
            self.process_cache.disk.put_data(
                self.controller,
                self.token,
                data_key,
                resolved["serialized_sha256"],
                payload,
            )
        if resolved["cache_globally"]:
            self.reporter.report_local(
                data_key,
                1,
                resolved["content_hash"],
                payload,
                tier="worker-disk",
            )
        return payload

    def _fetch_peer(self, data_key):
        excluded = []
        for _ in range(2):
            transfer_id = f"taskvine:{uuid.uuid4().hex}"
            try:
                resolved = self.client.resolve_worker_source(
                    data_key,
                    self.reporter.worker_id,
                    transfer_id,
                    excluded,
                )
            except Exception as exc:
                self.emit(
                    f"DATAVINE_PEER_RESOLVE_FAILED {data_key} "
                    f"{type(exc).__name__}: {exc}"
                )
                return None, None
            source = resolved["source"]
            source_url = source.get("source_url")
            if not source_url:
                self.client.release_replica(
                    resolved["lease"]["lease_id"], False
                )
                return None, None
            try:
                payload = self._fetch_source(
                    source_url,
                    source["content_hash"],
                    source["size"],
                    transfer_id,
                )
            except Exception as exc:
                self.emit(
                    f"DATAVINE_PEER_FETCH_FAILED {data_key} "
                    f"source={source.get('worker_id')} "
                    f"{type(exc).__name__}: {exc}"
                )
                self._release(resolved["lease"]["lease_id"], False)
                if not isinstance(exc, _InjectedCorruption):
                    try:
                        self.client.invalidate_observed_replica(
                            source["data_id"],
                            source["replica_id"],
                            source["attempt"],
                            source["content_hash"],
                            source["size"],
                            source["worker_id"],
                            source["worker_epoch"],
                        )
                    except Exception:
                        pass
                excluded.append(source["worker_id"])
                continue
            self._release(resolved["lease"]["lease_id"], True)
            self.emit(
                f"DATAVINE_PEER_FETCH {data_key} "
                f"source={source['worker_id']}"
            )
            return payload, source
        return None, None

    def resolve(self, binding):
        kind, data_id = binding
        key = (kind, data_id)
        if key in self.objects:
            return self.objects[key]
        if kind == "e":
            payload = self.fetch_edata(data_id)
        elif kind == "c":
            return self._resolve_container(key, data_id)
        elif kind == "i":
            payload = self._fetch_idata(data_id)
        else:
            raise ValueError(f"unknown binding kind {kind}")
        self.objects[key] = cloudpickle.loads(payload)
        return self.objects[key]

    def _resolve_container(self, key, data_id):
        template = cloudpickle.loads(self.fetch_edata(data_id))
        memo = {}
        for reference in iter_output_refs(template):
            producer = self._producer_task(reference.producer_task_id)
            memo[id(reference)] = self.resolve(
                (
                    "i",
                    producer.output_data_ids[reference.output_index],
                )
            )
        self.objects[key] = copy.deepcopy(template, memo)
        return self.objects[key]

    def _producer_task(self, task_id):
        task_id = int(task_id)
        producer_key = (self.controller, self.token, task_id)
        producer = self.process_cache.task_records.get(producer_key)
        if producer is None:
            producer = self.client.get_task(task_id)
            self.process_cache.task_records[producer_key] = producer
        return producer

    def _fetch_idata(self, data_id):
        data_id = int(data_id)
        with self.process_cache.fetch_lock(
            (self.controller, self.token, "i", data_id)
        ):
            return self._fetch_idata_locked(data_id)

    def _fetch_idata_locked(self, data_id):
        data_key = f"i:{data_id}"
        with self.process_cache.lock:
            payload = self.process_cache.data.get_local_data(
                self.controller,
                self.token,
                data_key,
            )
            if payload is None:
                payload = self.process_cache.disk.get_local_data(
                    self.controller, self.token, data_key
                )
        if payload is not None:
            self.emit(f"DATAVINE_LOCAL_HIT i{data_id}")
            self.emit(f"DATAVINE_LOCAL_IDATA i{data_id}")
            return payload
        status = None
        payload = None
        if self.allow_peer_transfer:
            payload, status = self._fetch_peer(data_key)
        if status is None:
            status = self.client.idata_status(data_id)
        if (
            payload is None
            and not self.allow_peer_transfer
            and status["durability"] != "durable"
            and not status["controller_inline"]
            and status.get("persistence_request")
        ):
            deadline = time.monotonic() + 30
            delay = 0.01
            while time.monotonic() < deadline:
                time.sleep(delay)
                status = self.client.idata_status(data_id)
                if (
                    status["durability"] == "durable"
                    or status["controller_inline"]
                    or not status.get("persistence_request")
                ):
                    break
                delay = min(delay * 2, 0.25)
        if payload is None:
            if status["durability"] == "durable":
                payload = Path(status["durable_path"]).read_bytes()
                if (
                    len(payload) != status["size"]
                    or hashlib.sha256(payload).hexdigest()
                    != status["content_hash"]
                ):
                    raise IOError(f"durable IDataID {data_id} is corrupt")
                self.emit(f"DATAVINE_SHAREDFS_FETCH i{data_id}")
            elif status["controller_inline"]:
                payload = self.client.fetch_idata(data_id)
            else:
                raise RuntimeError(f"IData is not available: i:{data_id}")
        self._record_fallback()
        hint = self.cache_values.get(data_key)
        with self.process_cache.lock:
            self.process_cache.data.put_data(
                self.controller,
                self.token,
                data_key,
                status["content_hash"],
                payload,
                hint["score"] if hint is not None else None,
            )
            self.process_cache.disk.put_data(
                self.controller,
                self.token,
                data_key,
                status["content_hash"],
                payload,
            )
        self.reporter.report_local(
            data_key,
            status["attempt"],
            status["content_hash"],
            payload,
            tier="worker-disk",
        )
        return payload
