"""Worker-side EData/IData fetching and binding resolution."""

import cloudpickle
import copy
import hashlib
from pathlib import Path
import uuid

from ..models import EDataRecord
from ..workflow import iter_output_refs


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
        if payload is not None:
            self.emit(f"DATAVINE_DRAM_HIT e{data_id}")
            return payload
        excluded = []
        while True:
            resolved = self.source_resolver.resolve(
                {
                    "data_id": data_id,
                    "destination_worker_id": self.reporter.worker_id,
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
                payload = self.client.fetch_source(
                    source["source_url"],
                    resolved["serialized_sha256"],
                    resolved["size"],
                )
            except Exception:
                self.client.release_replica(lease_id, False)
                self.client.invalidate_observed_replica(
                    source["data_id"],
                    source["replica_id"],
                    source["attempt"],
                    source["content_hash"],
                    source["size"],
                    source["worker_id"],
                    source["worker_epoch"],
                )
                excluded.append(source["worker_id"])
                continue
            self.client.release_replica(lease_id, True)
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
        if resolved["cache_globally"]:
            self.reporter.report_local(
                data_key,
                1,
                resolved["content_hash"],
                payload,
                tier="worker-dram",
            )
        return payload

    def _fetch_peer(self, data_key, content_hash, size):
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
            except Exception:
                return None
            source = resolved["source"]
            source_url = source.get("source_url")
            if not source_url:
                self.client.release_replica(
                    resolved["lease"]["lease_id"], False
                )
                return None
            try:
                payload = self.client.fetch_source(
                    source_url, content_hash, size
                )
            except Exception:
                self.client.release_replica(
                    resolved["lease"]["lease_id"], False
                )
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
            self.client.release_replica(
                resolved["lease"]["lease_id"], True
            )
            self.emit(
                f"DATAVINE_PEER_FETCH {data_key} "
                f"source={source['worker_id']}"
            )
            return payload
        return None

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
        if payload is not None:
            self.emit(f"DATAVINE_DRAM_HIT i{data_id}")
            self.emit(f"DATAVINE_LOCAL_IDATA i{data_id}")
            return payload
        status = self.client.idata_status(data_id)
        payload = (
            self._fetch_peer(
                data_key, status["content_hash"], status["size"]
            )
            if self.allow_peer_transfer
            else None
        )
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
        self.reporter.report_local(
            data_key,
            status["attempt"],
            status["content_hash"],
            payload,
            tier="worker-dram",
        )
        return payload
