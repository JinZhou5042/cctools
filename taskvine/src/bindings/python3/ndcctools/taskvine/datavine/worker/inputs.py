"""Worker-side EData/IData fetching and binding resolution."""

import cloudpickle
import copy
import hashlib
from pathlib import Path
import uuid

from ..models import EDataRecord
from ..protocol import DataVineRemoteError
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
        trust_taskvine_inputs=False,
        cache_values=None,
    ):
        self.controller = controller
        self.token = token
        self.client = client
        self.reporter = reporter
        self.process_cache = process_cache
        self.emit = emit
        self.trust_taskvine_inputs = bool(trust_taskvine_inputs)
        self.objects = {}
        self.cache_values = cache_values or {}

    @staticmethod
    def _file_identity(path):
        stat = path.stat()
        return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)

    def _local_payload(self, kind, data_id, path):
        if not path.is_file():
            return None
        key = (
            self.controller,
            self.token,
            kind,
            int(data_id),
            self._file_identity(path),
        )
        with self.process_cache.lock:
            payload = self.process_cache.data.get(key)
        if payload is not None:
            self.emit(f"DATAVINE_DRAM_HIT {kind}{int(data_id)}")
            return payload
        payload = path.read_bytes()
        hint = self.cache_values.get(f"{kind}:{int(data_id)}")
        if hint is not None:
            with self.process_cache.lock:
                self.process_cache.data.put(key, payload, hint["score"])
        return payload

    def fetch_edata(self, data_id):
        data_id = int(data_id)
        with self.process_cache.fetch_lock(
            (self.controller, self.token, "e", data_id)
        ):
            return self._fetch_edata_locked(data_id)

    def _fetch_edata_locked(self, data_id):
        cache_path = Path(f"datavine-edata-{data_id}.pkl")
        if self.trust_taskvine_inputs and cache_path.is_file():
            return self._local_payload("e", data_id, cache_path)
        metadata_key = (self.controller, self.token, data_id)
        info = self.process_cache.edata_metadata.get(metadata_key)
        prefetched_payload = None
        if info is None:
            try:
                metadata, prefetched_payload = (
                    self.client.fetch_edata_record(data_id)
                )
                info = {
                    "data_id": data_id,
                    "content_hash": EDataRecord.digest(
                        metadata, prefetched_payload
                    ),
                    "serialized_sha256": hashlib.sha256(
                        prefetched_payload
                    ).hexdigest(),
                    "size": len(prefetched_payload),
                    "metadata": metadata,
                    "storage": "controller-memory",
                    "origin_path": None,
                }
            except DataVineRemoteError as exc:
                if exc.status != 409:
                    raise
                info = self.client.get_edata_metadata(data_id)
            self.process_cache.edata_metadata[metadata_key] = info
        data_key = f"e:{data_id}"
        with self.process_cache.lock:
            payload = self.process_cache.data.get_data(
                self.controller,
                self.token,
                data_key,
                info["serialized_sha256"],
                info["size"],
            )
        if payload is not None:
            self.emit(f"DATAVINE_DRAM_HIT e{data_id}")
            return payload

        def fallback():
            if info["storage"] != "bulk-origin":
                if prefetched_payload is not None:
                    return prefetched_payload
                return self.client.fetch_edata_record(data_id)[1]
            origin = Path(info["origin_path"])
            payload = origin.read_bytes()
            if (
                len(payload) != info["size"]
                or EDataRecord.digest(info["metadata"], payload)
                != info["content_hash"]
            ):
                raise RuntimeError(
                    f"EDataID {data_id} bulk origin checksum mismatch"
                )
            self.emit(f"DATAVINE_BULK_ORIGIN e{data_id}")
            return payload

        if cache_path.is_file():
            payload = self._local_payload("e", data_id, cache_path)
            if (
                len(payload) != info["size"]
                or EDataRecord.digest(info["metadata"], payload)
                != info["content_hash"]
            ):
                self.reporter.reject_local(f"e:{data_id}")
                return fallback()
            self.reporter.report_local(
                f"e:{data_id}", 1, info["content_hash"], payload
            )
        else:
            payload = fallback()
        hint = self.cache_values.get(data_key)
        with self.process_cache.lock:
            self.process_cache.data.put_data(
                self.controller,
                self.token,
                data_key,
                info["serialized_sha256"],
                payload,
                hint["score"] if hint is not None else None,
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
        cache_path = Path(f"datavine-idata-{data_id}.pkl")
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
        if not cache_path.is_file():
            self.reporter.reject_local(data_key)
            payload = self._fetch_peer(
                data_key, status["content_hash"], status["size"]
            )
            if payload is None:
                if status["durability"] == "durable":
                    payload = Path(status["durable_path"]).read_bytes()
                    if (
                        len(payload) != status["size"]
                        or hashlib.sha256(payload).hexdigest()
                        != status["content_hash"]
                    ):
                        raise IOError(
                            f"durable IDataID {data_id} is corrupt"
                        )
                    self.emit(f"DATAVINE_SHAREDFS_FETCH i{data_id}")
                elif status["controller_inline"]:
                    payload = self.client.fetch_idata(data_id)
                else:
                    raise RuntimeError(
                        f"IData is not available: i:{data_id}"
                    )
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
        if self.trust_taskvine_inputs:
            payload = self._local_payload("i", data_id, cache_path)
            self.emit(f"DATAVINE_LOCAL_IDATA i{data_id}")
            return payload
        payload = self._local_payload("i", data_id, cache_path)
        if hashlib.sha256(payload).hexdigest() != status["content_hash"]:
            self.reporter.reject_local(f"i:{data_id}")
            return self.client.fetch_idata(data_id)
        self.reporter.report_local(
            f"i:{data_id}",
            status["attempt"],
            status["content_hash"],
            payload,
        )
        self.emit(f"DATAVINE_LOCAL_IDATA i{data_id}")
        return payload
