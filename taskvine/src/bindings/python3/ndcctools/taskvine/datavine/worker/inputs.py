"""Worker-side EData/IData fetching and binding resolution."""

import cloudpickle
import copy
import hashlib
from pathlib import Path

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
        cache_path = Path(f"datavine-edata-{data_id}.pkl")
        if self.trust_taskvine_inputs and cache_path.is_file():
            return self._local_payload("e", data_id, cache_path)
        metadata_key = (self.controller, self.token, data_id)
        info = self.process_cache.edata_metadata.get(metadata_key)
        if info is None:
            info = self.client.get_edata_metadata(data_id)
            self.process_cache.edata_metadata[metadata_key] = info

        def fallback():
            if info["storage"] != "bulk-origin":
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
            return payload
        return fallback()

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
        cache_path = Path(f"datavine-idata-{data_id}.pkl")
        if not cache_path.is_file():
            return self.client.fetch_idata(data_id)
        if self.trust_taskvine_inputs:
            payload = self._local_payload("i", data_id, cache_path)
            self.emit(f"DATAVINE_LOCAL_IDATA i{data_id}")
            return payload
        payload = self._local_payload("i", data_id, cache_path)
        status = self.client.idata_status(data_id)
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
