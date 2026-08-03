"""Worker-side EData/IData fetching and binding resolution."""

import base64
import cloudpickle
import copy
import hashlib
from pathlib import Path

from ..codec import decode_task_record
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
        inline_edata=None,
        inline_tasks=None,
    ):
        self.controller = controller
        self.token = token
        self.client = client
        self.reporter = reporter
        self.process_cache = process_cache
        self.emit = emit
        self.trust_taskvine_inputs = bool(trust_taskvine_inputs)
        self.inline_edata = inline_edata
        self.inline_tasks = inline_tasks
        self.objects = {}

    def fetch_edata(self, data_id):
        data_id = int(data_id)
        if self.inline_edata is not None and data_id in self.inline_edata:
            return self.inline_edata[data_id]
        cache_path = Path(f"datavine-edata-{data_id}.pkl")
        if self.trust_taskvine_inputs and cache_path.is_file():
            return cache_path.read_bytes()
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
            payload = cache_path.read_bytes()
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
        elif kind == "v":
            payload = base64.b64decode(data_id, validate=True)
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
        if producer is None and self.inline_tasks is not None:
            value = self.inline_tasks.get(task_id)
            if value is not None:
                producer = decode_task_record(value)
                self.process_cache.task_records[producer_key] = producer
        if producer is None:
            producer = self.client.get_task(task_id)
            self.process_cache.task_records[producer_key] = producer
        return producer

    def _fetch_idata(self, data_id):
        cache_path = Path(f"datavine-idata-{data_id}.pkl")
        if not cache_path.is_file():
            return self.client.fetch_idata(data_id)
        payload = cache_path.read_bytes()
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
