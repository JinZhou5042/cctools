"""Scheduler-side client for the standalone Data Controller."""

import base64
import json
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
    def __init__(self, endpoint, token, timeout=30):
        self.endpoint = endpoint.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _request(self, method, path, value=None):
        data = None
        headers = {TOKEN_HEADER: self.token}
        if value is not None:
            data = json.dumps(value).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.endpoint + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout
            ) as response:
                return response.read(), response.headers
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise DataVineRemoteError(
                f"Controller HTTP {exc.code}: {body}"
            ) from exc

    def health(self):
        payload, _ = self._request("GET", f"{API_PREFIX}/health")
        return json.loads(payload)

    def snapshot(self):
        payload, _ = self._request("GET", f"{API_PREFIX}/snapshot")
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

    def allocate_idata(self, producer_task_id):
        payload, _ = self._request(
            "POST",
            f"{API_PREFIX}/idata/allocate",
            {"producer_task_id": int(producer_task_id)},
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
        request = urllib.request.Request(
            self.endpoint
            + f"{API_PREFIX}/idata/{int(data_id)}/publish",
            data=serialized_bytes,
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout
            ) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise DataVineRemoteError(
                f"Controller HTTP {exc.code}: {body}"
            ) from exc

    def persist_idata(self, data_id):
        payload, _ = self._request(
            "POST", f"{API_PREFIX}/idata/{int(data_id)}/persist", {}
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
