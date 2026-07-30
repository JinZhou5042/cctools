# Copyright (C) 2026- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Frontend data identities used by the incremental DataVine implementation.

Phase 1 records serialized identity and task bindings without changing the
legacy VineGraph execution or data-movement paths.
"""

import dataclasses
import hashlib
import json
import pickle
import platform
import sys

import cloudpickle


@dataclasses.dataclass(frozen=True)
class SerializationMetadata:
    serializer: str
    serializer_version: str
    protocol: int
    python_implementation: str
    python_version: tuple
    type_module: str
    type_qualname: str

    def identity_bytes(self):
        values = dataclasses.asdict(self)
        values["python_version"] = list(self.python_version)
        return json.dumps(
            values, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")


@dataclasses.dataclass(frozen=True)
class EDataRecord:
    data_id: int
    content_hash: str
    metadata: SerializationMetadata
    serialized_bytes: bytes


class SerializedEDataRegistry:
    """Intern canonical serialized values and assign compact EDataIDs.

    Hashes only select a collision bucket. Metadata and serialized bytes are
    compared before an existing identity is reused.
    """

    def __init__(self, digest_func=None):
        self._digest_func = digest_func
        self._next_data_id = 1
        self._records = {}
        self._buckets = {}
        self._registrations = 0

    @staticmethod
    def cloudpickle_metadata(value):
        value_type = type(value)
        return SerializationMetadata(
            serializer="cloudpickle",
            serializer_version=getattr(cloudpickle, "__version__", "unknown"),
            protocol=pickle.HIGHEST_PROTOCOL,
            python_implementation=platform.python_implementation(),
            python_version=(sys.version_info.major, sys.version_info.minor),
            type_module=getattr(value_type, "__module__", ""),
            type_qualname=getattr(value_type, "__qualname__", value_type.__name__),
        )

    @staticmethod
    def raw_file_metadata():
        return SerializationMetadata(
            serializer="raw-file",
            serializer_version="1",
            protocol=0,
            python_implementation=platform.python_implementation(),
            python_version=(sys.version_info.major, sys.version_info.minor),
            type_module="builtins",
            type_qualname="bytes",
        )

    def _digest(self, payload):
        if self._digest_func is None:
            return hashlib.sha256(payload).hexdigest()
        digest = self._digest_func(payload)
        if isinstance(digest, bytes):
            return digest.hex()
        return str(digest)

    def register(self, value):
        metadata = self.cloudpickle_metadata(value)
        payload = cloudpickle.dumps(value, protocol=metadata.protocol)
        return self.register_serialized(payload, metadata)

    def register_file(self, path):
        with open(path, "rb") as stream:
            payload = stream.read()
        return self.register_serialized(payload, self.raw_file_metadata())

    def register_serialized(self, serialized_bytes, metadata):
        if not isinstance(serialized_bytes, bytes):
            raise TypeError("serialized edata must be bytes")
        if not isinstance(metadata, SerializationMetadata):
            raise TypeError("metadata must be SerializationMetadata")

        self._registrations += 1
        identity_payload = metadata.identity_bytes() + b"\0" + serialized_bytes
        content_hash = self._digest(identity_payload)
        bucket_key = (metadata, content_hash)

        for data_id in self._buckets.get(bucket_key, ()):
            record = self._records[data_id]
            if record.serialized_bytes == serialized_bytes:
                return data_id

        data_id = self._next_data_id
        self._next_data_id += 1
        self._records[data_id] = EDataRecord(
            data_id=data_id,
            content_hash=content_hash,
            metadata=metadata,
            serialized_bytes=serialized_bytes,
        )
        self._buckets.setdefault(bucket_key, []).append(data_id)
        return data_id

    def lookup(self, value):
        metadata = self.cloudpickle_metadata(value)
        payload = cloudpickle.dumps(value, protocol=metadata.protocol)
        identity_payload = metadata.identity_bytes() + b"\0" + payload
        content_hash = self._digest(identity_payload)
        for data_id in self._buckets.get((metadata, content_hash), ()):
            if self._records[data_id].serialized_bytes == payload:
                return data_id
        return None

    def get(self, data_id):
        try:
            return self._records[data_id]
        except KeyError:
            raise KeyError(f"unknown EDataID {data_id}") from None

    @property
    def records(self):
        return dict(self._records)

    def summary(self):
        unique_bytes = sum(len(record.serialized_bytes) for record in self._records.values())
        return {
            "registrations": self._registrations,
            "unique_edata": len(self._records),
            "deduplicated_registrations": self._registrations - len(self._records),
            "unique_serialized_bytes": unique_bytes,
        }


@dataclasses.dataclass(frozen=True)
class DataReference:
    data_kind: str
    data_id: int
    projection: tuple = ()


@dataclasses.dataclass(frozen=True)
class TaskInputBinding:
    slot_kind: str
    slot: object
    source_kind: str
    data_id: int
    references: tuple = ()


@dataclasses.dataclass(frozen=True)
class TaskOutputBinding:
    slot_kind: str
    slot: object
    data_id: int


@dataclasses.dataclass(frozen=True)
class TaskDataBindings:
    task_id: int
    workflow_key: object
    callable_edata_id: int
    inputs: tuple
    outputs: tuple


@dataclasses.dataclass(frozen=True)
class IDataRecord:
    data_id: int
    producer_task_id: int
    slot_kind: str
    slot: object


class IndexedDataIdentity:
    """Phase 1 snapshot of task, edata, idata, and binding relationships."""

    def __init__(self):
        self.edata = SerializedEDataRegistry()
        self.task_ids = {}
        self.idata = {}
        self.task_bindings = {}
        self.input_file_data_ids = {}
        self.output_file_data_ids = {}

    def summary(self):
        edata_summary = self.edata.summary()
        return {
            **edata_summary,
            "tasks": len(self.task_ids),
            "idata": len(self.idata),
            "input_bindings": sum(
                len(binding.inputs) for binding in self.task_bindings.values()
            ),
            "output_bindings": sum(
                len(binding.outputs) for binding in self.task_bindings.values()
            ),
        }

    def validate(self):
        """Reject dangling or conflicting relationships in the identity snapshot."""
        task_ids = set(self.task_ids.values())
        if len(task_ids) != len(self.task_ids) or any(
            not isinstance(task_id, int) or task_id <= 0 for task_id in task_ids
        ):
            raise ValueError("task identities must be unique positive integers")
        if set(self.task_bindings) != task_ids:
            raise ValueError("every TaskID must have exactly one binding record")

        edata_ids = set(self.edata.records)
        idata_ids = set(self.idata)
        if not set(self.input_file_data_ids.values()).issubset(edata_ids):
            raise ValueError("input file mapping contains unknown EDataID")
        if not set(self.output_file_data_ids.values()).issubset(idata_ids):
            raise ValueError("output file mapping contains unknown IDataID")
        if any(
            self.idata[data_id].slot_kind != "file"
            for data_id in self.output_file_data_ids.values()
        ):
            raise ValueError("output file mapping must reference file IData")
        for data_id, record in self.idata.items():
            if data_id != record.data_id:
                raise ValueError(f"IDataID key mismatch for {data_id}")
            if record.producer_task_id not in task_ids:
                raise ValueError(f"IDataID {data_id} has unknown producer TaskID")

        for task_id, binding in self.task_bindings.items():
            if task_id != binding.task_id:
                raise ValueError(f"TaskID key mismatch for {task_id}")
            if binding.callable_edata_id not in edata_ids:
                raise ValueError(f"TaskID {task_id} has unknown callable EDataID")

            for input_binding in binding.inputs:
                expected_ids = (
                    idata_ids
                    if input_binding.source_kind == "idata"
                    else edata_ids
                )
                if input_binding.data_id not in expected_ids:
                    raise ValueError(
                        f"TaskID {task_id} input has unknown "
                        f"{input_binding.source_kind} DataID"
                    )
                for reference in input_binding.references:
                    reference_ids = (
                        edata_ids if reference.data_kind == "edata" else idata_ids
                    )
                    if reference.data_kind not in ("edata", "idata"):
                        raise ValueError(
                            f"TaskID {task_id} input has invalid data kind"
                        )
                    if reference.data_id not in reference_ids:
                        raise ValueError(
                            f"TaskID {task_id} input has unknown referenced DataID"
                        )

            for output_binding in binding.outputs:
                if output_binding.data_id not in idata_ids:
                    raise ValueError(f"TaskID {task_id} has unknown output IDataID")
                if self.idata[output_binding.data_id].producer_task_id != task_id:
                    raise ValueError(
                        f"TaskID {task_id} output IDataID has conflicting producer"
                    )
        return True
