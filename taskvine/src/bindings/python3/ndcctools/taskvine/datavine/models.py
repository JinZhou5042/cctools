"""Protocol-neutral immutable DataVine records."""

import dataclasses
import hashlib
import json


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

    def to_dict(self):
        values = dataclasses.asdict(self)
        values["python_version"] = list(self.python_version)
        return values

    @classmethod
    def from_dict(cls, values):
        values = dict(values)
        values["python_version"] = tuple(values["python_version"])
        return cls(**values)


@dataclasses.dataclass(frozen=True)
class EDataRecord:
    data_id: int
    content_hash: str
    metadata: SerializationMetadata
    serialized_bytes: bytes

    @staticmethod
    def digest(metadata, serialized_bytes):
        return hashlib.sha256(
            metadata.identity_bytes() + b"\0" + serialized_bytes
        ).hexdigest()


@dataclasses.dataclass(frozen=True)
class IDataRecord:
    data_id: int
    producer_task_id: int
    content_hash: str | None = None
    serialized_bytes: bytes | None = None
    attempt: int = 0
    durability: str = "volatile"
    durable_path: str | None = None
    serialized_size: int | None = None


@dataclasses.dataclass(frozen=True)
class TaskRecord:
    task_id: int
    function_data_id: int
    positional: tuple
    keyword: tuple
    output_data_id: int

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "function_data_id": self.function_data_id,
            "positional": [list(value) for value in self.positional],
            "keyword": [[name, list(value)] for name, value in self.keyword],
            "output_data_id": self.output_data_id,
        }

    @classmethod
    def from_dict(cls, value):
        return cls(
            task_id=int(value["task_id"]),
            function_data_id=int(value["function_data_id"]),
            positional=tuple(
                (str(kind), int(data_id))
                for kind, data_id in value["positional"]
            ),
            keyword=tuple(
                (str(name), (str(binding[0]), int(binding[1])))
                for name, binding in value["keyword"]
            ),
            output_data_id=int(value["output_data_id"]),
        )
