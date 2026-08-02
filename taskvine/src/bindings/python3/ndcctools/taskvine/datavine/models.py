"""Protocol-neutral immutable DataVine records."""

import base64
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
    domain: str = "value"

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
    serialized_sha256: str
    metadata: SerializationMetadata
    serialized_bytes: bytes | None
    stable_path: str | None = None
    serialized_size: int | None = None

    def __post_init__(self):
        inline = self.serialized_bytes is not None
        stable = self.stable_path is not None
        if inline == stable:
            raise ValueError(
                "EData must have exactly one inline or stable origin"
            )
        size = (
            len(self.serialized_bytes)
            if inline
            else int(self.serialized_size)
        )
        if size < 0:
            raise ValueError("EData serialized size cannot be negative")
        object.__setattr__(self, "serialized_size", size)

    @staticmethod
    def digest(metadata, serialized_bytes):
        return hashlib.sha256(
            metadata.identity_bytes() + b"\0" + serialized_bytes
        ).hexdigest()


@dataclasses.dataclass(frozen=True)
class IDataRecord:
    data_id: int
    producer_task_id: int
    producer_output_index: int = 0
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
    output_data_ids: tuple
    input_data_ids: tuple

    def __post_init__(self):
        output_data_ids = self.output_data_ids
        if isinstance(output_data_ids, int):
            output_data_ids = (output_data_ids,)
        else:
            output_data_ids = tuple(int(value) for value in output_data_ids)
        if not output_data_ids:
            raise ValueError("TaskRecord requires at least one output")
        if len(set(output_data_ids)) != len(output_data_ids):
            raise ValueError("TaskRecord output IDataIDs must be unique")
        object.__setattr__(self, "output_data_ids", output_data_ids)
        for kind, value in (
            *self.positional,
            *(binding for _, binding in self.keyword),
        ):
            if kind == "v":
                payload = base64.b64decode(value, validate=True)
                if len(payload) > 1024:
                    raise ValueError(
                        "inline task value exceeds 1024 bytes"
                    )

    @property
    def output_data_id(self):
        """Return the sole output of a single-output task."""
        if len(self.output_data_ids) != 1:
            raise ValueError("multi-output TaskRecord has no sole output")
        return self.output_data_ids[0]

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "function_data_id": self.function_data_id,
            "positional": [list(value) for value in self.positional],
            "keyword": [[name, list(value)] for name, value in self.keyword],
            "output_data_ids": list(self.output_data_ids),
            "input_data_ids": list(self.input_data_ids),
        }

    @classmethod
    def from_dict(cls, value):
        output_data_ids = value.get("output_data_ids")
        if output_data_ids is None:
            output_data_ids = (value["output_data_id"],)
        return cls(
            task_id=int(value["task_id"]),
            function_data_id=int(value["function_data_id"]),
            positional=tuple(
                (
                    str(kind),
                    str(data_id) if str(kind) == "v" else int(data_id),
                )
                for kind, data_id in value["positional"]
            ),
            keyword=tuple(
                (
                    str(name),
                    (
                        str(binding[0]),
                        (
                            str(binding[1])
                            if str(binding[0]) == "v"
                            else int(binding[1])
                        ),
                    ),
                )
                for name, binding in value["keyword"]
            ),
            output_data_ids=tuple(
                int(data_id) for data_id in output_data_ids
            ),
            input_data_ids=tuple(
                int(data_id) for data_id in value["input_data_ids"]
            ),
        )
