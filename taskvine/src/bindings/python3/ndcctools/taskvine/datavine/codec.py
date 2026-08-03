"""Validated protocol record codecs."""

from .models import SerializationMetadata, TaskRecord
from .protocol import DataVineSchemaError


def require_mapping(value, path):
    if not isinstance(value, dict):
        raise DataVineSchemaError(
            "expected an object",
            path=path,
            details={"type": type(value).__name__},
        )
    return value


def decode_serialization_metadata(value, path="metadata"):
    value = require_mapping(value, path)
    try:
        metadata = SerializationMetadata.from_dict(value)
    except (KeyError, TypeError, ValueError) as exc:
        raise DataVineSchemaError(
            "invalid serialization metadata", path=path
        ) from exc
    if (
        not metadata.serializer
        or not metadata.serializer_version
        or not metadata.python_implementation
        or not metadata.type_module
        or not metadata.type_qualname
        or not metadata.domain
        or not isinstance(metadata.python_version, tuple)
        or not metadata.python_version
        or any(
            not isinstance(component, int)
            for component in metadata.python_version
        )
    ):
        raise DataVineSchemaError(
            "invalid serialization metadata", path=path
        )
    return metadata


def decode_task_record(value, path="task"):
    value = require_mapping(value, path)
    try:
        return TaskRecord.from_dict(value)
    except (KeyError, TypeError, ValueError) as exc:
        raise DataVineSchemaError(
            "invalid task record", path=path
        ) from exc
