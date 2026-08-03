"""Validated protocol record codecs."""

from .models import SerializationMetadata, TaskRecord
from .protocol import DataVineSchemaError


TASK_RECORD_COMPACT_FORMAT = "task-record-row-v1"


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


def encode_compact_task_record(record):
    if not isinstance(record, TaskRecord):
        raise TypeError("record must be a TaskRecord")
    return [
        record.task_id,
        record.function_data_id,
        [list(binding) for binding in record.positional],
        [
            [name, binding[0], binding[1]]
            for name, binding in record.keyword
        ],
        list(record.output_data_ids),
        list(record.input_data_ids),
    ]


def _decode_compact_binding(binding):
    if not isinstance(binding, list) or len(binding) != 2:
        raise ValueError("invalid compact task binding")
    kind = str(binding[0])
    return kind, str(binding[1]) if kind == "v" else int(binding[1])


def _decode_compact_keyword(binding):
    if not isinstance(binding, list) or len(binding) != 3:
        raise ValueError("invalid compact keyword binding")
    return str(binding[0]), _decode_compact_binding(binding[1:])


def decode_compact_task_record(value, path="task"):
    if not isinstance(value, list) or len(value) != 6:
        raise DataVineSchemaError(
            "invalid compact task record", path=path
        )
    try:
        (
            task_id,
            function_data_id,
            positional,
            keyword,
            output_data_ids,
            input_data_ids,
        ) = value
        return TaskRecord(
            task_id=int(task_id),
            function_data_id=int(function_data_id),
            positional=tuple(
                _decode_compact_binding(binding)
                for binding in positional
            ),
            keyword=tuple(
                _decode_compact_keyword(binding)
                for binding in keyword
            ),
            output_data_ids=tuple(int(item) for item in output_data_ids),
            input_data_ids=tuple(int(item) for item in input_data_ids),
        )
    except (IndexError, TypeError, ValueError) as exc:
        raise DataVineSchemaError(
            "invalid compact task record", path=path
        ) from exc
