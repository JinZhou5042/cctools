#!/usr/bin/env python3

import json
import urllib.error
import urllib.request

from ndcctools.taskvine.datavine.codec import (
    TASK_RECORD_COMPACT_FORMAT,
    decode_compact_task_record,
    decode_serialization_metadata,
    decode_task_record,
    encode_compact_task_record,
)
from ndcctools.taskvine.datavine.models import TaskRecord
from ndcctools.taskvine.datavine.controller.service import ControllerService
from ndcctools.taskvine.datavine.protocol import (
    DataVineRemoteError,
    DataVineSchemaError,
)
from ndcctools.taskvine.datavine.serialization import serialize
from ndcctools.taskvine.datavine.scheduler.client import ControllerClient


def main():
    metadata, _ = serialize({"value": 1})
    assert decode_serialization_metadata(metadata.to_dict()) == metadata
    record = TaskRecord(1, 2, (("v", "gAVLAS4="),), (), (3,), ())
    assert decode_task_record(record.to_dict()) == record
    compact = encode_compact_task_record(record)
    assert decode_compact_task_record(compact) == record
    assert TASK_RECORD_COMPACT_FORMAT == "task-record-row-v1"
    records = [
        TaskRecord(
            task_id,
            2,
            (("v", "gAVLAS4="),),
            (("name", ("e", 1)),),
            (task_id + 1000,),
            (),
        )
        for task_id in range(1, 1001)
    ]
    legacy_bytes = len(
        json.dumps(
            {"tasks": [item.to_dict() for item in records]},
            separators=(",", ":"),
        )
    )
    compact_bytes = len(
        json.dumps(
            {
                "task_record_format": TASK_RECORD_COMPACT_FORMAT,
                "tasks": [
                    encode_compact_task_record(item) for item in records
                ],
            },
            separators=(",", ":"),
        )
    )
    assert compact_bytes < legacy_bytes * 0.45, (
        compact_bytes,
        legacy_bytes,
    )

    invalid = (
        (
            lambda: decode_serialization_metadata(
                {**metadata.to_dict(), "python_version": ["3", 10]}
            ),
            "metadata",
        ),
        (lambda: decode_task_record({"task_id": 1}), "task"),
        (lambda: decode_task_record([], "tasks[2]"), "tasks[2]"),
        (
            lambda: decode_compact_task_record([1, 2], "tasks[3]"),
            "tasks[3]",
        ),
        (
            lambda: decode_compact_task_record(
                [1, 2, [["e"]], [], [3], []], "tasks[4]"
            ),
            "tasks[4]",
        ),
    )
    for operation, path in invalid:
        try:
            operation()
        except DataVineSchemaError as exc:
            assert exc.code == "invalid-schema"
            assert exc.path == path
            assert exc.to_dict()["error"] == str(exc)
        else:
            raise AssertionError("accepted invalid protocol record")

    body = json.dumps(
        {
            "error": "invalid task record",
            "code": "invalid-schema",
            "path": "tasks[2]",
            "details": {"field": "task_id"},
        }
    )
    remote = DataVineRemoteError.from_http(400, body)
    assert remote.status == 400
    assert remote.code == "invalid-schema"
    assert remote.path == "tasks[2]"
    assert remote.details == {"field": "task_id"}
    assert "Controller HTTP 400" in str(remote)

    service = ControllerService("127.0.0.1", 0, "codec-token")
    host, port = service.start()
    try:
        endpoint = f"http://{host}:{port}"
        request = urllib.request.Request(
            f"{endpoint}/v1/tasks/register",
            data=b"{}",
            headers={
                "Content-Type": "application/json",
                "X-DataVine-Token": "codec-token",
            },
            method="POST",
        )
        try:
            urllib.request.urlopen(request, timeout=10)
        except urllib.error.HTTPError as exc:
            assert exc.code == 400
            response = json.loads(exc.read())
            assert response["code"] == "invalid-schema"
            assert response["path"] == "task"
            assert response["error"] == "invalid task record"
        else:
            raise AssertionError("Controller accepted invalid task schema")

        compact_client = ControllerClient(endpoint, "codec-token")
        legacy_client = ControllerClient(
            endpoint, "codec-token", compact_task_records=False
        )
        function_metadata, function_payload = serialize(abs)
        function_data_id = compact_client.register_edata(
            function_metadata, function_payload
        )["data_id"]
        output_ids = compact_client.allocate_idata_batch(
            (task_id, 0) for task_id in range(1, 201)
        )
        compact_records = tuple(
            TaskRecord(
                task_id,
                function_data_id,
                (("v", "gAVLAS4="),),
                (),
                (output_ids[task_id - 1],),
                (),
            )
            for task_id in range(1, 101)
        )
        legacy_records = tuple(
            TaskRecord(
                task_id,
                function_data_id,
                (("v", "gAVLAS4="),),
                (),
                (output_ids[task_id - 1],),
                (),
            )
            for task_id in range(101, 201)
        )
        assert compact_client.register_tasks(compact_records) == compact_records
        assert legacy_client.register_tasks(legacy_records) == legacy_records
        route = "POST /v1/tasks/register-batch"
        compact_request_bytes = compact_client.request_metrics()[route][
            "request_bytes"
        ]
        legacy_request_bytes = legacy_client.request_metrics()[route][
            "request_bytes"
        ]
        assert compact_request_bytes < legacy_request_bytes * 0.5, (
            compact_request_bytes,
            legacy_request_bytes,
        )
    finally:
        service.stop()

    print("DataVine protocol codec/error contract PASS")


if __name__ == "__main__":
    main()
