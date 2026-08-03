#!/usr/bin/env python3

import json
import urllib.error
import urllib.request

from ndcctools.taskvine.datavine.codec import (
    decode_serialization_metadata,
    decode_task_record,
)
from ndcctools.taskvine.datavine.models import TaskRecord
from ndcctools.taskvine.datavine.controller.service import ControllerService
from ndcctools.taskvine.datavine.protocol import (
    DataVineRemoteError,
    DataVineSchemaError,
)
from ndcctools.taskvine.datavine.serialization import serialize


def main():
    metadata, _ = serialize({"value": 1})
    assert decode_serialization_metadata(metadata.to_dict()) == metadata
    record = TaskRecord(1, 2, (("v", "gAVLAS4="),), (), (3,), ())
    assert decode_task_record(record.to_dict()) == record

    invalid = (
        (
            lambda: decode_serialization_metadata(
                {**metadata.to_dict(), "python_version": ["3", 10]}
            ),
            "metadata",
        ),
        (lambda: decode_task_record({"task_id": 1}), "task"),
        (lambda: decode_task_record([], "tasks[2]"), "tasks[2]"),
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
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/tasks/register",
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
    finally:
        service.stop()

    print("DataVine protocol codec/error contract PASS")


if __name__ == "__main__":
    main()
