#!/usr/bin/env python3

import io

from ndcctools.taskvine.datavine.controller.http import (
    encode_json,
    read_json_request,
    request_authorized,
)
from ndcctools.taskvine.datavine.protocol import TOKEN_HEADER


def main():
    assert request_authorized(
        "/v1/health", {TOKEN_HEADER: "secret"}, "secret"
    )
    assert request_authorized(
        "/v1/health?token=secret", {}, "secret"
    )
    assert not request_authorized(
        "/v1/health?token=wrong", {TOKEN_HEADER: "wrong"}, "secret"
    )
    assert encode_json({"z": 1, "a": 2}) == b'{"a":2,"z":1}'

    payload = b'{"value":3}'
    assert read_json_request(
        io.BytesIO(payload), {"Content-Length": str(len(payload))}, 20
    ) == {"value": 3}
    invalid = (
        (io.BytesIO(b""), {}, 10),
        (io.BytesIO(b"{}"), {"Content-Length": "bad"}, 10),
        (io.BytesIO(b"{}"), {"Content-Length": "2"}, 1),
        (io.BytesIO(b"x"), {"Content-Length": "1"}, 10),
    )
    for stream, headers, limit in invalid:
        try:
            read_json_request(stream, headers, limit)
        except ValueError:
            pass
        else:
            raise AssertionError("accepted invalid JSON request")

    print("DataVine Controller HTTP codec/auth contract PASS")


if __name__ == "__main__":
    main()
