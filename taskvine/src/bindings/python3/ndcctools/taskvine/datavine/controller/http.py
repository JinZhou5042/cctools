"""Controller HTTP authentication and payload codecs."""

import json
import urllib.parse

from ..protocol import DataVineSchemaError, TOKEN_HEADER


def request_authorized(path, headers, token):
    query = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
    return (
        headers.get(TOKEN_HEADER) == token
        or query.get("token") == [token]
    )


def encode_json(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def read_json_request(stream, headers, maximum_bytes):
    try:
        length = int(headers.get("Content-Length", "0"))
    except (TypeError, ValueError) as exc:
        raise DataVineSchemaError(
            "invalid request size", path="Content-Length"
        ) from exc
    if length <= 0 or length > int(maximum_bytes):
        raise DataVineSchemaError(
            "invalid request size", path="Content-Length"
        )
    try:
        return json.loads(stream.read(length))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DataVineSchemaError("invalid JSON request", path="$") from exc
