"""Versioned Scheduler/Controller protocol constants and errors."""

PROTOCOL_VERSION = 1
API_PREFIX = f"/v{PROTOCOL_VERSION}"
TOKEN_HEADER = "X-DataVine-Token"


class DataVineProtocolError(RuntimeError):
    pass


class DataVineRemoteError(DataVineProtocolError):
    pass
