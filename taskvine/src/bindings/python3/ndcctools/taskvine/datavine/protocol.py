"""Versioned Scheduler/Controller protocol constants and errors."""

PROTOCOL_VERSION = 1
API_PREFIX = f"/v{PROTOCOL_VERSION}"
TOKEN_HEADER = "X-DataVine-Token"


class DataVineProtocolError(RuntimeError):
    code = "protocol-error"

    def __init__(self, message, *, code=None, path=None, details=None):
        super().__init__(str(message))
        self.code = str(code or self.code)
        self.path = None if path is None else str(path)
        self.details = details

    def to_dict(self):
        value = {"error": str(self), "code": self.code}
        if self.path is not None:
            value["path"] = self.path
        if self.details is not None:
            value["details"] = self.details
        return value


class DataVineSchemaError(DataVineProtocolError, ValueError):
    code = "invalid-schema"


class DataVineRemoteError(DataVineProtocolError):
    code = "remote-error"

    def __init__(self, message, *, status=None, **kwargs):
        super().__init__(message, **kwargs)
        self.status = None if status is None else int(status)

    @classmethod
    def from_http(cls, status, body):
        import json

        code = None
        path = None
        details = None
        try:
            value = json.loads(body)
        except (TypeError, json.JSONDecodeError):
            value = None
        if isinstance(value, dict):
            code = value.get("code")
            path = value.get("path")
            details = value.get("details")
        return cls(
            f"Controller HTTP {int(status)}: {body}",
            status=status,
            code=code,
            path=path,
            details=details,
        )
