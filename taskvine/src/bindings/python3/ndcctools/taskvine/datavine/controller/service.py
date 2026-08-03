"""Standalone Data Controller HTTP service running on its own thread."""

import threading

from .admission import ByteServingAdmission, BoundedThreadingHTTPServer
from .handler import ControllerHandlerFactory
from .state import ControllerState


class ControllerService:
    def __init__(
        self,
        host,
        port,
        token,
        state=None,
        max_request_concurrency=32,
        max_serving_concurrency=8,
        max_serving_bytes=64 * 1024 * 1024,
        serving_hook=None,
    ):
        if not token:
            raise ValueError("Controller token is required")
        self.host = host
        self.port = int(port)
        self.token = token
        self.state = state or ControllerState()
        self.max_request_concurrency = int(max_request_concurrency)
        self.byte_serving = ByteServingAdmission(
            max_serving_concurrency, max_serving_bytes
        )
        self.serving_hook = serving_hook
        self._server = None
        self._thread = None

    def snapshot(self):
        value = self.state.snapshot()
        value["byte_serving"] = self.byte_serving.snapshot()
        value["request_admission"] = (
            self._server.admission_snapshot()
            if self._server is not None
            else None
        )
        return value

    def start(self):
        Handler = ControllerHandlerFactory.create(self)

        self._server = BoundedThreadingHTTPServer(
            (self.host, self.port),
            Handler,
            self.max_request_concurrency,
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="datavine-controller",
            daemon=False,
        )
        self._thread.start()
        return self._server.server_address

    @property
    def thread_ident(self):
        return self._thread.ident if self._thread else None

    def stop(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                raise RuntimeError("Controller thread did not stop")
        self.state.stop()
        self._server = None
        self._thread = None
