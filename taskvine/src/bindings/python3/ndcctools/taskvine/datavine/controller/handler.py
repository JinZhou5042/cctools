"""Data Controller HTTP request handler."""

import http.server

from ..protocol import DataVineProtocolError
from .get_routes import GetRouteFactory
from .http import encode_json, read_json_request, request_authorized
from .post_routes import PostRouteFactory


class ControllerHandlerFactory:
    @staticmethod
    def create(owner):
        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            disable_nagle_algorithm = True

            def _authorized(self):
                return request_authorized(
                    self.path, self.headers, owner.token
                )

            def _json(self, status, value):
                payload = encode_json(value)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def _error(self, status, message):
                value = (
                    message.to_dict()
                    if isinstance(message, DataVineProtocolError)
                    else {"error": str(message)}
                )
                self._json(status, value)

            do_GET = GetRouteFactory.create(owner)

            do_POST = PostRouteFactory.create(owner)

            def _read_json(self):
                return read_json_request(
                    self.rfile,
                    self.headers,
                    owner.state.max_edata_bytes * 2,
                )

            def log_message(self, format_string, *args):
                return

        return Handler
