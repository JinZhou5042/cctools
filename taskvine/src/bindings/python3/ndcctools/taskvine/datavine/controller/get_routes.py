"""Data Controller GET routes."""

import base64
import json
import urllib.parse

from ..protocol import API_PREFIX, PROTOCOL_VERSION


class GetRouteFactory:
    @staticmethod
    def create(owner):
        class Routes:
            def do_GET(self):
                if not self._authorized():
                    self._error(403, "forbidden")
                    return
                parsed = urllib.parse.urlparse(self.path)
                if parsed.path == f"{API_PREFIX}/health":
                    self._json(
                        200,
                        {
                            "status": "ready",
                            "protocol_version": PROTOCOL_VERSION,
                            "controller_thread": owner._thread.ident,
                        },
                    )
                    return
                if parsed.path == f"{API_PREFIX}/snapshot":
                    self._json(200, owner.snapshot())
                    return
                if parsed.path == f"{API_PREFIX}/pruning/plan":
                    try:
                        plan = owner.state.pruning_plan()
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, plan)
                    return
                prefix = f"{API_PREFIX}/edata/"
                if parsed.path.startswith(prefix):
                    token = parsed.path[len(prefix):]
                    metadata_only = token.endswith("/metadata")
                    if metadata_only:
                        token = token[:-len("/metadata")]
                    if not token.isdigit():
                        self._error(400, "invalid EDataID")
                        return
                    try:
                        record = owner.state.get_edata(int(token))
                    except KeyError as exc:
                        self._error(404, exc)
                        return
                    if metadata_only:
                        self._json(
                            200,
                            {
                                "data_id": record.data_id,
                                "content_hash": record.content_hash,
                                "serialized_sha256": (
                                    record.serialized_sha256
                                ),
                                "size": record.serialized_size,
                                "metadata": record.metadata.to_dict(),
                                "storage": (
                                    "controller-memory"
                                    if record.serialized_bytes is not None
                                    else "bulk-origin"
                                ),
                                "origin_path": record.stable_path,
                            },
                        )
                        return
                    payload = record.serialized_bytes
                    if payload is None:
                        self._error(
                            409,
                            "EData uses a stable bulk origin and is not "
                            "served by the Controller",
                        )
                        return
                    if not owner.byte_serving.acquire(len(payload)):
                        self._error(503, "byte serving capacity exceeded")
                        return
                    owner.state.record_edata_fetch(record.data_id)
                    completed = False
                    try:
                        if owner.serving_hook is not None:
                            owner.serving_hook(f"e:{record.data_id}")
                        self.send_response(200)
                        self.send_header(
                            "Content-Type", "application/octet-stream"
                        )
                        self.send_header(
                            "Content-Length", str(len(payload))
                        )
                        self.send_header(
                            "X-DataVine-SHA256", record.content_hash
                        )
                        self.send_header(
                            "X-DataVine-Metadata",
                            base64.urlsafe_b64encode(
                                json.dumps(
                                    record.metadata.to_dict(),
                                    sort_keys=True,
                                    separators=(",", ":"),
                                ).encode("utf-8")
                            ).decode("ascii"),
                        )
                        self.end_headers()
                        self.wfile.write(payload)
                        completed = True
                    finally:
                        owner.byte_serving.release(
                            len(payload), completed
                        )
                    return
                task_prefix = f"{API_PREFIX}/tasks/"
                if parsed.path.startswith(task_prefix):
                    token = parsed.path[len(task_prefix):]
                    try:
                        record = owner.state.get_task(int(token))
                    except (KeyError, ValueError) as exc:
                        self._error(404, exc)
                        return
                    self._json(200, record.to_dict())
                    return
                idata_prefix = f"{API_PREFIX}/idata/"
                if parsed.path.startswith(idata_prefix):
                    token = parsed.path[len(idata_prefix):]
                    status_only = token.endswith("/status")
                    if status_only:
                        token = token[:-len("/status")]
                    try:
                        if status_only:
                            self._json(
                                200, owner.state.idata_status(int(token))
                            )
                            return
                        record = owner.state.get_idata(int(token))
                    except (KeyError, ValueError) as exc:
                        self._error(404, exc)
                        return
                    requested_attempt = urllib.parse.parse_qs(
                        parsed.query
                    ).get("attempt")
                    if requested_attempt is not None:
                        try:
                            requested_attempt = int(
                                requested_attempt[0]
                            )
                        except (TypeError, ValueError, IndexError):
                            self._error(400, "invalid IData attempt")
                            return
                        if requested_attempt != record.attempt:
                            self._error(
                                409,
                                "IData attempt no longer current",
                            )
                            return
                    if record.serialized_bytes is None:
                        self._error(409, "IData is not available")
                        return
                    payload = record.serialized_bytes
                    if not owner.byte_serving.acquire(len(payload)):
                        self._error(503, "byte serving capacity exceeded")
                        return
                    completed = False
                    try:
                        if owner.serving_hook is not None:
                            owner.serving_hook(f"i:{record.data_id}")
                        self.send_response(200)
                        self.send_header(
                            "Content-Type", "application/octet-stream"
                        )
                        self.send_header(
                            "Content-Length", str(len(payload))
                        )
                        self.send_header(
                            "X-DataVine-SHA256", record.content_hash
                        )
                        self.send_header(
                            "X-DataVine-Attempt", str(record.attempt)
                        )
                        self.end_headers()
                        self.wfile.write(payload)
                        completed = True
                    finally:
                        owner.byte_serving.release(
                            len(payload), completed
                        )
                    return
                replica_prefix = f"{API_PREFIX}/replicas/"
                if (
                    parsed.path.startswith(replica_prefix)
                    and (
                        parsed.path.endswith("/sources")
                        or parsed.path.endswith("/records")
                    )
                ):
                    records_only = parsed.path.endswith("/records")
                    suffix = "/records" if records_only else "/sources"
                    token = parsed.path[
                        len(replica_prefix):-len(suffix)
                    ]
                    pieces = token.strip("/").split("/")
                    if (
                        len(pieces) != 2
                        or pieces[0] not in ("e", "i")
                        or not pieces[1].isdigit()
                    ):
                        self._error(400, "invalid qualified DataID")
                        return
                    data_key = f"{pieces[0]}:{int(pieces[1])}"
                    try:
                        sources = (
                            owner.state.replicas.records_for(data_key)
                            if records_only
                            else owner.state.replicas.candidates(data_key)
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "data_id": data_key,
                            "records" if records_only else "sources": [
                                source.source_dict()
                                for source in sources
                            ],
                        },
                    )
                    return
                self._error(404, "not found")

        return Routes.do_GET
