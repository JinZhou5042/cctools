"""Data Controller POST routes."""

import base64
import dataclasses
import json
import struct
import urllib.parse

from ..codec import (
    TASK_RECORD_COMPACT_FORMAT,
    decode_compact_task_record,
    decode_serialization_metadata,
    decode_task_record,
    encode_compact_task_record,
)
from ..protocol import API_PREFIX, DataVineSchemaError
from .persistence_state import PersistenceBusy


def _resolve_edata(owner, request):
    data_id = int(request["data_id"])
    record = owner.state.get_edata(data_id)
    common = {
        "data_id": data_id,
        "content_hash": record.content_hash,
        "serialized_sha256": record.serialized_sha256,
        "size": record.serialized_size,
        "metadata": record.metadata.to_dict(),
        "cache_globally": owner.state.edata_has_shared_consumers(data_id),
    }
    resolved = None
    if common["cache_globally"] and request.get(
        "allow_peer_transfer", True
    ):
        try:
            resolved = owner.state.resolve_worker_source(
                f"e:{data_id}",
                request["destination_worker_id"],
                request["transfer_id"],
                request.get("excluded_worker_ids", ()),
                False,
            )
        except KeyError:
            pass
    if resolved is not None:
        source = resolved["source"]
        parsed_url = urllib.parse.urlsplit(source["source_url"])
        query = urllib.parse.parse_qs(parsed_url.query)
        query["sha256"] = [record.serialized_sha256]
        source["source_url"] = urllib.parse.urlunsplit(
            parsed_url._replace(
                query=urllib.parse.urlencode(query, doseq=True)
            )
        )
        return {
            **common,
            "source_type": "peer",
            "source": source,
            "lease": dataclasses.asdict(resolved["lease"]),
        }, None
    if record.serialized_bytes is not None:
        return {
            **common,
            "source_type": "controller-memory",
        }, record.serialized_bytes
    if record.native:
        native = owner.get_native_edata(
            data_id, allow_shared=True
        )
        return {
            **common,
            "source_type": "controller-memory",
        }, native["payload"]
    return {
        **common,
        "source_type": "sharedfs",
        "origin_path": record.stable_path,
    }, None


class PostRouteFactory:
    @staticmethod
    def create(owner):
        class Routes:
            def do_POST(self):
                if not self._authorized():
                    self._error(403, "forbidden")
                    return
                if self.path == f"{API_PREFIX}/faults/configure":
                    try:
                        request = self._read_json()
                        owner.transfer_faults.configure(**request)
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, owner.transfer_faults.snapshot())
                    return
                if self.path == f"{API_PREFIX}/faults/claim-transfer":
                    request = self._read_json()
                    self._json(
                        200,
                        owner.transfer_faults.claim_transfer(
                            request["transfer_id"], request["size"]
                        ),
                    )
                    return
                if self.path == f"{API_PREFIX}/faults/progress":
                    request = self._read_json()
                    owner.transfer_faults.progress(
                        request["transfer_id"],
                        request["bytes"],
                        request.get("deferred", False),
                    )
                    self._json(200, {"recorded": True})
                    return
                if self.path == f"{API_PREFIX}/faults/trigger":
                    self._read_json()
                    self._json(
                        200,
                        {"triggered": owner.transfer_faults.trigger_deferred()},
                    )
                    return
                if self.path == f"{API_PREFIX}/faults/wait-trigger":
                    request = self._read_json()
                    self._json(
                        200,
                        {
                            "triggered": owner.transfer_faults.wait_trigger(
                                request["transfer_id"],
                                request.get("timeout", 30),
                            )
                        },
                    )
                    return
                if self.path == f"{API_PREFIX}/faults/event":
                    request = self._read_json()
                    owner.transfer_faults.event(request["name"])
                    self._json(200, {"recorded": True})
                    return
                if self.path == f"{API_PREFIX}/faults/claim-release":
                    self._read_json()
                    self._json(
                        200,
                        {
                            "inject": (
                                owner.transfer_faults
                                .claim_release_failure()
                            )
                        },
                    )
                    return
                if self.path == f"{API_PREFIX}/faults/complete-release":
                    self._read_json()
                    try:
                        owner.transfer_faults.complete_release_retry()
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, {"completed": True})
                    return
                if self.path in (
                    f"{API_PREFIX}/edata/resolve-source",
                    f"{API_PREFIX}/edata/resolve-sources",
                ):
                    admitted_bytes = 0
                    completed = False
                    resolved = ()
                    try:
                        request = self._read_json()
                        requests = (
                            request["requests"]
                            if self.path.endswith("resolve-sources")
                            else (request,)
                        )
                        resolved = [
                            _resolve_edata(owner, item) for item in requests
                        ]
                        admitted_bytes = sum(
                            len(payload)
                            for _, payload in resolved
                            if payload is not None
                        )
                        if admitted_bytes and not owner.byte_serving.acquire(
                            admitted_bytes
                        ):
                            self._error(
                                503, "byte serving capacity exceeded"
                            )
                            return
                        for response, payload in resolved:
                            if payload is None:
                                continue
                            if owner.serving_hook is not None:
                                owner.serving_hook(
                                    f"e:{response['data_id']}"
                                )
                            owner.state.record_edata_fetch(
                                response["data_id"]
                            )
                        if self.path.endswith("resolve-source"):
                            response, payload = resolved[0]
                            if payload is None:
                                self._json(200, response)
                                completed = True
                                return
                            self.send_response(200)
                            self.send_header(
                                "Content-Type", "application/octet-stream"
                            )
                            self.send_header(
                                "Content-Length", str(len(payload))
                            )
                            self.send_header(
                                "X-DataVine-Data-ID",
                                str(response["data_id"]),
                            )
                            self.send_header(
                                "X-DataVine-Content-SHA256",
                                response["content_hash"],
                            )
                            self.send_header(
                                "X-DataVine-Serialized-SHA256",
                                response["serialized_sha256"],
                            )
                            self.send_header(
                                "X-DataVine-Cache-Globally",
                                "1"
                                if response["cache_globally"]
                                else "0",
                            )
                            self.send_header(
                                "X-DataVine-Metadata",
                                base64.urlsafe_b64encode(
                                    json.dumps(
                                        response["metadata"],
                                        sort_keys=True,
                                        separators=(",", ":"),
                                    ).encode("ascii")
                                ).decode("ascii"),
                            )
                            self.end_headers()
                            self.wfile.write(payload)
                            completed = True
                            return
                        header = []
                        payloads = []
                        for response, payload in resolved:
                            item = dict(response)
                            item["payload_length"] = len(payload or b"")
                            header.append(item)
                            if payload is not None:
                                payloads.append(payload)
                        encoded = json.dumps(
                            header, separators=(",", ":")
                        ).encode("utf-8")
                        length = 4 + len(encoded) + admitted_bytes
                        self.send_response(200)
                        self.send_header(
                            "Content-Type",
                            "application/x-datavine-resolve-batch",
                        )
                        self.send_header("Content-Length", str(length))
                        self.end_headers()
                        self.wfile.write(struct.pack("!I", len(encoded)))
                        self.wfile.write(encoded)
                        for payload in payloads:
                            self.wfile.write(payload)
                        completed = True
                    except Exception as exc:
                        self._error(400, exc)
                    finally:
                        if not completed:
                            for response, _ in resolved:
                                lease = response.get("lease")
                                if lease is not None:
                                    try:
                                        owner.state.release_replica(
                                            lease["lease_id"], False
                                        )
                                    except Exception:
                                        pass
                        if admitted_bytes:
                            owner.byte_serving.release(
                                admitted_bytes, completed
                            )
                    return
                if self.path == f"{API_PREFIX}/workers/join":
                    try:
                        request = self._read_json()
                        worker = owner.state.join_worker(
                            request["worker_id"], request["epoch"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, dataclasses.asdict(worker))
                    return
                if self.path == f"{API_PREFIX}/workers/claim":
                    try:
                        request = self._read_json()
                        worker = owner.claim_worker(
                            request["worker_id"], request.get("endpoint")
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, dataclasses.asdict(worker))
                    return
                if self.path == f"{API_PREFIX}/workers/disconnect":
                    try:
                        request = self._read_json()
                        worker = owner.disconnect_worker(
                            request["worker_id"], request["epoch"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, dataclasses.asdict(worker))
                    return
                if self.path == f"{API_PREFIX}/workers/reconcile":
                    try:
                        request = self._read_json()
                        (
                            disconnected,
                            affected_data_ids,
                        ) = owner.reconcile_workers(
                            request["active_worker_ids"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "disconnected": [
                                dataclasses.asdict(worker)
                                for worker in disconnected
                            ],
                            "affected_data_ids": list(
                                affected_data_ids
                            ),
                        },
                    )
                    return
                if self.path in (
                    f"{API_PREFIX}/replicas/prepare",
                    f"{API_PREFIX}/replicas/report",
                ):
                    try:
                        request = self._read_json()
                        arguments = (
                            request["data_id"],
                            request["replica_id"],
                            request["attempt"],
                            request["tier"],
                            request["content_hash"],
                            request["size"],
                            request["worker_id"],
                            request["worker_epoch"],
                            request.get("source_endpoint"),
                        )
                        if self.path.endswith("/prepare"):
                            replica = owner.state.prepare_worker_replica(
                                *arguments
                            )
                        else:
                            replica = owner.state.report_worker_replica(
                                *arguments
                            )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, replica.source_dict())
                    return
                if self.path == f"{API_PREFIX}/replicas/commit":
                    try:
                        request = self._read_json()
                        replica = owner.state.commit_worker_replica(
                            request["data_id"],
                            request["replica_id"],
                            request["generation"],
                            request["attempt"],
                            request["content_hash"],
                            request["size"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, replica.source_dict())
                    return
                if self.path == f"{API_PREFIX}/replicas/prepare-outputs":
                    try:
                        request = self._read_json()
                        replicas = owner.state.prepare_worker_outputs(
                            request["worker_id"],
                            request["worker_epoch"],
                            request["outputs"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        [replica.source_dict() for replica in replicas],
                    )
                    return
                if self.path == (
                    f"{API_PREFIX}/replicas/project-events"
                ):
                    try:
                        request = self._read_json()
                        replicas = []
                        for batch in request["batches"]:
                            replicas.extend(
                                owner.state.publish_worker_outputs(
                                    batch["worker_id"],
                                    batch["worker_epoch"],
                                    batch["outputs"],
                                )
                            )
                        for replica in request.get("replicas", ()):
                            replicas.append(
                                owner.state.report_worker_replica(
                                    replica["data_id"],
                                    replica["replica_id"],
                                    replica["attempt"],
                                    replica["tier"],
                                    replica["content_hash"],
                                    replica["size"],
                                    replica["worker_id"],
                                    replica["worker_epoch"],
                                    replica.get("source_endpoint"),
                                )
                            )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        [replica.source_dict() for replica in replicas],
                    )
                    return
                if self.path == f"{API_PREFIX}/replicas/commit-outputs":
                    try:
                        request = self._read_json()
                        replicas = owner.state.commit_worker_outputs(
                            request["outputs"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        [replica.source_dict() for replica in replicas],
                    )
                    return
                if self.path == f"{API_PREFIX}/replicas/invalidate":
                    try:
                        request = self._read_json()
                        kind, data_id = str(request["data_id"]).split(
                            ":", 1
                        )
                        owner.sync_native_leases((data_id,), kind)
                        replica = owner.state.invalidate_worker_replica(
                            request["data_id"],
                            request["replica_id"],
                            request["generation"],
                            request["worker_id"],
                            request["worker_epoch"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, replica.source_dict())
                    return
                if (
                    self.path
                    == f"{API_PREFIX}/replicas/invalidate-observed"
                ):
                    try:
                        request = self._read_json()
                        replica = (
                            owner.state
                            .invalidate_observed_worker_replica(
                                request["data_id"],
                                request["replica_id"],
                                request["attempt"],
                                request["content_hash"],
                                request["size"],
                                request["worker_id"],
                                request["worker_epoch"],
                            )
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, replica.source_dict())
                    return
                if self.path == f"{API_PREFIX}/replicas/acquire":
                    try:
                        request = self._read_json()
                        lease = owner.state.acquire_replica(
                            request["data_id"],
                            request["replica_id"],
                            request["generation"],
                            request["destination_worker_id"],
                            request["destination_worker_epoch"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, dataclasses.asdict(lease))
                    return
                if self.path == f"{API_PREFIX}/replicas/resolve-source":
                    try:
                        request = self._read_json()
                        resolved = owner.state.resolve_worker_source(
                            request["data_id"],
                            request["destination_worker_id"],
                            request["transfer_id"],
                            request.get("excluded_worker_ids", ()),
                            request.get("allow_local_source", False),
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "source": resolved["source"],
                            "lease": dataclasses.asdict(
                                resolved["lease"]
                            ),
                        },
                    )
                    return
                if self.path == f"{API_PREFIX}/replicas/release":
                    try:
                        request = self._read_json()
                        lease = owner.state.release_replica(
                            request["lease_id"], request["success"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, dataclasses.asdict(lease))
                    return
                if self.path == f"{API_PREFIX}/replicas/pruned":
                    try:
                        request = self._read_json()
                        result = owner.state.confirm_worker_pruned(
                            request["data_id"],
                            request["replica_id"],
                            request["generation"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, result)
                    return
                if self.path == f"{API_PREFIX}/pruning/task-state":
                    try:
                        request = self._read_json()
                        acknowledgement = owner.state.set_task_state(
                            request["task_id"], request["state"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, acknowledgement)
                    return
                if self.path == f"{API_PREFIX}/pruning/task-states":
                    try:
                        request = self._read_json()
                        acknowledgements = owner.state.set_task_states(
                            request["task_ids"], request["state"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, acknowledgements)
                    return
                if self.path == f"{API_PREFIX}/pruning/required-output":
                    try:
                        request = self._read_json()
                        acknowledgement = owner.state.set_required_output(
                            request["data_id"],
                            request.get("required", True),
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, acknowledgement)
                    return
                if self.path == f"{API_PREFIX}/pruning/apply":
                    try:
                        request = self._read_json()
                        data_ids = request.get("data_ids")
                        owner.sync_native_leases(
                            data_ids
                            if data_ids is not None
                            else owner.state.pruning_plan()["prunable"]
                        )
                        result = owner.state.apply_pruning(
                            request["graph_revision"],
                            request["state_revision"],
                            request.get("grace_seconds", 60),
                            request.get("data_ids"),
                            request.get("now"),
                        )
                        owner.apply_native_pruning(result)
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, result)
                    return
                if self.path == f"{API_PREFIX}/pruning/continue":
                    try:
                        request = self._read_json()
                        data_ids = request.get("data_ids")
                        owner.sync_native_leases(
                            data_ids
                            if data_ids is not None
                            else owner.state.deferred_pruning_ids()
                        )
                        result = owner.state.continue_deferred_pruning(
                            request["operation_id"],
                            request.get("data_ids")
                        )
                        owner.apply_native_pruning(result)
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, result)
                    return
                if self.path == f"{API_PREFIX}/pruning/restore":
                    try:
                        request = self._read_json()
                        result = owner.state.restore_quarantined(
                            request["data_id"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, {"restored": result})
                    return
                if self.path == f"{API_PREFIX}/pruning/hard-delete":
                    try:
                        request = self._read_json()
                        result = owner.state.hard_delete_quarantined(
                            request["graph_revision"],
                            request["state_revision"],
                            request.get("now"),
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, result)
                    return
                if self.path == f"{API_PREFIX}/idata/allocate":
                    try:
                        request = self._read_json()
                        record = owner.state.allocate_idata(
                            request["producer_task_id"],
                            request.get("producer_output_index", 0),
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, {"data_id": record.data_id})
                    return
                if self.path == f"{API_PREFIX}/idata/allocate-batch":
                    try:
                        request = self._read_json()
                        records = owner.state.allocate_idata_batch(
                            request["producer_slots"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {"data_ids": [record.data_id for record in records]},
                    )
                    return
                if self.path == f"{API_PREFIX}/idata/publish-batch":
                    try:
                        request = self._read_json()
                        records = owner.state.publish_idata_batch(
                            (
                                value["data_id"],
                                value["attempt"],
                                base64.b64decode(
                                    value["payload"], validate=True
                                ),
                            )
                            for value in request["publications"]
                        )
                    except MemoryError as exc:
                        self._error(507, exc)
                        return
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        [
                            {
                                "data_id": record.data_id,
                                "content_hash": record.content_hash,
                                "size": record.serialized_size,
                                "attempt": record.attempt,
                            }
                            for record in records
                        ],
                    )
                    return
                if self.path == f"{API_PREFIX}/idata/status-batch":
                    try:
                        request = self._read_json()
                        statuses = owner.state.idata_status_batch(
                            request["data_ids"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, statuses)
                    return
                if self.path == f"{API_PREFIX}/tasks/register":
                    try:
                        record = owner.state.register_task(
                            decode_task_record(self._read_json())
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, record.to_dict())
                    return
                if self.path == f"{API_PREFIX}/tasks/register-batch":
                    try:
                        request = self._read_json()
                        task_record_format = request.get(
                            "task_record_format"
                        )
                        if task_record_format != TASK_RECORD_COMPACT_FORMAT:
                            raise DataVineSchemaError(
                                "unsupported task record format "
                                f"{task_record_format!r}",
                                path="task_record_format",
                            )
                        records = owner.state.register_tasks(
                            decode_compact_task_record(
                                value, f"tasks[{index}]"
                            )
                            for index, value in enumerate(request["tasks"])
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    if request.get("bounded_acknowledgement") is True:
                        self._json(200, {"registered": len(records)})
                    else:
                        self._json(
                            200,
                            [record.to_dict() for record in records],
                        )
                    return
                if self.path == f"{API_PREFIX}/tasks/get-batch":
                    try:
                        request = self._read_json()
                        if request.get("include_cache_values"):
                            records, cache_values = (
                                owner.state.execution_bundle(
                                    request["task_ids"]
                                )
                            )
                        else:
                            records = owner.state.get_tasks(
                                request["task_ids"]
                            )
                            cache_values = None
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "task_record_format": TASK_RECORD_COMPACT_FORMAT,
                            "tasks": [
                                encode_compact_task_record(record)
                                for record in records
                            ],
                            **(
                                {"cache_values": cache_values}
                                if cache_values is not None
                                else {}
                            ),
                        },
                    )
                    return
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/persist")
                ):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):-len("/persist")
                    ]
                    try:
                        owner.state.request_persistence(int(token))
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(202, owner.state.idata_status(int(token)))
                    return
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/persist/cancel")
                ):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):-len("/persist/cancel")
                    ]
                    try:
                        request = self._read_json()
                        action = owner.state.cancel_persistence(
                            int(token), request.get("reason", "obsolete")
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "action": action,
                            "status": owner.state.idata_status(int(token)),
                        },
                    )
                    return
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/persist/begin")
                ):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):-len("/persist/begin")
                    ]
                    try:
                        request = self._read_json()
                        job = owner.state.begin_external_persistence(
                            int(token), request["request_id"]
                        )
                    except PersistenceBusy as exc:
                        self._error(429, exc)
                        return
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, job)
                    return
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/persist/complete")
                ):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):
                        -len("/persist/complete")
                    ]
                    try:
                        request = self._read_json()
                        owner.state.complete_external_persistence(
                            int(token), request["request_id"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200, owner.state.idata_status(int(token))
                    )
                    return
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/persist/fail")
                ):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):-len("/persist/fail")
                    ]
                    try:
                        request = self._read_json()
                        action = owner.state.fail_external_persistence(
                            int(token),
                            request["request_id"],
                            request["error"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, {"action": action})
                    return
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/invalidate")
                ):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):-len("/invalidate")
                    ]
                    try:
                        owner.invalidate_native_data(f"i:{int(token)}")
                        action = owner.state.invalidate_volatile_idata(
                            int(token)
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "action": action,
                            "status": owner.state.idata_status(int(token)),
                        },
                    )
                    return
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/publish")
                ):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):-len("/publish")
                    ]
                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                        if length < 0:
                            raise ValueError("invalid request size")
                        payload = self.rfile.read(length)
                        record = owner.state.publish_idata(
                            int(token),
                            int(self.headers.get("X-DataVine-Attempt", "1")),
                            payload,
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "data_id": record.data_id,
                            "content_hash": record.content_hash,
                            "size": len(record.serialized_bytes),
                            "attempt": record.attempt,
                        },
                    )
                    return
                if (
                    self.path.startswith(f"{API_PREFIX}/idata/")
                    and self.path.endswith("/publish-metadata")
                ):
                    token = self.path[
                        len(f"{API_PREFIX}/idata/"):
                        -len("/publish-metadata")
                    ]
                    try:
                        request = self._read_json()
                        record = owner.state.publish_idata_metadata(
                            int(token),
                            request["attempt"],
                            request["content_hash"],
                            request["size"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "data_id": record.data_id,
                            "content_hash": record.content_hash,
                            "size": record.serialized_size,
                            "attempt": record.attempt,
                            "controller_inline": False,
                        },
                    )
                    return
                if self.path == f"{API_PREFIX}/edata/register-origin":
                    try:
                        request = self._read_json()
                        metadata = decode_serialization_metadata(
                            request["metadata"], "metadata"
                        )
                        record = owner.state.register_edata_origin(
                            metadata,
                            request["origin_path"],
                            request["content_hash"],
                            request["size"],
                            request["data_id"],
                            request["serialized_sha256"],
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {
                            "data_id": record.data_id,
                            "content_hash": record.content_hash,
                            "serialized_sha256": (
                                record.serialized_sha256
                            ),
                            "size": record.serialized_size,
                            "storage": (
                                "controller-memory"
                                if record.serialized_bytes is not None
                                else "bulk-origin"
                            ),
                        },
                    )
                    return
                if self.path == f"{API_PREFIX}/edata/project-batch":
                    try:
                        request = self._read_json()
                        metadata = tuple(
                            decode_serialization_metadata(
                                value, f"metadata[{index}]"
                            )
                            for index, value in enumerate(
                                request["metadata"]
                            )
                        )
                        records = owner.state.register_native_edata_batch(
                            (
                                value["data_id"],
                                metadata[int(value["metadata"])],
                                value["content_hash"],
                                value["serialized_sha256"],
                                value["size"],
                            )
                            for index, value in enumerate(request["values"])
                        )
                    except MemoryError as exc:
                        self._error(507, exc)
                        return
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(
                        200,
                        {"registered": len(records)},
                    )
                    return
                self._error(404, "not found")

        return Routes.do_POST
