"""Data Controller POST routes."""

import base64
import dataclasses

from ..codec import (
    TASK_RECORD_COMPACT_FORMAT,
    decode_compact_task_record,
    decode_serialization_metadata,
    decode_task_record,
    encode_compact_task_record,
)
from ..protocol import API_PREFIX, DataVineSchemaError


class PostRouteFactory:
    @staticmethod
    def create(owner):
        class Routes:
            def do_POST(self):
                if not self._authorized():
                    self._error(403, "forbidden")
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
                        worker = owner.state.claim_worker(
                            request["worker_id"]
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, dataclasses.asdict(worker))
                    return
                if self.path == f"{API_PREFIX}/workers/disconnect":
                    try:
                        request = self._read_json()
                        worker = owner.state.disconnect_worker(
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
                        ) = owner.state.reconcile_workers(
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
                        result = owner.state.apply_pruning(
                            request["graph_revision"],
                            request["state_revision"],
                            request.get("grace_seconds", 60),
                            request.get("data_ids"),
                            request.get("now"),
                        )
                    except Exception as exc:
                        self._error(400, exc)
                        return
                    self._json(200, result)
                    return
                if self.path == f"{API_PREFIX}/pruning/continue":
                    try:
                        request = self._read_json()
                        result = owner.state.continue_deferred_pruning(
                            request["operation_id"],
                            request.get("data_ids")
                        )
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
                if self.path == f"{API_PREFIX}/edata/register-batch":
                    try:
                        request = self._read_json()
                        records = owner.state.register_edata_batch(
                            (
                                decode_serialization_metadata(
                                    value["metadata"],
                                    f"values[{index}].metadata",
                                ),
                                base64.b64decode(
                                    value["serialized_bytes"],
                                    validate=True,
                                ),
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
                        [
                            {
                                "data_id": record.data_id,
                                "content_hash": record.content_hash,
                                "serialized_sha256": (
                                    record.serialized_sha256
                                ),
                                "size": record.serialized_size,
                                "storage": "controller-memory",
                            }
                            for record in records
                        ],
                    )
                    return
                if self.path != f"{API_PREFIX}/edata/register":
                    self._error(404, "not found")
                    return
                try:
                    request = self._read_json()
                    metadata = decode_serialization_metadata(
                        request["metadata"], "metadata"
                    )
                    payload = base64.b64decode(
                        request["serialized_bytes"], validate=True
                    )
                    record = owner.state.register_edata(metadata, payload)
                except MemoryError as exc:
                    self._error(507, exc)
                    return
                except Exception as exc:
                    self._error(400, exc)
                    return
                self._json(
                    200,
                    {
                        "data_id": record.data_id,
                        "content_hash": record.content_hash,
                        "serialized_sha256": record.serialized_sha256,
                        "size": record.serialized_size,
                        "storage": (
                            "controller-memory"
                            if record.serialized_bytes is not None
                            else "bulk-origin"
                        ),
                    },
                )

        return Routes.do_POST
