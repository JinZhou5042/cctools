"""Controller IData persistence state machine."""

import dataclasses
import hashlib
import os
from pathlib import Path

from ..persistence.manager import PersistenceRequest


class PersistenceStateMixin:
    def request_persistence(self, data_id):
        with self._lock:
            if self._persistence is None:
                raise RuntimeError("persistence is disabled")
            old = self.get_idata(data_id)
            if old.durability in ("queued", "writing", "durable"):
                return old
            self._persistence_sequence += 1
            request_id = (
                f"i{old.data_id}-a{old.attempt}-"
                f"p{self._persistence_sequence}"
            )
            if old.serialized_bytes is None:
                if not self.replicas.candidates(f"i:{old.data_id}"):
                    raise ValueError(
                        "cannot persist unavailable IData"
                    )
                active_external = sum(
                    job.get("mode") == "worker"
                    and job["state"] in (
                        "queued",
                        "writing",
                        "cancelling",
                    )
                    for job in self._persistence_jobs.values()
                )
                capacity = (
                    self._persistence.queue_capacity
                    + self._persistence.worker_count
                )
                if active_external >= capacity:
                    raise RuntimeError(
                        "external persistence admission capacity exceeded"
                    )
                target = self._persistence.target_path(
                    old.data_id, old.attempt, old.content_hash
                )
                record = dataclasses.replace(old, durability="queued")
                self._idata[old.data_id] = record
                self._persistence_jobs[old.data_id] = {
                    "request_id": request_id,
                    "attempt": old.attempt,
                    "content_hash": old.content_hash,
                    "size": old.serialized_size,
                    "state": "queued",
                    "cancel_reason": None,
                    "mode": "worker",
                    "target_path": str(target),
                }
                self._persistence_requests += 1
                self.pruning.set_data_state(
                    old.data_id, persistence="queued"
                )
                return record
            request = PersistenceRequest(
                request_id=(
                    request_id
                ),
                data_id=old.data_id,
                attempt=old.attempt,
                payload=old.serialized_bytes,
                content_hash=old.content_hash,
            )
            record = dataclasses.replace(old, durability="queued")
            self._idata[old.data_id] = record
            self._persistence_jobs[old.data_id] = {
                "request_id": request.request_id,
                "attempt": request.attempt,
                "content_hash": request.content_hash,
                "state": "queued",
                "cancel_reason": None,
                "mode": "controller",
            }
            self._persistence_requests += 1
            try:
                self._persistence.submit(request)
            except Exception:
                self._idata[old.data_id] = old
                self._persistence_jobs.pop(old.data_id, None)
                self._persistence_requests -= 1
                raise
            self.pruning.set_data_state(
                old.data_id, persistence="queued"
            )
            return record

    def begin_external_persistence(self, data_id, request_id):
        with self._lock:
            old = self.get_idata(data_id)
            job = self._persistence_jobs.get(old.data_id)
            if (
                job is None
                or job.get("mode") != "worker"
                or job["request_id"] != str(request_id)
            ):
                raise ValueError("unknown external persistence request")
            if job["state"] == "durable":
                return dict(job)
            if job["state"] == "writing":
                return dict(job)
            if job["state"] != "queued":
                raise ValueError(
                    f"cannot begin persistence in state {job['state']}"
                )
            if (
                len(self._persistence_active_ids)
                >= self._persistence.worker_count
            ):
                raise RuntimeError(
                    "global persistence concurrency exceeded"
                )
            job["state"] = "writing"
            self._idata[old.data_id] = dataclasses.replace(
                old, durability="writing"
            )
            self._persistence_active_ids.add(job["request_id"])
            self._persistence_active = len(
                self._persistence_active_ids
            )
            self._persistence_max_active = max(
                self._persistence_max_active,
                self._persistence_active,
            )
            self.pruning.set_data_state(
                old.data_id, persistence="writing"
            )
            return dict(job)

    def complete_external_persistence(self, data_id, request_id):
        cancelled_target = None
        with self._lock:
            old = self.get_idata(data_id)
            job = self._persistence_jobs.get(old.data_id)
            if (
                job is not None
                and job.get("mode") == "worker"
                and job["request_id"] == str(request_id)
                and job["state"] == "durable"
                and old.durability == "durable"
                and old.durable_path == job["target_path"]
            ):
                return old
            if (
                job is not None
                and job.get("mode") == "worker"
                and job["request_id"] == str(request_id)
                and job["state"] in ("cancelled", "cancelling")
            ):
                cancelled_target = Path(job["target_path"])
                job["state"] = "cancelled"
                self._release_persistence_slot(job["request_id"])
                record = dataclasses.replace(
                    old,
                    durability="cancelled",
                    durable_path=None,
                )
                self._idata[old.data_id] = record
                self.pruning.set_data_state(
                    old.data_id, persistence="none"
                )
            else:
                record = None
            if record is None:
                if (
                    job is None
                    or job.get("mode") != "worker"
                    or job["request_id"] != str(request_id)
                    or job["state"] != "writing"
                    or old.attempt != job["attempt"]
                    or old.content_hash != job["content_hash"]
                    or old.serialized_size != job["size"]
                ):
                    raise ValueError(
                        "stale external persistence completion"
                    )
                target = Path(job["target_path"])
                expected = {
                    key: job[key]
                    for key in (
                        "request_id",
                        "attempt",
                        "content_hash",
                        "size",
                        "target_path",
                    )
                }
        if cancelled_target is not None:
            cancelled_target.unlink(missing_ok=True)
            return record
        digest = hashlib.sha256()
        size = 0
        with target.open("rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                digest.update(chunk)
        if (
            size != expected["size"]
            or digest.hexdigest() != expected["content_hash"]
        ):
            target.unlink(missing_ok=True)
            raise IOError("external persistence validation failed")
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        with self._lock:
            old = self.get_idata(data_id)
            job = self._persistence_jobs.get(old.data_id)
            if (
                job is not None
                and job.get("mode") == "worker"
                and job["request_id"] == expected["request_id"]
                and job["state"] in ("cancelled", "cancelling")
            ):
                job["state"] = "cancelled"
                self._release_persistence_slot(job["request_id"])
                record = dataclasses.replace(
                    old,
                    durability="cancelled",
                    durable_path=None,
                )
                self._idata[old.data_id] = record
                self.pruning.set_data_state(
                    old.data_id, persistence="none"
                )
                cancelled_after_validation = True
            else:
                cancelled_after_validation = False
            if (
                not cancelled_after_validation
                and (
                    job is None
                    or job.get("mode") != "worker"
                    or job["state"] != "writing"
                    or any(
                        job[key] != value
                        for key, value in expected.items()
                    )
                    or old.attempt != expected["attempt"]
                    or old.content_hash != expected["content_hash"]
                    or old.serialized_size != expected["size"]
                )
            ):
                target.unlink(missing_ok=True)
                self._persistence_stale_completions += 1
                raise ValueError("stale external persistence completion")
            if cancelled_after_validation:
                target.unlink(missing_ok=True)
                return record
            self._publish_replica(
                f"i:{old.data_id}",
                (
                    f"sharedfs-idata-{old.data_id}-"
                    f"attempt-{old.attempt}"
                ),
                old.attempt,
                "sharedfs",
                old.content_hash,
                old.serialized_size,
            )
            job["state"] = "durable"
            self._release_persistence_slot(job["request_id"])
            record = dataclasses.replace(
                old, durability="durable", durable_path=str(target)
            )
            self._idata[old.data_id] = record
            self.pruning.set_data_state(
                old.data_id,
                available=True,
                durable=True,
                persistence="none",
            )
            return record

    def fail_external_persistence(
        self, data_id, request_id, error
    ):
        with self._lock:
            old = self.get_idata(data_id)
            job = self._persistence_jobs.get(old.data_id)
            if (
                job is None
                or job.get("mode") != "worker"
                or job["request_id"] != str(request_id)
            ):
                return "stale"
            if job["state"] == "durable":
                return "too-late"
            cancelled = job["state"] in (
                "cancelled",
                "cancelling",
            )
            job["state"] = "cancelled" if cancelled else "failed"
            self._release_persistence_slot(job["request_id"])
            self._idata[old.data_id] = dataclasses.replace(
                old,
                durability="cancelled" if cancelled else "failed",
                durable_path=None,
            )
            if cancelled:
                Path(job["target_path"]).unlink(missing_ok=True)
            else:
                self._persistence_failures[old.data_id] = str(error)
            self.pruning.set_data_state(
                old.data_id, persistence="none"
            )
            return "cancelled" if cancelled else "failed"

    def cancel_persistence(self, data_id, reason="obsolete"):
        with self._lock:
            return self._cancel_persistence_locked(data_id, reason)

    def _cancel_persistence_locked(self, data_id, reason):
        data_id = int(data_id)
        job = self._persistence_jobs.get(data_id)
        if job is None or job["state"] not in ("queued", "writing"):
            return "not-active"
        if job.get("mode") == "worker":
            if job["state"] == "queued":
                result = "cancelled"
            else:
                result = "cancelling"
            job["state"] = result
        else:
            result = self._persistence.cancel(job["request_id"])
        if result in ("cancelled", "cancelling"):
            job["state"] = result
            job["cancel_reason"] = str(reason)
            self._persistence_capacity.notify_all()
            old = self.get_idata(data_id)
            self._idata[data_id] = dataclasses.replace(
                old, durability="cancelled"
            )
            self.pruning.set_data_state(
                data_id, persistence="none"
            )
        return result

    def _persistence_writing(self, request):
        with self._lock:
            job = self._persistence_jobs.get(request.data_id)
            current_idata = self._idata.get(request.data_id)
            if (
                job is None
                or current_idata is None
                or job["request_id"] != request.request_id
                or job["attempt"] != request.attempt
                or job["content_hash"] != request.content_hash
                or current_idata.attempt != request.attempt
                or current_idata.content_hash != request.content_hash
            ):
                return
            while (
                len(self._persistence_active_ids)
                >= self._persistence.worker_count
            ):
                if job["state"] in ("cancelled", "cancelling"):
                    raise InterruptedError("cancelled")
                self._persistence_capacity.wait()
            job["state"] = "writing"
            self._persistence_active_ids.add(request.request_id)
            self._persistence_active = len(
                self._persistence_active_ids
            )
            self._persistence_max_active = max(
                self._persistence_max_active,
                self._persistence_active,
            )
            old = current_idata
            self._idata[request.data_id] = dataclasses.replace(
                old, durability="writing"
            )
            self.pruning.set_data_state(
                request.data_id, persistence="writing"
            )

    def _persistence_complete(self, request, path, error):
        with self._lock:
            self._release_persistence_slot(request.request_id)
            job = self._persistence_jobs.get(request.data_id)
            old = self._idata.get(request.data_id)
            current = (
                job is not None
                and old is not None
                and job["request_id"] == request.request_id
                and old.attempt == request.attempt
                and old.content_hash == request.content_hash
            )
            if not current:
                self._persistence_stale_completions += 1
                if path is not None:
                    self._discard_persistence_path(path)
                return
            if error == "cancelled":
                job["state"] = "cancelled"
                self._idata[request.data_id] = dataclasses.replace(
                    old, durability="cancelled", durable_path=None
                )
                self.pruning.set_data_state(
                    request.data_id, persistence="none"
                )
                return
            if error is not None:
                job["state"] = "failed"
                self._idata[request.data_id] = dataclasses.replace(
                    old, durability="failed", durable_path=None
                )
                self.pruning.set_data_state(
                    request.data_id, persistence="none"
                )
                self._persistence_failures[request.data_id] = error
                return
            try:
                self._publish_replica(
                    f"i:{request.data_id}",
                    (
                        f"sharedfs-idata-{request.data_id}-"
                        f"attempt-{request.attempt}"
                    ),
                    request.attempt,
                    "sharedfs",
                    request.content_hash,
                    len(request.payload),
                )
            except Exception as exc:
                self._discard_persistence_path(path)
                job["state"] = "failed"
                self._idata[request.data_id] = dataclasses.replace(
                    old, durability="failed", durable_path=None
                )
                self.pruning.set_data_state(
                    request.data_id, persistence="none"
                )
                self._persistence_failures[request.data_id] = str(exc)
                return
            job["state"] = "durable"
            self._idata[request.data_id] = dataclasses.replace(
                old, durability="durable", durable_path=path
            )
            self.pruning.set_data_state(
                request.data_id,
                available=True,
                durable=True,
                persistence="none",
            )
            self._persistence_failures.pop(request.data_id, None)

    def _discard_persistence_path(self, path):
        try:
            Path(path).unlink(missing_ok=True)
        except OSError:
            self._persistence_cleanup_failures += 1
