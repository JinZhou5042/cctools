"""Worker replica observation and invalidation."""

import json
import time


class WorkerReplicaReporter:
    def __init__(
        self,
        client,
        controller,
        token,
        worker_id,
        worker_epoch,
        emit,
        process_cache,
    ):
        self.client = client
        self.controller = controller
        self.token = token
        self.worker_id = worker_id
        self.worker_epoch = int(worker_epoch)
        self.emit = emit
        self.process_cache = process_cache

    def replica_id(self, data_key):
        return f"taskvine-{self.worker_id}-{data_key.replace(':', '-')}"

    def report_local(self, data_key, attempt, content_hash, payload):
        report_key = (
            self.controller,
            self.token,
            self.worker_id,
            self.worker_epoch,
            data_key,
            int(attempt),
            content_hash,
            len(payload),
        )
        cached_report = self.process_cache.replica_reports.get(report_key)
        if (
            cached_report is None
            or time.monotonic() - cached_report[0] >= 1.0
        ):
            replica = self.client.report_replica(
                data_key,
                self.replica_id(data_key),
                attempt,
                "worker-disk",
                content_hash,
                len(payload),
                self.worker_id,
                self.worker_epoch,
            )
            self.process_cache.replica_reports[report_key] = (
                time.monotonic(),
                replica,
            )
        else:
            replica = cached_report[1]
        self.emit(
            "DATAVINE_REPLICA_OBSERVED "
            + json.dumps(
                {
                    "data_id": replica["data_id"],
                    "replica_id": replica["replica_id"],
                    "generation": replica["generation"],
                    "attempt": replica["attempt"],
                    "content_hash": replica["content_hash"],
                    "size": replica["size"],
                    "worker_id": self.worker_id,
                    "worker_epoch": self.worker_epoch,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )

    def reject_local(self, data_key):
        replica_id = self.replica_id(data_key)
        for source in self.client.replica_sources(data_key)["sources"]:
            if source["replica_id"] != replica_id:
                continue
            self.client.invalidate_replica(
                data_key,
                replica_id,
                source["generation"],
                self.worker_id,
                self.worker_epoch,
            )
            for report_key in tuple(self.process_cache.replica_reports):
                if (
                    report_key[2] == self.worker_id
                    and report_key[4] == data_key
                ):
                    self.process_cache.replica_reports.pop(report_key, None)
            return
