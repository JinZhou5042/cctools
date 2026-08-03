"""Process-local caches shared by persistent worker-library calls."""

import dataclasses
import threading


@dataclasses.dataclass
class WorkerProcessCache:
    clients: dict = dataclasses.field(default_factory=dict)
    worker_claims: dict = dataclasses.field(default_factory=dict)
    task_records: dict = dataclasses.field(default_factory=dict)
    edata_metadata: dict = dataclasses.field(default_factory=dict)
    replica_reports: dict = dataclasses.field(default_factory=dict)
    lock: threading.RLock = dataclasses.field(
        default_factory=threading.RLock
    )

    def clear(self):
        with self.lock:
            self.clients.clear()
            self.worker_claims.clear()
            self.task_records.clear()
            self.edata_metadata.clear()
            self.replica_reports.clear()


PROCESS_CACHE = WorkerProcessCache()
