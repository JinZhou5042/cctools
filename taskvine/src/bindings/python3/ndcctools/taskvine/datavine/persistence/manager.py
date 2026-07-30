"""Bounded, acknowledged, atomic persistence worker."""

import hashlib
import os
from pathlib import Path
import queue
import threading


class PersistenceManager:
    def __init__(
        self, root, on_writing, on_complete, workers=1, fail_first=False
    ):
        if workers < 1:
            raise ValueError("persistence workers must be positive")
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._on_writing = on_writing
        self._on_complete = on_complete
        self._queue = queue.Queue()
        self._failures_remaining = 1 if fail_first else 0
        self._failure_lock = threading.Lock()
        self._threads = [
            threading.Thread(
                target=self._run,
                name=f"datavine-persistence-{index}",
                daemon=False,
            )
            for index in range(workers)
        ]
        for thread in self._threads:
            thread.start()

    def submit(self, data_id, payload, digest):
        self._queue.put((int(data_id), bytes(payload), str(digest)))

    def _run(self):
        while True:
            item = self._queue.get()
            if item is None:
                self._queue.task_done()
                return
            data_id, payload, digest = item
            self._on_writing(data_id)
            target = self.root / f"idata-{data_id}.pkl"
            temporary = self.root / f".idata-{data_id}.{threading.get_ident()}.tmp"
            try:
                with self._failure_lock:
                    if self._failures_remaining:
                        self._failures_remaining -= 1
                        raise IOError("injected persistence failure")
                with temporary.open("wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                with temporary.open("rb") as stream:
                    actual = hashlib.sha256(stream.read()).hexdigest()
                if actual != digest:
                    raise IOError("durable checksum mismatch")
                temporary.replace(target)
                directory_fd = os.open(self.root, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
                self._on_complete(data_id, str(target), None)
            except Exception as exc:
                try:
                    temporary.unlink(missing_ok=True)
                finally:
                    self._on_complete(data_id, None, str(exc))
            finally:
                self._queue.task_done()

    def stop(self):
        for _ in self._threads:
            self._queue.put(None)
        for thread in self._threads:
            thread.join(timeout=30)
            if thread.is_alive():
                raise RuntimeError("persistence thread did not stop")
