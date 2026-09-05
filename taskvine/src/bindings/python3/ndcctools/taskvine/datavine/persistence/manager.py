"""Bounded, generation-aware, atomic persistence execution."""

import collections
import dataclasses
import hashlib
import os
from pathlib import Path
import queue
import threading


@dataclasses.dataclass(frozen=True)
class PersistenceRequest:
    request_id: str
    data_id: int
    attempt: int
    payload: bytes
    content_hash: str


class PersistenceManager:
    """Execute writes while ControllerState owns their semantic state."""

    def __init__(
        self,
        root,
        on_writing,
        on_complete,
        workers=1,
        fail_first=False,
        queue_capacity=64,
        terminal_capacity=1024,
        transition_hook=None,
    ):
        workers = int(workers)
        queue_capacity = int(queue_capacity)
        terminal_capacity = int(terminal_capacity)
        if workers < 1:
            raise ValueError("persistence workers must be positive")
        if queue_capacity < 1 or terminal_capacity < 1:
            raise ValueError("persistence capacities must be positive")
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._on_writing = on_writing
        self._on_complete = on_complete
        self._queue = queue.Queue(maxsize=queue_capacity)
        self._queue_capacity = queue_capacity
        self._worker_count = workers
        self._terminal_capacity = terminal_capacity
        self._transition_hook = transition_hook
        self._failures_remaining = 1 if fail_first else 0
        self._lock = threading.RLock()
        self._states = {}
        self._terminal = collections.OrderedDict()
        self._active = 0
        self._active_high_water = 0
        self._queued_high_water = 0
        self._callback_failures = 0
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

    @property
    def worker_count(self):
        return self._worker_count

    @property
    def queue_capacity(self):
        return self._queue_capacity

    def target_path(self, data_id, attempt, content_hash):
        return self.root / (
            f"idata-{int(data_id)}-attempt-{int(attempt)}-"
            f"{str(content_hash)}.pkl"
        )

    def _notify(self, request, state):
        if self._transition_hook is not None:
            self._transition_hook(request, state)

    def _complete_callback(self, request, path, error):
        try:
            self._on_complete(request, path, error)
        except Exception:
            with self._lock:
                self._callback_failures += 1

    def submit(self, request):
        if not isinstance(request, PersistenceRequest):
            raise TypeError("request must be PersistenceRequest")
        if request.data_id < 1 or request.attempt < 1:
            raise ValueError("invalid persistence DataID or attempt")
        if not request.request_id:
            raise ValueError("persistence request ID is required")
        if not isinstance(request.payload, bytes):
            raise TypeError("persistence payload must be bytes")
        actual = hashlib.sha256(request.payload).hexdigest()
        if actual != request.content_hash:
            raise ValueError("persistence request checksum mismatch")
        with self._lock:
            if (
                request.request_id in self._states
                or request.request_id in self._terminal
            ):
                raise ValueError("duplicate persistence request ID")
            self._states[request.request_id] = "queued"
            try:
                self._queue.put_nowait(request)
            except queue.Full:
                del self._states[request.request_id]
                raise RuntimeError(
                    "persistence queue admission capacity exceeded"
                ) from None
            self._queued_high_water = max(
                self._queued_high_water, self._queue.qsize()
            )
        self._notify(request, "queued")

    def cancel(self, request_id):
        request_id = str(request_id)
        with self._lock:
            state = self._states.get(request_id)
            if state is None:
                terminal = self._terminal.get(request_id)
                if terminal is None:
                    raise KeyError("unknown persistence request")
                return terminal
            if state == "queued":
                self._states[request_id] = "cancelled"
                return "cancelled"
            if state == "writing":
                self._states[request_id] = "cancelling"
                return "cancelling"
            if state in ("cancelled", "cancelling"):
                return state
            if state == "committing":
                return "too-late"
            raise RuntimeError(f"invalid persistence state {state}")

    def _begin(self, request):
        with self._lock:
            state = self._states[request.request_id]
            if state == "cancelled":
                return False
            if state != "queued":
                raise RuntimeError(
                    f"cannot begin persistence in state {state}"
                )
            self._states[request.request_id] = "writing"
            self._active += 1
            self._active_high_water = max(
                self._active_high_water, self._active
            )
            return True

    def _begin_commit(self, request):
        with self._lock:
            state = self._states[request.request_id]
            if state == "cancelling":
                return False
            if state != "writing":
                raise RuntimeError(
                    f"cannot commit persistence in state {state}"
                )
            self._states[request.request_id] = "committing"
            return True

    def _finish(self, request, terminal_state):
        with self._lock:
            state = self._states.pop(request.request_id)
            if state in ("writing", "cancelling", "committing"):
                self._active -= 1
            self._terminal[request.request_id] = terminal_state
            while len(self._terminal) > self._terminal_capacity:
                self._terminal.popitem(last=False)

    def _run(self):
        while True:
            request = self._queue.get()
            if request is None:
                self._queue.task_done()
                return
            target = self.target_path(
                request.data_id,
                request.attempt,
                request.content_hash,
            )
            temporary = self.root / (
                f".{request.request_id}.{threading.get_ident()}.tmp"
            )
            try:
                if not self._begin(request):
                    self._finish(request, "cancelled")
                    self._notify(request, "cancelled")
                    self._complete_callback(
                        request, None, "cancelled"
                    )
                    continue
                self._notify(request, "writing")
                self._on_writing(request)
                with self._lock:
                    if self._failures_remaining:
                        self._failures_remaining -= 1
                        raise IOError("injected persistence failure")
                with temporary.open("wb") as stream:
                    stream.write(request.payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                with temporary.open("rb") as stream:
                    actual = hashlib.sha256(stream.read()).hexdigest()
                if actual != request.content_hash:
                    raise IOError("durable checksum mismatch")
                self._notify(request, "before-commit")
                if not self._begin_commit(request):
                    raise InterruptedError("cancelled")
                self._notify(request, "committing")
                temporary.replace(target)
                directory_fd = os.open(self.root, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
                self._finish(request, "completed")
                self._notify(request, "completed")
                self._complete_callback(
                    request, str(target), None
                )
            except Exception as exc:
                temporary.unlink(missing_ok=True)
                with self._lock:
                    state = self._states.get(request.request_id)
                cancelled = state in ("cancelled", "cancelling")
                terminal = "cancelled" if cancelled else "failed"
                self._finish(request, terminal)
                self._notify(request, terminal)
                self._complete_callback(
                    request,
                    None,
                    "cancelled" if cancelled else str(exc),
                )
            finally:
                self._queue.task_done()

    def snapshot(self):
        with self._lock:
            states = collections.Counter(self._states.values())
            return {
                "workers": self._worker_count,
                "queue_capacity": self._queue_capacity,
                "queued": states["queued"],
                "active": self._active,
                "active_high_water": self._active_high_water,
                "queued_high_water": self._queued_high_water,
                "terminal_tombstones": len(self._terminal),
                "terminal_capacity": self._terminal_capacity,
                "callback_failures": self._callback_failures,
            }

    def stop(self):
        for _ in self._threads:
            self._queue.put(None)
        for thread in self._threads:
            thread.join(timeout=30)
            if thread.is_alive():
                raise RuntimeError("persistence thread did not stop")
