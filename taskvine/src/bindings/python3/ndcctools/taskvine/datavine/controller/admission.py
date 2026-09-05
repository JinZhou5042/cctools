"""Bounded Controller request and byte-serving admission."""

import http.server
import threading


class ByteServingAdmission:
    """Bound concurrent Controller byte responses without owning payloads."""

    def __init__(self, max_concurrency, max_inflight_bytes):
        self.max_concurrency = int(max_concurrency)
        self.max_inflight_bytes = int(max_inflight_bytes)
        if self.max_concurrency < 1 or self.max_inflight_bytes < 1:
            raise ValueError("byte-serving capacities must be positive")
        self._lock = threading.Lock()
        self._active = 0
        self._inflight_bytes = 0
        self._active_high_water = 0
        self._bytes_high_water = 0
        self._admitted = 0
        self._rejected = 0
        self._bytes_served = 0

    def acquire(self, size):
        size = int(size)
        if size < 0:
            raise ValueError("serving size cannot be negative")
        with self._lock:
            if (
                self._active >= self.max_concurrency
                or self._inflight_bytes + size > self.max_inflight_bytes
            ):
                self._rejected += 1
                return False
            self._active += 1
            self._inflight_bytes += size
            self._admitted += 1
            self._active_high_water = max(
                self._active_high_water, self._active
            )
            self._bytes_high_water = max(
                self._bytes_high_water, self._inflight_bytes
            )
            return True

    def release(self, size, completed):
        size = int(size)
        with self._lock:
            if self._active < 1 or self._inflight_bytes < size:
                raise RuntimeError("invalid byte-serving release")
            self._active -= 1
            self._inflight_bytes -= size
            if completed:
                self._bytes_served += size

    def snapshot(self):
        with self._lock:
            return {
                "active": self._active,
                "active_capacity": self.max_concurrency,
                "active_high_water": self._active_high_water,
                "inflight_bytes": self._inflight_bytes,
                "inflight_byte_capacity": self.max_inflight_bytes,
                "inflight_byte_high_water": self._bytes_high_water,
                "admitted": self._admitted,
                "rejected": self._rejected,
                "bytes_served": self._bytes_served,
            }


class BoundedThreadingHTTPServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, handler, max_requests):
        self.max_requests = int(max_requests)
        if self.max_requests < 1:
            raise ValueError("request concurrency must be positive")
        self._request_slots = threading.BoundedSemaphore(
            self.max_requests
        )
        self._request_lock = threading.Lock()
        self._request_active = 0
        self._request_high_water = 0
        self._request_rejected = 0
        super().__init__(address, handler)

    def process_request(self, request, client_address):
        if not self._request_slots.acquire(blocking=False):
            with self._request_lock:
                self._request_rejected += 1
            try:
                request.sendall(
                    b"HTTP/1.1 503 Service Unavailable\r\n"
                    b"Content-Length: 0\r\n"
                    b"Connection: close\r\n\r\n"
                )
            finally:
                self.shutdown_request(request)
            return
        with self._request_lock:
            self._request_active += 1
            self._request_high_water = max(
                self._request_high_water, self._request_active
            )
        try:
            super().process_request(request, client_address)
        except Exception:
            self._release_request_slot()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._release_request_slot()

    def _release_request_slot(self):
        with self._request_lock:
            self._request_active -= 1
        self._request_slots.release()

    def admission_snapshot(self):
        with self._request_lock:
            return {
                "active": self._request_active,
                "active_capacity": self.max_requests,
                "active_high_water": self._request_high_water,
                "rejected": self._request_rejected,
            }
