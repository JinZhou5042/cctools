"""Deterministic fault coordination for worker-direct transfer tests."""

import threading
import time


_COUNTERS = (
    "peer_transfer_starts",
    "peer_transfer_progress_events",
    "peer_transfer_progress_max_bytes",
    "deferred_peer_source_loss_pauses",
    "deferred_peer_source_loss_triggers",
    "deferred_peer_source_loss_expirations",
    "peer_transfer_cleanup_reports",
    "peer_transfer_cleanup_absent",
    "peer_source_losses_injected",
    "peer_corruptions_injected",
    "peer_corruptions_rejected",
    "peer_alternate_source_fallbacks",
    "peer_release_failures_injected",
    "peer_release_retries_succeeded",
    "peer_release_pending_high_water",
    "peer_release_capacity_backpressure",
)


class TransferFaults:
    def __init__(self):
        self._condition = threading.Condition()
        self.configure()

    def configure(
        self,
        source_losses=0,
        source_loss_after_bytes=0,
        defer_source_loss=False,
        corruptions=0,
        release_failures=0,
        release_capacity=1024,
    ):
        with self._condition:
            self._source_losses = int(source_losses)
            self._source_loss_after_bytes = int(source_loss_after_bytes)
            self._partial_remaining = int(self._source_loss_after_bytes > 0)
            self._defer_source_loss = bool(defer_source_loss)
            self._corruptions = int(corruptions)
            self._release_failures = int(release_failures)
            self._release_capacity = int(release_capacity)
            self._release_pending = 0
            self._deferred_transfer = None
            self._triggered = set()
            self._corrupt_fallback_pending = 0
            self._cleanup_pending = 0
            self._metrics = {name: 0 for name in _COUNTERS}
            self._condition.notify_all()

    def claim_transfer(self, transfer_id, size):
        with self._condition:
            self._metrics["peer_transfer_starts"] += 1
            if int(size) < 65536:
                return {"action": "none"}
            if self._corruptions:
                self._corruptions -= 1
                self._metrics["peer_corruptions_injected"] += 1
                return {"action": "corrupt"}
            if self._partial_remaining:
                self._partial_remaining -= 1
                return {
                    "action": "source-loss-after-bytes",
                    "bytes": self._source_loss_after_bytes,
                    "deferred": self._defer_source_loss,
                }
            if self._source_losses:
                self._source_losses -= 1
                self._metrics["peer_source_losses_injected"] += 1
                return {"action": "source-loss"}
            return {"action": "none"}

    def progress(self, transfer_id, byte_count, deferred):
        with self._condition:
            byte_count = int(byte_count)
            self._metrics["peer_transfer_progress_events"] += 1
            self._metrics["peer_transfer_progress_max_bytes"] = max(
                self._metrics["peer_transfer_progress_max_bytes"],
                byte_count,
            )
            self._cleanup_pending += 1
            if deferred:
                self._deferred_transfer = str(transfer_id)
                self._metrics["deferred_peer_source_loss_pauses"] += 1
            else:
                self._metrics["peer_source_losses_injected"] += 1
            self._condition.notify_all()

    def trigger_deferred(self):
        with self._condition:
            if self._deferred_transfer is None:
                return False
            transfer_id = self._deferred_transfer
            self._deferred_transfer = None
            self._triggered.add(transfer_id)
            self._metrics["deferred_peer_source_loss_triggers"] += 1
            self._metrics["peer_source_losses_injected"] += 1
            self._condition.notify_all()
            return True

    def wait_trigger(self, transfer_id, timeout=30):
        deadline = time.monotonic() + float(timeout)
        with self._condition:
            while str(transfer_id) not in self._triggered:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._metrics[
                        "deferred_peer_source_loss_expirations"
                    ] += 1
                    return False
                self._condition.wait(remaining)
            self._triggered.remove(str(transfer_id))
            return True

    def event(self, name):
        with self._condition:
            if name == "corruption-rejected":
                self._metrics["peer_corruptions_rejected"] += 1
                self._corrupt_fallback_pending = 1
            elif name == "alternate-source-fallback":
                self._metrics["peer_alternate_source_fallbacks"] += 1
                self._corrupt_fallback_pending = 0
            elif name == "partial-cleanup":
                self._metrics["peer_transfer_cleanup_reports"] += 1
                self._metrics["peer_transfer_cleanup_absent"] += 1
                self._cleanup_pending = max(0, self._cleanup_pending - 1)
            else:
                raise ValueError(f"unknown transfer fault event {name!r}")

    def claim_release_failure(self):
        with self._condition:
            if self._release_pending >= self._release_capacity:
                self._metrics["peer_release_capacity_backpressure"] += 1
                return False
            if not self._release_failures:
                return False
            self._release_failures -= 1
            self._release_pending += 1
            self._metrics["peer_release_failures_injected"] += 1
            self._metrics["peer_release_pending_high_water"] = max(
                self._metrics["peer_release_pending_high_water"],
                self._release_pending,
            )
            return True

    def complete_release_retry(self):
        with self._condition:
            if not self._release_pending:
                raise RuntimeError("no pending peer release retry")
            self._release_pending -= 1
            self._metrics["peer_release_retries_succeeded"] += 1

    def snapshot(self):
        with self._condition:
            return {
                **self._metrics,
                "deferred_peer_source_loss_pending": int(
                    self._deferred_transfer is not None
                ),
                "peer_transfer_cleanup_pending": self._cleanup_pending,
                "peer_corrupt_fallback_pending": (
                    self._corrupt_fallback_pending
                ),
                "peer_release_pending": self._release_pending,
                "peer_release_pending_capacity": self._release_capacity,
            }
