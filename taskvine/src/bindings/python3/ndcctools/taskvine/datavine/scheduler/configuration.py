"""Validated runtime options for one DataVine workflow run."""

import dataclasses


@dataclasses.dataclass(frozen=True)
class RuntimeTuning:
    peer_source_losses: int
    peer_source_loss_after_bytes: int
    defer_peer_source_loss_after_bytes: bool
    peer_corruptions: int
    idata_release_failures: int
    peer_release_retry_seconds: float
    peer_release_capacity: int


def configure_runtime(
    *,
    worker_disk_cache_admission_items,
    worker_disk_cache_admission_bytes,
    peer_source_losses,
    peer_source_loss_after_bytes,
    defer_peer_source_loss_after_bytes,
    peer_corruptions,
    idata_release_failures,
    peer_release_retry_seconds,
    peer_release_capacity,
):
    """Validate options and return canonical values."""
    if (
        worker_disk_cache_admission_items is not None
        and int(worker_disk_cache_admission_items) < 0
    ):
        raise ValueError(
            "worker disk cache admission item capacity is negative"
        )
    if (
        worker_disk_cache_admission_bytes is not None
        and int(worker_disk_cache_admission_bytes) < 0
    ):
        raise ValueError(
            "worker disk cache admission byte capacity is negative"
        )

    peer_source_losses = int(peer_source_losses)
    if peer_source_losses < 0:
        raise ValueError("peer source-loss injection count is negative")

    peer_source_loss_after_bytes = int(peer_source_loss_after_bytes)
    if peer_source_loss_after_bytes < 0:
        raise ValueError("peer source-loss byte threshold is negative")

    defer_peer_source_loss_after_bytes = bool(
        defer_peer_source_loss_after_bytes
    )
    if (
        defer_peer_source_loss_after_bytes
        and peer_source_loss_after_bytes <= 0
    ):
        raise ValueError(
            "deferred peer source loss requires a positive byte threshold"
        )

    peer_corruptions = int(peer_corruptions)
    if peer_corruptions < 0:
        raise ValueError("peer corruption count is negative")

    idata_release_failures = int(idata_release_failures)
    if idata_release_failures < 0:
        raise ValueError("IData release failure count is negative")

    peer_release_retry_seconds = float(peer_release_retry_seconds)
    if peer_release_retry_seconds < 0:
        raise ValueError("peer release retry delay is negative")

    peer_release_capacity = int(peer_release_capacity)
    if peer_release_capacity < 1:
        raise ValueError("peer release capacity is below one")

    return RuntimeTuning(
        peer_source_losses=peer_source_losses,
        peer_source_loss_after_bytes=peer_source_loss_after_bytes,
        defer_peer_source_loss_after_bytes=(
            defer_peer_source_loss_after_bytes
        ),
        peer_corruptions=peer_corruptions,
        idata_release_failures=idata_release_failures,
        peer_release_retry_seconds=peer_release_retry_seconds,
        peer_release_capacity=peer_release_capacity,
    )
