"""Validated TaskVine Manager tuning for one DataVine workflow run."""

import dataclasses


@dataclasses.dataclass(frozen=True)
class RuntimeTuning:
    library_batch_size: int
    peer_source_losses: int
    peer_source_loss_after_bytes: int
    defer_peer_source_loss_after_bytes: bool
    peer_corruptions: int
    idata_release_failures: int
    peer_release_retry_seconds: float
    peer_release_capacity: int


def _tune(manager, name, value, rejection):
    if manager.tune(name, value) != 0:
        raise RuntimeError(rejection)


def configure_runtime(
    manager,
    *,
    library_batch_size,
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
    """Validate options, tune the Manager, and return canonical values."""

    library_batch_size = int(library_batch_size)
    if library_batch_size < 1:
        raise ValueError("library batch size must be positive")

    admission_items = (
        -1
        if worker_disk_cache_admission_items is None
        else int(worker_disk_cache_admission_items)
    )
    if admission_items < -1:
        raise ValueError(
            "worker disk cache admission item capacity is negative"
        )
    _tune(
        manager,
        "datavine-cache-capacity-items",
        admission_items,
        "TaskVine Manager rejected cache admission capacity",
    )

    admission_bytes = (
        -1
        if worker_disk_cache_admission_bytes is None
        else int(worker_disk_cache_admission_bytes)
    )
    if admission_bytes < -1:
        raise ValueError(
            "worker disk cache admission byte capacity is negative"
        )
    _tune(
        manager,
        "datavine-cache-capacity-bytes",
        admission_bytes,
        "TaskVine Manager rejected cache byte admission capacity",
    )

    peer_source_losses = int(peer_source_losses)
    if peer_source_losses < 0:
        raise ValueError("peer source-loss injection count is negative")
    _tune(
        manager,
        "datavine-fault-peer-source-loss",
        peer_source_losses,
        "TaskVine Manager rejected peer source-loss injection",
    )

    peer_source_loss_after_bytes = int(peer_source_loss_after_bytes)
    if peer_source_loss_after_bytes < 0:
        raise ValueError("peer source-loss byte threshold is negative")
    _tune(
        manager,
        "datavine-fault-peer-source-loss-after-bytes",
        peer_source_loss_after_bytes,
        "TaskVine Manager rejected byte-counted peer source-loss injection",
    )

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
    _tune(
        manager,
        "datavine-fault-peer-source-loss-after-bytes-deferred",
        int(defer_peer_source_loss_after_bytes),
        "TaskVine Manager rejected deferred peer source loss",
    )

    peer_corruptions = int(peer_corruptions)
    if peer_corruptions < 0:
        raise ValueError("peer corruption count is negative")
    _tune(
        manager,
        "datavine-fault-peer-corruption",
        peer_corruptions,
        "TaskVine Manager rejected peer corruption injection",
    )

    idata_release_failures = int(idata_release_failures)
    if idata_release_failures < 0:
        raise ValueError("IData release failure count is negative")
    _tune(
        manager,
        "datavine-fault-idata-release-failure",
        idata_release_failures,
        "TaskVine Manager rejected IData release failure injection",
    )

    peer_release_retry_seconds = float(peer_release_retry_seconds)
    if peer_release_retry_seconds < 0:
        raise ValueError("peer release retry delay is negative")
    _tune(
        manager,
        "datavine-transfer-release-retry-seconds",
        peer_release_retry_seconds,
        "TaskVine Manager rejected peer release retry delay",
    )

    peer_release_capacity = int(peer_release_capacity)
    if peer_release_capacity < 1:
        raise ValueError("peer release capacity is below one")
    _tune(
        manager,
        "datavine-transfer-release-capacity",
        peer_release_capacity,
        "TaskVine Manager rejected peer release capacity",
    )

    return RuntimeTuning(
        library_batch_size=library_batch_size,
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
