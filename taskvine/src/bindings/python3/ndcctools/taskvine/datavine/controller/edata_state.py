"""Controller EData registration and lookup state."""

import hashlib
from pathlib import Path

from ..models import EDataRecord, SerializationMetadata


class EDataStateMixin:
    def register_edata(self, metadata, serialized_bytes):
        if not isinstance(metadata, SerializationMetadata):
            raise TypeError("metadata must be SerializationMetadata")
        if not isinstance(serialized_bytes, bytes):
            raise TypeError("serialized_bytes must be bytes")
        digest = EDataRecord.digest(metadata, serialized_bytes)
        bucket_key = (metadata, digest)
        with self._lock:
            self._registrations += 1
            for data_id in self._buckets.get(bucket_key, ()):
                record = self._edata[data_id]
                if self._edata_matches_bytes(record, serialized_bytes):
                    return record
            projected_bytes = self._edata_bytes + len(serialized_bytes)
            if projected_bytes > self.max_edata_bytes:
                raise MemoryError("Controller EData capacity exceeded")
            data_id = self._next_edata_id
            record = EDataRecord(
                data_id,
                digest,
                hashlib.sha256(serialized_bytes).hexdigest(),
                metadata,
                serialized_bytes,
            )
            self._publish_replica(
                f"e:{data_id}",
                f"controller-edata-{data_id}",
                1,
                "controller-memory",
                digest,
                len(serialized_bytes),
            )
            self._next_edata_id += 1
            self._edata[data_id] = record
            self._buckets.setdefault(bucket_key, []).append(data_id)
            self._edata_bytes += len(serialized_bytes)
            return record

    def register_edata_batch(self, values):
        with self._lock:
            return tuple(
                self.register_edata(metadata, payload)
                for metadata, payload in values
            )

    @staticmethod
    def _edata_matches_bytes(record, serialized_bytes):
        if record.serialized_bytes is not None:
            return record.serialized_bytes == serialized_bytes
        if len(serialized_bytes) != record.serialized_size:
            return False
        view = memoryview(serialized_bytes)
        offset = 0
        with open(record.stable_path, "rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                if chunk != view[offset:offset + len(chunk)]:
                    return False
                offset += len(chunk)
        return offset == len(serialized_bytes)

    @staticmethod
    def _edata_origins_equal(left, right, size):
        remaining = int(size)
        with open(left, "rb") as first, open(right, "rb") as second:
            while remaining:
                amount = min(1024 * 1024, remaining)
                if first.read(amount) != second.read(amount):
                    return False
                remaining -= amount
            return first.read(1) == second.read(1) == b""

    @staticmethod
    def _edata_inline_origin_equal(serialized_bytes, path):
        view = memoryview(serialized_bytes)
        offset = 0
        with open(path, "rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                if chunk != view[offset:offset + len(chunk)]:
                    return False
                offset += len(chunk)
        return offset == len(serialized_bytes)

    def register_edata_origin(
        self, metadata, stable_path, content_hash, serialized_size
    ):
        if not isinstance(metadata, SerializationMetadata):
            raise TypeError("metadata must be SerializationMetadata")
        if self.bulk_origin_root is None:
            raise RuntimeError("Controller bulk origin is not configured")
        requested = Path(stable_path)
        if requested.is_symlink():
            raise ValueError("bulk origin cannot be a symbolic link")
        resolved = requested.resolve(strict=True)
        try:
            resolved.relative_to(self.bulk_origin_root)
        except ValueError:
            raise ValueError("bulk origin escapes configured root") from None
        if not resolved.is_file():
            raise ValueError("bulk origin must be a regular file")
        if resolved.name != f"edata-{content_hash}.pkl":
            raise ValueError("bulk origin filename is not content-addressed")
        stat = resolved.stat()
        serialized_size = int(serialized_size)
        if stat.st_size != serialized_size:
            raise ValueError("bulk origin size mismatch")
        digest = hashlib.sha256()
        digest.update(metadata.identity_bytes())
        digest.update(b"\0")
        serialized_digest = hashlib.sha256()
        with resolved.open("rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                serialized_digest.update(chunk)
        if digest.hexdigest() != str(content_hash):
            raise ValueError("bulk origin content hash mismatch")
        bucket_key = (metadata, str(content_hash))
        with self._lock:
            self._registrations += 1
            for data_id in self._buckets.get(bucket_key, ()):
                record = self._edata[data_id]
                if record.serialized_size != serialized_size:
                    continue
                if record.serialized_bytes is not None:
                    if self._edata_inline_origin_equal(
                        record.serialized_bytes, resolved
                    ):
                        return record
                elif self._edata_origins_equal(
                    record.stable_path, resolved, serialized_size
                ):
                    return record
            data_id = self._next_edata_id
            record = EDataRecord(
                data_id,
                str(content_hash),
                serialized_digest.hexdigest(),
                metadata,
                None,
                str(resolved),
                serialized_size,
            )
            self._publish_replica(
                f"e:{data_id}",
                f"bulk-origin-edata-{data_id}",
                1,
                "sharedfs",
                record.content_hash,
                serialized_size,
            )
            self._next_edata_id += 1
            self._edata[data_id] = record
            self._buckets.setdefault(bucket_key, []).append(data_id)
            self._edata_bulk_bytes += serialized_size
            return record

    def get_edata(self, data_id):
        with self._lock:
            try:
                return self._edata[int(data_id)]
            except KeyError:
                raise KeyError(f"unknown EDataID {data_id}") from None

    def record_edata_fetch(self, data_id):
        with self._lock:
            self.get_edata(data_id)
            self._edata_fetches[int(data_id)] = (
                self._edata_fetches.get(int(data_id), 0) + 1
            )
