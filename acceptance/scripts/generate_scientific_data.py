#!/usr/bin/env python3
"""Generate and verify deterministic non-sparse scientific input shards."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import struct
import sys


MANIFEST_SCHEMA = "datavine.scientific-data-manifest/v1"
GENERATOR = "shake256-shards/v1"
DEFAULT_CHUNK_BYTES = 8 * 1024 * 1024


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def manifest_digest(manifest):
    value = dict(manifest)
    value.pop("manifest_sha256", None)
    return hashlib.sha256(canonical_json(value)).hexdigest()


def allocated_bytes(path):
    return path.stat().st_blocks * 512


def shard_chunk(seed, shard, offset, size):
    descriptor = struct.pack("!QQQ", int(seed), int(shard), int(offset))
    return hashlib.shake_256(descriptor).digest(size)


def pickle_framing(size):
    if size <= 0xffffffff:
        return b"\x80\x05B" + struct.pack("<I", size), b"."
    return b"\x80\x05\x8e" + struct.pack("<Q", size), b"."


def write_shard(path, seed, shard, size, chunk_bytes, sync, file_format):
    payload_digest = hashlib.sha256()
    stored_digest = hashlib.sha256()
    prefix, suffix = (b"", b"")
    if file_format == "cloudpickle-bytes":
        prefix, suffix = pickle_framing(size)
    temporary = path.with_name(path.name + ".part")
    if path.exists() or temporary.exists():
        raise FileExistsError(f"refusing to overwrite {path}")
    written = 0
    try:
        with temporary.open("xb", buffering=0) as stream:
            if prefix:
                stream.write(prefix)
                stored_digest.update(prefix)
            while written < size:
                count = min(chunk_bytes, size - written)
                payload = shard_chunk(seed, shard, written, count)
                stream.write(payload)
                payload_digest.update(payload)
                stored_digest.update(payload)
                written += count
            if suffix:
                stream.write(suffix)
                stored_digest.update(suffix)
            if sync:
                os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    return {
        "sha256": payload_digest.hexdigest(),
        "stored_sha256": stored_digest.hexdigest(),
        "stored_bytes": len(prefix) + size + len(suffix),
    }


def require_empty_output(output):
    if output.exists():
        if not output.is_dir():
            raise ValueError(f"output is not a directory: {output}")
        if any(output.iterdir()):
            raise ValueError(f"output directory is not empty: {output}")
    else:
        output.mkdir(parents=True)


def generate(output, shards, shard_bytes, seed, chunk_bytes, sync, file_format):
    require_empty_output(output)
    records = []
    for shard in range(shards):
        name = f"shard-{shard:06d}.bin"
        path = output / name
        digests = write_shard(
            path, seed, shard, shard_bytes, chunk_bytes, sync, file_format
        )
        records.append({
            "shard": shard,
            "path": name,
            "bytes": shard_bytes,
            "stored_bytes": digests["stored_bytes"],
            "allocated_bytes": allocated_bytes(path),
            "sha256": digests["sha256"],
            "stored_sha256": digests["stored_sha256"],
        })
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "generator": GENERATOR,
        "format": file_format,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "seed": seed,
        "shards": shards,
        "shard_bytes": shard_bytes,
        "logical_bytes": shards * shard_bytes,
        "chunk_bytes": chunk_bytes,
        "fsync": bool(sync),
        "files": records,
    }
    # Creation time is provenance, not content identity. The stable digest makes
    # separately generated datasets with the same parameters directly comparable.
    identity = dict(manifest)
    identity.pop("created_at")
    manifest["dataset_sha256"] = hashlib.sha256(
        canonical_json(identity)
    ).hexdigest()
    manifest["manifest_sha256"] = manifest_digest(manifest)
    manifest_path = output / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    if sync:
        with manifest_path.open("rb") as stream:
            os.fsync(stream.fileno())
        directory = os.open(output, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    return manifest


def load_manifest(path):
    manifest_path = path if path.is_file() else path / "manifest.json"
    return manifest_path, json.loads(manifest_path.read_text())


def verify(path, full_hash=True):
    manifest_path, manifest = load_manifest(path)
    errors = []
    if manifest.get("schema") != MANIFEST_SCHEMA:
        errors.append("unsupported manifest schema")
    if manifest.get("generator") != GENERATOR:
        errors.append("unsupported generator")
    file_format = manifest.get("format")
    if file_format not in {"raw", "cloudpickle-bytes"}:
        errors.append("unsupported shard format")
    if manifest.get("manifest_sha256") != manifest_digest(manifest):
        errors.append("manifest digest mismatch")
    files = manifest.get("files")
    if not isinstance(files, list) or len(files) != manifest.get("shards"):
        errors.append("manifest file count mismatch")
        files = []
    root = manifest_path.parent.resolve()
    logical_bytes = 0
    seen = set()
    for expected_shard, record in enumerate(files):
        relative = Path(str(record.get("path", "")))
        if (relative.is_absolute() or ".." in relative.parts or
                relative.name != str(relative)):
            errors.append(f"unsafe shard path: {relative}")
            continue
        if record.get("shard") != expected_shard or relative in seen:
            errors.append(f"invalid shard ordering: {relative}")
        seen.add(relative)
        shard_path = root / relative
        try:
            stat = shard_path.stat()
        except FileNotFoundError:
            errors.append(f"missing shard: {relative}")
            continue
        expected_bytes = int(record.get("bytes", -1))
        logical_bytes += max(expected_bytes, 0)
        expected_stored_bytes = int(record.get("stored_bytes", -1))
        if stat.st_size != expected_stored_bytes:
            errors.append(f"size mismatch: {relative}")
        allocated = allocated_bytes(shard_path)
        if allocated < stat.st_size:
            errors.append(f"sparse or compressed allocation: {relative}")
        if int(record.get("allocated_bytes", -1)) != allocated:
            errors.append(f"allocation changed: {relative}")
        if full_hash:
            stored_digest = hashlib.sha256()
            payload_digest = hashlib.sha256()
            prefix, suffix = (
                pickle_framing(expected_bytes)
                if file_format == "cloudpickle-bytes" else (b"", b"")
            )
            with shard_path.open("rb") as stream:
                if prefix:
                    actual_prefix = stream.read(len(prefix))
                    stored_digest.update(actual_prefix)
                    if actual_prefix != prefix:
                        errors.append(f"pickle prefix mismatch: {relative}")
                remaining = expected_bytes
                while remaining > 0:
                    chunk = stream.read(min(DEFAULT_CHUNK_BYTES, remaining))
                    if not chunk:
                        break
                    stored_digest.update(chunk)
                    payload_digest.update(chunk)
                    remaining -= len(chunk)
                actual_suffix = stream.read()
                stored_digest.update(actual_suffix)
                if remaining:
                    errors.append(f"short payload: {relative}")
                if actual_suffix != suffix:
                    errors.append(f"pickle suffix mismatch: {relative}")
            if payload_digest.hexdigest() != record.get("sha256"):
                errors.append(f"content digest mismatch: {relative}")
            if stored_digest.hexdigest() != record.get("stored_sha256"):
                errors.append(f"stored digest mismatch: {relative}")
    if logical_bytes != manifest.get("logical_bytes"):
        errors.append("logical byte count mismatch")
    result = {
        "status": "PASS" if not errors else "FAIL",
        "manifest": str(manifest_path),
        "manifest_sha256": manifest.get("manifest_sha256"),
        "dataset_sha256": manifest.get("dataset_sha256"),
        "shards": len(files),
        "logical_bytes": logical_bytes,
        "full_hash": bool(full_hash),
        "errors": errors,
    }
    return result


def positive(value):
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("generate")
    create.add_argument("--output-dir", required=True, type=Path)
    create.add_argument("--shards", required=True, type=positive)
    create.add_argument("--shard-bytes", required=True, type=positive)
    create.add_argument("--seed", required=True, type=int)
    create.add_argument("--chunk-bytes", type=positive, default=DEFAULT_CHUNK_BYTES)
    create.add_argument(
        "--format", choices=("raw", "cloudpickle-bytes"), default="raw"
    )
    create.add_argument("--no-fsync", action="store_true")
    check = subparsers.add_parser("verify")
    check.add_argument("path", type=Path)
    check.add_argument("--metadata-only", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "generate":
            result = generate(
                args.output_dir.resolve(), args.shards, args.shard_bytes,
                args.seed, args.chunk_bytes, not args.no_fsync, args.format,
            )
            print(json.dumps({
                "status": "PASS",
                "manifest": str(args.output_dir.resolve() / "manifest.json"),
                "manifest_sha256": result["manifest_sha256"],
                "dataset_sha256": result["dataset_sha256"],
                "logical_bytes": result["logical_bytes"],
            }, sort_keys=True))
        else:
            result = verify(args.path.resolve(), not args.metadata_only)
            print(json.dumps(result, sort_keys=True))
            if result["status"] != "PASS":
                return 1
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"generate_scientific_data: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
