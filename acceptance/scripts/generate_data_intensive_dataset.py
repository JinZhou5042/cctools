#!/usr/bin/env python3
"""Plan, generate, and verify resumable source-data parts for the I/O benchmark."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import struct
import sys

from data_intensive_workload import Workload, assert_full_contract, canonical_json


PART_SCHEMA = "datavine.data-intensive-dataset-part/v1"
DATASET_SCHEMA = "datavine.data-intensive-dataset/v1"
GENERATOR = "shake256-files/v1"
DEFAULT_PARTS = 128


def digest_without(value, key):
    copy = dict(value)
    copy.pop(key, None)
    return hashlib.sha256(canonical_json(copy)).hexdigest()


def content(source_index, offset, size):
    descriptor = struct.pack("!QQ", source_index, offset)
    return hashlib.shake_256(descriptor).digest(size)


def verify_prefix(path, source_index, size, chunk_bytes, complete):
    stat = path.stat()
    if stat.st_size > size or (complete and stat.st_size != size):
        raise RuntimeError(f"unexpected resumed source size: {path}")
    if stat.st_blocks * 512 < stat.st_size:
        raise RuntimeError(f"resumed source is sparse: {path}")
    digest = hashlib.sha256()
    offset = 0
    with path.open("rb") as stream:
        while offset < stat.st_size:
            count = min(chunk_bytes, stat.st_size - offset)
            block = stream.read(count)
            expected = content(source_index, offset, count)
            if block != expected:
                raise RuntimeError(f"resumed source content mismatch: {path}")
            digest.update(block)
            offset += count
    return digest, stat.st_size


def write_source(path, source_index, size, chunk_bytes, resume):
    temporary = path.with_name(path.name + ".part")
    if path.exists():
        if not resume or temporary.exists():
            raise FileExistsError(f"refusing to overwrite {path}")
        digest, written = verify_prefix(
            path, source_index, size, chunk_bytes, complete=True
        )
        return digest.digest(), path.stat().st_blocks * 512
    path.parent.mkdir(parents=True, exist_ok=True)
    if temporary.exists():
        if not resume:
            raise FileExistsError(f"refusing to overwrite {temporary}")
        digest, written = verify_prefix(
            temporary, source_index, size, chunk_bytes, complete=False
        )
        mode = "ab"
    else:
        digest = hashlib.sha256()
        written = 0
        mode = "xb"
    try:
        with temporary.open(mode, buffering=0) as stream:
            while written < size:
                count = min(chunk_bytes, size - written)
                block = content(source_index, written, count)
                stream.write(block)
                digest.update(block)
                written += count
        os.replace(temporary, path)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    stat = path.stat()
    if stat.st_size != size or stat.st_blocks * 512 < size:
        raise RuntimeError(f"source file is short or sparse: {path}")
    return digest.digest(), stat.st_blocks * 512


def workload_from_args(args):
    return Workload(args.cohorts, args.scale, args.size_profile)


def part_manifest_path(root, part):
    return root / "_parts" / f"part-{part:03d}.json"


def generate_part(root, workload, part, parts, chunk_bytes, resume):
    manifest_path = part_manifest_path(root, part)
    if manifest_path.exists():
        raise FileExistsError(f"part already has a manifest: {manifest_path}")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    first, count = workload.source_part_bounds(part, parts)
    aggregate = hashlib.sha256()
    logical_bytes = 0
    allocated_bytes = 0
    sizes = {}
    started = datetime.datetime.now(datetime.timezone.utc)
    for source_index in range(first, first + count):
        size = workload.source_size(source_index)
        relative = workload.source_path(source_index)
        digest, allocated = write_source(
            root / relative, source_index, size, chunk_bytes, resume
        )
        aggregate.update(struct.pack("!QQ", source_index, size))
        aggregate.update(digest)
        logical_bytes += size
        allocated_bytes += allocated
        sizes[str(size)] = sizes.get(str(size), 0) + 1
    manifest = {
        "schema": PART_SCHEMA,
        "generator": GENERATOR,
        "contract_sha256": workload.contract()["contract_sha256"],
        "part": part,
        "parts": parts,
        "first_source_index": first,
        "source_files": count,
        "logical_bytes": logical_bytes,
        "allocated_bytes": allocated_bytes,
        "size_histogram": sizes,
        "content_sha256": aggregate.hexdigest(),
        "started_at": started.isoformat(),
        "finished_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    manifest["manifest_sha256"] = digest_without(manifest, "manifest_sha256")
    temporary = manifest_path.with_suffix(".json.part")
    temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, manifest_path)
    return manifest


def expected_part(workload, part, parts):
    first, count = workload.source_part_bounds(part, parts)
    logical_bytes = 0
    sizes = {}
    for source_index in range(first, first + count):
        size = workload.source_size(source_index)
        logical_bytes += size
        sizes[str(size)] = sizes.get(str(size), 0) + 1
    return first, count, logical_bytes, sizes


def verify_part(root, workload, part, parts, full_hash):
    errors = []
    path = part_manifest_path(root, part)
    try:
        manifest = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError) as error:
        return {"status": "FAIL", "part": part, "errors": [str(error)]}
    first, count, logical_bytes, sizes = expected_part(workload, part, parts)
    checks = {
        "schema": PART_SCHEMA,
        "generator": GENERATOR,
        "contract_sha256": workload.contract()["contract_sha256"],
        "part": part,
        "parts": parts,
        "first_source_index": first,
        "source_files": count,
        "logical_bytes": logical_bytes,
        "size_histogram": sizes,
    }
    for name, expected in checks.items():
        if manifest.get(name) != expected:
            errors.append(f"{name} mismatch")
    if manifest.get("manifest_sha256") != digest_without(manifest, "manifest_sha256"):
        errors.append("manifest digest mismatch")
    aggregate = hashlib.sha256()
    observed_bytes = 0
    observed_allocated = 0
    for source_index in range(first, first + count):
        source = root / workload.source_path(source_index)
        size = workload.source_size(source_index)
        try:
            stat = source.stat()
        except FileNotFoundError:
            errors.append(f"missing source index {source_index}")
            continue
        if stat.st_size != size:
            errors.append(f"size mismatch source index {source_index}")
        if stat.st_blocks * 512 < stat.st_size:
            errors.append(f"sparse source index {source_index}")
        observed_bytes += stat.st_size
        observed_allocated += stat.st_blocks * 512
        if full_hash:
            digest = hashlib.sha256()
            with source.open("rb") as stream:
                while True:
                    block = stream.read(8 << 20)
                    if not block:
                        break
                    digest.update(block)
            aggregate.update(struct.pack("!QQ", source_index, size))
            aggregate.update(digest.digest())
    if observed_bytes != logical_bytes:
        errors.append("observed logical bytes mismatch")
    if observed_allocated != manifest.get("allocated_bytes"):
        errors.append("observed allocated bytes mismatch")
    if full_hash and aggregate.hexdigest() != manifest.get("content_sha256"):
        errors.append("content digest mismatch")
    return {
        "status": "PASS" if not errors else "FAIL",
        "part": part,
        "source_files": count,
        "logical_bytes": observed_bytes,
        "allocated_bytes": observed_allocated,
        "full_hash": bool(full_hash),
        "errors": errors,
    }


def assemble(root, workload, parts, full_hash):
    results = [verify_part(root, workload, part, parts, full_hash) for part in range(parts)]
    errors = [f"part {item['part']}: {error}" for item in results for error in item["errors"]]
    source_files = sum(item.get("source_files", 0) for item in results if item["status"] == "PASS")
    logical_bytes = sum(item.get("logical_bytes", 0) for item in results if item["status"] == "PASS")
    manifest = {
        "schema": DATASET_SCHEMA,
        "status": "PASS" if not errors else "FAIL",
        "contract": workload.contract(),
        "parts": parts,
        "verified_parts": sum(item["status"] == "PASS" for item in results),
        "source_files": source_files,
        "logical_bytes": logical_bytes,
        "full_hash": bool(full_hash),
        "errors": errors,
    }
    gates = {
        "all_parts_pass": not errors and manifest["verified_parts"] == parts,
        "exact_source_files": source_files == workload.source_files,
        "exact_source_bytes": logical_bytes == workload.source_bytes,
    }
    manifest["gates"] = gates
    if not all(gates.values()):
        manifest["status"] = "FAIL"
    manifest["manifest_sha256"] = digest_without(manifest, "manifest_sha256")
    output = root / "dataset-manifest.json"
    temporary = output.with_suffix(".json.part")
    temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, output)
    return manifest


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohorts", type=int, default=64)
    parser.add_argument("--scale", type=int, default=64)
    parser.add_argument("--size-profile", choices=("tiny", "full"), default="full")
    parser.add_argument("--acceptance", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    plan.add_argument("--parts", type=int, default=DEFAULT_PARTS)
    generate = subparsers.add_parser("generate-part")
    generate.add_argument("--root", required=True, type=Path)
    generate.add_argument("--part", required=True, type=int)
    generate.add_argument("--parts", type=int, default=DEFAULT_PARTS)
    generate.add_argument("--chunk-bytes", type=int, default=8 << 20)
    generate.add_argument("--resume", action="store_true")
    verify = subparsers.add_parser("verify-part")
    verify.add_argument("--root", required=True, type=Path)
    verify.add_argument("--part", required=True, type=int)
    verify.add_argument("--parts", type=int, default=DEFAULT_PARTS)
    verify.add_argument("--full-hash", action="store_true")
    dataset = subparsers.add_parser("assemble")
    dataset.add_argument("--root", required=True, type=Path)
    dataset.add_argument("--parts", type=int, default=DEFAULT_PARTS)
    dataset.add_argument("--full-hash", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    workload = workload_from_args(args)
    if args.acceptance:
        assert_full_contract(workload)
    if args.command == "plan":
        contract = workload.contract()
        contract["dataset_parts"] = args.parts
        contract["part_source_files"] = workload.source_files // args.parts
        contract["part_bounds"] = [workload.source_part_bounds(part, args.parts) for part in range(args.parts)]
        result = contract
    elif args.command == "generate-part":
        result = generate_part(
            args.root.resolve(), workload, args.part, args.parts,
            args.chunk_bytes, args.resume,
        )
    elif args.command == "verify-part":
        result = verify_part(args.root.resolve(), workload, args.part, args.parts, args.full_hash)
    else:
        result = assemble(args.root.resolve(), workload, args.parts, args.full_hash)
    print(json.dumps(result, indent=2, sort_keys=True))
    if result.get("status") == "FAIL":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
