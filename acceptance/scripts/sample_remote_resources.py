#!/usr/bin/env python3
"""Capture comparable Linux process, network, disk, and filesystem counters."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys
import tempfile
import threading
import time


SAMPLE_SCHEMA = "datavine.resource-snapshot/v1"
DELTA_SCHEMA = "datavine.resource-delta/v1"
CALIBRATION_SCHEMA = "datavine.resource-calibration/v1"


def read_text(path):
    try:
        return Path(path).read_text()
    except OSError:
        return None


def process_tree(root_pids):
    roots = {int(pid) for pid in root_pids if int(pid) > 0}
    result = set(roots)
    changed = True
    while changed:
        changed = False
        for stat_path in Path("/proc").glob("[0-9]*/stat"):
            try:
                fields = stat_path.read_text().split(") ", 1)[1].split()
                pid = int(stat_path.parent.name)
                parent = int(fields[1])
            except (OSError, IndexError, ValueError):
                continue
            if parent in result and pid not in result:
                result.add(pid)
                changed = True
    return sorted(result)


def process_counters(root_pids):
    totals = {
        "processes": 0,
        "cpu_ticks": 0,
        "rss_bytes": 0,
        "read_bytes": 0,
        "write_bytes": 0,
        "cancelled_write_bytes": 0,
        "fds": 0,
    }
    pids = process_tree(root_pids)
    for pid in pids:
        try:
            stat = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()
            status = Path(f"/proc/{pid}/status").read_text().splitlines()
            io_lines = Path(f"/proc/{pid}/io").read_text().splitlines()
            io_values = {
                key.rstrip(":"): int(value)
                for key, value in (line.split() for line in io_lines)
            }
            rss_kib = int(next(
                line.split()[1] for line in status if line.startswith("VmRSS:")
            ))
            fds = len(list(Path(f"/proc/{pid}/fd").iterdir()))
        except (OSError, StopIteration, IndexError, ValueError):
            continue
        totals["processes"] += 1
        totals["cpu_ticks"] += int(stat[11]) + int(stat[12])
        totals["rss_bytes"] += rss_kib * 1024
        totals["read_bytes"] += io_values.get("read_bytes", 0)
        totals["write_bytes"] += io_values.get("write_bytes", 0)
        totals["cancelled_write_bytes"] += io_values.get("cancelled_write_bytes", 0)
        totals["fds"] += fds
    totals["pids"] = pids
    return totals


def network_counters(interfaces=None):
    contents = read_text("/proc/net/dev")
    result = {}
    selected = None if interfaces is None else set(interfaces)
    if contents is None:
        return result
    for line in contents.splitlines()[2:]:
        if ":" not in line:
            continue
        name, values = line.split(":", 1)
        name = name.strip()
        if selected is not None and name not in selected:
            continue
        fields = values.split()
        if len(fields) < 16:
            continue
        result[name] = {
            "rx_bytes": int(fields[0]),
            "rx_packets": int(fields[1]),
            "tx_bytes": int(fields[8]),
            "tx_packets": int(fields[9]),
        }
    return result


def disk_counters(devices=None):
    contents = read_text("/proc/diskstats")
    result = {}
    selected = None if devices is None else set(devices)
    if contents is None:
        return result
    for line in contents.splitlines():
        fields = line.split()
        if len(fields) < 14:
            continue
        name = fields[2]
        if selected is not None and name not in selected:
            continue
        result[name] = {
            "reads_completed": int(fields[3]),
            "read_sectors": int(fields[5]),
            "writes_completed": int(fields[7]),
            "written_sectors": int(fields[9]),
        }
    return result


def filesystem_counters(paths):
    result = {}
    for value in paths:
        path = Path(value).resolve()
        try:
            stat = os.statvfs(path)
        except OSError:
            continue
        result[str(path)] = {
            "block_size": stat.f_frsize,
            "total_bytes": stat.f_blocks * stat.f_frsize,
            "available_bytes": stat.f_bavail * stat.f_frsize,
        }
    return result


def snapshot(root_pids=(), interfaces=None, devices=None, paths=()):
    return {
        "schema": SAMPLE_SCHEMA,
        "wall_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "monotonic_ns": time.monotonic_ns(),
        "hostname": platform.node(),
        "boot_id": (read_text("/proc/sys/kernel/random/boot_id") or "").strip(),
        "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
        "page_size": os.sysconf("SC_PAGE_SIZE"),
        "process": process_counters(root_pids),
        "network": network_counters(interfaces),
        "disk": disk_counters(devices),
        "filesystems": filesystem_counters(paths),
    }


def subtract_counter_map(after, before, fields):
    result = {}
    for name in sorted(set(after) & set(before)):
        values = {}
        valid = True
        for field in fields:
            difference = int(after[name][field]) - int(before[name][field])
            if difference < 0:
                valid = False
                break
            values[field] = difference
        if valid:
            result[name] = values
    return result


def delta(before, after):
    if before.get("schema") != SAMPLE_SCHEMA or after.get("schema") != SAMPLE_SCHEMA:
        raise ValueError("unsupported resource snapshot schema")
    if (before.get("hostname"), before.get("boot_id")) != (
            after.get("hostname"), after.get("boot_id")):
        raise ValueError("snapshots are from different hosts or boots")
    elapsed_ns = int(after["monotonic_ns"]) - int(before["monotonic_ns"])
    if elapsed_ns <= 0:
        raise ValueError("snapshots are not ordered")
    before_process = before["process"]
    after_process = after["process"]
    process = {}
    for field in ("cpu_ticks", "read_bytes", "write_bytes", "cancelled_write_bytes"):
        value = int(after_process[field]) - int(before_process[field])
        process[field] = value if value >= 0 else None
    process["cpu_seconds"] = (
        None if process["cpu_ticks"] is None else
        process["cpu_ticks"] / int(after["clock_ticks_per_second"])
    )
    process["peak_rss_bytes"] = max(
        int(before_process["rss_bytes"]), int(after_process["rss_bytes"])
    )
    network = subtract_counter_map(
        after["network"], before["network"],
        ("rx_bytes", "rx_packets", "tx_bytes", "tx_packets"),
    )
    disk = subtract_counter_map(
        after["disk"], before["disk"],
        ("reads_completed", "read_sectors", "writes_completed", "written_sectors"),
    )
    return {
        "schema": DELTA_SCHEMA,
        "hostname": after["hostname"],
        "boot_id": after["boot_id"],
        "elapsed_seconds": elapsed_ns / 1e9,
        "process": process,
        "network": network,
        "disk": disk,
        "filesystems_before": before["filesystems"],
        "filesystems_after": after["filesystems"],
    }


def transfer_loopback(payload):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    received = hashlib.sha256()

    def receive():
        connection, _ = listener.accept()
        with connection:
            while True:
                chunk = connection.recv(1024 * 1024)
                if not chunk:
                    break
                received.update(chunk)

    thread = threading.Thread(target=receive)
    thread.start()
    digest = hashlib.sha256()
    sent = 0
    with socket.create_connection(listener.getsockname()) as connection:
        while sent < payload:
            size = min(1024 * 1024, payload - sent)
            chunk = hashlib.shake_256(str(sent).encode()).digest(size)
            connection.sendall(chunk)
            digest.update(chunk)
            sent += size
    thread.join()
    listener.close()
    if digest.digest() != received.digest():
        raise RuntimeError("loopback transfer digest mismatch")


def calibrate(payload_bytes, directory):
    roots = (os.getpid(),)
    before = snapshot(roots, interfaces=("lo",), paths=(directory,))
    path = Path(directory) / "datavine-resource-calibration.bin"
    digest = hashlib.sha256()
    written = 0
    with path.open("xb", buffering=0) as stream:
        while written < payload_bytes:
            size = min(1024 * 1024, payload_bytes - written)
            chunk = hashlib.shake_256(f"disk:{written}".encode()).digest(size)
            stream.write(chunk)
            digest.update(chunk)
            written += size
        os.fsync(stream.fileno())
    transfer_loopback(payload_bytes)
    after = snapshot(roots, interfaces=("lo",), paths=(directory,))
    measured = delta(before, after)
    path.unlink()
    process_written = measured["process"]["write_bytes"]
    loopback = measured["network"].get("lo", {})
    rx = loopback.get("rx_bytes")
    tx = loopback.get("tx_bytes")
    gates = {
        "process_write_covers_payload": (
            process_written is not None and process_written >= payload_bytes
        ),
        "loopback_rx_covers_payload": rx is not None and rx >= payload_bytes,
        "loopback_tx_covers_payload": tx is not None and tx >= payload_bytes,
    }
    result = {
        "schema": CALIBRATION_SCHEMA,
        "status": "PASS" if all(gates.values()) else "FAIL",
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "hostname": platform.node(),
        "payload_bytes": payload_bytes,
        "payload_sha256": digest.hexdigest(),
        "measured": measured,
        "ratios": {
            "process_write_to_payload": (
                None if process_written is None else process_written / payload_bytes
            ),
            "loopback_rx_to_payload": None if rx is None else rx / payload_bytes,
            "loopback_tx_to_payload": None if tx is None else tx / payload_bytes,
        },
        "gates": gates,
    }
    return result


def remote_snapshot(host, args):
    script = str(Path(__file__).resolve())
    command = [
        "ssh", "-o", "BatchMode=yes", "--", host,
        sys.executable, script, "snapshot",
    ]
    for pid in args.pid:
        command.extend(("--pid", str(pid)))
    for interface in args.interface:
        command.extend(("--interface", interface))
    for device in args.device:
        command.extend(("--device", device))
    for path in args.path:
        command.extend(("--path", path))
    completed = subprocess.run(
        command, check=True, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    result = json.loads(completed.stdout)
    result["remote_command"] = command
    return result


def read_json(path):
    return json.loads(Path(path).read_text())


def write_result(value, output):
    payload = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if output:
        Path(output).write_text(payload)
    else:
        print(payload, end="")


def positive(value):
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    capture = subparsers.add_parser("snapshot")
    capture.add_argument("--pid", type=positive, action="append", default=[])
    capture.add_argument("--interface", action="append", default=[])
    capture.add_argument("--device", action="append", default=[])
    capture.add_argument("--path", action="append", default=[])
    capture.add_argument("--host")
    capture.add_argument("--output")
    compare = subparsers.add_parser("delta")
    compare.add_argument("before")
    compare.add_argument("after")
    compare.add_argument("--output")
    check = subparsers.add_parser("calibrate")
    check.add_argument("--bytes", type=positive, default=8 * 1024 * 1024)
    check.add_argument("--directory", type=Path)
    check.add_argument("--output")
    args = parser.parse_args()
    try:
        if args.command == "snapshot":
            interfaces = args.interface or None
            devices = args.device or None
            result = (
                remote_snapshot(args.host, args) if args.host else
                snapshot(args.pid, interfaces, devices, args.path)
            )
            write_result(result, args.output)
        elif args.command == "delta":
            write_result(delta(read_json(args.before), read_json(args.after)), args.output)
        else:
            if args.directory:
                args.directory.mkdir(parents=True, exist_ok=True)
                result = calibrate(args.bytes, args.directory.resolve())
            else:
                with tempfile.TemporaryDirectory(
                        prefix="datavine-resource-calibration-") as directory:
                    result = calibrate(args.bytes, directory)
            write_result(result, args.output)
            if result["status"] != "PASS":
                return 1
    except (OSError, ValueError, json.JSONDecodeError, subprocess.SubprocessError) as error:
        print(f"sample_remote_resources: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
