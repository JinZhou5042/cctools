"""Deterministic scientific kernels and isolated controls for the paper.

Every backend invokes this exact function. No task fusion or implicit batching
is performed. Runtime telemetry is excluded from the scientific checksum.
"""


def paper_kernel(key, kind, size, cpu_ms, seed, *parents):
    import hashlib
    import math
    import os
    import platform
    import tempfile
    import time

    entered = time.monotonic_ns()
    cpu_entered = time.process_time_ns()
    records = {}
    for parent in parents:
        records.update(parent["records"])
        if hashlib.sha256(parent["payload"]).hexdigest() != parent["payload_sha256"]:
            raise ValueError("corrupt parent payload")
    parent_bytes = sum(len(p["payload"]) for p in parents)
    parent_ids = "|".join(p["digest"] for p in parents)
    identity = hashlib.sha256(f"{key}|{seed}|{parent_ids}".encode()).digest()
    size = int(size)
    answer = None
    io_bytes = 0
    if kind == "spectral":
        import numpy as np

        count = max(256, size // 8)
        if parents:
            signal = np.frombuffer(parents[0]["payload"], dtype="<f8").copy()
        else:
            x = np.arange(count, dtype=np.float64)
            frequency = 3 + seed % 13
            signal = np.sin(2 * np.pi * frequency * x / count)
            signal += 0.125 * np.cos(2 * np.pi * 23 * x / count)
        # Frequency-domain low-pass filtering is real numerical work. Repeat
        # transforms without changing the filter's mathematical result.
        spectrum = np.fft.rfft(signal)
        spectrum[len(spectrum) // 3:] = 0
        filtered = np.fft.irfft(spectrum, n=len(signal))
        for _ in range(7):
            np.fft.irfft(np.fft.rfft(filtered), n=len(filtered))
        answer = float(np.dot(filtered, filtered) / len(filtered))
        if not math.isfinite(answer) or abs(answer - 0.5078125) > 1e-10:
            raise ArithmeticError(f"spectral energy mismatch: {answer}")
        payload = filtered.astype("<f8").tobytes()
    elif kind == "histogram":
        import numpy as np

        # Read every parent byte; count conservation is checked at every node.
        inputs = [p["payload"] for p in parents] or [identity * max(1, (size + 31) // 32)]
        counts = np.zeros(256, dtype=np.int64)
        for value in inputs:
            counts += np.bincount(np.frombuffer(value, dtype=np.uint8), minlength=256)
        answer = int(counts.sum())
        if answer != sum(map(len, inputs)):
            raise ArithmeticError("histogram count conservation failed")
        payload = hashlib.shake_256(counts.tobytes()).digest(size)
    elif kind == "quadrature":
        # Composite Simpson integration of a smooth, nontrivial integrand;
        # analytical integral provides an independent numerical oracle.
        steps = max(1000, size // 8)
        steps += steps % 2
        a, b = 0.0, 1.0
        h = (b - a) / steps
        total = 1.0 + math.exp(-1.0)
        for i in range(1, steps):
            total += (4 if i % 2 else 2) * math.exp(-(i * h) ** 2)
        answer = total * h / 3
        expected = math.sqrt(math.pi) * math.erf(1.0) / 2
        if abs(answer - expected) > 1e-10:
            raise ArithmeticError("quadrature oracle failed")
        payload = hashlib.shake_256(identity).digest(size)
    else:
        payload = hashlib.shake_256(identity).digest(size)
        if kind == "disk":
            # Real node-local writeback/read work, not a sleep approximation.
            with tempfile.TemporaryFile() as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
                block = 65536
                offsets = list(range(0, len(payload), block))
                offsets = offsets[::2] + offsets[1::2]
                for offset in offsets:
                    data = os.pread(stream.fileno(), min(block, len(payload) - offset), offset)
                    if data != payload[offset:offset + len(data)]:
                        raise IOError("node-local readback mismatch")
                    io_bytes += len(data)
            io_bytes += len(payload)
        elif kind == "sleep":
            time.sleep(max(0.0, cpu_ms / 1000))
        elif kind not in ("cpu", "noop"):
            raise ValueError(kind)
    if kind == "cpu":
        deadline = time.process_time_ns() + int(cpu_ms * 1e6)
        value = int.from_bytes(identity[:8], "little")
        while time.process_time_ns() < deadline:
            value = (value * 6364136223846793005 + 1) & ((1 << 64) - 1)
    digest = hashlib.sha256(identity + kind.encode()).hexdigest()
    records[key] = {
        "key": key, "kind": kind, "seed": seed, "answer": answer,
        "host": platform.node(), "pid": os.getpid(), "cpuset": sorted(os.sched_getaffinity(0)),
        "start_ns": entered, "finish_ns": time.monotonic_ns(),
        "cpu_ns": time.process_time_ns() - cpu_entered,
        "input_bytes": parent_bytes, "output_bytes": len(payload),
        "local_io_bytes": io_bytes,
    }
    return {"digest": digest, "payload": payload,
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
            "records": records}


def graph(workload, width, levels, size, cpu_ms):
    if min(width, levels, size) < 1:
        raise ValueError("positive width, levels and size required")
    stages = (["cpu", "disk", "cpu", "disk"] if workload == "phase"
              else [workload] * levels)
    nodes = []
    for stage, kind in enumerate(stages):
        for lane in range(width):
            parents = [] if stage == 0 else [f"{stage-1}:{lane}"]
            if stage and workload in ("histogram", "disk", "phase") and width > 1:
                parents.append(f"{stage-1}:{(lane+1)%width}")
            nodes.append(dict(key=f"{stage}:{lane}", kind=kind, size=size,
                              cpu_ms=cpu_ms, seed=lane + 101,
                              parents=parents))
    return nodes, [f"{len(stages)-1}:{i}" for i in range(width)]


def validate(values, nodes):
    import hashlib

    records = {}
    for value in values:
        if hashlib.sha256(value["payload"]).hexdigest() != value["payload_sha256"]:
            raise ValueError("invalid sink checksum")
        records.update(value["records"])
    expected = {n["key"] for n in nodes}
    if set(records) != expected:
        raise ValueError(f"logical identities differ: {len(records)} != {len(expected)}")
    for node in nodes:
        record = records[node["key"]]
        if record["kind"] != node["kind"] or record["seed"] != node["seed"]:
            raise ValueError("task identity or kernel differs")
        if record["finish_ns"] < record["start_ns"] or record["cpu_ns"] < 0:
            raise ValueError("invalid task timing")
    return records
