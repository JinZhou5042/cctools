#!/usr/bin/env python3
"""Small reusable primitives for DataVine architecture benchmarks."""

import math
import os
from pathlib import Path
import signal
import statistics
import subprocess
import threading
import time


def start_worker(port, cores):
    """Start one quiet local worker in its own process group."""
    return subprocess.Popen(
        [
            os.environ.get("VINE_WORKER", "vine_worker"),
            "127.0.0.1",
            str(port),
            "--cores",
            str(cores),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def stop_workers(workers):
    """Terminate worker process groups, escalating after ten seconds."""
    for worker in workers:
        if worker.poll() is not None:
            continue
        try:
            os.killpg(worker.pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
    for worker in workers:
        try:
            worker.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(worker.pid, signal.SIGKILL)
            worker.wait(timeout=10)


def wait_for_workers(manager, expected, timeout=30):
    """Wait until the manager observes the requested worker count."""
    deadline = time.monotonic() + timeout
    while len(manager.status("workers")) < expected:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"expected {expected} workers")
        manager.wait(1)


def wait_for_task(manager, task_id):
    """Wait for a specific task, ignoring unrelated completions."""
    while True:
        completed = manager.wait(1)
        if completed is not None and completed.id == task_id:
            return completed


def percentile(values, probability):
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * float(probability)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def latency_summary(values):
    values = tuple(values)
    return {
        "samples": len(values),
        "p50_seconds": percentile(values, 0.50),
        "p95_seconds": percentile(values, 0.95),
        "p99_seconds": percentile(values, 0.99),
        "max_seconds": max(values) if values else None,
    }


def throughput_summary(samples, field="application_tasks_per_second"):
    values = [float(sample[field]) for sample in samples]
    return {
        "median_tasks_per_second": statistics.median(values),
        "min_tasks_per_second": min(values),
        "max_tasks_per_second": max(values),
    }


class BoundedLatencySampler:
    """Track completion latency for an evenly sampled bounded task subset."""

    def __init__(self, task_count, capacity=10_000):
        task_count = int(task_count)
        capacity = int(capacity)
        if task_count < 1 or capacity < 1:
            raise ValueError("task count and latency capacity must be positive")
        self.stride = max(1, math.ceil(task_count / capacity))
        self._submitted = {}
        self._latencies = []

    def submitted(self, ordinal, task_id, timestamp=None):
        if int(ordinal) % self.stride == 0:
            self._submitted[int(task_id)] = float(
                time.monotonic() if timestamp is None else timestamp
            )

    def completed(self, task_id, timestamp=None):
        started = self._submitted.pop(int(task_id), None)
        if started is not None:
            completed = time.monotonic() if timestamp is None else timestamp
            self._latencies.append(float(completed) - started)

    def summary(self):
        value = latency_summary(self._latencies)
        value["sampling_stride"] = self.stride
        value["unresolved_samples"] = len(self._submitted)
        return value


class ProcessTreeSampler:
    """Bounded Linux /proc sampler for one process and all descendants."""

    def __init__(self, root_pid=None, interval_seconds=0.1):
        self.root_pid = int(root_pid or os.getpid())
        self.interval_seconds = float(interval_seconds)
        if self.interval_seconds <= 0:
            raise ValueError("sampling interval must be positive")
        self._page_size = os.sysconf("SC_PAGE_SIZE")
        self._clock_ticks = os.sysconf("SC_CLK_TCK")
        self._stop = threading.Event()
        self._thread = None
        self._samples = 0
        self._peak_total_rss = 0
        self._peak_rss_by_role = {}
        self._cpu_ticks_by_pid = {}
        self._cpu_ticks_total = 0
        self._peak_observed_processes = 0
        self._errors = 0

    @staticmethod
    def _children(pid):
        path = Path(f"/proc/{pid}/task/{pid}/children")
        try:
            return tuple(int(value) for value in path.read_text().split())
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            return ()

    def _process_tree(self):
        pending = [self.root_pid]
        seen = set()
        while pending:
            pid = pending.pop()
            if pid in seen:
                continue
            seen.add(pid)
            pending.extend(self._children(pid))
        return seen

    @staticmethod
    def _role(pid, root_pid):
        if pid == root_pid:
            return "driver"
        try:
            command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(
                b"\0", b" "
            ).decode("utf-8", "replace")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            return "other"
        if "datavine.controller.cli" in command:
            return "controller"
        if "vine_worker" in command:
            return "worker"
        if "library_code.py" in command:
            return "library"
        if "python" in command:
            return "python-task"
        return "other"

    def _process_sample(self, pid):
        stat = Path(f"/proc/{pid}/stat").read_text()
        fields = stat[stat.rfind(")") + 2:].split()
        ticks = int(fields[11]) + int(fields[12])
        rss_bytes = int(fields[21]) * self._page_size
        return rss_bytes, ticks

    def sample(self):
        rss_by_role = {}
        total_rss = 0
        current_cpu_ticks = {}
        for pid in self._process_tree():
            try:
                rss_bytes, ticks = self._process_sample(pid)
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                continue
            except (IndexError, OSError, ValueError):
                self._errors += 1
                continue
            role = self._role(pid, self.root_pid)
            rss_by_role[role] = rss_by_role.get(role, 0) + rss_bytes
            total_rss += rss_bytes
            previous = self._cpu_ticks_by_pid.get(pid)
            if previous is not None and ticks >= previous:
                self._cpu_ticks_total += ticks - previous
            current_cpu_ticks[pid] = ticks
        self._cpu_ticks_by_pid = current_cpu_ticks
        self._peak_observed_processes = max(
            self._peak_observed_processes, len(current_cpu_ticks)
        )
        self._samples += 1
        self._peak_total_rss = max(self._peak_total_rss, total_rss)
        for role, rss_bytes in rss_by_role.items():
            self._peak_rss_by_role[role] = max(
                self._peak_rss_by_role.get(role, 0), rss_bytes
            )

    def _run(self):
        while not self._stop.wait(self.interval_seconds):
            self.sample()

    def start(self):
        if self._thread is not None:
            raise RuntimeError("process sampler already started")
        self.sample()
        self._thread = threading.Thread(
            target=self._run,
            name="datavine-benchmark-process-sampler",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self):
        if self._thread is None:
            raise RuntimeError("process sampler was not started")
        self.sample()
        self._stop.set()
        self._thread.join(timeout=max(1.0, self.interval_seconds * 4))
        if self._thread.is_alive():
            raise RuntimeError("process sampler did not stop")
        self._thread = None
        return {
            "sample_interval_seconds": self.interval_seconds,
            "samples": self._samples,
            "peak_observed_processes": self._peak_observed_processes,
            "peak_tree_rss_bytes": self._peak_total_rss,
            "peak_rss_bytes_by_role": dict(
                sorted(self._peak_rss_by_role.items())
            ),
            "sampled_tree_cpu_seconds": (
                self._cpu_ticks_total / self._clock_ticks
            ),
            "sampling_errors": self._errors,
        }
