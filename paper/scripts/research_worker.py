#!/usr/bin/env python3
"""Pinned worker with retained process-tree CPU and memory measurements."""
import argparse
import json
import os
from pathlib import Path
import resource
import signal
import socket
import subprocess
import tempfile
import time


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--cores', type=int, required=True)
    p.add_argument('--cpus')
    p.add_argument('--sample', action='store_true')
    p.add_argument('command', nargs=argparse.REMAINDER)
    args = p.parse_args()
    allowed = sorted(os.sched_getaffinity(0))
    selected = [int(c) for c in args.cpus.split(',')] if args.cpus else allowed[:args.cores]
    if len(selected) != args.cores or not set(selected) <= set(allowed):
        raise ValueError('worker CPU capacity unavailable')
    os.sched_setaffinity(0, selected)
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    samples = []
    if args.sample:
        import psutil
    with tempfile.TemporaryDirectory(prefix='datavine-research-worker-') as scratch:
        command += ['--workdir', scratch]
        child = subprocess.Popen(command)
        def terminate(signum, frame):
            if child.poll() is None:
                child.terminate()
        signal.signal(signal.SIGTERM, terminate)
        signal.signal(signal.SIGINT, terminate)
        root = psutil.Process(child.pid) if args.sample else None
        started = time.monotonic_ns()
        while child.poll() is None:
            if args.sample:
                try:
                    processes = [root, *root.children(recursive=True)]
                    rss = cpu = 0
                    count = 0
                    for process in processes:
                        try:
                            t = process.cpu_times()
                            cpu += t.user + t.system + t.children_user + t.children_system
                            rss += process.memory_info().rss
                            count += 1
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                    samples.append(dict(monotonic_ns=time.monotonic_ns(),
                        cpu_seconds=cpu, summed_rss_bytes=rss, process_count=count))
                except psutil.NoSuchProcess:
                    pass
            time.sleep(.1)
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    args.output.write_text(json.dumps(dict(host=socket.getfqdn(), cpuset=selected,
        start_ns=started, finish_ns=time.monotonic_ns(), exit_code=child.returncode,
        lifetime_cpu_seconds=usage.ru_utime+usage.ru_stime,
        max_child_rss_kib=usage.ru_maxrss, samples=samples,
        scope='Lifetime includes worker and library startup and shutdown. RSS sums can double-count shared pages. Process sampling is optional and non-atomic.'), indent=2)+'\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
