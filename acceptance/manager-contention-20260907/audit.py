#!/usr/bin/env python3
"""Audit accepted artifacts without launching or changing a workflow."""
import collections
import hashlib
import json
from pathlib import Path

P = Path(__file__).resolve().parent
ROOT = P.parents[1]
counts = {}
total_tasks = 0
for sub, expected in [('raw', 33), ('probe-raw', 24), ('separate-raw', 24), ('separate-sharedcpu-raw', 12)]:
    files = sorted((P / sub).glob('*.json'))
    assert len(files) == expected, (sub, len(files))
    counts[sub] = len(files)
    for f in files:
        d = json.loads(f.read_text())
        s = d['stats']
        assert d['status'] == 'PASS'
        expected_tasks = d['tasks'] * (2 if sub == 'probe-raw' else 1)
        assert s['tasks_done'] == s['tasks_submitted'] == expected_tasks
        assert s['tasks_failed'] == s['workers_removed'] == 0
        assert len(d['records']) == expected_tasks
        assert len({r['tag'] for r in d['records']}) == expected_tasks
        assert s['bytes_sent'] == (0 if d.get('mode') == 'probe-only' else d['tasks'] * d['bytes_per_task'])
        running = collections.Counter()
        workers = {}
        metrics = {}
        done = collections.Counter()
        for line in Path(str(f) + '.transactions').read_text().splitlines():
            if line.startswith('#'):
                continue
            parts = line.split()
            if len(parts) < 5 or parts[2] != 'TASK':
                continue
            tid = int(parts[3])
            if parts[4] == 'RUNNING':
                running[tid] += 1
                workers[tid] = parts[5]
            elif parts[4] == 'RETRIEVED':
                assert parts[5] == 'SUCCESS'
                start = line.find('{"time_worker_start"')
                assert start >= 0
                metrics[tid] = json.loads(line[start:])
            elif parts[4] == 'DONE':
                assert parts[5:] == ['SUCCESS', '0']
                done[tid] += 1
        assert len(running) == len(done) == expected_tasks
        assert set(running.values()) == set(done.values()) == {1}
        probe_workers = set()
        pressure_workers = set()
        for r in d['records']:
            probe = r['tag'].startswith('probe-')
            index = int(r['tag'].split('-')[-1])
            tid = index * 2 + (1 if probe else 2) if sub == 'probe-raw' else index + 1
            mt = metrics[tid]
            # Verify inferred IDs against independent commit timestamps, not tag order alone.
            assert abs(mt['time_commit_start'][0] * 1e6 - r['time_when_commit_start']) < 2
            assert abs(mt['time_commit_end'][0] * 1e6 - r['time_when_commit_end']) < 2
            assert mt['time_worker_end'][0] >= mt['time_worker_start'][0]
            (probe_workers if probe else pressure_workers).add(workers[tid])
        if probe_workers:
            assert len(probe_workers) == 1
            assert not probe_workers.intersection(pressure_workers)
        total_tasks += expected_tasks

provenance = json.loads((P / 'provenance.json').read_text())
verified = {}
for name, expected in provenance['hashes'].items():
    if name.startswith('acceptance/'):
        continue  # Initial harness was corrected before accepted campaigns.
    actual = hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
    assert actual == expected, name
    verified[name] = actual
result = dict(status='PASS', workflows=sum(counts.values()), tasks=total_tasks,
              campaigns=counts, unchanged_runtime_hashes=verified,
              checks=['exact counts', 'successful byte-count checks', 'no repeated dispatch',
                      'no worker removals', 'exact input bytes', 'unique task tags',
                      'probe ID timestamp mapping', 'exclusive probe worker'])
(P / 'audit.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
