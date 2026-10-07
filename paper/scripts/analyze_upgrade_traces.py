#!/usr/bin/env python3
"""Validate retained local-clock traces and paired instrumentation controls."""
import hashlib
import json
from pathlib import Path
import re
import statistics

PAPER = Path(__file__).resolve().parents[1]
FIELDS = ['received_us', 'prepare_start_us', 'prepare_ready_us', 'execute_start_us', 'complete_us']


def main():
    source = PAPER / 'results/upgrade-attribution-v2/summary.json'
    campaign = json.loads(source.read_text())
    assert campaign['status'] == 'PASS' and campaign['passed'] == 60
    inputs = {str(source.relative_to(PAPER)): hashlib.sha256(source.read_bytes()).hexdigest()}
    summaries = []
    for row in campaign['runs']:
        if row['variant'] != 'dv-profile':
            continue
        traces = []
        peaks = []
        for worker in range(row['workers']):
            path = Path(row['path']) / f'worker-{worker}.debug'
            inputs[str(path.relative_to(PAPER))] = hashlib.sha256(path.read_bytes()).hexdigest()
            local = []
            for line in path.read_text().splitlines():
                if 'datavine-attempt ' not in line:
                    continue
                values = {key: int(value) for key, value in re.findall(r'(\w+)=(-?\d+)', line)}
                times = [values[key] for key in FIELDS]
                assert times[0] > 0 and times == sorted(times) and values['result'] == 0
                local.append(values)
            assert len({t['task'] for t in local}) == len(local)
            traces += local
            stages = {}
            for name, begin, end in [('queued', 0, 1), ('preparing', 1, 2),
                                     ('prepared_waiting', 2, 3), ('executing', 3, 4)]:
                events = []
                for t in local:
                    start, finish = t[FIELDS[begin]], t[FIELDS[end]]
                    if finish > start:
                        events += [(start, 1), (finish, -1)]
                count = peak = 0
                for _, delta in sorted(events):
                    count += delta
                    peak = max(peak, count)
                    assert count >= 0
                assert count == 0
                stages[name] = peak
            peaks.append(stages)
        assert len(traces) == row['tasks']
        assert len({t['task'] for t in traces}) == row['tasks']
        intervals = {name: statistics.median((t[FIELDS[i+1]] - t[FIELDS[i]]) / 1000 for t in traces)
                     for i, name in enumerate(['queued_ms', 'preparing_ms', 'prepared_wait_ms', 'executing_ms'])}
        summaries.append(dict(trial=Path(row['path']).name, attempts=len(traces),
                              local_peak_stage_counts=peaks, median_intervals=intervals))
    paired = []
    for size in [1024, 1048576]:
        rows = [r for r in campaign['runs'] if r['configuration']['bytes'] == size]
        by_key = {(r['repetition'], r['variant']): r for r in rows}
        for baseline, variant in [('dv-fixed', 'dv-profile'), ('tv-stock', 'tv-profile'),
                                  ('dv-fixed', 'dv-cold'), ('tv-stock', 'tv-cold')]:
            ratios = [by_key[r, variant]['elapsed_seconds'] / by_key[r, baseline]['elapsed_seconds']
                      for r in range(1, 6)]
            paired.append(dict(bytes=size, baseline=baseline, variant=variant,
                               ratios=ratios, median=statistics.median(ratios),
                               minimum=min(ratios), maximum=max(ratios)))
    report = dict(status='PASS', inputs=inputs, total_attempts=sum(s['attempts'] for s in summaries),
                  traces=summaries, paired=paired,
                  scope='Intervals and occupancy counts use each Worker clock independently. These are task-stage counts, not physical prepared bytes. Profiling combines process sampling and DV attempt tracing; RSS sums double-count shared pages. No cross-host clock subtraction.')
    (PAPER / 'results/upgrade-traces.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: report[k] for k in ['status', 'total_attempts', 'paired']}))


if __name__ == '__main__':
    main()
