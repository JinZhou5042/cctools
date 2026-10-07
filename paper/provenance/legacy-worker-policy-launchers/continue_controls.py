#!/usr/bin/env python3
"""Complete the non-grouping control blocks after a diagnostic interruption.

No successful non-grouping run is replaced. Native-group trials in the source
campaign are diagnostics after a start-recall compatibility defect was found.
Their replacement comparison is a separately versioned, paired campaign.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import research_campaign as campaign


def main():
    source = campaign.PAPER / 'results/upgrade-controls-v3'
    output = campaign.PAPER / 'results/upgrade-controls-v4'
    previous = json.loads((source / 'summary.json').read_text())
    if previous['status'] == 'RUNNING' and not (source / 'interruption.json').exists():
        raise RuntimeError('previous controller must terminate before pool reuse')
    plan = json.loads((source / 'plan.json').read_text())
    variants = {'dv-fixed', 'dv-elastic', 'dv-eager', 'tv-stock', 'tv-4c'}
    order = [job for job in plan['order'] if job['variant'] in variants]
    assert len(order) == 75
    def label(job):
        return f"r{job['repetition']}-w{job['workers']}-{job['workload']}-b{job['bytes']}-{job['variant']}"
    retained = {label(row['configuration']): row for row in previous['runs']
                if row.get('variant') in variants and row['status'] == 'PASS'}
    for row in previous['runs']:
        if row.get('configuration', {}).get('variant') in variants and row['status'] != 'PASS':
            raise RuntimeError('a non-grouping failure requires separate investigation')
    # Preserve complete paired blocks. Re-run every variant in an interrupted
    # block on the new allocation; never pair variants across allocations.
    def block(job):
        return (job['repetition'], job['workers'], job['workload'], job['bytes'])
    complete = {block(job) for job in order if all(
        label(other) in retained for other in order if block(other) == block(job))}
    retained = {name: row for name, row in retained.items() if block(row['configuration']) in complete}
    plan['order'] = order
    plan['config']['variants'] = ','.join(sorted(variants))
    plan['config']['output'] = str(output)
    plan['continuation'] = dict(source=str(source.relative_to(campaign.PAPER)),
        summary_sha256=hashlib.sha256((source / 'summary.json').read_bytes()).hexdigest(),
        retained_trials=len(retained),
        reason='Exclude every incompatible native-group trial; retain complete paired blocks and rerun incomplete blocks on a new allocation.')
    plan['fingerprints'][str(Path(__file__).resolve().relative_to(campaign.ROOT))] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    output.mkdir(exist_ok=False)
    campaign.atomic(output / 'plan.json', plan)
    rows = []
    references = {}
    mismatches = []
    for job in order:
        for name, digest in plan['fingerprints'].items():
            if hashlib.sha256((campaign.ROOT / name).read_bytes()).hexdigest() != digest:
                raise RuntimeError('measured input changed: ' + name)
        name = label(job)
        if name in retained:
            row = retained[name]
            assert json.loads((Path(row['path']) / 'result.json').read_text()) == {
                k: v for k, v in row.items() if k not in ['path', 'configuration']}
        else:
            trial = output / name
            command = [sys.executable, str(campaign.PAPER / 'scripts/deployment_trial.py'),
                *campaign.VARIANTS[job['variant']], '--cores', '4', '--batch-type', 'local',
                '--timeout', '600', '--output', str(trial)]
            for key, value in job.items():
                command += ['--' + key.replace('_', '-'), str(value)]
            with (output / (name + '.log')).open('w') as log:
                done = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
                                      timeout=820, start_new_session=True)
            result = json.loads((trial / 'result.json').read_text())
            if done.returncode:
                result.update(status='FAIL', runner_exit_code=done.returncode)
            row = dict(path=str(trial), configuration=job, **result)
        rows.append(row)
        key = (job['workload'], job['bytes'], job['workers'])
        science = [row.get('result_digest'), row.get('payload_sha256')]
        if key in references and references[key] != science:
            mismatches.append(name)
        references.setdefault(key, science)
        passed = sum(r['status'] == 'PASS' for r in rows)
        status = ('PASS' if passed == 75 and not mismatches else
                  'RUNNING' if row['status'] == 'PASS' else 'PARTIAL')
        summary = dict(status=status, planned=75, completed=len(rows), passed=passed,
                       scientific_mismatches=mismatches, runs=rows)
        campaign.atomic(output / 'summary.json', summary)
        print(json.dumps({k: summary[k] for k in ['status', 'planned', 'completed', 'passed']}), flush=True)
        if row['status'] != 'PASS':
            return 1
    return 0 if status == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
