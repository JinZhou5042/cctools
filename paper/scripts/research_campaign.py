#!/usr/bin/env python3
"""Five randomized paired blocks on one persistent compute allocation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import time
from run_campaign import atomic

PAPER = Path(__file__).resolve().parents[1]
ROOT = PAPER.parent
VARIANTS = {
    'dv-elastic': ['--backend','datavine'],
    'tv-stock': ['--backend','taskvine'],
    'tv-4c': ['--backend','taskvine','--window','4'],
    'tv-cold': ['--backend','taskvine','--cold'],
    'dv-profile': ['--backend','datavine','--profile','--trace'],
    'tv-profile': ['--backend','taskvine','--profile'],
}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--mode',choices=['controls','application','scaling','attribution'],required=True)
    p.add_argument('--variants',default='dv-elastic,tv-stock,tv-4c')
    p.add_argument('--repetitions',type=int,default=5)
    p.add_argument('--workers',default='2')
    p.add_argument('--cores',type=int,default=4)
    p.add_argument('--batch-type',default='local',choices=['local','reserved'])
    p.add_argument('--pool',type=Path)
    p.add_argument('--manifest',type=Path)
    p.add_argument('--timeout',type=float,default=600)
    args = p.parse_args()
    names = args.variants.split(',')
    if set(names)-VARIANTS.keys():
        p.error('unknown variant')
    if args.mode == 'application' and not args.manifest:
        p.error('application requires pinned manifest')
    if args.mode == 'application':
        cases = [dict(workload='atlas',width=16,levels=1,bytes=100000,cpu_ms=0)]
    elif args.mode == 'attribution':
        cases = [dict(workload='noop',width=64,levels=8,bytes=b,cpu_ms=0)
                 for b in [1024,1048576]]
    elif args.mode == 'scaling':
        cases = [dict(workload='cpu',width=256,levels=8,bytes=1024,cpu_ms=100),
                 dict(workload='noop',width=256,levels=8,bytes=1048576,cpu_ms=0)]
    else:
        cases = [dict(workload='noop',width=64,levels=8,bytes=1048576,cpu_ms=0),
                 dict(workload='cpu',width=64,levels=8,bytes=1024,cpu_ms=50),
                 dict(workload='phase',width=64,levels=4,bytes=262144,cpu_ms=100)]
    order = []
    rng = random.Random(2026090602)
    for repetition in range(1,args.repetitions+1):
        blocks = [(int(w),case) for w in args.workers.split(',') for case in cases]
        rng.shuffle(blocks)
        for workers,case in blocks:
            shuffled = names.copy(); rng.shuffle(shuffled)
            order += [dict(repetition=repetition,workers=workers,variant=v,**case) for v in shuffled]
    files = [*sorted((PAPER/'scripts').glob('research_*.py')),PAPER/'scripts/atlas_application_v2.py',
             PAPER/'scripts/run_trial.py',PAPER/'scripts/kernels.py',
             PAPER/'scripts/run_upgrade_campaign.py',PAPER/'scripts/node_trial.py',
             PAPER/'scripts/node_pool.py',
             ROOT/'taskvine/src/tools/datavine_executor',
             ROOT/'taskvine/src/worker/vine_worker',ROOT/'taskvine/src/tools/datavine_workflow',
             ROOT/'taskvine/src/bindings/python3/ndcctools/taskvine/_cvine.so']
    fingerprints = {str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    config = {k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()}
    plan = dict(schema='datavine.research-campaign/v1',config=config,order=order,
                fingerprints=fingerprints,host=os.uname().nodename,
                cpuset=sorted(os.sched_getaffinity(0)),job_id=os.environ.get('JOB_ID'),
                condor_ad=Path(os.environ['_CONDOR_JOB_AD']).read_text() if '_CONDOR_JOB_AD' in os.environ else None)
    args.output.mkdir(parents=True,exist_ok=False)
    atomic(args.output/'plan.json',plan)
    rows = []
    references = {}
    mismatches = []
    for job in order:
        label = f"r{job['repetition']}-w{job['workers']}-{job['workload']}-b{job['bytes']}-{job['variant']}"
        trial = args.output/label
        command = [sys.executable,str(PAPER/'scripts/research_trial.py'),*VARIANTS[job['variant']],
            '--cores',str(args.cores),'--batch-type',args.batch_type,'--timeout',str(args.timeout),
            '--output',str(trial)]
        for k,v in job.items():
            command += ['--'+k.replace('_','-'),str(v)]
        if args.manifest:
            command += ['--manifest',str(args.manifest.resolve())]
        if args.pool:
            command += ['--pool',str(args.pool.resolve())]
        with (args.output/(label+'.log')).open('w') as log:
            try:
                done = subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,
                                      timeout=args.timeout+220,start_new_session=True)
            except subprocess.TimeoutExpired:
                # Never advance to a second timed trial beside an orphan.
                raise RuntimeError('trial exceeded outer deadline; inspect owned process group')
        if (trial/'result.json').exists():
            result = json.loads((trial/'result.json').read_text())
            if done.returncode != 0:
                result['status'] = 'FAIL'
                result['runner_exit_code'] = done.returncode
        else:
            result = dict(status='FAIL',error='no terminal result',runner_exit_code=done.returncode)
        row = dict(path=str(trial),configuration=job,**result)
        rows.append(row)
        if result['status'] == 'PASS':
            key = (job['workload'],job['bytes'],job['workers'])
            science = result['research'].get('scientific_output',
                [result.get('result_digest'),result.get('payload_sha256')])
            if key in references and references[key] != science:
                mismatches.append(label)
            references.setdefault(key,science)
        summary = dict(status='RUNNING',planned=len(order),completed=len(rows),
            passed=sum(r['status']=='PASS' for r in rows),runs=rows,scientific_mismatches=mismatches)
        if len(rows) == len(order):
            summary['status'] = 'PASS' if summary['passed']==len(order) and not mismatches else 'PARTIAL'
        atomic(args.output/'summary.json',summary)
        print(json.dumps({k:summary[k] for k in ['status','planned','completed','passed']}),flush=True)
        if result['status'] != 'PASS':
            summary['status'] = 'PARTIAL'
            atomic(args.output/'summary.json',summary)
            return 1
    return 0 if summary['status']=='PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
