#!/usr/bin/env python3
"""Randomized blocked trial campaign with provenance-checked resume.

Failed and incomplete trials are retained and excluded from performance
summaries. Reusing a campaign after changing executable inputs is prohibited.
"""

import argparse
import hashlib
import json
from pathlib import Path
import random
import subprocess
import sys
import time

PAPER = Path(__file__).resolve().parents[1]
ROOT = PAPER.parent
VARIANTS = {
    'dv-fixed1': ['--backend','datavine','--policy','fixed','--window','1'],
    'dv-fixed2': ['--backend','datavine','--policy','fixed','--window','2'],
    'dv-fixed4': ['--backend','datavine','--policy','fixed','--window','4'],
    'dv-elastic': ['--backend','datavine','--policy','elastic'],
    'dv-eager': ['--backend','datavine','--policy','elastic','--preparation','eager'],
    'tv-fixed1': ['--backend','taskvine','--window','1'],
    'tv-fixed2': ['--backend','taskvine','--window','2'],
    'tv-fixed4': ['--backend','taskvine','--window','4'],
}


def atomic(path,value):
    temp=path.with_suffix('.tmp')
    temp.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
    temp.replace(path)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--workloads',default='spectral,histogram,phase,quadrature')
    p.add_argument('--variants',default=','.join(VARIANTS))
    p.add_argument('--repetitions',type=int,default=3)
    p.add_argument('--workers',default='2',help='comma-separated Worker counts')
    p.add_argument('--cores',type=int,default=4)
    p.add_argument('--width',type=int,default=64)
    p.add_argument('--levels',type=int,default=4)
    p.add_argument('--bytes',type=int,default=262144)
    p.add_argument('--cpu-ms',type=float,default=100)
    p.add_argument('--batch-type',choices=('local','condor'),default='local')
    p.add_argument('--timeout',type=float,default=300)
    p.add_argument('--admission-timeout',type=float,default=180)
    p.add_argument('--seed',type=int,default=20260906)
    p.add_argument('--trace',action='store_true')
    p.add_argument('--plan-only',action='store_true')
    args=p.parse_args()
    names=args.variants.split(',');workloads=args.workloads.split(',')
    workers=[int(x) for x in args.workers.split(',')]
    if set(names)-set(VARIANTS) or min(workers+[args.cores,args.width,args.levels,args.repetitions])<1:
        p.error('invalid variants or non-positive trial dimensions')
    config=vars(args).copy();config['output']=str(args.output.resolve());config.pop('plan_only')
    paths=[PAPER/'scripts/run_preloaded_trial.py',PAPER/'scripts/kernels.py',PAPER/'scripts/pinned_worker.py',Path(__file__).resolve(),
           PAPER/'scripts/run_trial.py', PAPER/'implementation/datavine_python_preloaded_executor', PAPER/'implementation/preload-provenance.json', ROOT/'taskvine/src/tools/datavine_executor', ROOT/'taskvine/src/worker/vine_worker',ROOT/'taskvine/src/tools/datavine_workflow']
    fingerprints={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    rng=random.Random(args.seed);order=[]
    for repetition in range(1,args.repetitions+1):
        blocks=[(w,k) for w in workers for k in workloads];rng.shuffle(blocks)
        for w,k in blocks:
            variants=names.copy();rng.shuffle(variants)
            order.extend(dict(repetition=repetition,workers=w,workload=k,variant=v) for v in variants)
    plan=dict(schema='datavine.paper-campaign/v1',config=config,fingerprints=fingerprints,order=order)
    args.output.mkdir(parents=True,exist_ok=True)
    plan_path=args.output/'plan.json'
    if plan_path.exists() and json.loads(plan_path.read_text())!=plan:
        raise RuntimeError('campaign configuration/source changed; choose a new output directory')
    atomic(plan_path,plan)
    if args.plan_only:
        print(json.dumps({'status':'PLAN_ONLY','trials':len(order)}));return 0
    rows=[]
    for job in order:
        name=f"r{job['repetition']}-w{job['workers']}-{job['workload']}-{job['variant']}"
        destination=args.output/name;result_path=destination/'result.json'
        if result_path.exists():
            result=json.loads(result_path.read_text())
        elif destination.exists():
            # An interrupted trial is not silently rerun under the same identity.
            result=dict(status='INCOMPLETE',error='trial directory lacks terminal result',**job)
        else:
            command=[sys.executable,str(PAPER/'scripts/run_preloaded_trial.py'),*VARIANTS[job['variant']],
                     '--variant',job['variant'],'--workload',job['workload'],
                     '--repetition',str(job['repetition']),'--workers',str(job['workers']),
                     '--cores',str(args.cores),'--width',str(args.width),'--levels',str(args.levels),
                     '--bytes',str(args.bytes),'--cpu-ms',str(args.cpu_ms),
                     '--batch-type',args.batch_type,'--timeout',str(args.timeout),
                     '--admission-timeout',str(args.admission_timeout),'--output',str(destination)]
            if args.trace:command+=['--trace']
            with (args.output/f'{name}.runner.log').open('w') as stream:
                completed=subprocess.run(command,stdout=stream,stderr=subprocess.STDOUT)
            result=json.loads(result_path.read_text()) if result_path.exists() else dict(
                status='FAIL',error=f'runner exited {completed.returncode} without result',**job)
        rows.append(dict(path=str(destination),**result))
        summary=dict(status='RUNNING',completed=len(rows),planned=len(order),
                     passed=sum(r['status']=='PASS' for r in rows),runs=rows,
                     updated=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()))
        atomic(args.output/'summary.json',summary)
        print(json.dumps({k:summary[k] for k in ('status','completed','planned','passed')}),flush=True)
    # Compare scientific content across variants within the same workload and
    # topology. Timings and per-host provenance deliberately do not enter this.
    references={};mismatches=[]
    for row in rows:
        if row['status']!='PASS':continue
        key=(row['workload'],row['workers'])
        value=(row['result_digest'],row['payload_sha256'])
        if key in references and references[key]!=value:mismatches.append(row['path'])
        references.setdefault(key,value)
    summary.update(status='PASS' if summary['passed']==len(order) and not mismatches else 'PARTIAL',
                   scientific_mismatches=mismatches)
    atomic(args.output/'summary.json',summary)
    return 0 if summary['status']=='PASS' else 1


if __name__=='__main__':raise SystemExit(main())
