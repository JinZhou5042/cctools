#!/usr/bin/env python3
import json, os, random, subprocess, sys, time
from pathlib import Path
P = Path(__file__).resolve().parent
ROOT = P.parents[1]
cpus = sorted(os.sched_getaffinity(0))[:12]
assert len(cpus) == 12
plan = []
rng = random.Random(9072026)
for rep in range(1, 4):
    block = [(threads, policy) for threads in [1, 2, 4, 8] for policy in ['durable', 'local']]
    rng.shuffle(block)
    plan.extend(dict(rep=rep, threads=t, policy=p) for t,p in block)
(P/'campaign.json').write_text(json.dumps(plan, indent=2)+'\n')
for c in plan:
    name = f"r{c['rep']}-d{c['threads']}-{c['policy']}"
    output = P/'raw'/(name+'.json')
    cmd = ['taskset','-c',','.join(map(str,cpus)),sys.executable,
           str(ROOT/'acceptance/scripts/benchmark_native_workflow.py'),
           '--tasks','256','--workers','4','--cores','2','--memory','1024',
           '--disk','2048','--registration','sealed','--workflow-recovery','none',
           '--executor','command','--command-argv-json',json.dumps(['/bin/dd','if=/dev/zero','of=payload','bs=1048576','count=1']),
           '--command-output-name','payload','--expected-result-file',str(P/'expected.bin'),
           '--idata-backup','worker-local','--persistence-diagnostics',
           '--request-all-outputs' if c['policy']=='durable' else '--no-requested-output',
           '--workflow-timeout','120','--worker-timeout','60','--output',str(output)]
    env = dict(os.environ,DATAVINE_RPC_THREADS='1',DATAVINE_DATA_THREADS=str(c['threads']))
    started=time.time()
    print(name,flush=True)
    with (P/'raw'/(name+'.driver.log')).open('w') as log:
        result=subprocess.run(cmd,env=env,stdout=log,stderr=log,timeout=200)
    if result.returncode:raise RuntimeError(f'{name}: return {result.returncode}; logs retained')
    record=dict(config=c,command=cmd,cpus=cpus,rpc_threads=1,data_threads=c['threads'],started_epoch=started,finished_epoch=time.time())
    (P/'raw'/(name+'.invocation.json')).write_text(json.dumps(record,indent=2)+'\n')
print('CAMPAIGN PASS',flush=True)
