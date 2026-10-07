#!/usr/bin/env python3
"""Real controller metadata protocol, no physical worker or task execution."""
import json,os,random,subprocess
from pathlib import Path
P=Path(__file__).resolve().parent;ROOT=P.parents[1]
out=P/'rpc-raw';out.mkdir(exist_ok=True)
cpus=sorted(os.sched_getaffinity(0))[:12];assert len(cpus)==12
plan=[];rng=random.Random(1709)
for rep in range(1,4):
    ts=[1,2,4,8];rng.shuffle(ts)
    plan.extend(dict(rep=rep,threads=t) for t in ts)
(P/'rpc-campaign.json').write_text(json.dumps(plan,indent=2)+'\n')
env=dict(os.environ,PYTHONPATH=str(ROOT/'test_support/python_modules/python3'))
for c in plan:
    name=f"r{c['rep']}-s{c['threads']}";print(name,flush=True)
    cmd=['taskset','-c',','.join(map(str,cpus)),
         '/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin/python',
         str(ROOT/'acceptance/scripts/benchmark_controller_rpc.py'),
         '--records','1024','--connections','16','--service-threads',str(c['threads']),
         '--data-threads','1','--iterations','100','--latency-sample-stride','16',
         '--client-mode','processes','--output',str(out/(name+'.json'))]
    with (out/(name+'.log')).open('w') as log:
        r=subprocess.run(cmd,env=env,stdout=log,stderr=log,timeout=120)
    if r.returncode:raise RuntimeError(name+' failed; retained log')
print('RPC CAMPAIGN PASS',flush=True)
