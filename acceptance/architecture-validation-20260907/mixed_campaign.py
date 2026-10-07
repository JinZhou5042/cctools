#!/usr/bin/env python3
import json,os,random,subprocess,time
from pathlib import Path
P=Path(__file__).resolve().parent
out=P/'mixed-raw';out.mkdir(exist_ok=True)
plan=[];rng=random.Random(7092026)
for rep in range(1,4):
    block=[(variant,objects,gc) for variant in ['baseline','candidate'] for objects in [1,256] for gc in ['immediate','deferred']]
    rng.shuffle(block)
    plan.extend(dict(rep=rep,variant=v,objects=o,gc=g) for v,o,g in block)
(P/'mixed-campaign.json').write_text(json.dumps(plan,indent=2)+'\n')
for c in plan:
    name=f"r{c['rep']}-{c['variant']}-o{c['objects']}-{c['gc']}"
    cmd=['/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin/python',str(P/'mixed.py'),'--objects',str(c['objects']),'--gc',c['gc'],'--output',str(out/(name+'.json'))]
    env=dict(os.environ,BENCH_CVINE=str(P/'build'/c['variant']/'_cvine.so'))
    print(name,flush=True)
    with (out/(name+'.log')).open('w') as log:
        r=subprocess.run(cmd,env=env,stdout=log,stderr=log,timeout=420)
    if r.returncode:raise RuntimeError(name+' failed; logs retained')
print('MIXED CAMPAIGN PASS',flush=True)
