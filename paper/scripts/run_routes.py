#!/usr/bin/env python3
"""Repeat existing route-isolation experiments; preserve all failed attempts."""
import hashlib,json,os,random,subprocess,sys
from pathlib import Path
PAPER=Path(__file__).resolve().parents[1];ROOT=PAPER.parent
out=PAPER/'results/routes-v1';out.mkdir(exist_ok=False)
env=dict(os.environ,PYTHONNOUSERSITE='1',PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=str(ROOT/'test_support/python_modules/python3'))
env['PATH']=str(Path(sys.executable).parent)+':'+env['PATH']
rng=random.Random(20260906);rows=[]
script=ROOT/'acceptance/scripts/benchmark_peer_vs_controller.py'
for repetition in range(1,4):
    modes=['peer','controller'];rng.shuffle(modes)
    for mode in modes:
        name=f'r{repetition}-{mode}';path=out/f'{name}.json'
        command=[sys.executable,str(script),'--mode',mode,'--files','128','--bytes-per-file','262144','--producer-workers','2','--consumer-workers','2','--cores-per-worker','1','--producer-memory','1024','--consumer-memory','2048','--batch-type','condor','--timeout','180','--output',str(path)]
        with (out/f'{name}.log').open('w') as log:
            code=subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
        result=json.loads(path.read_text()) if path.exists() else dict(status='FAIL',returncode=code)
        rows.append(dict(repetition=repetition,mode=mode,**{k:v for k,v in result.items() if k!='mode'}))
        summary=dict(status='RUNNING',planned=6,runs=rows,source_sha256=hashlib.sha256(script.read_bytes()).hexdigest(),scope='route isolation; batch CPU shares; byte-count oracle; frontend network counters are shared')
        (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
        print(name,result['status'],flush=True)
summary['status']='PASS' if len(rows)==6 and all(r['status']=='PASS' for r in rows) else 'PARTIAL'
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
raise SystemExit(0 if summary['status']=='PASS' else 1)
