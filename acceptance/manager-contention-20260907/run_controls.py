import json,random,subprocess,sys,tempfile,time
from pathlib import Path
P=Path(__file__).resolve().parent;out=P/'separate-raw';out.mkdir(exist_ok=True)
plan=[(r,o,g) for r in range(1,4) for o in [1,256] for g in ['immediate','deferred']];random.Random(701).shuffle(plan)
(P/'separate-campaign.json').write_text(json.dumps(plan))
for rep,objects,gc in plan:
    print(rep,objects,gc,flush=True)
    with tempfile.TemporaryDirectory(prefix='manager-barrier-') as td:
        gate=str(Path(td)/'go');jobs=[];logs=[]
        try:
            for mode in ['probe-only','pressure-only']:
                name=f'r{rep}-o{objects}-{gc}-{mode}';log=(out/(name+'.log')).open('w');logs.append(log)
                cmd=[sys.executable,str(P/'control.py'),'--mode',mode,'--gate',gate,'--tasks','256','--objects',str(objects),'--gc',gc,'--output',str(out/(name+'.json'))]
                jobs.append(subprocess.Popen(cmd,stdout=log,stderr=log))
            until=time.monotonic()+90
            while not all(Path(gate+'.'+m+'.ready').exists() for m in ['probe-only','pressure-only']):
                if any(p.poll() is not None for p in jobs) or time.monotonic()>until:raise RuntimeError('barrier failed')
                time.sleep(.05)
            Path(gate).touch()
            for p in jobs:assert p.wait(timeout=360)==0
        finally:
            for p in jobs:
                if p.poll() is None:p.terminate();p.wait(timeout=20)
            for log in logs:log.close()
print('SEPARATE CONTROLS PASS',flush=True)
