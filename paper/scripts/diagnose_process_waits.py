#!/usr/bin/env python3
"""Diagnostic-only process wait sampling; excluded from performance summaries."""
import json,os,sys,threading,time
from pathlib import Path
import run_trial
output=Path(sys.argv[sys.argv.index('--output')+1]).resolve()
original=run_trial.launch_workers
finished=threading.Event();samplers=[]
def launch(*args,**kwargs):
    processes=original(*args,**kwargs)
    def sample():
        started=time.monotonic()
        with (output/'process-waits.jsonl').open('w') as stream:
            while not finished.is_set():
                pending=[p.pid for p in processes];seen=set();rows=[]
                while pending:
                    pid=pending.pop()
                    if pid in seen:continue
                    seen.add(pid)
                    try:
                        path=Path('/proc')/str(pid)
                        pending.extend(map(int,(path/'task'/str(pid)/'children').read_text().split()))
                        fields=(path/'stat').read_text().rsplit(')',1)[1].split()
                        rows.append(dict(pid=pid,state=fields[0],wchan=(path/'wchan').read_text().strip(),cpu_ticks=int(fields[11])+int(fields[12]),comm=(path/'comm').read_text().strip()))
                    except (OSError,ValueError,IndexError):pass
                stream.write(json.dumps(dict(elapsed=time.monotonic()-started,processes=rows))+'\n');stream.flush()
                finished.wait(.25)
    thread=threading.Thread(target=sample,daemon=True);thread.start();samplers.append(thread)
    return processes
run_trial.launch_workers=launch
try:code=run_trial.main()
finally:
    finished.set()
    for t in samplers:t.join(timeout=2)
raise SystemExit(code)
