#!/usr/bin/env python3
"""Retain a trial's full service and worker scratch state for root-cause analysis."""
import os,tempfile,shutil,sys
retained=[]
from pathlib import Path
import run_trial
class RetainedDirectory:
    def __init__(self,*args,**kwargs):
        self.name=tempfile.mkdtemp(prefix='datavine-paper-diagnostic-')
        retained.append(Path(self.name))
        print('Retained diagnostic scratch:',self.name,flush=True)
    def __enter__(self):return self.name
    def __exit__(self,*args):pass
run_trial.tempfile.TemporaryDirectory=RetainedDirectory
os.environ['DATAVINE_TASKVINE_TRANSACTION_LOG']='1'
os.environ['DATAVINE_TASKVINE_DEBUG_LOG']='1'
os.environ['DATAVINE_PERSISTENCE_DIAGNOSTICS']='1'
try:
    code=run_trial.main()
finally:
    if '--output' in sys.argv:
        output=Path(sys.argv[sys.argv.index('--output')+1]).resolve()
        for index,scratch in enumerate(retained):
            target=output/f'diagnostic-{index}';target.mkdir(exist_ok=True)
            for path in scratch.rglob('*'):
                if path.is_file() and (path.suffix in ('.debug','.log') or path.name in ('debug','transactions')):
                    destination=target/path.relative_to(scratch);destination.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,destination)
raise SystemExit(code)
