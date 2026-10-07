#!/usr/bin/env python3
"""Same paper trial with symmetric, explicit library preloading on both backends."""
import importlib,json,os,sys
from pathlib import Path
import run_trial
PAPER=Path(__file__).resolve().parents[1]
metadata=json.loads((PAPER/'implementation/preload-provenance.json').read_text())
os.environ['OPENBLAS_NUM_THREADS']='1'
os.environ['OMP_NUM_THREADS']='1'
modules=[importlib.import_module(name) for name in metadata['modules']]
original=run_trial.vine.Manager.create_library_from_functions

def create(manager,*args,**kwargs):
    kwargs['hoisting_modules']=modules
    return original(manager,*args,**kwargs)
run_trial.vine.Manager.create_library_from_functions=create
os.environ['DATAVINE_PYTHON_EXECUTOR_PATH']=str(PAPER/'implementation/datavine_python_preloaded_executor')
code=run_trial.main()
out=Path(sys.argv[sys.argv.index('--output')+1]).resolve()
p=out/'result.json';result=json.loads(p.read_text())
result['library_preload']=metadata
run_trial.atomic_json(p,result)
raise SystemExit(code)
