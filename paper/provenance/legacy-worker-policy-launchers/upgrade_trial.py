#!/usr/bin/env python3
"""Versioned deployment controls for research trials."""
import json
import os
from pathlib import Path
import sys
import research_trial


def main():
    if os.environ.get('DATAVINE_RESEARCH_LOCAL_DEPS'):
        directory=Path(os.environ['DATAVINE_RESEARCH_LOCAL_DEPS'])
        research_trial.DEPS=directory
        sys.path[:3]=[str(directory),str(research_trial.ROOT/'test_support/python_modules/python3'),
                      str(research_trial.PAPER/'.deps/python3.10')]
    code=research_trial.main()
    output=Path(sys.argv[sys.argv.index('--output')+1])
    result=json.loads((output/'result.json').read_text())
    graph=json.loads((output/'graph.json').read_text())
    eligible=sum(len(n['parents'])==1 for n in graph['nodes'])
    result['research']['single_parent_tasks']=eligible
    if result['batch_type']=='reserved':
        result['research']['allocation']['scope']='Persistent host-pinned Condor allocations; requested CPU counts and physical host identity are retained in job ads. Does not establish whole-node exclusivity.'
    result['research']['dependency_storage']='node-local staged' if os.environ.get('DATAVINE_RESEARCH_LOCAL_DEPS') else 'shared filesystem'
    research_trial.base.atomic_json(output/'result.json',result)
    print(json.dumps(dict(status=result['status'],final=True,error=result.get('error'))),flush=True)
    return code


if __name__=='__main__':raise SystemExit(main())
