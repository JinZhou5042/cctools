#!/usr/bin/env python3
"""Enforce the paper's CPU capacity without changing Worker resource promises."""
import os,sys
allowed=sorted(os.sched_getaffinity(0))
cores=int(os.environ['DATAVINE_PAPER_WORKER_CORES'])
requested=os.environ.get('DATAVINE_PAPER_CPUSET','')
selected=[int(x) for x in requested.split(',')] if requested else allowed[:cores]
if len(selected)!=cores or not set(selected).issubset(allowed):
    raise SystemExit('Worker CPU affinity cannot satisfy requested capacity')
os.sched_setaffinity(0,selected)
binary=os.environ['DATAVINE_PAPER_WORKER_BINARY']
print('paper-worker-cpuset='+','.join(map(str,sorted(os.sched_getaffinity(0)))),file=sys.stderr,flush=True)
os.execv(binary,[binary,*sys.argv[1:]])
