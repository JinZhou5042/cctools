#!/usr/bin/env python3
"""Build benchmark-only scheduler variants without changing repository binaries."""
import difflib, hashlib, json, os, shlex, shutil, subprocess
from pathlib import Path
P=Path(__file__).resolve().parent
ROOT=P.parents[1]
source=ROOT/'taskvine/src/manager/vine_schedule.c'
original=source.read_text()
needle='\t\t/* compute the size of cached and uncached input files on the worker */'
insert='''\t\t/* Benchmark candidate: reject workers with no committable resources
\t\t * before traversing every input mount. The final compatibility gate
\t\t * already applies this same predicate. No data work is offloaded. */
\t\tif (!check_worker_have_committable_resources(q, w)) {
\t\t\tcontinue;
\t\t}

'''
assert original.count(needle)==1
optimized=original.replace(needle,insert+needle)
(P/'scheduler-candidate.patch').write_text(''.join(difflib.unified_diff(original.splitlines(True),optimized.splitlines(True),fromfile='baseline/vine_schedule.c',tofile='candidate/vine_schedule.c')))
env=dict(os.environ,PATH='/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:'+os.environ['PATH'])
def dry(directory,target,prefix):
    lines=subprocess.check_output(['make','-n','-B',target],cwd=directory,env=env,text=True).splitlines()
    return shlex.split(next(line.split(';',1)[-1] for line in lines if prefix in line))
compile_cmd=dry(ROOT/'taskvine/src/manager','vine_schedule.o','echo COMPILE vine_schedule.o;')
link_cmd=dry(ROOT/'taskvine/src/bindings/python3','ndcctools/taskvine/_cvine.so','echo LINK ndcctools/taskvine/_cvine.so;')
commands=[]
for variant,text in [('baseline',original),('candidate',optimized)]:
    out=P/'build'/variant;out.mkdir(parents=True,exist_ok=True)
    (out/'vine_schedule.c').write_text(text)
    lib=out/'libtaskvine.a'
    shutil.copy2(ROOT/'taskvine/src/manager/libtaskvine.a',lib)
    cmd=[str(out/'vine_schedule.o') if x=='vine_schedule.o' else str(out/'vine_schedule.c') if x=='vine_schedule.c' else x for x in compile_cmd]
    subprocess.run(cmd,env=env,check=True);commands.append(cmd)
    cmd=['ar','r',str(lib),str(out/'vine_schedule.o')]
    subprocess.run(cmd,check=True);commands.append(cmd)
    cmd=[str(out/'_cvine.so') if x=='ndcctools/taskvine/_cvine.so' else str(ROOT/'taskvine/src/bindings/python3/vine_wrap.o') if x=='vine_wrap.o' else str(lib) if x==str(ROOT/'taskvine/src/manager/libtaskvine.a') else x for x in link_cmd]
    subprocess.run(cmd,env=env,check=True);commands.append(cmd)
(P/'build-commands.json').write_text(json.dumps(commands,indent=2)+'\n')
print('ISOLATED BUILDS PASS')
