#!/usr/bin/env python3
"""Reconstruct starting dirty source privately and emit only this work's delta."""
import difflib,hashlib,json,subprocess,tempfile
from pathlib import Path
P=Path(__file__).resolve().parents[1];ROOT=P.parent;D=P/'provenance'
before=json.loads((D/'starting-dirty-files.json').read_text())
allowed={'DATAVINE_PRODUCTION.md','acceptance/README.md','acceptance/matrix.md','acceptance/scripts/benchmark_worker_churn.py','taskvine/src/worker/vine_process.h','taskvine/src/worker/vine_worker.c','taskvine/src/manager/vine_manager.c','taskvine/src/manager/vine_worker_pool.c','taskvine/src/manager/vine_worker_pool.h'}
changed=[n for n,h in before.items() if not (ROOT/n).exists() or hashlib.sha256((ROOT/n).read_bytes()).hexdigest()!=h]
if set(changed)-allowed:raise RuntimeError('unexpected starting source change: '+str(set(changed)-allowed))
head='c2e9a85be3e52d2b7f0210487a6632b8c03a413e';patch=[];records=[]
original_dirty_changed=len(changed)
tracked=set(subprocess.check_output(['git','-C',str(ROOT),'diff','--name-only'],text=True).splitlines())
new_tracked=tracked-set(before)
if new_tracked-allowed:raise RuntimeError('unexpected formerly clean source change: '+str(new_tracked-allowed))
changed.extend(sorted(new_tracked))
starting_hashes=dict(before)
for n in new_tracked:starting_hashes[n]=hashlib.sha256(subprocess.check_output(['git','-C',str(ROOT),'show',head+':'+n])).hexdigest()
with tempfile.TemporaryDirectory(prefix='datavine-paper-scope-') as tmp:
    base=Path(tmp)
    for n in changed:
        target=base/n;target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(subprocess.check_output(['git','-C',str(ROOT),'show',head+':'+n]))
    subprocess.run(['git','apply',*[f'--include={n}' for n in changed],str(D/'starting.diff')],cwd=base,check=True)
    for n in sorted(changed):
        old=(base/n).read_bytes();new=(ROOT/n).read_bytes()
        if hashlib.sha256(old).hexdigest()!=starting_hashes[n]:raise RuntimeError('starting reconstruction mismatch: '+n)
        patch.extend(difflib.unified_diff(old.decode().splitlines(True),new.decode().splitlines(True),fromfile='starting/'+n,tofile='current/'+n))
        records.append(dict(path=n,before=starting_hashes[n],after=hashlib.sha256(new).hexdigest()))
(D/'this-work-source.patch').write_text(''.join(patch))
(D/'scope-audit.json').write_text(json.dumps(dict(status='PASS',starting_head=head,changed_sources=records,preserved_starting_dirty_files=len(before)-original_dirty_changed,scope_exception='Initial root make rebuilt some unrelated ignored objects. No unrelated source edit/deletion. This check covers starting tracked dirty sources, not ignored object history.'),indent=2)+'\n')
print(json.dumps(dict(status='PASS',changed=len(records),preserved=len(before)-original_dirty_changed)))
