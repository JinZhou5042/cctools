#!/usr/bin/env python3
"""Fail-closed paper/evidence audit; missing campaigns are not successful audits."""
import argparse,hashlib,json,re,subprocess
from pathlib import Path
PAPER=Path(__file__).resolve().parents[1];ROOT=PAPER.parent

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--allow-running',action='store_true');args=parser.parse_args()
    checks=[]
    def check(name,ok,detail=''):checks.append(dict(check=name,status='PASS' if ok else 'OPEN',detail=detail))
    for name,count in [('compute-campaign-v4',96),('distributed-campaign-v4',48),('data-pipeline-v1',15),('preloaded-campaign-v1',48),('preloaded-data-v1',15)]:
        path=PAPER/'results'/name/'summary.json'
        if not path.exists():check(name,False,'missing summary');continue
        summary=json.loads(path.read_text());rows=summary['runs']
        check(name,summary['status']=='PASS' and len(rows)==count and not summary.get('scientific_mismatches'),f"{summary['status']}: {summary.get('passed',0)}/{count}")
        valid=True
        for row in rows:
            if row['status']!='PASS':valid=False;continue
            valid &= row['logical_identities']==row['tasks']
            valid &= row['worker_cpu_limit']=='explicit affinity to declared cores'
            if row['backend']=='datavine':valid &= row['physical_counts']==dict(submissions=row['tasks'],completions=row['tasks'])
        check(name+' invariants',valid,'logical counts, physical attempts, CPU affinity; failures retained')
        agreement=True
        for row in rows:
            trial=path.parent/Path(row['path']).name/'result.json'
            agreement &= trial.exists() and json.loads(trial.read_text())=={k:v for k,v in row.items() if k!='path'}
        check(name+' retained trial agreement',agreement,'summary rows equal terminal per-trial results')

        plan=json.loads((path.parent/'plan.json').read_text())
        check(name+' executable fingerprints',all((ROOT/key).exists() and hashlib.sha256((ROOT/key).read_bytes()).hexdigest()==value for key,value in plan['fingerprints'].items()),'current inputs must match measured version')
    regression=json.loads((PAPER/'results/regression-recall-fix.json').read_text())
    check('DataVine regression',regression['status']=='PASS' and regression['passed_count']==regression['test_count'],f"{regression['passed_count']}/{regression['test_count']}")
    mechanism=json.loads((PAPER/'results/mechanism-acceptance-v3/summary.json').read_text())
    check('mechanism and generic runtime',mechanism['status']=='PASS')
    churn=json.loads((PAPER/'results/worker-churn-v3/summary.json').read_text())
    check('worker removal correctness',churn['status']=='PASS' and churn['correctness']['journal']['unique_completed_tasks']==1024 and churn['correctness']['all_sinks_match'])
    pressure=json.loads((PAPER/'results/pressure-v2.json').read_text())
    check('memory and recall pressure',pressure['status']=='PASS' and len(pressure['results'])==24)
    check('pressure native fingerprints',all(hashlib.sha256((ROOT/p).read_bytes()).hexdigest()==pressure['provenance'][k] for k,p in [('datavine_workflow_sha256','taskvine/src/tools/datavine_workflow'),('vine_worker_sha256','taskvine/src/worker/vine_worker')]))
    routes=json.loads((PAPER/'results/routes-v1/summary.json').read_text())
    check('source-isolated routes',routes['status']=='PASS' and len(routes['runs'])==6)
    check('route benchmark source',hashlib.sha256((ROOT/'acceptance/scripts/benchmark_peer_vs_controller.py').read_bytes()).hexdigest()==routes['source_sha256'])
    for name in ['supplemental-provenance.json','preloaded-provenance.json']:
        data=json.loads((PAPER/'figures'/name).read_text())
        check(name,all(hashlib.sha256((PAPER/k).read_bytes()).hexdigest()==v for k,v in data['inputs'].items()))
    scope=json.loads((PAPER/'provenance/scope-audit.json').read_text())
    check('reviewable delta matches source',scope['status']=='PASS' and all(hashlib.sha256((ROOT/r['path']).read_bytes()).hexdigest()==r['after'] for r in scope['changed_sources']))
    pdf=PAPER/'build/paper.pdf'
    check('compiled PDF',pdf.exists())
    if pdf.exists():
        text=subprocess.check_output(['pdftotext','-layout',str(pdf),'-'],text=True)
        pages=text.split('\f');bodypages=len(pages)-1
        for i,page in enumerate(pages):
            match=re.search(r'R\s*E\s*F\s*E\s*R\s*E\s*N\s*C\s*E\s*S',page)
            if match:
                bodypages=i+int(bool(page[:match.start()].strip()))
                break
        check('body page limit',bodypages<=10,f'{bodypages} body pages excluding a references-only page')
        check('anonymous paper text',not re.search(r'jzhou24|/users/|/groups/|condorfe|crc\.nd\.edu',text),'artifact provenance itself is internal and not anonymized')
        check('AI disclosure and citation','ACKNOWLEDGMENTS' in text and 'OpenAI Codex' in text)
        log=(PAPER/'build/latex-pass3.log').read_text()
        check('LaTeX references and layout',not re.search(r'undefined|Overfull|LaTeX Error',log))
    bib=(PAPER/'references.bib').read_text()
    check('bibliography complete-author form','and others' not in bib and all('author' in e and ('url=' in e or 'url =' in e or 'doi=' in e or 'doi =' in e) for e in bib.split('@')[1:]))
    figure=json.loads((PAPER/'figures/provenance.json').read_text())
    check('figures current',all(hashlib.sha256((PAPER/k).read_bytes()).hexdigest()==v for k,v in figure['inputs'].items()),'regenerate figures after measurements or plotting inputs change')
    before=json.loads((PAPER/'provenance/starting-dirty-files.json').read_text())
    allowed={'DATAVINE_PRODUCTION.md','acceptance/README.md','acceptance/matrix.md','acceptance/scripts/benchmark_worker_churn.py','taskvine/src/worker/vine_process.h','taskvine/src/worker/vine_worker.c','taskvine/src/manager/vine_manager.c','taskvine/src/manager/vine_worker_pool.c','taskvine/src/manager/vine_worker_pool.h'}
    changed=[name for name,digest in before.items() if not (ROOT/name).exists() or hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=digest]
    check('preserve unrelated starting dirty files',not(set(changed)-allowed),str(sorted(set(changed)-allowed)))
    tracked=set(subprocess.check_output(['git','-C',str(ROOT),'diff','--name-only'],text=True).splitlines())
    check('tracked source scope',not(tracked-set(before)-allowed),str(sorted(tracked-set(before)-allowed)))
    report=dict(status='PASS' if all(c['status']=='PASS' for c in checks) else 'OPEN',checks=checks,
                scope_note='Initial root build rebuilt some unrelated ignored object files; stopped and subsequent builds scoped to TaskVine. This audit does not claim those objects were untouched.',
                readiness='A passing artifact audit verifies retained evidence and format, not conference acceptance or production-application coverage.')
    (PAPER/'results/artifact-audit.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
    return 0 if report['status']=='PASS' or args.allow_running else 1
if __name__=='__main__':raise SystemExit(main())
