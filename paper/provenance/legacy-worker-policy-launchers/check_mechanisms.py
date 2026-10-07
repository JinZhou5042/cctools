#!/usr/bin/env python3
"""Focused execution-order acceptance plus the existing generic library test."""
import json,os,signal,subprocess,sys,tempfile,time
from pathlib import Path
PAPER=Path(__file__).resolve().parents[1]
ROOT=PAPER.parent

def main():
    out=Path(sys.argv[1]).resolve() if len(sys.argv)>1 else PAPER/'results/mechanism-acceptance'
    out.mkdir(exist_ok=False)
    rows=[]
    for policy in ('deferred','eager'):
        command=[sys.executable,str(PAPER/'scripts/run_trial.py'),'--workload','phase',
                 '--width','8','--bytes','4096','--cpu-ms','30','--workers','1',
                 '--cores','1','--policy','fixed','--window','1','--trace',
                 '--preparation',policy,'--output',str(out/policy)]
        completed=subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        (out/f'{policy}.log').write_text(completed.stdout)
        result=json.loads((out/policy/'result.json').read_text())
        traces=result['attempt_trace']
        valid=(completed.returncode==0 and len(traces)==result['tasks'] and all(
            0<r['received_us']<=r['prepare_start_us']<=r['prepare_ready_us']<=r['execute_start_us']<=r['complete_us']
            for r in traces))
        rows.append(dict(case=policy,status='PASS' if valid else 'FAIL',attempts=len(traces)))
    # Run the repository's actual generic serverless test in a private scratch
    # directory, so its inputs and outputs cannot overwrite repository files.
    env=dict(os.environ,PYTHONNOUSERSITE='1',PYTHONDONTWRITEBYTECODE='1',
             PYTHONPATH=str(ROOT/'test_support/python_modules/python3'),
             PATH=f'{Path(sys.executable).parent}:{os.environ["PATH"]}')
    with tempfile.TemporaryDirectory(prefix='datavine-paper-generic-') as temp:
        scratch=Path(temp);worker=None;manager=None
        with (out/'generic-serverless.log').open('w') as log:
            try:
                manager=subprocess.Popen([sys.executable,str(ROOT/'taskvine/test/vine_python_serverless.py'),str(scratch/'port')],cwd=scratch,env=env,stdout=log,stderr=log,start_new_session=True)
                deadline=time.monotonic()+30
                while not (scratch/'port').exists() and manager.poll() is None and time.monotonic()<deadline:time.sleep(.1)
                port=(scratch/'port').read_text().strip()
                worker=subprocess.Popen([str(ROOT/'taskvine/src/worker/vine_worker'),'--cores','8','--memory','1000','--disk','1000','--idle-timeout','90','localhost',port],cwd=scratch,env=env,stdout=log,stderr=log,start_new_session=True)
                code=manager.wait(timeout=150)
                rows.append(dict(case='generic-serverless',status='PASS' if code==0 else 'FAIL',returncode=code))
            except Exception as error:
                rows.append(dict(case='generic-serverless',status='FAIL',error=str(error)))
            finally:
                for process in (worker,manager):
                    if process and process.poll() is None:
                        os.killpg(process.pid,signal.SIGTERM)
                        try:process.wait(timeout=10)
                        except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait()
    summary=dict(status='PASS' if all(r['status']=='PASS' for r in rows) else 'FAIL',checks=rows)
    (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary));return 0 if summary['status']=='PASS' else 1
if __name__=='__main__':raise SystemExit(main())
