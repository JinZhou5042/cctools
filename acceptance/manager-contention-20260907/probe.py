#!/usr/bin/env python3
import argparse, hashlib, json, os, random, resource, subprocess, sys, tempfile, time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'test_support/python_modules/python3'))
import ndcctools.taskvine as v
OUT=Path(__file__).resolve().parent

def trial(a):
    orig=sorted(os.sched_getaffinity(0)); assert len(orig)>=5
    os.sched_setaffinity(0,{orig[0]})
    records=[]; workers=[]; logs=[]
    with tempfile.TemporaryDirectory(prefix='manager-contention-') as td:
        td=Path(td); m=v.Manager(port=0,run_info_path=str(td/'info'))
        try:
            for i in range(4):
                log=open(td/f'w{i}.log','w');logs.append(log)
                workers.append(subprocess.Popen(['taskset','-c',str(orig[i+1]),str(ROOT/'taskvine/src/worker/vine_worker'),'--feature',('probe' if i==0 else 'pressure'),'--cores','1','--memory','512','--disk','1024','--idle-timeout','120','--workdir',str(td/f'w{i}'),'localhost',str(m.port)],stdout=log,stderr=log))
            deadline=time.monotonic()+60
            while True:
                m.wait(1);m._refresh_stats()
                if m.stats.workers_connected==4 and m.stats.total_cores==4:break
                if time.monotonic()>deadline:raise TimeoutError('admission')
            names=['tasks_done','tasks_failed','tasks_submitted','workers_removed','bytes_sent','bytes_received','time_send','time_receive','time_status_msgs','time_internal','time_polling','time_application','time_scheduling','time_workers_execute_good']
            def stats():
                m._refresh_stats();return {k:int(getattr(m.stats,k)) for k in names}
            before=stats(); files={}; load=time.monotonic()
            for t in range(a.tasks):
                probe=v.Task('sleep 0.01');probe.set_cores(1);probe.add_feature('probe');probe.set_priority(100);probe.set_tag('probe-'+str(t));m.submit(probe)
                # Equal bytes, unique objects, no hashing/dedup reuse across tasks.
                task=v.Task(f'test "$(cat in* | wc -c)" -eq {a.bytes}');task.set_cores(1);task.add_feature('pressure');task.set_tag(str(t))
                fs=[]
                for j in range(a.objects):
                    size=a.bytes//a.objects;prefix=f'{t:08x}{j:08x}'.encode()
                    assert size>=len(prefix)
                    f=m.declare_buffer(prefix+b'x'*(size-len(prefix)),cache='workflow',peer_transfer=False)
                    task.add_input(f,f'in{j:05d}');fs.append(f)
                files[str(t)]=fs;m.submit(task)
            load=time.monotonic()-load
            start=time.monotonic(); epoch=time.time_ns()/1000; cpu=time.process_time(); thread_cpu=time.thread_time(); gc=0
            deadline=start+300
            for _ in range(2*a.tasks):
                while True:
                    t=m.wait(1)
                    if t:break
                    if time.monotonic()>deadline:raise TimeoutError('execution')
                assert t.successful(),(t.result,t.exit_code,t.output)
                rec={k:int(t.get_metric(k)) for k in ['time_when_submitted','time_when_commit_start','time_when_commit_end','time_when_done','time_workers_execute_last','bytes_sent']}
                rec['tag']=t.tag;records.append(rec)
                if a.gc=='immediate' and not t.tag.startswith('probe-'):
                    g=time.monotonic()
                    for f in files.pop(t.tag):m.undeclare_file(f)
                    gc+=time.monotonic()-g
            wall=time.monotonic()-start;cpu=time.process_time()-cpu;thread_cpu=time.thread_time()-thread_cpu;after=stats()
            g=time.monotonic()
            for fs in files.values():
                for f in fs:m.undeclare_file(f)
            deferred_gc=time.monotonic()-g
            delta={k:after[k]-before[k] for k in names}
            assert delta['tasks_done']==2*a.tasks and delta['tasks_failed']==0 and delta['workers_removed']==0,delta
            assert delta['tasks_submitted']==2*a.tasks
            tx=next((td/'info').rglob('transactions'))
            transaction_text=tx.read_text()
            assert sum(not line.startswith('#') and ' TASK ' in line and ' RUNNING ' in line for line in transaction_text.splitlines())==2*a.tasks
            Path(str(a.output)+'.transactions').write_text(transaction_text)
            result=dict(status='PASS',tasks=a.tasks,objects=a.objects,bytes_per_task=a.bytes,gc_policy=a.gc,probe_tasks=a.tasks,load_seconds=load,execution_seconds=wall,manager_cpu_seconds=cpu,manager_main_thread_cpu_seconds=thread_cpu,immediate_gc_seconds=gc,deferred_gc_seconds=deferred_gc,stats=delta,records=records,execution_start_epoch_us=epoch,cpus=orig[:5],host=os.uname().nodename,python=sys.executable,extension=v.cvine.__file__)
            Path(a.output).write_text(json.dumps(result,indent=2)+'\n')
        finally:
            for w in workers:
                w.terminate()
            for w in workers:
                try:w.wait(timeout=10)
                except subprocess.TimeoutExpired:w.kill();w.wait()
            for log in logs:log.close()
            m.__del__()

def main():
    p=argparse.ArgumentParser();p.add_argument('--tasks',type=int,default=256);p.add_argument('--objects',type=int,default=1);p.add_argument('--bytes',type=int,default=65536);p.add_argument('--gc',default='immediate');p.add_argument('--output');p.add_argument('--campaign',action='store_true');a=p.parse_args()
    if not a.campaign:return trial(a)
    raw=OUT/'probe-raw';raw.mkdir(exist_ok=True)
    configs={(256,o,65536,g) for o in [1,8,64,256] for g in ['immediate','deferred']}
    rng=random.Random(20260907);plan=[]
    for rep in range(3):
        order=sorted(configs);rng.shuffle(order)
        for n,o,b,g in order:plan.append(dict(rep=rep+1,tasks=n,objects=o,bytes=b,gc=g))
    (OUT/'probe-campaign.json').write_text(json.dumps(plan,indent=2))
    for c in plan:
        name=f"r{c['rep']}-t{c['tasks']}-o{c['objects']}-b{c['bytes']}-{c['gc']}"
        path=raw/(name+'.json');cmd=[sys.executable,str(Path(__file__).resolve()),'--tasks',str(c['tasks']),'--objects',str(c['objects']),'--bytes',str(c['bytes']),'--gc',c['gc'],'--output',str(path)]
        print(name,flush=True)
        with (raw/(name+'.log')).open('w') as log:r=subprocess.run(cmd,stdout=log,stderr=log,timeout=420)
        if r.returncode:raise RuntimeError(f'failed {name}; retained log')
    print('CAMPAIGN PASS',flush=True)
if __name__=='__main__':main()
