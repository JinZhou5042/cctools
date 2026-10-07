#!/usr/bin/env python3
import collections, hashlib, json, statistics
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
P=Path(__file__).resolve().parent
ROOT=P.parents[1]

def aggregate(groups):
    out=[]
    for key,rs in sorted(groups.items()):
        metrics=[k for k,v in rs[0].items() if isinstance(v,(int,float))]
        out.append(dict(config=key,n=len(rs),median={k:statistics.median(r[k] for r in rs) for k in metrics},minimum={k:min(r[k] for r in rs) for k in metrics},maximum={k:max(r[k] for r in rs) for k in metrics},runs=rs))
    return out

def native():
    groups=collections.defaultdict(list)
    for f in sorted((P/'raw').glob('r*-d*-*.json')):
        if f.name.endswith('.invocation.json'):continue
        d=json.loads(f.read_text());inv=json.loads(f.with_suffix('.invocation.json').read_text())
        policy=inv['config']['policy'];threads=inv['data_threads'];s=d['service_metrics']
        assert d['status']=='PASS' and d['tasks']==256 and d['worker_inventory']==4
        assert d['physical_tasks']==dict(submissions=256,completions=256)
        assert d['workflow']['state']=='completed'
        assert len(inv['cpus'])==12 and inv['rpc_threads']==1
        assert d['workflow_recovery']=='none' and d['idata_backup']=='worker-local'
        expected=256 if policy=='durable' else 0
        assert d['durable_file_count']==expected and d['durable_file_bytes']==expected*1048576
        assert s['agent_persistence_jobs']==expected and s['agent_persistence_bytes']==expected*1048576
        assert s['agent_persistence_failures']==s['agent_persistence_retries']==0
        r=dict(path=str(f.relative_to(P)),wall_s=d['execution_seconds'],service_wall_s=d['service_execution_seconds'],service_cpu_s=d['system_metrics']['service']['cpu_seconds'],manager_owner_s=s['manager_owner_execute_seconds'],scheduler_delay_s=s['scheduler_delay_seconds'],fsync_sum_s=s['agent_persistence_fsync_seconds'],queue_wait_sum_s=s['agent_persistence_queue_wait_seconds'],peak_queue=s['agent_persistence_peak_queue_depth'],throughput=256/d['execution_seconds'])
        groups[(policy,threads)].append(r)
    return aggregate(groups)

def mixed():
    groups=collections.defaultdict(list)
    for f in sorted((P/'mixed-raw').glob('*.json')):
        d=json.loads(f.read_text());name=f.stem.split('-');variant=name[1]
        assert Path(d['loaded_extension_path'])==P/'build'/variant/'_cvine.so'
        assert d['status']=='PASS' and d['tasks']==256 and d['probe_tasks']==256
        s=d['stats'];assert s['tasks_done']==s['tasks_submitted']==512
        assert s['tasks_failed']==s['workers_removed']==0 and s['bytes_sent']==256*65536
        assert len(d['records'])==len({r['tag'] for r in d['records']})==512
        running=collections.Counter();done=collections.Counter();workers={};metrics={}
        for line in Path(str(f)+'.transactions').read_text().splitlines():
            if line.startswith('#'):continue
            parts=line.split()
            if len(parts)<5 or parts[2]!='TASK':continue
            tid=int(parts[3])
            if parts[4]=='RUNNING':running[tid]+=1;workers[tid]=parts[5]
            elif parts[4]=='DONE':
                assert parts[5:]==['SUCCESS','0'];done[tid]+=1
            elif parts[4]=='RETRIEVED':
                assert parts[5]=='SUCCESS'
                metrics[tid]=json.loads(line[line.index('{"time_worker_start"'):])
        assert len(running)==len(done)==512 and set(running.values())==set(done.values())=={1}
        probes=[];pressure=[];pw=set();dw=set()
        for r in d['records']:
            probe=r['tag'].startswith('probe-');index=int(r['tag'].split('-')[-1]);tid=index*2+(1 if probe else 2)
            mt=metrics[tid]
            assert abs(mt['time_commit_start'][0]*1e6-r['time_when_commit_start'])<2
            assert abs(mt['time_commit_end'][0]*1e6-r['time_when_commit_end'])<2
            row=(mt['time_worker_start'][0],mt['time_worker_end'][0],r['time_when_done']/1e6)
            (probes if probe else pressure).append(row)
            (pw if probe else dw).add(workers[tid])
        assert len(pw)==1 and not pw.intersection(dw)
        probes.sort();pressure.sort()
        gaps=[(b[0]-a[1])*1000 for a,b in zip(probes,probes[1:])]
        assert min(gaps)>=0
        start=d['execution_start_epoch_us']/1e6
        r=dict(path=str(f.relative_to(P)),wall_s=d['execution_seconds'],cpu_s=d['manager_cpu_seconds'],thread_cpu_s=d['manager_main_thread_cpu_seconds'],gap_p95_ms=float(np.percentile(gaps,95)),probe_span_s=probes[-1][1]-probes[0][0],probe_execute_s=sum(b-a for a,b,c in probes),probe_gap_s=sum(gaps)/1000,pressure_completion_s=max(t[2] for t in pressure)-start,pressure_worker_execute_s=sum(b-a for a,b,c in pressure),gc_s=d['immediate_gc_seconds'],post_gc_s=d['deferred_gc_seconds'])
        groups[(variant,d['gc_policy'],d['objects'])].append(r)
    return aggregate(groups)

def main():
    n=native();m=mixed()
    assert len(n)==8 and all(r['n']==3 for r in n),'native campaign incomplete'
    assert len(m)==8 and all(r['n']==3 for r in m),'mixed campaign incomplete'
    provenance=json.loads((P/'provenance.json').read_text())
    for f,h in provenance['hashes'].items():assert hashlib.sha256((ROOT/f).read_bytes()).hexdigest()==h,f
    data=dict(status='PASS',native=n,mixed=m,workflows=48,physical_tasks=18432)
    (P/'summary.json').write_text(json.dumps(data,indent=2)+'\n')
    lines=['# Accepted repeated measurements','','All values are medians of three runs. Ranges and raw paths are in summary.json.','','|Data policy|Threads|Workflow s|Service CPU s|Manager owner scope s|Summed fsync s|','|---|---:|---:|---:|---:|---:|']
    for r in n:
        c=r['config'];v=r['median'];lines.append(f"|{c[0]}|{c[1]}|{v['wall_s']:.3f}|{v['service_cpu_s']:.3f}|{v['manager_owner_s']:.6f}|{v['fsync_sum_s']:.3f}|")
    lines+=['','|Coupled build|Undeclare|Objects/task|Whole workflow s|Data completion s|Probe span s|Probe gap p95 ms|Manager CPU s|','|---|---|---:|---:|---:|---:|---:|---:|']
    for r in m:
        c=r['config'];v=r['median'];lines.append(f"|{c[0]}|{c[1]}|{c[2]}|{v['wall_s']:.3f}|{v['pressure_completion_s']:.3f}|{v['probe_span_s']:.3f}|{v['gap_p95_ms']:.3f}|{v['cpu_s']:.3f}|")
    (P/'RESULTS.md').write_text('\n'.join(lines)+'\n');print('\n'.join(lines))
    plt.rcParams.update({'font.size':9,'axes.spines.top':False,'axes.spines.right':False,'pdf.fonttype':42})
    fig,axs=plt.subplots(1,3,figsize=(12,3.7))
    def plot(ax,rs,key,xs,label,color):
        ax.errorbar(xs,[r['median'][key] for r in rs],yerr=[[r['median'][key]-r['minimum'][key] for r in rs],[r['maximum'][key]-r['median'][key] for r in rs]],marker='o',label=label,color=color,capsize=3)
    for v,color in [('baseline','#b33c3c'),('candidate','#2c71a0')]:
        rs=[r for r in m if r['config'][0]==v and r['config'][1]=='immediate']
        plot(axs[0],rs,'gap_p95_ms',[r['config'][2] for r in rs],v,color)
    axs[0].set(xscale='log',yscale='log',xlabel='Objects/task; fixed 64 KiB/task',ylabel='Probe worker gap p95 (ms)',title='(a) Coupled scheduler: reject full workers early')
    axs[0].set_xticks([1,256],['1','256'])
    for policy,color in [('durable','#b33c3c'),('local','#2c71a0')]:
        rs=[r for r in n if r['config'][0]==policy]
        plot(axs[1],rs,'wall_s',[r['config'][1] for r in rs],policy,color)
        plot(axs[2],rs,'manager_owner_s',[r['config'][1] for r in rs],policy,color)
    axs[1].set(xlabel='Data persistence threads',ylabel='Whole workflow elapsed (s)',title='(b) Real DataVine: fixed 12-CPU budget')
    axs[2].set(xlabel='Data persistence threads',ylabel='Manager owner execute scope (s)',title='(c) Manager owner scope stays small')
    for ax in axs[1:]:ax.set_xticks([1,2,4,8])
    for ax in axs:ax.grid(alpha=.2);ax.legend(fontsize=8)
    fig.text(.5,.02,'Median and min–max, n=3. Panels (a) and (b,c) are different workloads, not a direct architecture speedup.',ha='center',fontsize=8)
    fig.tight_layout(rect=[0,.05,1,1]);fig.savefig(P/'architecture-evidence.png',dpi=180);fig.savefig(P/'architecture-evidence.pdf')

if __name__=='__main__':main()
