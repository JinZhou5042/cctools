#!/usr/bin/env python3
import hashlib,json,statistics,re
from collections import defaultdict
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
P=Path(__file__).resolve().parent

def summarize(sub):
    groups=defaultdict(list)
    for f in sorted((P/sub).glob('*.json')):
        d=json.loads(f.read_text());assert d['status']=='PASS'
        if d.get('mode')=='pressure-only':continue
        r=dict(path=str(f.relative_to(P)),tasks=d['tasks'],objects=d['objects'],bytes=d['bytes_per_task'],gc=d['gc_policy'],wall_s=d['execution_seconds'],cpu_s=d['manager_cpu_seconds'],send_s=d['stats']['time_send']/1e6,receive_s=d['stats']['time_receive']/1e6,status_s=d['stats']['time_status_msgs']/1e6,internal_s=d['stats']['time_internal']/1e6,scheduling_nested_s=d['stats']['time_scheduling']/1e6,worker_execute_s=d['stats']['time_workers_execute_good']/1e6,gc_s=d['immediate_gc_seconds'],post_gc_s=d['deferred_gc_seconds'],actual_input_bytes=d['stats']['bytes_sent'])
        assert d['stats']['bytes_sent']==(0 if d.get('mode')=='probe-only' else d['tasks']*d['bytes_per_task'])
        if d.get('probe_tasks') and d.get('mode')!='pressure-only':
            probes=sorted((t for t in d['records'] if t['tag'].startswith('probe-')),key=lambda t:t['time_when_commit_start'])
            gaps=[max(0,b['time_when_commit_start']-a['time_when_done'])/1000 for a,b in zip(probes,probes[1:])]
            worker_times={}
            for line in Path(str(f)+'.transactions').read_text().splitlines():
                if line.startswith('#') or ' RETRIEVED ' not in line:continue
                tid=int(line.split()[3]);start=line.find('{"time_worker_start"')
                if start>=0:
                    metrics=json.loads(line[start:]);worker_times[tid]=(metrics['time_worker_start'][0],metrics['time_worker_end'][0])
            ids=[int(t['tag'].split('-')[1])*(1 if d.get('mode')=='probe-only' else 2)+1 for t in probes]
            wt=[worker_times[i] for i in ids]
            worker_gaps=[max(0,b[0]-a[1])*1000 for a,b in zip(wt,wt[1:])]
            r.update(probe_worker_gap_p95_ms=float(np.percentile(worker_gaps,95)),probe_worker_gap_total_s=sum(worker_gaps)/1000,probe_worker_span_s=wt[-1][1]-wt[0][0],probe_worker_gap_fraction=sum(worker_gaps)/1000/(wt[-1][1]-wt[0][0]))
            r.update(probe_gap_p95_ms=float(np.percentile(gaps,95)),probe_gap_median_ms=statistics.median(gaps),probe_gap_total_s=sum(gaps)/1000,probe_span_s=(probes[-1]['time_when_done']-probes[0]['time_when_commit_start'])/1e6,probe_execution_s=sum(t['time_workers_execute_last'] for t in probes)/1e6,main_thread_cpu_s=d['manager_main_thread_cpu_seconds'])
        groups[(r['tasks'],r['objects'],r['bytes'],r['gc'])].append(r)
    result=[]
    for key,rs in sorted(groups.items()):
        nums=[k for k,v in rs[0].items() if isinstance(v,(int,float))]
        result.append(dict(config=dict(tasks=key[0],objects=key[1],bytes=key[2],gc=key[3]),n=len(rs),median={k:statistics.median(r[k] for r in rs) for k in nums},minimum={k:min(r[k] for r in rs) for k in nums},maximum={k:max(r[k] for r in rs) for k in nums},runs=rs))
    return result

def main():
    base=summarize('raw');probe=summarize('probe-raw')
    separate=summarize('separate-raw')
    sharedcpu=summarize('separate-sharedcpu-raw')
    data=dict(baseline=base,probe=probe,separate=separate,sharedcpu=sharedcpu)
    (P/'summary.json').write_text(json.dumps(data,indent=2)+'\n')
    plt.rcParams.update({'font.size':9,'axes.spines.top':False,'axes.spines.right':False,'pdf.fonttype':42})
    fig,axs=plt.subplots(1,3,figsize=(11,3.4))
    for gc,color in [('immediate','#b44b45'),('deferred','#2878a5')]:
        rs=[r for r in base if r['config']['tasks']==256 and r['config']['bytes']==65536 and r['config']['gc']==gc]
        for ax,k,label in [(axs[0],'cpu_s','Manager process CPU (s)'),(axs[1],'wall_s','Execution-phase elapsed (s)')]:
            xx=[r['config']['objects'] for r in rs];yy=[r['median'][k] for r in rs]
            ax.errorbar(xx,yy,yerr=[[r['median'][k]-r['minimum'][k] for r in rs],[r['maximum'][k]-r['median'][k] for r in rs]],marker='o',color=color,label=gc+' undeclare',capsize=3)
            ax.set(xscale='log',xlabel='Objects per task; fixed 64 KiB/task',ylabel=label);ax.grid(alpha=.2)
        rs=[r for r in probe if r['config']['gc']==gc]
        if rs:
            k='probe_worker_gap_p95_ms';axs[2].errorbar([r['config']['objects'] for r in rs],[r['median'][k] for r in rs],yerr=[[r['median'][k]-r['minimum'][k] for r in rs],[r['maximum'][k]-r['median'][k] for r in rs]],color=color,marker='o',label=gc+' undeclare',capsize=3)
    isolated=[r for r in separate if r['config']['gc']=='immediate']
    if isolated:
        axs[2].plot([r['config']['objects'] for r in isolated],[r['median']['probe_worker_gap_p95_ms'] for r in isolated],color='black',ls='--',marker='s',label='Separate managers (+1 CPU)')
        axs[2].legend(fontsize=7)
    axs[0].legend(fontsize=8);axs[2].set(xscale='log',xlabel='Pressure-task objects; probe has no inputs',ylabel='Dedicated probe worker gap p95 (ms)');axs[2].grid(alpha=.2)
    fig.suptitle('Fixed input bytes, more objects: manager cost and isolated scheduling interference')
    fig.tight_layout();fig.savefig(P/'motivation-pilot.pdf');fig.savefig(P/'motivation-pilot.png',dpi=180);plt.close(fig)
    lines=['# Measured configurations','', '|Campaign|Tasks|Objects/task|Bytes/task|Undeclare|n|Wall s|Manager CPU s|Probe worker gap p95 ms|','|---|---:|---:|---:|---|---:|---:|---:|---:|']
    for campaign,rs in [('baseline',base),('probe',probe),('separate',separate),('samecpu',sharedcpu)]:
        for r in rs:
            c=r['config'];m=r['median'];lines.append(f"|{campaign}|{c['tasks']}|{c['objects']}|{c['bytes']}|{c['gc']}|{r['n']}|{m['wall_s']:.3f}|{m['cpu_s']:.3f}|{m.get('probe_worker_gap_p95_ms',float('nan')):.3f}|")
    (P/'RESULTS.md').write_text('\n'.join(lines)+'\n')
    print('\n'.join(lines))
if __name__=='__main__':main()
