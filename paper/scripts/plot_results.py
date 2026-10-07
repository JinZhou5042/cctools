#!/usr/bin/env python3
"""Generate every paper figure and numerical statement from retained evidence."""
import csv,hashlib,json,statistics
from collections import defaultdict
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import numpy as np
PAPER=Path(__file__).resolve().parents[1]
RESULTS=PAPER/'results';FIGURES=PAPER/'figures'
CAMPAIGNS=['compute-campaign-v4','distributed-campaign-v4']
LABELS={'dv-fixed1':'DV 1C','dv-fixed2':'DV 2C','dv-fixed4':'DV 4C','dv-elastic':'DV elastic','dv-eager':'DV eager','tv-fixed1':'TV 1C','tv-fixed2':'TV+ 2C','tv-fixed4':'TV+ 4C'}
COLORS=['#4477AA','#66CCEE','#228833','#CCBB44','#EE6677','#AA3377','#BBBBBB','#333333']
plt.rcParams.update({'font.size':8,'axes.spines.top':False,'axes.spines.right':False,'pdf.fonttype':42,'ps.fonttype':42,'savefig.bbox':'tight'})

def read(path):return json.loads(path.read_text())
def save(fig,name):
    fig.savefig(FIGURES/f'{name}.pdf');fig.savefig(FIGURES/f'{name}.png',dpi=180);plt.close(fig)
def good(summary):
    if summary.get('scientific_mismatches'):raise ValueError('cross-backend scientific mismatch in campaign')
    return [r for r in summary.get('runs',[]) if r['status']=='PASS']
def values(rows,workload,variant,workers=None):
    return [r['elapsed_seconds'] for r in rows if r['workload']==workload and r['variant']==variant and (workers is None or r['workers']==workers)]
def stat(xs):return float(np.median(xs)),min(xs),max(xs)

def architecture():
    fig,ax=plt.subplots(figsize=(3.5,2.7));ax.set(xlim=(0,10),ylim=(0,6));ax.axis('off')
    def box(x,y,w,h,label,color):
        ax.add_patch(FancyBboxPatch((x,y),w,h,boxstyle='round,pad=.10',facecolor=color,edgecolor='#334455',linewidth=1))
        ax.text(x+w/2,y+h/2,label,ha='center',va='center',fontsize=7.5)
    box(.2,4.6,3.5,1,'Logical scheduler\nsingle reactor / tasks','#DCE8F3')
    box(6.0,4.6,3.6,1,'Data controller\nRPC + persistence pools','#E3EEDB')
    box(.2,2.6,3.5,1,'Descriptor queue\ncapacity Q','#EDEDED')
    box(5.7,2.6,3.9,1,'Worker agent\nresolve / prepare','#E3EEDB')
    box(2.5,.3,4.8,1.1,'Execution library\ninput ready; W','#F7E7C6')
    def arrow(a,b,label='',xy=None,style='-'):
        ax.annotate('',xy=b,xytext=a,arrowprops=dict(arrowstyle='->',lw=1.3,linestyle=style,color='#334455'))
        if label:ax.text(*xy,label,ha='center',fontsize=6.5)
    arrow((1.9,4.5),(1.9,3.7),'dispatch',(2.5,4.0))
    arrow((3.8,3.1),(5.5,3.1),'defer until\nopportunity',(4.65,3.65))
    arrow((7.7,3.7),(7.7,4.5),'resolve',(8.7,4.0))
    arrow((6.8,2.5),(5.8,1.5),'ready',(6.8,1.9))
    ax.plot([2.4,0,0],[.8,.8,5.1],ls='--',lw=1,color='#334455')
    arrow((0,5.1),(.15,5.1),'completion',(.9,1.8),'--')
    ax.plot([7.4,9.9,9.9],[.8,.8,5.1],ls='--',lw=1,color='#334455')
    arrow((9.9,5.1),(9.7,5.1),'replica /\nbackup',(9.0,1.3),'--')
    ax.text(5,-.12,'Direct peer data movement occurs between worker agents.',ha='center',fontsize=6.5)
    save(fig,'architecture')

def main():
    FIGURES.mkdir(exist_ok=True)
    summaries={name:read(RESULTS/name/'summary.json') if (RESULTS/name/'summary.json').exists() else {'status':'NOT_RUN','runs':[]} for name in CAMPAIGNS}
    rows=good(summaries[CAMPAIGNS[0]]);remote=good(summaries[CAMPAIGNS[1]])
    architecture()
    fig,axes=plt.subplots(2,2,figsize=(7.2,4.4))
    axes=axes.ravel()
    for ax,workload in zip(axes,['spectral','histogram','quadrature','phase']):
        for i,(variant,label) in enumerate(LABELS.items()):
            xs=values(rows,workload,variant)
            if not xs:continue
            med,lo,hi=stat(xs)
            ax.bar(i,med,color=COLORS[i],alpha=.65,width=.7)
            ax.errorbar(i,med,yerr=[[med-lo],[hi-med]],color='black',capsize=2,lw=.8)
            ax.scatter(i+np.linspace(-.16,.16,len(xs)),xs,c='black',s=7,zorder=3)
        ax.set_title(workload.capitalize());ax.set_xticks(range(8),LABELS.values(),rotation=55,ha='right');ax.set_ylim(bottom=0)
        ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    axes[0].set_ylabel('Completion time (s)')
    if summaries[CAMPAIGNS[0]]['status']!='PASS':fig.suptitle('Campaign in progress / incomplete: observations shown with actual n',fontsize=7.5)
    fig.tight_layout();save(fig,'completion')
    fig,axes=plt.subplots(1,2,figsize=(3.5,2.3))
    for ax,workload in zip(axes,['histogram','phase']):
        for variant,marker,color in zip(['dv-fixed1','dv-elastic','tv-fixed1','tv-fixed4'],['o','s','^','D'],COLORS):
            points=[]
            for w in [2,4]:
                xs=values(remote,workload,variant,w)
                if xs:points.append((w,*stat(xs)))
            if points:
                w,m,lo,hi=map(np.array,zip(*points));ax.errorbar(w,m,yerr=[m-lo,hi-m],label=LABELS[variant],marker=marker,color=color,capsize=3)
        ax.set_title(workload.capitalize());ax.set_xticks([2,4]);ax.set_xlabel('Workers (4 CPUs each)');ax.set_ylim(bottom=0);ax.grid(alpha=.2)
    axes[0].set_ylabel('Completion time (s)');axes[1].legend(fontsize=7)
    fig.tight_layout();save(fig,'worker-pool')
    fig,ax=plt.subplots(figsize=(3.5,1.9));parts=['Before prepare','Preparation','Ready to execute','Execution']
    intervals=[('received_us','prepare_start_us'),('prepare_start_us','prepare_ready_us'),('prepare_ready_us','execute_start_us'),('execute_start_us','complete_us')]
    for i,policy in enumerate(['deferred','eager']):
        p=RESULTS/'mechanism-acceptance-v3'/policy/'result.json'
        if not p.exists():continue
        r=read(p);traces=r.get('attempt_trace',[])
        if r['status']!='PASS' or len(traces)!=r['tasks']:raise ValueError('invalid trace fixture')
        left=0
        for j,(a,b) in enumerate(intervals):
            xs=[(t[b]-t[a])/1000 for t in traces]
            if min(xs)<0:raise ValueError('reversed timestamp')
            width=float(np.median(xs));ax.barh(i,width,left=left,color=COLORS[j],label=parts[j] if i==0 else None);left+=width
    ax.set_yticks([0,1],['Deferred','Eager']);ax.set_xlabel('Median component duration (ms)');ax.legend(fontsize=6,ncol=2,loc='upper right');fig.tight_layout();save(fig,'preparation')
    extra_paths=[RESULTS/name/'summary.json' for name in ['preloaded-campaign-v1','data-pipeline-v1','preloaded-data-v1']]
    extra_count=sum(len(good(read(p))) for p in extra_paths if p.exists())
    regression=read(RESULTS/'regression-recall-fix.json')
    (RESULTS/'numbers.tex').write_text(f"\\newcommand{{\\CampaignPassed}}{{{len(rows)+len(remote)+extra_count}}}\n\\newcommand{{\\RegressionPassed}}{{{regression['passed_count']}}}\n")
    lines=[];table=[]
    for name,rs in zip(CAMPAIGNS,[rows,remote]):
        s=summaries[name]
        lines.append(f"The {'colocated' if name==CAMPAIGNS[0] else 'worker-pool'} campaign currently has {len(rs)} accepted trials out of {s.get('planned',0)} planned (status: {s['status'].replace('_',' ')}).")
    for workload in ['spectral','histogram','quadrature','phase']:
        medians={v:stat(values(rows,workload,v))[0] for v in LABELS if len(values(rows,workload,v))==3}
        if len(medians)!=len(LABELS):continue
        dv=medians['dv-elastic'];tv=medians['tv-fixed1'];besttv=min(medians[v] for v in ['tv-fixed1','tv-fixed2','tv-fixed4']);bestdv=min(medians[v] for v in ['dv-fixed1','dv-fixed2','dv-fixed4'])
        lines.append(f"For {workload}, median completion times are {dv:.3f} s for DataVine elastic and {tv:.3f} s for stock TaskVine. Their ratio (TaskVine/DataVine) is {tv/dv:.2f}; comparing the best tested TaskVine fixed setting with DataVine elastic gives {besttv/dv:.2f}. DataVine elastic takes {dv/bestdv:.2f} times the best tested DataVine fixed-window time.")
        table.append(dict(workload=workload,elastic_seconds=dv,stock_taskvine_seconds=tv,stock_ratio=tv/dv,best_taskvine_ratio=besttv/dv,elastic_over_best_dv=dv/bestdv))
    lines.append('Ratios above one in the TaskVine/DataVine comparison favor DataVine; ratios below one favor TaskVine. These are ratios of medians over three randomized repetition blocks, not population confidence bounds.')
    (RESULTS/'evaluation-summary.tex').write_text('\n\n'.join(lines)+'\n')
    (RESULTS/'effect-sizes.json').write_text(json.dumps(table,indent=2)+'\n')
    churn=RESULTS/'worker-churn-v3/summary.json'
    recovery='The scheduled worker-removal experiment has not yet produced a validated terminal summary. No quantitative recovery claim is made from it.'
    if churn.exists():
        c=read(churn)
        if c.get('status')=='PASS':
            j=c['correctness']['journal']
            recovery=f"A separate four-worker experiment removed three connected, factory-owned worker jobs during a 1,024-task CPU workflow. All 1,024 logical identities and 16 sink values passed. The journal records {j['tasks_resubmitted']} resubmitted tasks, {j['task_events']['submitted']} physical submissions, and no repeated logical completion. This is worker-loss evidence; the recorded in-place recovery epoch count is zero, so it must not be called three controller recoveries."
        else:recovery='The worker-removal experiment did not pass its acceptance gate. Its logs are retained, and it is excluded from successful recovery claims.'
    (RESULTS/'recovery-summary.tex').write_text(recovery+'\n')
    inputs=[RESULTS/name/'summary.json' for name in CAMPAIGNS]+extra_paths+[RESULTS/'regression-recall-fix.json',churn,RESULTS/'mechanism-acceptance-v3/deferred/result.json',RESULTS/'mechanism-acceptance-v3/eager/result.json',Path(__file__).resolve()]
    (FIGURES/'provenance.json').write_text(json.dumps(dict(inputs={str(p.relative_to(PAPER)):hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs if p.exists()},matplotlib=matplotlib.__version__,numpy=np.__version__,aggregation='median and full observed range; individual points; no imputed failures'),indent=2)+'\n')
    with (RESULTS/'accepted-trials.csv').open('w') as stream:
        keys=['campaign','workload','variant','workers','cores','repetition','elapsed_seconds','tasks','hosts']
        writer=csv.DictWriter(stream,fieldnames=keys);writer.writeheader()
        for campaign,rs in zip(CAMPAIGNS,[rows,remote]):
            for r in rs:writer.writerow({k:campaign if k=='campaign' else r.get(k) for k in keys})
    print(json.dumps(dict(accepted=len(rows)+len(remote),campaigns={k:v['status'] for k,v in summaries.items()})))
if __name__=='__main__':main()
