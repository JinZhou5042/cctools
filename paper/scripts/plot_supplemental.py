#!/usr/bin/env python3
"""Plot independently scoped memory, source-path and larger-DAG experiments."""
import hashlib,json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
PAPER=Path(__file__).resolve().parents[1];R=PAPER/'results';F=PAPER/'figures'
plt.rcParams.update({'font.size':7,'pdf.fonttype':42,'axes.spines.top':False,'axes.spines.right':False,'savefig.bbox':'tight'})
inputs=[]
def load(path):
    if not path.exists():return None
    inputs.append(path);return json.loads(path.read_text())
def save(fig,name):
    fig.tight_layout();fig.savefig(F/f'{name}.pdf');fig.savefig(F/f'{name}.png',dpi=180);plt.close(fig)
def bars(ax,groups,labels,ylabel):
    for i,xs in enumerate(groups):
        if not xs:continue
        med=np.median(xs);ax.bar(i,med,color=['#4477AA','#66CCEE','#228833','#CCBB44','#AA3377'][i%5],alpha=.75)
        ax.errorbar(i,med,yerr=[[med-min(xs)],[max(xs)-med]],fmt='none',color='black',capsize=3)
        ax.scatter(i+np.linspace(-.12,.12,len(xs)),xs,color='black',s=9,zorder=3)
    ax.set_xticks(range(len(labels)),labels);ax.set_ylabel(ylabel);ax.set_ylim(bottom=0);ax.grid(axis='y',alpha=.15);ax.set_axisbelow(True)
text=[]
pressure=load(R/'pressure-v2.json')
fig,axes=plt.subplots(1,2,figsize=(3.5,2.2))
if pressure and pressure['status']=='PASS':
    rows=[r for r in pressure['results'] if r['case']=='memory']
    groups=[[r for r in rows if r['window_policy']==policy and (policy=='elastic' or r['fixed_window_multiplier']==m)] for policy,m in [('fixed',1),('fixed',2),('fixed',4),('elastic',1)]]
    bars(axes[0],[[r['elapsed_seconds'] for r in g] for g in groups],['1C','2C','4C','Elastic'],'Completion (s)')
    bars(axes[1],[[100*max(t['memory_fraction'] for t in r['window_trace']) for r in g] for g in groups],['1C','2C','4C','Elastic'],'Peak memory (%)')
    peaks=[max(t['memory_fraction'] for t in r['window_trace']) for r in rows if r['window_policy']=='elastic']
    evictions=[r for r in pressure['results'] if r['case']=='eviction']
    text.append(f"The separate resource campaign passed {len(pressure['results'])} trials. In its 32-task memory workload, each task touches 128 MiB; the single worker declares 2 GiB and is restricted to two CPUs. Elastic trials reached at most {100*max(peaks):.2f}\\% sampled worker memory. All {len(evictions)} late-worker eviction trials passed their result, recall-accounting and worker-removal gates. These sampled bounds apply to this declared-memory workload, not arbitrary allocations.")
else:
    for ax in axes:ax.text(.5,.5,'No validated terminal pressure campaign',ha='center',transform=ax.transAxes);ax.set_axis_off()
    text.append('The resource campaign has not produced a passing terminal acceptance record; no successful memory/eviction claim is drawn from it.')
save(fig,'memory-pressure')
routes=load(R/'routes-v1/summary.json')
fig,ax=plt.subplots(figsize=(3.7,2.7))
if routes and routes['status']=='PASS':
    groups=[[r['consumer_seconds'] for r in routes['runs'] if r['mode']==m] for m in ['peer','controller']]
    bars(ax,groups,['Peer source','Controller source'],'Consumer phase completion (s)')
    text.append(f"Six route-isolation trials deliver 128 files of 256 KiB each to a separately admitted consumer pool. Producer workers remain connected but are ineligible execution targets in peer mode; they disconnect after backup admission in controller mode. Median consumer-phase times are {np.median(groups[0]):.3f} s and {np.median(groups[1]):.3f} s, respectively. File lengths and consumer completion counts are checked. This is a source-path experiment with batch CPU shares, not an adaptive routing policy or an exclusive-network bandwidth measurement.")
else:ax.text(.5,.5,'No validated terminal route campaign',ha='center',transform=ax.transAxes);ax.set_axis_off()
save(fig,'routes')
data=load(R/'data-pipeline-v1/summary.json')
fig,axes=plt.subplots(1,2,figsize=(3.5,2.3));ax=axes[0]
variants=['dv-fixed1','dv-elastic','dv-eager','tv-fixed1','tv-fixed4']
if data:
    rows=[r for r in data['runs'] if r['status']=='PASS']
    if data.get('scientific_mismatches'):raise ValueError('hash pipeline scientific mismatch')
    groups=[[r['elapsed_seconds'] for r in rows if r['variant']==v] for v in variants]
    bars(ax,groups,['DV C','DV E','DV P','TV C','TV+4C'],'Completion (s)')
    if data['status']!='PASS':ax.set_title('Incomplete campaign; accepted observations only',fontsize=9)
    if data['status']=='PASS':
        text.append('A larger data pipeline uses 128 lanes and eight stages: 1,024 individual tasks, each producing 1 MiB. Every consumer hashes its parent payload and generates its output with SHAKE256. The runner calls this the no-extra-kernel branch; it is not a zero-work native no-op. Both services use two four-core workers pinned to disjoint CPU sets. All 15 randomized trials pass logical identities and cross-variant output checks.')
        text.append('Median times for DataVine fixed 1C, elastic, eager, stock TaskVine, and extended TaskVine 4C are '+', '.join(f'{np.median(g):.3f}' for g in groups)+' s, respectively. This characterizes data-pipeline behavior at a larger graph size; it does not substitute for a production scientific application.')
else:ax.text(.5,.5,'No completed data-pipeline trials',ha='center',transform=ax.transAxes);ax.set_axis_off()
ax.set_title('Fresh library')
warm=load(R/'preloaded-data-v1/summary.json');ax=axes[1]
if warm and warm['status']=='PASS':
    if warm.get('scientific_mismatches'):raise ValueError('preloaded hash pipeline mismatch')
    groups=[[r['elapsed_seconds'] for r in warm['runs'] if r['status']=='PASS' and r['variant']==v] for v in variants]
    bars(ax,groups,['DV C','DV E','DV P','TV C','TV+4C'],'Completion (s)')
    text.append('The independently allocated preloaded version of the same 1,024-task pipeline also passes all 15 trials. In the same variant order, its median times are '+', '.join(f'{np.median(g):.3f}' for g in groups)+' s. Compare backends within each deployment; the two allocations do not establish a paired causal estimate of preloading.')
else:ax.text(.5,.5,'Preloaded campaign incomplete',ha='center',transform=ax.transAxes)
ax.set_title('Preloaded')
for ax in axes:ax.tick_params(axis='x',labelrotation=45)
save(fig,'data-pipeline')
(R/'supplemental-summary.tex').write_text('\n\n'.join(text)+'\n')
(F/'supplemental-provenance.json').write_text(json.dumps({'inputs':{str(p.relative_to(PAPER)):hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs+[Path(__file__).resolve()]}},indent=2)+'\n')
