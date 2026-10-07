#!/usr/bin/env python3
"""Plot explicit dependency-preload controls and retain all cold-start evidence."""
import hashlib,json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
P=Path(__file__).resolve().parents[1];R=P/'results';F=P/'figures'
inputs=[Path(__file__).resolve()]
plt.rcParams.update({'font.size':8,'pdf.fonttype':42,'axes.spines.top':False,'axes.spines.right':False,'savefig.bbox':'tight'})
s=R/'preloaded-campaign-v1/summary.json'
summary=json.loads(s.read_text()) if s.exists() else {'status':'NOT_RUN','runs':[]}
if s.exists():inputs.append(s)
if summary.get('scientific_mismatches'):raise ValueError('preload campaign content mismatch')
variants=['dv-fixed1','dv-elastic','tv-fixed1','tv-fixed4'];labels=['DV 1C','DV elastic','TV 1C','TV+ 4C']
fig,axes=plt.subplots(1,4,figsize=(7.2,2.5));effects=[]
for ax,workload in zip(axes,['spectral','histogram','quadrature','phase']):
    medians={}
    for i,v in enumerate(variants):
        rows=[r for r in summary['runs'] if r['status']=='PASS' and r['workload']==workload and r['variant']==v]
        if any(not r.get('library_preload') for r in rows):raise ValueError('preload provenance missing')
        xs=[r['elapsed_seconds'] for r in rows]
        if not xs:continue
        m=np.median(xs);ax.bar(i,m,color=['#4477AA','#CCBB44','#AA3377','#333333'][i],alpha=.7)
        ax.errorbar(i,m,yerr=[[m-min(xs)],[max(xs)-m]],fmt='none',color='black',capsize=3)
        ax.scatter(i+np.linspace(-.12,.12,len(xs)),xs,color='black',s=8,zorder=3)
        if len(xs)==3:medians[v]=float(m)
    ax.set_title(workload.capitalize());ax.set_xticks(range(4),labels,rotation=35,ha='right');ax.set_ylim(bottom=0);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    if len(medians)==4:effects.append(dict(workload=workload,medians=medians,stock_over_elastic=medians['tv-fixed1']/medians['dv-elastic'],extended_over_elastic=medians['tv-fixed4']/medians['dv-elastic'],elastic_over_fixed=medians['dv-elastic']/medians['dv-fixed1']))
axes[0].set_ylabel('Preloaded completion time (s)')
if summary['status']!='PASS':fig.suptitle('Incomplete preload campaign; actual accepted observations shown')
fig.tight_layout();fig.savefig(F/'preloaded.pdf');fig.savefig(F/'preloaded.png',dpi=180);plt.close(fig)
text=[f"The symmetric-preload campaign has {sum(r['status']=='PASS' for r in summary['runs'])} accepted trials of 48 planned (status: {summary['status']})."]
for e in effects:
 m=e['medians'];text.append(f"For {e['workload']}, median times are {m['dv-fixed1']:.3f}, {m['dv-elastic']:.3f}, {m['tv-fixed1']:.3f}, and {m['tv-fixed4']:.3f} s for DataVine fixed $C$, DataVine elastic, stock TaskVine, and extended TaskVine $4C$, respectively. Stock and extended TaskVine divided by DataVine elastic are {e['stock_over_elastic']:.2f} and {e['extended_over_elastic']:.2f}.")
if len(effects)==4 and summary['status']=='PASS':
    ratios=[e['stock_over_elastic'] for e in effects];strong=[e['extended_over_elastic'] for e in effects]
    (R/'preloaded-numbers.tex').write_text('\\newcommand{\\WarmStockRange}{%.2f--%.2f}\n\\newcommand{\\WarmExtendedRange}{%.2f--%.2f}\n' % (min(ratios),max(ratios),min(strong),max(strong)))
    text.append(f'Across these four controls, DataVine elastic is {min(ratios):.2f}--{max(ratios):.2f} times faster than stock TaskVine by median completion time and {min(strong):.2f}--{max(strong):.2f} times faster than the extended fixed 4C control. These results characterize this deployment and graph size; they do not isolate a single architectural cause.')
(R/'preloaded-summary.tex').write_text('\n\n'.join(text)+'\n')
(R/'preloaded-effects.json').write_text(json.dumps(effects,indent=2)+'\n')
(F/'preloaded-provenance.json').write_text(json.dumps({'inputs':{str(p.relative_to(P)):hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs}},indent=2)+'\n')
