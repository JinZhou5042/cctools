#!/usr/bin/env python3
"""Audit complete new campaigns and generate paired summaries and paper figures."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics

PAPER=Path(__file__).resolve().parents[1]
ROOT=PAPER.parent
CAMPAIGNS={'controls':'upgrade-controls-v4','application':'upgrade-application-v4',
           'attribution':'upgrade-attribution-v2','scaling':'upgrade-node-scaling-v3'}
EXPECTED={'controls':75,'application':20,'attribution':60,'scaling':120}


def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    p=argparse.ArgumentParser();p.add_argument('--preview',action='store_true')
    p.add_argument('--check-only',action='store_true');a=p.parse_args()
    checks=[];groups={};inputs={};rows=[];excluded=[]
    for kind,campaign in CAMPAIGNS.items():
        directory=PAPER/'results'/campaign
        path=directory/'summary.json'
        if not path.exists():
            checks.append(dict(check=campaign,passed=False,detail='missing summary'));continue
        data=json.loads(path.read_text())
        inputs[str(path.relative_to(PAPER))]=digest(path)
        valid=(data['status']=='PASS' and data['planned']==EXPECTED[kind]
               and data['passed']==EXPECTED[kind] and not data['scientific_mismatches'])
        checks.append(dict(check=campaign,passed=valid,detail=f"{data['passed']}/{EXPECTED[kind]}"))
        plan=json.loads((directory/'plan.json').read_text())
        def measured_input(key,value):
            archived=PAPER/'provenance/measured-upgrade-v1'/key
            return any(path.exists() and digest(path)==value for path in [ROOT/key,archived])
        checks.append(dict(check=campaign+' measured inputs',passed=all(
            measured_input(key,value) for key,value in plan['fingerprints'].items())))
        inputs[str((directory/'plan.json').relative_to(PAPER))]=digest(directory/'plan.json')
        if 'continuation' in plan:
            prior=PAPER/plan['continuation']['source']/'summary.json'
            checks.append(dict(check=campaign+' retained predecessor',passed=
                digest(prior)==plan['continuation']['summary_sha256']))
            inputs[str(prior.relative_to(PAPER))]=digest(prior)
        for row in data['runs']:
            if row['status']!='PASS':continue
            if kind=='application' and row['variant']=='tv-group':
                excluded.append(dict(path=row['path'],reason='All grouping trials with the incompatible branch start-recall policy are diagnostic, regardless of observed outcome.'))
                continue
            path=Path(row['path'])
            result=json.loads((path/'result.json').read_text())
            # Campaign-added keys do not belong in the per-trial record.
            checks.append(dict(check=path.name+' retained result',passed=result=={
                k:v for k,v in row.items() if k not in ['path','configuration']}))
            if row['backend']=='datavine' and row['research']['preloaded']:
                proof=path/'deployment-proof.json'
                checks.append(dict(check=path.name+' actual deployment',passed=proof.exists() and
                    json.loads(proof.read_text())['status']=='PASS'))
            valid=row['logical_identities']==row['tasks']
            if row['backend']=='datavine':
                valid &= row['physical_counts']==dict(submissions=row['tasks'],completions=row['tasks'])
            if row['backend']=='taskvine':
                valid &= row['manager_stats']['tasks_done']==row['tasks']
                valid &= row['manager_stats']['tasks_failed']==0
            valid &= len(row['research']['workers_usage'])==row['workers']
            capacity=[(w['host'],tuple(w['cpuset'])) for w in row['research']['workers_usage']]
            valid &= all(len(cpus)==row['cores'] for _,cpus in capacity)
            allocated={(host,cpu) for host,cpus in capacity for cpu in cpus}
            valid &= len(allocated)==row['workers']*row['cores']
            tasks=json.loads((path/'tasks.json').read_text())
            valid &= len(tasks)==row['tasks'] and all(
                (t['host'],tuple(t['cpuset'])) in capacity for t in tasks.values())
            if kind=='scaling':valid &= len(row['hosts'])==row['workers']
            checks.append(dict(check=path.name+' identity/capacity',passed=bool(valid)))
            config=row['configuration']
            key=(kind,row['workload'],config['bytes'],row['workers'],row['variant'])
            groups.setdefault(key,[]).append(row)
            rows.append((kind,row))
    summaries=[]
    for key,values in sorted(groups.items()):
        kind,workload,size,workers,variant=key
        times=[row['elapsed_seconds'] for row in values]
        control=groups.get((kind,workload,size,workers,'tv-stock'),[])
        baseline={r['repetition']:r['elapsed_seconds'] for r in control}
        paired=[baseline[r['repetition']]/r['elapsed_seconds'] for r in values if r['repetition'] in baseline]
        entry=dict(campaign=kind,workload=workload,bytes=size,workers=workers,variant=variant,
            n=len(values),median=statistics.median(times),minimum=min(times),maximum=max(times),
            paired_vs_stock=paired,paired_wins=sum(v>1 for v in paired),
            body_cpu_median=statistics.median(r['task_body_cpu_seconds'] for r in values),
            worker_lifetime_cpu_median=statistics.median(sum(w['lifetime_cpu_seconds'] for w in
                r['research']['workers_usage']) for r in values))
        summaries.append(entry)
        checks.append(dict(check='five blocks '+str(key),passed=len(values)==5 and
            {r['repetition'] for r in values}==set(range(1,6))))
    # Every local comparison block must use the same actual CPU allocation.
    blocks={}
    for kind,row in rows:
        key=(kind,row['repetition'],row['workload'],row['configuration']['bytes'],row['workers'])
        allocation=sorted((w['host'],tuple(w['cpuset'])) for w in row['research']['workers_usage'])
        blocks.setdefault(key,[]).append(allocation)
    checks.append(dict(check='paired allocation equality',passed=all(
        all(a==values[0] for a in values) for values in blocks.values())))
    regression=PAPER/'results/upgrade-regression-final-v3.json'
    checks.append(dict(check='current regression',passed=regression.exists() and
        json.loads(regression.read_text())['status']=='PASS'))
    report=dict(status='PASS' if all(c['passed'] for c in checks) else 'OPEN',
        checks=checks,inputs=inputs,summaries=summaries,accepted_trials=len(rows),excluded=excluded,
        scope='Scientific outputs and declared CPU capacity are validated. Host allocations are not exclusive. Manager bytes are not total network bytes.')
    out=PAPER/'results'/('upgrade-preview.json' if a.preview else 'upgrade-audit.json')
    out.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(dict(status=report['status'],accepted_trials=len(rows),
        failed_checks=[c['check'] for c in checks if not c['passed']][:12])),flush=True)
    if report['status']!='PASS' and not a.preview:return 1
    if a.preview or a.check_only:return 0
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size':9,'pdf.fonttype':42,'ps.fonttype':42})
    labels={'dv-fixed':'DV fixed','dv-elastic':'DV elastic','dv-eager':'DV eager',
            'tv-stock':'TV','tv-4c':'TV 4C',
            'dv-profile':'DV instrumented','tv-profile':'TV instrumented',
            'dv-cold':'DV cold','tv-cold':'TV cold'}
    colors={'dv-fixed':'#2563eb','dv-elastic':'#15803d','dv-eager':'#7c3aed',
            'tv-stock':'#374151','tv-4c':'#be123c'}
    def save(fig,name):
        fig.tight_layout();fig.savefig(PAPER/'figures'/f'{name}.pdf',bbox_inches='tight')
        fig.savefig(PAPER/'figures'/f'{name}.png',dpi=180,bbox_inches='tight');plt.close(fig)
    fig,axes=plt.subplots(1,3,figsize=(7.2,2.65))
    variants=['dv-fixed','dv-elastic','dv-eager','tv-stock','tv-4c']
    for ax,workload in zip(axes,['cpu','phase','noop']):
        for i,v in enumerate(variants):
            row=next(s for s in summaries if s['campaign']=='controls' and s['workload']==workload and s['variant']==v)
            ax.bar(i,row['median'],color=colors[v],alpha=.8)
            ax.errorbar(i,row['median'],yerr=[[row['median']-row['minimum']],[row['maximum']-row['median']]],fmt='none',color='black',capsize=2)
        ax.set_xticks(range(len(variants)),[labels[v] for v in variants],rotation=55,ha='right')
        ax.set_title({'cpu':'CPU chains','phase':'CPU / I/O DAG','noop':'1 MiB hash chains'}[workload])
        ax.set_ylabel('Completion time (s)');ax.set_ylim(bottom=0)
    save(fig,'upgrade-controls')
    fig,ax=plt.subplots(figsize=(3.45,2.5))
    variants=['dv-fixed','dv-elastic','tv-stock']
    for i,v in enumerate(variants):
        row=next(s for s in summaries if s['campaign']=='application' and s['variant']==v)
        ax.bar(i,row['median'],color=colors[v],alpha=.8)
        ax.errorbar(i,row['median'],yerr=[[row['median']-row['minimum']],[row['maximum']-row['median']]],fmt='none',color='black',capsize=3)
    ax.set_xticks(range(len(variants)),[labels[v] for v in variants],rotation=20,ha='right')
    ax.set_ylabel('Full ATLAS analysis (s)');save(fig,'upgrade-application')
    fig,axes=plt.subplots(1,2,figsize=(7.2,2.45))
    for ax,workload in zip(axes,['cpu','noop']):
        for v in variants:
            data=sorted([s for s in summaries if s['campaign']=='scaling' and s['workload']==workload and s['variant']==v],key=lambda s:s['workers'])
            ax.errorbar([s['workers'] for s in data],[s['median'] for s in data],
                yerr=[[s['median']-s['minimum'] for s in data],[s['maximum']-s['median'] for s in data]],
                label=labels[v],marker='o',color=colors[v],capsize=2)
        ax.set_xticks([1,2,4,8]);ax.set_xlabel('Distinct physical hosts (8 cores each)')
        ax.set_ylabel('Completion time (s)');ax.set_title('CPU chains' if workload=='cpu' else '1 MiB hash chains')
        ax.set_ylim(bottom=0)
    axes[1].legend(fontsize=8);save(fig,'upgrade-scaling')
    lines=['# Verified research upgrade','',f'{len(rows)} accepted trials; five randomized blocks per configuration.','',
           '| Campaign | Workload | Hosts/workers | Variant | Median seconds | Range seconds | Paired wins vs TV |',
           '|---|---|---:|---|---:|---:|---:|']
    for s in summaries:
        lines.append(f"| {s['campaign']} | {s['workload']} ({s['bytes']} B) | {s['workers']} | {s['variant']} | {s['median']:.3f} | {s['minimum']:.3f}--{s['maximum']:.3f} | {s['paired_wins']}/{len(s['paired_vs_stock'])} |")
    (PAPER/'results/upgrade-results.md').write_text('\n'.join(lines)+'\n')
    def select(kind,workload,variant,workers=2):
        return next(s for s in summaries if s['campaign']==kind and s['workload']==workload and s['variant']==variant and s['workers']==workers)
    app={v:select('application','atlas',v) for v in ['dv-fixed','dv-elastic','tv-stock']}
    macros={'UpgradeTrials':str(len(rows)),
            'UpgradeRegressionPassed':str(json.loads(regression.read_text())['passed_count']),
            'UpgradeAtlasFixedMedian':f"{app['dv-fixed']['median']:.2f}",
            'UpgradeAtlasElasticMedian':f"{app['dv-elastic']['median']:.2f}",
            'UpgradeAtlasTVMedian':f"{app['tv-stock']['median']:.2f}",
            'UpgradeAtlasElasticSpeedup':f"{statistics.median(app['dv-elastic']['paired_vs_stock']):.2f}",
            'UpgradeAtlasFixedSpeedup':f"{statistics.median(app['dv-fixed']['paired_vs_stock']):.2f}"}
    (PAPER/'results/upgrade-numbers.tex').write_text(''.join(
        '\\newcommand{\\'+key+'}{'+value+'}\n' for key,value in macros.items()))
    for kind,variants,name in [('controls',['dv-fixed','dv-elastic','dv-eager','tv-stock','tv-4c'],'upgrade-controls-table.tex')]:
        lines=[]
        for work,label in [('cpu','CPU'),('phase','CPU/I/O'),('noop','Hash')]:
            lines.append(label+' & '+' & '.join(f"{select(kind,work,v)['median']:.2f}" for v in variants)+r' \\')
        header=' & DV fixed & Elastic & Eager & TV & TV 4C'
        (PAPER/'results'/name).write_text('\\begin{tabular}{@{}lrrrrr@{}}\n\\toprule\n'+
            header+r' \\'+'\n\\midrule\n'+'\n'.join(lines)+'\n\\bottomrule\n\\end{tabular}\n')
    cpu_one=select('scaling','cpu','dv-fixed',1)
    cpu_eight=select('scaling','cpu','dv-fixed',8)
    one={r['repetition']:r['elapsed_seconds'] for r in groups[('scaling','cpu',1024,1,'dv-fixed')]}
    speed=statistics.median(one[r['repetition']]/r['elapsed_seconds'] for r in groups[('scaling','cpu',1024,8,'dv-fixed')])
    tv_one=select('scaling','cpu','tv-stock',1);tv_eight=select('scaling','cpu','tv-stock',8)
    dv_data=[select('scaling','noop','dv-fixed',n)['median'] for n in [1,2,4,8]]
    tv_data=[select('scaling','noop','tv-stock',n)['median'] for n in [1,2,4,8]]
    scaling=(f"For CPU chains, fixed DataVine changes from {cpu_one['median']:.2f} s at one host to "
        f"{cpu_eight['median']:.2f} s at eight hosts, a median paired one-to-eight speedup of {speed:.2f}. "
        f"TaskVine changes from {tv_one['median']:.2f} to {tv_eight['median']:.2f} s. "
        "For 1-MiB hash chains, fixed DataVine medians at one, two, four, and eight hosts are "
        +', '.join(f'{v:.2f}' for v in dv_data)+" s; TaskVine medians are "
        +', '.join(f'{v:.2f}' for v in tv_data)+" s. These data-heavy graphs do not obtain the CPU case's scaling benefit.\n")
    (PAPER/'results/upgrade-scaling-summary.tex').write_text(scaling)
    trace=json.loads((PAPER/'results/upgrade-traces.json').read_text())
    def ratio(variant,size):
        return next(p['median'] for p in trace['paired'] if p['variant']==variant and p['bytes']==size)
    attribution=("Instrumented/uninstrumented median paired time ratios at 1 KiB and 1 MiB are "
        f"{ratio('dv-profile',1024):.2f} and {ratio('dv-profile',1048576):.2f} for DataVine, and "
        f"{ratio('tv-profile',1024):.2f} and {ratio('tv-profile',1048576):.2f} for TaskVine. "
        "These instrumented runs are excluded from the primary runtime comparisons. "
        f"Cold/preloaded ratios are {ratio('dv-cold',1024):.2f} and {ratio('dv-cold',1048576):.2f} "
        f"for DataVine, versus {ratio('tv-cold',1024):.2f} and {ratio('tv-cold',1048576):.2f} for TaskVine. "
        "Explicit preloading strongly benefits DataVine here but adds setup cost for TaskVine; it is not a universal initialization improvement.\n")
    (PAPER/'results/upgrade-attribution-summary.tex').write_text(attribution)
    cpu_rows=[]
    for kind,work,label in [('controls','cpu','CPU chains'),('controls','noop','Hash chains'),
                            ('controls','phase','CPU/I/O'),('application','atlas','ATLAS')]:
        dv=select(kind,work,'dv-fixed');tv=select(kind,work,'tv-stock')
        values=[dv['body_cpu_median'],tv['body_cpu_median'],
                dv['worker_lifetime_cpu_median'],tv['worker_lifetime_cpu_median']]
        cpu_rows.append(label+' & '+' & '.join(f'{v:.2f}' for v in values)+r' \\')
    (PAPER/'results/upgrade-cpu-table.tex').write_text(
        '\\begin{tabular}{@{}lrrrr@{}}\n\\toprule\n'+
        r' & \multicolumn{2}{c}{Task bodies} & \multicolumn{2}{c}{Worker lifetime} \\'+'\n'+
        r' & DV & TV & DV & TV \\'+'\n\\midrule\n'+
        '\n'.join(cpu_rows)+'\n\\bottomrule\n\\end{tabular}\n')
    artifact_inputs=dict(inputs)
    artifact_inputs[str((PAPER/'results/upgrade-traces.json').relative_to(PAPER))]=digest(PAPER/'results/upgrade-traces.json')
    artifact_inputs[str(regression.relative_to(PAPER))]=digest(regression)
    artifact_inputs[str(Path(__file__).resolve().relative_to(PAPER))]=digest(Path(__file__))
    outputs=[*sorted((PAPER/'figures').glob('upgrade-*.pdf')),*sorted((PAPER/'results').glob('upgrade-*.tex'))]
    (PAPER/'figures/upgrade-provenance.json').write_text(json.dumps(dict(inputs=artifact_inputs,
        outputs={str(p.relative_to(PAPER)):digest(p) for p in outputs}),indent=2)+'\n')
    return 0


if __name__=='__main__':raise SystemExit(main())
