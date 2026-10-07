#!/usr/bin/env python3
"""Render the main evidence figure from audited repeated-run summaries."""
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

P = Path(__file__).resolve().parent
d = json.loads((P / 'summary.json').read_text())
plt.rcParams.update({'font.size': 9, 'axes.spines.top': False,
                     'axes.spines.right': False, 'pdf.fonttype': 42})
fig, axes = plt.subplots(1, 3, figsize=(12, 3.8))

def plot(ax, rows, key, label, color, style='-'):
    rows = sorted(rows, key=lambda r: r['config']['objects'])
    ax.errorbar([r['config']['objects'] for r in rows],
                [r['median'][key] for r in rows],
                yerr=[[r['median'][key] - r['minimum'][key] for r in rows],
                      [r['maximum'][key] - r['median'][key] for r in rows]],
                label=label, color=color, linestyle=style, marker='o', capsize=3)

for gc, color in [('immediate', '#b63a3a'), ('deferred', '#336ca2')]:
    rows = [r for r in d['baseline'] if r['config']['tasks'] == 256
            and r['config']['bytes'] == 65536 and r['config']['gc'] == gc]
    plot(axes[0], rows, 'cpu_s', gc.capitalize() + ' undeclare', color)
    rows = [r for r in d['probe'] if r['config']['gc'] == gc]
    for ax, key in zip(axes[1:], ['probe_worker_gap_p95_ms', 'probe_worker_span_s']):
        plot(ax, rows, key, 'Shared / ' + gc, color)

for campaign, label, color in [('sharedcpu', 'Split / same CPU', '#956c22'),
                               ('separate', 'Split / +1 CPU', '#25794d')]:
    rows = [r for r in d[campaign] if r['config']['gc'] == 'immediate']
    for ax, key in zip(axes[1:], ['probe_worker_gap_p95_ms', 'probe_worker_span_s']):
        plot(ax, rows, key, label, color, '--')

for ax in axes:
    ax.set_xscale('log', base=2)
    ax.set_xticks([1, 8, 64, 256], ['1', '8', '64', '256'])
    ax.set_xlabel('Data objects per pressure task')
    ax.grid(alpha=.2)
axes[0].set_title('(a) Data objects consume manager CPU')
axes[0].set_ylabel('Manager process CPU time (s)')
axes[0].legend(fontsize=8)
axes[1].set_title('(b) Unrelated tasks develop execution gaps')
axes[1].set_ylabel('Probe worker inter-task gap p95 (ms)')
axes[1].set_yscale('log')
axes[1].legend(fontsize=7)
axes[2].set_title('(c) The same probe work takes longer')
axes[2].set_ylabel('Probe worker first-start to last-end (s)')
axes[2].set_ylim(bottom=0)
fig.suptitle('256 data tasks, fixed 64 KiB/task; probes have no inputs and a dedicated worker', fontsize=11)
fig.text(.5, .015, 'Median and min–max of 3 runs. Split controls use independent TaskVine managers; they are not a DataVine ablation.',
         ha='center', fontsize=8)
fig.tight_layout(rect=[0, .045, 1, .96])
fig.savefig(P / 'motivation-evidence.png', dpi=200)
fig.savefig(P / 'motivation-evidence.pdf')
