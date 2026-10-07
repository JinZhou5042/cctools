#!/usr/bin/env python3
"""Render the retained million-task throughput summary."""

import json
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parent
RAW = ROOT / "raw/results"


def load(name):
    return json.loads((RAW / name).read_text())


baseline = load("baseline-streaming-local-8x16.json")
fastpath = load("fastpath-streaming-local-8x16.json")
local = [load(f"datavine-eventfast-final-1m-r{index}.json") for index in (1, 2)]
condor = [load(f"condor-staged-none-8x16-r{index}.json") for index in (1, 2)]

milestones = [baseline, fastpath, *local, *condor]
labels = ["baseline\nlocal 8x16", "fast path\nlocal 8x16",
          "final local\n2x24 r1", "final local\n2x24 r2",
          "Condor\n8x16 r1", "Condor\n8x16 r2"]
service = [item["service_runtime_tasks_per_second"] / 1000 for item in milestones]
end_to_end = [item["tasks_per_second"] / 1000 for item in milestones]

workers = [1, 2, 4, 8, 12, 16]
worker_rates = [
    load(f"datavine-workers-300k-w{count}.json")[
        "service_runtime_tasks_per_second"
    ] / 1000
    for count in workers
]

plt.style.use("seaborn-v0_8-whitegrid")
figure, axes = plt.subplots(1, 2, figsize=(13.5, 5.2), constrained_layout=True)

x = range(len(labels))
width = 0.36
axes[0].bar([value - width / 2 for value in x], service, width,
            label="Service runtime", color="#176B87")
axes[0].bar([value + width / 2 for value in x], end_to_end, width,
            label="Workflow E2E", color="#64CCC5")
axes[0].set_xticks(list(x), labels)
axes[0].set_ylabel("Throughput (thousand tasks/s)")
axes[0].set_title("One-million-task exact runs")
axes[0].legend(frameon=False)
axes[0].set_ylim(0, 32)
for index, value in enumerate(service):
    axes[0].text(index - width / 2, value + 0.35, f"{value:.1f}",
                 ha="center", va="bottom", fontsize=8)

axes[1].plot(workers, worker_rates, marker="o", linewidth=2.2,
             color="#B2533E")
axes[1].set_xticks(workers)
axes[1].set_xlabel("Local Workers (16 advertised slots each)")
axes[1].set_ylabel("Service throughput (thousand tasks/s)")
axes[1].set_title("300K local Worker sweep (32-core cgroup)")
axes[1].set_ylim(10, 25)
for worker, value in zip(workers, worker_rates):
    axes[1].annotate(f"{value:.1f}", (worker, value),
                     textcoords="offset points", xytext=(0, 7), ha="center")

figure.suptitle("DataVine native per-task throughput, 2026-09-03", fontsize=15)
figure.savefig(ROOT / "throughput.png", dpi=180)
