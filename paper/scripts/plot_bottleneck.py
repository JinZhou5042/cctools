#!/usr/bin/env python3
"""Plot retained scale-up/scale-out evidence for the motivation section."""

import hashlib
import json
import statistics
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


PAPER = Path(__file__).resolve().parents[1]
ROOT = PAPER.parent
OUT = PAPER / "results" / "bottleneck-scale"
FIGURES = PAPER / "figures"
RAW = OUT / "raw"

plt.rcParams.update({
    "font.size": 8,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
    "savefig.bbox": "tight",
})


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def median(rows, key):
    return statistics.median(float(row[key]) for row in rows)


def main():
    FIGURES.mkdir(exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)

    scale_files = sorted(RAW.glob("tasks-*.json"))
    if len(scale_files) != 6:
        raise RuntimeError(f"expected six fixed-topology scale-up runs, got {len(scale_files)}")
    scale_rows = [json.loads(path.read_text()) for path in scale_files]
    by_tasks = {}
    for row in scale_rows:
        by_tasks.setdefault(int(row["tasks"]), []).append(row)
    if sorted(by_tasks) != [64, 256, 1024] or any(len(rows) != 2 for rows in by_tasks.values()):
        raise RuntimeError("scale-up fixture does not contain two repetitions at 64, 256, and 1024 tasks")
    scale = {
        tasks: {
            "service_seconds": median(rows, "service_execution_seconds"),
            "manager_owner_ms": 1000 * median(
                [row["service_metrics"] for row in rows], "manager_owner_execute_seconds"
            ),
            "tasks_per_second": median(rows, "tasks_per_second"),
            "durable_mib": median(rows, "durable_file_bytes") / 2**20,
        }
        for tasks, rows in sorted(by_tasks.items())
    }

    fanin_path = ROOT / "acceptance" / "controller-remote-fanin-20260828.json"
    fanin_doc = json.loads(fanin_path.read_text())
    fanin = {
        int(row["workers"]): row
        for row in fanin_doc["requested_output_runs"]
        if row["payload_mib"] == 1 and row["workers"] in (16, 32, 64)
    }
    if sorted(fanin) != [16, 32, 64]:
        raise RuntimeError("remote fan-in fixture is missing 16, 32, or 64-worker rows")

    coupled_path = ROOT / "acceptance" / "data-intensive-fixed-ab-20260830.json"
    coupled_doc = json.loads(coupled_path.read_text())
    coupled_rows = coupled_doc["runs"]
    coupled = {
        "TaskVine (coupled)": {
            "seconds": median(coupled_rows, "taskvine_seconds"),
            "manager_mib": median(coupled_rows, "taskvine_manager_bytes") / 2**20,
        },
        "DataVine (separated)": {
            "seconds": median(coupled_rows, "datavine_seconds"),
            "manager_mib": median(coupled_rows, "datavine_manager_bytes") / 2**20,
        },
    }

    fig, axes = plt.subplots(2, 2, figsize=(7.2, 2.45))
    ax = axes[0, 0]
    tasks = np.array(sorted(scale))
    service = np.array([scale[x]["service_seconds"] for x in tasks])
    owner = np.array([scale[x]["manager_owner_ms"] for x in tasks])
    ax.bar(np.arange(3), service, color="#4477AA", alpha=0.82, label="data-plane service")
    ax.set_xticks(np.arange(3), [f"{x:,}" for x in tasks])
    ax.set_xlabel("Tasks / 1-MiB durable outputs")
    ax.set_ylabel("Service interval (s)")
    ax.set_title("(a) Scale-up: more data items")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", alpha=.2)
    right = ax.twinx()
    right.plot(np.arange(3), owner, color="#CC6677", marker="o", linewidth=1.5, label="manager owner")
    right.set_ylabel("Manager owner (ms)")
    right.set_ylim(bottom=0)
    ax.text(.03, .93, "64 MiB → 1 GiB", transform=ax.transAxes, fontsize=7)

    ax = axes[0, 1]
    workers = np.array(sorted(fanin))
    throughput = np.array([fanin[x]["mib_per_second"] for x in workers])
    cpu = np.array([fanin[x]["controller_cpu_seconds"] for x in workers])
    ax.bar(np.arange(3), throughput, color="#228833", alpha=0.82)
    ax.set_xticks(np.arange(3), [str(x) for x in workers])
    ax.set_xlabel("Concurrent workers / data endpoints")
    ax.set_ylabel("Fan-in (MiB/s)")
    ax.set_title("(b) Scale-out: remote fan-in")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", alpha=.2)
    right = ax.twinx()
    right.plot(np.arange(3), cpu, color="#AA3377", marker="s", linewidth=1.5)
    right.set_ylabel("Controller CPU (s)")
    right.set_ylim(bottom=0)
    ax.text(.03, .93, "4 GiB total per run", transform=ax.transAxes, fontsize=7)

    labels = list(coupled)
    colors = ["#CC6677", "#4477AA"]
    x = np.arange(2)
    ax = axes[1, 0]
    values = [coupled[label]["manager_mib"] for label in labels]
    ax.bar(x, values, color=colors, alpha=.85)
    ax.set_xticks(x, ["Coupled", "Separated"])
    ax.set_ylabel("Manager bytes (MiB)")
    ax.set_title("(c) Same 20k-task data workload")
    ax.set_yscale("log")
    ax.grid(axis="y", alpha=.2)
    for index, value in enumerate(values):
        ax.text(index, value * 1.25, f"{value:.3g}", ha="center", fontsize=7)

    ax = axes[1, 1]
    values = [coupled[label]["seconds"] for label in labels]
    ax.bar(x, values, color=colors, alpha=.85)
    ax.set_xticks(x, ["Coupled", "Separated"])
    ax.set_ylabel("Completion (s)")
    ax.set_title("(d) Coupled path becomes the service bottleneck")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", alpha=.2)
    for index, value in enumerate(values):
        ax.text(index, value + max(values) * .04, f"{value:.1f}", ha="center", fontsize=7)

    fig.suptitle("Why scheduling and data management must not share one serialized service path", fontsize=9.5)
    fig.tight_layout(rect=(0, 0, 1, .95), w_pad=2.4)
    fig.savefig(FIGURES / "bottleneck-motivation.pdf")
    fig.savefig(FIGURES / "bottleneck-motivation.png", dpi=180)
    plt.close(fig)

    summary = {
        "status": "PASS",
        "scale_up": scale,
        "scale_out_remote_fanin": fanin,
        "coupled_comparison": coupled,
        "interpretation": {
            "scale_up": "At a fixed 4-worker by 2-core topology, 1-MiB durable-object count grows from 64 to 1024; the service interval grows 15.9x while manager owner time remains milliseconds.",
            "scale_out": "With 4 GiB total remote fan-in, throughput declines from 542.8 to 442.9 MiB/s as concurrent workers increase from 16 to 64 while controller CPU remains about 36-37 s.",
            "coupled": "On the matched retained 20k-task data workload, the coupled TaskVine path carries 64.1 MiB of manager data-plane traffic versus about 0.04 MiB for the separated path and completes about 16.1x slower.",
        },
        "inputs_sha256": {
            str(path.relative_to(ROOT)): digest(path)
            for path in [*scale_files, fanin_path, coupled_path, Path(__file__).resolve()]
        },
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"status": "PASS", "figure": str(FIGURES / "bottleneck-motivation.pdf")}))


if __name__ == "__main__":
    main()
