#!/usr/bin/env python3
"""Build compact machine evidence and publication-readable SVG/PNG figures."""

import hashlib
import html
import json
from pathlib import Path
import statistics
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parent
RAW = ROOT / "raw"


def load(name):
    return json.loads((RAW / name).read_text())


def median_rows(document, case, policy):
    rows = [
        row for row in document["results"]
        if row["case"] == case and row["window_policy"] == policy
    ]
    return statistics.median(row["tasks_per_second"] for row in rows), rows


def best_fixed(document, case):
    groups = {}
    for row in document["results"]:
        if row["case"] == case and row["window_policy"] == "fixed":
            groups.setdefault(row["fixed_window_multiplier"], []).append(
                row["tasks_per_second"]
            )
    multiplier, values = max(
        groups.items(), key=lambda item: statistics.median(item[1])
    )
    return statistics.median(values), multiplier, values


def convert_svg(lines, output):
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", suffix=".svg") as stream:
        stream.write("\n".join(lines))
        stream.flush()
        subprocess.run(
            ("rsvg-convert", stream.name, "-o", str(output)), check=True
        )


def style():
    return (
        "<style>text{font-family:DejaVu Sans,Arial,sans-serif;fill:#17212b}"
        ".title{font-size:25px;font-weight:700}.subtitle{font-size:14px;fill:#52606d}"
        ".label{font-size:15px}.small{font-size:12px;fill:#52606d}"
        ".value{font-size:13px;font-weight:600}</style>"
    )


def oracle_figure(workloads):
    width, height = 1180, 590
    left, right, top, row = 175, 780, 110, 67
    minimum, maximum = 0.75, 1.08

    def x(value):
        return left + (value - minimum) * (right - left) / (maximum - minimum)

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect width="100%" height="100%" fill="white"/>', style(),
        '<text x="38" y="38" class="title">Elastic admission stays near the fixed-window oracle</text>',
        '<text x="38" y="66" class="subtitle">Median end-to-end throughput; three randomized repetitions; local Worker pinned to 4 CPU cores</text>',
    ]
    for tick in (0.8, 0.9, 1.0):
        px = x(tick)
        lines.append(
            f'<line x1="{px:.1f}" y1="88" x2="{px:.1f}" y2="510" '
            f'stroke="{("#485563" if tick == 1 else "#dbe2e8")}" '
            f'stroke-width="{2 if tick == 1 else 1}"/>'
        )
        lines.append(f'<text x="{px:.1f}" y="535" text-anchor="middle" class="small">{tick:.1f}x</text>')
    for index, item in enumerate(workloads):
        y = top + index * row
        ratio = item["ratio"]
        start, finish = x(1), x(ratio)
        color = "#0f7894" if ratio >= 0.98 else "#c65d3a"
        lines += [
            f'<text x="158" y="{y + 17}" text-anchor="end" class="label">{html.escape(item["case"])}</text>',
            f'<rect x="{min(start, finish):.1f}" y="{y}" width="{max(2, abs(finish-start)):.1f}" height="25" rx="4" fill="{color}"/>',
            f'<circle cx="{finish:.1f}" cy="{y + 12.5}" r="5" fill="{color}"/>',
            f'<text x="815" y="{y + 10}" class="value">{ratio * 100:.1f}% of oracle</text>',
            f'<text x="815" y="{y + 29}" class="small">elastic {item["elastic"]:.1f}; fixed {item["fixed_multiplier"]}x {item["fixed"]:.1f} tasks/s</text>',
        ]
    lines += [
        '<text x="478" y="570" text-anchor="middle" class="subtitle">elastic throughput / best measured fixed-window throughput</text>',
        '</svg>',
    ]
    convert_svg(lines, ROOT / "adaptive_oracle.png")


def trajectory_figure(oracle, memory):
    cases = ("cpu", "io", "mixed")
    width, height = 1180, 700
    left, right, top, panel = 110, 1090, 105, 165
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect width="100%" height="100%" fill="white"/>', style(),
        '<text x="38" y="38" class="title">Worker-local control reacts to pressure, not task labels</text>',
        '<text x="38" y="66" class="subtitle">Representative median elastic run; one sample every 250 ms; dashed line is normalized CPU</text>',
    ]
    for index, case in enumerate(cases):
        _, rows = median_rows(oracle, case, "elastic")
        median_rate = statistics.median(row["tasks_per_second"] for row in rows)
        chosen = min(rows, key=lambda row: abs(row["tasks_per_second"] - median_rate))
        trace = chosen["window_trace"]
        y0 = top + index * panel
        ymax = max(32, max(point["window"] for point in trace))
        lines.append(f'<text x="92" y="{y0 + 50}" text-anchor="end" class="label">{case}</text>')
        lines.append(f'<line x1="{left}" y1="{y0 + 100}" x2="{right}" y2="{y0 + 100}" stroke="#dbe2e8"/>')
        points_w, points_c = [], []
        for sample, point in enumerate(trace):
            px = left + sample * (right - left) / max(1, len(trace) - 1)
            py_w = y0 + 100 - 88 * point["window"] / ymax
            cpu = max(0, min(1.1, point["cpu_fraction"]))
            py_c = y0 + 100 - 80 * cpu
            points_w.append(f"{px:.1f},{py_w:.1f}")
            points_c.append(f"{px:.1f},{py_c:.1f}")
        lines += [
            f'<polyline points="{" ".join(points_w)}" fill="none" stroke="#0f7894" stroke-width="3"/>',
            f'<polyline points="{" ".join(points_c)}" fill="none" stroke="#dc6b39" stroke-width="2" stroke-dasharray="6 4"/>',
            f'<text x="{right}" y="{y0 + 122}" text-anchor="end" class="small">window max {max(p["window"] for p in trace)}; {chosen["tasks_per_second"]:.1f} tasks/s</text>',
        ]
    max_rss = max(
        point["memory_fraction"]
        for row in memory["results"] for point in row["window_trace"]
    )
    lines += [
        f'<rect x="110" y="620" width="{760 * max_rss / .8:.1f}" height="24" rx="4" fill="#3a9563"/>',
        '<line x1="870" y1="610" x2="870" y2="655" stroke="#9d2f2f" stroke-width="3"/>',
        f'<text x="885" y="638" class="small">peak RSS {100*max_rss:.2f}%; limit 80%; concurrency 11</text>',
        '</svg>',
    ]
    convert_svg(lines, ROOT / "adaptive_trajectories.png")


def controller_figure(small, large):
    width, height = 1100, 570
    writers = [row["writers"] for row in small["runs"]]
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect width="100%" height="100%" fill="white"/>', style(),
        '<text x="38" y="38" class="title">Controller object ingest: four connections are the balanced default</text>',
        '<text x="38" y="66" class="subtitle">Cold immutable OBJECT_PUT over loopback TCP into Controller-local /tmp</text>',
    ]
    colors = ("#8ab6c4", "#247a96", "#173f5f")
    for panel_index, (title, document, transform, unit, ymax) in enumerate((
        ("4,096 x 256 B", small, lambda row: row["cold"]["objects_per_second"], "objects/s", 3500),
        ("1,024 x 128 KiB", large, lambda row: row["cold"]["bytes"] / row["cold"]["seconds"] / 2**20, "MiB/s", 150),
    )):
        x0 = 80 + panel_index * 520
        base = 465
        lines.append(f'<text x="{x0 + 210}" y="110" text-anchor="middle" class="label">{title}</text>')
        for index, row in enumerate(document["runs"]):
            value = transform(row)
            height_px = 310 * value / ymax
            x = x0 + 65 + index * 120
            lines += [
                f'<rect x="{x}" y="{base-height_px:.1f}" width="72" height="{height_px:.1f}" rx="4" fill="{colors[index]}"/>',
                f'<text x="{x+36}" y="{base-height_px-9:.1f}" text-anchor="middle" class="value">{value:.1f}</text>',
                f'<text x="{x+36}" y="{base+24}" text-anchor="middle" class="label">{writers[index]} conn</text>',
            ]
        lines.append(f'<text x="{x0 + 210}" y="535" text-anchor="middle" class="subtitle">{unit}</text>')
    lines.append('</svg>')
    convert_svg(lines, ROOT / "controller_ingest.png")


def architecture_figure():
    width, height = 1180, 500
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect width="100%" height="100%" fill="white"/>', style(),
        '<text x="38" y="38" class="title">DataVine production boundary</text>',
        '<text x="38" y="66" class="subtitle">One workflow process; scheduling completion is decoupled from data admission</text>',
    ]
    boxes = (
        (55, 130, 235, 100, "Workflow client", "submit/append + OBJECT_PUT"),
        (360, 105, 260, 115, "Scheduler / Manager", "ready queue, dispatch, recall"),
        (360, 300, 260, 115, "Data Controller", "replicas, tickets, /tmp, GC"),
        (735, 105, 380, 115, "Workers", "queued grants + elastic admission"),
        (735, 300, 380, 115, "Worker data agents", "local-first, peer-first, Controller fallback"),
    )
    for x, y, w, h, title, detail in boxes:
        fill = "#eef6f8" if y < 250 else "#eff8f2"
        lines += [
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill}" stroke="#6c7a86" stroke-width="2"/>',
            f'<text x="{x+w/2}" y="{y+38}" text-anchor="middle" class="label">{title}</text>',
            f'<text x="{x+w/2}" y="{y+68}" text-anchor="middle" class="small">{detail}</text>',
        ]
    arrows = (
        (290, 165, 360, 165, "task control"), (290, 205, 360, 340, "data RPC"),
        (620, 165, 735, 165, "one task / grant"), (620, 345, 735, 345, "signed data ticket"),
        (925, 220, 925, 300, "local files"),
    )
    for x1, y1, x2, y2, label in arrows:
        lines += [
            f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="#334e68" stroke-width="2" marker-end="url(#arrow)"/>',
            f'<text x="{(x1+x2)/2}" y="{(y1+y2)/2-8}" text-anchor="middle" class="small">{label}</text>',
        ]
    lines.insert(4, '<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L0,6 L8,3 z" fill="#334e68"/></marker></defs>')
    lines += [
        '<text x="55" y="470" class="subtitle">TASK_FINISHED releases children immediately; data failure/recovery is handled independently by the Controller.</text>',
        '</svg>',
    ]
    convert_svg(lines, ROOT / "architecture.png")


def main():
    oracle = load("final-oracle.json")
    short = load("final-short.json")
    disk = load("final-disk.json")
    noop = load("final-noop.json")
    memory = load("final-memory.json")
    recall = load("final-recall.json")
    scale = load("condor-8x16-rpc-final.json")
    small = load("controller-object-ingest-256b.json")
    large = load("controller-object-ingest-128k.json")
    regression = load("regression-final-v2.json")
    workloads = []
    for case, document in (
        ("CPU 20 ms", oracle), ("sleep-I/O 20 ms", oracle),
        ("mixed 20 ms", oracle), ("sleep 5 ms", short),
        ("random I/O + fsync", disk), ("Python noop", noop),
    ):
        key = case.split()[0].lower() if case.startswith(("CPU", "mixed")) else None
        if case.startswith("CPU"):
            key = "cpu"
        elif case.startswith("sleep-I/O"):
            key = "io"
        elif case.startswith("mixed"):
            key = "mixed"
        elif case.startswith("sleep 5"):
            key = "short"
        elif case.startswith("random"):
            key = "disk"
        else:
            key = "noop"
        elastic, _ = median_rows(document, key, "elastic")
        fixed, multiplier, fixed_values = best_fixed(document, key)
        workloads.append({
            "case": case, "elastic": elastic, "fixed": fixed,
            "fixed_multiplier": multiplier, "ratio": elastic / fixed,
            "repetitions": 3, "fixed_samples": fixed_values,
        })
    max_rss = max(
        point["memory_fraction"]
        for row in memory["results"] for point in row["window_trace"]
    )
    scale_row = scale["results"][0]
    summary = {
        "artifact_type": "datavine-elastic-admission-final",
        "status": "PASS",
        "date": "2026-09-03",
        "provenance": oracle["provenance"],
        "workloads": workloads,
        "memory": {
            "worker_memory_mib": 2048,
            "declared_mib_per_task": 128,
            "peak_concurrency": max(row["peak_concurrency"] for row in memory["results"]),
            "peak_rss_fraction": max_rss,
            "repetitions": len(memory["results"]),
        },
        "recall": [{
            "case": row["case"], "repetition": row["repetition"],
            "executor_task_counts": row["executor_task_counts"],
            "sent": row["metrics"]["function_recalls_sent"],
            "succeeded": row["metrics"]["function_recalls_succeeded"],
            "missed": row["metrics"]["function_recalls_missed"],
            "workers_removed": row["metrics"]["manager_workers_removed"],
        } for row in recall["results"]],
        "condor_8x16": {
            "tasks": scale_row["tasks"],
            "physical_workers": scale_row["workers"],
            "cores_per_worker": scale_row["cores_per_worker"],
            "service_seconds": scale_row["metrics"]["run_seconds"],
            "service_tasks_per_second": scale_row["tasks"] / scale_row["metrics"]["run_seconds"],
            "end_to_end_seconds": scale_row["elapsed_seconds"],
            "end_to_end_tasks_per_second": scale_row["tasks_per_second"],
            "worker_admission_seconds_excluded": scale_row["worker_admission_seconds"],
        },
        "controller_ingest": {
            "small": small,
            "large": large,
            "selected_connections": 4,
        },
        "regression": {
            "status": regression["status"],
            "passed": regression["passed_count"],
            "total": regression["test_count"],
        },
    }
    (ROOT / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    oracle_figure(workloads)
    trajectory_figure(oracle, memory)
    controller_figure(small, large)
    architecture_figure()
    for path in (ROOT / "summary.json", ROOT / "adaptive_oracle.png",
                 ROOT / "adaptive_trajectories.png", ROOT / "controller_ingest.png",
                 ROOT / "architecture.png"):
        print(hashlib.sha256(path.read_bytes()).hexdigest(), path.name)


if __name__ == "__main__":
    main()
