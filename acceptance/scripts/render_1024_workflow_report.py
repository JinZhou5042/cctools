#!/usr/bin/env python3
"""Render fixed-core campaign JSON into Markdown and CSV without remeasurement."""

import argparse
import csv
import json
from pathlib import Path


def value(item):
    return "" if item is None else item


def number(item, digits=4):
    return "-" if item is None else f"{float(item):.{digits}f}"


def row_for(item):
    parameters = item["parameters"]
    graph = item["graph"]
    bottleneck = item["bottleneck"]
    interval = item["paired_bootstrap_95pct"]
    return {
        "case_id": item["case_id"],
        "phase": item["phase"],
        "topology": item["topology"],
        "logical_tasks": graph["logical_tasks"],
        "logical_edges": graph["logical_edges"],
        "max_indegree": graph["max_indegree"],
        "max_outdegree": graph["max_outdegree"],
        "edge_payload_bytes": graph["logical_edge_payload_bytes"],
        "requested_payload_bytes": graph["requested_payload_bytes"],
        "target_cpu_ms": parameters.get("target_cpu_ms"),
        "taskvine_median_seconds": item["taskvine_median_seconds"],
        "datavine_median_seconds": item["datavine_median_seconds"],
        "datavine_to_taskvine_rate": item["datavine_to_taskvine_rate"],
        "ratio_ci_low": interval[0],
        "ratio_ci_high": interval[1],
        "paired_repetitions": item["paired_repetitions"],
        "bottleneck_status": bottleneck.get("status"),
        "bottleneck_class": bottleneck.get("classification"),
        "absolute_excess_seconds": bottleneck.get("absolute_excess_seconds"),
        "explained_fraction": bottleneck.get("explained_fraction"),
        "improvement": bottleneck.get("improvement"),
    }


def render_markdown(report, rows, source):
    environment = report["environment"]
    gates = report.get("gates", {})
    lines = [
        "# DataVine versus TaskVine fixed-core result",
        "",
        f"Source artifact: `{source}`",
        "",
        f"Status: **{report['status']}**; scope: **{report['scope']}**.",
        "",
        (f"Resource contract: {environment['workers']} workers x "
         f"{environment['cores_per_worker']} cores = "
         f"{environment['total_cores']} cores per backend; "
         f"batch type `{environment['batch_type']}`."),
        "",
        "A ratio above 1 favors DataVine. A performance conclusion requires a "
        "non-null paired 95% interval. Accumulated parallel service times are "
        "reported as work amplification and are not treated as wall critical path.",
        "",
        "## Gates",
        "",
    ]
    for name, passed in gates.items():
        lines.append(f"- `{name}`: `{passed}`")
    lines.extend((
        "", "## Results", "",
        "| Case | Phase | Topology | Tasks | Edges | CPU ms | In/Out degree | TV s | DV s | DV/TV | 95% CI | Attribution |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ))
    for item, row in zip(report.get("summary", ()), rows):
        cpu = value(row["target_cpu_ms"])
        interval = item["paired_bootstrap_95pct"]
        ci = "-" if interval[0] is None else f"[{interval[0]:.3f}, {interval[1]:.3f}]"
        attribution = row["bottleneck_class"] or row["bottleneck_status"] or "-"
        lines.append(
            f"| `{row['case_id']}` | {row['phase']} | {row['topology']} | "
            f"{row['logical_tasks']} | {row['logical_edges']} | {cpu} | "
            f"{row['max_indegree']}/{row['max_outdegree']} | "
            f"{number(row['taskvine_median_seconds'])} | "
            f"{number(row['datavine_median_seconds'])} | "
            f"{number(row['datavine_to_taskvine_rate'], 3)} | {ci} | {attribution} |"
        )
    regressions = [item for item in report.get("summary", ())
                   if item["bottleneck"].get("required")]
    lines.extend(("", "## DataVine regressions and causes", ""))
    if not regressions:
        lines.append("No statistically established regression is available in this artifact.")
    for item in regressions:
        bottleneck = item["bottleneck"]
        lines.extend((
            f"### {item['case_id']}", "",
            (f"DataVine excess: {bottleneck['absolute_excess_seconds']:.6f} s; "
             f"classification: `{bottleneck['classification']}`; "
             f"status: `{bottleneck['status']}`."),
            "",
            f"Improvement: {bottleneck.get('improvement') or 'blocked pending attribution.'}",
            "",
            "Evidence:", "",
            "```json", json.dumps(bottleneck.get("evidence", {}), indent=2, sort_keys=True), "```", "",
        ))
    if report["status"] != "PASS":
        lines.extend((
            "## Publication boundary", "",
            "This artifact is not a final performance result. Missing repetitions, "
            "failed admission, or unresolved attribution remain explicit in the gates.", "",
        ))
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=Path)
    parser.add_argument("--markdown", type=Path, required=True)
    parser.add_argument("--csv", type=Path, required=True)
    args = parser.parse_args()
    report = json.loads(args.summary.read_text())
    rows = [row_for(item) for item in report.get("summary", ())]
    args.markdown.write_text(render_markdown(report, rows, args.summary))
    fields = list(row_for(report["summary"][0])) if report.get("summary") else ["case_id"]
    with args.csv.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps({
        "status": "PASS", "rows": len(rows),
        "markdown": str(args.markdown), "csv": str(args.csv),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
