#!/usr/bin/env python3
"""Render adaptive-concurrency A/B evidence without Python plotting packages."""

import argparse
import html
import json
from pathlib import Path
import statistics
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("results", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    document = json.loads(args.results.read_text())
    cases = sorted({row["case"] for row in document["results"]})
    medians = {
        (case, multiplier): statistics.median(
            row["tasks_per_second"]
            for row in document["results"]
            if row["case"] == case and row["queue_multiplier"] == multiplier
        )
        for case in cases
        for multiplier in (1, 8)
    }

    width, height = 1100, 580
    left, right, top, row_height = 150, 760, 105, 58
    minimum, maximum = 0.80, 1.50

    def x(value):
        return left + (value - minimum) * (right - left) / (maximum - minimum)

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<style>text{font-family:DejaVu Sans,Arial,sans-serif;fill:#17212b}.title{font-size:24px;font-weight:700}.subtitle{font-size:14px;fill:#52606d}.label{font-size:15px}.value{font-size:13px;fill:#35404a}.tick{font-size:12px;fill:#66727d}</style>',
        '<text x="40" y="38" class="title">Adaptive FunctionCall concurrency</text>',
        '<text x="40" y="65" class="subtitle">Median throughput ratio; local 1x4, except rebalance with a late second Worker</text>',
    ]
    for tick in (0.8, 1.0, 1.2, 1.4):
        position = x(tick)
        color = "#59636e" if tick == 1.0 else "#d9dfe5"
        thickness = 2 if tick == 1.0 else 1
        lines.append(f'<line x1="{position:.1f}" y1="82" x2="{position:.1f}" y2="515" stroke="{color}" stroke-width="{thickness}"/>')
        lines.append(f'<text x="{position:.1f}" y="545" text-anchor="middle" class="tick">{tick:.1f}x</text>')
    for index, case in enumerate(cases):
        baseline = medians[(case, 1)]
        adaptive = medians[(case, 8)]
        ratio = adaptive / baseline
        y = top + index * row_height
        start, finish = x(1.0), x(ratio)
        color = "#167d9a" if ratio >= 1 else "#c65d3a"
        lines.append(f'<text x="135" y="{y + 16}" text-anchor="end" class="label">{html.escape(case)}</text>')
        lines.append(f'<rect x="{min(start, finish):.1f}" y="{y}" width="{max(2, abs(finish - start)):.1f}" height="23" rx="3" fill="{color}"/>')
        lines.append(f'<circle cx="{finish:.1f}" cy="{y + 11.5}" r="5" fill="{color}"/>')
        lines.append(f'<text x="790" y="{y + 16}" class="value">{baseline:.1f} to {adaptive:.1f} tasks/s ({ratio:.2f}x)</text>')
    lines.append('<text x="455" y="570" text-anchor="middle" class="subtitle">adaptive throughput / fixed-window throughput</text>')
    lines.append('</svg>')

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", suffix=".svg") as stream:
        stream.write("\n".join(lines))
        stream.flush()
        subprocess.run(
            ("rsvg-convert", stream.name, "-o", str(args.output)), check=True
        )


if __name__ == "__main__":
    main()
