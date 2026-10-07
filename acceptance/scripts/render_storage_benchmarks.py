#!/usr/bin/env python3
"""Render the retained 32x16 storage benchmark evidence as PNG figures."""

import argparse
import json
from pathlib import Path
import statistics

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


COLORS = {
    "Controller /tmp": "#1677b8",
    "SharedFS direct": "#e07a1f",
    "Pure peer": "#238b57",
}


def load(path):
    with path.open() as stream:
        return json.load(stream)


def render_matrix(root):
    summary = load(root / "summary.json")
    labels = ["Controller /tmp", "SharedFS direct", "Pure peer"]
    keys = ["controller_tmp", "worker_direct_sharedfs", "peer"]
    panels = [
        (
            "Empty-file metadata lifecycle",
            summary["metadata_results"],
            "rounds_files_per_second",
            "files/s",
        ),
        (
            "32 GiB movement (4096 x 8 MiB)",
            summary["big_data_results"],
            "rounds_mib_per_second",
            "MiB/s",
        ),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.6), constrained_layout=True)
    for axis, (title, results, field, unit) in zip(axes, panels):
        medians = [statistics.median(results[key][field]) for key in keys]
        positions = np.arange(len(labels))
        bars = axis.bar(
            positions,
            medians,
            color=[COLORS[label] for label in labels],
            width=0.62,
            alpha=0.82,
            zorder=2,
        )
        offsets = (-0.13, 0.0, 0.13)
        for position, key, label in zip(positions, keys, labels):
            values = results[key][field]
            axis.scatter(
                [position + offsets[index] for index in range(len(values))],
                values,
                s=40,
                facecolor="white",
                edgecolor=COLORS[label],
                linewidth=1.7,
                zorder=3,
            )
        axis.bar_label(
            bars,
            labels=[f"{value:,.0f}" for value in medians],
            padding=4,
            fontsize=10,
            fontweight="bold",
        )
        axis.set_title(title, fontsize=13, fontweight="bold")
        axis.set_ylabel(unit)
        axis.set_xticks(positions, labels, rotation=10)
        axis.grid(axis="y", color="#d7d7d7", linewidth=0.8, alpha=0.8, zorder=0)
        axis.spines[["top", "right"]].set_visible(False)
        axis.set_ylim(0, max(max(results[key][field]) for key in keys) * 1.18)
    fig.suptitle(
        "DataVine storage paths — 32 Workers x 16 cores\n"
        "bars: three-round medians; circles: retained rounds",
        fontsize=15,
        fontweight="bold",
    )
    output = root / "storage_matrix_32x16.png"
    fig.savefig(output, dpi=180, facecolor="white")
    plt.close(fig)
    return output


def render_crossover(root, matrix_root):
    summary = load(root / "summary.json")
    matrix = load(matrix_root / "summary.json")
    repeated_sizes = np.array([128, 192, 256], dtype=float)
    controller_rounds = []
    sharedfs_rounds = []
    controller_medians = []
    sharedfs_medians = []
    for size in repeated_sizes.astype(int):
        record = summary["primary_results"][f"{size * 1024}_bytes"]
        controller_rounds.append(record["controller_files_per_second"])
        sharedfs_rounds.append(record["sharedfs_files_per_second"])
        controller_medians.append(record["controller_median_files_per_second"])
        sharedfs_medians.append(record["sharedfs_median_files_per_second"])

    controller_empty = matrix["metadata_results"]["controller_tmp"][
        "median_files_per_second"
    ]
    sharedfs_empty = matrix["metadata_results"]["worker_direct_sharedfs"][
        "median_files_per_second"
    ]
    anchor = summary["single_round_anchors"]["524288_bytes"]
    controller_512 = anchor["controller_files_per_second"]
    sharedfs_512 = anchor["sharedfs_files_per_second"]

    size_bytes = 512 * 1024
    controller_fixed = 1.0 / controller_empty
    sharedfs_fixed = 1.0 / sharedfs_empty
    controller_slope = (1.0 / controller_512 - controller_fixed) / size_bytes
    sharedfs_slope = (1.0 / sharedfs_512 - sharedfs_fixed) / size_bytes
    crossing_bytes = (sharedfs_fixed - controller_fixed) / (
        controller_slope - sharedfs_slope
    )
    crossing_kib = crossing_bytes / 1024.0

    x_model_kib = np.linspace(0, 560, 500)
    x_model_bytes = x_model_kib * 1024
    controller_model_time = controller_fixed + controller_slope * x_model_bytes
    sharedfs_model_time = sharedfs_fixed + sharedfs_slope * x_model_bytes

    fig, axes = plt.subplots(1, 2, figsize=(14.5, 6.0), constrained_layout=True)
    throughput = axes[0]
    jitter = (-2.7, 0.0, 2.7)
    for sizes, rounds, label in (
        (repeated_sizes, controller_rounds, "Controller /tmp"),
        (repeated_sizes, sharedfs_rounds, "SharedFS direct"),
    ):
        for size, values in zip(sizes, rounds):
            throughput.scatter(
                [size + offset for offset in jitter],
                values,
                s=43,
                facecolor="white",
                edgecolor=COLORS[label],
                linewidth=1.6,
                zorder=4,
            )
    throughput.plot(
        repeated_sizes,
        controller_medians,
        marker="o",
        linewidth=2.2,
        color=COLORS["Controller /tmp"],
        label="Controller median",
        zorder=3,
    )
    throughput.plot(
        repeated_sizes,
        sharedfs_medians,
        marker="o",
        linewidth=2.2,
        color=COLORS["SharedFS direct"],
        label="SharedFS median",
        zorder=3,
    )
    for size in (160, 512):
        record = summary["single_round_anchors"][f"{size * 1024}_bytes"]
        throughput.scatter(
            [size],
            [record["controller_files_per_second"]],
            marker="D",
            s=62,
            facecolor="none",
            edgecolor=COLORS["Controller /tmp"],
            linewidth=1.7,
            zorder=4,
        )
        throughput.scatter(
            [size],
            [record["sharedfs_files_per_second"]],
            marker="D",
            s=62,
            facecolor="none",
            edgecolor=COLORS["SharedFS direct"],
            linewidth=1.7,
            zorder=4,
        )
    throughput.axvspan(192, 256, color="#8e68c8", alpha=0.13, label="Measured band")
    throughput.set_title("Measured end-to-end throughput", fontsize=13, fontweight="bold")
    throughput.set_xlabel("file size (KiB)")
    throughput.set_ylabel("files/s")
    throughput.set_xlim(105, 535)
    throughput.set_ylim(0, 4300)
    throughput.grid(color="#d7d7d7", linewidth=0.8, alpha=0.8)
    throughput.spines[["top", "right"]].set_visible(False)
    throughput.legend(frameon=False, loc="upper right")
    throughput.text(
        505,
        150,
        "diamonds: single-round anchors",
        ha="right",
        va="bottom",
        fontsize=9,
        color="#555555",
    )

    model = axes[1]
    model.plot(
        x_model_kib,
        controller_model_time * 1e6,
        color=COLORS["Controller /tmp"],
        linewidth=2.3,
        label="Controller calibrated model",
    )
    model.plot(
        x_model_kib,
        sharedfs_model_time * 1e6,
        color=COLORS["SharedFS direct"],
        linewidth=2.3,
        label="SharedFS calibrated model",
    )
    model.axvspan(192, 256, color="#8e68c8", alpha=0.13, label="Measured band")
    model.axvline(crossing_kib, color="#333333", linestyle="--", linewidth=1.5)
    model.scatter(
        [crossing_kib],
        [(controller_fixed + controller_slope * crossing_bytes) * 1e6],
        color="#333333",
        s=35,
        zorder=4,
    )
    model.annotate(
        f"calibrated crossing\n{crossing_kib:.1f} KiB",
        xy=(crossing_kib, (controller_fixed + controller_slope * crossing_bytes) * 1e6),
        xytext=(crossing_kib + 48, 1250),
        arrowprops={"arrowstyle": "->", "color": "#333333"},
        fontsize=10,
    )
    model.text(
        330,
        405,
        "empty-file fixed cost\nController 88 us/file\nSharedFS 676 us/file",
        fontsize=10,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "edgecolor": "#bbbbbb"},
    )
    model.text(
        545,
        90,
        "8-MiB asymptotic model: 634 KiB\n(not valid in the small-file regime)",
        fontsize=9,
        ha="right",
        va="bottom",
        color="#555555",
    )
    model.set_title("Affine model calibrated at 0 and 512 KiB", fontsize=13, fontweight="bold")
    model.set_xlabel("file size (KiB)")
    model.set_ylabel("mean time per file (us)")
    model.set_xlim(0, 560)
    model.set_ylim(0, 1750)
    model.grid(color="#d7d7d7", linewidth=0.8, alpha=0.8)
    model.spines[["top", "right"]].set_visible(False)
    model.legend(frameon=False, loc="upper left")

    fig.suptitle(
        "Controller vs SharedFS small-file crossover — 32 Workers x 16 cores\n"
        "circles: retained rounds; lines: three-round medians",
        fontsize=15,
        fontweight="bold",
    )
    output = root / "small_file_crossover_32x16.png"
    fig.savefig(output, dpi=180, facecolor="white")
    plt.close(fig)
    return output


def dense_result_path(root, name):
    """Accept both the historical flat raw layout and the indexed layout."""
    organized = root / "raw" / "results" / name
    if organized.exists():
        return organized
    return root / "raw" / name


def dense_documents(root, mode):
    documents = []
    for name in (
        f"dense-uniform-{mode}.json",
        f"dense-confirm-{mode}.json",
    ):
        path = dense_result_path(root, name)
        if path.exists():
            documents.append(load(path))
    return documents


def dense_series(documents):
    grouped = {}
    sequence_offset = 0
    for campaign_index, document in enumerate(documents, 1):
        for phase in document["phases"]:
            annotated = dict(phase)
            annotated["sample_id"] = (campaign_index, phase["round"])
            annotated["sequence_index"] = sequence_offset + phase["phase_index"]
            grouped.setdefault(phase["size_kib"], []).append(annotated)
        sequence_offset += len(document["phases"])
    return grouped


def render_dense_crossover(root):
    controller_documents = dense_documents(root, "controller")
    sharedfs_documents = dense_documents(root, "sharedfs")
    if not controller_documents or not sharedfs_documents:
        raise RuntimeError("dense crossover inputs are missing")
    if any(document["status"] != "PASS" for document in
           controller_documents + sharedfs_documents):
        raise RuntimeError("dense crossover inputs are not PASS")
    controller_by_size = dense_series(controller_documents)
    sharedfs_by_size = dense_series(sharedfs_documents)
    sizes = np.array(sorted(controller_by_size), dtype=float)
    if list(sizes.astype(int)) != sorted(sharedfs_by_size):
        raise RuntimeError("dense crossover size grids differ")

    def rates(grouped, size):
        return [phase["files_per_second"] for phase in grouped[int(size)]]

    controller_values = [rates(controller_by_size, size) for size in sizes]
    sharedfs_values = [rates(sharedfs_by_size, size) for size in sizes]
    controller_median = np.array([statistics.median(values) for values in controller_values])
    sharedfs_median = np.array([statistics.median(values) for values in sharedfs_values])
    controller_min = np.array([min(values) for values in controller_values])
    controller_max = np.array([max(values) for values in controller_values])
    sharedfs_min = np.array([min(values) for values in sharedfs_values])
    sharedfs_max = np.array([max(values) for values in sharedfs_values])

    paired_ratios = []
    for size in sizes:
        controller_rounds = {
            phase["sample_id"]: phase["files_per_second"]
            for phase in controller_by_size[int(size)]
        }
        sharedfs_rounds = {
            phase["sample_id"]: phase["files_per_second"]
            for phase in sharedfs_by_size[int(size)]
        }
        if controller_rounds.keys() != sharedfs_rounds.keys():
            raise RuntimeError(f"round mismatch at {size:g} KiB")
        paired_ratios.append([
            sharedfs_rounds[sample_id] / controller_rounds[sample_id]
            for sample_id in sorted(controller_rounds)
        ])
    ratio_median = np.array([statistics.median(values) for values in paired_ratios])
    ratio_min = np.array([min(values) for values in paired_ratios])
    ratio_max = np.array([max(values) for values in paired_ratios])

    fig, axes = plt.subplots(2, 1, figsize=(15.5, 10.0), sharex=True,
                             constrained_layout=True,
                             gridspec_kw={"height_ratios": [2.0, 1.0]})
    throughput, ratio = axes
    for grouped_values, label in (
        (controller_values, "Controller /tmp"),
        (sharedfs_values, "SharedFS direct"),
    ):
        for size, values in zip(sizes, grouped_values):
            jitter = np.linspace(-5.0, 5.0, len(values))
            throughput.scatter(
                [size + offset for offset in jitter], values,
                s=18, facecolor="white", edgecolor=COLORS[label],
                linewidth=0.9, alpha=0.78, zorder=4,
            )
    throughput.fill_between(
        sizes, controller_min, controller_max,
        color=COLORS["Controller /tmp"], alpha=0.11, linewidth=0,
        label="Controller round range",
    )
    throughput.fill_between(
        sizes, sharedfs_min, sharedfs_max,
        color=COLORS["SharedFS direct"], alpha=0.11, linewidth=0,
        label="SharedFS round range",
    )
    throughput.plot(
        sizes, controller_median, marker="o", markersize=3.6, linewidth=1.8,
        color=COLORS["Controller /tmp"], label="Controller median", zorder=3,
    )
    throughput.plot(
        sizes, sharedfs_median, marker="o", markersize=3.6, linewidth=1.8,
        color=COLORS["SharedFS direct"], label="SharedFS median", zorder=3,
    )
    throughput.set_ylabel("end-to-end throughput (files/s)")
    throughput.set_title(
        "Every retained round on a uniform 32-KiB grid",
        fontsize=13, fontweight="bold",
    )
    throughput.grid(color="#d7d7d7", linewidth=0.7, alpha=0.75)
    throughput.spines[["top", "right"]].set_visible(False)
    throughput.legend(frameon=False, ncols=2, loc="upper right")

    for size, values in zip(sizes, paired_ratios):
        jitter = np.linspace(-5.0, 5.0, len(values))
        ratio.scatter(
            [size + offset for offset in jitter], values,
            s=17, facecolor="white", edgecolor="#6b4c9a",
            linewidth=0.8, alpha=0.75, zorder=4,
        )
    ratio.fill_between(
        sizes, ratio_min, ratio_max, color="#8e68c8", alpha=0.14,
        linewidth=0, label="paired round range",
    )
    ratio.plot(
        sizes, ratio_median, marker="o", markersize=3.5, linewidth=1.7,
        color="#6b4c9a", label="paired median ratio", zorder=3,
    )
    ratio.axhline(1.0, color="#333333", linestyle="--", linewidth=1.2,
                  label="equal throughput")
    ratio.fill_between(
        sizes, 1.0, ratio_median, where=ratio_median >= 1.0,
        color=COLORS["SharedFS direct"], alpha=0.10, interpolate=True,
    )
    ratio.fill_between(
        sizes, ratio_median, 1.0, where=ratio_median < 1.0,
        color=COLORS["Controller /tmp"], alpha=0.08, interpolate=True,
    )
    ratio.set_xlabel("file size (KiB); uniform 32-KiB spacing")
    ratio.set_ylabel("SharedFS / Controller\npaired throughput")
    ratio.set_xlim(sizes[0] - 16, sizes[-1] + 16)
    ratio.set_xticks(np.arange(32, 1025, 64))
    ratio.grid(color="#d7d7d7", linewidth=0.7, alpha=0.75)
    ratio.spines[["top", "right"]].set_visible(False)
    ratio.legend(frameon=False, ncols=2, loc="upper left")

    fig.suptitle(
        "Controller vs SharedFS dense crossover — 32 Workers x 16 cores\n"
        f"32 sizes x {len(controller_values[0])} randomized samples x "
        "4096 files/path/point",
        fontsize=15, fontweight="bold",
    )
    output = root / "small_file_crossover_dense_32x16.png"
    fig.savefig(output, dpi=190, facecolor="white")
    plt.close(fig)
    return output


def render_dense_stability(root):
    documents = [
        ("Controller /tmp", dense_documents(root, "controller")),
        ("SharedFS direct", dense_documents(root, "sharedfs")),
    ]
    fig, axis = plt.subplots(figsize=(15.5, 5.7), constrained_layout=True)
    maximum_index = 0
    campaign_boundaries = set()
    for label, path_documents in documents:
        grouped = dense_series(path_documents)
        medians = {
            size: statistics.median(
                phase["files_per_second"] for phase in phases
            )
            for size, phases in grouped.items()
        }
        ordered = sorted(
            (phase for phases in grouped.values() for phase in phases),
            key=lambda phase: phase["sequence_index"],
        )
        indices = [phase["sequence_index"] for phase in ordered]
        normalized = [phase["files_per_second"] / medians[phase["size_kib"]]
                      for phase in ordered]
        maximum_index = max(maximum_index, max(indices))
        offset = 0
        for document in path_documents[:-1]:
            offset += len(document["phases"])
            campaign_boundaries.add(offset + 0.5)
        axis.scatter(indices, normalized, s=22, alpha=0.62,
                     color=COLORS[label], label=label)
        window = 9
        rolling = [
            statistics.median(normalized[max(0, index - window + 1):index + 1])
            for index in range(len(normalized))
        ]
        axis.plot(indices, rolling, linewidth=1.8, color=COLORS[label],
                  label=f"{label} rolling median")
    axis.axhline(1.0, color="#333333", linestyle="--", linewidth=1.1)
    for boundary in sorted(campaign_boundaries):
        axis.axvline(boundary, color="#555555", linestyle=":", linewidth=1.1)
    axis.set_xlim(0, maximum_index + 1)
    axis.set_yscale("log")
    axis.set_ylim(0.08, 8.5)
    axis.set_yticks(
        [0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0],
        labels=["0.1", "0.25", "0.5", "1", "2", "4", "8"],
    )
    axis.set_xlabel("randomized phase order")
    axis.set_ylabel("throughput / same-size median")
    axis.set_title(
        "Long-run drift check after removing file-size effect",
        fontsize=14, fontweight="bold",
    )
    axis.grid(color="#d7d7d7", linewidth=0.7, alpha=0.75)
    axis.spines[["top", "right"]].set_visible(False)
    axis.legend(frameon=False, ncols=2, loc="upper right")
    output = root / "storage_crossover_phase_stability_32x16.png"
    fig.savefig(output, dpi=190, facecolor="white")
    plt.close(fig)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--matrix-root",
        type=Path,
        default=Path("acceptance/storage-matrix-20260901"),
    )
    parser.add_argument(
        "--crossover-root",
        type=Path,
        default=Path("acceptance/storage-crossover-20260901"),
    )
    args = parser.parse_args()
    outputs = [
        render_matrix(args.matrix_root),
        render_crossover(args.crossover_root, args.matrix_root),
    ]
    dense_inputs = [
        dense_result_path(args.crossover_root, "dense-uniform-controller.json"),
        dense_result_path(args.crossover_root, "dense-uniform-sharedfs.json"),
    ]
    if all(path.exists() for path in dense_inputs):
        outputs.extend([
            render_dense_crossover(args.crossover_root),
            render_dense_stability(args.crossover_root),
        ])
    for output in outputs:
        print(output)


if __name__ == "__main__":
    main()
