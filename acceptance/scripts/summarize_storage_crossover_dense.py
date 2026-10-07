#!/usr/bin/env python3
"""Validate and summarize the retained dense storage-crossover campaigns."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics


CAMPAIGNS = ("dense-uniform", "dense-confirm")
MODES = ("controller", "sharedfs")


def result_path(root, name):
    organized = root / "raw" / "results" / name
    return organized if organized.exists() else root / "raw" / name


def load_campaigns(root):
    loaded = {}
    for campaign in CAMPAIGNS:
        paths = {
            mode: result_path(root, f"{campaign}-{mode}.json")
            for mode in MODES
        }
        exists = {mode: path.exists() for mode, path in paths.items()}
        if len(set(exists.values())) != 1:
            raise RuntimeError(f"unpaired campaign inputs: {paths}")
        if all(exists.values()):
            loaded[campaign] = {
                mode: json.loads(path.read_text())
                for mode, path in paths.items()
            }
    if not loaded:
        raise RuntimeError("no paired dense campaigns found")
    return loaded


def correlation(left, right):
    left_mean = statistics.mean(left)
    right_mean = statistics.mean(right)
    numerator = sum(
        (x - left_mean) * (y - right_mean) for x, y in zip(left, right)
    )
    denominator = (
        sum((x - left_mean) ** 2 for x in left)
        * sum((y - right_mean) ** 2 for y in right)
    ) ** 0.5
    return numerator / denominator if denominator else 0.0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path,
        default=Path("acceptance/storage-crossover-20260901"),
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    campaigns = load_campaigns(root)

    samples = {mode: defaultdict(list) for mode in MODES}
    aggregates = {mode: [] for mode in MODES}
    sequence = {mode: 0 for mode in MODES}
    total_files = {mode: 0 for mode in MODES}
    total_bytes = {mode: 0 for mode in MODES}
    topology = None
    for campaign, documents in campaigns.items():
        controller = documents["controller"]
        sharedfs = documents["sharedfs"]
        if controller["plan_seed"] != sharedfs["plan_seed"]:
            raise RuntimeError(f"plan seed mismatch in {campaign}")
        for mode, document in documents.items():
            if document["status"] != "PASS":
                raise RuntimeError(f"{campaign} {mode} is not PASS")
            current_topology = (
                document["workers"], document["cores_per_worker"],
                document["available_cores"], document["files_per_phase"],
            )
            if topology is None:
                topology = current_topology
            elif topology != current_topology:
                raise RuntimeError("dense campaign topology mismatch")
            if document["phase_count"] != len(document["phases"]):
                raise RuntimeError(f"phase count mismatch in {campaign} {mode}")
            total_files[mode] += sum(phase["files"] for phase in document["phases"])
            total_bytes[mode] += sum(phase["total_bytes"] for phase in document["phases"])
            for phase in document["phases"]:
                sequence[mode] += 1
                samples[mode][phase["size_kib"]].append({
                    "campaign": campaign,
                    "round": phase["round"],
                    "sequence": sequence[mode],
                    "files_per_second": phase["files_per_second"],
                    "mib_per_second": phase["mib_per_second"],
                })
            for round_index in range(1, document["rounds"] + 1):
                phases = [
                    phase for phase in document["phases"]
                    if phase["round"] == round_index
                ]
                elapsed = sum(phase["execution_seconds"] for phase in phases)
                aggregates[mode].append({
                    "campaign": campaign,
                    "round": round_index,
                    "files_per_second": sum(phase["files"] for phase in phases) / elapsed,
                    "mib_per_second": sum(phase["total_bytes"] for phase in phases)
                    / elapsed / 1048576,
                })

    sizes = sorted(samples["controller"])
    if sizes != sorted(samples["sharedfs"]):
        raise RuntimeError("Controller and SharedFS size grids differ")
    per_size = {}
    winner_states = []
    expected_samples = None
    for size in sizes:
        controller_records = samples["controller"][size]
        sharedfs_records = samples["sharedfs"][size]
        controller_by_key = {
            (record["campaign"], record["round"]): record
            for record in controller_records
        }
        sharedfs_by_key = {
            (record["campaign"], record["round"]): record
            for record in sharedfs_records
        }
        if controller_by_key.keys() != sharedfs_by_key.keys():
            raise RuntimeError(f"unpaired samples at {size} KiB")
        if expected_samples is None:
            expected_samples = len(controller_records)
        if len(controller_records) != expected_samples:
            raise RuntimeError(f"sample count mismatch at {size} KiB")
        controller_rates = [record["files_per_second"] for record in controller_records]
        sharedfs_rates = [record["files_per_second"] for record in sharedfs_records]
        ratios = [
            sharedfs_by_key[key]["files_per_second"]
            / controller_by_key[key]["files_per_second"]
            for key in sorted(controller_by_key)
        ]
        controller_median = statistics.median(controller_rates)
        sharedfs_median = statistics.median(sharedfs_rates)
        sharedfs_wins = sum(ratio > 1.0 for ratio in ratios)
        winner = "sharedfs" if sharedfs_median > controller_median else "controller"
        winner_states.append(winner)
        per_size[f"{size}_kib"] = {
            "controller_files_per_second": controller_rates,
            "controller_median_files_per_second": controller_median,
            "controller_range_files_per_second": [min(controller_rates), max(controller_rates)],
            "sharedfs_files_per_second": sharedfs_rates,
            "sharedfs_median_files_per_second": sharedfs_median,
            "sharedfs_range_files_per_second": [min(sharedfs_rates), max(sharedfs_rates)],
            "paired_sharedfs_over_controller": ratios,
            "paired_median_ratio": statistics.median(ratios),
            "sharedfs_paired_wins": sharedfs_wins,
            "samples": len(ratios),
            "median_winner": winner,
        }

    transitions = [
        sizes[index] for index in range(1, len(sizes))
        if winner_states[index] != winner_states[index - 1]
    ]
    drift = {}
    for mode in MODES:
        size_medians = {
            size: statistics.median(
                record["files_per_second"] for record in records
            )
            for size, records in samples[mode].items()
        }
        ordered = sorted(
            (record | {"size_kib": size}
             for size, records in samples[mode].items() for record in records),
            key=lambda record: record["sequence"],
        )
        normalized = [
            record["files_per_second"] / size_medians[record["size_kib"]]
            for record in ordered
        ]
        drift[mode] = {
            "sequence_vs_size_normalized_throughput_correlation": correlation(
                [record["sequence"] for record in ordered], normalized
            ),
            "campaign_round_aggregate": aggregates[mode],
        }

    workers, cores, available_cores, files_per_phase = topology
    result = {
        "artifact_type": "datavine-dense-storage-crossover-summary",
        "status": "PASS",
        "campaigns": list(campaigns),
        "topology": {
            "workers": workers,
            "cores_per_worker": cores,
            "available_cores": available_cores,
            "files_per_phase": files_per_phase,
        },
        "sizes_kib": sizes,
        "uniform_spacing_kib": sizes[1] - sizes[0],
        "samples_per_size_per_path": expected_samples,
        "phases_per_path": len(sizes) * expected_samples,
        "total_files_per_path": total_files,
        "total_bytes_per_path": total_bytes,
        "per_size": per_size,
        "median_winner_transition_sizes_kib": transitions,
        "median_winner_transition_count": len(transitions),
        "unanimous_controller_sizes_kib": [
            size for size in sizes
            if per_size[f"{size}_kib"]["sharedfs_paired_wins"] == 0
        ],
        "unanimous_sharedfs_sizes_kib": [
            size for size in sizes
            if per_size[f"{size}_kib"]["sharedfs_paired_wins"] == expected_samples
        ],
        "drift": drift,
        "conclusion": {
            "single_monotonic_crossover_observed": len(transitions) == 1,
            "hard_magic_size_supported": False,
        },
    }
    output = args.output.resolve() if args.output else root / "dense-summary.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(output)


if __name__ == "__main__":
    main()
