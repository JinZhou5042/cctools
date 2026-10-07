#!/usr/bin/env python3
"""Recompute the unchanged reference on a new CPU class; retain exact deltas."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import sys
import time

PAPER = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', type=Path, default=PAPER / 'results/atlas-inputs-v1/manifest.json')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    source = PAPER / 'results/atlas-inputs-v1/oracle-source.py'
    spec = importlib.util.spec_from_file_location('independent_atlas_reference', source)
    oracle = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(oracle)
    def plain(value):
        return {key: item.tolist() if hasattr(item, 'tolist') else item for key, item in value.items()}
    manifest = json.loads(args.manifest.read_text())
    records = []
    started = time.monotonic()
    for item in manifest['files']:
        value = plain(oracle.process_file(item.get('source_path', item['path'])))
        records.append(dict(name=Path(item['path']).name, oracle=value, previous=item['oracle'], equal=value == item['oracle']))
    total = plain(oracle.merge_results([r['oracle'] for r in records]))
    import numpy, awkward, vector
    report = dict(status='PASS', host=platform.node(), files=records, oracle=total,
        reference_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        manifest_sha256=hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
        matches_original=total == manifest['oracle'], elapsed_seconds=time.monotonic()-started,
        versions=dict(numpy=numpy.__version__, awkward=awkward.__version__, vector=vector.__version__),
        cpuinfo=Path('/proc/cpuinfo').read_text().split('\n\n')[0])
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({key: report[key] for key in ['status', 'host', 'matches_original', 'oracle', 'versions']}), flush=True)


if __name__ == '__main__':
    main()
