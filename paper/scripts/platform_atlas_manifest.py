#!/usr/bin/env python3
"""Build a new platform-specific manifest from an independent reference run.

Runtime outputs are never used as an oracle. Preserve the previous manifest
and require the reference source and staged dataset identities to match.
"""
import argparse
import copy
import hashlib
import json
from pathlib import Path

PAPER = Path(__file__).resolve().parents[1]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--oracle', type=Path, required=True)
    parser.add_argument('--inputs', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    report = json.loads(args.oracle.read_text())
    inputs = json.loads(args.inputs.read_text())
    if report['status'] != 'PASS' or sha(args.inputs) != report['manifest_sha256']:
        raise ValueError('independent reference does not match its input manifest')
    if report['reference_sha256'] != inputs['oracle_source_sha256']:
        raise ValueError('reference implementation changed')
    by_name = {item['name']: item for item in report['files']}
    if len(by_name) != 16 or set(by_name) != {Path(f['path']).name for f in inputs['files']}:
        raise ValueError('reference must process every pinned input exactly once')
    manifest = copy.deepcopy(inputs)
    manifest['previous_oracle'] = manifest['oracle']
    for item in manifest['files']:
        reference = by_name[Path(item['path']).name]['oracle']
        if reference['entries'] != item['entries']:
            raise ValueError('reference did not process all input events')
        item['oracle'] = reference
        item['path'] = item.get('original_path', item.get('source_path', item['path']))
    for key in ['histogram', 'cutflow']:
        summed = [sum(item['oracle'][key][i] for item in manifest['files'])
                  for i in range(len(report['oracle'][key]))]
        if summed != report['oracle'][key]:
            raise ValueError('reference reduction does not match per-file results')
    manifest['oracle'] = report['oracle']
    manifest['numerical_platform'] = dict(host=report['host'], versions=report['versions'],
        cpuinfo=report['cpuinfo'], oracle_report_sha256=sha(args.oracle),
        source_manifest_sha256=sha(args.inputs), matches_previous=report['matches_original'])
    args.output.write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps(dict(status='PASS', selected=manifest['oracle']['selected'],
                         matches_previous=report['matches_original'])))


if __name__ == '__main__':
    main()
