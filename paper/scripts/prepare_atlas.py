#!/usr/bin/env python3
"""Pin the full cached public dataset and rerun the independent vector oracle."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    args.output.mkdir(parents=True)
    source = args.source / 'hyy_analysis.py'
    spec = importlib.util.spec_from_file_location('independent_hyy', source)
    oracle = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(oracle)
    manifest = dict(schema='datavine.atlas-inputs/v1', release='2025e-13tev-beta',
        source_url='https://github.com/atlas-outreach-data-tools/notebooks-collection-opendata/blob/master/13-TeV-examples/uproot_python/HyyAnalysis.ipynb',
        source_commit=subprocess.check_output(['git', '-C', str(args.source), 'rev-parse', 'HEAD'], text=True).strip(),
        oracle_source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        selection_note='Unchanged existing Hyy selection, including permissive eta OR predicate. Runtime equivalence only; no new physics claim.',
        files=[])
    import uproot
    for path in sorted((args.source / 'data').glob('*.root')):
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            for block in iter(lambda: stream.read(8*1024*1024), b''):
                digest.update(block)
        with uproot.open(path) as file:
            entries = file['analysis'].num_entries
        started = time.monotonic()
        value = oracle.process_file(str(path))
        value = {key: val.tolist() if hasattr(val, 'tolist') else val for key, val in value.items()}
        manifest['files'].append(dict(path=str(path.resolve()), bytes=path.stat().st_size,
            sha256=digest.hexdigest(), entries=entries, oracle=value,
            oracle_seconds=time.monotonic()-started))
        print(json.dumps(dict(file=path.name, entries=entries, selected=value['selected'])), flush=True)
    if len(manifest['files']) != 16:
        raise ValueError('full dataset requires all 16 ROOT files')
    total = oracle.merge_results([v['oracle'] for v in manifest['files']])
    total = {key: val.tolist() if hasattr(val, 'tolist') else val for key, val in total.items()}
    historical = json.loads((args.source/'hyy-results.json').read_text())
    if any(total[key] != historical[key] for key in total):
        raise ValueError('fresh independent oracle differs from existing historical scientific output')
    manifest['oracle'] = total
    manifest['total_bytes'] = sum(v['bytes'] for v in manifest['files'])
    manifest['status'] = 'PASS'
    (args.output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    (args.output/'oracle-source.py').write_bytes(source.read_bytes())
    print(json.dumps(dict(status='PASS', bytes=manifest['total_bytes'], entries=total['entries'])))


if __name__ == '__main__':
    main()
