#!/usr/bin/env python3
"""Fresh, reproducible entry point for the four research-upgrade campaigns."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

PAPER = Path(__file__).resolve().parents[1]
ROOT = PAPER.parent
VARIANTS = {
    'controls': 'dv-elastic,tv-stock,tv-4c',
    'application': 'dv-elastic,tv-stock',
    'attribution': 'dv-elastic,dv-profile,tv-stock,tv-profile,tv-cold',
    'scaling': 'dv-elastic,tv-stock',
}


def sha(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind', choices=VARIANTS, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, default=PAPER / 'results/atlas-inputs-v1/manifest.json')
    parser.add_argument('--pool', type=Path)
    parser.add_argument('--repetitions', type=int, default=5)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error('repetitions must be positive')
    if args.kind == 'scaling' and args.pool is None:
        parser.error('scaling requires a contact file for an admitted distinct-host pool')
    output = args.output.resolve()
    if output.exists() or output.with_suffix('.staging.json').exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix='datavine-upgrade-') as temporary:
        staging = Path(temporary)
        env = dict(os.environ, OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1',
                   PYTHONNOUSERSITE='1', PYTHONDONTWRITEBYTECODE='1')
        env['PATH'] = str(Path(sys.executable).parent) + os.pathsep + env.get('PATH', '')
        env['PYTHONPATH'] = os.pathsep.join(map(str, [ROOT / 'test_support/python_modules/python3',
            PAPER / '.deps/research-python3.10', PAPER / '.deps/python3.10']))
        env.pop('DATAVINE_RESEARCH_LOCAL_DEPS', None)
        record = dict(kind=args.kind, launcher_sha256=sha(Path(__file__)), dataset_bytes=0,
                      scope='Setup excluded; complete graph through fetched and fsynced sinks timed. OS caches are not flushed.')
        if args.kind != 'scaling':
            deps = staging / 'python'
            shutil.copytree(PAPER / '.deps/research-python3.10', deps)
            env['DATAVINE_RESEARCH_LOCAL_DEPS'] = str(deps)
            record['dependencies'] = 'node-local staged'
        else:
            record['dependencies'] = 'shared filesystem on remote Workers'
        command = [sys.executable, str(PAPER / 'scripts/research_campaign.py'),
            '--mode', args.kind,
            '--variants', VARIANTS[args.kind], '--output', str(output),
            '--repetitions', str(args.repetitions)]
        if args.kind == 'application':
            manifest = json.loads(args.manifest.read_text())
            if manifest['status'] != 'PASS':
                raise RuntimeError('input oracle has not passed')
            record['original_manifest_sha256'] = sha(args.manifest)
            for item in manifest['files']:
                source = Path(item.get('source_path', item['path']))
                target = staging / source.name
                shutil.copyfile(source, target)
                if sha(target) != item['sha256']:
                    raise RuntimeError('ROOT input changed: ' + source.name)
                item['source_path'], item['path'] = str(source), str(target)
                record['dataset_bytes'] += item['bytes']
            # Retain the exact remapped manifest even after staged files expire.
            local_manifest = output.with_suffix('.inputs.json')
            local_manifest.write_text(json.dumps(manifest, indent=2) + '\n')
            command += ['--manifest', str(local_manifest)]
        if args.kind == 'scaling':
            command += ['--workers', '1,2,4,8', '--cores', '8', '--batch-type', 'reserved',
                        '--pool', str(args.pool.resolve())]
        record.update(setup_seconds=time.monotonic() - started, command=command, status='PASS')
        output.with_suffix('.staging.json').write_text(json.dumps(record, indent=2) + '\n')
        return subprocess.call(command, env=env)


if __name__ == '__main__':
    raise SystemExit(main())
