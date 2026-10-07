#!/usr/bin/env python3
"""Stage exact inputs before a platform-specific independent reference run."""
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix='datavine-oracle-stage-') as temporary:
        staging = Path(temporary)
        shutil.copytree(PAPER / '.deps/research-python3.10', staging / 'python')
        manifest = json.loads((PAPER / 'results/atlas-inputs-v1/manifest.json').read_text())
        for item in manifest['files']:
            source = Path(item['path'])
            target = staging / source.name
            shutil.copyfile(source, target)
            digest = hashlib.sha256()
            with target.open('rb') as stream:
                for block in iter(lambda: stream.read(8*1024*1024), b''):
                    digest.update(block)
            if digest.hexdigest() != item['sha256']:
                raise RuntimeError('staged ROOT input differs')
            item['original_path'], item['path'] = str(source), str(target)
            print('staged ' + source.name, flush=True)
        local_manifest = args.output.with_suffix('.inputs.json')
        local_manifest.write_text(json.dumps(manifest, indent=2) + '\n')
        env = dict(os.environ, PYTHONPATH=str(staging / 'python') + os.pathsep + str(PAPER / '.deps/python3.10'),
                   OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1')
        args.output.with_suffix('.staging.json').write_text(json.dumps(dict(
            status='PASS', setup_seconds=time.monotonic()-started, dataset_bytes=manifest['total_bytes']), indent=2)+'\n')
        return subprocess.call([sys.executable, str(PAPER / 'scripts/oracle_on_host.py'),
                                '--manifest', str(local_manifest), '--output', str(args.output)], env=env)


if __name__ == '__main__':
    raise SystemExit(main())
