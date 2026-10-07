#!/usr/bin/env python3
"""Stage immutable application inputs once and record the excluded setup cost."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

PAPER=Path(__file__).resolve().parents[1]


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--mode',choices=['application','controls','attribution'],required=True)
    args=p.parse_args()
    args.output=args.output.resolve()
    if args.output.exists():raise FileExistsError(args.output)
    record=args.output.with_suffix('.staging.json')
    started=time.monotonic()
    with tempfile.TemporaryDirectory(prefix='datavine-research-stage-') as temporary:
        directory=Path(temporary)
        deps=directory/'python'
        shutil.copytree(PAPER/'.deps/research-python3.10',deps)
        manifest=json.loads((PAPER/'results/atlas-inputs-v1/manifest.json').read_text())
        if args.mode=='application':
            for item in manifest['files']:
                source=Path(item['path'])
                target=directory/source.name
                shutil.copyfile(source,target)
                digest=hashlib.sha256()
                with target.open('rb') as stream:
                    for block in iter(lambda:stream.read(8*1024*1024),b''):digest.update(block)
                if digest.hexdigest()!=item['sha256']:raise ValueError('staged ROOT checksum differs')
                item['source_path']=item['path'];item['path']=str(target)
                print('staged '+source.name,flush=True)
            local_manifest=directory/'manifest.json'
            local_manifest.write_text(json.dumps(manifest,indent=2)+'\n')
        record.write_text(json.dumps(dict(status='PASS',setup_seconds=time.monotonic()-started,
            dataset_bytes=manifest['total_bytes'] if args.mode=='application' else 0,
            scope='Serial copy and verification excluded from workflow timer. Root reading, decompression, selections, intermediate transfer and durable sinks remain inside each trial. OS caches are not flushed.'),indent=2)+'\n')
        env=dict(os.environ,DATAVINE_RESEARCH_LOCAL_DEPS=str(deps))
        variants={'application':'dv-elastic,tv-stock',
                  'controls':'dv-elastic,tv-stock,tv-4c',
                  'attribution':'dv-elastic,dv-profile,tv-stock,tv-profile,dv-cold,tv-cold'}[args.mode]
        command=[os.sys.executable,str(PAPER/'scripts/upgrade_campaign.py'),
                 '--mode',args.mode,'--variants',variants,'--output',str(args.output)]
        if args.mode=='application':command+=['--manifest',str(local_manifest),'--timeout','600']
        return subprocess.call(command,env=env)


if __name__=='__main__':raise SystemExit(main())
