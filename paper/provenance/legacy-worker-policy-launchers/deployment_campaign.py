#!/usr/bin/env python3
"""Run verified deployment controls; reject source changes during a campaign."""
import hashlib
from pathlib import Path
import research_campaign as campaign

fingerprints={}
original_run=campaign.subprocess.run
def run(command,*args,**kwargs):
    if len(command)>1 and command[1]==str(campaign.PAPER/'scripts/research_trial.py'):
        for name,digest in fingerprints.items():
            if hashlib.sha256((campaign.ROOT/name).read_bytes()).hexdigest()!=digest:
                raise RuntimeError('campaign input changed: '+name)
        command=[command[0],str(campaign.PAPER/'scripts/deployment_trial.py'),*command[2:]]
    return original_run(command,*args,**kwargs)
campaign.subprocess.run=run
original_atomic=campaign.atomic
def atomic(path,value):
    if path.name=='plan.json':
        for name in ['deployment_campaign.py','deployment_trial.py','upgrade_trial.py',
                     'atlas_application_v2.py','node_trial.py','node_pool.py','stage_verified.py']:
            source=campaign.PAPER/'scripts'/name
            value['fingerprints'][str(source.relative_to(campaign.ROOT))]=hashlib.sha256(source.read_bytes()).hexdigest()
        fingerprints.update(value['fingerprints'])
    original_atomic(path,value)
campaign.atomic=atomic
if __name__=='__main__':raise SystemExit(campaign.main())
