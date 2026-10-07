#!/usr/bin/env python3
"""Run the immutable campaign engine with a fingerprinted deployment adapter."""
import hashlib
from pathlib import Path
import research_campaign as campaign

original_run=campaign.subprocess.run
def run(command,*args,**kwargs):
    if len(command)>1 and command[1]==str(campaign.PAPER/'scripts/research_trial.py'):
        command=[command[0],str(campaign.PAPER/'scripts/upgrade_trial.py'),*command[2:]]
    return original_run(command,*args,**kwargs)
campaign.subprocess.run=run
original_atomic=campaign.atomic
def atomic(path,value):
    if path.name=='plan.json':
        for name in ['upgrade_campaign.py','upgrade_trial.py','stage_application.py']:
            source=campaign.PAPER/'scripts'/name
            value['fingerprints'][str(source.relative_to(campaign.ROOT))]=hashlib.sha256(source.read_bytes()).hexdigest()
    original_atomic(path,value)
campaign.atomic=atomic

if __name__=='__main__':raise SystemExit(campaign.main())
