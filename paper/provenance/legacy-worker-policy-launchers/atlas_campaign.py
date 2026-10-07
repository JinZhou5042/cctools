#!/usr/bin/env python3
"""Versioned ATLAS application correction with unchanged acceptance checks."""
import hashlib
import upgrade_campaign
campaign=upgrade_campaign.campaign
original_run=campaign.subprocess.run
def run(command,*args,**kwargs):
    if len(command)>1 and command[1]==str(campaign.PAPER/'scripts/research_trial.py'):
        command=[command[0],str(campaign.PAPER/'scripts/atlas_trial.py'),*command[2:]]
    return original_run(command,*args,**kwargs)
campaign.subprocess.run=run
original_atomic=campaign.atomic
def atomic(path,value):
    if path.name=='plan.json':
        for name in ['atlas_application_v2.py','atlas_trial.py','atlas_campaign.py','stage_atlas_v2.py']:
            source=campaign.PAPER/'scripts'/name
            value['fingerprints'][str(source.relative_to(campaign.ROOT))]=hashlib.sha256(source.read_bytes()).hexdigest()
    original_atomic(path,value)
campaign.atomic=atomic
if __name__=='__main__':raise SystemExit(campaign.main())
