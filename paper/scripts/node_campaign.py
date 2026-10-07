#!/usr/bin/env python3
"""Paired scaling on fixed physical hosts with source-pinned TCP orchestration."""
import hashlib
import research_campaign as campaign

original_run=campaign.subprocess.run
def run(command,*args,**kwargs):
    if len(command)>1 and command[1]==str(campaign.PAPER/'scripts/research_trial.py'):
        command=[command[0],str(campaign.PAPER/'scripts/node_trial.py'),*command[2:]]
    return original_run(command,*args,**kwargs)
campaign.subprocess.run=run
original_atomic=campaign.atomic
def atomic(path,value):
    if path.name=='plan.json':
        for name in ['node_campaign.py','node_trial.py','node_pool.py']:
            source=campaign.PAPER/'scripts'/name
            value['fingerprints'][str(source.relative_to(campaign.ROOT))]=hashlib.sha256(source.read_bytes()).hexdigest()
    original_atomic(path,value)
campaign.atomic=atomic
if __name__=='__main__':raise SystemExit(campaign.main())
