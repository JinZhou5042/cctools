#!/usr/bin/env python3
"""Use the existing staging contract with the corrected scientific kernel."""
import stage_application as stage
original=stage.subprocess.call
def call(command,*args,**kwargs):
    if len(command)>1 and command[1]==str(stage.PAPER/'scripts/upgrade_campaign.py'):
        command=[command[0],str(stage.PAPER/'scripts/atlas_campaign.py'),*command[2:]]
    return original(command,*args,**kwargs)
stage.subprocess.call=call
if __name__=='__main__':raise SystemExit(stage.main())
