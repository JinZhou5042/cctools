#!/usr/bin/env python3
"""Full-data trial preserving the reference's floating-point operation order."""
import sys
import atlas_application_v2
sys.modules['atlas_application']=atlas_application_v2
import upgrade_trial
if __name__=='__main__':raise SystemExit(upgrade_trial.main())
