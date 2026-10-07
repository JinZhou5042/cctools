#!/usr/bin/env python3
"""Run research trials with the installed Poncho executor."""
import json
from pathlib import Path
import sys
import research_trial

base=research_trial.base


def main():
    if '--workload' in sys.argv and sys.argv[sys.argv.index('--workload')+1]=='atlas':
        import atlas_application_v2
        sys.modules['atlas_application']=atlas_application_v2
    if '--batch-type' in sys.argv and sys.argv[sys.argv.index('--batch-type')+1]=='reserved':
        import node_trial
        base.run=node_trial.run
        if '--memory' not in sys.argv:sys.argv+=['--memory','4096']
        code=research_trial.main()
        output=Path(sys.argv[sys.argv.index('--output')+1])
        result=json.loads((output/'result.json').read_text())
        result['research']['allocation']=dict(workers=json.loads((output/'node-allocation.json').read_text()),
            scope='Persistent allocations on distinct physical hosts, eight CPU shares each and explicit affinity; not whole-node exclusivity.')
        base.atomic_json(output/'result.json',result)
        return code
    import upgrade_trial
    return upgrade_trial.main()


if __name__=='__main__':raise SystemExit(main())
