#!/usr/bin/env python3
"""Record inherited stdout and module/storage paths; diagnostic, not timing evidence."""
import os
import sys
import research_trial

original=research_trial.base.kernels.paper_kernel
def diagnostic(key,*args):
    result=original(key,*args)
    from pathlib import Path
    record=result['records'][key]
    record['cwd']=os.getcwd()
    record['stdout_path']=os.readlink('/proc/self/fd/1')
    record['stdout_prefix']=Path(record['stdout_path']).read_text(errors='replace')[:4096]
    record['module_paths']={name:getattr(module,'__file__',None) for name,module in sys.modules.copy().items()
                            if name in ['kernels','run_trial','numpy','cloudpickle','pathlib','hashlib','platform']}
    return result
research_trial.base.kernels.paper_kernel=diagnostic
if __name__=='__main__':raise SystemExit(research_trial.main())
