#!/usr/bin/env python3
"""Generate an explicit DataVine scientific-library preload experiment.

Uses the existing DATAVINE_PYTHON_EXECUTOR_PATH interface. No repository executor
is overwritten. Preloads happen before the executor's first fork and readiness
message, so callers cannot run against a partially initialized library.
"""
import hashlib,json
from pathlib import Path
PAPER=Path(__file__).resolve().parents[1];ROOT=PAPER.parent
modules=['hashlib','math','os','platform','tempfile','time','numpy']
base=ROOT/'taskvine/src/tools/datavine_executor'
source=base.read_text();marker='\nFRAME_LIMIT ='
assert source.count(marker)==1
block='''
# Paper experiment: initialize the same modules as the TaskVine hoisted library.
# OPENBLAS_NUM_THREADS and OMP_NUM_THREADS are set to 1 by the trial environment.
# This controlled module set is not a promise that arbitrary imports are fork-safe.
import importlib as _paper_importlib
for _paper_module in %r:
    _paper_importlib.import_module(_paper_module)
del _paper_module, _paper_importlib
''' % modules
source=source.replace(marker,block+marker)
out=PAPER/'implementation/datavine_python_preloaded_executor'
out.write_text(source);out.chmod(0o755)
(PAPER/'implementation/preload-provenance.json').write_text(json.dumps(dict(base=str(base.relative_to(ROOT)),base_sha256=hashlib.sha256(base.read_bytes()).hexdigest(),generated_sha256=hashlib.sha256(out.read_bytes()).hexdigest(),modules=modules,mechanism='initialize explicit modules before first fork and readiness; existing executor-path override'),indent=2)+'\n')
