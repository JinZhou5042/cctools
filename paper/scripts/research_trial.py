#!/usr/bin/env python3
"""Research controls using the unchanged, fingerprinted paper trial engine.

Each invocation is a fresh interpreter. Adapter hooks select the application,
preloading, manager observations and allocated worker hosts.
"""
import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

import run_trial as base

PAPER, ROOT = base.PAPER, base.ROOT
DEPS = Path(os.environ.get('DATAVINE_RESEARCH_LOCAL_DEPS',
                          PAPER/'.deps/research-python3.10'))
# The old engine constructs PYTHONPATH from the first three entries.
sys.path[:0] = [str(DEPS), str(ROOT/'test_support/python_modules/python3'),
                str(PAPER/'.deps/python3.10')]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--backend', choices=['datavine','taskvine'], required=True)
    p.add_argument('--workload', default='noop')
    p.add_argument('--variant', required=True)
    p.add_argument('--queue', type=int, default=8)
    p.add_argument('--window', type=int, default=1)
    p.add_argument('--width', type=int, default=32)
    p.add_argument('--levels', type=int, default=4)
    p.add_argument('--bytes', type=int, default=262144)
    p.add_argument('--cpu-ms', type=float, default=100)
    p.add_argument('--workers', type=int, default=2)
    p.add_argument('--cores', type=int, default=4)
    p.add_argument('--memory', type=int)
    p.add_argument('--batch-type', choices=['local','reserved'], default='local')
    p.add_argument('--pool', type=Path)
    p.add_argument('--backup', default='worker-local')
    p.add_argument('--timeout', type=float, default=600)
    p.add_argument('--admission-timeout', type=float, default=120)
    p.add_argument('--repetition', type=int, default=1)
    p.add_argument('--trace', action='store_true')
    p.add_argument('--profile', action='store_true')
    p.add_argument('--cold', action='store_true',
                   help='disable TaskVine library module preloading')
    p.add_argument('--manifest', type=Path)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if args.memory is None:
        args.memory = 4096 if args.batch_type == 'reserved' else 8192
    args.output = args.output.resolve()
    if min(args.workers,args.cores,args.width,args.levels,args.bytes) < 1:
        p.error('positive task dimensions required')
    observations = {'preloaded':args.backend == 'taskvine' and not args.cold}
    manager_ref = []
    if args.workload == 'atlas':
        if args.manifest is None:
            p.error('--manifest is required for atlas')
        import atlas_application_v2 as application
        application.MANIFEST = json.loads(args.manifest.read_text())
        if application.MANIFEST['status'] != 'PASS':
            raise ValueError('dataset oracle is not accepted')
        for item in application.MANIFEST['files']:
            if Path(item['path']).stat().st_size != item['bytes']:
                raise ValueError('dataset file size changed')
        observations['manifest_sha256'] = hashlib.sha256(args.manifest.read_bytes()).hexdigest()
        base.kernels = application
        base.cloudpickle.register_pickle_by_value(application)
        class ApplicationWorkflow(base.Workflow):
            def python_callable(self, function, *values, **kwargs):
                kwargs['resources'] = {'cores':1,'memory_mb':512}
                return super().python_callable(function,*values,**kwargs)
        base.Workflow = ApplicationWorkflow
        original_call = base.vine.FuturesExecutor.future_funcall
        def application_call(executor,*values,**kwargs):
            task = original_call(executor,*values,**kwargs)
            set_memory = task.set_memory
            task.set_memory = lambda value: set_memory(512)
            return task
        base.vine.FuturesExecutor.future_funcall = application_call
    modules = ['hashlib','math','os','platform','tempfile','time','numpy']
    if args.workload == 'atlas':
        modules += ['awkward','uproot','vector']
    os.environ.update(OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1')
    hoisted = [] if args.cold else [importlib.import_module(name) for name in modules]
    original_create = base.vine.Manager.create_library_from_functions
    def create(manager, *functions, **kwargs):
        manager_ref.append(manager)
        base.vine.cvine.vine_enable_debug_log(str(args.output/'manager.debug'))
        kwargs['hoisting_modules'] = hoisted
        return original_create(manager, *functions, **kwargs)
    base.vine.Manager.create_library_from_functions = create
    original_validate = base.kernels.validate
    def validate(values, nodes):
        records = original_validate(values, nodes)
        if manager_ref:
            manager = manager_ref[-1]
            manager._refresh_stats()
            observations['manager_stats'] = {key:getattr(manager.stats,key) for key in
                ['tasks_submitted','tasks_dispatched','tasks_done','tasks_failed',
                 'time_send','time_receive','time_status_msgs','time_internal',
                 'time_scheduling','bytes_sent','bytes_received']}
        if args.workload == 'atlas':
            observations['scientific_output'] = base.cloudpickle.loads(values[0]['payload'])
        observations['validated_ns'] = time.monotonic_ns()
        return records
    # Keep the application's serialized callable module unmodified: the hook
    # belongs only to the driver's namespace.
    from types import SimpleNamespace
    base.kernels = SimpleNamespace(graph=base.kernels.graph,
        paper_kernel=base.kernels.paper_kernel, validate=validate)
    original_atomic = base.atomic_json
    def atomic(path, value):
        if Path(path).name == 'provenance.json':
            value['parameters']['manifest'] = str(args.manifest) if args.manifest else None
            value['parameters']['pool'] = str(args.pool) if args.pool else None
        return original_atomic(path, value)
    base.atomic_json = atomic
    def launch(port, config, scratch, env, logs):
        env = dict(env)
        allowed = sorted(os.sched_getaffinity(0))
        if len(allowed) < args.workers*args.cores:
            raise ValueError('insufficient disjoint local CPUs')
        observations['allocation'] = dict(
            hosts=[socket.getfqdn()]*args.workers, exclusive=False)
        processes = []
        for index in range(args.workers):
            log = (args.output/f'worker-{index}.stderr').open('w'); logs.append(log)
            command = [sys.executable,str(PAPER/'scripts/research_worker.py'),
                '--output',str(args.output/f'worker-{index}-usage.json'),'--cores',str(args.cores)]
            command += ['--cpus',','.join(map(str,allowed[index*args.cores:(index+1)*args.cores]))]
            if args.profile:
                command += ['--sample']
            command += ['--',str(ROOT/'taskvine/src/worker/vine_worker'),
                '--cores',str(args.cores),'--memory',str(args.memory),'--disk','8192',
                '--idle-timeout','120','-d','vine','-o',str(args.output/f'worker-{index}.debug'),
                socket.getfqdn(),str(port)]
            processes.append(subprocess.Popen(
                command,stdout=log,stderr=log,env=env,start_new_session=True))
        return processes
    if args.batch_type == 'local':
        base.launch_workers = launch
    # Parent directory exists; the trial engine owns creating the trial itself.
    args.output.parent.mkdir(parents=True,exist_ok=True)
    debug_path = args.output.parent/(args.output.name+'.manager.debug')
    base.vine.cvine.vine_enable_debug_log(str(debug_path))
    started = time.monotonic()
    if args.batch_type == 'reserved':
        import node_trial
        code = node_trial.run(args)
        observations['allocation'] = dict(
            workers=json.loads((args.output/'node-allocation.json').read_text()),
            scope='Persistent allocations on distinct physical hosts, eight CPU shares each and explicit affinity; not whole-node exclusivity.')
    else:
        code = base.run(args)
    result = json.loads((args.output/'result.json').read_text())
    graph = json.loads((args.output/'graph.json').read_text())
    observations['single_parent_tasks'] = sum(
        len(node['parents']) == 1 for node in graph['nodes'])
    observations['dependency_storage'] = (
        'node-local staged' if os.environ.get('DATAVINE_RESEARCH_LOCAL_DEPS')
        else 'shared filesystem')
    observations['trial_lifetime_seconds'] = time.monotonic()-started
    observations['client_cpu_seconds'] = time.process_time()
    usage = []
    for path in sorted(args.output.glob('worker-*-usage.json')):
        usage.append(json.loads(path.read_text()))
    observations['workers_usage'] = usage
    observations['metrics_scope'] = 'Manager file counters exclude peer traffic; worker CPU includes startup/shutdown; task CPU excludes deserialization. No total-cluster-byte claim.'
    result['research'] = observations
    if result['status'] == 'PASS' and len(usage) != args.workers:
        result.update(status='FAIL',error='missing terminal worker accounting')
        code = 1
    base.atomic_json(args.output/'result.json',result)
    print(json.dumps(dict(status=result['status'],variant=args.variant,
        elapsed=result.get('elapsed_seconds'),error=result.get('error'))),flush=True)
    return code


if __name__ == '__main__':
    raise SystemExit(main())
