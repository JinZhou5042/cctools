#!/usr/bin/env python3
"""One exact, fail-closed DataVine/TaskVine scientific DAG trial.

Run with the configured DataVine Python. Each backend gets a fresh service and
the full requested Worker pool before timing. Results are never overwritten.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback

PAPER = Path(__file__).resolve().parents[1]
ROOT = PAPER.parent
sys.path[:0] = [str(PAPER / '.deps/python3.10'), str(ROOT / 'test_support/python_modules/python3'),
                str(ROOT / 'acceptance/scripts')]
import cloudpickle
import ndcctools.taskvine as vine
from ndcctools.taskvine.datavine import Workflow
from ndcctools.taskvine.datavine.workflow_client import WorkflowClient
from benchmark_static_scale import worker_inventory
from compare_workflows import physical_counts, runtime_stages
from benchmark_adaptive_concurrency import parse_window_trace
import kernels

cloudpickle.register_pickle_by_value(kernels)


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    os.replace(temporary, path)


def stop(process):
    if process is None or process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=35)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)


def launch_workers(port, args, scratch, env, logs):
    binary = ROOT / 'taskvine/src/worker/vine_worker'
    wrapper = PAPER / 'scripts/pinned_worker.py'
    env = dict(env, DATAVINE_PAPER_WORKER_BINARY=str(binary), DATAVINE_PAPER_WORKER_CORES=str(args.cores))
    if args.batch_type == 'local':
        processes = []
        allowed = sorted(os.sched_getaffinity(0))
        if len(allowed) < args.workers * args.cores:
            raise RuntimeError('insufficient distinct CPUs for local worker pool')
        for index in range(args.workers):
            stream = (scratch / f'worker-{index}.stderr').open('w')
            logs.append(stream)
            command = [sys.executable, str(wrapper), '--cores', str(args.cores), '--memory', str(args.memory),
                       '--disk', '4096', '--idle-timeout', '120', '-d', 'vine',
                       '-o', str(scratch / f'worker-{index}.debug'), 'localhost', str(port)]
            processes.append(subprocess.Popen(command, stdout=stream, stderr=stream,
                                               env=dict(env, DATAVINE_PAPER_CPUSET=','.join(map(str,allowed[index*args.cores:(index+1)*args.cores]))), start_new_session=True))
        return processes
    stream = (scratch / 'factory.log').open('w')
    logs.append(stream)
    command = [shutil.which('vine_factory', path=env['PATH']), '--batch-type', 'condor',
               '--min-workers', str(args.workers), '--max-workers', str(args.workers),
               '--workers-per-cycle', str(args.workers), '--factory-period', '1',
               '--factory-timeout', str(int(args.admission_timeout + args.timeout + 60)),
               '--timeout', '120', '--cores', str(args.cores), '--memory', str(args.memory),
               '--disk', '4096', '--gpus', '0', '--debug-workers', '--parent-death',
               '--worker-binary', str(wrapper), '--scratch-dir', str(scratch / 'factory')]
    for key in ('DATAVINE_PAPER_WORKER_BINARY', 'DATAVINE_PAPER_WORKER_CORES', 'PATH', 'PYTHONPATH', 'PYTHONNOUSERSITE', 'PYTHONDONTWRITEBYTECODE',
                'DATAVINE_TRACE_ATTEMPTS', 'OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS'):
        command += ['--env', f'{key}={env[key]}']
    command += [socket.getfqdn(), str(port)]
    return [subprocess.Popen(command, stdout=stream, stderr=stream, env=env,
                             start_new_session=True)]


def wait_pool(port, args, processes, manager=None):
    started = time.monotonic()
    while time.monotonic() - started < args.admission_timeout:
        if any(p.poll() is not None for p in processes):
            raise RuntimeError('worker/factory exited before full admission')
        if manager is not None:
            manager.wait(1)
            manager._refresh_stats()
            if manager.stats.workers_connected == args.workers and manager.stats.total_cores == args.workers * args.cores:
                return time.monotonic() - started
        else:
            inventory = worker_inventory(ROOT / 'taskvine/src/tools/vine_status', port)
            if inventory == [args.cores] * args.workers:
                return time.monotonic() - started
            time.sleep(0.1)
    raise TimeoutError('full requested pool was not admitted; not a performance sample')


def attempt_trace(paths):
    pattern = re.compile(r'datavine-attempt task=(\d+) received_us=(\d+) '
                         r'prepare_start_us=(\d+) prepare_ready_us=(\d+) '
                         r'execute_start_us=(\d+) complete_us=(\d+) result=(\d+)')
    keys = ('task', 'received_us', 'prepare_start_us', 'prepare_ready_us',
            'execute_start_us', 'complete_us', 'result')
    result = []
    for index, path in enumerate(paths):
        for line in path.read_text(errors='replace').splitlines():
            match = pattern.search(line)
            if match:
                row = dict(zip(keys, map(int, match.groups())))
                row['worker_log'] = index
                result.append(row)
    return result


def run(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    spec, sinks = kernels.graph(args.workload, args.width, args.levels, args.bytes, args.cpu_ms)
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path[:3]),
               PYTHONNOUSERSITE='1', PYTHONDONTWRITEBYTECODE='1',
               PATH=f'{Path(sys.executable).parent}:{os.environ["PATH"]}',
               DATAVINE_WORKFLOW_METRICS='1',
               DATAVINE_FUNCTION_QUEUE_MULTIPLIER=str(args.queue),
               DATAVINE_TRACE_ATTEMPTS='1' if args.trace else '0',
               OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1')
    # TaskVine's generated Python library inherits the current process env.
    os.environ.update(env)
    provenance = dict(host=platform.node(), python=sys.version, executable=sys.executable,
                      imported_bindings=vine.__file__, parameters=vars(args).copy(),
                      head=subprocess.check_output(['git','-C',str(ROOT),'rev-parse','HEAD'],text=True).strip(),
                      affinity=sorted(os.sched_getaffinity(0)),
                      timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                      binaries={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
                                for p in [ROOT/'taskvine/src/tools/datavine_workflow',ROOT/'taskvine/src/worker/vine_worker']})
    provenance['parameters']['output'] = str(output)
    atomic_json(output / 'provenance.json', provenance)
    atomic_json(output / 'graph.json', dict(nodes=spec, sinks=sinks))
    result = {'status':'FAIL', 'backend':args.backend, 'workload':args.workload,
              'variant':args.variant, 'repetition':args.repetition,
              'tasks':len(spec), 'workers':args.workers, 'cores':args.cores,
              'batch_type':args.batch_type, 'trace_enabled':args.trace, 'worker_cpu_limit':'explicit affinity to declared cores',
              'intermediate_policy':args.backup,
              'requested_output_policy':'all sinks fetched; local fsync before acceptance'}
    service = client = executor = None
    processes, logs = [], []
    original = Path.cwd()
    with tempfile.TemporaryDirectory(prefix='datavine-paper-trial-') as name:
        scratch = Path(name)
        try:
            os.chdir(scratch)
            if args.backend == 'datavine':
                stream = (scratch / 'service.log').open('w'); logs.append(stream)
                service = subprocess.Popen([str(ROOT/'taskvine/src/tools/datavine_workflow'),
                    'serve',str(scratch/'journal'),'paper-token'],stdout=subprocess.PIPE,
                    stderr=stream,text=True,env=env,start_new_session=True)
                if not select.select([service.stdout],[],[],30)[0]:
                    raise TimeoutError('service startup contact timeout')
                contact = json.loads(service.stdout.readline())
                port = contact['manager_port']
                client = WorkflowClient(contact['endpoint'],'paper-token')
            else:
                executor = vine.FuturesExecutor(port=0,factory=False)
                manager = executor.manager
                for key in ('transactions-log-enabled','taskgraph-log-enabled','performance-log-enabled'):
                    manager.tune(key,0)
                library = manager.create_library_from_functions('paper-functions',kernels.paper_kernel,
                                                                add_env=False,exec_mode='fork')
                if args.window > 1:
                    # Experimental overcommit control, not stock TaskVine:
                    # remove only the generated fork-library startup restriction.
                    # Repository Poncho sources remain unchanged. The Worker
                    # retains its real CPU/memory allocation and enforces slots.
                    generated = list(Path(manager.cache_directory).rglob('library_code.py'))
                    if len(generated) != 1:
                        raise RuntimeError('ambiguous generated library for overcommit baseline')
                    source = generated[0].read_text()
                    guard = 'args.function_slots > args.library_cores\n        and library_exec_method != "direct"'
                    if source.count(guard) != 1:
                        raise RuntimeError('upstream library guard changed; review baseline extension')
                    extended = scratch / 'paper_overcommit_library.py'
                    extended.write_text(source.replace(guard, 'False  # paper-only fixed overcommit control'))
                    library = manager.create_library_from_serverized_files('paper-functions', str(extended))
                    library.add_input(manager.declare_file(str(generated[0].with_name('library_info.clpk')), cache=True), 'library_info.clpk')
                    library.set_function_exec_mode_from_string('fork')
                    result['baseline_extension'] = 'generated-library fork slot guard disabled; unchanged physical allocation'
                    result['baseline_library_sha256'] = hashlib.sha256(extended.read_bytes()).hexdigest()
                library.set_cores(args.cores)
                library.set_function_slots(args.cores * args.window)
                manager.install_library(library)
                port = manager.port
            processes = launch_workers(port,args,scratch,env,logs)
            result['admission_seconds'] = wait_pool(port,args,processes,
                                                    None if executor is None else executor.manager)
            refs = {}
            started = time.monotonic()
            if args.backend == 'datavine':
                workflow = Workflow('paper',workflow_id='paper',maximum_tasks=len(spec),
                                    maximum_edges=sum(len(n['parents']) for n in spec),
                                    idata_backup=args.backup)
                for node in spec:
                    refs[node['key']] = workflow.python_callable(kernels.paper_kernel,node['key'],
                        node['kind'],node['size'],node['cpu_ms'],node['seed'],
                        *(refs[key] for key in node['parents']),resources={'cores':1,'memory_mb':16})
                workflow.request(*(refs[key] for key in sinks))
                workflow.submit(client)
                registered = time.monotonic()
                deadline = started + args.timeout
                while time.monotonic() < deadline:
                    state = client.describe_workflow('paper')
                    result['terminal_snapshot'] = state
                    if state['state'] in ('completed','failed','cancelled'):
                        break
                    time.sleep(0.02)
                else:
                    raise TimeoutError('workflow execution deadline exceeded')
                if state['state'] != 'completed':
                    raise RuntimeError(state)
                values = [cloudpickle.loads(client.fetch_workflow_result('paper',refs[k].data_id)) for k in sinks]
            else:
                for node in spec:
                    task = executor.future_funcall('paper-functions','paper_kernel',node['key'],
                        node['kind'],node['size'],node['cpu_ms'],node['seed'],
                        *(refs[key] for key in node['parents']))
                    task.set_cores(1)
                    task.set_memory(16)
                    refs[node['key']] = executor.submit(task)
                registered = time.monotonic()
                values = [refs[key].result(timeout=max(1,int(args.timeout-(time.monotonic()-started)))) for key in sinks]
            # Both measured intervals include durable materialization of fetched
            # sink values. DataVine additionally fsyncs its Controller result.
            for index,value in enumerate(values):
                with (scratch / f'sink-{index}.pkl').open('wb') as stream:
                    cloudpickle.dump(value,stream); stream.flush(); os.fsync(stream.fileno())
            finished = time.monotonic()
            records = kernels.validate(values,spec)
            if any(len(r['cpuset']) != args.cores for r in records.values()):
                raise ValueError('executed task CPU affinity differs from Worker promise')
            execution_cpus = {}
            for record in records.values():
                execution_cpus.setdefault(record['host'],set()).update(record['cpuset'])
            result['observed_execution_cpus'] = {host:sorted(cpus) for host,cpus in execution_cpus.items()}

            result.update(status='PASS',elapsed_seconds=finished-started,
                          registration_seconds=registered-started,tasks_per_second=len(spec)/(finished-started),
                          logical_identities=len(records),result_digest=[v['digest'] for v in values],
                          payload_sha256=[v['payload_sha256'] for v in values],
                          task_body_cpu_seconds=sum(r['cpu_ns'] for r in records.values())/1e9,
                          local_io_bytes=sum(r['local_io_bytes'] for r in records.values()),
                          logical_input_bytes=sum(r['input_bytes'] for r in records.values()),
                          hosts=sorted({r['host'] for r in records.values()}))
            atomic_json(output/'tasks.json',records)
            if args.backend == 'datavine':
                for log in logs:log.flush()
                result['physical_counts'] = physical_counts(scratch/'service.log','paper')
                if result['physical_counts'] != {'submissions':len(spec),'completions':len(spec)}:
                    raise ValueError('physical task counts differ')
                result['runtime'] = runtime_stages(scratch/'service.log','paper')
            else:
                executor.manager._refresh_stats()
                result['manager_stats'] = {key:getattr(executor.manager.stats,key) for key in
                    ('tasks_submitted','tasks_done','tasks_failed','workers_connected')}
                # Library tasks are implementation overhead; user task identity
                # is checked separately through all kernel records.
        except BaseException as error:
            result.update(status='FAIL',error=str(error),traceback=traceback.format_exc())
        finally:
            for process in reversed(processes):stop(process)
            if client is not None:client.close()
            stop(service)
            if executor is not None:executor.manager.__del__()
            for log in logs:log.close()
            debug_paths = sorted(p for p in scratch.rglob('*') if p.is_file() and
                                 ('debug' in p.name or p.suffix == '.debug' or re.match(r'worker[.-]\d+\.log$', p.name)))
            result['window_trace'] = parse_window_trace(debug_paths)
            result['attempt_trace'] = attempt_trace(debug_paths)
            for index,path in enumerate(debug_paths):
                if result['status'] != 'PASS':shutil.copy2(path,output/f'worker-{index}-failure.log')
                lines = [l for l in path.read_text(errors='replace').splitlines()
                         if 'adaptive-window ' in l or 'datavine-attempt ' in l]
                if lines:(output/f'worker-{index}-metrics.log').write_text('\n'.join(lines)+'\n')
            for filename in ('service.log','factory.log'):
                if (scratch/filename).exists():shutil.copy2(scratch/filename,output/filename)
            os.chdir(original)
    atomic_json(output/'result.json',result)
    print(json.dumps({k:result.get(k) for k in ('status','backend','workload','variant','elapsed_seconds','error')}),flush=True)
    return 0 if result['status']=='PASS' else 1


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--backend',choices=('datavine','taskvine'),default='datavine')
    p.add_argument('--workload',choices=('spectral','histogram','quadrature','phase','disk','cpu','noop','sleep'),default='phase')
    p.add_argument('--variant',default='elastic')
    p.add_argument('--queue',type=int,default=8);p.add_argument('--window',type=int,default=1)
    p.add_argument('--width',type=int,default=32);p.add_argument('--levels',type=int,default=4)
    p.add_argument('--bytes',type=int,default=262144);p.add_argument('--cpu-ms',type=float,default=50)
    p.add_argument('--workers',type=int,default=2);p.add_argument('--cores',type=int,default=4)
    p.add_argument('--memory',type=int,default=2048)
    p.add_argument('--batch-type',choices=('local','condor'),default='local')
    p.add_argument('--backup',choices=('worker-local','controller-background'),default='worker-local')
    p.add_argument('--timeout',type=float,default=300);p.add_argument('--admission-timeout',type=float,default=180)
    p.add_argument('--repetition',type=int,default=1);p.add_argument('--trace',action='store_true')
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    if min(args.queue,args.window,args.width,args.levels,args.bytes,args.workers,args.cores,args.memory)<1:
        p.error('positive sizes and capacities required')
    if args.backend=='taskvine' and args.backup!='worker-local':
        p.error('TaskVine baseline has no equivalent Controller backup; use worker-local')
    return run(args)


if __name__=='__main__':
    raise SystemExit(main())
