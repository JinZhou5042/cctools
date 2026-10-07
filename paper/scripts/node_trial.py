#!/usr/bin/env python3
"""Use the accepted scientific trial with TCP-controlled allocated workers."""
import json
import socket
import sys
import time
import run_trial as base
from node_pool import request

original_run=base.run
original_stop=base.stop


def run(args):
    contact=json.loads(args.pool.read_text())
    allocation=[]
    class Worker:
        def __init__(self,index):self.index=index
        def poll(self):
            done=request(contact,f'/status/{self.index}').get('done')
            return done['returncode'] if done else None
    def stop(process):
        if not isinstance(process,Worker):return original_stop(process)
        request(contact,f'/stop/{process.index}',{})
        deadline=time.monotonic()+45
        while process.poll() is None and time.monotonic()<deadline:time.sleep(.1)
        if process.poll() is None:raise RuntimeError('remote shutdown did not complete')
    def launch(port,config,scratch,env,logs):
        processes=[]
        for index in range(args.workers):
            ready=request(contact,f'/status/{index}')['ready']
            if ready['host'] in [v['host'] for v in allocation]:
                raise ValueError('duplicate physical host in scaling trial')
            allocation.append(ready)
            command=[sys.executable,str(base.PAPER/'scripts/research_worker.py'),
                '--output',str(args.output/f'worker-{index}-usage.json'),
                '--cores',str(args.cores),'--',str(base.ROOT/'taskvine/src/worker/vine_worker'),
                '--cores',str(args.cores),'--memory',str(args.memory),'--disk','8192',
                '--idle-timeout','120','-d','vine','-o',str(args.output/f'worker-{index}.debug'),
                socket.getfqdn(),str(port)]
            request(contact,f'/dispatch/{index}',dict(id=str(args.output),
                command=command,environment=env,log=str(args.output/f'worker-{index}.stderr')))
            processes.append(Worker(index))
        return processes
    base.launch_workers=launch
    base.stop=stop
    code=original_run(args)
    (args.output/'node-allocation.json').write_text(json.dumps(allocation,indent=2)+'\n')
    return code
