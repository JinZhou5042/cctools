#!/usr/bin/env python3
"""Execute serial trial workers inside a persistent, owned batch allocation."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time


def atomic(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value,indent=2)+'\n')
    temporary.replace(path)


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--pool',type=Path,required=True)
    p.add_argument('--index',type=int,required=True)
    args=p.parse_args()
    directory=args.pool/str(args.index)
    directory.mkdir(parents=True,exist_ok=True)
    atomic(directory/'ready.json',dict(host=socket.getfqdn(),pid=os.getpid(),
        affinity=sorted(os.sched_getaffinity(0)),started=time.time(),
        condor_ad=Path(os.environ['_CONDOR_JOB_AD']).read_text()))
    seen=set()
    deadline=time.monotonic()+7200
    child=None
    def stop(signum,frame):
        if child and child.poll() is None:
            os.killpg(child.pid,signal.SIGTERM)
        raise SystemExit(128+signum)
    signal.signal(signal.SIGTERM,stop)
    try:
        while time.monotonic()<deadline and not (args.pool/'STOP').exists():
            request=directory/'request.json'
            if not request.exists():
                time.sleep(.2);continue
            job=json.loads(request.read_text())
            if job['id'] in seen:
                time.sleep(.2);continue
            seen.add(job['id'])
            destination=Path(job['directory'])
            with (destination/f"remote-{args.index}.stderr").open('w') as stream:
                child=subprocess.Popen(job['command'],env=job['environment'],
                    stdout=stream,stderr=stream,start_new_session=True)
                atomic(destination/f"remote-{args.index}.started.json",dict(host=socket.getfqdn(),pid=child.pid))
                while child.poll() is None:
                    if (destination/f"remote-{args.index}.stop").exists() or (args.pool/'STOP').exists():
                        os.killpg(child.pid,signal.SIGTERM)
                        try:child.wait(timeout=30)
                        except subprocess.TimeoutExpired:
                            os.killpg(child.pid,signal.SIGKILL);child.wait()
                        break
                    time.sleep(.1)
                atomic(destination/f"remote-{args.index}.done.json",dict(returncode=child.returncode,host=socket.getfqdn()))
                child=None
    finally:
        if child and child.poll() is None:
            os.killpg(child.pid,signal.SIGTERM)
            child.wait(timeout=35)
        atomic(directory/'terminal.json',dict(host=socket.getfqdn(),time=time.time()))


if __name__=='__main__':main()
