#!/usr/bin/env python3
"""Small authenticated control channel for persistent experimental allocations.

Control messages use TCP so NFS negative-directory caching cannot delay worker
shutdown. The existing shared filesystem carries retained evidence only.
"""
import argparse
import hmac
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def request(contact, path, value=None):
    req=urllib.request.Request(contact['url']+path,
        data=json.dumps(value).encode() if value is not None else None,
        headers={'Authorization':contact['token'],'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=15) as response:
        return json.load(response)


def broker(output):
    output.mkdir(parents=True,exist_ok=False)
    token=secrets.token_hex(24)
    state={}
    lock=threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def handle_request(self):
            if not hmac.compare_digest(self.headers.get('Authorization',''),token):
                self.send_error(403);return
            value=json.loads(self.rfile.read(int(self.headers.get('Content-Length','0'))) or b'{}')
            operation,index=self.path.strip('/').split('/')
            with lock:
                row=state.setdefault(index,{})
                if operation=='ready':
                    row['ready']=value
                    (output/f'worker-{index}-ready.json').write_text(json.dumps(value,indent=2)+'\n')
                elif operation=='dispatch':
                    if row.get('job') and not row.get('done'):
                        self.send_error(409,'previous worker still running');return
                    row.update(job=value,stop=False,done=None)
                elif operation=='done':
                    if value['id']!=row['job']['id']:
                        self.send_error(409,'stale completion');return
                    row['done']=value
                elif operation=='stop':row['stop']=True
                elif operation not in ['poll','status']:
                    self.send_error(404);return
                reply=dict(row)
                if operation=='status':reply.pop('job',None)
            data=json.dumps(reply).encode()
            self.send_response(200);self.send_header('Content-Length',str(len(data)))
            self.end_headers();self.wfile.write(data)
        do_GET=handle_request
        do_POST=handle_request
    server=ThreadingHTTPServer(('0.0.0.0',0),Handler)
    contact=dict(url=f'http://{socket.getfqdn()}:{server.server_address[1]}',token=token)
    file=output/'contact.json'
    file.write_text(json.dumps(contact)+'\n');file.chmod(0o600)
    print(json.dumps(dict(status='BROKER_READY',contact=str(file))),flush=True)
    server.serve_forever()


def agent(contact_path,index):
    contact=json.loads(contact_path.read_text())
    request(contact,f'/ready/{index}',dict(host=socket.getfqdn(),
        affinity=sorted(os.sched_getaffinity(0)),
        hardware=subprocess.check_output(['lscpu'],text=True),
        condor_ad=Path(os.environ['_CONDOR_JOB_AD']).read_text()))
    seen=set()
    child=None
    def stop(signum,frame):
        if child and child.poll() is None:
            os.killpg(child.pid,signal.SIGTERM)
            try:child.wait(timeout=30)
            except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL)
        raise SystemExit(128+signum)
    signal.signal(signal.SIGTERM,stop)
    deadline=time.monotonic()+7200
    while time.monotonic()<deadline:
        row=request(contact,f'/poll/{index}')
        job=row.get('job')
        if not job or job['id'] in seen:
            time.sleep(.2);continue
        seen.add(job['id'])
        with Path(job['log']).open('w') as stream:
            child=subprocess.Popen(job['command'],env=job['environment'],
                stdout=stream,stderr=stream,start_new_session=True)
            while child.poll() is None:
                row=request(contact,f'/status/{index}')
                if row.get('stop'):
                    os.killpg(child.pid,signal.SIGTERM)
                    try:child.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid,signal.SIGKILL);child.wait()
                    break
                time.sleep(.2)
            request(contact,f'/done/{index}',dict(id=job['id'],returncode=child.returncode))
            child=None


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('mode',choices=['broker','agent'])
    p.add_argument('--output',type=Path)
    p.add_argument('--contact',type=Path)
    p.add_argument('--index',type=int)
    a=p.parse_args()
    broker(a.output) if a.mode=='broker' else agent(a.contact,a.index)
