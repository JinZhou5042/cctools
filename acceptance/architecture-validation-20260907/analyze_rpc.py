#!/usr/bin/env python3
import json,statistics
from pathlib import Path
P=Path(__file__).resolve().parent
groups={}
for f in sorted((P/'rpc-raw').glob('*.json')):
    d=json.loads(f.read_text());assert d['status']=='PASS'
    assert d['connections']==16 and d['records']==1024 and d['iterations']==100
    assert d['data_threads']==1 and d['client_mode']=='processes'
    for phase in ['publish','resolve']:
        r=d['phases'][phase];assert r['records']==102400
        groups.setdefault((d['service_threads'],phase),[]).append(dict(path=str(f.relative_to(P)),rate=r['records_per_second'],cpu_s=r['service_cpu_seconds'],p95_us=r['latency_microseconds']['p95']))
assert len(groups)==8 and all(len(rs)==3 for rs in groups.values())
rows=[]
for (threads,phase),rs in sorted(groups.items()):
    rows.append(dict(threads=threads,phase=phase,n=3,median={k:statistics.median(r[k] for r in rs) for k in ['rate','cpu_s','p95_us']},minimum={k:min(r[k] for r in rs) for k in ['rate','cpu_s','p95_us']},maximum={k:max(r[k] for r in rs) for k in ['rate','cpu_s','p95_us']},runs=rs))
(P/'rpc-summary.json').write_text(json.dumps(dict(status='PASS',sessions=12,checked_metadata_operations=2457600,rows=rows),indent=2)+'\n')
lines=['# Metadata RPC thread control','','16 process clients, 1,024 records, 100 iterations, one record/request. Data persistence threads stay at one. These tests do not execute physical tasks.','','|RPC threads|Operation|Median records/s|Median sampled p95 us|','|---|---|---:|---:|']
for r in rows:lines.append(f"|{r['threads']}|{r['phase']}|{r['median']['rate']:.0f}|{r['median']['p95_us']:.1f}|")
(P/'RPC-RESULTS.md').write_text('\n'.join(lines)+'\n');print('\n'.join(lines))
