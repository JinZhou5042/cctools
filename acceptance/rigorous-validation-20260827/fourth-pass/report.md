# DataVine fourth-pass validation

Date: 2026-08-27  
Host: `daccssfe.crc.nd.edu`  
Base commit: `9818f7f42cd0ac83d4e830697a520e6c13b4bba7`  
Status: **PASS**

## Change

The persistent pull connection was held until the Controller finished local
`fsync`, `close` and atomic `rename`. Once a complete framed response has been
consumed, those operations cannot affect the connection protocol. The lock is
now released at that boundary so another data thread may use the connection
while local durability completes.

An incomplete header/body still closes the connection before retry. A local
persistence failure keeps the fully consumed connection healthy but does not
commit metadata; the file is removed and retried. This preserves fail-closed
result admission.

## Fixed-core A/B

Every row uses exactly 16 total Worker cores, 8,000 command tasks, 8,000
requested outputs, exact file/byte/content validation and three repetitions.
The rate is the native service execution window, excluding client registration
and Worker admission.

| Topology | 4 KiB before | 4 KiB after | Change | After CV |
|---|---:|---:|---:|---:|
| 1 Worker x 16 cores | 1,124.9 files/s | 1,169.3 files/s | +3.95% | 0.62% |
| 4 Workers x 4 cores | 3,202.1 files/s | 3,208.6 files/s | +0.20% | 0.79% |
| 16 Workers x 1 core | 3,386.9 files/s | 3,391.4 files/s | +0.14% | 0.38% |

The one-Worker gain is modest and should not be interpreted as a new
throughput regime. It confirms that local durability was unnecessarily inside
the per-Worker connection critical section. Multi-Worker results remain flat,
so this lock was not the global ceiling.

## Empty versus 4 KiB

| Topology | Empty output | 4 KiB output | 4 KiB penalty |
|---|---:|---:|---:|
| 1 Worker x 16 cores | 1,176.5 files/s | 1,169.3 files/s | 0.61% |
| 4 Workers x 4 cores | 3,441.8 files/s | 3,208.6 files/s | 6.77% |
| 16 Workers x 1 core | 3,657.7 files/s | 3,391.4 files/s | 7.28% |

Empty-output service throughput rises 192.5% from one to four Workers but only
6.3% from four to sixteen. The 4 KiB payload costs only 7-8% once several
Workers are active. Together with the independently measured 100k/s metadata
RPC path, this isolates the approximately 3.6k/s local plateau to per-task
Worker completion and scheduling work rather than Controller tables, disk
bandwidth, or small payload bytes.

The service-reported scheduler delay tells the same story: 2.50 seconds at
1x16, 0.865 seconds at 4x4, and 0.814 seconds at 16x1 for the empty runs.
Concentrating 16 slots behind one Worker serializes more of TaskVine's
per-Worker task protocol; four Workers remove most of that cost.

## Bulk guard

The post-change 16x1 run persisted 4,096 files of 256 KiB: exactly 1 GiB in
1.561539 service seconds, or 2,623 files/s and 656.0 MiB/s. Exact task, file,
byte and content validation passed. The small-file critical-section change did
not damage the bulk path.

## Acceptance

- all 27 fixed-core small-file runs passed exact task/file/byte checks;
- the 1 GiB bulk guard passed exact validation;
- forced native builds completed without warnings;
- full DataVine regression passed 18/18;
- no batching, extra owner, per-file thread, or alternate runtime was added.

The source JSON and service/Factory logs are retained in this directory.
`SHA256SUMS` covers the report, summary, regression and all benchmark JSON.
