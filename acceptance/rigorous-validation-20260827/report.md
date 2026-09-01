# DataVine rigorous validation

Date: 2026-08-27  
Host: `daccssfe.crc.nd.edu`  
Base commit: `9818f7f42cd0ac83d4e830697a520e6c13b4bba7`  
Status: **PASS**

Every workflow result below checks one physical TaskVine submission and
completion per logical task. Requested-output runs also verify the exact file
count, byte count and payload content. Rates from different worker topologies
are reported separately.

## Result

The task path is stable and scales beyond the local Worker. The Controller's
in-memory tables are not the current limit. A third-pass process-driven test
corrected the earlier Python-thread measurement: single-record TCP publication
exceeds 100k/s, not 20k/s. Requested small files had a separate, severe
transport-latency bug: Nagle plus delayed ACK inserted
about 40 ms between the transfer header and body. Tuning the persistent
Worker/Controller socket for interactive latency removed it with a 42.6x
speedup and no measurable bulk-stream regression. A fourth pass then shortened
the per-Worker connection critical section and isolated the remaining local
ceiling to per-task completion/scheduling work.

## Task throughput

| Topology | Workload | Repetitions | Runtime rate | Service rate |
|---|---:|---:|---:|---:|
| local 1 Worker x 16 cores | 50,000 builtin tasks | 3 | mean 4,067.7 tasks/s, CV 1.28% | 4,509-4,589 tasks/s |
| Condor 16 Workers x 4 cores | 50,000 builtin tasks | 1 admitted run | 13,686.4 tasks/s | 19,277.7 tasks/s |

The Condor run admitted exactly 16 Workers and completed exactly 50,000 tasks.
A bounded repeat attempt left all 16 jobs idle for 300 seconds, failed the
inventory gate, and automatically removed them. It is not mixed into the
throughput result.

## Controller metadata

- One-million-DataID lifecycle: 0.958 s, 8.56M publishes/s and 2.33M random
  resolves/s; maximum RSS 214,816 KiB.
- Two-million-replica disconnect: 3.96M replica removals/s; maximum RSS
  302,492 KiB.
- Real TCP, one record per RPC, 102,400 records per phase: process clients at
  64 connections average 101.1k publications/s and 84.0k resolves/s. At 128
  connections publication averages 98.7k/s with substantially higher variance.
- The original Python-thread driver reached only about 20k/s because its GIL
  serialized request generation. It was a client ceiling, not a Controller
  ceiling.

The single Controller owner uses about 6.87 us CPU per publication at the
64-connection operating point. Its balanced saturation region is approximately
64 active connections and 100k individual publications/s on this host. See
`third-pass/report.md` for the corrected experiment and RPC fast-path change.

## Filesystem I/O

All writes create a private file, call `fsync`, and atomically rename it.
Sixteen threads were used on both filesystems.

| Operation | Controller `/tmp` | SharedFS `/users` | `/tmp` advantage |
|---|---:|---:|---:|
| 5,000 x 4 KiB write | 15,037 files/s | 815 files/s | 18.44x |
| 1 GiB direct write, 1 MiB files | 2,807 MiB/s | 351 MiB/s | 8.00x |
| 1 GiB direct read, 1 MiB files | 3,569 MiB/s | 993 MiB/s | 3.59x |

Buffered read numbers are retained in raw evidence but are page-cache results,
not storage bandwidth: `/tmp` 39,382 MiB/s and SharedFS 701 MiB/s for 1 MiB
files. The direct-I/O result is the defensible read comparison.

## End-to-end data movement

| Workload | Result | End-to-end | Controller service window |
|---|---:|---:|---:|
| 20,000 empty outputs, 1x16 | 3 reps, exact | mean 978.8 files/s, CV 0.66% | 1,016-1,033 files/s |
| 2,000 x 4 KiB before fix | exact 7.8125 MiB | 24.24 files/s | 24.32 files/s |
| 2,000 x 4 KiB after fix | 3 reps, exact | mean 1,034.2 files/s, CV 0.51% | mean 1,203.8 files/s |
| 4,096 x 256 KiB after fix | 2 reps, exact 1 GiB | mean 141.5 MiB/s | mean 150.2 MiB/s |

The 1 GiB pre-fix service rate was 150.4 MiB/s. The two post-fix values were
149.0 and 151.4 MiB/s, so the latency fix preserved bulk throughput.

The fixed-16-core fourth-pass matrix is in `fourth-pass/report.md`. Releasing a
fully consumed Worker connection before local durability improved the 1x16
three-run mean by 3.95%, from 1,124.9 to 1,169.3 files/s, while 4x4 and 16x1
were unchanged. Empty-output throughput plateaued at 3,657.7 files/s; adding a
4 KiB payload cost only 7-8% with multiple Workers. A 16x1 exact 1 GiB guard
reached 656.0 MiB/s. These results identify per-task completion/scheduling, not
the Controller data plane, as the remaining local small-task limit.

The fifth pass instrumented that completion path and accepted one narrow
DataVine-only tuning. Interleaved 50k A/B runs show `prefer-dispatch` raising
service throughput from 15,035.4 to 16,538.8 tasks/s at 4x4 (+10.00%) and from
13,862.2 to 15,879.2 tasks/s at 16x1 (+14.55%). It removes most
completion-to-refill delay without batching, adding an owner, or changing the
Scheduler/Controller boundary. Exact empty, 4 KiB and 1 GiB requested-output
guards remained stable. A direct-poll-pointer candidate regressed 0.96% and was
reverted. See `fifth-pass/report.md`.

The new layered guard also exposed a correctness gap: native FunctionCall bytes
existed in Worker memory but not in the sandbox file consumed by the DataVine
Agent. The Worker now materializes that output before commit. A dedicated
regression and an exact 20,000-task, 39,488-edge layered workflow both pass.

These local-Worker runs still exercise real TaskVine processes, the Worker
transfer server, persistent TCP framing, Controller data threads, SHA-256
validation, `/tmp` fsync and atomic rename. They do not claim cross-host network
bandwidth. The admitted Condor task run proves multi-Worker task scaling; a new
cross-host data repetition remains dependent on cluster availability.

## Correctness and residue

- Forced warning-clean build: DataVine library, Manager, Worker and five tools.
- Python compile/static checks: PASS.
- Full regression with a freshly compiled Go client: 18/18 PASS.
- Final Condor/process audit: no jobs and no DataVine benchmark processes.

Run `sha256sum -c SHA256SUMS` in this directory to verify the evidence set.
