# DataVine 1M no-op big-Worker-pool experiment

Date: 2026-09-03  
Status: **PASS at 32 Workers x 16 cores; larger pool admission unavailable**

## Answer

DataVine completed exactly 1,000,000 native no-op tasks on 32 physical Condor
Workers x 16 cores (512 advertised cores), with no input, output, Controller
backup, durable file, or recovery journal.

| Boundary | 32x16 result |
|---|---:|
| Physical submissions/completions | 1,000,000 / 1,000,000 |
| Service time | 36.942 s |
| Service throughput | **27,069 tasks/s** |
| Python end-to-end time | 48.532 s |
| End-to-end throughput | **20,605 tasks/s** |
| Post-registration time | 37.545 s |
| Post-registration throughput | 26,635 tasks/s |
| Worker admission, excluded | 44.550 s |
| Requested outputs/durable files | 0 / 0 |
| Journal bytes | 0 |
| Peak frontend RSS | 1.257 GiB |

The prior 8x16 Condor campaign completed the same 1M native workload twice at a
mean 28,705 service tasks/s and 21,663 end-to-end tasks/s. It is a historical
same-workload reference, not an interleaved code-identical A/B: it requested
3 GiB rather than 2 GiB per Worker and preceded the latest unrelated data-path
edits. The new 32x16 point provides 94.30% of its service rate (-5.70%) and
95.12% of its end-to-end rate (-4.88%). More Workers did not increase no-op
throughput in this observation.

## Diagnosis

The 512-core pool is deliberately overprovisioned for zero-work tasks. The
Scheduler is not the limiter:

- total Scheduler delay: 0.002728 s;
- Manager scheduling timer: 0.797 s;
- Manager send path: 11.125 s;
- Manager status path: 14.011 s;
- completion processing: 2.260 s;
- service CPU: 43.62 CPU-seconds during 36.94 wall-seconds.

The remaining ceiling is serialized one-task message transport and lifecycle
processing in the Manager/Worker protocol. Increasing the pool from 8 to 32
adds sockets and status fan-in but cannot parallelize this owner path. For this
workload the highest measured efficiency and throughput remain at 8x16, around
28.7k tasks/s. A 32x16 pool is proven functional, but not faster.

This is useful rather than a failure: DataVine already has enough Worker-side
capacity to saturate the frontend with eight Workers. Larger pools should be
used for tasks containing actual CPU, I/O, or peer data work, where capacity
can scale independently of the per-task control ceiling.

## Larger-pool admission

The harness requires the complete requested pool before starting the workflow,
so partial admission cannot contaminate timing.

- 64x16 (1024 cores) reached 55 connected Workers, then a collective vacate
  reduced it to 47; the attempt was cancelled before task submission.
- 48x16 (768 cores) reached 29 connected Workers and stopped progressing; it
  was cancelled before task submission.
- A requested 32x16 repeat reached only 22 Workers after cluster availability
  changed; it was cancelled before task submission.
- A 16x16 diagnostic immediately afterward received no slots, confirming a
  transient Condor allocation boundary rather than a DataVine failure.

All cancelled jobs were removed. These are admission observations, not runtime
performance samples. The one successful 32x16 run is the largest valid complete
pool in this campaign.

## Exact workload and policy

- one logical task equals one physical TaskVine FunctionCall;
- native builtin executor, independent tasks, no semantic batching;
- staged registration in ten 100,000-task deltas;
- `recovery:none`, `idata_backup:worker-local`;
- no requested output and no generated file;
- Worker admission excluded from both workflow and service intervals;
- exact physical identity and terminal completion checked by the harness.

## Implication

For no-op throughput substantially beyond 30k tasks/s, tuning Worker count,
queue depth, or adaptive execution is the wrong layer. The next architectural
experiment should target a compact native task/status wire representation or
parallel message parsing while retaining one-task semantics and one authoritative
Manager owner. Before changing the protocol, the 8x16 and 32x16 points should
be repeated in a reserved/stable allocation to quantify normal run variance.

The full machine record is `raw/condor-32x16-run1.json`; `summary.json` contains
the compact comparison and provenance.
