# DataVine fifth-pass completion/dispatch validation

Date: 2026-08-27  
Host: `daccssfe.crc.nd.edu`  
Base commit: `9818f7f42cd0ac83d4e830697a520e6c13b4bba7`  
Status: **PASS**

## Result

The remaining independent short-task ceiling was dominated by latency between
receiving a completion and refilling the newly free Worker slot. It was not
DataVine's logical transition or Controller path. DataVine now enables the
existing Manager `prefer-dispatch` policy: if READY work already exists, free
slots are refilled before the retrieved completion is returned to the workflow
reactor. When no READY task exists, the completion returns immediately so the
Scheduler can release dependent children.

This is one-record, one-task operation. It adds no batch, queue, thread, owner,
or alternate runtime. The tuning is selected only by DataVine; generic
TaskVine retains its default behavior for baseline comparison.

## Fixed-core A/B

The rigorous comparison interleaved old and new binaries in the sequence
old/new/new/old/old/new/new/old. Every run used 50,000 builtin no-output tasks,
exactly 16 total Worker cores, and exactly 50,000 physical submissions and
completions.

| Topology | Before | `prefer-dispatch` | Change | After CV |
|---|---:|---:|---:|---:|
| 4 Workers x 4 cores | 15,035.4 tasks/s | 16,538.8 tasks/s | **+10.00%** | 0.38% |
| 16 Workers x 1 core | 13,862.2 tasks/s | 15,879.2 tasks/s | **+14.55%** | 6.90% |

Mean Scheduler delay fell from 0.255203 to 0.014663 seconds at 4x4 and from
0.277963 to 0.015696 seconds at 16x1. The larger-worker topology remains more
variable, but all four new-policy 4x4 observations exceeded all four old-policy
observations.

## Why this was the right target

Aggregate metrics were added behind `DATAVINE_WORKFLOW_METRICS`; production
runs without metrics do not execute the added timers. In the fixed-16-core
baseline, DataVine completion processing was roughly 6 microseconds per task,
including only about 3 microseconds for logical transition. Manager status,
receive-framework work and completion-to-refill delay were larger. The A/B
result confirms that shortening refill latency matters more than optimizing
the already small DataVine bookkeeping path.

A second candidate passed a direct `vine_worker_info` pointer from the poll
table and avoided per-status link-to-hash lookup. At 16x1 its status CPU fell
from 0.724 to 0.661 seconds, but the interleaved 50k mean fell from 14,341.4 to
14,204.0 tasks/s (-0.96%). That candidate was rejected and completely reverted.

## Workload guards

| Workload | Topology | Result |
|---|---|---:|
| `/usr/bin/true`, no output, 8k | 4x4 | -3.21% in a two-run guard |
| `/usr/bin/true`, no output, 8k | 16x1 | +0.75% in a two-run guard |
| requested empty output, 8k | 4x4 | +1.33% |
| requested empty output, 8k | 16x1 | +2.07% |
| requested 4 KiB, 8k, 3 reps | 4x4 | 3,250.4 files/s, +1.30% vs fourth pass |
| requested 4 KiB, 8k, 3 reps | 16x1 | 3,462.8 files/s, +2.10% vs fourth pass |
| requested 256 KiB, exact 1 GiB | 16x1 | 663.3 MiB/s |

The command guard is dominated by process creation and does not establish a
speedup; it shows no structural regression. Every requested-output run checked
the exact task count, physical submission/completion count, file count, byte
count and content. Bulk throughput is slightly above the prior 656.0 MiB/s
guard, so the completion tuning does not damage the Controller stream path.

## Correctness finding

The new layered benchmark initially failed on task 1 with `OUTPUT_MISSING`.
TaskVine FunctionCall keeps its returned bytes in Worker memory, while the
DataVine Agent commits the declared sandbox file. The Worker now materializes
that in-memory result into `.taskvine.stdout` immediately before the Agent
commit. Failure remains fail-closed as `COMMIT_IO_FAILED`.

A dedicated `native-function-output` regression checks exact returned bytes.
The final layered run completed exactly 20,000 FunctionCall tasks and 39,488
edges at width 256, with one physical execution per logical task. Its 1,249.5
tasks/s rate and 12.125 seconds of polling show that narrow dependency waves
are a separate Worker-readiness/polling regime, not the independent-task
completion-refill limit addressed here.

## Acceptance

- accepted implementation: DataVine-only `prefer-dispatch` tuning;
- rejected direct-poll-pointer candidate: fully reverted;
- FunctionCall output materialization: focused and layered tests PASS;
- forced warning-clean DataVine library, Manager, Worker and five-tool build;
- full DataVine regression: 18/18 PASS with a freshly compiled Go client;
- no batching, extra owner, per-task priority queue, or new data path.

Raw benchmark JSON and service/Factory logs are retained in this directory.
`SHA256SUMS` covers the report, summary, regression and every benchmark JSON.
