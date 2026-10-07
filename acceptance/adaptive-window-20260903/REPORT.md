# DataVine elastic execution and Controller RPC campaign

Date: 2026-09-03  
Status: **PASS**, with the scope limits stated below.

## Result

DataVine now uses an elastic, Worker-local FunctionCall admission controller by
default. It eagerly accepts work, starts at `2 * cores`, and adjusts the number
of runnable calls from measured CPU and memory pressure. The Manager may recall
only queued, never-started calls when another Worker becomes idle. This preserves
one-task execution and completion semantics; it is not task batching.

The final local randomized A/B reached the best fixed-window oracle within 2%
on all representative workloads:

| Workload | Elastic tasks/s | Best fixed tasks/s | Elastic / oracle |
|---|---:|---:|---:|
| CPU, 20 ms | 149.32 | 149.31 (`4C`) | 100.01% |
| sleep-I/O, 20 ms | 402.18 | 409.94 (`4C`) | 98.11% |
| mixed, 20 ms | 296.97 | 296.95 (`4C`) | 100.00% |
| sleep, 5 ms | 466.57 | 463.30 (`2C`) | 100.71% |
| random 256-KiB I/O + `fsync` | 276.87 | 275.71 (`2C`) | 100.42% |
| Python no-op | 482.32 | 481.56 (`4C`) | 100.16% |

Each value is the median of three repetitions. Trial order was randomized and
the fixed-window oracle was measured, not inferred. The raw samples include
occasional system-noise outliers; the conclusion is based on medians.

The declared-memory guard ran 32 calls declaring 128 MiB each on a 2-GiB
Worker. All three repetitions peaked at 11 concurrent calls; the maximum
observed RSS was 76.81%, below the 80% policy limit. The controller proposed
windows as large as 13, but the predictive admission guard prevented unsafe
launches.

Three rebalance repetitions each moved exactly eight queued calls to a late
Worker, producing a 24/8 execution split. In three eviction repetitions, the
late Worker was removed after receiving eight calls and the survivor returned
all 32 results. Started calls were never recalled.

## Remote scale point

One final Condor run used eight physical Workers with 16 cores each and 10,000
mixed 20-ms calls:

- 10,000/10,000 completed with no Worker loss or recall;
- service interval: 1.962 s, or **5,096 tasks/s**;
- Python end to end: 4.489 s, or **2,227 tasks/s**;
- 9.783 s Worker admission was measured separately and excluded.

The service number measures an already-admitted workflow. The end-to-end number
also includes Python graph and object registration. Neither is the native C
scheduler ceiling: the separate exact 1M-task Condor 8x16 campaign reached a
mean 28,705 service tasks/s and 21,663 end-to-end tasks/s with native no-op
tasks, no output, backup, or journal. See
`../million-task-throughput-20260903/REPORT.md`.

## Controller object path

Large Python invocation objects no longer expose Controller-local filesystem
paths. The frontend uploads and Workers fetch through a signed Controller RPC
capability. Cross-node DVP4 validation passed with four 131,273-byte invocation
objects; local DVP3 and DVP4 paths also passed.

The connection sweep supports a single default of four upload connections:

| Payload | 1 connection | 4 connections | 16 connections |
|---|---:|---:|---:|
| 256 B, 4096 cold objects | 2,656 objects/s | **3,185 objects/s** | 3,112 objects/s |
| 128 KiB, 1024 cold objects | 881 objects/s | 1,018 objects/s | **1,068 objects/s** |
| 128 KiB cold bandwidth | 110.2 MiB/s | 127.2 MiB/s | 133.5 MiB/s |

Sixteen connections improve the large-object rate by only 5.0% over four while
raising aggregate RPC wait from 3.96 to 15.09 seconds. Four is therefore the
simple unified default; the data payload still bypasses the Scheduler and
Manager.

## Final architecture

1. The Scheduler owns dependency counters, readiness, completion, retry, and
   opaque task dispatch. Completion releases children immediately.
2. The Manager eagerly drains ready work to the least-loaded Worker. DataVine
   grants an 8x per-core queued capacity; generic TaskVine keeps its default.
3. Each Worker controls actual execution admission. Every 250 ms while active,
   it observes descendant CPU, RSS, and runnable state. It starts at `2C`,
   doubles under clear CPU/memory headroom, otherwise grows by `C`, and backs
   off by `C` after two pressure epochs. CPU and memory targets are 95% and 80%;
   absolute runnable admission is capped at `3C`.
4. If a Worker is idle and the global ready queue is empty, the Manager asks the
   most-loaded Worker to return generation-bound, not-yet-started calls. No
   running process is stopped or migrated.
5. The Controller independently owns DataIDs, replicas, signed tickets,
   background backup, requested-result persistence, and GC. Input resolution is
   local-first, peer-first, Controller fallback.

This division is the paper-worthy contribution: eager ownership removes the
frontend dispatch round trip from the fast-task path, pressure-guided local
admission adapts without task labels, targeted recall corrects placement, and
the Scheduler/data-controller boundary prevents durability from gating task
progress. AIMD and work stealing themselves are established ideas; the novelty
claim must be their minimal integration with a decoupled workflow/data plane,
not invention of either primitive.

## Correctness and compatibility

- full DataVine regression: **21/21 PASS**;
- generic TaskVine Python serverless regression: **PASS**;
- warning-clean native build and Python compile checks: **PASS**;
- `git diff --check`: **PASS**;
- policy A/B remains available as `legacy`, `fixed`, `aggressive`, `aimd`, and
  `elastic`; `elastic` is the DataVine default;
- generic TaskVine retains queue multiplier 1 and its existing scheduler/data
  execution paths unless DataVine explicitly enables the optimized path.

## Scope and next scientific gates

This campaign establishes mechanism viability, local oracle proximity, memory
safety, recall/eviction correctness, Controller RPC correctness, and one 8x16
remote scale point. It does not yet establish superiority over another runtime.
A paper evaluation still needs repeated multi-node sweeps, confidence intervals,
scientific applications, utilization/latency distributions, failure injection
during pressure, and matched comparisons with TaskVine and other workflow
runtimes. The Python fork executor also retains roughly millisecond-scale fixed
cost, which is distinct from the verified native C scheduler capacity.

## Reproduction and provenance

Run `sha256sum -c SHA256SUMS` in this directory. `summary.json` is the compact
machine-readable result, `raw/README.md` identifies retained raw evidence, and
`render_results.py` regenerates all four figures. The tested checkout is based
on Git `c2e9a85be3e52d2b7f0210487a6632b8c03a413e`; exact dirty-source and binary
hashes are recorded in `summary.json` and every final benchmark JSON.

