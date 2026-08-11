# DataVine workflow characterization against TaskVine

Date: 2026-08-10
Status: **cluster pilot PASS; publication-scale causal claims remain OPEN**

## Executive summary

This campaign compares the current DataVine native workflow service with the
documented TaskVine `FuturesExecutor`/`FunctionCall-fork` path. Both execute the
same deterministic Python kernels as one logical task per physical TaskVine
task. There is no semantic batching. Each backend owns one persistent HTCondor
Factory allocation of 10 workers with 16 cores each. No measured workflow is
submitted until all 10 workers and all 160 cores are connected, and workers
remain resident across every repetition for that backend.

The main five-repetition campaign completed 25,840 physical task executions;
the feature campaign completed another 12,810. Together with a three-repetition
crossed-order campaign, 54,154 successful physical executions were checked with
zero logical/physical count mismatch and identical requested result content.

DataVine is strongest on wide workflows whose graph can be registered once and
driven by the C runtime: 4.78x on a 640-way map, 4.08x on a heavy-tailed map,
3.74x on fan-out, 3.55x on fan-in, and 3.60x on a three-stage pipeline. It is
slower on eight serial result-driven dynamic tasks (0.30x), an eight-task chain
(0.48x), and tiny selective multi-output (0.39x). These cases expose the fixed
cost of service RPC, durable workflow generations, journaling, and result fetch.

The data-size experiment rejects a simple “data-aware is always faster”
hypothesis. DataVine is 0.58x at 32 MiB reusable-root output and 0.42x on a wide
multi-output workload. For the latter, median native runtime is 4.091 s, of
which 3.896 s is publication. The then-current DVP2/base64/stdout and durable journal
path copies large retained values and is the clearest optimization target.

That result path was first replaced by direct executor output files and then by
the active worker-local/peer design. The historical baseline, intermediate
file-backed rerun, and final worker-local rerun are all retained below rather
than mixing results from different implementations.

### Post-Data-Controller rerun: 2026-08-11

The two affected workflows were rerun with both backend orders, five
repetitions per order, and independent resident 10-worker x 16-core Condor
pools. Every pool reached exactly 10 connected workers and 160 connected cores
before warmup or dispatch. All 40 backend-runs passed exact physical-task and
result-content checks.

| Workflow | TaskVine median | DataVine median | DV/TV rate | Old DV/TV |
|---|---:|---:|---:|---:|
| 32 MiB reuse | 0.791 s | 2.542 s | 0.311x | 0.576x |
| wide multi-output | 1.593 s | 8.783 s | 0.181x | 0.424x |

The redesign successfully removed synchronous result publication from the
Runtime hot path: median measured publication wait fell from about 0.52 s to
0.195 s for 32 MiB reuse and from about 3.90 s to effectively zero for wide
multi-output. This did not improve end-to-end performance. Median task
materialization rose from about 0.003 s to 0.475 s and from about 0.008 s to
1.769 s respectively; ordinary TaskVine per-output file declaration, file
return, and controller hashing now dominate. The correct conclusion is that
the ownership redesign is cleaner and removes Runtime payload work, but its
current file granularity is slower for these two data-heavy shapes.

The crossed-order result is robust to backend launch order, but it still uses
separate opportunistic Condor allocations rather than paired homogeneous
nodes. Raw reports and the compact comparison artifact are under
`acceptance/sc-workflow-data-controller-20260811-10x16-{n5,reverse-n5}/` and
`acceptance/data-controller-experiment-20260811.json`.

These are credible implementation and design results, but not yet an SC-ready
hardware result: the two backends used separate opportunistic Condor pools on
heterogeneous nodes. Backend order was crossed and every directional conclusion
was preserved, but a final paper must repeat on homogeneous reserved nodes with
paired allocations, at least ten repetitions, confidence intervals, and worker
resource/traffic counters.

### Worker-local/peer redesign: 2026-08-11

The per-output manager-file design above is now historical. The active design
uses TaskVine's existing data plane instead of adding another daemon or
protocol:

- a Python child serializes each output once and streams SHA-256 during that
  write;
- its FunctionCall result is only a compact DVM1 size/hash manifest;
- non-requested intermediates are `VINE_TEMP` files kept in worker cache and
  shared through TaskVine peer transfer;
- requested public values alone are returned to Data-Controller-owned files;
- Runtime routes task state and never reads payload bytes or parses result
  metadata; the Data Controller reads only the compact DVM1 size/hash manifest
  from TaskVine stdout;
- a service restart recomputes a producer if its retained worker-local output
  is no longer active.

The final taskvine-first 10x16 n=5 run used the exact committed implementation.
A preceding datavine-first hybrid n=5 run provides order corroboration.

| Workflow | TaskVine median | DataVine median | DV/TV rate | Cross-order rate | Old file-backed rate |
|---|---:|---:|---:|---:|---:|
| 32 MiB reuse | 0.815 s | 2.500 s | 0.326x | 0.437x | 0.311x |
| wide multi-output | 1.543 s | 4.273 s | 0.361x | 0.392x | 0.181x |

All exact task counts and requested-result comparisons passed. Storage evidence
is equally important: across warmup and ten measured DataVine workflows, the
Controller retained 2,401 requested files totaling 555,786 bytes, with a
maximum file size of 236 bytes. The 32 MiB reusable roots and the 512 KiB
multi-output intermediates never appeared in the Controller data directory.

Thus the architecture goal is met and wide multi-output relative performance
more than doubles in the crossed-order comparison. DataVine nevertheless
remains slower for both output-heavy shapes. Remaining cost is dominated by
per-task FunctionCall/materialization and durable requested-output handling;
this result must not be described as parity or a performance win. Variation
between the two opportunistic Condor orders also reinforces the reserved-node
gate below.

Raw evidence:

- `acceptance/sc-workflow-worker-local-final-20260811-10x16-n5/summary.json`
- `acceptance/sc-workflow-worker-local-hybrid-20260811-10x16-n5/summary.json`
- `acceptance/worker-local-data-plane-20260811.json`
- `acceptance/native-worker-local-hybrid-1x1x{100k,1m}.json`

## Research questions

1. Which workflow structures benefit from moving graph scheduling and durable
   state out of the user Python process and into DataVine's C runtime?
2. At what granularity does DataVine's fixed durable-control cost dominate?
3. Does native data identity and selective retention improve large-intermediate
   workflows in the current implementation?
4. Can the comparison preserve one task/one lease, executor isolation, exact
   output semantics, and a stable worker pool?

The expected outcome was an advantage for wide DAGs and dynamic durability, a
disadvantage for tiny serial workflows, and an advantage for data reuse. The
first two are supported. The current large-data hypothesis is rejected.

## Systems compared

### DataVine

The client registers Workflow IR through the language-neutral RPC boundary.
The C service owns graph validation, journal/replay, generation CAS, readiness,
TaskVine task declaration, completion, publication, and terminal state. Python
callables execute through one preloaded, single-threaded parent and isolated
fork children. The Python process builds or appends graph deltas and fetches
requested values; it does not run scheduler policy.

### TaskVine reference

The reference uses `ndcctools.taskvine.FuturesExecutor`, a preinstalled function
library in `FunctionCall-fork` mode, and ordinary futures/dependencies. Dynamic
work is expressed as the natural client-side loop: wait for a value, derive the
next seed, and submit the next task.

This is therefore a comparison of the DataVine workflow interface against the
TaskVine Python futures interface, not against a hypothetical hand-written bare
C TaskVine manager application.

## Experimental contract

| Property | Contract |
|---|---|
| Allocation | HTCondor; 10 workers x 16 cores = 160 cores per backend |
| Residency | One Factory and Manager/service lifetime per backend |
| Start gate | Exactly 10 connected workers and at least 160 connected cores before warmup or measured user dispatch |
| Execution | Same `work_unit`, `split_unit`, and `select_unit`; fork mode; one core per task |
| Identity | One logical task equals one physical TaskVine submission and completion |
| Batching | Disabled; no grouped/noop or semantic batching path |
| Correctness | Requested digest, payload length, and payload SHA-256 compared across backends |
| Timing | End-to-end graph build/submission through requested-result availability; Factory startup and one warmup excluded |
| Sampling | Same 50 ms coordinator process-tree CPU/RSS/FD sampler |
| Repetition | Five primary repetitions; three crossed-order corroborating repetitions |

Environment: commit `b4eece13856c3a04f28bbe3a43529cbc8bbda6bf`,
Python 3.10.20, Linux 5.14/glibc 2.34, submit host
`daccssfe.crc.nd.edu`. The submit-host cgroup had 32 effective CPUs; execution
capacity came from the 160 remote worker cores. Raw JSON records exact commands,
calibration, per-run metrics, result hashes, manager counters, and pool gates.

## Workflow matrix

Widths are derived from `P=160` connected cores so that parallel cases can
exercise the full pool.

| Workflow | Physical tasks/run | Characteristic |
|---|---:|---|
| map | 640 | Four waves of independent 20 ms CPU tasks |
| chain | 8 | Fully serial dependencies |
| fan-out | 321 | One 4 KiB root, 320 consumers |
| fan-in | 321 | 320 producers, one merge |
| diamond | 162 | One root, 160 middle tasks, one merge |
| pipeline | 480 | Three 160-wide stages with cross-lane inputs |
| heavy-tail | 320 | Independent 5-160 ms tasks |
| large reuse | 321 | One 2 MiB root reused by 320 tasks |
| selective multi-output | 3 | One 3-output split, two retained branches |
| dynamic | 8 | Each successful value constructs the next task |
| map 100 ms | 320 | Granularity sweep |
| map 500 ms | 320 | Granularity sweep |
| large reuse 32 MiB | 161 | One 32 MiB root reused by 160 tasks |
| wide multi-output | 480 | 160 splits plus 320 selective consumers |

CPU durations are calibration targets, not real-time deadlines. Each result
also carries a deterministic payload and accumulated child CPU accounting.

## Primary five-repetition results

`DV speedup` is TaskVine median wall time divided by DataVine median wall time;
values above one favor DataVine. CV is sample standard deviation divided by
mean and is shown as TaskVine/DataVine.

| Workflow | TaskVine median (s) | DataVine median (s) | DV speedup | CV TV/DV |
|---|---:|---:|---:|---:|
| map | 3.5144 | 0.7350 | **4.782x** | 0.110 / 0.119 |
| heavy-tail | 1.8025 | 0.4419 | **4.079x** | 0.372 / 0.037 |
| fan-out | 1.6922 | 0.4525 | **3.740x** | 0.427 / 0.106 |
| pipeline | 2.4925 | 0.6922 | **3.601x** | 0.204 / 0.048 |
| fan-in | 1.7524 | 0.4942 | **3.546x** | 0.239 / 0.126 |
| large reuse, 2 MiB | 1.6906 | 0.5397 | **3.132x** | 0.110 / 0.046 |
| diamond | 0.9262 | 0.3455 | **2.680x** | 0.453 / 0.091 |
| chain | 0.1236 | 0.2557 | 0.483x | 0.276 / 0.050 |
| selective multi-output, tiny | 0.0533 | 0.1382 | 0.385x | 0.171 / 0.053 |
| dynamic | 0.1316 | 0.4435 | 0.297x | 0.229 / 0.052 |

The broad parallel-DAG separation is larger than the prior single-worker CPU
fork parity result because this experiment includes the Python futures graph
construction and per-task staging path. The result supports the design claim
that a compact registered graph and C-owned scheduler reduce orchestration
overhead. It does not imply that the underlying DataVine executor makes an
individual CPU function several times faster.

## Duration and data-size features

| Feature | TaskVine median (s) | DataVine median (s) | DV speedup | CV TV/DV |
|---|---:|---:|---:|---:|
| map, 100 ms x 320 | 1.2739 | 0.4387 | **2.904x** | 0.064 / 0.215 |
| map, 500 ms x 320 | 1.3367 | 0.6363 | **2.101x** | 0.106 / 0.315 |
| reused 32 MiB root x 160 | 0.8449 | 1.4669 | 0.576x | 0.099 / 0.030 |
| wide selective multi-output | 1.8771 | 4.4275 | 0.424x | 0.069 / 0.009 |

Map speedup declines from 4.78x at the nominal 20 ms kernel to 2.90x at 100 ms
and 2.10x at 500 ms. This is the expected control-plane amortization trend,
although heterogeneity prevents using these points as a precise crossover.

DataVine stage medians explain the large-output cases:

| Feature | Native run (s) | Publication (s) | Materialize (s) | Submit (s) |
|---|---:|---:|---:|---:|
| map 100 ms | 0.2266 | 0.1521 | 0.0072 | 0.0120 |
| map 500 ms | 0.4374 | 0.2732 | 0.0073 | 0.0120 |
| reused 32 MiB | 1.3354 | 0.5228 | 0.0025 | 0.0034 |
| wide multi-output | 4.0906 | **3.8959** | 0.0085 | 0.0123 |

The fixed feature campaign's durable journal grew to approximately 972 MiB.
That is not a leak claim—the experiment intentionally retains large requested
values—but it demonstrates that current durable payload recording is a major
data-plane and storage cost. The inactive journal is retained losslessly as a
3.70 MiB gzip artifact; its uncompressed SHA-256 is
`f410c3e3c7beeb3308a72fab55bef78a774ae322d3902fa565b45c578a466268`.

## Dynamic-workflow interpretation

The dynamic case is deliberately only eight serial 20 ms tasks. TaskVine's
ephemeral local loop is the fastest implementation of that narrow contract.
DataVine additionally provides durable successful values, generation CAS,
open-quiescent state, detach/attach, restart replay, and a language-neutral
append interface. The 0.297x ratio quantifies the current price of those
semantics at tiny granularity; it is not evidence that dynamic workflows should
be removed. Follow-up experiments should increase per-step computation, branch
width after each decision, notebook disconnect time, and injected restart.

## Crossed backend order

A three-repetition run allocated TaskVine before DataVine. Every conclusion kept
the same direction: parallel cases favored DataVine by 1.76-3.53x; chain,
dynamic, and tiny selective multi-output favored TaskVine. Magnitudes differ
from the primary run, confirming cluster variability and strengthening the
need for paired homogeneous nodes. Its resource sampler predated the final
common sampler, so those process-resource figures are not merged with the
primary campaign.

## Correctness and defect found

All successful campaigns require exact physical submissions and completions,
zero failed tasks, and identical requested result hashes. Successful totals are:

| Campaign | Backend-runs | Physical executions |
|---|---:|---:|
| Primary, five repetitions | 100 | 25,840 |
| Feature, five repetitions | 40 | 12,810 |
| Crossed order, three repetitions | 60 | 15,504 |
| **Total** | **200** | **54,154** |

The first 160-wide multi-output experiment failed with `publish_failed`. The
failure was reproducible and was not excluded as an outlier. The executor's
`send_frame()` used one `os.writev()` call and assumed that a legal partial pipe
write had sent the entire frame. Large concurrent DVP2 frames could therefore
be truncated. The implementation now loops until length, delimiter, and payload
bytes are fully written. A regression forces writes of at most seven bytes and
asserts byte-exact reconstruction. The affected native execution contract and
all five fixed distributed repetitions pass.

Final source validation passed the warning-clean DataVine/tools build, module
boundary test, affected native execution contract, driver syntax/help check,
artifact checksums, and `git diff --check`. The full supported-contract runner
passed eight of nine tests. The Go adaptor test was not runnable because this
environment has no configured Go binary/compiler; this gate remains explicitly
environment-blocked rather than being reported as a 9/9 pass.

## Resource observations

The common sampler covers the submit-side Python process tree and Factory, not
remote workers. Across the resident primary pools, coordinator high-water RSS
was roughly 109-122 MiB for DataVine and 116-139 MiB for TaskVine near the third
repetition; DataVine used about 39 FDs and TaskVine 28-29. These are cumulative
pool-lifetime high-water observations, not isolated per-workflow memory. Current
TaskVine transfer counters reported zero received bytes on this function path
and are not suitable for a network-volume comparison, so no transfer-reduction
claim is made.

## Threats to validity

1. Condor supplied heterogeneous and opportunistic nodes, including different
   CPU classes. Separate backend allocations are not paired hardware.
2. Five repetitions characterize this implementation but are insufficient for
   final confidence intervals and outlier policy.
3. Synthetic kernels isolate structure and transport; they do not represent
   application libraries, input services, or failure distributions.
4. Coordinator RSS excludes remote workers and child-process peaks between
   50 ms samples. Network-byte counters are currently incomparable.
5. DataVine includes durable journal semantics while the TaskVine dynamic
   reference is ephemeral. This is intentional but not semantic equivalence.
6. Pool and library startup are excluded. Long-running scientific services
   amortize that cost; short one-shot workflows may not.
7. Results apply to the TaskVine FuturesExecutor reference path and this commit,
   not every possible TaskVine application or future transport optimization.

## SC publication gates

Before making final paper claims:

1. Reserve homogeneous nodes and pair backend runs on the same node set; rotate
   backend order per pair.
2. Run at least ten repetitions and publish raw samples, bootstrap 95% confidence
   intervals, effect sizes, and a predeclared outlier rule.
3. Measure remote worker CPU, RSS, bytes, disk, executor children, and utilization
   with synchronized sampling.
4. Add strong-scaling and weak-scaling sweeps over workers and cores, plus DAG
   width, depth, task duration, fan degree, payload size, and retained fraction.
5. Add real scientific workflows: event analysis/map-reduce, iterative parameter
   search, notebook-driven adaptive analysis, and a data-reuse pipeline.
6. Add worker loss, executor crash, client detach/reconnect, service restart,
   and journal replay; report recovery latency and duplicate physical attempts.
7. Measure and reduce FunctionCall/materialization and requested-output
   durability cost without reintroducing manager-side intermediate payloads.
8. Compare against a bare TaskVine manager API where appropriate and state which
   API/lifecycle each baseline represents.

## Reproduction

Run from the repository root after building/installing the checkout. Each
command starts one backend pool at a time, waits for all 10 workers/160 cores,
runs every repetition without stopping the pool, then cleanly stops it before
the next backend.

```sh
python acceptance/scripts/compare_workflows.py \
  --workers 10 --cores 16 --batch-type condor \
  --backend-order datavine-first --repetitions 5 \
  --workflows map,chain,fanout,fanin,diamond,pipeline,heavy_tail,large_reuse,selective_multi_output,dynamic \
  --output-dir acceptance/sc-workflow-comparison-20260810-10x16-reverse-n5

python acceptance/scripts/compare_workflows.py \
  --workers 10 --cores 16 --batch-type condor \
  --backend-order taskvine-first --repetitions 5 \
  --workflows map_100ms,map_500ms,large_reuse_32mb,selective_multi_output_wide \
  --output-dir acceptance/sc-workflow-features-20260810-10x16-fixed-n5
```

Authoritative machine-readable results are the `summary.json` files in those
directories. The crossed-order corroboration is in
`acceptance/sc-workflow-comparison-20260810-10x16-final/summary.json`. Verify
the retained summaries and compressed large journal with:

```sh
sha256sum -c acceptance/sc-workflow-comparison-20260810.sha256
```
