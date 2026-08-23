# DataVine versus TaskVine: fixed 1024-core campaign

Date: 2026-08-21
Status: **FINAL CAMPAIGN PASS**

## Goal and success contract

The goal is a publication-quality, reproducible answer to: under which graph,
data and CPU regimes does DataVine outperform or underperform native TaskVine,
and why?  Both backends must receive exactly 1024 execution cores throughout a
measured block.  A result is final only when correctness, resource admission,
paired statistics and bottleneck attribution all pass.

The reported rate is `TV-native wall / DV-native wall`, so a value above 1
favors DataVine.  `TV-durable-sink` may be
used only as a diagnostic for requested-output durability; it is not the
headline baseline.

Final success requires:

1. exactly 64 workers x 16 cores = 1024 connected cores for each backend before
   warmup and before every measured run, with no partial-pool result accepted;
2. the same kernel, graph, task resources, logical inputs, requested outputs,
   one-task/one-lease rule and fork execution semantics;
3. exact logical/submitted/completed counts, zero failures and identical result
   digests for every paired run;
4. worker-process CPU time within the declared tolerance for fixed-CPU cases;
5. raw repetitions, medians, paired ratios, bootstrap 95% confidence intervals,
   coordinator and manager resource counters, transfer counters and DataVine
   internal stage timings;
6. every case where the DataVine upper 95% ratio bound is below 1.0 has a
   machine-readable bottleneck classification and a confirmation experiment.
   `unresolved` is a failed publication gate, never explanatory prose.

## Independent variables

The pool never changes.  The controlled variables are:

| Axis | Values or shapes |
|---|---|
| topology | independent map, cohort chains, fan-out, fan-in, diamond, cyclic-degree pipeline, reduction tree, shared broadcast, serial dynamic append |
| target CPU/task | 0, 1, 10, 100, 1,000 and 10,000 ms of child process CPU |
| payload/edge | 0 B, 1 KiB, 64 KiB, 1 MiB and 32 MiB at bounded graph widths |
| requested output/task | 0 B, 1 KiB, 64 KiB, 1 MiB and bounded 32 MiB cases |
| maximum indegree | 0, 1, 4, 16 and 64 |
| maximum outdegree | 0, 1, 4, 16, 64 and 1024 broadcast |
| duration shape | fixed and a declared heavy-tail mixture |

CPU duration is measured inside each fork child with `process_time_ns()`.  The
0 ms case skips the CPU loop.  Output construction, serialization and transfer
remain outside the requested CPU interval and are intentionally visible.

## Experimental design

A full Cartesian product would spend most allocation time on redundant points
and make causal diagnosis harder.  The fixed manifest therefore uses staged
one-factor sweeps plus selected interactions and confirmation runs.

### Phase A: harness and admission

- Validate manifest/schema, local reference results and deterministic digests.
- Start each backend pool and fail unless exactly 64 x 16 cores connect.
- Record worker inventory and environment; run warmup outside measurements.
- Run immediate-return/zero-byte and fixed-CPU controls.

Deliverable: `admission.json`, environment inventory and control-case raw runs.

### Phase B: CPU amortization

- Independent four-wave maps at 0/1/10/100/1000/10000 ms.
- Keep graph width, outputs and all 1024 cores fixed.
- Locate the point at which control-plane overhead is amortized.

Deliverable: CPU sweep with useful-CPU equality and paired confidence intervals.

### Phase C: input and output movement

- Sweep requested output size on independent maps.
- Sweep shared input size with 1024-way broadcast.
- Separate retained sink bytes from recomputable intermediate bytes.
- Use bounded widths for 32 MiB points while retaining the 1024-core pool.

Deliverable: network/write/controller amplification and size-slope models.

### Phase D: graph structure and degree

- Compare map, chains, fan-out, fan-in, diamond, pipeline, tree and dynamic
  append at a common CPU and payload point.
- Sweep indegree and outdegree through 1/4/16/64 with constant 1024-wide layers.

Deliverable: topology table plus per-edge and per-task overhead estimates.

### Phase E: interactions and bottleneck confirmation

- Cross representative CPU (0/100 ms), payload (0/64 KiB/1 MiB), and degree
  (1/16/64) points.
- Automatically select confirmation cases for every DataVine regression:
  zero-sink versus durable sink, zero-payload versus payload, degree-1 versus
  high-degree, and 0 ms versus CPU-amortized controls as applicable.

Deliverable: one evidence-backed cause for every regression, with ambiguous
cases marked `unresolved` and therefore failing the publication gate.

The fixed matrix contains 49 cases.  The final case is a 128-way, zero-byte
broadcast matched control for the bounded 32 MiB broadcast; it changes only
the payload axis and is required for causal attribution of that large-input
case.

### Phase F: final paired campaign

- Ten measured repetitions per selected case after one unmeasured warmup.
- Alternate backend order by block and retain order as a factor.
- Compute deterministic paired bootstrap 95% intervals with 10,000 resamples.
- Repeat any unstable case rather than deleting outliers.

Deliverable: immutable raw artifacts, derived summary and final report.

## Final campaign outcome

The post-upgrade fixed-core campaign completed 49 cases, one warmup per
case/backend and ten paired measured repetitions: 98 accepted warmups plus 980
accepted backend runs. Every accepted measurement used exactly 64 workers x 16
cores per backend, exact result digests and exact logical/physical task counts.
Attempts overlapping external Condor eviction were retained as 28 invalidated
runs and excluded by contract. All publication gates pass.

Using the paired 95% interval, DataVine is significantly faster in 46 cases,
significantly slower in 2, and statistically inconclusive in 1:

| Representative case | TV median s | DV median s | TV/DV rate | Paired 95% CI | Result |
|---|---:|---:|---:|---|---|
| CPU 0 ms, 4096 tasks | 36.749 | 19.611 | 1.874 | [1.773, 2.008] | DataVine faster |
| CPU 10 s, 4096 tasks | 65.764 | 57.233 | 1.149 | [1.111, 1.160] | DataVine faster |
| 128 x 32 MiB requested outputs | 13.008 | 11.224 | 1.159 | [1.055, 1.274] | DataVine faster |
| 32 MiB input broadcast to 128 consumers | 1.262 | 1.371 | 0.920 | [0.872, 0.966] | DataVine slower |
| degree 64 regular pipeline | 25.230 | 15.487 | 1.548 | [1.491, 1.583] | DataVine faster |
| fan-in 16 | 9.113 | 4.674 | 1.950 | [1.900, 2.240] | DataVine faster |
| heavy tail | 9.916 | 5.607 | 1.769 | [1.636, 1.885] | DataVine faster |
| dynamic serial append | 0.186 | 1.007 | 0.184 | [0.149, 0.202] | DataVine slower |

### Confirmed DataVine bottlenecks

1. **32 MiB broadcast is limited by requested-result fetch, not input
   transfer.** The median DataVine excess is 0.109 s. Separated fetch timing is
   0.121637 s for DataVine versus 0.000302 s for TaskVine, a 0.121335 s direct
   critical-path excess that covers the complete wall gap. Useful CPU agrees
   within 0.003%. The next interface is parallel/streaming multi-DataID result
   reads; scheduling and peer input placement do not need to change.
2. **Dynamic serial append re-enters the Runtime nine times.** Eight dependent
   submit/result steps plus seal produce median `runtime_invocations=9`.
   DataVine terminal-wait excess is 0.986509 s against a 0.821531 s wall gap,
   while useful CPU is equal. The fix is a resident WorkflowSession runtime
   lane that accepts appended tasks and returns completions without re-running
   setup/quiescence/checkpoint projection each step, then seals once.

### Throughput

- 4096 immediate one-core tasks: DataVine 208.9 tasks/s versus TaskVine 111.5
  tasks/s (1.874x).
- 4096 tasks at fixed 10 s CPU: DataVine 71.6 tasks/s versus TaskVine 62.3
  tasks/s (1.149x); useful CPU equality remains enforced.
- fan-in 16: DataVine 232.8 tasks/s versus TaskVine 119.4 tasks/s (1.950x).
- cold SharedFS eData ingest, 4096 distinct 256-byte objects: 83.4 objects/s
  with one writer and 752.9 objects/s with 16 bounded writers. Warm deduplicated
  ingest reaches 3162.2 objects/s at 16 writers. Cold aggregate SharedFS time
  is 73.66 s behind 5.44 s wall time, confirming per-file durability metadata
  as parallel work amplification rather than serialization or RPC.

The complete generated table and per-regression evidence are in
`acceptance/1024-core-workflows/data-plane-v2-current-final-20260821/report.md`;
the compact artifact
retains all 980 measured records and 98 warmups without embedding large result
values.

## Bottleneck classification contract

Classification uses measurements, not intuition:

| Class | Required evidence | Improvement direction |
|---|---|---|
| fixed-control | regression is largest at 0 ms/0 B and shrinks with CPU | reduce RPC, graph materialization and completion projection per task |
| graph-materialization | DataVine materialize/build excess grows with tasks or edges and degree confirmation isolates it | compact bulk graph ingestion, indexed dependencies, fewer copies |
| scheduler/submit | submit or scheduling time dominates and grows with ready width | bulk native submission and completion draining without semantic batching |
| input-transfer | bytes/time sent and size slope explain the gap | locality, cache hit, peer transfer, input declaration deduplication |
| executor | useful CPU differs or execution residual remains after control/data costs | fork lifecycle, decode, callable invocation and worker profiling |
| publication | publication/fetch/controller write time and requested-byte slope explain the gap | retained-output batching, hashing/fsync and fetch path |
| dependency/serialization | gap grows with indegree or logical edge bytes after payload control | compact DataID bindings and avoid repeated Python serialization |
| unresolved | no class meets its quantitative rule | collect a targeted trace; publication is blocked |

The final report must state both the absolute excess seconds and its fraction
of the DataVine minus TaskVine wall-time gap.  An optimization is proposed only
after its owner and confirmation gate are identified.

## Final deliverables

- versioned campaign manifest and artifact schema;
- reusable 1024-core runner and dry-run matrix renderer;
- raw result per backend/case/repetition, worker inventory and logs;
- exact-result and exact-task-count audit;
- paired statistical summary and machine-readable bottleneck report;
- Markdown/CSV tables and plots generated from raw JSON only;
- reproduction commands, commit/environment/package hashes;
- updated acceptance matrix, progress, agent plan and handoff checksums.

No 1024-core performance claim is allowed from local tests, a partial pool,
different core counts, failed tasks, mismatched outputs, missing attribution or
historical 160-core artifacts.
