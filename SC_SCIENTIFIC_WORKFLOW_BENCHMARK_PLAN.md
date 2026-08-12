# Data-intensive scientific workflow benchmark plan

Date: 2026-08-11
Status: **DESIGN COMPLETE; IMPLEMENTATION AND RESERVED-NODE RUNS OPEN**

## Objective

Measure when DataVine's registered graph, native DataID lineage, worker-local
intermediates, peer transfer, selective retention, and restart recovery create
a real advantage over direct TaskVine. The suite must distinguish control-plane
speed, data movement, durability cost, and recovery behavior rather than reduce
all four to one end-to-end number.

The principal question is not whether DataVine makes an individual Python fork
faster. Both systems execute the same physical TaskVine work. The question is
whether DataVine reduces orchestration and repeated data movement enough to
offset its workflow-service and requested-result durability costs at scientific
scale.

## Systems and semantic contracts

Every workload is run against these explicitly named contracts:

1. **TV-native**: direct TaskVine `FuturesExecutor`/FunctionCall-fork. This is
   the performance ceiling for the current Python TaskVine reference, not a
   durability-equivalent implementation.
2. **DV-native**: current DataVine Workflow IR, native Runtime and Controller,
   worker-local `VINE_TEMP` intermediates, and durable requested results.
3. **TV-durable-sink**: optional diagnostic baseline in which requested TaskVine
   results are transferred, hashed, fsynced, and recorded by the benchmark
   harness on the same filesystem used by the Data Controller. This equalizes
   final sink I/O only. It does not claim graph replay, generation CAS, attach,
   or service-restart equivalence.

Results must always show DV/TV-native and DV/TV-durable-sink separately. A
TaskVine result may be called semantically equivalent only for a contract that
the driver has actually implemented and fault-tested.

## Controlled workload families

All kernels are deterministic, stream their input where scientifically
plausible, record useful child CPU, and produce exact digests and byte counts.
One logical task remains one physical TaskVine task; semantic batching is
forbidden.

| Family | Scientific analogue | Graph/data behavior | Primary hypothesis |
|---|---|---|---|
| Partitioned scan-reduce | HEP event analysis, telescope catalog scan | Many immutable shards -> filter/calibrate -> partial histograms -> tree reduction -> tiny final | Worker-local partials and registered reduction reduce coordinator traffic |
| Shared calibration fan-out | Detector calibration, reference genome/index reuse | Large immutable calibration/index reused by many shard tasks across waves | Cache and peer reuse approach one transfer per worker, not per task |
| Scatter-transform-shuffle-merge | Genomics align/sort/merge, external scientific sort | Partitioned inputs -> multi-output transform -> key redistribution -> hierarchical merge | Selective outputs and locality reduce bytes and file count |
| Tiled iterative stencil | Climate regridding, image deconvolution | Tiled state, neighbor halos, multiple iterations, periodic checkpoints | Retained tile locality helps across iterations; depth exposes scheduling overhead |
| Adaptive parameter search | Simulation calibration, Bayesian optimization | Batches produce metrics; client or native decision appends/prunes next batch | DataVine detach/append and durable decisions improve recovery, not single-step CPU |
| Ensemble checkpoint-reduce | Molecular dynamics/Monte Carlo ensembles | Independent long tasks emit checkpoints and small observables; failed members resume/recompute | Durable small checkpoints bound failure loss without persisting bulk trajectory data |

### Canonical initial configurations

The first publication-quality campaign uses fixed configurations so results are
comparable across commits:

| ID | Workload | Tasks | Input/intermediate scale | Requested output |
|---|---|---:|---:|---:|
| HEP-S | scan 256 shards, two transforms, 16-way tree reduce | 545 | 64 GiB input; 8 GiB transient | <=16 MiB |
| HEP-L | scan 4,096 shards, two transforms, 32-way tree reduce | about 8,325 | 1 TiB input; 128 GiB transient | <=64 MiB |
| CAL-L | 2 GiB calibration reused by 8 waves x 1,024 shards | 8,193 | 2 GiB shared + 512 GiB shard input | <=64 MiB |
| GEN-L | 2,048 scatter tasks x 4 outputs, 256 partitions, 3 merge levels | about 2,400 | 512 GiB input; 1 TiB logical shuffle | <=32 GiB |
| CLIM-L | 1,024 tiles x 20 iterations, 4-neighbor halos | 20,480 | 256 GiB state; 20 logical passes | one 256 GiB checkpoint plus <=1 GiB diagnostics |
| ADAPT-L | 64 initial simulations, 8 append rounds, 50% pruning/round | 120-256 | 4-16 GiB/model; 0.5-5 s decisions | all decision metrics, final best checkpoints |
| ENS-L | 4,096 members, 1-10 s heavy-tail, checkpoint every stage | 12,288+ | 256 GiB checkpoints; small observables | <=4 GiB |

Large configurations are not launched until the corresponding small pilot has
passed exactness, storage, resource, and cleanup gates. Generated data is
content-addressed and reproducible; a run records the generator seed and exact
manifest hash. Large repeated-byte blobs are forbidden because compression or
sparse-file behavior would invalidate transport measurements.

## Independent parameter axes

The controlled suite sweeps one axis at a time around a declared base case:

- input shard: 8 MiB, 64 MiB, 256 MiB, 1 GiB;
- intermediate/task: 1 MiB, 16 MiB, 128 MiB, 512 MiB;
- total working set: 1x, 2x, 8x aggregate worker cache;
- reuse fan-out: 1, 4, 16, 64 consumers per value;
- reuse distance: same wave, 1 wave, 4 waves, cache-pressure eviction;
- retained fraction: 0%, 1%, 10%, 50%, 100%;
- outputs/task: 1, 2, 4, 16, with consumed fraction varied independently;
- reduction fan-in: 2, 8, 32, 128;
- task CPU: 20 ms, 100 ms, 1 s, 10 s;
- duration skew: uniform, lognormal, and fixed 100x heavy tail;
- graph scale: 1K, 10K, 100K, and 1M physical tasks where data size permits;
- failure: no fault, one worker loss, 10% worker loss, executor crash, client
  detach, service restart, and cold restart after all worker-local state is gone.

The driver records the exact realized graph and bytes. Labels such as “1 TiB”
refer to logical payload bytes and must also report unique source bytes, bytes
read, bytes written, and network bytes.

### Theoretical byte lower bounds

Every graph generator emits an edge/placement manifest from which the driver
computes a lower bound before execution:

- partitioned scan: unique source bytes plus one copy of each partial along its
  selected reduction edge;
- shared calibration: unique shard bytes plus calibration bytes times the
  minimum number of workers that execute consumers;
- shuffle: unique source bytes plus exactly the retained key partitions that
  cross worker boundaries;
- iterative tile: initial state plus required cross-worker halo bytes per
  iteration plus declared checkpoints;
- requested sink: exact requested serialized bytes once, plus metadata.

Report `network amplification = measured network bytes / placement lower bound`,
`worker write amplification = worker bytes written / logically retained bytes`,
and `Controller amplification = Controller bytes / requested bytes`. A zero or
missing denominator is reported as not applicable, never silently converted to
zero. If actual placement cannot be reconstructed, no lower-bound efficiency
claim is permitted.

## Data-placement modes

Each applicable workload has four separate modes:

1. **Cold**: fresh worker sandboxes/caches; source input must be transferred or
   read once during the measured run.
2. **Warm**: the exact declared input is present in worker cache before timing;
   cache preparation is reported but excluded.
3. **Peer-required**: producers and selected consumers are deliberately placed
   on different workers; the run must prove peer transfer occurred.
4. **Cache-pressure**: unrelated content fills a declared percentage of cache
   between production and reuse, measuring eviction and recomputation.

Placement claims require TaskVine log/counter evidence. Merely observing a fast
run is not proof of a cache hit or peer transfer.

Peer-required mode is enabled only if both backends can express the same generic
worker-placement constraint and the artifact records producer/consumer worker
identities. If the current Workflow IR cannot do so without DataVine-specific
scheduler policy, the mode remains OPEN rather than adding a second placement
authority solely for the benchmark.

## Timing and resource metrics

### End-to-end phases

Report these independently and in total:

- graph construction and local serialization;
- submit/append RPC and workflow journal commit;
- physical task materialization and submission;
- worker input transfer/cache wait;
- fork startup, decode, useful function CPU, serialization, and fsync;
- peer transfer and output return;
- Controller prepare, queue, journal barriers, install, and requested fetch;
- terminal checkpoint and client-side result decode.

### Data and resource accounting

Per run retain:

- logical, submitted, completed, failed, retried, and recomputed task counts;
- unique source, logical edge, worker-read, worker-written, peer, manager, and
  Controller bytes;
- cache hits/misses/evictions and replicas per DataID where observable;
- requested versus non-requested files and bytes in Controller storage;
- Manager/service/worker/executor CPU, peak RSS, FDs, PIDs, disk bytes and
  network bytes;
- aggregate useful CPU, makespan, core utilization, throughput and cost per
  useful TiB;
- journal records/barriers/bytes and restart replay time;
- recovery detection latency, lost useful CPU, additional bytes, duplicate
  attempts, and time back to the pre-fault frontier.

Remote metrics are sampled on every reserved worker at one second or faster.
TaskVine counters are accepted only after a calibration run proves their byte
definitions for the executor path; otherwise `/proc`, worker logs and interface
counters are the authority and the limitation is stated.

## Correctness and storage gates

A run is invalid unless all gates pass:

- exact logical/physical task and retry counts;
- identical final scientific digests, shapes, record counts and numeric
  tolerances across backends;
- deterministic same-backend results across repetitions;
- no non-requested large intermediate in Controller storage;
- Controller bytes equal the declared requested set within metadata overhead;
- worker-local loss either retains an active replica or recomputes the precise
  producer lineage;
- no success from truncated output, missing partition, duplicate merge input,
  stale attempt or mismatched codec/hash;
- all Factory, Worker, service and batch jobs cleaned after the run.

Floating-point workloads use a predeclared tolerance and also compare invariant
counts/sums. Exact integer/digest fixtures remain alongside them to distinguish
numerical ordering from missing or duplicated data.

## Scale ladder

### Stage 0: local development

- 1 worker x 4 cores, <=16 GiB logical data, three repetitions;
- validates graph, metrics, result contract and cleanup only;
- cannot support a distributed performance claim.

### Stage 1: resident pilot

- 10 workers x 16 cores, 64-256 GiB logical data, five crossed-order
  repetitions;
- verifies cache, peer, retained-fraction and output-policy behavior;
- opportunistic Condor results remain a pilot.

### Stage 2: paired reserved-node campaign

- same homogeneous node set for both backends;
- strong scaling: 10, 25, 50 workers at fixed 1 TiB and fixed graph;
- weak scaling: 10, 25, 50 workers at 16 GiB input and 256 tasks per worker;
- at least ten paired repetitions/configuration;
- alternate backend order AB/BA and randomize workload order within each pair.

### Stage 3: stress and recovery

- 100 workers only after Stage 2 resource curves are healthy;
- 100K/1M task control cases, 1-10 TiB logical data cases, cache-pressure and
  injected failure campaigns;
- resource caps and stop conditions are declared before launch.

### Campaign budget and stop conditions

The suite uses screening followed by confirmation, not a Cartesian product.
Stage 1 screens each axis with three repetitions and advances only effects that
change wall time or bytes by at least 10%. Stage 2 freezes at most two
configurations per workload. The initial reserved campaign is therefore capped
at 240 successful paired backend runs: two primary workloads, strong and weak
scaling, three worker counts, and ten AB/BA pairs.

Each run declares maximum wall time, aggregate scratch bytes, Controller bytes,
journal bytes, PIDs, retries and worker losses. The driver cancels and marks the
run failed if any cap is exceeded, if fewer than the exact worker/core gate
remain connected, or if observed throughput for two consecutive intervals is
below the predeclared progress floor. Source datasets are retained by manifest;
per-run intermediates and caches are cleaned after evidence collection.

## Statistical contract

- Primary effect is the paired ratio `TaskVine wall / DataVine wall`.
- Publish every raw repetition, median paired ratio, bootstrap 95% confidence
  interval, median absolute deviation and paired effect size.
- Do not delete performance outliers. Exclude only a fail-closed infrastructure
  or correctness failure and preserve it in a rejected-run ledger.
- A speed claim requires the entire 95% interval above 1.0 and exactness gates.
- A parity claim requires the entire interval inside [0.95, 1.05].
- Strong-scaling efficiency and weak-scaling efficiency are reported with the
  same paired confidence method.
- Backend launch order, node identities, CPU model, memory, disk, kernel,
  network and package hashes are part of every artifact.

## Promotion hypotheses and gates

These are predeclared targets, not current claims:

1. **Control-heavy win:** DV-native >=1.5x TV-native for >=10K tasks with
   <=100 ms useful CPU/task and <=1 MiB outputs.
2. **Reuse efficiency:** after the first replica per worker, DataVine transfers
   no more than 1.25x the theoretical minimum shared-input bytes and at least
   2x fewer bytes than TV-native in CAL-L.
3. **Selective output:** Controller stores only requested bytes; at 10% retained
   fraction DataVine writes at least 5x fewer Controller bytes than a
   return-all baseline.
4. **Output-heavy parity:** DV-native reaches at least 0.90x TV-native on GEN-L
   before any broad performance promotion.
5. **Weak scaling:** >=80% efficiency from 10 to 50 workers for HEP-L and CAL-L.
6. **Recovery:** after one worker loss, DataVine returns the exact result with
   no stale attempt and <=1.5x the minimum producer recomputation; after service
   restart it resumes without client resubmission.

Failure of a hypothesis is a measured result, not a reason to change workload
parameters after inspection.

## Real application ladder

Synthetic acceptance precedes these application cases:

1. **ATLAS/Coffea event analysis**: fixed ROOT files and analysis code; preserve
   Coffea/WorkItems and replace only execution. Compare histogram contents and
   event cutflow exactly/tolerantly as appropriate.
2. **Genomics scatter/gather**: fixed public reads/reference/index with a
   containerized align/sort/merge pipeline; compare read counts, checksums and
   final index/statistics.
3. **Climate regrid or tiled array pipeline**: fixed NetCDF/Zarr input,
   conservative regridding and multi-step tile reuse; compare coordinate,
   missing-value and aggregate invariants.
4. **Adaptive simulation/notebook**: deterministic simulator fixture first,
   then a real parameter-search application with client detach and service
   restart.

Application environments are content-addressed and run with user-site packages
disabled. Input licenses and public retrieval manifests are recorded. Results
from synthetic substitutes and real applications are never mixed.

## Implementation deliverables

1. `acceptance/scripts/generate_scientific_data.py`: deterministic non-sparse
   shard generator and manifest verifier.
2. `acceptance/scripts/compare_scientific_workflows.py`: common graph models,
   three named backend contracts, placement modes, fault injection and paired
   run ledger.
3. `acceptance/scripts/sample_remote_resources.py`: worker/process/network/disk
   sampling with explicit provenance.
4. `acceptance/scientific-workflows/schema.json`: versioned raw artifact schema.
5. Focused executable contracts for byte accounting, cache/peer attribution,
   retained-fraction pruning, durable-sink parity and recovery attempts.
6. Immutable small-pilot, resident-pilot and reserved-node artifacts plus a
   concise report generated from raw JSON rather than hand-copied numbers.

The first implementation slice is HEP-S plus CAL-L-small because together they
exercise partitioned scan/reduce and shared-data reuse without requiring a
shuffle implementation. GEN-L follows only after per-worker byte accounting is
trusted. Climate, adaptive and fault campaigns follow in that order.

## Ordered execution

1. Implement artifact schema, deterministic generator and remote byte/resource
   sampler; calibrate accounting against known transfers.
2. Implement HEP-S scan/tree-reduce for TV-native, DV-native and
   TV-durable-sink; pass local exactness.
3. Implement calibration reuse and prove cold/warm/peer-required attribution.
4. Run Stage 1 10x16 pilots and freeze workload parameters.
5. Add GEN-L shuffle/merge and output-cardinality sweep.
6. Add climate iterative reuse, adaptive append/prune and fault injection.
7. Reserve homogeneous nodes and execute the paired Stage 2 matrix.
8. Only after confidence intervals and all correctness/storage gates pass,
   update performance claims or prioritize the next measured bottleneck.
