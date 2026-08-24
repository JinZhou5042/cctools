# Data-intensive million-task benchmark

This benchmark is a fail-closed comparison of DataVine and traditional
TaskVine under high task concurrency, high file cardinality, worker-to-worker
movement, SharedFS input pressure, and intermediate-result garbage collection.
It does not use sleeps or semantic task batching: one logical task is one
physical TaskVine execution.

## Frozen full workload

The acceptance contract SHA-256 is
`796255d97815b1dca6ed192931f04d8c97ee086bdfad49b532a14bedddc45ee0`.
`acceptance/scripts/data_intensive_workload.py` is the executable source of
truth.

| Item | Exact value |
|---|---:|
| Cohorts | 64 |
| A tasks | 262,144 |
| B tasks | 655,360 |
| C tasks | 131,072 |
| All tasks | 1,048,576 |
| Source files | 9,437,184 |
| Task output files | 1,048,576 |
| Workflow payload files | 10,485,760 |
| Shared code objects | 3 |
| Static IR data records | 10,485,763 |
| External input references | 9,437,184 |
| Scheduler edges | 5,898,240 |
| All input references | 15,335,424 |
| Workers | exactly 128 |
| Cores per worker | exactly 16 |
| Aggregate cores | exactly 2,048 |

Each cohort is independent, so no single global barrier separates A, B, and C.
Within a cohort:

- 4,096 A tasks each read 36 unique source files and write one 512 KiB file.
- 10,240 B tasks each read eight A outputs and write one 256 KiB file.
- 2,048 C tasks each read five B outputs and write one 512 KiB file.
- A-to-B is a deterministic regular graph. Every A output has exactly 20 B
  consumers, while every B output has exactly one C consumer.
- Only C outputs are requested and durable. The 917,504 A/B intermediates,
  totaling 288 GiB, must exercise worker cache movement and GC.

Source sizes repeat an exact 64-file distribution: 29 x 1 KiB, 19 x 16 KiB,
13 x 128 KiB, and 3 x 1 MiB. The resulting byte budget is:

| Data | Bytes | Binary size |
|---|---:|---:|
| Sources | 765,393,371,136 | 712.828125 GiB |
| A outputs | 137,438,953,472 | 128 GiB |
| B outputs | 171,798,691,840 | 160 GiB |
| C outputs | 68,719,476,736 | 64 GiB |
| Stored artifacts | 1,143,350,493,184 | 1.039871 TiB |
| Logical reads | 3,685,971,132,416 | 3.352371 TiB |
| Logical read + write path | 4,063,928,254,464 | 3.696121 TiB |
| Hard stored-byte limit | 2,199,023,255,552 | 2 TiB |

The factory advertises 4 GiB disk per worker. Thus the source dataset, durable
C results, and maximum aggregate advertised worker scratch remain below 2 TiB.
The run still records actual storage and fails closed on all observable byte
contracts.

## Task kernel

Every task performs real work:

1. It shuffles its input-file order deterministically.
2. It shuffles 64 KiB `pread` offsets within each file and reads every byte.
3. It performs a process-CPU-time busy loop. The deterministic duration mix is
   80% 2-10 ms, 15% 10-50 ms, 4% 50-500 ms, 0.9% 0.5-2 s, and 0.1% 2-5 s.
4. It writes the exact stage output size without sparse files or sleep-based
   simulation.

DataVine uses `python source-v1` through the managed FunctionCall-fork library.
TaskVine uses the same kernel through a FunctionCall-fork library. Sampled C
output SHA-256 values must match across backends before a comparison is valid.

## Dataset generation

The full dataset is split into 128 independent parts. Each part owns exactly
73,728 source files and can safely resume a verified complete or partial file.
It never silently overwrites conflicting content. A part manifest is installed
atomically only after every file in that part is complete and non-sparse.

Plan without writing data:

```sh
export PYTHONNOUSERSITE=1
PY=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin/python
$PY acceptance/scripts/generate_data_intensive_dataset.py \
  --acceptance plan --parts 128
```

Submit all generators:

```sh
RUN=/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/full-v1-20260823
mkdir -p "$RUN/dataset" "$RUN/logs"
condor_submit \
  repository=/users/jzhou24/cctools_repo/datavine \
  python_bin="$PY" dataset_root="$RUN/dataset" run_root="$RUN" \
  acceptance/scripts/data_intensive_dataset.condor.sub
```

After all 128 part manifests exist, perform the metadata gate first. The final
full-hash gate rereads 712.8 GiB with 128 independent Condor verifiers. Each
atomic verification artifact is bound to the current part-manifest digest;
the final assembler verifies all 128 artifact digests before accepting them.

```sh
$PY acceptance/scripts/generate_data_intensive_dataset.py \
  --acceptance assemble --root "$RUN/dataset" --parts 128

mkdir -p "$RUN/full-hash-parts"
condor_submit \
  repository=/users/jzhou24/cctools_repo/datavine \
  python_bin="$PY" dataset_root="$RUN/dataset" run_root="$RUN" \
  verification_root="$RUN/full-hash-parts" \
  acceptance/scripts/data_intensive_verify.condor.sub

# Run only after all 128 full-hash artifacts exist.
$PY acceptance/scripts/generate_data_intensive_dataset.py --acceptance assemble \
  --root "$RUN/dataset" --parts 128 --full-hash \
  --verification-root "$RUN/full-hash-parts"
```

## Running the pair

Use the production environment and current checked build:

```sh
export PATH=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin:$PATH
export PYTHONNOUSERSITE=1
export DATAVINE_GO_BINARY=/users/jzhou24/graph_optimization/factories/datavine_workflow_go-20260811
PY=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin/python
RUN=/project01/ndcms/jzhou24/datavine-benchmarks/data-intensive-large-scale/full-v1-20260823

$PY acceptance/scripts/benchmark_data_intensive_taskvine.py \
  --acceptance --dataset-root "$RUN/dataset" \
  --output-dir "$RUN/taskvine-r1"

$PY acceptance/scripts/benchmark_data_intensive_datavine.py \
  --acceptance --dataset-root "$RUN/dataset" \
  --output-dir "$RUN/datavine-r1"
```

Run the backends sequentially, in alternating order across repetitions, so they
do not contend with each other. Five successful pairs are required for a
production claim. The resumable campaign driver freezes the commit and dataset
digest, alternates backend order, and records an atomic state file:

```sh
$PY acceptance/scripts/run_data_intensive_campaign.py --acceptance \
  --dataset-root "$RUN/dataset" --run-root "$RUN/campaign"
```

The equivalent manual comparison command is:

```sh
$PY acceptance/scripts/compare_data_intensive_runs.py \
  --datavine "$RUN/datavine-r1" --taskvine "$RUN/taskvine-r1" \
  --datavine "$RUN/datavine-r2" --taskvine "$RUN/taskvine-r2" \
  --datavine "$RUN/datavine-r3" --taskvine "$RUN/taskvine-r3" \
  --datavine "$RUN/datavine-r4" --taskvine "$RUN/taskvine-r4" \
  --datavine "$RUN/datavine-r5" --taskvine "$RUN/taskvine-r5" \
  --minimum-repetitions 5 --output "$RUN/comparison.json"
```

## Acceptance and claim boundary

A DataVine run is invalid unless all of these hold:

- workflow, physical submissions, and physical completions are exactly
  1,048,576;
- the resident pool is exactly 128 x 16 and no worker is removed;
- between 5% and 90% completion, READY + RUNNING never falls below 32,768 and
  active cores never fall below 90% of 2,048;
- all 1,048,576 retained outputs use the worker-local/peer path;
- exactly 131,072 requested C outputs are durable;
- task output payload bytes bypass the manager;
- SharedFS source-stage bytes equal the 712.8 GiB source dataset exactly; the
  three shared code payloads are inline IR records and do not enter that count;
- the 16,384-entry recovery cache reaches its limit and accounts for every
  required eviction from the 917,504 intermediates;
- sampled result sizes and SHA-256 values match TaskVine.

The TaskVine baseline uses the same exact-pool, no-worker-removal, READY +
RUNNING, and 90%-active-core gates. Its sampler records at one-second cadence,
so evidence size is proportional to run time rather than the million-task
count. It declares the 9,437,184 source inputs with TaskVine's native
`declare_url` and canonical `file:///project01/...` URIs. Thus both backends
read source data from the same SharedFS path; manager-byte differences measure downstream
intermediate/result movement rather than an avoidable baseline source relay.
Source URLs use task-scoped cache because each is consumed exactly once. This
prevents 712.8 GiB of one-use inputs from accumulating against the 512 GiB
aggregate worker scratch budget; A/B temporary outputs remain workflow-scoped
and provide the intended movement and GC pressure.
It also seals the complete graph before starting the factory. During admission,
an unreachable `wait-for-workers=129` scheduling threshold keeps execution
closed until the driver observes exactly 128 workers and 2,048 cores, then the
driver opens scheduling and starts the execution timer.

The comparison reports a DataVine advantage only when all five full pairs pass,
the median TaskVine/DataVine execution-time ratio is above one, and manager
data-plane bytes are reduced. A pilot, a failed baseline, a mismatched result,
or a run without worker attribution remains `OPEN` and cannot support a
production performance claim.

## Current evidence

The 1 x 1 tiny-profile mechanism pilot is PASS for both backends with 256/256
logical/physical tasks and matching sampled output hashes. Its single paired
execution measured 27.01 s for TaskVine and 26.21 s for DataVine (1.03x) with
76.38% fewer manager data-plane bytes, while DataVine reported zero manager task-output payload bytes and only 32 requested
durable outputs. This is useful mechanism evidence only. The full 128 x 16,
five-pair performance result remains OPEN until its dataset and runs complete.
