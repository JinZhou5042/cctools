# DataVine production data plane

## Contract

DataVine separates data ownership from task scheduling.

- Workflow IR and the scheduler contain logical `DataID` dependencies only.
- External immutable data is represented as `{kind: "object", sha256: ...}`.
- One content object is one file in the v1 store. The store validates SHA-256,
  writes through a private temporary file, fsyncs, and atomically links the
  finished object into a hash-sharded directory.
- A v1 object is at most 67,108,800 bytes because the binary RPC frame is 64
  MiB including its 64-byte digest. The service advertises this limit as
  `object_max_bytes`; future chunked objects can extend the object-store
  interface without changing DataIDs or scheduler contracts.
- The Data Controller is the single metadata/lifecycle owner. SharedFS holds
  object bytes; the Controller owns content identity, locations, persistence
  state, result catalog, publication, recovery, pruning and GC decisions. It
  does not cache output payloads in Controller memory.
- The scheduler never resolves locations, probes availability, transfers bytes,
  serializes Python values, or publishes outputs.
- A worker receives only task/DataID metadata, then actively resolves and pulls
  missing data. Worker-local cache and peer transfer are preferred; immutable
  SharedFS content is the durable fallback. SHA-256 is verified before cache
  install.
- Function and invocation bytes use the production callable-v1/DVP1 contract. They are not declared as
  scheduler inputs. The fixed-width task descriptor carries two 32-byte content
  keys, deadline, output policy, and eData keys; it carries no serialized
  invocation, URI, service address, token, or output path. On SharedFS a worker
  resolves a cache miss directly from the content key with no Controller RPC,
  verifies SHA-256, and retains its bounded function cache. A future non-shared
  backend can add one batched miss resolver without changing the key contract.
- Requested eData uses `(workflow_key, DataID, attempt)`. The attempt prevents a
  retry from publishing a stale result. Object/result roots and persistence
  authentication are configured once per worker library rather than repeated
  in every task or output descriptor.

The current Controller address is deliberately absent from durable Workflow IR.
On service restart the same content/eData keys resolve under the new worker
context, so a new port or host does not change workflow semantics.

The native service uses one nonblocking epoll RPC thread by default.
`DATAVINE_SERVICE_THREADS` is the single bounded override (1--256). A static
client caches capabilities and the object root, submits the DAG once, parks one
terminal-wait request, then resolves every requested DataID in one metadata-only
descriptor request. Requested result bytes are read independently from SharedFS
through up to eight ordered local readers. This aggregates metadata, never
tasks: every task remains an independent scheduler dispatch, execution and
completion unit.

## Serialization and deduplication

Python remains the serialization owner; native C treats payloads as bytes.

1. A callable object is cloudpickled once per workflow snapshot.
2. Immutable primitive literals are cloudpickled once and reused.
3. Structurally identical invocation specifications reuse one invocation
   DataID, so repeated calls do not create duplicate eData records.
4. Remaining object bytes are hashed once in the adaptor. A workflow-local map
   suppresses duplicate SharedFS installations.
5. The adaptor writes each unique immutable object once to its hash-derived
   SharedFS path with bounded post-serialization concurrency, atomically links
   it, fsyncs it, and validates type and size locally. There is no redundant
   per-object registration RPC; readers verify SHA-256 independently.
6. The Data Controller maps one digest to one stable TaskVine file identity, so
   tasks and workflows reuse worker cache replicas and peer transfers.

Results use one Controller-owned two-level catalog:
`workflow_id -> itable<DataID, result>`. Lookup, retry replacement, workflow
loss and GC use integer DataIDs without allocating composite string keys.

Cloudpickle remains GIL-bound, but only unique semantic values are serialized.
Parallelism begins after immutable bytes exist: object PUT handling, worker
pulls, function-call child processes, output serialization, and publication can
all proceed concurrently.

## Output lifecycle and GC

Callable outputs are serialized and hashed on the worker in one write pass.
Task completion is reported to the scheduler immediately and does not wait for
durability. The worker Data Agent first sends an availability notice so
downstream consumers can use worker-local/peer transfer, then persists the
object lazily to its fixed SharedFS path and sends `RESULT_PERSISTED`. Only
metadata crosses the Controller RPC. The Controller can then prune stale
locations and run lifecycle/GC decisions without retaining payload bytes.
Requested outputs remain durable and fetchable after workers exit;
unrequested intermediates stay local/peer-first and are released when dead.

Scheduler recovery and data recovery remain separate. The TaskVine manager
detects a lost worker and resubmits lost running tasks. The Data Controller
removes that worker's locations and reports only permanently lost, still-live
DataIDs requiring producer recovery; it does not duplicate task retry logic.

Input objects form the durable content cache. V1 intentionally does not delete
them automatically: deleting a digest without a complete mark over retained
workflow journals could break restart or another workflow. The object-store API
is isolated behind the Data Controller so a future journal mark-and-sweep or
retention policy can be added without changing scheduler, worker, or Workflow
IR contracts. Temporary partial files are never visible as objects.

## Profiling

Every service writes `<workflow-journal>.profile` by default. Set
`DATAVINE_PROFILE_PATH` to select another artifact, set
`DATAVINE_WORKFLOW_METRICS=1` for the existing stderr stream, or set it to `0`
to disable service metrics.

The runtime profile records setup/materialization/submission, scheduling delay,
direct URL stage-in bytes and time, executor object-pull time, worker execution,
Python decode/function/serialization/fsync, stage-out, publication queue/commit,
checkpointing, and failures. `dominant_stage` uses a core-normalized
critical-path estimate rather than the misleading sum of all task wait times.

`Workflow.profile()` and `WorkflowSession.profile()` report adaptor-side
function/value/invocation serialization, hashing, unique and deduplicated object
records, PUT bytes/RPC time, and `dominant_ingest_stage`.

## Verified commands

```sh
PYTHONNOUSERSITE=1 bash taskvine/test/TR_datavine_data_plane_v2.sh run
PYTHONNOUSERSITE=1 python acceptance/scripts/compare_workflows.py --workflows map,fanin,large_reuse --repetitions 1 --workers 4 --cores 16 --batch-type condor
PYTHONNOUSERSITE=1 bash taskvine/test/TR_datavine_notebook_workflow.sh run
PYTHONNOUSERSITE=1 python3 acceptance/scripts/benchmark_edata_scale.py --tasks 1000000
PYTHONNOUSERSITE=1 python3 acceptance/scripts/benchmark_data_plane_profiles.py --tasks 16 --input-bytes 4194304 --cpu-ms 0 --unique-inputs --cores 8
```

The complete regression suite also requires either `DATAVINE_GO_BINARY` or
`DATAVINE_GO_COMPILER` for its language-neutral Go adaptor test.

## Known output boundary

Requested callable outputs no longer pass through Runtime or Controller memory.
The remaining measured boundary is client-side retrieval of many DataIDs: in
the 32 MiB broadcast case, separated result fetch adds 0.121335 s over
TaskVine and explains the full 0.109198 s wall regression. The clean next
extension is parallel/streaming multi-DataID SharedFS reads behind the existing
fetch interface. The second boundary is dynamic append: eight dependent tasks
plus seal currently enter the Runtime nine times; a resident WorkflowSession
lane should accept append/result cycles and seal once.
