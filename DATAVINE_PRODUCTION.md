# DataVine production contract

Updated: 2026-09-01

Status: production candidate with strict-singleton regression 21/21 PASS,
committed provenance and independently verified candidate package. Promotion
over the existing canonical package remains an explicit operator action.

## Scope

One `datavine_workflow` process runs one workflow. A user who needs another
workflow starts another process. DataVine supports one v1 protocol family and
does not migrate retired journal result formats or negotiate old execution
paths.

Every logical task is one physical TaskVine task per attempt. Workflow-delta
framing, compact IR and native parametric descriptions reduce registration
cost; they never batch task execution or completion.

## Ownership

- Scheduler: dependency counters, ready state, dispatch, completion and retry.
- TaskVine Manager: Worker connections and physical task transport. It carries
  one opaque DataVine task frame but owns no DataID, replica or GC policy.
- Data Controller: DataID metadata, Worker sessions, replica state, requested
  result persistence, loss transitions and GC decisions.
- Worker Data Agent: local replicas, input resolution, peer transfer,
  publication metadata and release acknowledgement.

Task completion and data progress are independent. Physical success marks the
logical task done and releases children immediately. A child may wait on its
Worker for data, but Scheduler dispatch never waits for Controller persistence.

Dynamic Python invocation records are control metadata. Up to 64 KiB travels
inside a DVP2 `function_input` task frame and never enters the SharedFS object
store. Larger invocations retain the DVP1 object path. The reusable callable
object is still content-addressed and Worker-cached.

## Data path

Ordinary outputs stay Worker-local and volatile. Consumers resolve locally or
peer-first. Open workflows retain an otherwise unused output because a later
delta may add a consumer; sealing releases outputs with no consumer and no
result request.

The default workflow policy `idata_backup: controller-background` adds a
Controller-local safety replica for every live retained output. Workflows may
explicitly select `worker-local` to disable backup for controlled baseline
comparisons. Backup admission is not on task completion or child dispatch:
publication queues only the compact DataID, and at most one quarter of the 16
data threads copy background data.

Backup is additive: admitting the Controller copy never removes the Worker
local replica. Input resolution remains local-first, then peer-first while any
live Worker replica exists, with the Controller copy used only as fallback.
This boundary is performance-critical. In two 10,000-file, 10,000-MiB Condor
runs with 32 producer and 64 consumer Workers, peer delivery sustained a
median 1,779 MiB/s versus 832 MiB/s through the Controller, a 2.14x speedup.
The slowest peer run still exceeded the fastest Controller run by 32 percent.
Controller routing sent about 10.98 GB per run through the Frontend; peer
routing left only about 10--11 MB of Frontend control traffic. Evidence and
the exact route-isolation method are in
`acceptance/peer-vs-controller-20260831.json`.
Foreground requested-result persistence always uses the high-priority queue;
Worker input resolution also pauses new background admission briefly. A
million-entry backlog therefore uses about 8 MiB rather than one heap object
per file, while at least 12 data threads remain available to foreground work.
Only one background pull may wait on a given Worker's persistent connection,
so a foreground pull cannot land behind a queue of background files; different
Workers can still supply four backups concurrently.

Background copies use the same framed Worker stream, SHA-256 verification,
`fsync`, private temporary file and atomic rename as requested results, but do
not enter the requested-result journal or delay logical progress. Once a copy
exists, loss of the last Worker replica does not invalidate its producer. A
new Worker obtains a signed, range-bounded Controller ticket, downloads the
copy in 1 MiB chunks, verifies the complete digest, and republishes a normal
Worker replica. Static and dynamically appended consumers use this identical
resolution path.

### Unified static/dynamic DataID lifecycle

Dynamic append changes only graph metadata: tasks, DataID definitions,
consumer counts and requested-result intent. It does not create a second data
plane. A newly appended task resolves generated inputs exactly like a task in
the initial static graph: exact local generation, then a live peer, then an
admitted Controller backup, otherwise publication wait or lazy producer
replay.

The dense replica table is the live state machine; the Controller result
catalog is the payload directory. After journal replay, a durable result is
hydrated into the dense table on its first Worker resolve from immutable
`{DataID, generation, size, SHA-256}` identity. This is O(1) per DataID used,
does not scan persisted files at startup and rejects a stale generation. It
closes late-consumer-after-restart without adding file state to Scheduler or a
dynamic-only table.

Open workflows retain generated outputs because a future delta may add a
consumer. Seal moves outputs with neither a remaining consumer nor a result
request to dead: Worker replicas are released and unrequested Controller
backups are removed. External source data and generated data share one DataID
namespace and Worker Agent lifecycle; immutable origins use the source-fetch
branch while generated data uses the replica resolver. Small Python invocations
remain bounded DVP2 control frames and do not become files.

Requested outputs use one path for every file size:

1. Worker reports the replica and continues task progress.
2. One of 16 fixed Controller data threads pulls a stream from the Worker.
3. After the complete framed body is consumed, the per-Worker connection is
   released for reuse while the Controller calls `fsync` and atomically
   renames the private `/tmp` file.
4. Result metadata is committed only after local durability succeeds.

A live dynamic frontend consumes requested control results through one
Controller-admitted result stream. The stream is woken by Controller admission,
not Scheduler completion, and resumes by a monotonic sequence reconstructed
from `DATA_READY_BATCH` journal order. Results up to 64 KiB may accompany the
stream record after durability; larger results use the existing descriptor.

`fsync` is required by this contract. In particular, delayed local writeback
errors such as `ENOSPC` or `EIO` may not be reported by `write`, `close` or
`rename`; the Controller must see the `fsync` result before advertising the
requested output. Removing or deferring it would redefine requested results as
page-cache accepted rather than locally durable and is therefore an explicit
policy change, not a performance-only implementation choice.

Worker-direct SharedFS persistence, Controller DRAM payload caching and
file-size routing are not production paths. `/tmp` survives Worker loss but
not Controller-host loss, reboot or external cleanup. Cross-host durability is
a future explicit feature, not an implicit fallback. The containing directory
is not fsynced after rename, consistent with that host-crash boundary.

## Failure semantics

- Worker disconnect: Controller removes that Worker incarnation and its
  replicas. Running physical tasks are retried by TaskVine. A completed output
  with no remaining replica is recovered only when a live consumer or result
  request needs it. In `controller-background` mode an admitted backup is a
  valid remaining replica; otherwise normal producer recovery is used.
- I/O, capacity, digest or persistence failure: the affected publication fails
  closed. Metadata is never committed before the durable local rename.
- Restart: current v1 workflow and Controller journals replay. Retired
  payload-in-workflow-journal result records are rejected rather than copied
  into a compatibility data plane. Durable result identities enter the live
  replica table lazily, so restart cost is independent of files no later task
  reads.
- Corrupt or stale generation reports cannot replace a newer replica/result.

The `ENOSPC`/`EIO` contract is enforced without a production fault hook by
`TR_datavine_persistence_faults.sh`. It injects errors at the private result
file `fsync` boundary and verifies no phantom metadata, no temporary-file leak,
recovery from the same journal, exactly one successful commit and replay without
a Worker.

## Fixed performance choices

- one workflow reactor and one Manager owner;
- one Controller workflow identity, bound by replay or first submission;
- paged DataID and Worker arrays with integer-indexed replica/waiter arenas;
- no generic hash table or `itable` in Controller metadata paths;
- one stable pull connection per Worker slot, reset in place on reconnect;
- DataVine enables `prefer-dispatch` so an already-READY task refills a free
  Worker slot before a retrieved completion is returned to the reactor;
- one metadata RPC event-loop thread;
- one eventfd-driven Controller result stream; no live completion polling;
- immediate nonblocking small-response writes; `EPOLLOUT` only on backpressure;
- connection maintenance bounded to 10 ms resolution under sustained traffic;
- worker-first physical dispatch, depth 1, bounded at 4,096 sends per turn;
- a generic, opt-in dense Worker availability ring with generation-checked
  slots; ordinary TaskVine does not allocate it and retains task-first by
  default;
- direct integer parsing for hot task-description and completion records; the
  line protocol itself is unchanged;
- 16 sleeping Controller data threads;
- background iData backup is enabled by default, limited to four concurrent copies, and
  represented by an 8-byte DataID backlog plus one queued bit per live DataID;
- signed Controller-to-Worker fallback is chunked and digest-verified;
- persistent Worker/Controller transfer sockets tuned for interactive latency;
- Worker connection locks cover framed transport, not local durability;
- no per-Worker or per-file thread creation;
- no per-task scheduling priority queue in the DataVine path;
- no TaskVine transaction/taskgraph/performance/debug logs unless explicitly
  enabled for diagnosis.

For a DataVine FunctionCall, the Worker materializes TaskVine's in-memory
return bytes into the declared sandbox output immediately before Agent commit.
The Agent therefore uses the same digest/publication path as command output;
materialization failure is reported fail-closed.

Generic TaskVine retains its normal scheduler and file machinery as the
baseline. DataVine does not carry a second runtime strategy merely to perform
the comparison.

A second workflow ID is rejected fail-closed. Historical tests that submit
several workflows to one `serve` process are not production acceptance; each
workflow must start its own process and journal.

## External contract

- workflow schema: `datavine.workflow/v1`
- delta schema: `datavine.workflow-delta/v1`
- native RPC: version 1
- Python source/callable: `source-v1` and `callable-v1`
- TaskVine executor: `builtin-v1`
- Python ticket/output manifest: `DVP1` object fallback, `DVP2` inline
  invocation and `DVM1` output manifest

Compact records and defaults are accepted through `vine_datavine_ir.c`; full
v1 records remain valid inputs, not a separate runtime. The Controller address
and physical file paths never enter durable Workflow IR.

## Maintainer map

- `taskvine/src/datavine/vine_datavine_workflow_runtime.c`: workflow reactor
  and Scheduler integration.
- `taskvine/src/datavine/vine_datavine_data_controller.c`: data authority and
  requested-result persistence.
- `taskvine/src/worker/vine_datavine_agent.c`: Worker-local data plane.
- `taskvine/src/datavine/vine_datavine_workflow_store.c`: workflow state and
  task-event journal, never result payload storage.
- `taskvine/src/datavine/vine_datavine_rpc.c`: bounded metadata/client RPC.
- `taskvine/src/manager/vine_manager.c` and `vine_worker_pool.c`: generic
  worker-first dispatch and optional dense availability index used by
  DataVine.

## Acceptance

The current commands and state are in `acceptance/README.md`,
`acceptance/matrix.md` and `progress.md`. Current storage evidence is under
`acceptance/controller-local-tmp-20260827/` and must be verified from that
directory with `sha256sum -c SHA256SUMS`. Deterministic persistence-failure
evidence is `acceptance/controller-persistence-faults-20260828.json`.

The apparent drop from roughly 12k dummy tasks/s to roughly 1.1k tasks/s in
the first background-iData benchmark was a comparison-boundary error, not a
tenfold Controller regression. The 12k result uses persistent builtin tasks
distributed over many Worker processes; the 1.1k result uses one 16-slot
Worker that forks `/bin/true` and publishes one output per task. With current
binaries, 16 one-core Workers sustain 12,166 builtin tasks/s while publishing
one empty live iData each, and 12,218 logical tasks/s with background backup
enabled. The backup then drains independently at 6,637 files/s. The complete
controlled comparison is
`acceptance/throughput-scale-comparison-20260831.json`; throughput claims must
always state executor, Worker count, cores per Worker, output lifecycle and
whether backup-drain time is included.

A five-run, 50,000-file follow-up isolates the zero-byte backup ceiling. Once
a backlog exists, four background copy slots drain a median 7,786 files/s
(range 7,707--7,938). Measured from task submission through the final durable
copy, including backup ramp-up, the median is 6,236 files/s. Payload bytes are
zero, so this case measures fixed per-file TCP request, file creation, `fsync`,
digest and rename cost rather than network bandwidth. Evidence is
`acceptance/empty-idata-backup-throughput-20260831.json`.
