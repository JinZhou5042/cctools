# DataVine production contract

Updated: 2026-10-06

Project navigation, source ownership and evidence entry points are indexed in
`DATAVINE_MAP.md`.

Delivery state and outstanding work are recorded in
[the handoff](DATAVINE_HANDOFF_20260827.md).

## Scope

One `datavine_workflow` process runs one workflow. A user who needs another
workflow starts another process. DataVine supports one v1 protocol family and
does not migrate retired journal result formats or negotiate old execution
paths.

Every logical task is one physical TaskVine task per attempt. Workflow-delta
framing, compact IR and native parametric descriptions reduce registration
cost; they never batch task execution or completion.

Workflow metadata recovery defaults to `policy.recovery: journal`. A workflow
may explicitly select `recovery: none` when Controller process loss already
means workflow loss. That process-lifetime mode keeps live status/events and
all data-plane semantics but writes no workflow submit, delta, task-event or
checkpoint records. It does not weaken requested-output persistence or change
`idata_backup`; those are independent Controller policies.

## Ownership

- Scheduler: dependency counters, ready state, dispatch, completion and retry.
- TaskVine Manager: Worker selection, connections and physical task transport. It carries
  one opaque DataVine task frame but owns no DataID, replica or GC policy.
- Data Controller: DataID metadata, Worker sessions, replica state, requested
  result persistence, loss transitions and GC decisions.
- Worker Data Agent: local replicas, input resolution, peer transfer,
  publication metadata and release acknowledgement.
- Python frontend: IR construction, serialization, RPC and result consumption.
  Submission and Session append share one builder path. The native validator
  owns IR acceptance; the frontend does not negotiate execution variants.

Task completion and data progress are independent. Physical success marks the
logical task done and releases children immediately. A child may wait on its
Worker for data, but Scheduler dispatch never waits for Controller persistence.

Dynamic Python invocation records are control metadata. Source calls use DVP1;
callable invocations up to 64 KiB use DVP3 inline task frames. Larger DVP4
invocations are uploaded by Controller RPC and referenced by a signed
content-addressed capability. Reusable callables are staged once in the Worker
library cache; per-call payloads stay in the task sandbox. No frontend or
Worker receives a Controller-local filesystem path.

## Data path

Outputs are initially published on Worker-local storage. Retained outputs
receive an asynchronous Controller backup under the default policy; they remain
volatile until a backup is admitted. Consumers resolve locally or peer-first. Open workflows retain an otherwise unused output because a later
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
The route-isolated comparison, storage matrix and dense crossover are indexed
in [acceptance/README.md](acceptance/README.md). They support peer-first bulk
delivery and do not establish a fixed file-size routing threshold.

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
remain bounded DVP3 control frames and do not become files.

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
  replica table lazily. This avoids eagerly hydrating every persisted DataID;
  journal replay still depends on retained journal contents. Recovery requires
  the journal and Controller files to remain available. `recovery:none` does
  not reconstruct the workflow after service loss.
- Corrupt or stale generation reports cannot replace a newer replica/result.

The retry contract contains only `maximum_attempts`. Failed logical attempts
may be retried within that budget; result-name filters are unsupported.

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
- least-loaded selection over the dense Worker ring, plus generation-bound
  recall of queued (never started) fork calls when a late Worker becomes idle;
- fork executors use an 8x dispatch queue and a Worker-owned elastic execution
  window. With C denoting Worker cores, it starts at 2C and samples every
  250 ms while active. Headroom permits growth within granted capacity. At
  CPU utilization of at least 95%, more than 3C observed runnable processes
  for two pressure epochs trigger a reduction by C, down to a 2C floor.
  The 3C value is a feedback threshold, not a hard execution-window cap.
  Memory pressure at 80% uses a separate halving/drain path, with predictive
  declared-memory checks before admission;
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

A second workflow ID is rejected fail-closed; each workflow requires its own
process and journal.

## External contract

- workflow schema: `datavine.workflow/v1`
- delta schema: `datavine.workflow-delta/v1`
- native RPC: version 1
- Python source/callable: `source-v1` and `callable-v1`
- TaskVine executor: `builtin-v1`
- Python execution frames: `DVP1` source, `DVP3` inline callable invocation,
  `DVP4` Controller-RPC callable reference, and `DVM1` output manifest

Compact records and defaults are accepted through `vine_datavine_ir.c`; full
v1 records remain valid inputs, not a separate runtime. The Controller address
and physical file paths never enter durable Workflow IR.

## Verification

Source ownership is indexed in [DATAVINE_MAP.md](DATAVINE_MAP.md). Build and
runtime commands are in [acceptance/README.md](acceptance/README.md); recorded
PASS results and their limits are in [acceptance/matrix.md](acceptance/matrix.md).
Performance comparisons must state executor, Worker topology, output lifecycle,
recovery policy and whether asynchronous backup drain is included.

## Preparation and diagnostics

Worker preparation requires a real library execution opportunity and input
readiness. The Worker has one elastic execution policy, with the constants
specified above. Alternative admission and eager-preparation controls have
been removed from production and fresh campaign entry points.
`DATAVINE_TRACE_ATTEMPTS=1` records worker-local receipt, preparation, execution,
and completion timestamps for auxiliary-payload attempts. It is disabled by
default. These timestamps are not synchronized across hosts.

Dense least-loaded dispatch visits each offered Worker at most once in a
bounded scheduling pass. Reoffering an incompatible low-load Worker must not
starve a recalled task's reserved destination. Worker-removal tests
explicitly enable `DATAVINE_TASKVINE_TRANSACTION_LOG=1`, because fault selection
must correlate connected Workers with jobs owned by the test factory.

Paper evidence and reproduction instructions live in
[paper](paper/README.md).

## Unified execution environment

Workers run inside the same installed Poncho environment. DataVine ships one
`datavine_executor` script and installs one `datavine-executor-v1` library.
Built-in noop/echo requests finish inline; Python source and callables run in
forked children of that same process. Python execution uses the interpreter
and dependencies supplied by Poncho. Task executors cannot specify an
`environment` override, including through `task_defaults`. Executor-path
overrides are removed; the service always stages its installed sibling executor.

Each Worker Agent holds one workflow for its current Manager connection and
releases that state on disconnect. Builtin noop and echo share the same inline
invocation format; there is no separate noop ticket decoder. The executor
terminates and reaps active Python children when its input pipe closes.

This replaces the separate C builtin and Python libraries. Native-only work
now requires the Poncho Python environment as well, and shares the execution
library's slot budget. Historical throughput measurements used the earlier
layout and do not establish this layout's performance.

DataVine always enables the dense Worker pool and least-loaded Worker selection.
Requested results use the existing result descriptor and fetch/stream interfaces;
the unused single-result path RPC has been removed. Scheduler recovery uses
its existing rebuild path rather than a separate DONE rollback operation.
