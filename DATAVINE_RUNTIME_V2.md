# DataVine Runtime v2: decoupled task and data planes

Status: **LOCAL NATIVE RUNTIME PASS; EXACT 128x16 CAMPAIGN OPEN**

Updated: 2026-08-25

## Hard boundary

DataVine Runtime v2 keeps TaskVine's ordinary compute scheduler intact while
removing DataVine files from the TaskVine Manager data plane.

| State or operation | Sole owner |
|---|---|
| Worker allocation, resources, physical task dispatch/completion | TaskVine Manager |
| Logical TaskID dependencies, readiness, and compute retry | DataVine Scheduler/Runtime |
| DataID identity, replicas, persistence, waiters, liveness, and recovery requests | Data Controller |
| Local paths, atomic installation, pins, disk pressure, and physical deletion | Worker Data Agent |
| Payload bytes | Worker, peer worker, immutable SharedFS/origin |

The Manager carries a generic opaque `auxiliary_payload` frame. It must
not create a `vine_file`, cache name, replica-table entry, path, hash, transfer,
or unlink for a DataVine input or output.

Static and runtime gates enforce:

```text
DataVine vine_file entries in Manager             = 0
DataVine cache-update events to Manager            = 0
DataVine unlink commands from Manager              = 0
DataVine task-output payload bytes through Manager = 0
```

The traditional TaskVine file path remains available and unchanged for
ordinary TaskVine tasks and is the performance baseline.

For an unescaped local `file:///` immutable origin, Worker Data Agent
performs an atomic sequential copy directly into the sandbox. It does not fork
a transfer process or create a worker-cache record; the executor's random reads
then remain local. Each task-preparation turn has a 25-ms copy budget and
returns to `WAIT_DATA` after crossing it, so fast small files are grouped while
SharedFS latency cannot starve heartbeats or task completion processing.
Synchronous source reads retry `EINTR`: dense FunctionCall `SIGCHLD` delivery
is normal worker activity and must never be interpreted as permanent data
loss. Non-retryable copy failures retain an operation-specific errno, with
diagnostics bounded per worker so one bad origin cannot create a log storm.

The in-flight window and event-loop batch are deliberately distinct. Runtime
may retain 4,096 physical tasks, but creates/submits and drains at most 128 per
Manager-lane turn so connection acceptance, heartbeats, and status traffic are
not starved by graph materialization. `FORSAKEN` denotes infrastructure
reclamation and is resubmitted with a separate 64-attempt safety bound; it does
not consume `maximum_attempts`, which remains the application-failure policy.
The underlying generic link listener uses the operating system `SOMAXCONN`
backlog instead of five, allowing a 128-worker startup burst to queue safely
across those bounded Manager turns.
The Worker consumes and resets one `waiting_data` hint per event-loop turn,
reducing the next poll timeout from 5 seconds to 1 ms without changing ordinary
TaskVine task state. Escaped and remote URIs retain the generic
cache-transfer path, while generated inputs continue to use generation-checked
local or peer replicas. This URI-local rule does not depend on a separate
consumer-count hint.

Runtime encodes this decision explicitly as the fixed-width task-spec input
kind `LOCAL_FILE`; Worker does not infer it from consumer counts or cache
behavior. Generic `URI`, ephemeral `URI`, and generated-replica inputs remain
separate protocol kinds.

## Independent progress

Task and data events are concurrent and unordered:

```text
Producer Worker                    Manager / Scheduler
TASK_FINISHED  -------------------------> mark task DONE -> dispatch children

Producer Data Agent               Data Controller
DATA_READY_BATCH -----------------------> install replica metadata

Child Data Agent
RESOLVE_BATCH --------------------------> available reply or waiter
```

The Scheduler never waits for Controller admission. A dispatched child stays
in a bounded worker prefetch queue until its inputs resolve; it does not start
an executor child or consume an execution credit while waiting.

A successful task must atomically install every retained output in its local
Data Agent before reporting `TASK_FINISHED`. It does not wait for Controller
admission, peer replication, or SharedFS persistence.

## Compact identity and protocol

The worker protocol uses fixed-width network-order records. A DataKey is:

```text
workflow_slot:u64 data_id:u64 generation:u32 flags:u32
```

One authenticated persistent connection is maintained per Worker Data Agent.
Every mutable message includes `session_epoch` and a monotonic sequence. The
minimal protocol consists of:

```text
HELLO
DATA_READY_BATCH
RESOLVE_BATCH / RESOLVE_REPLY
DATA_FAULT_BATCH
HEARTBEAT / RELEASE_BATCH / RELEASE_ACK
PERSISTED
```

Metadata batches are bounded at 1,024 records and the service retains its
64 MiB hard frame ceiling. Duplicate frames are idempotent. A stale session
or generation fails closed. The hot path contains no JSON, JX walk, pathname
allocation, payload copy, or per-event `malloc`.

## Controller representation

Each workflow owns a chunked DataID table. Slabs are allocated only for live
generated data. `DataRecord`, `ReplicaRecord`, and waiter records live in
geometrically grown arenas and reference one another by indices. Replica
records are linked both by DataID and worker session, providing O(1) lookup
and O(replicas-on-session) disconnect invalidation.

The metadata event loop is the sole writer. Transient worker replicas are not
journaled individually: after Controller restart, active workers reconnect
and re-advertise inventory. Only requested/persistent content metadata and
workflow/task checkpoints are durable.

## Minimal state machines

Scheduler:

```text
WAITING -> READY -> RUNNING -> DONE
```

Worker task:

```text
RECEIVED -> WAIT_DATA -> READY_TO_RUN -> RUNNING -> FINISHED
```

Worker session:

```text
ACTIVE -> SUSPECT -> DEAD
ACTIVE -> DRAINING
```

Data availability is derived, not duplicated: a live record with at least one
active replica is available. `REQUESTED`, `PERSISTED`, and
`RECOVERY_INFLIGHT` are independent bits.

## Failure and recovery

A worker disconnect revokes the complete `(worker_slot, session_epoch)` index
without sending one loss event per file. A file/device fault revokes only the
reported replica. Recovery follows one decision tree:

1. dead DataID: forget it;
2. another valid replica or durable origin: use it and optionally replenish;
3. immutable source: fetch from the origin;
4. last live generated replica: issue one deduplicated producer replay.

Producer replay is a physical recovery attempt. It does not roll back logical
`DONE`, retract already dispatched children, or release dependency counters a
second time. A regenerated output must match the retained size/SHA-256; a
mismatch fails closed.

Parametric producer recovery has one explicit single-instance state machine:

```text
NONE -> QUEUED -> RUNNING -> AWAIT_ADMISSION -> NONE
RUNNING -> NONE (failed physical attempt)
AWAIT_ADMISSION -> QUEUED (bounded admission timeout)
```

`QUEUED`, `RUNNING`, and `AWAIT_ADMISSION` are mutually exclusive for each
logical TaskID. Recovery queue compaction preserves O(live queued recovery)
work and cannot enqueue a second physical replay for the same producer. A
successful replay enters `AWAIT_ADMISSION`; the Runtime polls the Controller's
authoritative replica table and clears the recovery bit only after that table
contains the regenerated DataID. This admission wait is physical recovery
bookkeeping only. The Scheduler remains `DONE`, and ordinary children neither
wait for it nor receive a second dependency release.

The two failure classes deliberately take different paths:

- Worker disconnect retires the complete session generation and invalidates
  only replicas indexed by that `(worker_slot, session_epoch)`. The Controller
  coalesces newly dead DataIDs into recovery epochs and schedules at most one
  replay per producer.
- ENOSPC, EIO, missing local content, or a transfer server's authenticated
  source error revokes the exact `(DataID, source worker, source session,
  replica token)` record. A generic peer refusal or timeout does not prove the
  source copy is gone; the consumer retries resolve locally with bounded
  100-ms to 1.6-s exponential backoff. Size or SHA-256 mismatch still fails
  closed.

Replica publication is atomic for current records, while stale generations
and already-GC'd tombstones are idempotent per-record no-ops. One stale record
therefore cannot reject unrelated valid replicas in the same bounded batch.
Availability is never inferred from the Manager's legacy result table: Worker
Agent intermediates exist only in the Controller replica table.

Output creation failure before atomic local installation is a task attempt
failure. Failure of lazy persistence while the local copy remains available
does not fail the task. Precise transfer, hash, ENOSPC, and EIO failures fail
closed; device-level quarantine and pressure-directed eviction remain OPEN.

## GC

Logical consumer completion reaches the Controller asynchronously and never
blocks scheduling. When a non-requested DataID becomes dead, the Controller
sends generation-checked `RELEASE_BATCH` commands. A pinned local object is
marked eviction-pending and removed after its final pin; release is idempotent.

## Scale invariants

- one logical task remains one independent TaskVine physical execution;
- graph registration uses parametric families where the expansion is proven;
- only a bounded ready frontier is physically materialized;
- worker metadata uses fixed records and bounded batches;
- source paths are derived lazily with no per-source Manager object;
- Controller lookup/update is O(1) and worker loss is proportional only to the
  replicas on the lost session;
- a Controller delay can delay worker input start but cannot delay Manager
  completion processing or Scheduler child dispatch.

The acceptance contract and phased implementation evidence remain in
`DATAVINE_DATA_INTENSIVE_BENCHMARK.md`, `acceptance/matrix.md`, and
`progress.md`.

## 2026-08-24 acceptance

The implemented path now has a compact chunked replica/session/waiter table,
HMAC-authenticated Worker Agent sessions, peer resolve, atomic digest-checked
local output install, asynchronous DATA_READY, direct durable-result install,
generation-checked GC with ACK/replay, reconnect re-advertisement, and physical
recovery attempts which do not roll Scheduler DONE backward. DataVine cache
names are hidden from Manager cache-update/invalid/unlink messages. The Manager
only transports the generic opaque frame; its archive contains no DataVine
object.

The complete local regression is PASS: 17/17 DataVine contracts, including
dynamic append, Owner restart, Worker loss, direct protocol, data plane,
scientific workflow foundation, and the new parametric evaluator. Ordinary
TaskVine `TR_vine_single` is also PASS after the generic frame change.

The 2026-08-25 double-disconnect gate killed and replaced a worker twice in one
8,192-task run. It completed all logical tasks exactly once with 9,295 physical
submissions/completions: 8,192 normal tasks + 1,096 recovery replays + 7
infrastructure retries. The Controller observed two recovery epochs, zero
recovery-admission timeouts, zero non-infrastructure failures, no repeated
logical completion, and restored the 4-worker pool after each loss. This gate
specifically covers recovery coalescing, stale publication, authoritative
Controller admission, and the distinction between transport uncertainty and
an exact source fault.

The full benchmark descriptor expands to exactly 1,048,576 tasks and
10,485,760 files but serializes to 1,344 bytes. It is wired into Store,
Scheduler, and Runtime with a 4,096-task materialization window. Full native
graph setup takes about 2.59 seconds and 172 MiB maximum RSS; its exhaustive
topology digest matches the independent explicit Workload oracle. A current
local E2E passed 256/256 physical executions, exact Worker-reported reads,
requested-only durability, Manager payload bypass, parallelism, and GC gates.
A crash/restart test killed the owner and all workers, restarted from the same
journal with empty caches, and completed via bounded physical replay without
rolling logical Scheduler DONE backward. The exact million-task 128x16
execution remains OPEN and must not be reported as PASS until its artifact is
complete.
