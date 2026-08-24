# DataVine Runtime v2: decoupled task and data planes

Status: **LOCAL NATIVE RUNTIME PASS; EXACT 128x16 CAMPAIGN OPEN**

Updated: 2026-08-24

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

For an unescaped local `file:///` immutable origin, Worker Data Agent links the
SharedFS path directly into the sandbox. It does not fork a transfer process or
copy the object into worker cache. The executor still reads the source bytes
from SharedFS, while generated inputs continue to use generation-checked local
or peer replicas. Escaped and remote URIs retain the generic transfer path.

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
