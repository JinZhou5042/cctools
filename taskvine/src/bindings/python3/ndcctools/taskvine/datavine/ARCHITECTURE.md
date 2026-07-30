# DataVine Runtime Architecture

`ndcctools.taskvine.datavine` is the DataVine implementation. The existing
`vine_graph` package is frozen reference code; the new runtime does not import
it or place new semantic state there. DataVine uses TaskVine's public physical
task/file interfaces directly.

## Runtime ownership

- `scheduler/`: compute graph, readiness, attempts, invalidation, and TaskVine
  dispatch.  A `TaskSchedulerThread` is the only thread allowed to mutate
  scheduler state.
- `controller/`: logical data identity, canonical serialized bytes, lineage,
  replicas, durability, recovery liveness, and pruning proofs.  It runs in a
  separate `datavine_controller` process; its HTTP server runs on a dedicated
  controller thread.
- `worker/`: per-worker inventory, bounded caches, demand/prefetch traffic,
  execution-time deserialization, output staging, and publication.
- `cache/`: Scheduler-side retention and eviction policy. It may request
  physical deletion, but it never defines logical availability or replica
  truth; those remain Controller-owned.
- `persistence/`: admitted and acknowledged durable writes.
- `recovery/`: global-loss decisions and compute invalidation requests.
- `placement/`: source selection, peer transfer, and deterministic bounded
  prefetch policy.
- `legacy/`: reserved boundary for narrow, explicitly removable adapters; it
  currently owns no runtime behavior.

Scheduler and Controller never share Python objects or mutable globals.  They
communicate through the versioned protocol in `protocol.py`.  Data-plane bytes
are transferred separately from JSON control records and are verified against
Controller-owned identity metadata.

## Process and thread topology

```text
application process
TaskSchedulerThread -- versioned HTTP protocol --> datavine_controller process
datavine_controller process ---------------------> ControllerService thread

TaskVine workers  -- data/source protocol ---------->  ControllerService thread
```

The command/process boundary is mandatory even for local tests.  In-process
Controller construction is allowed only in focused unit tests.

## Data movement and persistence

EData is registered once as canonical serialized bytes under a Controller-owned
content identity. Workers pull it on demand through authorized URLs, validate
the content hash, and allow TaskVine's worker cache and transfer server to
reuse it. Controller EData admission is byte-bounded.

IData has one stable logical ID across attempts. A producer stages and fsyncs
canonical bytes locally, publishes the logical record to the Controller, and
declares the same bytes as a TaskVine temporary output. Downstream demand first
uses that physical IData realization (local cache or peer transfer), validates
it against Controller metadata, and falls back to Controller bytes only when
needed.

When persistence is requested, one bounded writer consumes admitted requests.
It writes a temporary file, fsyncs, verifies the content hash, atomically
renames, fsyncs the parent directory, and only then acknowledges `durable`.
Availability and durability are separate Controller states.

## Recovery and placement

Loss of a local realization is a cache miss. Global loss means no accepted
volatile or durable realization remains. The Controller invalidates that IData;
the Scheduler makes its original producer pending again and submits the same
TaskID/IDataID at a new attempt. Recovery therefore uses ordinary graph
scheduling and never creates a special recovery task.

Before demand execution, the Scheduler may inspect existing graph/DataID
metadata and select high-fanout EData under fixed byte and item budgets.
Prefetch operations request zero cores and priority `-1000`; demand tasks keep
normal priority. Prefetch success, failure, and disablement cannot alter graph
correctness. The current conservative policy is static and deterministic; it
does not claim topology-aware optimization.

Composite argument bindings store one serialized template plus referenced
IData IDs. A worker reconstructs the value with a memoized deep copy, preserving
alias identity when the same `OutputRef` appears more than once.

## Worker-cache retention boundary

Workers report cache observations with qualified DataID, content metadata,
attempt, WorkerID, and worker epoch. The Controller validates and generations
those physical replicas. When a record has no remaining direct consumers, the
cache policy may invalidate that exact generation and ask TaskVine to unlink
the matching file on one specific worker. Physical pruning is confirmed only
after a UUID-correlated acknowledgement from that worker.

Worker disappearance while an unlink is pending resolves the operation as a
failure and releases its tracker; it is not treated as proof that a retained
workspace was deleted. The current policy bounds retained dead data after
acknowledgements. It does not yet impose a strict instantaneous disk bound:
task working sets can temporarily exceed the target, and no DataVine DRAM tier
exists. Dispatch admission and worker-side fail-closed capacity enforcement
are therefore required before cache-capacity acceptance.
