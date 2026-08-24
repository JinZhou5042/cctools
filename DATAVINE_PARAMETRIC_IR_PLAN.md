# DataVine parametric IR and lazy graph materialization plan

Updated: 2026-08-24

Status: **NATIVE RUNTIME PASS — exact 128x16 full run OPEN**

## Why this phase exists

The first 1,048,576-task / 10,485,760-file TaskVine baseline exposed a
control-plane limit before worker execution began.  The Python client created
one object per task, declared 9,437,184 source URLs, attached 15,335,424 input
references, and submitted every task to one manager.  At 1,007,504 loaded
tasks the manager process had reached 20,101,136 KiB RSS.  It had read zero
source payload bytes but had already issued 3,270,731 write calls and generated
6,290,751,488 bytes of filesystem writes.

That run also exposed a separate scheduler-startup limit.  TaskVine's default
`attempt-schedule-depth` is 100.  Function libraries are installed lazily, so
the first dispatch pass populated exactly 100 of 128 workers.  Stable execution
used 1,600 of 2,048 cores while 28 connected workers stayed idle despite more
than one million ready tasks.  The run was stopped at the 30,000-task
checkpoint because it could not pass the required 1,844-active-core central
window gate.  The raw, non-PASS evidence is indexed by
`acceptance/data-intensive-large-scale-control-plane-diagnostic-20260824.json`.

The scheduling-width defect has a narrow benchmark fix.  The graph-ingestion
cost needs an architectural fix.

A later node-local-log attempt isolated two additional multipliers.  The
explicit FunctionCall path generated one 51-byte staging argument file for
every logical task, so graph construction created another 1,048,576 inodes
before execution.  During execution, each short task produced per-file cache
updates and a long sequence of manager-to-worker `unlink` commands.  These are
not unavoidable consequences of one-task/one-execution semantics: family-level
arguments, batched cache-state messages, and batch unlink/GC commands can
preserve task identity while removing most control messages and filesystem
operations.

## Boundary and invariant

The target is not semantic task batching.  One logical task must still become
one independently scheduled physical TaskVine task on every attempt.  Retry,
resource, completion, publication, and failure identity remain per task.

What can disappear is the requirement to construct and retain a Python object,
a JX object, a file object, and an explicit edge object for every member of a
regular family before any work can run.

Production v1 full and compact records remain readable.  Parametric families
are an optional new representation with the same expanded graph semantics.
Irregular tasks and deltas continue to use explicit records.  A family must
fail validation before mutation if its bounds, arithmetic, inverse mapping, or
expanded counts cannot be proved safe.

## Proposed representation

Add three native, declarative record types:

1. A **source family** defines a DataID range and a URI function.  The current
   benchmark needs one root plus bounded integer formatting for cohort, A
   index, and source slot.  No source path is allocated or `stat`ed until a
   task using it is materialized.
2. A **task family** defines a TaskID interval, shared executor/resources, and
   formulas mapping a local task index to input and output DataIDs.
3. A **dependency family** defines both the forward input mapping and a checked
   inverse consumer mapping.  The inverse is required so producer completion
   can update downstream readiness without storing every edge.

The expression language must be deliberately small: checked integer
add/multiply/divmod, bounded ranges, affine permutations, and lookup of a small
constant table.  It is not embedded Python, shell, JX evaluation, or arbitrary
user code.  Validation computes exact expanded task, data, edge, and requested
output counts with overflow checks.

For this workload the complete graph can be represented by three task
families, one source family, three output families, and per-cohort permutation
parameters.  A source URI is derived from `(cohort, a_local, slot)`.  A-to-B
consumers are the 20 checked inverse permutations for that cohort.  B-to-C has
one inverse affine mapping.  The current 26,869,763 explicit task/data/input
entries become on the order of hundreds of family and cohort descriptors.

## Native execution model

### Implemented milestone

The production runtime now accepts an optional sealed `parametric` record with
kind `data-intensive-v1`. Admission is strict and side-effect free: the three
inline callable payloads, seed, scale, cohort count, file origin, exact policy
limits, and checked expanded counts must agree before Store mutation. The
ordinary explicit `datavine.workflow/v1` representation remains available and
unchanged.

The native evaluator builds the existing packed Scheduler directly, without
creating a million JX task records or ten million JX Data records. For the
frozen full graph it expands 1,048,576 TaskIDs and 5,898,240 dependency edges
in about 2.59 seconds with about 172 MiB maximum RSS. A full exhaustive native
topology digest equals the independent Python Workload oracle digest
`331d538bebd47fa15b20443ca757a605bc4f6230a2319d2d7e384a07e96ae6c8`.

Runtime materialization is bounded to 4,096 transient task views. Source URIs,
input DataIDs, producer IDs, consumer counts, executor records, and requested
outputs are synthesized only for a task entering that window and destroyed at
physical completion. Worker task/data completion remains decoupled and one
logical task still produces one physical TaskVine task per attempt. Completion
journal records use batches of up to 256.

The first exact 128x16 execution of this milestone exposed a separate Worker
source-ingest multiplier. Although all 128 workers and 2,048 slots remained
admitted, each of the 9,437,184 `file:///` sources launched a curl transfer and
was copied into worker cache before the task read it. Ten simultaneous curl
processes per worker were observed blocked in SharedFS I/O; the stable rate was
only about 2.7 tasks/s and could not finish within the 24-hour worker lifetime.
That run was stopped and retained as a diagnostic, not acceptance evidence.

A direct-symlink experiment removed the copy but made the task's random preads
hit SharedFS, reducing rather than improving full-scale throughput. The final
path recognizes unescaped local `file:///` origins and performs one
in-process sequential copy directly into the task sandbox. The task then does
its random reads locally. This preserves exactly one 712.8-GiB SharedFS read
while removing the redundant worker-cache object and 9.4 million curl process
launches. One task-preparation turn has a 25-ms copy budget and returns
`WAIT_DATA` after crossing it, grouping fast small files while bounding
heartbeat latency under SharedFS pressure. A consumed Worker Agent hint reduces
the next poll timeout from 5 seconds to 1 ms while any DataVine task waits for
data. Remote or escaped URIs retain the generic cache-transfer path. The
Shell workflow test asserts that a one-use SharedFS source is not transferred
into worker cache; the complete regression and tiny data-intensive E2E must
remain PASS.

The Runtime-to-Worker task spec carries local SharedFS input policy explicitly
as `LOCAL_FILE`. This avoids the failed implicit approach in which the Worker
had to infer staging policy from a consumer-count hint or URI side effect.

This milestone deliberately reuses the existing packed Scheduler edge arrays,
so current native graph memory is still O(edges), not the final
O(families + task-state bitmap) target described below. That remaining
optimization is no longer a prerequisite for this benchmark: measured setup is
already far below the 60-second and 4-GiB acceptance limits. It remains useful
future work for substantially larger or denser workflows.

### Compile once

The workflow store parses each family into a packed native descriptor.  The
raw accepted document remains available for replay, but the hot path never
walks JX to answer TaskID, DataID, producer, consumer, URI, or defaults queries.
Family lookup is a binary search over a small sorted range table.

### Materialize a bounded frontier

The runtime keeps a configurable ready window, initially four times aggregate
cores.  It creates an ordinary `vine_task` only when a logical task enters this
window and releases the physical object after terminal processing.  A million
logical tasks therefore do not imply a million resident manager tasks.

Logical state uses packed bitmaps and narrow counters:

- two bits per task for pending/ready/running/done plus sparse retry metadata;
- a bounded readiness counter only for not-yet-ready task families;
- output replica/location objects only while data is live, requested, or
  recoverable;
- no permanent per-edge allocation when a checked inverse family exists.

FunctionCall arguments for a materialization window should be encoded in one
immutable cohort/family blob plus a compact task-index vector.  They must not
be staged as one filesystem inode per task.  Workers should expand the same
checked family formula locally for the selected index.

Cache updates and GC need a bounded batch protocol.  A Worker Data Agent may
report a vector of `(DataID, size, generation, state)` transitions directly to
the Data Controller in one frame, and the Controller may return a vector/range
of disposable DataIDs in one command. The TaskVine Manager is not part of this
protocol and retains no DataVine file or replica state. Batch
application is generation-checked and idempotent, so reconnect/replay cannot
delete a newer replica.  This changes message count, not logical file identity
or the point at which each file becomes collectable.

Completing an A task generates its 20 B consumers from the inverse formula and
increments their eight-input counters.  Completing a B task identifies its one
C consumer and increments that five-input counter.  This preserves exact
producer-completion readiness without an explicit 5,898,240-edge table.

### Journal compact state, not declarations

The durable journal records the accepted family descriptor once.  Task
transitions are appended in bounded batches and periodically checkpointed as
compressed bitmaps plus sparse attempts/failures.  A checkpoint commit is
atomic; replay applies the checkpoint and later transition batches.  Individual
task identity and retry history remain recoverable even though transaction
writes are amortized.

Manager runtime logs should use node-local scratch during execution.  The
accepted compact checkpoint, summary, selected diagnostics, and checksums are
copied atomically to durable campaign storage.  A paired diagnostic will retain
the current shared-filesystem log placement to quantify how much synchronous
transaction logging amplifies graph-load and execution I/O.

That diagnostic is now available: at roughly 44k completed tasks, debug,
taskgraph, transaction, and performance streams totaled 4,354,722,193 bytes on
NFS and coincided with repeated manager read failures from workers.  The
benchmark isolation change is implemented in the runners. Native parametric
admission, bounded materialization, completion batching, and Worker-Controller
data ownership are implemented; bitmap checkpoints and formula-backed
dependency traversal remain later optimizations.

## Complexity target

| Concern | Current explicit path | Parametric target |
|---|---|---|
| Registration payload/objects | O(tasks + data + input references) | O(families + cohort parameters) |
| Stored dependency graph | O(edges) | O(families + task-state bitmap) |
| Resident physical task objects | O(all tasks) before execution | O(active ready window) |
| Source path objects | O(source files) | O(active task inputs) |
| Completion journal writes | one fine-grained stream | bounded batches + bitmap checkpoints |
| FunctionCall arguments | one staging inode per task | family blob + bounded index vectors |
| Manager cache update / unlink traffic | messages per file | zero for DataVine data |
| Controller replica / release traffic | not independent | generation-checked vectors/ranges |
| Physical executions/completions | O(tasks) | O(tasks), unchanged |

The last row is a hard lower bound.  The goal is orders-of-magnitude reduction
in graph registration, memory, and journal syscall count, not an impossible
sublinear number of independent task executions.

## Delivery sequence

1. Keep the current production v1 behavior and land the 128-worker scheduling
   width fix with a real 128x16 admission, central-parallelism, and final-pool
   recovery gate.  Record opportunistic Condor churn separately from logical
   task failure.
2. Specify the family schema and implement a side-effect-free native evaluator
   with expansion equivalence tests against explicit IR on small randomized
   graphs.
3. Add checked inverse dependency families and compare readiness transitions,
   retry propagation, requested outputs, and final hashes against explicit IR.
4. Add the bounded materialization window and packed logical state.  Measure
   graph-load RSS independently from execution RSS.
5. Add batched transition journaling and atomic bitmap checkpoints, followed by
   truncation, restart, worker-loss, and publication-loss recovery tests.
6. Add direct Worker-Controller generation-checked replica and GC vectors, plus family/index
   FunctionCall argument transport with no per-task staging inode.
7. Rerun the full million-task/ten-million-file pair and preserve both
   execution-only and end-to-end comparisons.

Steps 2 through 4 and the hot-path completion batching in step 5 now have
executable evidence in `acceptance/scripts/data_intensive_parametric_ir.py`,
`taskvine/test/datavine_parametric_ir.py`, and the native runtime. The full
descriptor is 1,344 bytes; small exhaustive and full exhaustive topology
digests match the independent explicit oracle. A current-code local E2E
completed 256/256 independent physical tasks through a 12,105-byte registration
with every correctness, Worker Agent, Manager-bypass, actual-read-byte,
parallelism, and GC gate true. An owner-and-all-worker SIGKILL test resumed the
same journal with empty worker caches, replayed the missing producer closure
without rolling logical DONE backward, and completed with exact requested
results. The exact 128x16 execution remains the only benchmark-scale OPEN gate.

## Acceptance gates

The optimization is not complete until all of these pass:

- expanded counts and sampled mappings exactly match explicit IR;
- exactly 1,048,576 independent physical submissions and successful
  completions occur;
- graph registration uses no per-source `stat` and produces no source payload
  reads;
- registration RPC payload is at least 100x smaller than compact explicit IR;
- graph load completes in at most 60 seconds and pre-execution process-tree RSS
  remains below 4 GiB on the acceptance manager;
- resident materialized tasks remain within the configured window plus a
  documented bounded recovery allowance;
- the 128x16 central window uses at least 1,844 active cores; worker removals
  and failed attempts are explicit, no attempt is exhausted, and the exact
  pool is restored before acceptance;
- task results, retry/recovery behavior, durable-output count, worker-local
  output policy, source bytes, movement, and GC accounting match the explicit
  graph;
- restart from every journal/checkpoint boundary either reproduces the same
  terminal state or fails closed before mutation;
- execution-only, graph-load, and end-to-end timings are reported separately.

Native parametric execution is implemented and locally accepted. Until the
exact full campaign passes, its scale/performance claim remains OPEN and the
existing production v1 path remains the comparison authority.
