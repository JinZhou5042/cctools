# DataVine Implementation Plan

> **Status:** Living architecture and implementation contract
> **Starting point:** https://github.com/cooperative-computing-lab/cctools/pull/4253
> **Local repository:** `/users/jzhou24/cctools_repo/datavine`
> **Goal:** Incrementally evolve the existing DAGVine/TaskVine runtime into DataVine while preserving correctness, recoverability, compatibility, and rollback safety.

---

## 1. Mission

DataVine is a workflow-owned data plane for dynamic scientific workflows running on disposable workers.

Its central principle is:

> **Centralize semantic truth; decentralize physical work.**

The Task Scheduler owns computation semantics. The Data Controller owns data identity, serialized representation, lineage, availability, durability, and recoverability. Workers execute tasks, fetch and cache serialized data, prefetch future inputs, stage outputs, and transfer replicas directly when appropriate.

DataVine must remove repeated per-task data provisioning from the manager scheduling path without sacrificing TaskVine’s ability to recover from worker loss.

The implementation must be incremental. Every completed step must leave behind a complete, correct, testable, and rollback-safe runtime.

---

## 2. Background and Motivation

The current runtime combines task scheduling with repeated preparation and management of:

* function code;
* positional arguments;
* keyword arguments;
* dependency files;
* intermediate outputs;
* physical data locations;
* recovery state.

For fine-grained Python tasks, repeatedly serializing, declaring, binding, and transferring task-specific data creates significant manager overhead.

Moving everything through SharedFS avoids some manager traffic, but introduces small-file overhead, metadata pressure, uncontrolled concurrent I/O, and continuously growing intermediate storage.

Worker-local DRAM and disk provide scalable capacity and bandwidth, but are volatile. The runtime must benefit from these resources without making correctness depend on any worker remaining alive.

The fundamental problem is therefore:

> **How can scheduling and data movement make independent progress while stable workflow lineage—not temporary replicas—remains the basis of correctness and recovery?**

---

## 3. Core Observations

### 3.1 Logical data and physical replicas are different

A logical data item has stable workflow meaning:

* identity;
* producer;
* consumers;
* serialized content;
* lifetime;
* durability;
* recovery path.

A physical replica has temporary runtime properties:

* location;
* storage tier;
* availability;
* transfer state;
* source load;
* eviction state.

Worker failure may destroy replicas, but must never destroy logical identity or lineage.

### 3.2 Tasks should reference data, not contain repeated data

Function code, arguments, keyword values, dependency files, and intermediate outputs must be represented as independently indexed data items.

Task and graph records should carry lightweight integer or compact string IDs rather than repeated Python objects, serialized payloads, hashes, paths, or large metadata structures.

> **Bindings belong to tasks; serialized bytes belong to the Data Graph.**

### 3.3 Serialization is the data-plane boundary

All transferable and storable Python data must use a canonical `cloudpickle` representation.

The normal path is:

```text
serialize once
→ identify once
→ move or store serialized bytes
→ deserialize only at worker execution
```

Moving data between Controller memory, worker DRAM, worker disk, peers, and SharedFS should normally preserve the same serialized bytes without deserializing and serializing again.

### 3.4 Compute completion and durability are different events

```text
task execution completed
≠
output locally published
≠
output durably persisted
```

These states must remain distinguishable.

### 3.5 Local absence and global loss require different actions

```text
Local miss  → fetch or transfer data
Global loss → invalidate the realization and recompute its producer
```

A cache miss must not trigger recovery.

### 3.6 Persistence and pruning are dual operations

Persistence establishes a newer durable recovery point. Pruning removes older data that the newer point makes unnecessary.

> **Persist forward; prune backward.**

---

## 4. Core Architecture

DataVine maintains two related but separately owned views.

### Compute Graph

Owned by the Task Scheduler. It determines:

* which tasks exist;
* dependency readiness;
* execution attempts;
* priorities and resources;
* retry and invalidation;
* workflow completion.

### Data Graph

Owned by the Data Controller. It determines:

* which edata and idata items exist;
* task-to-data bindings;
* producer and consumer relationships;
* canonical serialized representation;
* replica locations;
* data availability;
* durability;
* recovery responsibility;
* pruning safety.

Workers join the two graphs at execution time:

```text
Scheduler assigns TaskID and input/output DataIDs
→ Worker resolves missing serialized data
→ Worker fetches and caches bytes
→ Worker deserializes immediately before execution
→ Worker executes
→ Worker cloudpickles outputs
→ Worker publishes idata replicas
```

The initial implementation may keep scheduling and data control in the same process and event loop. Logical ownership must be established before introducing threads, separate processes, or more aggressive concurrency.

> **Scheduler owns compute. Controller owns data. Worker joins them.**

---

## 5. Data Identity and Serialized Representation

## 5.1 Edata

Edata includes every independently reusable external execution input, including:

* function code;
* each positional argument;
* each keyword argument value;
* dependency files;
* configuration and runtime artifacts where applicable.

Every edata item must:

1. be independently converted to a canonical serialized representation;
2. be content-hashed;
3. be globally interned and deduplicated;
4. receive a lightweight `EDataID`;
5. be referenced by tasks only through that ID and task-specific bindings.

Conceptually:

```text
cloudpickle(value)
→ content hash
→ global lookup
→ reuse existing EDataID or create a new one
```

Identical immutable edata registered by different tasks must reuse the same logical data item and canonical serialized representation.

Serialization type, protocol, or version information must be included wherever necessary to prevent incompatible content from aliasing.

Hash collisions must be detected rather than silently accepted.

## 5.2 Idata

Every logical task output must receive one unique `IDataID`.

Its identity must be stable across retries and recomputation of the same logical output. It should reflect workflow lineage or output identity rather than merely its byte content.

After production, the worker cloudpickles the output and may compute a content hash for integrity verification and physical-replica validation.

Different logical idata items must not automatically be merged solely because they happen to produce identical bytes; their lineage, consumers, recovery responsibility, and lifetime may differ.

## 5.3 Lightweight references

The runtime should use compact IDs for tasks, edata, idata, workers, derivations, and physical replicas.

The exact types, allocation schemes, index structures, and persistence representations are implementation decisions for the agent after studying existing CCTools conventions.

The required semantic property is:

> Core graph relationships and runtime messages reference lightweight IDs, not repeated serialized objects or task-specific copies.

## 5.4 Controller ownership

Every edata and idata item must have a Data Controller-owned canonical serialized representation or a Controller-owned reference to that representation.

This does not require all bytes to remain in Controller process memory.

Possible physical realizations include:

* Controller memory;
* Controller-managed backing storage;
* worker DRAM;
* worker local disk;
* SharedFS.

Small, highly shared serialized objects may remain in Controller memory. Large data should normally use distributed or durable storage.

---

## 6. Main Components

### Task Scheduler

Responsible for compute readiness, assignment, attempts, invalidation, and completion.

It must not become responsible for replica placement, cache eviction, peer-transfer details, persistence admission, or pruning proofs.

### Data Controller

Responsible for:

* global edata hashing and deduplication;
* unique idata indexing;
* task-to-data bindings;
* serialized-data ownership;
* Data Graph and lineage;
* replica metadata;
* source resolution;
* durability state;
* SharedFS I/O admission;
* recovery-liveness;
* pruning safety.

### Worker Data Agent

Responsible for:

* local serialized-data inventory;
* DRAM and disk caches;
* missing-input detection;
* demand pull;
* peer transfer;
* prefetch;
* output serialization and staging;
* replica publication;
* transfer validation and fallback.

### Persistence Control

Task outputs should normally be staged locally before durable persistence.

The Controller controls when queued writes enter SharedFS, limits concurrent I/O, confirms successful completion, and may cancel obsolete persistence requests that have not yet begun.

### Recovery

If a DataID has no valid replica or durable origin but is still required, the Controller reports global loss.

The scheduler rolls the existing Compute Graph back only as far as needed to reproduce the lost data.

The final model should not require a separate semantic class of recovery tasks.

> **Recovery is normal execution over invalidated data.**

### Recovery-Aware Pruning

An idata item remains live while it has either:

* unfinished direct consumers; or
* recovery value for downstream volatile data.

Consumer completion alone is insufficient for deletion.

Data may be pruned only when downstream durable state fully covers all future execution and recovery requirements.

This forms a moving **durability frontier** and a **minimum recoverable cut**.

---

## 7. Caching, Prefetch, and Source Selection

Controller memory, worker DRAM, and worker disk are bounded resources.

Caching policy should favor data with high expected future benefit relative to capacity cost. Relevant signals may include:

* serialized size;
* remaining and near-future consumers;
* expected reuse;
* fetch cost;
* recomputation cost;
* source availability;
* worker reliability;
* current load.

Small, high-fanout data—especially function code and commonly reused arguments—should generally have high cache value. Very large or one-time data should generally bypass limited memory caches.

Workers may prefetch likely future inputs so that task execution and data transfer overlap. Prefetch must have independent budgets and must never delay demand reads.

When multiple sources exist, the Controller may compare:

* Controller memory;
* peer worker DRAM;
* peer worker disk;
* SharedFS;
* external durable storage.

The initial implementation should prefer a simple, correct, deterministic policy. More adaptive load-aware cost models may be added later.

Cache, prefetch, and source-selection policies are optimizations. Disabling them must preserve correctness.

---

## 8. Persistence and Pruning Semantics

A representative output lifecycle is:

```text
produced on worker
→ cloudpickled
→ stored on local disk
→ published to Controller
→ available as volatile data
→ queued for persistence
→ admitted to SharedFS
→ validated and acknowledged as durable
```

SharedFS must be treated as a controlled durability backend rather than an unrestricted default communication path.

The Controller must limit concurrent SharedFS operations to avoid overload.

A queued persistence request may be cancelled if the data becomes safely prunable before the write begins.

Pruning must be introduced conservatively:

1. compute and log pruning decisions without deleting;
2. delete worker-local replicas while retaining durable protection;
3. quarantine SharedFS data before permanent deletion;
4. enable hard deletion only after extensive recovery testing.

Caching decisions may use heuristics. Pruning decisions must be justified by explicit recoverability conditions.

---

## 9. Design Philosophy

1. **Centralize semantic truth; decentralize physical work.**
2. **One fact has one authoritative owner.**
3. **Tasks and graph edges reference lightweight IDs.**
4. **Serialize once, identify once, move many times.**
5. **Dispatch must not synchronously wait for physical placement.**
6. **Worker storage is useful soft state, not workflow truth.**
7. **Local misses move data; global losses reproduce data.**
8. **Persistence is scheduled, rate-limited, validated, and revocable.**
9. **Caching may be approximate; deletion requires proof.**
10. **Optimization failure must degrade to a correct slower path.**
11. **Reuse existing TaskVine mechanisms where they fit the new semantics.**
12. **Make the smallest coherent change, not the fewest possible lines.**

---

## 10. Implementation Freedom and Constraints

The agent must first study the existing PR, TaskVine manager, worker protocol, file abstractions, graph executor, recovery behavior, build system, and tests.

The plan intentionally does not prescribe:

* exact C structures;
* exact ID widths or formats;
* exact hash algorithm;
* exact source files or directories;
* exact module names;
* exact message formats;
* exact queue implementation;
* exact cache algorithm;
* exact threading model;
* exact persistence policy.

The agent should choose designs that fit existing CCTools conventions, minimize duplication, and preserve clear ownership boundaries.

However, the following requirements are non-negotiable:

* every edata item is hashed and globally deduplicated;
* every idata item has one stable unique logical index;
* task/data relationships use lightweight IDs;
* transferable Python data has a Controller-owned cloudpickle representation;
* workers deserialize at execution time;
* retries reuse logical identities;
* storage placement does not change identity;
* Data Controller owns data semantics;
* Scheduler owns compute semantics;
* worker replicas are disposable;
* pruning cannot destroy recoverability.

The implementation should remain modular. Avoid both extremes:

* scattering DataVine logic across unrelated files;
* creating an isolated parallel runtime that duplicates existing TaskVine functionality.

Module and directory structure should be chosen after investigating the repository. Each subsystem should have a clear interface and responsibility.

---

## 11. Incremental Implementation Roadmap

Each phase must be a stable checkpoint.

### Phase 0: Baseline and observability

* establish representative workflows and correctness oracles;
* measure current serialization, manager, transfer, storage, and recovery behavior;
* add deterministic failure-injection capability where needed.

### Phase 1: Indexed serialized identity

* introduce lightweight task/data identities;
* cloudpickle, hash, intern, and deduplicate every edata item;
* assign stable unique identities to idata output slots;
* represent task inputs and outputs through IDs and bindings;
* preserve legacy movement and execution behavior.

### Phase 2: Shadow Data Graph

* construct Data Controller state from existing execution events;
* keep legacy structures authoritative;
* continuously compare both representations;
* make no new placement or deletion decisions.

### Phase 3: Data Controller authority

* make the Controller authoritative for logical data identity, lineage, serialized representation, and availability;
* retain existing transfer and recovery paths through compatibility adapters.

### Phase 4: Worker data preparation

* introduce worker-side serialized-data inventory;
* resolve missing DataIDs through the Controller;
* implement stable-source worker pull;
* retain the legacy path as rollback.

### Phase 5: Peer transfer and bounded caching

* allow validated worker-to-worker transfer;
* add Controller and worker cache capacities;
* preserve safe fallback sources.

### Phase 6: Controlled persistence

* stage serialized outputs locally;
* queue and rate-limit SharedFS writes;
* add durable acknowledgement and retry.

### Phase 7: Volatile publication and unified recovery

* allow downstream work to consume published worker-local idata;
* detect global data loss;
* re-enable original producer computations through normal graph scheduling.

### Phase 8: Prefetch and adaptive placement

* overlap future data movement with execution;
* add conservative load-aware source selection;
* ensure demand traffic always has priority.

### Phase 9: Recovery-aware pruning

* begin with shadow pruning;
* progress to local deletion;
* later add SharedFS quarantine and hard deletion;
* require zero known false-positive pruning decisions.

Legacy mechanisms may be removed only after their replacement has passed repeated end-to-end and failure-injection testing.

---

## 12. Mandatory Engineering Workflow

### Starting point

All work begins from PR #4253 and the local repository:

```text
/users/jzhou24/cctools_repo/datavine
```

The agent may modify any necessary code, but should prefer reuse, narrow adapters, and incremental migration over broad rewrites.

### Build rule

After every manager-side, graph-runtime, relevant header, binding, protocol, or build-system source change, run:

```bash
cd /users/jzhou24/cctools_repo/datavine/taskvine/src
make clean && make -j8 && make install
```

A clean successful build is required before accepting workflow results.

### Factory rule

Use:

```bash
/users/jzhou24/graph_optimization/factories/run_factory.sh
```

The agent must avoid duplicate factories, record relevant logs, and never terminate unrelated user processes.

### Testing rule

After every atomic implementation task:

1. rebuild when required;
2. run focused unit or component tests;
3. construct or update a complete workflow exercising the new behavior;
4. run that workflow end to end;
5. verify exact outputs against an oracle;
6. run relevant existing regressions;
7. inject failures at newly introduced state transitions;
8. verify resource and process cleanup;
9. update project tracking documents.

Compilation alone is never acceptance.

### Rollback rule

New behavior should initially be controlled by a feature flag or explicit mode whenever practical.

Disabling it must return to the preceding stable behavior until the new path has proven sufficiently mature.

### Failure honesty

The agent must not:

* suppress failed tests;
* weaken correctness checks merely to pass;
* ignore nonzero commands;
* silently accept fallback behavior;
* mark partially validated work complete;
* delete data whose recoverability is uncertain.

---

## 13. Acceptance Contract

Every completed incremental task must satisfy all applicable gates.

### Build

* clean build and installation succeed;
* no unexplained new warnings.

### Functional correctness

* the focused end-to-end workflow completes;
* outputs exactly match the reference;
* the previous stable path remains functional.

### Identity and serialization

* all applicable edata is hashed and interned;
* identical edata reuses one logical ID;
* idata identity remains stable across retry;
* graph and runtime relationships use IDs;
* data movement preserves canonical serialized bytes;
* workers deserialize only for execution.

### Invariants

* no unknown IDs;
* no conflicting authority;
* no durable state without acknowledgement;
* no valid replica associated with the wrong content;
* no pruned item remains required for execution or recovery.

### Failure recovery

* failures introduced at new transitions yield correct eventual completion or an explicit safe failure;
* worker loss does not destroy data identity or lineage.

### Resources

* configured capacities and I/O limits are respected;
* no persistent process, memory, temporary-file, or storage leak is introduced.

### Traceability

* `progress.md` reflects current status and exact acceptance results;
* completed work receives a concise `history.md` entry;
* detailed logs remain available outside `history.md`;
* commits remain small enough to review and revert.

---

## 14. Project Documents

### `plans.md`

Stable motivation, architecture, principles, constraints, and engineering contract.

Change it only when the project direction materially changes.

### `progress.md`

Current phase, active task, immediate next work, exact acceptance criteria, build/test results, blockers, and rollback status.

Update it after every implementation step.

### `history.md`

A concise record of completed checkpoints:

* date;
* high-level change;
* validation performed;
* important result;
* commit identifier when available.

Do not turn it into a raw command log.

Before beginning work, the agent must read all three documents. After each task, it must update `progress.md` and add to `history.md` only after acceptance succeeds.

---

## 15. Project-Level Success Criteria

DataVine is successful when it demonstrates:

* global hash deduplication of all reusable edata;
* stable lightweight indexing of tasks, edata, and idata;
* one canonical serialized representation per logical data item;
* significantly reduced repeated manager-side serialization and data provisioning;
* independent scheduling and data progress;
* worker-driven demand pull and prefetch;
* bounded Controller and worker caches;
* controlled SharedFS concurrency;
* safe use of volatile worker-local outputs;
* lineage-driven recovery without special recovery-task semantics;
* storage demand that can decrease while the workflow continues;
* recovery-aware pruning that never destroys required state.

The implementation must ultimately support the paper claim:

> **DataVine centralizes workflow-owned data semantics and serialized identity while distributing data movement, caching, persistence, and re-realization across disposable workers.**

---

## 16. Final Maxim

> **Never optimize data that the system cannot yet name. Never delete data that the system cannot yet recover.**
