# DataVine Progress

## Current status

- Phase: **Ultimate Acceptance — OPEN, not accepted**
- Branch: `datavine`
- Starting point: `origin/task-graph` / PR #4253 head `345dc7fcde3851400bebb81ccfd877c93a93cdca`
- New runtime root: `ndcctools.taskvine.datavine`
- Local Phase 4 acceptance: **PASS**
- Prescribed factory acceptance: **PASS**
- Reference runtime: `ndcctools.taskvine.vine_graph` is frozen at accepted
  Phase 4A and is no longer the DataVine implementation.
- Active task: **Phase 9 branched minimum recoverable cut**
- Validated code commit: `79bcbc832`

## Ultimate acceptance reset

The Phase 8 result remains a valid component checkpoint, but it is not final
DataVine acceptance. The binding contract is now `acceptance/README.md`, and
the live status is `acceptance/matrix.md`.

Architecture Review A at pushed commit `1045fa2dd0` is **FAIL** with critical
findings. In particular, the independent runtime lacks physical replica epochs,
multi-output identity, large-data bypass, bounded byte serving and queues,
persistence cancellation, repeated frontier-aware recovery, all pruning
algorithms, and the Grand Challenge comparison. No final completion claim is
permitted while those rows remain open or failed.

### Phase 9 multi-output partial-publication process-loss checkpoint

Commit `79bcbc832` adds a coordinated publication fault barrier. A worker
publishes and prepares output slot zero, then pauses before slot one. Scheduler
queries Controller-owned replica records, identifies the exact preparing
WorkerID, cancels the physical task first, shuts down that worker second, and
invalidates every output in the incomplete attempt. Only the cancellation
completion releases the original logical task as attempt two.

The accepted design performs three physical attempts for two logical tasks.
Producer attempt one publishes only IData 1 and never becomes logically
complete; producer attempt two republishes stable IDataIDs `[1, 2]`; the
consumer runs once and returns the oracle. No DataVine recovery reexecution is
counted because the producer never completed, and TaskVine reports zero Legacy
recovery tasks.

Self-review rejected two designs. Killing only the main worker left its
transfer-server child holding the manager connection and timed out. Killing
the worker process group from inside the task allowed TaskVine to reassign the
same faulting physical command, which repeatedly killed replacements. The
accepted ordering keeps retry ownership in the DataVine Scheduler by cancelling
the old physical task before process shutdown.

The required clean build/install and all 18 installed regressions pass. Three
local repetitions have identical semantic hashes. Package SHA-256 is
`73205f65cd5e5cf8aac2070ee3c1e27e055652047e990d6d30ee81476e04e615`.
Package-only factory `datavine-partial-79bcbc832` observes exact remote
WorkerID shutdown, completes with a replacement, and removes all remaining
workers on stop. Evidence:
`acceptance/artifacts/partial-publication-79bcbc832.json`.

Self-review status is **PASS for MULTIOUT and this scoped publication fault,
FAIL for Ultimate Acceptance**. Concurrent multi-output persistence/pruning,
other publication failure stages, scale, and the Grand Challenge remain. The
next smallest safe task is a branched fan-out/fan-in minimum recoverable-cut
case with unequal durability frontiers and branch-selective loss, proving that
an unaffected completed branch is neither invalidated nor replayed.

### Phase 9 stable multi-output identity checkpoint

Commit `605426341` replaces the single-output TaskRecord assumption with an
ordered set of logical output slots. The Controller allocates and validates
one stable IDataID for each `(TaskID, output index)`, pruning lineage records
all slots, worker assignments stage and publish every slot independently, and
Scheduler refuses logical completion until every expected publication and
replica preparation is validated.

The deterministic E2E creates two equal-byte outputs with distinct IDataIDs,
consumes only output zero through a cyclic nested container, and repeats the
same OutputRef at three locations. Worker reconstruction preserves both the
container cycle and object alias identity. Loss of demanded IData 1 causes one
ordinary replay of the original TaskID: output IDs remain `[1, 2]`, both slot
attempts become two, the consumer remains at attempt one, the exact oracle is
returned, and no legacy recovery task runs.

The prescribed clean build/install and all 18 installed-path regressions pass.
Three local repetitions produce identical machine-readable output hashes.
Package SHA-256 is
`662acb77d961016f4717581728a00adc2d5fe1138c7ad8cf6c2ac591b53a3e9f`;
`poncho_package_run` verifies cloudpickle 3.1.2 and the multi-output API.
Package-only factory `datavine-multi-605426341` passes the normal and recovery
cases with two requested workers, and factory shutdown removes both workers.
Evidence: `acceptance/artifacts/multi-output-605426341.json`.

Self-review status is **PASS for this scoped checkpoint and FAIL for Ultimate
Acceptance**. The test does not kill a worker between output-slot publications,
does not exercise partial publication cleanup, and does not combine
multi-output with persistence cancellation or pruning. The next smallest safe
task is deterministic worker loss after one slot is published but before the
remaining slots publish, proving that no partial logical completion is
observable and that retry reuses every original output IDataID.

### Phase 9 two-frontier minimum recoverable cut checkpoint

Commit `fafead8bde` adds selective durability frontiers and target-driven
recovery closure. A nine-task, 512 KiB lineage persists task 1 on its first
attempt and task 5 only after its recovery attempt. Loss detection starts from
unfinished consumers and required results instead of invalidating every
completed output. The two measured rollback waves are tasks 5–2 at depth four
and tasks 8–6 at depth three; TaskID and IDataID remain stable across 16
physical attempts, seven ordinary reexecutions, and zero legacy TaskVine
recovery tasks.

After task 5 becomes durable, Controller proof authorizes physical deletion of
IData 2–4. TaskVine workers acknowledge all three deletions, the acknowledgement
trackers are released, and a second loss still recovers only tasks 6–8 from the
task-5 durability frontier. Controller global-loss handling now invalidates
every current non-durable replica rather than only its own inline replica.

Self-review rejected six materially flawed implementations: a timer-driven
replacement schedule with a 90-second timeout; a nested pruning wait that
consumed an ordinary completion; durability gating in persistence-disabled
mode; treating default persistence as an explicit scheduling frontier;
graceful worker release that reconnected the same WorkerID; and shutdown of a
worker not proven to own the target replica. The accepted process-loss hook
selects the actual unique volatile source from Controller replica truth and
shuts down that exact WorkerID. Three clean local repetitions each observe
unique-source losses with worker counts 3→2→1 and complete in 17, 16, and 17
seconds.

The prescribed clean build/install passes. The installed suite passes 17/17;
post-clean minimum-cut, Phase 7 graceful-release recovery, replica protocol,
DataVine flake8, and diff checks pass. The rebuilt package SHA-256 is
`1f1da5b661f61eaa4c532b1df65d09f84a061f96278a729df21c861f2c87761d`;
`poncho_package_run` verifies cloudpickle 3.1.2, Workflow, and the deterministic
shutdown API. Factory `datavine-mincut-fafead8bd` uses three package-only
workers, shuts down the two actual unique-replica owners, returns the exact
oracle, and removes all workers.

This is a scoped linear minimum-cut checkpoint, not Ultimate Acceptance.
Multi-branch retained-cut optimality, asynchronous non-blocking pruning, DRAM,
active-transfer loss, scale, and the Grand Challenge remain unresolved.
Evidence: `acceptance/artifacts/minimum-cut-fafead8bd.json`.

The next smallest safe task is multi-output task identity with stable
per-output-slot IDataIDs, nested partial demand, retry stability, and
loss/recovery of only one demanded output.

### Phase 9 persistence/global-loss/pruning recovery-barrier checkpoint

Commit `aac966a09a` closes one actual runtime race. While a worker persistence
task is in `writing`, Scheduler records a pruning proof, invalidates the only
volatile replica, records the post-loss proof, and cancels the exact
persistence request. Recovery remains suspended until both the old request
task has drained and TaskVine has acknowledged deletion of the old cached
file. Only then is the original logical TaskID released as ordinary pending
computation. Request-ID matching prevents an unrelated request for the same
IDataID from opening the barrier.

The first three implementations were rejected by their end-to-end tests:
unavailable IData was retried for persistence; prune acknowledgement raced
ahead of old-request completion; and DataID-only drain matching admitted a
new attempt too early. Those failures also revealed that worker curl transfer
accepted an HTTP 409 response body as a cache file. TaskVine now uses
fail-on-HTTP-error semantics, so the error body cannot become a replica, and
remote-URL failure is no longer misclassified as a malformed peer transfer.

After the final prescribed clean build/install, all 16 installed-path
regressions pass and three consecutive local race repetitions pass. The
rebuilt package SHA-256 is
`d9fff8aef1e52a2d8ab574de9903df06e463a3221da370d977c78e498b2a18ce`.
Package-only factory `datavine-persist-loss-aac966a09` used two workers and
completed the exact oracle through four physical attempts for two logical
tasks, two ordinary recovery reexecutions, one active-persistence global
loss, one 2,097,161-byte worker persistence, zero legacy recovery tasks, zero
stale persistence completions, and 79 bytes of Controller IData high-water.
The pre-loss pruning proof is `keep` with `persistence-writing`; the post-loss
proof is `absent` with `no-accepted-replica`. Both factory workers were
removed. Evidence:
`acceptance/artifacts/persistence-loss-race-aac966a09.json`.

Self-review leaves `RACES`, `RECOVERY`, `MIN-CUT`, Review B, and Ultimate
Acceptance FAIL/OPEN. The next smallest safe task is a real minimum
recoverable-cut runtime checkpoint with two durability frontiers and a second
loss after the first recovery, proving bounded rollback and safe pruning of
the covered upstream branch.

### Phase 9 bounded Controller IData and worker-local large-IData checkpoint

Runtime commit `53db69f1e` adds explicit Controller retained-IData and
per-object inline capacities. Outputs at or below the inline threshold retain
the existing validated Controller fallback; larger outputs publish only
stable IDataID, attempt, SHA-256, and serialized size. Their serialized bytes
remain in the attempt-qualified TaskVine worker/peer cache identity. Logical
availability now derives from current physical replicas, while
`rematerializable` separately means a stable non-worker Controller or durable
source. The cache policy can no longer delete the last worker-only future
input merely because some volatile replica is currently observable.

The first large-IData run was rejected: a released worker remained briefly
visible and a downstream task failed terminally after its selected source
disappeared. The correction reconciles worker epochs on input failure and
turns global loss into ordinary logical rollback. A later complete regression
exposed that deterministic worker-loss injection itself could dispatch a
consumer before asynchronous status convergence. The accepted scheduler
revokes producer completion in the same injection event and decrements
logical input-use counts only once across attempts. Test commit `e6ef08b16`
requires the same churn in the factory run.

After the final prescribed clean build/install, all 16 installed-path
regressions pass. The accepted local and package-only factory workload creates
a 2,097,152-byte intermediate with Controller retained-IData capacity 131,072
bytes and inline-object limit 65,536 bytes. It completes the exact digest
oracle after one worker release using three ordinary attempts for two logical
tasks, one recovery replay, and zero legacy recovery tasks. The Controller
retains only the 79-byte final result; its IData high-water is 79 bytes and
the large intermediate produces two metadata-only publications.

The rebuilt package SHA-256 is
`f56ea4078e90cebcf58f4f5592c899761544931ad55bc7771e06386b7570ad07`;
`poncho_package_run -e` verified the new capacity behavior. Factory
`datavine-idata-e6ef08b16` used two requested package workers, injected churn,
passed, and removed both workers.

Self-review keeps `CTRL-BOUND`, `PERSIST`, `RECOVERY`, Review B, and Ultimate
Acceptance **FAIL**. Retained bytes are bounded, but Controller metadata and
completed workflow history are not cleaned; large IData lacks worker-driven
persistence and large final-result return; repeated churn, DRAM, and the Grand
Challenge remain open. Evidence:
`acceptance/artifacts/idata-capacity-e6ef08b16.json`.

### Phase 9 worker-driven large-IData durability checkpoint

Commit `4e8f19f1f` extends the bounded-IData path without returning payloads to
the Controller. For metadata-only IData, the Controller creates a bounded,
attempt/hash/size-qualified persistence request and target. A bounded
Scheduler data-operation task consumes the existing TaskVine file identity;
the worker validates the source, writes a same-directory temporary file,
fsyncs, renames, and acknowledges. The Controller stream-validates outside
its state lock, then compare-and-commits the unchanged request as a SharedFS
replica and durability frontier. Scheduler never reads persistence payload
bytes.

The same durable path supports a large final result. A requested final that is
not Controller-inline is accepted only when durable; Scheduler validates the
SharedFS bytes and deserializes the result. Explicit durable recovery validates
and re-registers the SharedFS source without loading it into Controller
memory. Duplicate begin/complete for the same durable request is idempotent;
different attempts remain stale.

After the final clean build/install, all 16 regressions pass. Local and
package-only factory runs combine a 2 MiB worker-only IData, deterministic
worker release, ordinary recomputation, worker-driven persistence, downstream
consumption, and return of that same large object as a final. The accepted
factory run completes two logical tasks through three ordinary attempts, one
recovery replay, one 2,097,161-byte worker persistence operation, and zero
legacy recovery tasks. Controller IData high-water remains 79 bytes under a
128 KiB limit.

Package SHA-256 is
`9eb6c9e6ba2f31b5989cd0e0c35408a6f17423aee6dca8c2d90121558fdb4db2`;
factory `datavine-persist-4e8f19f1f` passed and removed both workers. Evidence:
`acceptance/artifacts/worker-persistence-4e8f19f1f.json`.

Self-review keeps `PERSIST`, `CTRL-BOUND`, `RECOVERY`, Review B, and Ultimate
Acceptance **FAIL**. Active-write cancellation, SharedFS overload/failure
retry, validation latency, data-operation fairness, repeated recovery, and
Grand Challenge scale remain unproved.

### Phase 9 active worker-persistence cancellation checkpoint

Commits `52f7b8139` and `426ea2195` make worker-driven persistence
cancellable after it has entered `writing`. Controller owns the transition
from `writing` to `cancelling`; a late acknowledgement removes the target,
releases the bounded active slot, remains non-durable, and causes Scheduler
to issue a new request for the same stable IDataID. Duplicate completion
remains idempotent only for the exact durable request.

The first package-only factory PASS at `52f7b8139` was rejected during
self-review. Its delay injected cancellation before Controller validation,
but cancellation arriving after stream validation and before the final
compare-and-commit was incorrectly classified as stale. Commit `426ea2195`
closes that race. A deterministic threaded test blocks at the directory-fsync
boundary, cancels the active request, then proves that completion returns
`cancelled`, deletes the target, releases admission, and permits a new request
to reach durable. Worker logs also distinguish a cancelled acknowledgement
from `DATAVINE_PERSISTED`.

The prescribed clean build/install and all 16 installed-path regressions pass.
The rebuilt archive SHA-256 is
`57b98edc2b5583d9dcfe49fb1698cc3c0f23940ff13fd53f9e5ef625967d54d6`,
and `poncho_package_run -e` verified TaskVine, Workflow, and worker persistence
imports. Factory `datavine-cancel-race-426ea2195` used two package workers,
performed one active cancellation and successful retry, persisted 2,097,161
bytes on a worker, evicted one worker, recovered through one ordinary task
replay, produced the exact oracle result, used zero legacy recovery tasks,
kept Controller IData high-water at 79 bytes, and removed both workers.

Self-review leaves `PERSIST`, `RACES`, Review B, and Ultimate Acceptance
**OPEN/FAIL**. The next smallest safe task is deterministic runtime injection
of failed and temporarily unavailable SharedFS writes, with bounded retry,
admission/backpressure, independent Controller responsiveness measurement,
and no Scheduler starvation. Evidence:
`acceptance/artifacts/worker-persistence-cancel-426ea2195.json`.

### Phase 9 bounded SharedFS failure recovery checkpoint

Commit `c4d5258b6` gives worker persistence explicit, Scheduler-owned failure
recovery rather than relying on TaskVine's opaque task retries. Each failed
attempt reaches a terminal Controller state; retry uses a new persistence
request for the same attempt-stable IDataID. Retry count, exponential delay,
and maximum delay are independently bounded. Zero allowed retries produces an
explicit workflow failure.

The worker failure injection writes part of the SharedFS temporary file,
delays while ordinary work can run, reports a stable failure marker, and
raises. The worker error path removes the temporary and tells Controller that
the request failed. Two consecutive injected failures are required before the
accepted third request can publish durable.

Self-review rejected the first implementation even though the failure workflow
completed: Controller-inline and worker persistence had separate admission
checks, so a configured concurrency of one reached a global high-water of two.
The accepted Controller has one active-request authority across both paths.
A second review correction added a separate maximum retry delay; a finite
retry count alone was not accepted as a complete time bound.

After the final prescribed clean build/install, all 16 installed-path
regressions pass. Local contracts additionally prove that Controller state
queries remain responsive while streaming durability validation is
deliberately blocked, no `.tmp` survives, normal compute completes while
persistence is active, and a permanent failure is visible.

The package-only factory run at exact commit `c4d5258b6` uses archive SHA-256
`592cc09333d91ff2b36ce94a06d18189de18b874dcedcb202b88dcd00684ac2a`.
It observes two partial-write failures, two bounded retries totaling 0.75 s,
global persistence high-water one, one ordinary compute completion during
persistence, no temporary files, one 2,097,161-byte successful worker write,
one worker-loss recovery replay, zero legacy recovery tasks, exact output, and
79 bytes of Controller IData high-water. Both package workers were removed.
Evidence:
`acceptance/artifacts/worker-persistence-failure-c4d5258b6.json`.

Self-review keeps `PERSIST`, `RACES`, `PERF-FS`, Review B, and Ultimate
Acceptance **OPEN/FAIL**. The next smallest safe task is a single deterministic
runtime schedule that races persistence completion with global-loss detection
and pruning proof evaluation. Scale-level separate read/write admission,
queue/fairness measurement, real filesystem outage behavior, and the Grand
Challenge remain unproved.

### Phase 9 worker-enforced disk cache capacity and combined recovery checkpoint

Commits `3993bd475` through `6d3b77042` add worker-side hard item/byte
admission, reserve normal-task output slots before execution, reject
unpublished or oversized outputs, and make Scheduler dispatch wait until an
acknowledged targeted unlink has actually released physical capacity.

After the required clean build/install, the source and installed workers had
the identical SHA-256
`8ccbc98c343542cf4f39771ca617247c4517a88045206c8253e3e3c146000a7a`.
All 15 installed-path regressions passed. The local bounded workflow stayed at
six items and 239,308 bytes on each worker; a five-item task working set failed
before execution, and a 1 KiB worker byte limit rejected an oversized output
without publishing it. A combined prefetch/cache-pressure test completed
seven logical tasks through eight ordinary attempts and one recovery replay
after two intentional worker disconnects, while both workers remained at or
below six items and 238,743 bytes.

The environment archive was rebuilt from the active DataVine environment.
`poncho_package_run -e` verified cloudpickle 3.1.2 and the new worker symbol;
archive SHA-256 is
`667b2615e28566650e56afca48ab38fc3604ad36e8b1dbbf92b8ed00fde05671`.
Two package-only factory workers reproduced the recovery workflow under the
same limits and were removed cleanly.

Self-review rejected three non-acceptance results: a build whose install step
hit `Text file busy` because interrupted local workers still held the binary;
a factory sequence contaminated by reusing one catalog project name; and an
eviction policy that deleted future-used volatile IData when the Controller
reported another source. The last policy caused TaskVine to submit special
legacy recovery tasks and could race a newer replica generation, so it was
fully removed.

The complete `CACHE`, `RECOVERY`, Review B, and Ultimate Acceptance rows remain
**FAIL**. DRAM remains unbounded, the injected loss is a Manager worker release
rather than a proven process kill, active-transfer loss is absent, and
future-used volatile IData cannot yet be evicted without falling into the
obsolete TaskVine recovery path. Machine-readable evidence:
`acceptance/artifacts/worker-cache-capacity-6d3b77042.json`.

### Phase 9 DataVine-owned IData rematerialization checkpoint

Commits `997d63acf` and `9512638c5` replace DataVine's `VINE_TEMP` output
identity with an attempt-qualified, Controller-backed URL that is both a
writable worker cache identity and a stable rematerialization source. Only a
Controller-qualified URL may be used as a task output. Consumers still prefer
worker/peer cache, but a missing physical replica can be fetched directly
without asking TaskVine to create a special recovery task. The Controller
returns HTTP 409 for an old attempt URL after a newer attempt is published.

The first package-only factory run was rejected when worker release raced
cache eviction and exposed a stale local replica generation. Commits
`f6c1c712e` and `f1237b8b8` correct that race with a Controller-atomic observed
invalidation: attempt, content hash, size, worker identity, and worker epoch
must still match, then the Controller invalidates its current physical
generation. A stale generation can no longer fail the workflow merely because
the same physical cache identity was safely rematerialized; different content
or a different incarnation still fails closed.

After the repeated required clean build/install, all 15 installed regressions
pass. Local bounded and zero-cache cases physically evict IData with one
future consumer and reconstruct the exact oracle. The combined local run has
seven logical tasks, eight ordinary attempts, one DataVine recovery replay,
and zero TaskVine recovery tasks. The protocol test advances a physical
replica from generation 1 to 2, invalidates generation 2 through the
identity-checked operation, and rejects a wrong hash.

The rebuilt archive SHA-256 is
`2bbb11c55b86b08719b4173e32ad0a93b0cdb1961dc68c948ce83342a1cec9f6`.
Factory `datavine-idata-f1237b8b8` reproduced future-used IData eviction,
prefetch, one worker release, one ordinary recovery replay, exact output, and
zero legacy recovery tasks. Physical cache high-water was six items and
238,726 bytes against limits of six and 238,743; both workers were removed.

Self-review keeps `CTRL-BOUND`, `CACHE`, `RECOVERY`, Review B, and Ultimate
Acceptance **FAIL**. The Controller still retains ordinary IData bytes, so
this checkpoint proves ownership and rematerialization but not the required
volatile worker-local/bulk-data architecture. Worker loss was a Manager
release, only one recovery cycle ran, DRAM remains absent, and the factory
Scheduler's soft logical observation briefly counted seven records while the
worker's authoritative physical high-water remained six. Evidence:
`acceptance/artifacts/idata-rematerialization-f1237b8b8.json`.

### Phase 9 shadow pruning checkpoint

Commit `2108b68a8` adds a full-scan pruning oracle and an event-indexed
incremental evaluator in the independent runtime. The installed-path test
passes 40 deterministic random DAGs and 6,400 state events with zero semantic
mismatches and zero observed false-positive prune decisions. It also covers
dynamic growth, multiple frontiers, mixed durability, repeated recovery,
queued-persistence cancellation decisions, proof revisions, and recovery-depth
progression. The full local topology and Phase 4–8 regressions pass after the
required clean build/install.

This is **shadow-only**. No physical data is deleted. Architecture Review B is
FAIL because replica epochs/tiers, in-flight reads, atomic runtime revisions,
real persistence cancellation, and SharedFS quarantine are not implemented.
Machine-readable evidence:
`acceptance/artifacts/phase9-shadow-20260729.json`.

### Phase 9 physical replica directory checkpoint

Commit `e1843b9bd` adds a Controller-owned, fail-closed physical replica
directory. Physical keys are qualified (`e:<id>` or `i:<id>`) so equal numeric
EData and IData IDs cannot collide. Records distinguish preparation,
availability, retirement, invalidity, SharedFS quarantine, and final pruning;
worker replicas carry monotonically checked worker epochs and logical attempt
numbers.

Source selection returns multiple deterministic candidates and acquisition
revalidates the selected generation and both worker epochs. Active source
leases protect concurrent readers: invalidation moves an in-use source to
`retiring`, and the final release makes it invalid. Replica, worker, active
lease, and completed idempotency-tombstone collections all have explicit
capacities; terminal records have revision-checked cleanup.

The installed-path component test and 20 repeated race runs pass after the
required clean build/install. They include two concurrent destinations, source
invalidation during both reads, stale source selection, partial publication,
duplicate commit/release, old attempt completion, worker reincarnation,
corrupt metadata, one-of-many versus all-replica loss, zero-byte objects,
SharedFS quarantine/restore/grace/hard-delete, overload rejection, and terminal
cleanup.

This checkpoint does **not** close Review B. The directory is not yet wired to
ControllerState, worker protocol, persistence generations, or the pruning
executor, and no real bytes are deleted. Evidence:
`acceptance/artifacts/replica-directory-e1843b9bd.json`.

### Phase 9 persistence generation-safety checkpoint

Commit `17577b058` wires Controller-owned EData, volatile IData, and durable
IData records into the physical replica directory and replaces unqualified
persistence callbacks with bounded requests carrying request ID, IDataID,
attempt, and content hash. SharedFS targets are attempt- and
content-addressed, so an old attempt cannot overwrite a new value.

Queued cancellation, active cancellation before the atomic commit boundary,
and explicit too-late cancellation after that boundary are distinct.
Controller callbacks compare the full request generation and reject a
completion after a newer attempt is published; a stale committed file is
removed and cannot acknowledge durability. The cancellation path is available
through protocol v1, and persistence queue/active/terminal collections expose
hard capacities and high-water metrics.

The deterministic race suite reproduces queued overload, active cancellation,
and old completion concurrent with a new attempt. It passes through both
direct state calls and the real HTTP service/client, repeats 20 times, and
reports zero callback or cleanup failures. The complete installed local
topology and Phase 4–9 regression passes after the required clean
build/install.

Review B remains **FAIL**: pruning does not yet issue these cancellations,
worker DRAM/disk replicas are not protocol-connected, and SharedFS quarantine
does not yet rename/delete real files. Evidence:
`acceptance/artifacts/persistence-races-17577b058.json`.

### Phase 9 worker replica protocol checkpoint

Commit `fbddcc70d` connects the physical replica directory to the real
Controller HTTP protocol, Task Scheduler, and TaskVine worker processes.
Every worker process exports its unique TaskVine WorkerID as an incarnation
identity, joins the Controller, and reports only replicas whose qualified
DataID, attempt, content hash, and serialized size match Controller truth.
Worker output publication is two phase: the task prepares its local output
replica, and the Scheduler commits that exact generation only after TaskVine
reports successful task completion.

The Scheduler reconciles Controller worker incarnations against TaskVine
manager status after every wait. A disappeared worker invalidates all of its
available and preparing replicas; a late commit cannot resurrect them.
TaskVine worker status now exposes WorkerID for local as well as factory
workers. Corrupt local cache reports are invalidated before fallback, and
zero-byte IData is accepted when its hash is correct.

The required clean build/install passes. The protocol component test passes 20
repetitions, and installed topology, Phase 4 worker-loss, and Phase 7 recovery
tests pass with zero leaked preparing replicas. The recovery run executes four
logical tasks through five ordinary attempts, records one worker
disconnection, leaves five old replicas invalid, and completes with the exact
result.

Self-review keeps Review B **FAIL**. The source records do not yet expose
fetchable peer endpoints, runtime byte transfers do not yet acquire Controller
leases, worker cache capacities are not owned by DataVine, and worker
reconciliation assumes one Scheduler/Controller workflow pair. Evidence:
`acceptance/artifacts/worker-replica-protocol-fbddcc70d.json`.

### Phase 9 revision-safe SharedFS pruning checkpoint

Commit `347f60531` makes the Controller's full-reference-checked incremental
lineage proof part of runtime state. Task records now carry the complete,
sorted IData dependency set, including dependencies hidden inside nested
containers. Scheduler pending/running/completed/recovery transitions update
the same Controller-owned proof state.

The compare-and-apply protocol rejects stale graph/state revisions. It cancels
obsolete queued persistence, preserves active writes, invalidates
Controller-memory replicas, retires replicas with active source leases, and
renames owned durable files into a private SharedFS quarantine using file and
directory durability barriers. Quarantined files are excluded from source
selection. Restore validates size/hash before making the replica available.
Hard deletion requires a fresh proof, an expired grace period, unchanged
replica generation/revision, no lease, and a machine-readable audit record.

The E2E deliberately mutates required-output state to reject an old proof,
cancels queued persistence while another write is active, holds a source read
through pruning, corrupts a quarantined file, adds a dynamic consumer, restores
from quarantine, rejects early deletion, and finally deletes only after a new
proof and grace expiry. The retained required output remains exact. The test
passes 20 repetitions; installed topology and Phase 4–9 regressions pass, and
the worker-loss recovery case passes five repetitions after removing a manual
disconnect race.

Self-review found and fixed four destructive-ordering defects before this
checkpoint: absent quarantined data could never reach hard delete; unlink ran
before grace validation; lineage rejection could leave a half-registered
task; and corrupt quarantine bytes could be exposed before validation.

At this checkpoint Review B remained **FAIL**. Worker-local files were not yet
physically deleted, real transfers did not acquire source leases,
quarantine/audit state was not restart-persistent, and stable bulk origins plus
pin/final-output protocol coverage remained open. Evidence:
`acceptance/artifacts/sharedfs-pruning-347f60531.json`.

### Phase 9 acknowledged worker-local pruning checkpoint

Commit `3f993f15b` connects proven Controller pruning decisions to physical
TaskVine worker-cache deletion. The Manager assigns every unlink a UUID,
accepts an acknowledgement only from the intended worker and pending
operation, ignores duplicate/stale/reordered acknowledgements, and releases
completed tracker state so workflow history does not grow per pruned file.
Legacy unlink remains unacknowledged and unchanged.

Worker output and later local-input observations now use one physical replica
identity per WorkerID/DataID instead of inventing a second attempt-suffixed
record for the same cache file. The Scheduler requires Controller and
TaskVine physical replica counts to agree, waits for all worker
acknowledgements, and only then advances the exact Controller generations from
`invalid` to `pruned`.

The accepted eight-task, two-worker fan-out/fan-in workflow has multiple
physical replicas for shared IData. Across five local repetitions it issued
10–11 physical unlinks per run, received exactly the same number of unique
successful acknowledgements, decreased observable cache entries by exactly
that count, marked the same number of Controller replicas pruned, released
every acknowledgement tracker, and returned oracle value 62. Replica,
SharedFS pruning, Phase 4–9, and worker-loss recovery regressions pass after
the required clean build/install.

The rebuilt `datavine.tar.gz`
(`af92ca3718fab236366b307d6ad98b4bd30df0b04c72b31ff8c2e41677cfd663`)
contains Python 3.10.20, cloudpickle 3.1.2, the prune-state cleanup symbol, and
the UUID acknowledgement protocol. The prescribed factory supplied two
workers; the same workflow completed with 11 requests, 11 acknowledgements,
11 Controller-pruned replicas, no failures, and exact oracle output. Both
factory workers were removed.

Self-review keeps Ultimate Acceptance and Review B **OPEN/FAIL**. Worker loss
while an unlink ACK is pending is not handled, real transfers still bypass
Controller source leases, worker cache capacities/admission are not
DataVine-owned, and recovery after local pruning has not yet passed. Evidence:
`acceptance/artifacts/worker-local-pruning-3f993f15b.json`.

### Phase 9 bounded Controller request and byte-serving checkpoint

Commit `d694bef4a` replaces the unbounded HTTP thread model with two explicit,
fail-closed admission layers. The standalone Controller has a hard maximum
number of live request threads. Byte responses additionally acquire both a
concurrency slot and an in-flight byte budget before any payload is sent.
Overload returns HTTP 503 without creating an unbounded queue. Active counts,
high-water marks, rejections, admitted responses, and completed bytes are
reported in the Controller snapshot.

The deterministic test holds one 524,297-byte response in flight, proves a
second response is rejected at concurrency one, proves metadata remains
responsive on a separate admitted request, and rejects a 1.5 MiB serialized
object against a 1 MiB byte budget. It then verifies both active counters
return to zero. A separate request-capacity case holds the sole request slot
and receives an immediate 503 for another request. Twenty repetitions pass.

After the required clean build/install, all current DataVine component and
Phase 4–9 tests pass. The environment archive was rebuilt from the DataVine
environment and verified to contain Python 3.10.20, cloudpickle 3.1.2, and the
new bounded server. Its SHA-256 is
`9c31fdbe190e223a38708db17bb7064e687eb69b83a9b5056d4641eab9161391`.
The prescribed factory supplied two workers to the Phase 4 workflow; normal
and shared-input modes returned exact results, exercised both workers, and
showed bounded request/byte-serving high-water telemetry. The factory was
stopped and both workers removed.

Self-review keeps `CTRL-BOUND`, Review B, and Ultimate Acceptance **FAIL**.
The new limit deliberately rejects a payload larger than the byte budget; it
does not yet provide the required stable bulk origin and large-object bypass.
Actual TaskVine peer movement still does not acquire Controller leases, and
Controller restart remains untested. Evidence:
`acceptance/artifacts/controller-admission-d694bef4a.json`.

### Phase 9 stable bulk EData origin checkpoint

Commits `13193c99a` and `643cddd68` add a content-addressed stable-origin path
for serialized EData that is too large for Controller memory or byte-serving
budgets. The Scheduler cloudpickles each repeated object reference once,
assigns serialization domains so function/value/container bytes cannot alias,
writes one atomic read-only origin, and registers its path, size, metadata, and
metadata-aware hash. The Controller accepts only regular content-addressed
files beneath its configured root, streams the hash check, records a SharedFS
replica, and never admits or serves the bulk bytes.

The deterministic two-worker workflow reuses one 4,194,313-byte serialized
object through eight bindings while both Controller capacities are 1 MiB. It
returns the exact oracle with alias identity intact, one EDataID, one bulk
serialization, 1,170 Controller inline bytes, and 3,146 Controller-served
bytes in the accepted factory run. Hash mismatch, root escape, symlink,
cross-domain aliasing, and a distinct oversized inline registration all fail
closed. All 14 current DataVine regressions pass after the required clean
build/install.

Self-review of the first factory recovery run exposed a real epoch bug:
reconnected workers hard-coded epoch 1 and were rejected as stale. Commit
`643cddd68` moves incarnation allocation to the Controller, makes active claims
idempotent, advances an inactive identity, and preserves explicit stale-epoch
rejection. Five local recovery repetitions and the same factory worker-loss
workflow now pass through five ordinary attempts for four logical tasks with
one disconnection and one replay.

The rebuilt package SHA-256 is
`857eb5a8d4f586c369ab0755b7f249557a8b1c08e1e3f367a5635dbfef3a5cd6`.
The accepted factory `datavine-epoch-643cddd68` was stopped and both workers
were removed. The earlier failing package/run is recorded as rejected evidence,
not acceptance. At this checkpoint `CTRL-BOUND`, Review B, and Ultimate
Acceptance remained **FAIL**: actual transfers did not acquire Controller
leases, cache admission was not DataVine-owned, bulk-origin mutation/restart
recovery was open, and Controller history cleanup plus the Grand Challenge
remained absent. Evidence:
`acceptance/artifacts/bulk-origin-643cddd68.json`.

### Phase 9 direct transfer authority checkpoint

Commits `4367f95ca`, `11e555bef`, and `ef605c343` connect TaskVine's actual
worker-to-worker byte-transfer path to Controller-owned source leases.
TaskVine files carry their qualified EDataID or IDataID; substitute peer URLs
preserve that identity. Before a peer pull is sent, the Manager acquires an
idempotent lease tied to the Controller's current source and destination
worker epochs. Success, failure, and worker removal terminate the physical
transfer and release the lease. A failed release retains only a detached,
bounded retry record, so it cannot reference a deleted worker.

The accepted Phase 5 test was strengthened because its old two-worker metric
did not prove shared-data consumers ran on both workers. Two single-core
workers now execute the shared consumers concurrently. Across three local
repetitions, peer-on performed exactly 5 authorized acquisitions and 5
releases with zero active leases; peer-off performed zero acquisitions.
Controller fetches for the 1,769,481-byte shared EData remained one with peer
transfer and two without it.

Self-review of the first full regression exposed an authority/fallback bug:
prefetch can create a TaskVine cache replica before a DataVine runner reports
it. The Controller correctly rejected this unverified source, but TaskVine's
old error path aborted scheduling. Commit `ef605c343` preserves sole
Controller authority, discards the substitute, and retries the stable origin
without compute rollback. Phase 8 now asserts this rejection/fallback path,
balanced authorized leases, zero leaked leases, and the exact oracle.

The required clean build/install and all 14 local regressions pass at
`ef605c343`. The rebuilt package SHA-256 is
`fc44eadfc93a207f919279854036701248dafe4b98ba6bc45279a253ba89e110`.
Factory `datavine-transfer-ef605c343` passed peer-on/off, prefetch fallback,
and worker-loss recovery; it was stopped and both workers were removed.

Review B and Ultimate Acceptance remain **FAIL/OPEN**. Worker loss during an
active peer transfer and Controller release-timeout retry are not yet
deterministically injected, worker cache capacities/admission remain outside
DataVine authority, and the Grand Challenge is absent. Evidence:
`acceptance/artifacts/transfer-authority-ef605c343.json`.

### Phase 9 acknowledged cache-retention checkpoint

Commits `2e0d8ebdd` and `c20db01a1` add the separate
`datavine.cache.admission` module and targeted TaskVine
`prune_file_on_worker`. Worker observations carry exact generations. The
policy evicts only replicas with zero remaining direct consumers, invalidates
through the Controller, requests one WorkerID-targeted unlink, waits
asynchronously for its UUID acknowledgement, and only then confirms pruning.

The first two-branch stress run exposed a stale-generation race when an
earlier prototype evicted shared EData with future consumers. The Controller
correctly rejected the old acknowledgement after a new generation appeared.
The accepted policy excludes all data with remaining direct consumers.
Self-review also corrected worker-loss semantics: a missing worker resolves a
pending tracker as failed/unavailable, not physically deleted, because a
keep-workspace directory may survive.

At exact commit `c20db01a1`, the prescribed clean build/install and all 15
local regressions pass. The two-chain/fan-in case actually uses both workers,
performs 32 acknowledged evictions, and finishes with six observed retained
items per worker under a six-item target. Zero retention performs 22
acknowledged evictions and finishes empty. Pending unlink plus immediate worker
loss yields one request, zero confirmations, one explicit failure, and a
released tracker.

The rebuilt `datavine.tar.gz` SHA-256 is
`852eb4aeaa1d7041046ea1a514aef9d949e74dd48eee28c7cce25193a130091d`.
Factory `datavine-cache-c20db01a1` reproduced both modes with two workers, then
stopped and removed both workers.

This is deliberately **not** strict cache-capacity acceptance. Observed
execution high-water was nine items for the six-item target and seven for zero
retention because task working sets and asynchronous unlink remain outside
admission control. No DRAM tier exists. `CACHE`, Review B, and Ultimate
Acceptance remain **FAIL**. Evidence:
`acceptance/artifacts/cache-retention-c20db01a1.json`.

### Phase 9 strict cache-item dispatch admission checkpoint

Commits `9d03dbf4d` and `88b7d1a44` add a Manager-side item admission gate
before worker selection. Its projection includes the worker's reported cache,
outputs reserved by already assigned tasks, the candidate's distinct inputs
and outputs, and physical files whose acknowledged unlink is still pending.
The Scheduler rejects a capacity smaller than the largest task working set and
reduces dead-data retention to reserve that working-set headroom.

Two prototypes were rejected during self-review. Retaining six dead items
under a six-item admission limit deadlocked the next root task. After adding
headroom, evicting reproducible EData exposed a stale confirmation race against
another running root; running-task inputs are now explicitly protected.

At exact commit `88b7d1a44`, the required clean build/install and all 15
installed regressions pass. The accepted 13-task, two-worker run uses a
six-item capacity: both physical worker high-water marks are exactly six,
admission rejects six placements while other placements continue, 44
acknowledged evictions complete, both final caches are empty, and no unlink is
pending. A five-item limit for a six-item task fails closed before execution.

The rebuilt package contains cloudpickle 3.1.2 and has SHA-256
`fc8003bb0a5422909214cb62311f1678ea5de37c37a192e0abe0fdcfe75e6e71`.
Factory `datavine-cache-88b7d1a44` reproduces the six-item physical bound on
both workers, records seven admission rejections, returns the exact oracle,
and removes both workers on shutdown.

This is a strict **Manager-coordinated item** bound, not full cache acceptance.
The worker itself does not enforce the contract, byte and DRAM bounds are
absent, prefetch inputs are not yet protected by this accounting, admission
under recovery/churn is untested, and the projection currently allocates a
temporary set in the scheduling hot path. `CACHE`, Review B, and Ultimate
Acceptance remain **FAIL**. Evidence:
`acceptance/artifacts/cache-item-admission-88b7d1a44.json`.

## Phase 8 acceptance

The independent Scheduler now derives deterministic prefetch candidates from
the already-materialized logical graph. It selects only repeated EData inputs
under explicit byte and item budgets, submits zero-core prefetch operations at
priority `-1000`, and leaves ready demand tasks at the normal priority. The
policy never registers or serializes application values a second time.

The accepted two-worker workflow has a slow ready root and six consumers of a
shared 1,638,409-byte EData object. In both local and factory runs:

- three deterministic candidates totaling 1,638,959 bytes were selected under
  the 8 MiB test budget;
- all three prefetch operations completed and overlapped useful execution;
- the first running TaskVine ID was demand task 4, ahead of prefetch IDs 1–3;
- the eight logical demand tasks completed through exactly eight physical
  demand executions;
- the workflow performed 21 registrations in enabled, failure, and disabled
  modes, proving policy inspection adds no serialization/registration work;
- injected failure made all three prefetch operations fail without changing
  the exact workflow result;
- disabling prefetch selected zero candidates and produced the same result.

Nested `OutputRef` bindings are also restored in the new runtime. Composite
arguments are reconstructed from a copied template with an OutputRef-to-IData
memo, so repeated nested references preserve alias identity. The two-task
nested case produces the exact result and records a validated local IData hit.

The exact prescribed clean build/install passed after the final source change.
The complete local topology plus Phase 4–8 regression passed. With the rebuilt
package, the two-worker Phase 8 factory suite passed all four modes and a final
combined Phase 4–7 factory regression also passed. The final package-only
rerun used `datavine-phase8-postlint-20260729`; it was stopped and both workers
were removed.
The accepted package SHA-256 is
`4a283955d934c6f6a4123fc877ff9a4c185ca70afd1634dcd5ad6a162e7c85c5`.
`poncho_package_run` also imports the installed
`ndcctools.taskvine.datavine` package successfully with cloudpickle 3.1.2.

Machine-readable results:

- `phase0-artifacts/baseline-datavine-phase8-local-20260729.json`
- `phase0-artifacts/baseline-datavine-phase8-factory-20260729.json`

### Phase 8 self-review / self-critique

- The first policy draft serialized candidate values during fanout analysis,
  doubling registrations from 21 to 42. That design was rejected. The accepted
  policy reads only existing TaskRecords and DataIDs; all three main modes now
  remain at exactly 21 registrations.
- Merely observing completed prefetch tasks would not prove priority. The gate
  checks the TaskVine running transaction order and requires a ready demand
  task to begin before every low-priority prefetch ID.
- Successful speculative traffic is insufficient correctness evidence.
  Injected prefetch failures and the explicit prefetch-off rollback both retain
  the exact result.
- The combined factory regression initially waited unusually long after many
  short-lived managers reused one project name and left catalog endpoints
  visible. It was allowed to reach its actual two-worker gates; no one-worker
  result was accepted as two-worker evidence. All four phases passed.
- Final lint found three project-style slice-spacing violations and one unused
  import. After the mechanical cleanup, the prescribed clean build/install,
  project-rule flake8, topology test, local Phase 4–8 suites, package rebuild,
  `poncho_package_run`, and two-worker factory Phase 8 suite all passed again.
- The policy is deliberately conservative: static fanout, size, and fixed
  budgets, with TaskVine priority providing demand precedence. It does not yet
  model network topology or continuously changing worker load.
- Phase 9 recovery-aware pruning is a separate roadmap phase and has not been
  started under the Phase 4B–8 objective.

## Phase 7 acceptance

IData is now both a logical Controller record and a TaskVine temporary object.
The producing worker stages/fsyncs canonical bytes, publishes the logical
IData, and exposes the same bytes as a worker-local TaskVine output. Downstream
workers mount that IData by ID, validate its Controller-owned content hash, and
consume it locally or through TaskVine peer transfer before falling back to
Controller bytes.

Global loss is distinct from a local miss. When a non-durable IData has no
accepted realization, the Controller marks it unavailable. The Scheduler
removes the original producer from `done`, prunes obsolete physical state, and
submits that same logical TaskID with the same IDataID and an incremented
attempt. No separate recovery-task semantic is created.

Accepted recovery evidence:

- one worker is connected and runs producer TaskID 1;
- TaskVine intentionally evicts that worker after volatile publication;
- Controller invalidates the only non-durable realization;
- a replacement worker runs producer TaskID 1 attempt 2;
- four logical tasks complete through five normal physical executions;
- exactly one lineage recovery re-execution occurs;
- local test records two worker IDs; factory records the explicit worker
  disconnection/reconnection lifecycle;
- downstream execution records four validated worker-local IData hits;
- no-loss rollback executes four physical tasks and zero recovery replays.

Phase 4, 5, and 6 suites continue to pass with the new worker-local IData
mounts. Clean build/install and local/factory recovery/no-loss modes pass.
Factory `datavine-phase7-accept-20260729` was stopped and its worker removed.
The accepted package SHA-256 is
`30491e946851aa892c9f0f36f0ae5365dbd0a13f30b95ff5ea3df8f2c690f62a`.

### Phase 7 self-review / self-critique

- The first loss test invalidated logical bytes but did not guarantee that the
  evicted worker held the producer replica. It was rejected. The accepted
  local test begins with exactly one worker, evicts it, and starts a replacement
  after the loss.
- Factory workers may reconnect with the same stable WorkerID. Therefore
  “two WorkerIDs” is not a valid distributed loss oracle; the factory gate uses
  the TaskVine DISCONNECTION record plus the five-execution recovery report.
- IData temporary files reuse TaskVine's proven physical cache and peer
  transfer, while recovery authority is in the new Scheduler/Controller. The
  old VineGraph special recovery task is not used by this runtime.
- Controller memory remains a fallback replica. The failure injection removes
  it deliberately to exercise global loss; ordinary local misses do not cause
  recomputation.
- Nested output references are still not supported by the independent Workflow
  API. This remains a compatibility gap to close before final acceptance.

## Phase 6 acceptance

Worker output now crosses explicit state boundaries:

```text
serialized → worker-local staged and fsynced → published volatile
→ queued → writing → atomically renamed and directory-fsynced → durable
```

The Controller exposes durability separately from availability. Persistence is
handled by a configured bounded queue; the accepted configuration permits one
write at a time. Every durable acknowledgement follows a reread/hash check and
atomic rename. The Scheduler can require durability, or persistence can be
disabled without changing the workflow result.

The seven-output, two-worker workflow passes in all modes:

- enabled: seven durable files, seven valid hashes, maximum active writes 1;
- injected first-write failure: eight requests, one retry, seven durable
  outputs, zero final failures;
- disabled rollback: seven volatile outputs, zero persistence requests/files.

Clean TaskVine build/install, Phase 4/5 regressions, topology test, local
three-mode suite, and the two-worker factory three-mode suite pass. Factory
`datavine-phase6-accept-20260729` was stopped and both workers were removed.
The accepted `datavine.tar.gz` SHA-256 is
`c4b66e32a920c6a4546e33a12e8633bb30d536cc93a2348f683541a929bf5736`.

### Phase 6 self-review / self-critique

- An early implementation persisted Controller-memory bytes but did not expose
  a worker-local staging boundary. It was not accepted; the worker now stages
  and fsyncs before publication and cleans the stage file after acknowledgement.
- A deterministic first-write fault initially had no retry contract. The final
  path records `failed`, retries once through the same queue, and only reports
  success after `durable`.
- The persistence thread is a physical I/O executor; Controller semantic state
  remains serialized by the Controller service/state lock. Maximum active
  writers is measured, not inferred.
- The backend is a local/shared path supplied to the standalone Controller.
  Object-store backends and cancellation of queued obsolete writes remain
  future extensions.
- The initial Phase 6 build command was mistakenly launched at repository root
  and interrupted. No source was lost, but local build products outside
  TaskVine were cleaned. The full repository was rebuilt/installed, followed
  by the exact required TaskVine clean build/install; the interrupted command
  is not counted as evidence.
- Phase 6 still retains Controller memory as the volatile IData source.
  Worker-local volatile publication and lineage-driven loss recovery are the
  Phase 7 transition.

## Phase 5 acceptance

Phase 5 reuses TaskVine's worker-lifetime disk cache and transfer server through
a narrow DataVine placement adapter. EData URLs are Controller-authorized,
worker-driven transfers; cached objects are content-validated before
deserialization. TaskVine peer transfer is enabled by default and can be
disabled as an explicit correctness rollback.

The two-worker staged workflow proves real peer reuse: a 1,769,481-byte shared
EData object is first warmed on one worker and then consumed concurrently on
two distinct workers. With peer transfer enabled, the Controller served that
payload once. With peer transfer disabled, it served the same payload twice.
Both modes produced the exact eight-task result.

Additional Phase 5 gates:

- corrupt cached/peer bytes are rejected and fetched again from the Controller
  stable source before execution;
- Controller total EData bytes never exceed the configured 64 MiB bound;
- the Phase 4 normal/shared/worker-loss suite still passes;
- two worker IDs are present in TaskVine transaction evidence;
- clean build/install and standalone topology tests pass;
- the rebuilt-package two-worker factory passed peer-on and peer-off modes;
- factory shutdown removed both workers.

Machine-readable local result:
`phase0-artifacts/baseline-datavine-phase5-local-20260729.json`.
The accepted archive SHA-256 is
`bd9fd463988ba5dff2244996584a9c61787270ee357eaeaf0b1dfb902d708164`.

### Phase 5 self-review / self-critique

- The first distributed attempts allowed execution after only one worker
  connected, so they could not prove peer transfer. Those attempts are
  rejected. The Scheduler now drives its own TaskVine event loop while waiting
  and requires the requested worker count before submission.
- `workers_connected` statistics were stale before a wait cycle; status-only
  polling was therefore invalid. The accepted gate uses `Manager.wait(1)` on
  the owner thread followed by worker-status enumeration.
- Controller payload counts plus two distinct TaskVine worker IDs distinguish
  peer reuse from single-worker cache reuse.
- Worker capacity currently follows TaskVine's worker-reported disk bound,
  while Controller EData uses a hard total bound. A value-based eviction policy
  is deliberately deferred; correctness currently fails closed on Controller
  capacity exhaustion.
- The signed URL places the workflow token in the transfer URL. It is scoped to
  the workflow but may appear in debug logs; a later protocol hardening task
  should replace it with short-lived object capabilities.
- Phase 5 does not make IData volatile replicas peer-readable. That semantic
  transition belongs to Phase 7.

The architecture was reset in response to the project organization contract.
The new DataVine system is an independent package. The Task Scheduler owns its
TaskVine manager and compute state from one dedicated
`datavine-task-scheduler` thread. The Data Controller is launched through the
separate `datavine_controller` command in another process, where one dedicated
`datavine-controller` thread serializes Controller state transitions. They
share no Python objects and communicate through protocol version 1.

Phase 4 now has a real data path: TaskVine task commands contain a TaskID and
endpoint credentials, not functions or argument payloads. Workers fetch the
Task record and canonical EData/IData bytes from the Controller, cache one
deserialized object per qualified DataID for the duration of execution,
execute, serialize the output once, publish it under the stable IDataID, and
only then may the Scheduler mark the logical task complete.

### Phase 4 acceptance and evidence

1. Standalone process/thread topology, authentication, capacity rejection,
   hash deduplication, checksum validation, and cleanup: PASS.
2. Normal two-task DAG exact result: PASS.
3. Repeated shared input: PASS; 17 registrations produced seven unique EData
   records and ten deduplicated registrations.
4. Repeated references to the same EDataID preserve Python alias identity
   within an execution: PASS.
5. Worker process-group loss during execution followed by a replacement
   worker and exact completion: PASS.
6. Clean prescribed build/install after the runtime source change: PASS.
7. Rebuilt `datavine.tar.gz` factory normal and shared-input workflows: PASS.
8. Controller and worker processes cleaned up after each case: PASS.

Local snapshots:

| Case | Tasks | Unique EData | Registrations | Deduplicated | Available IData |
|---|---:|---:|---:|---:|---:|
| normal | 2 | 4 | 5 | 1 | 2 |
| repeated shared input | 5 | 7 | 17 | 10 | 5 |
| worker loss | 2 | 4 | 4 | 0 | 2 |

The accepted factory archive SHA-256 is
`45fa55d134567018b7e23031361a8c380b5b22218a0459ef4ecdd7448af5c6b4`.
The one-worker factory `datavine-phase4-20260729` completed both distributed
cases and was shut down with all workers removed.

### Phase 4 self-review / self-critique

- The first shared-input run failed because repeated references were
  independently deserialized and lost alias identity. The test was retained;
  the worker now memoizes by qualified DataID, and the exact assertion passes.
- The first worker-loss harness killed only the worker parent, allowing its
  child and inherited connection to survive. The harness now starts a distinct
  process group and kills the whole group; recovery passes with a genuinely
  lost execution.
- EData registration control messages still use base64 JSON, although worker
  demand reads and IData publication use binary bodies. Phase 5 must remove
  this avoidable registration expansion.
- The Controller is still the only stable byte source and stores all IData in
  memory. That is correct for Phase 4 but not bounded distributed caching.
- Worker-loss recovery currently relies on TaskVine physical retry. It is not
  the Phase 7 lineage invalidation/re-execution model and is not claimed as
  such.
- Only direct output references are supported by the new public Workflow.
  Nested binding support must be restored before the new runtime can supersede
  all useful `vine_graph` workflows.

## Phase 4A implementation

`worker_data_agent.py` introduces qualified EData/IData inventory keys,
immutable stable-source and preparation records, and one inventory per
Workflow in each long-lived task-runner process. The Controller now provides:

- deterministic required DataID sets for each TaskID;
- compact wire assignments such as `T2|e1,e3,i1`;
- strict assignment parsing and comparison;
- stable source resolution to Controller context, legacy frontend file,
  legacy parent result, or legacy produced file.

The C materializer adds the compact assignment as the second task-runner
argument without embedding serialized payloads. Before user execution, the
worker verifies the assignment and sources, updates its local inventory, and
writes an audit marker. After successful execution and output validation, the
C completion path verifies that exact marker and records one preparation
audit. Recovery tasks deliberately do not create a second logical preparation
audit; the existing TaskVine recovery adapter continues to restore the
original output.

### Phase 4A acceptance

All gates pass:

1. Empty, partial, complete, and stale inventories are deterministic; a stale
   item with no available stable source fails closed.
2. Unknown TaskIDs/DataIDs, altered assignments, unrequired DataIDs, and
   missing Controller prerequisites are rejected.
3. Assignments contain only compact TaskID and qualified DataIDs, never
   serialized bytes or physical payloads.
4. Worker reports are validated by the actual C completion path exactly once
   for every successful logical task.
5. Normal, repeated-shared-input, worker-loss, and the 13-task
   nested/container/file workflow pass with Phase 4A enabled.
6. Worker Agent-on, Controller-only, Phase 2-only, Phase 1-only, and full-off
   modes pass the full deterministic baseline.
7. A clean prescribed rebuild/install and the rebuilt-package factory suite
   pass.

Machine-readable results:

- `phase0-artifacts/baseline-phase4-worker-agent-local-20260729.json`
- `phase0-artifacts/baseline-phase4-worker-agent-factory-20260729.json`

| Runtime / case | Physical tasks | Worker audits | Makespan | Mismatches |
|---|---:|---:|---:|---:|
| local normal | 4 | 4 | 0.310761 s | 0 |
| local repeated shared input | 5 | 5 | 3.378136 s | 0 |
| local one-worker loss | 7, including 3 recovery | 4 | 6.777762 s | 0 |
| factory normal | 4 | 4 | 0.100564 s | 0 |
| factory repeated shared input | 5 | 5 | 0.107700 s | 0 |
| factory one-worker loss | 7, including 3 recovery | 4 | 7.041292 s | 0 |

The final factory archive was rebuilt with `poncho_package_create` from the
DataVine environment and installed at
`/users/jzhou24/graph_optimization/factories/datavine.tar.gz`. Its SHA-256 is
`2a3a150a967204cc2f1783dc357b41e5425cbfab9291ce10b6adc965810383a6`.
Condor job `4942.0` supplied the worker and was removed after acceptance.

## Phase 3 implementation

`data_controller.py` introduces an immutable Controller registry and compact
per-task materialization plans. When `data-controller=1`,
`Workflow.finalize()` transfers the validated Phase 1 identity and Phase 2
shadow state into the Controller and drops the two duplicate public
authorities. The Controller owns:

- canonical serialized EData bytes and serialization metadata;
- unique lineage-owned IData records;
- workflow key to TaskID and input/output file to DataID mappings;
- callable, argument, return, file, producer, and parent bindings;
- initial EData `controller` availability and IData `unproduced` state.

At the Python-to-C bridge, each logical task queries its Controller plan. The
legacy parent/input/output mounts are checked against that plan before
execution. The C executor checks the expected mount counts again at the actual
submit-time materialization boundary, rejects disagreement or duplicate
materialization, and exposes per-task and aggregate audit counts.

Physical data lifecycle behavior intentionally does not change in this phase:
existing `vine_file` transport, TaskVine replicas, worker caches, recovery
tasks, deletion, and pruning remain adapters. `task-group` is explicitly
rejected when Controller authority is enabled because grouped materialization
does not yet have a one-logical-task/one-audit mapping.

### Phase 3 acceptance

All Phase 3 gates pass:

1. Controller mappings and records reject mutation, including after a
   cloudpickle round trip.
2. Every TaskID lookup returns the exact Phase 1/2 bindings, lineage, and file
   DataIDs.
3. Injected Python binding mismatches and direct C mount-count mismatches fail
   closed.
4. Every submitted logical task is audited exactly once at actual C
   materialization; all accepted runs report zero mismatches.
5. The 13-task nested/container/file corner workflow passes Controller-on.
6. Controller-on, Phase 2-only, Phase 1-only, and full-off modes pass the
   deterministic normal/shared-input/worker-loss baseline.
7. The final Controller-on local and prescribed-factory suites both pass
   worker-loss recovery.

Machine-readable results:

- `phase0-artifacts/baseline-phase3-controller-local-20260729.json`
- `phase0-artifacts/baseline-phase3-factory-final-20260729.json`

| Runtime / case | Physical tasks | Logical audits | Makespan | Mismatches |
|---|---:|---:|---:|---:|
| local normal | 4 | 4 | 0.318092 s | 0 |
| local repeated shared input | 5 | 5 | 3.424605 s | 0 |
| local one-worker loss | 7, including 3 recovery | 4 | 6.771064 s | 0 |
| factory normal | 4 | 4 | 0.163347 s | 0 |
| factory repeated shared input | 5 | 5 | 0.152987 s | 0 |
| factory one-worker loss | 7, including 3 recovery | 4 | 7.200750 s | 0 |

The final factory archive was rebuilt with `poncho_package_create` from
`/groups/dthain/users/jzhou24/miniconda/envs/datavine` and installed as
`/users/jzhou24/graph_optimization/factories/datavine.tar.gz`. Its SHA-256 is
`13d79ae8bdc1644e5c38ba6bca902d5c20ec783c5fea48ad4d6fb54be5118f61`.
Condor job `4941.0` supplied the worker and was removed after acceptance. Its
first manager attempt hit the suite's 240-second global timeout while the
worker was queued; with the already-started worker and a 600-second bound, the
complete suite passed. This was an infrastructure wait, not a correctness
failure.

## Phase 2 implementation

`shadow_data_graph.py` introduces compact shadow Task, EData, IData, and
consumer nodes. `ShadowDataGraph.from_workflow()` derives them exclusively
from the validated Phase 1 identity snapshot and fails closed if it finds:

- a missing or extra TaskID, EDataID, or IDataID;
- a producer mapping that differs from the Workflow;
- task dependency edges that differ from the Workflow;
- a non-controller initial EData availability or non-unproduced initial IData
  state.

`Workflow.finalize(indexed_data_identity=True, shadow_data_graph=True)`
constructs the graph. The separately controlled VineGraph parameter
`shadow-data-graph` defaults to zero and requires
`indexed-data-identity=1`. The graph is observational only: legacy task
materialization, `vine_file` transport, TaskVine recovery, pruning, and
deletion remain authoritative.

### Phase 2 acceptance

All locally applicable gates pass:

1. The component workflow contains three tasks, four IData nodes, three
   dependency edges, and four producers; all relations match exactly.
2. Direct, nested, positional, keyword, return, and file bindings produce
   deterministic consumer records.
3. Every EData node starts at `controller`; every IData node starts
   `unproduced`.
4. Rebuilding produces byte-identical comparison JSON.
5. An injected Workflow dependency mismatch is rejected rather than reported
   as healthy.
6. The full normal/shared-input/worker-loss suite reports zero mismatches.
7. The 13-task nested/container/file corner suite reports zero mismatches.
8. Phase 2-on, Phase 1-only, and full-off modes all pass the full deterministic
   baseline, including loss of the only worker replica.
9. Clean build/install, compile checks, shell checks, component tests, exact
   workflow oracles, recovery, and cleanup pass.

Machine-readable Phase 2 result:
`phase0-artifacts/baseline-phase2-shadow-20260729.json`.

| Case | Tasks | EData | IData | Dependency edges | Consumer edges | Mismatches |
|---|---:|---:|---:|---:|---:|---:|
| normal | 4 | 7 | 4 | 4 | 12 | 0 |
| repeated shared input | 5 | 7 | 5 | 4 | 21 | 0 |
| one worker loss | 4 | 6 | 4 | 3 | 10 | 0 |

The corresponding makespans were 0.440546 s normal, 3.690010 s repeated
input, and 4.088521 s worker loss. These remain small regression samples, not
performance improvement claims.

## Phase 1 implementation

`data_identity.py` introduces:

- compact positive integer TaskIDs, EDataIDs, and IDataIDs;
- fixed-protocol cloudpickle serialization for callables, positional
  arguments, keyword values, and structured argument templates;
- raw-content identity for declared input files;
- SHA-256 identity over serialization metadata and serialized bytes;
- workflow-global collision buckets which byte-compare before reusing an ID;
- unique lineage-owned IDataIDs for every Python return and declared output
  file slot;
- positional, keyword, return, and file binding records;
- explicit IData references for direct and nested task-output dependencies;
- an invariant validator which rejects unknown IDs, missing task bindings, and
  conflicting IData producers.

`Workflow.finalize(indexed_data_identity=True)` constructs the representation.
The VineGraph parameter `indexed-data-identity` controls it and defaults to
zero. Legacy `task_dict`, task-runner serialization, `vine_file` mounts,
recovery tasks, and executor deletion remain authoritative.

### Phase 1 acceptance

All locally applicable gates pass:

1. Equal but distinct serialized values reuse one EDataID.
2. Forced hash collision buckets keep unequal serialized bytes separate.
3. Serializer, serializer version, protocol, Python version, and Python type
   metadata participate in identity.
4. Every task has compact input/output binding records and no unknown IDs.
5. Every logical return and output-file slot has a stable unique IDataID.
6. Re-finalizing after normal execution or worker-loss recovery preserves all
   TaskIDs, IDataIDs, and bindings.
7. The repeated-input workflow makes 17 registration attempts, stores seven
   unique EData records, and maps all eight uses of its 950,272-byte payload to
   one EDataID.
8. The complete normal/shared-input/worker-loss suite passes with the feature
   enabled and disabled.
9. The 13-task nested/container/file corner-case workflow passes with the
   feature enabled and disabled.
10. Clean build/install, compile checks, shell checks, invariant component
	tests, exact workflow oracles, recovery, and process cleanup pass.

Machine-readable results:

- `phase0-artifacts/baseline-phase1-enabled-20260729.json`
- `phase0-artifacts/baseline-phase1-disabled-20260729.json`

Enabled identity counts:

| Case | Tasks | EData registrations | Unique EData | Deduplicated | IData |
|---|---:|---:|---:|---:|---:|
| normal | 4 | 8 | 7 | 1 | 4 |
| repeated shared input | 5 | 17 | 7 | 10 | 5 |
| one worker loss | 4 | 7 | 6 | 1 | 4 |

The performance samples from the final enabled recovery run were 0.472830 s
normal, 3.615482 s repeated input, and 6.754381 s worker loss. Phase 1 is
shadow bookkeeping, so these values are regression evidence, not a performance
improvement claim.

## Phase 0 acceptance criteria

Phase 0 is accepted locally when all of the following hold:

1. A clean `make clean && make -j8 && make install` succeeds.
2. Normal execution returns its exact oracle with no recovery tasks.
3. Four tasks repeatedly consuming the same 950,272-byte object return exact
   size and SHA-256 oracles and preserve within-task alias identity.
4. A four-node chain loses its only worker replica, releases exactly one
   worker, completes at least one recovery task, and returns its exact oracle.
5. Each case records workflow serialization size/hash, makespan, wall time,
   throughput, manager transfer/recovery counters, and post-cleanup storage.
6. Existing VineGraph corner-case execution still passes end to end.
7. No manager, local worker, output file, or checkpoint file remains after the
   accepted local test.

All seven local criteria pass.

## Phase 0 baseline suite

Source:

- `taskvine/test/vine_graph_phase0_baseline.py`
- `taskvine/test/TR_vine_graph_phase0_baseline.sh`

Machine-readable accepted result:
`phase0-artifacts/baseline-local-20260729.json`.

| Case | Correctness | Workflow pickle | Makespan | Manager bytes sent / received | Recovery |
|---|---:|---:|---:|---:|---:|
| normal, 4 tasks | PASS | 2,101 B | 0.324973 s | 49,959 / 138 B | 0 |
| shared input, 5 tasks | PASS | 952,693 B | 3.833482 s | 990,961 / 446 B | 0 |
| one worker loss, 4 logical tasks | PASS | 2,418 B | 6.765726 s | 90,818 / 191 B | 3 recovery tasks |

The worker-loss case released and removed exactly one worker. It completed
seven physical tasks: four user tasks and three TaskVine recovery tasks.
All explicit output and checkpoint directories contained zero files after
executor cleanup. The manager runtime directory contained nine log/cache files
using 169,617 bytes before manager exit.

Performance values are a small deterministic regression baseline, not a
capacity benchmark. They were collected with one local two-core worker on
2026-07-29.

## Prescribed factory result

The initial factory attempts were invalid because `run_factory.sh` defaulted
to the unrelated `dagvine-env.tar.gz`. That archive used Python 3.11 while the
manager used Python 3.10, producing:

```text
TypeError: code() argument 13 must be str, not int
```

This was a package-selection and environment-refresh error, not a valid
DataVine blocker. The correct package was rebuilt from the active DataVine
environment with:

```bash
poncho_package_create \
  /groups/dthain/users/jzhou24/miniconda/envs/datavine \
  /users/jzhou24/graph_optimization/factories/datavine.tar.gz
```

The rebuilt archive contains Python 3.10.20 and cloudpickle 3.1.2.
`run_factory.sh` now defaults to `datavine.tar.gz`. Condor job `4939.0`
attached to the unique Phase 2 manager and the complete distributed baseline
passed:

| Case | Completed tasks | Makespan | Result |
|---|---:|---:|---:|
| normal | 4 | 0.110303 s | PASS |
| repeated shared input | 5 | 0.129617 s | PASS |
| one worker loss | 7, including 3 recovery | 7.101868 s | PASS |

All Phase 2 shadow comparisons reported zero mismatches. The accepted result
is `phase0-artifacts/baseline-phase2-factory-fixed-20260729.json`; worker
evidence is under `phase0-artifacts/factory-phase2-fixed-worker-logs/`.
Job `4939.0` and its worker were removed during cleanup.

## Current implementation map

### Compute graph

- `Workflow` owns Python task keys, callable indices, arguments, keyword
  arguments, parent/child sets, file declarations, and generated task IDs.
- `VineGraph.build_capi_bridge()` mirrors the Python DAG into a C
  `vine_graph`; C node IDs are mapped back to Python workflow keys.
- `vine_graph_executor` owns readiness, priority, submission, retry queues,
  completion handling, target retrieval, grouping, and progress.

### Task materialization

- C graph nodes initially have no `vine_task`.
- `vine_graph_executor_materialize_node()` creates a library call only when a
  node is submitted, mounts parent result files and declared file handles,
  declares outputs, and adds a small JSON infile containing scheduler keys.
- The whole Python `Workflow` is nevertheless embedded in the task-runner
  library context, so the small per-task infile does not mean task data has
  independent identity.

### Serialization

- The frontend cloudpickles the entire `Workflow` into the library context.
- With Phase 1 enabled, the frontend also serializes and interns each callable,
  positional argument, keyword value, structured binding template, and input
  file independently. This representation is validated but not authoritative.
- Library generation separately cloudpickles functions and context metadata.
- Each worker task loads parent result files, executes the callable, wraps its
  result in `TaskOutputWrapper`, and cloudpickles it into a per-node outfile.
- The legacy workflow pickle still benefits only from its own memo. The Phase 1
  registry separately interns equal serialized EData and assigns DataIDs, but
  those identities do not control movement yet.

### Data movement and storage

- Task dependencies are `vine_file` mounts, not logical data bindings.
- Non-target outputs default to `VINE_TEMP`; targets use managed local files.
- A configured checkpoint fraction changes selected outputs to direct
  shared-filesystem paths. There is no queued/rate-limited persistence state.
- Existing TaskVine replica tables, peer-transfer selection, temp replication,
  manager transfer accounting, and worker caches perform physical movement.
- Executor deletion undeclares files and removes local/shared outputs.

### Recovery

- A temp `vine_file` retains its producer/recovery task.
- Removing the only replica causes TaskVine to submit special recovery tasks.
- `vine_graph_executor` maps recovery completions back to the original graph
  node via `recovery_source_task_id`, resets cut/prune flags, and postprocesses
  the recovered output.
- Recovery therefore works, but currently depends on a separate semantic class
  of recovery task rather than normal invalidation and recomputation.

### Pruning

- Cut propagation deletes an output when every child is anchored or cut and no
  child is mid-recovery.
- `prune-depth` independently releases temporary outputs after descendants
  within the configured depth complete.
- Recovery clears `cut` and `released_by_prune_depth`.
- These rules are tied to completion depth and special recovery state; they do
  not prove preservation of a minimum recoverable data cut.

## Component disposition

| Component | Decision | Phase 0 conclusion |
|---|---|---|
| TaskVine manager/worker protocol and event loop | Reuse | Mature scheduling, worker lifecycle, statistics, and transport foundation. |
| `vine_file`, replica tables, peer transfer, worker cache | Adapt | Reuse physical mechanisms behind Data Controller-owned logical identity and availability. |
| Python `Workflow`, handles, topology validation | Adapt | Useful public graph construction layer; split embedded values from task bindings. |
| C `vine_graph` topology, node IDs, ready scheduling | Adapt | Useful compute-plane base; remove data authority from nodes/tasks over later phases. |
| Lazy task materialization | Reuse and narrow | Preserve submit-time construction, but materialize compact DataID bindings instead of parent file bundles. |
| Whole-workflow cloudpickle library context | Replace | Monolithic, ABI-coupled, and not independently interned or transferable. |
| Per-node cloudpickle result files as logical identity | Replace | Keep serialized bytes as a transport form, but give outputs stable IDataIDs independent of paths/replicas. |
| Special TaskVine recovery tasks | Replace | Move to graph invalidation and normal producer re-execution once Data Controller authority exists. |
| `checkpoint-fraction` direct SharedFS writes | Replace | Introduce staged, admitted, acknowledged persistence. |
| Cut/prune-depth deletion rules | Replace | Retain only as disabled/reference behavior until recovery-aware pruning proves safety. |
| Dask/legacy adaptors | Isolate or remove later | Keep outside the Data Controller core; no old internal data-architecture compatibility requirement. |

## Validation record

- PASS: final clean build and install after Phase 1 source changes.
- PASS: final clean build and install after Phase 2 source changes.
- PASS: final clean build and install after Phase 3 source changes.
- PASS: final clean build and install after Phase 4A source changes.
- PASS: `python -m compileall` for changed VineGraph Python and Phase 0 test code.
- PASS: `TR_vine_graph_data_identity.sh`.
- PASS: `TR_vine_graph_shadow_data_graph.sh`.
- PASS: `TR_vine_graph_data_controller.sh`.
- PASS: `TR_vine_graph_worker_data_agent.sh`.
- PASS: `TR_vine_graph_phase0_baseline.sh`.
- PASS: Phase 0 suite in Phase 2-on, Phase 1-only, and full-off modes.
- PASS: `TR_vine_graph_workflow_examples.sh` with
  Phase 2 enabled (13-task corner-case workflow).
- BLOCKED: Dask adaptor execution because `dask` is not installed.
- PASS: prescribed factory normal/shared-input/worker-loss baseline using the
  rebuilt `datavine.tar.gz`.

## Smallest safe next Phase 4 task

Add independent demand pull for Controller-owned non-file EData only. On a
worker inventory miss, the Worker Data Agent should fetch the Controller's
canonical serialized bytes through one bounded, checksummed source endpoint
instead of relying on the whole-workflow context copy. IData and declared
files remain on legacy mounts, which limits the first movement change to
immutable, already-content-addressed EData.

Acceptance:

1. A worker with an empty inventory pulls each required non-file EDataID once,
   verifies metadata plus SHA-256, and reuses the identical serialized bytes.
2. Repeated shared inputs produce one pull per worker, not one pull per task.
3. Corrupt, truncated, unknown, unavailable, and mismatched responses fail
   closed and never enter inventory.
4. Demand traffic is bounded; no unbounded thread, request, or memory growth is
   introduced.
5. IData and declared-file mounts, recovery, pruning, and deletion remain
   unchanged.
6. Disabling demand pull restores the accepted Phase 4A legacy-source path.
7. Component, corner, five-mode local baseline, worker-loss, clean rebuild,
   rebuilt package, and factory recovery all pass with exact pull telemetry.

This is the smallest real data-movement transition because immutable EData is
already canonical and content-addressed, while volatile IData recovery remains
outside the change.
