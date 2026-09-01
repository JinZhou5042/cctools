# DataVine current state

Updated: 2026-08-30

## Result-driven dynamic workflow pilot, 2026-08-30

- Added an adaptive workflow whose completed results determine one to three
  children; the frontend cannot pre-materialize the final DAG.
- Local 1x4 correctness gate: exact 477 tasks/445 edges, DataVine 1.582 s and
  TaskVine 2.856 s.
- Three alternating remote 2x4 pairs preserved identical graph/result hashes,
  exact physical counts, zero failures/recoveries and equal useful CPU.
- Remote medians reversed the local result: DataVine 5.230 s versus TaskVine
  2.808 s. DataVine performs 1,441 frontend RPCs per 477-task run, making the
  per-completion watch/result/append loop the next explicit bottleneck.
- Full evidence: `DATAVINE_DYNAMIC_ADAPTIVE_20260830.md` and
  `acceptance/dynamic-adaptive-fixed-ab-20260830.json`.

## Source state

- Branch: `benchmark/data-intensive-million-file`
- Base HEAD at the start of this work: `9818f7f42cd0ac83d4e830697a520e6c13b4bba7`
- Worktree: intentionally dirty and uncommitted
- Production architecture: `DATAVINE_PRODUCTION.md`
- Continuation details: `DATAVINE_HANDOFF_20260827.md`

Never reset or clean this worktree as recovery residue. The modified files are
the active implementation until provenance is closed.

## Current architecture

- one workflow process, one C workflow reactor and one Manager owner;
- Scheduler releases children immediately after physical task success;
- Controller independently owns DataID/replica/result state;
- outputs are Worker-local volatile by default and local/peer-first for
  consumers;
- requested outputs stream Worker -> Controller -> local `/tmp` and commit by
  fsync plus atomic rename;
- one metadata RPC thread and 16 fixed Controller data threads;
- DataVine uses worker-first dispatch with a generic optional dense Worker
  availability ring; generic TaskVine retains its baseline scheduler and does
  not allocate the ring;
- no Worker-direct SharedFS result path, size routing or legacy Store result
  fallback.

## Retained evidence

The authoritative current storage summary is
`acceptance/controller-local-tmp-20260827/summary.json`; raw inputs and logs are
checksummed by the adjacent `SHA256SUMS`.

- 128 remote Workers x 4 cores, 200,000 requested empty outputs: 200,000 exact
  completions in 20.633 Controller seconds, 9,693 files/s.
- matched 20,000-output DataVine runs: local `/tmp` 5,005 files/s versus
  SharedFS 196 files/s, 25.55x.
- matched 96-Worker mixed topology: Controller `/tmp` 816.7 files/s versus
  Worker-direct SharedFS 487.7 files/s, 1.675x; all 96 jobs exited zero.
- 4 -> 16 Controller data threads improved 6,218 -> 9,649 files/s; a raw
  64-thread sweep regressed relative to 16.

The 200,000 empty-output run validates durability/file creation throughput. The
96-Worker topology test supplies the remote-stream evidence; do not merge those
claims.

## Maintenance cleanup in this worktree

- removed unused Manager-file origin/restore/publication functions;
- removed an allocated-but-never-populated recovery result cache and its
  meaningless metrics/configuration;
- removed retired result payload replay and Store/RPC fallback APIs;
- fixed the production RPC thread count to one;
- fixed DataVine scheduling to its measured worker-first path;
- consolidated current documentation and removed conflicting phase histories;
- removed generated benchmark pools, run logs, caches and local test binaries.

Build and test results for this cleanup must be added here only after they have
run; compilation alone is not runtime acceptance.

## Post-cleanup acceptance

- warning-clean forced native, Worker and five-tool builds: PASS;
- Python compile and module-boundary checks: PASS;
- full DataVine regression: 18/18 PASS;
- retired result-journal record: rejected fail-closed;
- 10,000-task, one-Worker x 16-core smoke: 3,527 runtime tasks/s, exact
  10,000/10,000 physical completion;
- residue audit: `acceptance/` 740 KiB, `taskvine/test/` 496 KiB, no generated
  caches, benchmark processes or Condor jobs.

Machine-readable details are in `acceptance/cleanup-audit-20260827.json`.

## Rigorous validation and small-stream fix

The second audit renamed the versioned production data-plane test to
`datavine_production_data_plane`, removed the last Runtime-v2 labels, simplified
the worker-first loop, removed the `datavine-v2` cache prefix, gave the two DVP1
ticket layouts semantic names, and made the native benchmark load this
checkout's standard-library workflow client directly.

Rigorous evidence is in `acceptance/rigorous-validation-20260827/`:

- 1M DataID lifecycle completed in 0.958 seconds; the 2M-replica disconnect
  path removed 3.96M replicas/s;
- single-record Controller TCP publication saturates near 100k records/s with
  process clients; the earlier 19.85k observation was Python-GIL limited;
- 3x local 50k no-output tasks averaged 4,067.7 runtime tasks/s (CV 1.28%);
- one admitted Condor 16-Worker x 4-core run completed 50k tasks at 19,277.7
  service tasks/s;
- `/tmp` exceeded SharedFS by 18.44x for 4 KiB fsync+rename writes, 8.00x for
  1 MiB direct writes, and 3.59x for 1 MiB direct reads;
- 4 KiB requested streams exposed a Nagle/delayed-ACK stall. Tuning the
  persistent transfer socket for interactive latency raised the three-run mean
  from the 24.24 files/s pre-fix observation to 1,034.2 files/s (42.6x, CV
  0.51%);
- two post-fix 1 GiB runs averaged 150.2 MiB/s in the Controller service
  window, matching the 150.4 MiB/s pre-fix run;
- the final forced build was warning-clean and the full regression passed
  18/18 with a freshly compiled Go client.

The bounded Condor repeat saw 16 jobs remain idle for 300 seconds; it failed the
inventory gate and cleaned the queue. It is recorded as an admission limitation
and excluded from throughput statistics. No benchmark process or Condor job
remained afterward.

## Third-pass RPC correction

The original Controller RPC benchmark used Python threads. That driver was
GIL-limited near 20k one-record requests/s and did not measure the Controller's
actual ceiling. `benchmark_controller_rpc.py` now defaults to independent
process clients.

Two narrow RPC-loop changes preserve the single owner and one-record protocol:

- small responses are sent immediately from the read handler; `EPOLLOUT` is
  armed only on real socket backpressure;
- idle and terminal-waiter maintenance scans run at most once per the existing
  10 ms event-loop resolution instead of after every busy epoll turn.

Three process-driven repetitions per topology found:

- 16 connections: 94.2k publishes/s and 81.4k resolves/s;
- 64 connections: 101.1k publishes/s and 84.0k resolves/s;
- 128 connections: 98.7k publishes/s and 87.3k resolves/s, with publication CV
  increasing to 10.4%.

The balanced point is about 64 active connections. At that point Controller CPU
averages 6.87 us/publication and 7.94 us/resolve. Relative to the original RPC
loop, service CPU per record fell 22-28% depending on topology. Three exact
2,000 x 4 KiB requested-output runs retained 1,199.0 service files/s versus
1,203.8 previously (-0.4%).

Regression also exposed a `/proc` process-exit race in the Notebook test; it now
handles the full `OSError` family. Focused Notebook/Shell gates and the final
full regression passed. Evidence is in
`acceptance/rigorous-validation-20260827/third-pass/`.

## Fourth-pass fixed-core data-path isolation

The requested-output path held each persistent Worker connection through local
`fsync` and atomic rename even after the complete framed response was consumed.
The connection is now released at that protocol-safe boundary. Incomplete
transport still closes the connection; local persistence remains fail-closed
and retries without poisoning a healthy stream.

At a fixed total of 16 Worker cores, three exact 8,000-file repetitions per
topology measured:

- 1x16 requested 4 KiB: 1,124.9 -> 1,169.3 service files/s (+3.95%);
- 4x4: 3,202.1 -> 3,208.6 files/s (+0.20%);
- 16x1: 3,386.9 -> 3,391.4 files/s (+0.14%);
- empty outputs: 1,176.5, 3,441.8 and 3,657.7 files/s respectively.

The 4 KiB payload costs only 7-8% at multiple Workers, and empty throughput
gains only 6.3% from four to sixteen Workers. Combined with the 100k/s metadata
RPC result, the remaining approximately 3.6k/s local ceiling belongs to
per-task Worker completion/scheduling rather than Controller metadata, local
disk bandwidth, or payload bytes. A 16x1 run persisted an exact 1 GiB at 656.0
MiB/s. Forced builds passed and the final regression remained 18/18 PASS.
Evidence is in `acceptance/rigorous-validation-20260827/fourth-pass/`.

## Fifth-pass completion/dispatch optimization

Aggregate metrics isolated the remaining independent-task ceiling to the
Manager completion-to-refill interval, not DataVine's logical transition or
Controller data plane. DataVine now sets the existing `prefer-dispatch` tuning:
when READY work already exists, the Manager refills a newly free Worker slot
before returning the retrieved completion to the workflow reactor. It adds no
batch, thread, queue, owner, or alternate scheduler.

Interleaved 50k old/new A/B runs measured 15,035.4 -> 16,538.8 service tasks/s
at 4x4 (+10.00%) and 13,862.2 -> 15,879.2 at 16x1 (+14.55%). A direct Worker
pointer poll-table candidate reduced status CPU but regressed end-to-end
throughput by 0.96%; it was fully reverted.

Layered FunctionCall testing found that TaskVine kept function return bytes in
Worker memory while the DataVine Agent committed the declared sandbox file.
The Worker now materializes those bytes before commit. The exact-byte focused
test and a 20,000-task, 39,488-edge layered DAG pass. Final requested-output
guards reached 3,250.4 files/s at 4x4, 3,462.8 files/s at 16x1, and 663.3 MiB/s
for an exact 1 GiB stream. Forced builds were warning-clean and the final full
regression passed 18/18. Evidence is in
`acceptance/rigorous-validation-20260827/fifth-pass/`.

## Open gates

- Git commit/provenance closure;
- package rebuild/promotion, only if explicitly requested;
- cross-host durability, if later required.

## Remote requested-output fan-in, 2026-08-28

Remote Condor Workers generated exactly 4 GiB per requested-output run while
the Controller persisted every output to local `/tmp`. At the current 16 data
threads, the service sustained 542.8 MiB/s for 1-MiB files, 546.4 MiB/s for
16-MiB files and 511.4 MiB/s for 64-MiB files. Scaling fan-in from 16 to 32 and
64 Workers regressed throughput by 9-27%, confirming that additional transfer
concurrency is counterproductive after the 16-Worker knee.

The Controller received about 4.19 GiB per 4-GiB run but used only 3-5 CPU
cores and less than half of the active 10-Gbit link. Combined with the retained
706-770 MiB/s raw durable-write ceiling, this isolates the limit to integrated
pull, copy, synchronization and per-file durable write. Use 0.48 GiB/s as the
conservative capacity model until component timings are added.

The fair Worker-local control used explicit 1-MiB `payload.bin` outputs: 4,096
tasks completed with zero durable files, 1.44 MiB Controller RX and 0.57 MiB
Controller writes. A requested run of the same command produced all 4,096
files and moved the full 4 GiB. An earlier stdout control exposed a separate
OPEN issue: generic TaskVine retrieves unrequested/no-consumer command stdout,
causing avoidable network traffic after DataVine declines retention.

Details are in `CONTROLLER_REMOTE_FANIN_20260828.md`; compact evidence is in
`acceptance/controller-remote-fanin-20260828.json`.

A memory-only remote TCP follow-up measured the actual inbound network ceiling
without DataVine or storage. One stream reached 9.4008 Gbit/s; three 16-stream,
4-GiB repetitions averaged 9.4120 Gbit/s (1,122 MiB/s). The best integrated
DataVine run therefore consumes 51.1% of measured usable network capacity and
has at most about 2.05x network-only payload headroom. This confirms local
durability is the current limit and 10 GbE becomes the next ceiling after a
faster disk roughly doubles throughput. Evidence is
`acceptance/controller-network-ceiling-20260828.json`.

## Controller persistence-stage diagnosis, 2026-08-28

Opt-in per-thread counters now measure the existing requested-output path
without changing its queue, connection, 16-thread, `fsync`, rename, verification
or commit semantics. Cache-line separation was required: a rejected shared
counter layout caused false sharing and reduced the diagnostic run to about
355 MiB/s. The accepted layout measured 548.0 MiB/s versus 547.0 MiB/s with
diagnostics disabled for the matched 1-MiB run.

The accepted 4-GiB diagnostic accounts for 99.8% of the service window. `fsync`
is 50.6% of active work, the post-write SHA-256 reread 21.8%, Worker connection
wait 11.7%, and combined socket-read/file-write streaming 10.9%. Commit itself
is 0.07%; enqueue blocking is negligible despite a 3,501-job backlog. Two
16-MiB probes put `fsync` near 73.3%, but their absolute rate was probe-sensitive,
so production throughput continues to use the adjacent probe-off result.

Follow-up 16-thread, 4-GiB local mixed comparisons screened incremental SHA-256,
`fdatasync`, `posix_fallocate` and `sync_file_range`. None produced a stable
improvement: streaming SHA was neutral at 1 MiB and had a median 2.5% regression
at 16 MiB; the other candidates either matched or increased active time. The
production path therefore remains unchanged.

Per-file `fsync` is required by the current fail-closed contract because delayed
local `ENOSPC` or `EIO` may first be reported there and must be observed before
metadata commit. It is not a Controller-host crash guarantee: `/tmp` remains
host-lifetime storage and the containing directory is not fsynced. Removing or
deferring `fsync` requires an explicit policy change. Evidence is
`acceptance/controller-persistence-diagnostic-20260828.json` with interpretation
in `CONTROLLER_PERSISTENCE_DIAGNOSTIC_20260828.md`.

The formerly OPEN deterministic persistence-fault gate is now PASS. A
Linux-only, test-built `LD_PRELOAD` shim targets only Controller private `.part`
file `fsync` calls and injects `ENOSPC` or `EIO`; production runtime code is
unchanged. Both cases exhaust eight internal attempts without exposing a
descriptor, final file or terminal workflow, and shutdown leaves no temporary
file. Restart with the fault removed recomputes the producer, records one commit
and survives a second replay without a Worker. The focused gate passed five
times, and production data-plane and module-boundary tests pass. The historical
15/19 aggregate result was superseded on 2026-09-01 by the 21/21 process-isolated
strict-singleton regression. Evidence is
`acceptance/controller-persistence-faults-20260828.json`.

## Sixth-pass native hot-path optimization

The post-slimming runtime now uses a generic optional dense Worker availability
ring. Worker slots are integer indexed and generation checked, and a stale
ready-ring entry cannot resurrect a disconnected Worker. DataVine enables the
ring explicitly; ordinary TaskVine still allocates nothing for it and retains
its task-first default. At 64 Workers x 2 cores, paired 50k runs improved mean
service throughput from 13,902.9 to 14,493.7 tasks/s (+4.25%) and E2E throughput
from 10,078.2 to 10,378.4 (+2.98%). At 8 Workers it was neutral, establishing
that this is a Worker-cardinality optimization rather than a universal claim.

The Runtime physical-ID-to-logical-attempt `itable` is now a direct growing
array. Four-run completion processing fell 3.56%; total throughput remained
inside run noise. A slab allocator candidate passed correctness tests but
reduced service throughput by 1.6%, so it was removed.

Perf sampling then found textual parsing, not the remaining scheduler tables,
as the larger native hot spot. Direct prefix/integer parsing preserves the
TaskVine line protocol while reducing Worker `vfscanf` samples from 4.23% to
1.77%. Four-run 8x16 means improved 12,493.5 -> 12,953.4 E2E tasks/s (+3.68%)
and 18,436.7 -> 19,169.4 service tasks/s (+3.97%); Manager status-processing
time fell 4.90%. A final exact 50k run reached 13,419.6 E2E and 19,666.9 service
tasks/s with 50,000/50,000 physical completions.

Two acceptance failures produced useful hardening. Explicit Make dependencies
now prevent a stale `vine_worker_info.o` from mixing old and new Worker layouts
in incremental builds. The transfer server also receives a parent-death signal
and cannot remain as an orphan after an abruptly killed Worker.

Clean native build, module boundaries, scheduler, Worker-loss/restart,
Shell dynamic/recovery, production data plane, generic TaskVine single and
multicore, and transfer-server parent-death gates pass. The historical 14/18
result identified four harnesses that submitted a second workflow ID. They are
now process-isolated and the current aggregate is 21/21; no compatibility switch
was added. Detailed optimization evidence remains in
`acceptance/native-fastpath-20260827.json`.

## Comprehensive Controller characterization

A fresh current-tree campaign isolated Controller metadata, one-record TCP RPC,
requested-output persistence and Worker-local output policy. At ten million
DataIDs with one replica and one waiter, the full in-memory lifecycle completed
in 13.48 seconds at 2.28 GiB peak RSS. Expect and publish remained above
8.4M operations/s; random resolve was the first cache-sensitive phase at
1.97M/s. At one million DataIDs, increasing from one to four replicas reduced
random resolve from 2.82M/s to 0.85M/s.

Unbatched single-record RPC reached 111.5k publications/s and 85.4k resolves/s.
Sixteen connections gave the best latency-throughput balance: 87.1k publish/s
and 81.9k resolve/s with sub-314 us p99. At 128 connections throughput improved
only modestly while publish p99 rose to 2.18 ms.

Controller-local `/tmp` persistence peaked at 9,667 empty files/s and 4,848
4-KiB files/s near 16 Workers. Exact 64-MiB outputs reached 411 MiB/s against a
fresh 706-770 MiB/s direct+fsync filesystem ceiling. The same-host topology
writes each byte once at the Worker and once at the Controller, explaining the
approximately half-device integrated ceiling. Unrequested 64-MiB outputs
reached 1.21 GiB/s with zero durable files, confirming that Worker-local data
avoids the second write.

Late consumers, late/inflight request promotion, volatile-loss replay, Worker
loss, restart, checkpoint resume, retries, dedup and replica invariants pass.
The campaign found an observability gap: agent persistence does not increment
the existing `publication_*` counters. Deterministic ENOSPC/EIO injection also
remains OPEN. Evidence is in `acceptance/controller-comprehensive-20260827.json`
and `CONTROLLER_PERFORMANCE_20260827.md`.

## Controller metadata slimming

The Controller now implements the production single-workflow boundary rather
than carrying multi-workflow dictionaries. Workflow identity is bound by
journal replay or first submission; another ID fails closed. DataID result
metadata and loss de-duplication share an 8-byte tagged paged catalog. Worker
endpoints directly own stable pull connections. The Controller contains no
generic hash table or `itable`, and persistence admission no longer constructs
a transient duplicate table.

Replica state was compacted without removing failure gates: Data records are
56 bytes, replica records 40, waiter records 48 and Worker sessions 16. A
publication job is one 336-byte allocation instead of roughly 960 bytes plus
paths. The exact retained 1M-DataID/128-Worker lifecycle comparison measured
0.9583 -> 0.8521 seconds and 214,816 -> 189,592 KiB RSS. Evidence and exact
phase rates are in `acceptance/controller-slimming-20260827.json`.

The strict singleton binary passes replica and production data-plane gates.
The old full-suite harness is no longer a valid aggregate gate because six
tests reuse one service for several workflow IDs. Those tests need process
isolation; no multi-workflow compatibility switch was added.
2026-08-30 fixed-topology A/B: three alternating 2-Worker x 4-core Condor
pairs passed the 1,024-task full-size data-intensive contract. TaskVine times
were 112.03/229.26/160.41 s; DataVine times were 10.03/11.95/9.97 s, for a
16.09x median paired speedup and 99.938% median generic-Manager byte reduction.
Useful Worker execution totals and output hashes matched, attributing the wall
gap to data/sandbox movement rather than skipped work. Evidence is
`acceptance/data-intensive-fixed-ab-20260830.json`; exact dirty-source
provenance is checksummed under the adjacent `/groups` benchmark tree. This is
a pilot, not the 128x16 million-task production claim. Generator NFS block
accounting and TaskVine user-task identity gates were corrected and covered by
the workload regression. Focused gates passed; aggregate remains expected
15/19 with only four retired strict-singleton-incompatible harnesses failing.

2026-08-31 dynamic-control root fix: corrected the earlier bottleneck
attribution. The 477-task adaptive workflow spent 3.41-4.11 seconds installing
478 tiny SharedFS objects, not primarily in its 1,441 frontend RPCs. DVP2 now
carries <=64-KiB Python invocation control records directly in TaskVine's
`function_input`; DVP1 remains the large/old-runtime fallback. Object puts fell
478 -> 1 and two new 2x4 Condor DataVine runs took 1.2236/1.2246 seconds versus
the old 5.2302-second median. A durable Controller-admitted result stream cuts
matched RPCs from 1,654 to 483, resumes after disconnect/restart from data
journal order, and does not couple Scheduler completion to Controller
admission. A 948-task local 1x16 run reached 573.9 tasks/s versus TaskVine's
275.5. Exact evidence is `acceptance/dynamic-control-root-fix-20260831.json`;
design and limitations are in `DATAVINE_DYNAMIC_CONTROL_20260831.md`.
An additional 10,000-requested-empty-result 1x16 gate delivered every sequence
exactly once at 1,175.6 results/s and terminal completion with one stream
request and five total client RPCs.

2026-08-31 iData resilience implementation: added workflow policy
`idata_backup=controller-background`; the initial experiment kept
`worker-local` as the default, and the later production decision below changed
the default to Controller background backup.
Each live retained Worker output queues one DataID in an 8-byte background
backlog; at most four of the 16 Controller data threads perform background
copies, while requested-result jobs and Worker input resolution take priority.
Copies use the existing persistent Worker stream, SHA-256, `fsync`, private
temporary files and atomic rename, without delaying task completion or child
dispatch. If the last Worker replica disappears, a new Worker fetches the
Controller copy through signed 1-MiB range requests and republishes it as an
ordinary Worker replica. A focused test killed the only Worker, recovered a
1-MiB+17-byte iData on a fresh Worker, and observed zero producer replay.

Three alternating local 1x16 runs per mode, each with 10,000 `/bin/true` tasks
and one empty live iData per task, measured median logical throughput of
1,163.3 tasks/s for `worker-local` and 1,133.8 tasks/s for background backup, a
2.54% reduction. Background admission completed all 10,000 files at median
1,128.4 files/s and drained only 42 ms after logical completion. This is a
single-host latency/throughput acceptance result, not a multi-host production
capacity claim. Exact values are in
`acceptance/idata-background-backup-20260831.json`.
The final ring-queue/per-Worker admission implementation also passed 10,000
files at 1x16 and 4x4; the 4-Worker run reached 3,351.1 logical tasks/s and
3,318.5 fsynced backup files/s, then logical GC removed every unrequested copy.

2026-08-31 production-default and source-routing decision: background iData
backup is now the default when `policy.idata_backup` is omitted by raw IR or
the Python builder. `worker-local` remains an explicit opt-out. Backup is
additive and asynchronous: the primary file stays on Worker local disk, the
Controller receives a safety copy in `/tmp`, and resolution remains
local-first, peer-first, then Controller fallback.

A route-isolated Condor A/B waited for every Controller backup before adding
consumers. Peer runs kept 32 low-memory producer Workers connected while 64
high-memory consumer Workers were forced onto fresh Workers; Controller runs
removed every producer Worker before starting the same consumers. Each run
read 10,000 distinct 1-MiB files. Across reverse-order repetitions, peer
delivery had median 1,779 MiB/s versus 832 MiB/s through the Controller, a
2.14x speedup; the slowest peer run still beat the fastest Controller run by
32%. Controller routing sent about 10.98 GB through the Frontend per run,
whereas peer routing left only 10--11 MB of control traffic. Exact evidence is
`acceptance/peer-vs-controller-20260831.json`. The production decision is to
keep peer transfer as the normal distribution path and use the Controller
copy for resilience and fallback.

2026-08-31 unified dynamic data lifecycle: reproduced a Controller-restart
failure where a requested result remained frontend-readable but a newly
appended Worker task could not resolve the same DataID. Journal replay had
restored the payload catalog, while the fresh dense replica table lacked its
generation/size/digest identity and returned PENDING until the Worker was
forsaken. Added one lazy `restore_persisted` transition at Worker resolve. It
hydrates one immutable catalog identity in O(1), rejects stale generations and
preserves peer-first routing; there is no restart scan and no Scheduler data
state.

Focused gates passed `7 -> restart -> 49 -> 50` with zero producer replay,
volatile late-consumer replay, background-backup Worker-loss fallback, result
stream restart, lifecycle, persistence faults and production boundaries. A
fresh local forced-peer dynamic append completed 1,000 x 64-KiB consumers in
3.314 seconds and seal GC removed every Controller file. A fresh adaptive run
discovered the exact 477-task/445-edge graph in 1.564 seconds at 304.9 tasks/s,
with 477 physical submissions/completions and zero recovery. Evidence is
`acceptance/dynamic-data-management-20260831.json`.
