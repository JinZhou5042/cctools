# DataVine handoff: production cleanup

Updated: 2026-09-01

## Production-baseline closure

The source/test boundary is commit `58bc4288b`; the consolidated production
baseline is `b481c3768` on branch `production-baseline`. A clean rebuild and
the strict-singleton suite pass 21/21. Current performance and package checks
are recorded in `acceptance/production-baseline-20260901.json`. The verified
candidate package is
`/users/jzhou24/graph_optimization/factories/datavine.production-baseline-b481c3768.tar.gz`;
the canonical `datavine.tar.gz` was not overwritten.

This is the continuation point for the uncommitted DataVine implementation.
Read `DATAVINE_PRODUCTION.md` and
`acceptance/cleanup-audit-20260827.json` before editing.

The current dynamic-control continuation is
`DATAVINE_DYNAMIC_CONTROL_20260831.md`. Tiny per-task Python invocation records
now use the DVP2 inline task-frame path; DVP1 remains the compatible object
fallback. Live requested control results use the Controller-admitted durable
sequence stream, while Scheduler completion remains fully decoupled.

## Frozen production boundary

- One `datavine_workflow` process owns one workflow.
- Scheduler owns task readiness, dispatch, completion and retry. Physical
  success releases children immediately; it never waits for data admission.
- Manager owns Worker connections and physical task transport only. Its
  DataVine frame is opaque.
- Controller owns DataIDs, replicas, requested results, data loss and GC.
- Outputs remain Worker-local and volatile unless needed by a consumer or
  explicitly requested.
- Consumers resolve local or peer replicas. Payload bytes do not pass through
  Manager.
- Every requested output follows one path: Worker stream -> one of 16 fixed
  Controller data threads -> Controller-local `/tmp` -> fsync -> atomic rename.
- There is no Worker-direct SharedFS persistence, size routing, result payload
  replay through Workflow Store, per-Worker thread pool or multi-workflow lane.
- DataVine selects generic worker-first dispatch plus the optional dense
  generation-safe Worker availability ring. Generic TaskVine allocates no ring
  and retains its normal task-first scheduler by default.
- DataVine enables the Manager's `prefer-dispatch` policy. It refills an
  already-free slot from existing READY work before returning a completion;
  dependency release remains Scheduler-owned and unbatched.

`/tmp` durability survives Worker loss but not Controller-host loss, reboot or
external cleanup. Cross-host durability is a separate future feature.

## Cleanup delivered

- Removed retired Store result records, replay paths and RPC fallbacks.
- Removed unused Manager-file restore/publication functions and an empty
  recovery result cache.
- Removed Worker/Python direct-persistence queues, notification RPCs and the
  extra Python persistence subprocess.
- Reduced the Python output ticket policy from 10 bytes to one retention byte
  per output.
- Fixed metadata RPC service concurrency at one and Controller data workers at
  16.
- Fixed DataVine to the measured worker-first scheduler while preserving the
  TaskVine baseline.
- Made worker-first dispatch safe when a send removes a Worker by iterating
  stable Worker keys and re-resolving the Worker before use.
- Fixed local Factory benchmarks to execute the requested source-tree Worker,
  not a stale `PATH` installation.
- Consolidated current documentation and removed superseded plans, reports,
  generated run pools, logs, caches and local test binaries.
- Renamed the last versioned production data-plane test, removed remaining
  Runtime-v2/cache-key labels, gave DVP1 ticket layouts semantic names, and
  simplified the worker-first dispatch loop.
- Made the native benchmark self-contained against this checkout and hardened
  `/proc` sampling against normal process-exit races.
- Fixed a measured 40 ms small-stream stall by enabling interactive socket
  tuning on both ends of persistent Worker/Controller transfers.
- Released the per-Worker pull connection after a complete framed body, before
  local `fsync` and rename; network failure still resets the connection and
  local persistence remains fail-closed.
- Enabled DataVine-only `prefer-dispatch` after interleaved 50k A/B showed
  +10.00% at 4x4 and +14.55% at 16x1; a direct poll-pointer candidate regressed
  0.96% and was reverted.
- Materialized in-memory native FunctionCall return bytes into the sandbox
  output before Data Agent commit; exact-byte and 20k layered-DAG tests pass.
- Replaced all eight persistent/transient Controller hash/itable objects with
  one workflow owner, an 8-byte tagged paged DataID catalog, Worker slots and
  existing integer arenas. The Controller hot path now uses zero generic
  hash/itable objects.
- Reduced replica Data records from 64 to 56 bytes, replica records from 56 to
  40, waiter records from 56 to 48 and Worker sessions from 24 to 16 without
  changing stale-session, last-replica or GC semantics.
- Collapsed each requested-output persistence job from three copied output
  records plus unused metrics to one 336-byte job, and removed the per-commit
  duplicate hash. Admission's `persistence_queued` bit is the uniqueness
  invariant.
- Moved persistent transfer ownership directly into each Worker slot. This
  removed the connection hash and an existing double-free error path; Worker
  reconnect resets the stable connection under its own mutex.
- Precomputed the single workflow result directory and made replay bind the
  workflow identity. A second workflow ID is rejected fail-closed.
- Replaced Runtime physical-attempt hashing with a direct integer array.
- Replaced hot Worker task-description and Manager completion `sscanf` parsing
  with direct integer parsing without changing protocol bytes.
- Added explicit Worker-layout build dependencies after a clean-build gate
  exposed a stale-object mixed-ABI failure.
- Made the transfer server die with an abruptly killed Worker, closing the
  orphan-process path observed during a failed regression.

The cleanup removed about 1.10 million tracked lines. `acceptance/` fell from
about 645 MiB to under 1 MiB, and `taskvine/test/` from 161 MiB to under 1 MiB.
Git history remains the archive for deleted campaign output.

## Acceptance

- Warning-clean forced builds: native DataVine library, Worker and five tools.
- Python compile/static checks: PASS.
- Full DataVine regression: 18/18 PASS in
  `/groups/dthain/users/jzhou24/miniconda/envs/datavine` with
  `PYTHONNOUSERSITE=1`.
- Retired result-journal opcode 107: valid record rejected fail-closed.
- Post-cleanup scheduler smoke: 10,000/10,000 tasks on one 16-core local Worker;
  3,527 runtime tasks/s and 4,560 service tasks/s.
- Final process audit: no DataVine service, Factory or Worker processes and no
  Condor jobs.

The rigorous second-round report is
`acceptance/rigorous-validation-20260827/report.md`. Highlights:

- three local 50k no-output runs averaged 4,067.7 runtime tasks/s;
- one exact Condor 16x4 run reached 19,277.7 service tasks/s;
- the Controller lifecycle handled 1M DataIDs in 0.958 seconds, while
  single-record TCP publication topped out near 19.85k/s;
- `/tmp` beat SharedFS 18.44x on 4 KiB fsync+rename and 8.00x on 1 MiB direct
  writes;
- 4 KiB end-to-end requested-output throughput improved 42.6x after the socket
  fix, from 24.24 to a three-run mean of 1,034.2 files/s;
- two post-fix 1 GiB streams averaged 150.2 MiB/s in the Controller service
  window, with exact digest/file/byte validation;
- final forced builds were warning-clean and the full regression passed 18/18
  using a freshly compiled Go client.

Third-pass RPC evidence is under
`acceptance/rigorous-validation-20260827/third-pass/`. It corrects the earlier
20k/s interpretation: Python threads were GIL-limited. Independent process
clients measured a three-run mean of 101.1k one-record publications/s and
84.0k resolves/s at 64 connections. Immediate nonblocking response writes and
10 ms-bounded maintenance scans reduced service CPU per record by 22-28%
without batching or adding an owner. The 128-connection sweep added variance
without improving publication throughput. The final full regression remains
18/18 PASS.

Fourth-pass fixed-core evidence is under
`acceptance/rigorous-validation-20260827/fourth-pass/`. Moving local durability
outside the connection critical section improved the three-run 1x16 mean from
1,124.9 to 1,169.3 requested 4 KiB files/s (+3.95%); 4x4 and 16x1 were
unchanged. Empty-output scaling plateaus at 3,657.7 files/s and 4 KiB payloads
cost only 7-8% with multiple Workers, isolating the remaining local ceiling to
per-task completion/scheduling rather than Controller metadata or byte
bandwidth. A 16x1 exact 1 GiB guard reached 656.0 MiB/s, and the full regression
passed 18/18.

Fifth-pass completion/dispatch evidence is under
`acceptance/rigorous-validation-20260827/fifth-pass/`. DataVine completion
bookkeeping measured only about 6 microseconds/task; completion-to-refill delay
was the useful target. Interleaved 50k A/B runs reached 16,538.8 tasks/s at 4x4
and 15,879.2 tasks/s at 16x1, improvements of 10.00% and 14.55%. Final 4 KiB
guards reached 3,250.4 and 3,462.8 files/s, the exact 1 GiB guard reached
663.3 MiB/s, and the final full regression passed 18/18.

Controller-slimming evidence is
`acceptance/controller-slimming-20260827.json`. Against the retained exact
1M-DataID/128-Worker lifecycle run, total time fell from 0.9583 to 0.8521
seconds (1.125x), RSS from 214,816 to 189,592 KiB (-11.7%), expect rose from
7.49M to 9.74M operations/s, and publish from 8.56M to 10.69M operations/s.
Replica and production data-plane gates pass. The strict-singleton service
rejects a second workflow ID; all legacy scenarios were subsequently isolated
as one process and journal per workflow without a compatibility mode.

Sixth-pass native evidence is `acceptance/native-fastpath-20260827.json`.
At 64 Workers x 2 cores the dense ring improved service throughput by 4.25%
and E2E throughput by 2.98%; it was neutral at 8 Workers. Direct protocol
parsing improved four-run 8x16 means by 3.97% service and 3.68% E2E, and a final
exact 50k run reached 19,666.9 service tasks/s. Clean build, module boundaries,
failure/restart, production data plane and generic TaskVine smoke gates pass.
On 2026-09-01 the aggregate runner passed 21/21 after the notebook, execution,
service and scientific harnesses were process-isolated. No test was skipped.

Verify that evidence with:

```sh
cd acceptance/rigorous-validation-20260827
sha256sum -c SHA256SUMS
```

Current storage evidence remains
`acceptance/controller-local-tmp-20260827/summary.json`:

- 128 remote Workers x 4 cores, 200,000 requested empty outputs: 9,693 files/s.
- matched 20,000-output runs: `/tmp` 5,005 files/s versus SharedFS 196 files/s.
- topology-matched mixed payloads: Controller `/tmp` 816.7 files/s versus
  Worker-direct SharedFS 487.7 files/s.
- 16 Controller data threads outperformed 4, while raw 64-thread writes
  regressed relative to 16.

Verify retained storage inputs with:

```sh
cd acceptance/controller-local-tmp-20260827
sha256sum -c SHA256SUMS
```

## Repository state

- Branch: `benchmark/data-intensive-million-file`
- Base HEAD: `9818f7f42cd0ac83d4e830697a520e6c13b4bba7`
- Worktree: intentionally dirty and uncommitted
- Machine-readable audit: `acceptance/cleanup-audit-20260827.json`

Do not reset or clean this worktree. The remaining open gates are Git
commit/provenance, an explicitly requested package rebuild, and any future
cross-host durability design. No benchmark is active.

## Comprehensive Controller campaign

Fresh evidence is `acceptance/controller-comprehensive-20260827.json`, with
interpretation in `CONTROLLER_PERFORMANCE_20260827.md`. Ten million DataIDs
complete the direct C lifecycle in 13.48 seconds at 2.28 GiB peak RSS.
Unbatched one-record RPC peaks at 111.5k publish/s and 85.4k resolve/s; 16
connections provide the best tail-latency balance. `/tmp` persistence peaks
near 16 Workers for small files and reaches 411 MiB/s for 64-MiB files, versus
a fresh 706-770 MiB/s raw durable-write ceiling. Worker-local unrequested
outputs create zero durable files.

Do not increase the 16 Controller data pthreads. The next narrow work is to add
agent pull/fsync/queue profiling because current `publication_*` metrics omit
that path, then add deterministic ENOSPC/EIO fault injection. Multi-replica
resolve is worth optimizing only if real workloads retain several replicas per
DataID.

## Remote fan-in campaign

The 2026-08-28 remote 4-GiB sweeps establish the requested-output byte ceiling.
At 16 Workers, 1-MiB and 16-MiB outputs reached 542.8 and 546.4 MiB/s; 64-MiB
outputs reached 511.4 MiB/s. Increasing fan-in to 32 or 64 Workers reduced
throughput by 9-27% while increasing file descriptors. The 10-Gbit NIC remained
below half utilization and the Controller used only about 3-5 cores, so the
narrow bottleneck is the pull plus durable local-write path. Keep 16 data
threads and plan around a conservative 0.48 GiB/s sustained drain rate.

A same-command explicit-file A/B generated 4 GiB at 16 Workers. Worker-local
retention produced zero durable files, only 1.44 MiB of Controller RX and 0.57
MiB of Controller writes; requesting every output produced 4,096 durable files,
4,287 MiB RX and 4,097 MiB writes. This proves unrequested declared files stay
off the Controller data path.

The campaign also found one OPEN policy issue: unrequested, consumer-free
command stdout is still retrieved by generic TaskVine after the DataVine Agent
declines retention. The fair explicit-file control does not have this traffic.
Fix this only in the DataVine opt-in path; do not alter ordinary TaskVine stdout
semantics. Evidence and interpretation are in
`acceptance/controller-remote-fanin-20260828.json` and
`CONTROLLER_REMOTE_FANIN_20260828.md`.

The follow-up diagnostic adds opt-in, cache-line-separated per-thread timing
without changing the production path. For the 1-MiB/4-GiB run, accounted work
divided by 16 was 7.460 seconds against a 7.475-second service window (99.8%).
`fsync` consumed 50.6% of active work, the durable-file SHA-256 reread 21.8%,
Worker connection wait 11.7%, and combined socket-read/file-write streaming
10.9%. Queue admission and commit were negligible. Two 16-MiB diagnostic runs
put `fsync` at about 73.3%, although their absolute rates were probe-sensitive;
production throughput remains the probe-off measurement. The next clean
candidate was incremental SHA-256 during streaming, but subsequent randomized
local comparisons found no stable gain: 1-MiB active time was effectively
neutral and the median 16-MiB result regressed 2.5%. Preallocation,
`fdatasync`, and `sync_file_range` writeback likewise produced no measured
improvement. No production path changed.

Keep per-file `fsync` while requested outputs are fail-closed against delayed
local `ENOSPC`/`EIO`: those errors can appear after `write` and must be observed
before metadata commit. This is local runtime durability only; `/tmp` and the
non-fsynced directory entry do not promise Controller-host crash or cross-host
durability. A substantial software speedup now requires an explicit semantic
change, while the semantics-preserving route is faster or sharded
Controller-local storage. Evidence and the decision boundary are in
`acceptance/controller-persistence-diagnostic-20260828.json` and
`CONTROLLER_PERSISTENCE_DIAGNOSTIC_20260828.md`.

The deterministic `fsync` failure gate is now complete. The test-only Linux
preload shim injects `ENOSPC` and `EIO` only into private result `.part` files;
there is no production fault branch. Five focused repetitions prove fail-closed
metadata, cleanup, same-source internal retries, restart recomputation, a single
successful commit and replay without a Worker. The new gate passes in the
aggregate runner, which is now the expected 15/19 with only the same four
retired multi-workflow harness failures. Evidence is
`acceptance/controller-persistence-faults-20260828.json`.

Pure remote TCP testing establishes the active port's empirical inbound ceiling
at 9.412 Gbit/s (1,122 MiB/s). A single stream reached 9.4008 Gbit/s and three
synchronized 16-stream repetitions were effectively identical. The best
integrated DataVine run uses 51.1% of that measured ceiling, leaving about 2.05x
network-only payload headroom. The second Intel X520 port is down with no
carrier. Evidence is `acceptance/controller-network-ceiling-20260828.json`.
