# DataVine acceptance matrix

Reviewed: 2026-10-06. Historical runtime evidence dates remain those of the
linked campaigns; current local verification is recorded below.

PASS means the recorded gate passed on its recorded build. [The result index](README.md) owns reports
and methods; this matrix owns status and remaining scope.

| Gate | State | Evidence |
|---|---|---|
| Ownership boundary | PASS | Manager carries opaque task frames; Controller owns DataID/replica/result lifecycle |
| Task/data decoupling | PASS | physical completion releases children without persistence admission |
| Worker-local/peer-first data | PASS | `volatile-worker-local-20260826.json` and full regression |
| Default background iData backup | PASS | Controller-background by default with explicit Worker-local opt-out; compact low-priority ring, 4/16-thread cap, at most one background pull per Worker, fsync+rename+digest admission |
| Peer-first input delivery | PASS | 10k x 1 MiB, 32 producer + 64 consumer Workers: peer median 1,779 MiB/s vs Controller 832 MiB/s; Worker local replicas remain after backup |
| Background backup recovery | PASS | only Worker killed; fresh Worker fetched a 1-MiB+17-byte output in bounded signed chunks; producer replay count 0 |
| Late consumer and lazy replay | PASS | Shell workflow focused cases |
| Requested result persistence | PASS | Worker stream -> Controller `/tmp`, fsync and atomic rename |
| Million-task no-op throughput | PASS | 2x exact Condor 8x16: mean 28,705.2 service and 21,662.9 E2E tasks/s; each 1M/1M with zero output/backup/journal; `million-task-throughput-20260903/` |
| Million-task big Worker pool | PASS / ceiling | exact Condor 32x16: 1M/1M, 27,069 service and 20,605 E2E tasks/s, zero output/backup/journal; historical 8x16 comparison, not code-identical A/B; Manager send/status dominate recorded timers; `million-task-bigpool-20260903/` |
| Optional workflow recovery | PASS | `journal` remains default; explicit `none` keeps live events while omitting workflow journal/checkpoints; validator and local exact gates pass |
| Controller indexed metadata | PASS | zero generic hash/itable objects; 8-byte DataID slots; strict single workflow |
| Controller throughput | PASS | fresh 10M lifecycle 13.48 s, 2.28 GiB RSS, 8.63M publish/s; unbatched RPC 111.5k publish/s |
| Controller concurrency knee | PASS | small-file persistence peaks near 16 Workers; 64 Workers regress and inflate RSS/FDs |
| Remote large-output fan-in | PASS | 4-GiB sweeps peak at 542.8-546.4 MiB/s with 16 Workers; 32/64 Workers regress |
| Controller inbound network ceiling | PASS | memory-only remote TCP: 9.4008 Gbit/s single stream; three 16-stream runs mean 9.4120 Gbit/s (1,122 MiB/s) |
| Exact 10k x 1-MiB persistence | PASS | two remote 16-Worker runs: 23.03-33.22 s service, 28.13 s mean, exact 10,000 files and 10,000 MiB per run |
| Fixed-topology data-intensive A/B | PASS / pilot | three 2x4 pairs, exact 1,024 tasks and 10,240 workflow files: median paired DataVine speedup 16.09x; Manager bytes -99.938%; identical sampled hashes |
| Result-driven dynamic correctness | PASS / pilot | three 2x4 pairs discover the same 477-task/445-edge graph; exact physical identity, hashes, CPU work, zero failure/recovery |
| Result-driven dynamic performance | PASS / pilot | root cause was 478 tiny SharedFS object puts; inline control frames reduced them to one. Two new 2x4 Condor runs took 1.2236/1.2246 s; stream RPCs 483 vs matched poll 1,539 |
| Unified dynamic data lifecycle | PASS | one static/dynamic resolver; restart hydration is lazy O(1); 7->restart->49->50 has zero producer replay; 1k forced-peer late consumers complete and post-seal files=0 |
| Durable result stream | PASS | Controller-admitted sequence, empty/inline/descriptor records, disconnect resume, restart replay and terminal frame |
| Dead command stdout suppression | OPEN | recorded command-stdout transfer remains unresolved; not reproduced during this documentation review |
| Controller agent persistence metrics | PASS | opt-in per-thread counters explain 99.8% of 1-MiB service time; `fsync` 50.6%, verify reread 21.8% |
| Requested-output `fsync` contract | PASS | required before metadata commit to surface delayed local ENOSPC/EIO; not a Controller-host crash guarantee |
| Persistence optimization screening | PASS / no promotion | streaming SHA, `fdatasync`, preallocation and `sync_file_range` showed no stable matched-load improvement |
| Controller persistence fault injection | PASS | test-only LD_PRELOAD gate: ENOSPC/EIO leave no descriptor/final/temp file; restart recovery commits once and replays without a Worker |
| `/tmp` storage decision | PASS | `controller-local-tmp-20260827/summary.json` plus checksummed raw evidence |
| Exact 32x16 storage-path matrix | PASS | 18/18 rounds; empty-file medians: Controller `/tmp` 11,342, peer 3,119, SharedFS 1,480 files/s; 32-GiB medians: peer 6095, SharedFS 822, Controller 462 MiB/s; `storage-matrix-20260901/` |
| Dense Controller/SharedFS crossover | PASS / no threshold | Uniform 32--1024 KiB grid, 5 samples/size/path, 1,310,720 files and 660 GiB total; median winner changes 7 times, so storage state dominates a fixed size rule; `storage-crossover-20260901/` |
| Elastic FunctionCall admission | PASS | six randomized local workloads reach 98.11--100.71% of measured fixed-window oracle; 2-GiB memory peak 76.81% under 80% limit; `adaptive-window-20260903/` |
| Queued-call recall and eviction | PASS | 3/3 late-Worker runs recall exactly 8 queued calls for a 24/8 split; 3/3 killed-receiver runs recover all 32 results; running calls are never recallable |
| Elastic remote scale | PASS / single point | Condor 8x16, 10k mixed 20-ms calls: 5,096 service tasks/s, 2,227 Python E2E tasks/s, no loss; Worker admission excluded |
| Controller RPC object path | PASS | local DVP3/DVP4 plus cross-node 4 x 131,273-byte DVP4; signed capability exposes no Controller-local path; four upload connections selected |
| Worker child lifetime | PASS | transfer server exits when an abruptly killed Worker loses its parent |
| Native FunctionCall output | PASS | exact-byte regression and 20k-task/39,488-edge layered DAG |
| Strict-singleton full regression | PASS | 21/21 recorded again in `adaptive-window-20260903/summary.json`; one owner per workflow; generic serverless also passed in that campaign |
| Retired journal fail-closed | PASS | valid opcode-107 record rejected by workflow service test |
| Current elastic provenance | PASS / uncommitted | base `c2e9a85`; dirty-source and binary SHA-256 recorded in `adaptive-window-20260903/summary.json`; commit/promotion remains operator work |
| Production package | PRIOR CANDIDATE | prior 791,236,983-byte package passed clean-build/import/smoke checks; it does not contain the current elastic/Controller-RPC work and canonical `datavine.tar.gz` was not overwritten |
| Cross-host result durability | OUT OF SCOPE | local `/tmp` does not survive Controller-host loss |

## Current architecture verification, 2026-10-06

The module review is in [DATAVINE_MAP.md](../DATAVINE_MAP.md#source-map).
These checks used the rebuilt local TaskVine/DataVine tree and matching Python
bindings, with `/users/jzhou24/miniconda3/envs/dagvine-env/bin/python`.

| Check | Result | Evidence and scope |
|---|---|---|
| Scoped native build | PASS | `make -C taskvine/src -j8`; `/tmp/datavine-architecture-final-build.log` |
| Full DataVine regression | PASS, 21/21 | `/tmp/datavine-architecture-final-regression.json`; IR, Scheduler, protocol, static/dynamic execution, data lifecycle, persistence faults, recovery and result streaming |
| Elastic admission and queued-call lifecycle | PASS, 4/4 | Short calls, declared-memory pressure, late Worker and Worker eviction; `/tmp/datavine-architecture-admission.json`; correctness checks, not performance promotion |
| Generic TaskVine behavior | PASS | Original serverless direct/fork check and multicore benchmark with all 16 outputs |
| Resource and value correctness | PASS | Executor EOF kills/reaps an active Python child; forced object short read preserves FD count; frontend retains the sign of floating-point zero |
| Current research trial path | PASS | Four-task DataVine/stock TaskVine pair; exact matching result and payload digests; `/tmp/datavine-architecture-paper-smoke-89du6laz/summary.json` |
| Active script/schema checks | PASS | Python syntax, schema JSON parse, current trial/campaign and admission CLI entry points |

The suite first exposed a stale recovery-source substring assertion; that
implementation-mirroring block was removed while lifecycle/recovery behavior
checks remain. The final complete suite above passed after rebuilding the
retry contract and removing the unused submission wrapper.

The Poncho factory package, remote scaling, full ATLAS replay and historical
performance campaigns were not refreshed. Optional process sampling was not
run in this environment. Temporary verification reports describe this dirty
checkout; they are not a published release or durable benchmark archive.

Historical intermediate tuning, pre-singleton regressions and earlier baseline
measurements remain in their campaign files, indexed by [README.md](README.md).
They are not separate current acceptance gates. Follow the all-campaign checksum
command there to validate evidence integrity; it does not rerun behavior tests.
