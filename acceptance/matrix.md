# DataVine acceptance matrix

Updated: 2026-08-31

| Gate | State | Evidence |
|---|---|---|
| Ownership boundary | PASS | Manager carries opaque task frames; Controller owns DataID/replica/result lifecycle |
| Task/data decoupling | PASS | physical completion releases children without persistence admission |
| Worker-local/peer-first data | PASS | `volatile-worker-local-20260826.json` and full regression |
| Default background iData backup | PASS | Controller-background by default with explicit Worker-local opt-out; compact low-priority ring, 4/16-thread cap, at most one background pull per Worker, fsync+rename+digest admission |
| Peer-first input delivery | PASS | 10k x 1 MiB, 32 producer + 64 consumer Workers: peer median 1,779 MiB/s vs Controller 832 MiB/s; Worker local replicas remain after backup |
| Background backup recovery | PASS | only Worker killed; fresh Worker fetched a 1-MiB+17-byte output in bounded signed chunks; producer replay count 0 |
| Background backup overhead | PASS / local | alternating 3x 10k empty-output runs: median logical throughput 1,163.3 -> 1,133.8 tasks/s (-2.54%); 4x4 validation backed up 10k at 3,318.5 files/s |
| Late consumer and lazy replay | PASS | Shell workflow focused cases |
| Requested result persistence | PASS | Worker stream -> Controller `/tmp`, fsync and atomic rename |
| Scheduler throughput | PASS | 3x local 50k mean 4,067.7 tasks/s; Condor 16x4 19,277.7 service tasks/s |
| Controller indexed metadata | PASS | zero generic hash/itable objects; 8-byte DataID slots; strict single workflow |
| Controller throughput | PASS | fresh 10M lifecycle 13.48 s, 2.28 GiB RSS, 8.63M publish/s; unbatched RPC 111.5k publish/s |
| Controller RPC latency | PASS | 16 connections: 87.1k publish/s and 81.9k resolve/s, p99 below 314 us |
| Controller concurrency knee | PASS | small-file persistence peaks near 16 Workers; 64 Workers regress and inflate RSS/FDs |
| Remote large-output fan-in | PASS | 4-GiB sweeps peak at 542.8-546.4 MiB/s with 16 Workers; 32/64 Workers regress |
| Controller inbound network ceiling | PASS | memory-only remote TCP: 9.4008 Gbit/s single stream; three 16-stream runs mean 9.4120 Gbit/s (1,122 MiB/s) |
| Exact 10k x 1-MiB persistence | PASS | two remote 16-Worker runs: 23.03-33.22 s service, 28.13 s mean, exact 10,000 files and 10,000 MiB per run |
| Fixed-topology data-intensive A/B | PASS / pilot | three 2x4 pairs, exact 1,024 tasks and 10,240 workflow files: median paired DataVine speedup 16.09x; Manager bytes -99.938%; identical sampled hashes |
| Result-driven dynamic correctness | PASS / pilot | three 2x4 pairs discover the same 477-task/445-edge graph; exact physical identity, hashes, CPU work, zero failure/recovery |
| Result-driven dynamic performance | PASS / pilot | root cause was 478 tiny SharedFS object puts; DVP2 reduced them to one. Two new 2x4 Condor runs took 1.2236/1.2246 s; stream RPCs 483 vs matched poll 1,539 |
| Unified dynamic data lifecycle | PASS | one static/dynamic resolver; restart hydration is lazy O(1); 7->restart->49->50 has zero producer replay; 1k forced-peer late consumers complete and post-seal files=0 |
| Durable result stream | PASS | Controller-admitted sequence, empty/inline/descriptor records, disconnect resume, restart replay and terminal frame |
| Worker-local byte avoidance | PASS | explicit 4-GiB control: 1.44 MiB Controller RX, 0 durable files |
| Dead command stdout suppression | OPEN | generic TaskVine retrieves unrequested/no-consumer stdout after DataVine declines retention |
| Controller agent persistence metrics | PASS | opt-in per-thread counters explain 99.8% of 1-MiB service time; `fsync` 50.6%, verify reread 21.8% |
| Requested-output `fsync` contract | PASS | required before metadata commit to surface delayed local ENOSPC/EIO; not a Controller-host crash guarantee |
| Persistence optimization screening | PASS / no promotion | streaming SHA, `fdatasync`, preallocation and `sync_file_range` showed no stable matched-load improvement |
| Controller persistence fault injection | PASS | test-only LD_PRELOAD gate: ENOSPC/EIO leave no descriptor/final/temp file; restart recovery commits once and replays without a Worker |
| `/tmp` storage decision | PASS | `controller-local-tmp-20260827/summary.json` plus checksummed raw evidence |
| End-to-end data movement | PASS | exact 1 GiB Worker stream at mean 150.2 MiB/s service rate |
| Small-file transfer latency | PASS | TCP delayed-ACK fix: 24.24 -> mean 1,034.2 files/s, 42.6x |
| Fixed-core data-path scaling | PASS | 1/4/16 Worker matrix; empty plateau 3,657.7 files/s; exact 1 GiB guard 656.0 MiB/s |
| Completion-to-dispatch latency | PASS | interleaved 50k A/B: `prefer-dispatch` +10.00% at 4x4 and +14.55% at 16x1 |
| Dense Worker availability | PASS | 64x2 paired 50k A/B: +4.25% service and +2.98% E2E; neutral at 8 Workers |
| Native line parsing | PASS | 8x16 four-run mean +3.97% service; Worker `vfscanf` 4.23% -> 1.77% |
| Physical attempt lookup | PASS | DataVine Runtime direct array; completion-processing time -3.56% |
| Worker child lifetime | PASS | transfer server exits when an abruptly killed Worker loses its parent |
| Native FunctionCall output | PASS | exact-byte regression and 20k-task/39,488-edge layered DAG |
| Warning-clean build | PASS | native library, Worker and five tools rebuilt on 2026-08-27 |
| Pre-singleton full regression | PASS | final 18/18; `rigorous-validation-20260827/fifth-pass/regression.json` |
| Strict-singleton production regression | PASS | replica table, production data plane, scheduler, build and second-ID rejection |
| Strict-singleton full regression | PASS | 21/21 on 2026-09-01; all formerly multi-workflow notebook, execution, service and scientific scenarios now use one owner per workflow with no skipped tests |
| Retired journal fail-closed | PASS | valid opcode-107 record rejected by workflow service test |
| Rigorous post-cleanup suite | PASS | `rigorous-validation-20260827/summary.json`, raw evidence and 18/18 regression |
| Git provenance | OPEN | worktree remains uncommitted |
| Production package | OPEN / not requested | rebuild and promotion require explicit authorization |
| Cross-host result durability | OUT OF SCOPE | local `/tmp` does not survive Controller-host loss |

Run `sha256sum -c SHA256SUMS` inside
`acceptance/rigorous-validation-20260827/` to verify the new evidence. Only the
latest accepted evidence is listed here. Old campaign diagnostics,
admission failures and superseded architecture comparisons are not current
product gates.
