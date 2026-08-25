# DataVine acceptance matrix

Updated: 2026-08-25

Status: **FIXED 1024-CORE CAMPAIGN PASS; SCIENTIFIC FOUNDATION LOCAL PASS; REAL-APPLICATION DISTRIBUTED ATTRIBUTION OPEN**

## Architecture and correctness

| Gate | State | Current evidence |
|---|---|---|
| One native authority | PASS | C Runtime owns control; C Data Controller exclusively owns result files, metadata, hashes, fetch, retention, and GC |
| Runtime v2 Manager boundary | LOCAL PASS | Manager transports only a generic opaque auxiliary payload; DataVine inputs/outputs create no Manager vine_file/cache-update/unlink state; manifest and task-data telemetry are parsed only by Controller; direct Worker Agent protocol and module scan pass |
| Native parametric execution | DISTRIBUTED RECOVERY PASS / FULL PERF OPEN | exact full topology builds in about 2.59 s / 172 MiB with matching exhaustive oracle digest; bounded 4,096-task views; replacement-churn gate removed half of 8 Workers and completed exact 8,192 logical + 943 replay + 32 disconnect tasks with zero recovery timeout |
| Data-intensive exact 128x16 | OPEN | 1,048,576-task / 10,485,760-file production run has not yet produced a terminal accepted artifact |
| Completion/data admission decoupling | LOCAL PASS | physical success marks Scheduler DONE immediately; child dispatch does not wait for DATA_READY or requested-result persistence |
| Worker replica state machine | DISTRIBUTED PASS | chunked DataID table, indexed sessions, HMAC HELLO, digest/generation checks, reconnect advertisement, exact replica faults, idempotent GC ACK, coalesced replay, whole-closure Controller arming, O(n) A/B/C replay ordering, and bounded local origin retry; three 8,192-task disconnect gates have exact physical conservation and zero admission timeout |
| Parametric family evaluator | PASS | 1,344-byte full descriptor; exact 1,048,576 tasks and 10,485,760 files; 4,096 full samples plus exhaustive small-cohort inverse equivalence; native bounded-frontier execution and distributed replay are integrated |
| Production v1 contract | PASS | Annotated tag `datavine-production-v1-20260823` freezes one fail-closed production v1; historical callable ticket and manifest parsers are removed; post-freeze regression is 13/13 |
| Decoupled input data plane | PASS | IR/scheduler retain only DataIDs and SHA-256 identities; Data Controller resolves locations; workers pull and verify digest-scoped objects into stable cache identities |
| Serialization deduplication | PASS | callable and repeated invocation bytes serialize once; 1,000,000 repeated calls build at 44,945 tasks/s with 1,000,002 Data records |
| Stage attribution | PASS | final orthogonal runs identify `scheduler_delay` for 256 immediate tasks, `data_stage_in` for 16 unique 4 MiB inputs, and `python_function` for 64 x 50 ms CPU tasks |
| Core isolation | PASS | DataVine policy is in `libdatavine.a`; core additions are generic helpers |
| Thin language adaptors | PASS | Python, Shell, and Go share Workflow IR/RPC and contain no scheduler policy |
| Dynamic workflow | PASS | result-driven append, quiescent/resume, generation CAS, detach/attach, and restart |
| Python lifecycle | PASS | preloaded single-threaded parent, independent fork children, process-group cancel/wall-time cleanup |
| Logical/physical identity | PASS | one logical task creates one physical TaskVine task; no noop grouping path remains |
| Static compact IR | PASS | task/data defaults plus compact records; full records remain compatible; shared native accessors preserve one C graph owner |
| Producer completion readiness | PASS | Scheduler uses physical producer completion only; DATA_READY and persistence are independent Worker-to-Controller progress |
| Multi-output callable | PASS | direct cloudpickle output files are atomically published as one metadata batch |
| Live result | PASS | a requested DataID is fetchable immediately after its task commit while downstream work is still running |
| Payload isolation | PASS | 2 MiB result stays in immutable Data Controller storage; workflow journal remains under 1 MiB |
| Corruption | PASS | modified result file makes service restart fail closed |
| Regression | PASS | 17/17 contracts pass, including direct agent protocol, replica table, parametric evaluator, production data plane, scientific foundation, and hash-verified Go adaptor; ordinary TaskVine single-worker smoke also passes |

## Scale and CPU

| Gate | State | Current evidence |
|---|---|---|
| Exact 100k | PASS | worker-local 100,000/100,000; 28.63 s E2E; 4,303 Runtime tasks/s; 447 MB peak RSS |
| Exact 1M | PASS | worker-local 1,000,000/1,000,000; 295.64 s E2E; 4,122 Runtime tasks/s; 4.04 GB peak RSS; 44 FDs; five processes |
| Production static IR exact 1M, 4x16 | PASS | fresh local pool; 1,000,000 submissions/completions; 38.59 MB payload; 10.69 s registration; 4,271 Runtime tasks/s; 1.606 GB process-tree RSS |
| Production static IR 100k / 1M-I/O, 4x16 | PASS | real Python FunctionCall tasks; 26 RPC requests; 26.98 MB payload; 6.81 s graph load; 1.188 GB load RSS; exact 100,000 physical tasks |
| CPU fork 1 core | PASS | DataVine/FunctionCall rate 1.003; 97.56% useful CPU |
| CPU fork 4 cores | PASS | rate 1.003; 97.07% useful CPU |
| CPU fork 16 cores | PASS | rate 0.966; 92.01% useful CPU |
| Multi-worker workflow characterization | PILOT PASS | worker-local persistent 10x16 Condor pools reached the exact all-connected gate with zero physical-task or result mismatch; homogeneous reserved-node publication run remains OPEN |
| Data-aware advantage | MIXED / LIMITATION | bottleneck-optimized exact 10x16 n=5: 32 MiB reuse is 0.814x and wide multi-output 0.599x versus TaskVine; wide wall falls 28.8%, but parity remains OPEN |
| Fixed 1024-core synthetic campaign | PASS | exact 64x16 pools per backend; 49 cases, 98 accepted warmups, 980 accepted runs, 10 paired repetitions; paired CI gives 46 DV-faster, 2 DV-slower, 1 inconclusive; every regression attributed |
| 1024-core requested-output path | PASS / ADVANTAGE | 128 x 32 MiB: TV 13.008 s, DV 11.224 s, rate 1.159, paired CI [1.055, 1.274]; worker-local-first publication removed the old durable Controller hop bottleneck |
| 1024-core high-degree path | PASS / ADVANTAGE | degree 64: TV 25.230 s, DV 15.487 s, rate 1.548, paired CI [1.491, 1.583]; fan-in 16 reaches 232.8 versus 119.4 tasks/s |
| 1024-core large broadcast path | CONFIRMED LIMITATION | 32 MiB broadcast: TV 1.262 s, DV 1.371 s; separated result-fetch excess is 0.121335 s and covers the 0.109198 s wall gap; add parallel/streaming multi-DataID reads |
| 1024-core dynamic tiny path | CONFIRMED LIMITATION | TV 0.186 s, DV 1.007 s, paired CI [0.149, 0.202]; 8 dependent steps plus seal cause 9 Runtime invocations; keep a resident session lane |
| SharedFS eData object ingest | PASS / PROFILED LIMITATION | 4096 distinct 256-byte objects: 83.4 cold objects/s at 1 writer and 752.9 at 16; warm deduplicated 3162.2/s; cold SharedFS metadata/durability is the bottleneck |
| Scientific foundation | LOCAL PASS | versioned artifact schema, deterministic non-sparse generator, calibrated resource sampler and HEP-S TV-native/DV-native/TV-durable-sink driver pass 6/6 local runs with 19/19 exact physical counts and one result digest |
| Scientific distributed attribution | OPEN | producer/consumer worker identity, per-worker byte deltas, CAL-small cold/warm/peer gates, 10x16 pilots and reserved-node repetitions have not run |

The 100k/1M rows are worker-local scale runs, and the historical 10x16
data-heavy row predates the fixed-core result. The fixed 1024-core rows are the
current synthetic publication campaign. Real scientific-application
performance remains OPEN. Historical CPU and workflow-shape rows remain
pre-worker-local baselines unless explicitly labeled 1024-core above.

The removed grouped-noop implementation previously reported roughly 16k to
17k tasks/s at 1M scale. That number is historical and invalid for independent
TaskVine task throughput because up to 256 logical noops shared one physical
execution.

## Durability and operations

| Gate | State | Current evidence |
|---|---|---|
| Journal integrity | PASS | bounded records, replay, truncated-tail recovery, checksum corruption rejection |
| Result identity | PASS | DataID, producer/output, attempt, codec, size, and SHA-256 survive restart |
| RPC bounds | PASS | capabilities preflight and bounded frames, clients, queues, identifiers, tasks, and results |
| Worker/restart recovery | PASS | lifecycle suite covers retry, worker loss, restart, checkpoint resume, and cancellation |
| Production static IR live-loss recovery | PASS | four worker removals; exact sinks; 5,232 lost DataIDs caused 1,779 completed-task invalidations and 1,844 extra attempts; maximum attempt 3 |
| Generated residue | PASS | runtime info is temporary; old run directories, snapshots, logs, and retired test binaries removed |
| Production package | PASS, promoted | active `datavine.tar.gz` SHA-256 is `6019adc524f86bf4d14b984e8a6f07928cf08964ec2a6a19031e36829f50adcd`; package contract/hashes pass; active-path smoke passed 10,000/10,000 at 3,820.6 Runtime tasks/s; prior `32e136...abba6d` package remains as rollback |
| Multi-manager/Foreman | OPEN | no current sharded metadata or partition acceptance |
| Historical protocol migration | OUT OF SCOPE | production accepts v1 only and fails closed on removed executor/ticket/manifest generations |
| Multi-tenant security | OPEN | workflow-token authentication and one-object GET HMAC tickets exist; TLS, rotation, and tenant authorization domains do not |

No OPEN row may be promoted from historical output. Artifact hashes
and reproduction commands are in `progress.md` and `current-handoff.sha256`.
Workflow-shape methodology, raw artifacts, limitations, and publication gates
are in `SC_WORKFLOW_COMPARISON_REPORT.md`.
The production data-plane contract, final regression, stage experiments, and checksums
are in `DATAVINE_DATA_PLANE_V2.md` and `acceptance/data-plane-v2/`.
The current static-IR contract and 2026-08-23 acceptance artifacts are indexed
in `STATIC_IR_V2.md`.
