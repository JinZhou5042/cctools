# DataVine acceptance matrix

Updated: 2026-08-11

Status: **WORKER-LOCAL CORRECTNESS AND 10/10 PASS; PERFORMANCE PARITY OPEN**

## Architecture and correctness

| Gate | State | Current evidence |
|---|---|---|
| One native authority | PASS | C Runtime owns control; C Data Controller exclusively owns result files, metadata, hashes, fetch, retention, and GC |
| Core isolation | PASS | DataVine policy is in `libdatavine.a`; core additions are generic helpers |
| Thin language adaptors | PASS | Python, Shell, and Go share Workflow IR/RPC and contain no scheduler policy |
| Dynamic workflow | PASS | result-driven append, quiescent/resume, generation CAS, detach/attach, and restart |
| Python lifecycle | PASS | preloaded single-threaded parent, independent fork children, process-group cancel/wall-time cleanup |
| Logical/physical identity | PASS | one logical task creates one physical TaskVine task; no noop grouping path remains |
| Multi-output callable | PASS | direct cloudpickle output files are atomically published as one metadata batch |
| Live result | PASS | a requested DataID is fetchable immediately after its task commit while downstream work is still running |
| Payload isolation | PASS | 2 MiB result stays in immutable Data Controller storage; workflow journal remains under 1 MiB |
| Corruption | PASS | modified result file makes service restart fail closed |
| Regression | PASS | 10/10 contracts pass, including focused output retention/durability and the hash-verified Go adaptor |

## Scale and CPU

| Gate | State | Current evidence |
|---|---|---|
| Exact 100k | PASS | worker-local 100,000/100,000; 28.63 s E2E; 4,303 Runtime tasks/s; 447 MB peak RSS |
| Exact 1M | PASS | worker-local 1,000,000/1,000,000; 295.64 s E2E; 4,122 Runtime tasks/s; 4.04 GB peak RSS; 44 FDs; five processes |
| CPU fork 1 core | PASS | DataVine/FunctionCall rate 1.003; 97.56% useful CPU |
| CPU fork 4 cores | PASS | rate 1.003; 97.07% useful CPU |
| CPU fork 16 cores | PASS | rate 0.966; 92.01% useful CPU |
| Multi-worker workflow characterization | PILOT PASS | worker-local persistent 10x16 Condor pools reached the exact all-connected gate with zero physical-task or result mismatch; homogeneous reserved-node publication run remains OPEN |
| Data-aware advantage | MIXED / LIMITATION | bottleneck-optimized exact 10x16 n=5: 32 MiB reuse is 0.814x and wide multi-output 0.599x versus TaskVine; wide wall falls 28.8%, but parity remains OPEN |

The 100k/1M rows are worker-local scale runs, and the two data-heavy rows are
the bottleneck-optimized exact 10x16, n=5 results. CPU and the remaining
workflow-shape rows remain historical pre-worker-local baselines.

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
| Generated residue | PASS | runtime info is temporary; old run directories, snapshots, logs, and retired test binaries removed |
| Production package | PASS, promoted | active `datavine.tar.gz` SHA-256 is `32e1361324df69be2db88258565f3ad393d2eb16ff9228e99387b895d9abba6d`; active-path smoke passed 10,000/10,000 at 3,332 Runtime tasks/s; prior `9c1c8372...5b9ca` package remains as rollback |
| Multi-manager/Foreman | OPEN | no current sharded metadata or partition acceptance |
| Cross-version migration | OPEN | replay-compatible v1 records exist; incompatible migration tool does not |
| Multi-tenant security | OPEN | token authentication exists; TLS, rotation, and authorization domains do not |

No OPEN row may be promoted from historical output. Artifact hashes
and reproduction commands are in `progress.md` and `current-handoff.sha256`.
Workflow-shape methodology, raw artifacts, limitations, and publication gates
are in `SC_WORKFLOW_COMPARISON_REPORT.md`.
