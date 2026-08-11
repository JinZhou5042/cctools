# DataVine acceptance matrix

Updated: 2026-08-11

Status: **WORKER-LOCAL CORRECTNESS PASS; FULL 9/9 AND PERFORMANCE PARITY OPEN**

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
| Regression | PARTIAL PASS | 8/8 executable contracts pass; ninth Go contract is environment-blocked by absent compiler/binary |

## Scale and CPU

| Gate | State | Current evidence |
|---|---|---|
| Exact 100k | PASS | worker-local 100,000/100,000; 28.63 s E2E; 4,303 Runtime tasks/s; 447 MB peak RSS |
| Exact 1M | PASS | worker-local 1,000,000/1,000,000; 295.64 s E2E; 4,122 Runtime tasks/s; 4.04 GB peak RSS; 44 FDs; five processes |
| CPU fork 1 core | PASS | DataVine/FunctionCall rate 1.003; 97.56% useful CPU |
| CPU fork 4 cores | PASS | rate 1.003; 97.07% useful CPU |
| CPU fork 16 cores | PASS | rate 0.966; 92.01% useful CPU |
| Multi-worker workflow characterization | PILOT PASS | worker-local persistent 10x16 Condor pools reached the exact all-connected gate with zero physical-task or result mismatch; homogeneous reserved-node publication run remains OPEN |
| Data-aware advantage | MIXED / LIMITATION | final-order worker-local 10x16 rerun: 32 MiB reuse is 0.326x and wide multi-output 0.361x versus TaskVine; no large intermediate persisted to the Controller |

The 100k/1M and the two data-heavy rows are fresh worker-local runs. CPU and
the remaining workflow-shape rows remain historical pre-worker-local baselines.

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
| Packed candidate | PASS, not promoted | worker-local SHA-256 `67c202c328b38f59d5da68a68ade5a05abdc076bdb1b57f49f15ad00a5b6f0bf`; package-local hashes match and packed 1x2x10k exact execution passes; production remains unchanged pending Go gate |
| Multi-manager/Foreman | OPEN | no current sharded metadata or partition acceptance |
| Cross-version migration | OPEN | replay-compatible v1 records exist; incompatible migration tool does not |
| Multi-tenant security | OPEN | token authentication exists; TLS, rotation, and authorization domains do not |

No OPEN row may be promoted from historical output. Artifact hashes
and reproduction commands are in `progress.md` and `current-handoff.sha256`.
Workflow-shape methodology, raw artifacts, limitations, and publication gates
are in `SC_WORKFLOW_COMPARISON_REPORT.md`.
