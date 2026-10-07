# DataVine project map

Updated: 2026-10-06

This is the canonical starting point for maintainers and follow-up agents. Read
this file first, then open only the linked contract, source, test or evidence
needed for the task. Historical experiment narratives are not architecture
specifications.

## Current source state

- Branch: `production-baseline`
- HEAD: `c2e9a85be3e52d2b7f0210487a6632b8c03a413e`
- Status: production candidate; the active benchmark/document worktree is
  intentionally uncommitted.
- Do not reset or broadly clean this checkout. Inspect `git status --short` and
  preserve unrelated changes.
- Canonical architecture and semantics: `DATAVINE_PRODUCTION.md`
- Current gate list and commands: `acceptance/README.md` and
  `acceptance/matrix.md`
- Decision rationale: `progress.md`
- Delivery state and next work: `DATAVINE_HANDOFF_20260827.md`

## Runtime boundary

One `datavine_workflow` process owns one workflow. The Scheduler owns task
dependencies, readiness, dispatch and retry. The Data Controller independently
owns DataIDs, replicas, requested-result persistence and data GC. Task success
releases children immediately; it never waits for output persistence.

Data stays Worker-local and resolves local-first, then peer-first. The default
background backup adds a Controller-local `/tmp` replica without deleting the
Worker replica. Requested durable outputs use Worker-to-Controller streaming,
SHA-256 verification, per-file `fsync` and atomic rename. SharedFS and
file-size routing are benchmark alternatives, not production data paths.

Python source execution uses DVP1, small callable invocations use inline DVP3,
and large DVP4 invocations use signed Controller RPC objects. The frontend never
reads or exposes the Controller's local object-store path.

## Source map

Native modules below are under `taskvine/src/datavine/`, Worker modules under
`taskvine/src/worker/`, and Manager modules under `taskvine/src/manager/`.

| Module | Sole responsibility | Architecture review, 2026-10-06 |
|---|---|---|
| Python `datavine/workflow.py` | Build IR, serialize functions, submit deltas and consume results | Session commits reuse builder append; native validation owns acceptance; no frontend scheduling or capability negotiation |
| Python `datavine/workflow_client.py` | Framed RPC and result access | Ordered batch reads use the standard thread pool; retained descriptors support large local results |
| `vine_datavine_ir.[ch]` | Expand compact records into the current IR | One normalized representation; defaults reduce metadata without a second execution path |
| `vine_datavine_workflow.[ch]` and workflow schema | Validate graph and executor contracts | Removed unused retry filters; synchronized object origins, input records, recovery and parametric descriptions |
| `vine_datavine_parametric.[ch]` | Evaluate the frozen data-intensive graph description | Retained for large graph registration; uses the same task, data and recovery owners |
| `vine_datavine_scheduler.[ch]` | Dependency readiness and logical task transitions | Retained bounded native scheduling and rebuild recovery; no Worker selection or persistence state |
| `vine_datavine_workflow_runtime.c` | Single workflow reactor and physical-attempt mapping | Direct native TaskVine submit; one builtin execution frame; no unused Manager parameter or submission wrapper |
| `vine_datavine_workflow_store.[ch]` | Accepted graph, workflow state and recovery events | Retained one workflow identity and shared static/dynamic lifecycle |
| `vine_datavine_journal.[ch]` | Ordered metadata writes and replay | Retained bounded writer queue and durability barriers needed for restart |
| `vine_datavine_data_controller.[ch]` | Data authority, persistence admission and GC | Retained separate foreground/background queues and 16 fixed data threads; removed forwarding wrapper |
| `vine_datavine_replica_table.[ch]` | Indexed replicas, generations and waiters | Retained integer-indexed state under Controller ownership; no additional data plane |
| `vine_datavine_object_store.[ch]` | Immutable callable/invocation objects | Retained digest identity and bounded reads; close descriptors on short reads |
| `vine_datavine_protocol.h`, `vine_datavine_rpc.[ch]` | Current v1 wire framing and service event loop | Retained bounded framing, authentication and streaming; no retired result-path endpoint |
| `vine_datavine_agent.[ch]` | Worker-local data preparation, publication and release | One workflow per Manager connection; removed the multi-workflow list |
| `vine_datavine_transfer.[ch]` | Peer and Controller payload transport | Retained range bounds, complete framing and digest checks needed to prevent corrupt data admission |
| `vine_worker.c`, `vine_function_call.[ch]` | Worker execution capacity and queued-call transport | One elastic admission policy, preparation when an execution opportunity exists; removed fixed/aggressive/AIMD/eager variants |
| `vine_manager.c`, `vine_worker_pool.[ch]`, `vine_schedule.c` | TaskVine physical dispatch and Worker choice | DataVine selects least-loaded native dispatch; generic defaults and generation-bound queued-call recall remain |
| `tools/datavine_workflow.c`, `tools/datavine_executor`, tools Makefile | Service startup and the installed Poncho execution library | One sibling executor and one library; builtin requests inline, Python work forked; EOF reaps children |
| `taskvine/test`, `acceptance/scripts`, `paper/scripts` | Behavioral checks and current experiment entry points | Removed duplicate preflight and recovery-source substring checks; one current trial/campaign path replaces forwarding wrappers; optional process sampling imports its dependency only when enabled |

The Controller metadata hot path uses paged integer-indexed arrays and compact
arenas, not generic hash tables or `itable`. Generic TaskVine keeps its normal
file and scheduling paths unless DataVine is explicitly selected.

The review covers every DataVine source module and its shared TaskVine
integration, frontend, tests and active launchers. Necessary graph/reference,
generation, payload-bound, digest and durability checks remain because removing
them can break normal execution or recovery. DataVine has no task-group path.
Current verification and unrefreshed package/performance gates are recorded in
[the acceptance matrix](acceptance/matrix.md).

## Build and evidence

Use [acceptance/README.md](acceptance/README.md) for build commands, the result
index and retention rules. [acceptance/matrix.md](acceptance/matrix.md) records
gate status and scope. Campaign reports own benchmark numbers and methods;
open their machine results before quoting performance.

## Work and decisions

[The handoff](DATAVINE_HANDOFF_20260827.md) lists delivery state and next work.
[The decision record](progress.md) links retained design decisions to evidence.
[The maintenance workflow](agent-plans.md) describes change and verification
responsibilities. Thread counts, admission limits and recovery semantics are
maintained only in [the production contract](DATAVINE_PRODUCTION.md).

## Maintenance

Maintenance is limited to DataVine. Do not change or delete unrelated modules'
files, including regenerable caches and binaries. Repeated cleanup requests do
not broaden this boundary; see [the scope rule](agent-plans.md#before-editing).

Keep this map short: source ownership and project boundaries belong here;
commands and result navigation belong in `acceptance/README.md`; architecture
semantics belong in `DATAVINE_PRODUCTION.md`. `progress.md` records decisions;
the handoff records outstanding work. Neither should duplicate the contract or
full experiment reports.

The 2026-09-06 cleanup is recorded in `acceptance/maintenance-20260906.md`.
