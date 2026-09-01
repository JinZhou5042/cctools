# DataVine maintainer plan

Updated: 2026-08-27

Read `DATAVINE_PRODUCTION.md`, then `progress.md`, before editing. The first is
the architecture contract; the second records what is actually verified in the
current worktree. Historical phase narratives are intentionally not maintained.

## Design filter

Every change must keep DataVine lightweight, efficient, high performance and
maintainable:

1. one clear owner for each state transition;
2. no duplicate task/data control plane;
3. bounded hot-path memory, queues and work per reactor turn;
4. no payload through Manager or workflow journal;
5. one production behavior, with failures closed rather than compatibility
   fallback;
6. one logical task remains one physical TaskVine task.

TaskVine core remains a usable baseline. Comparison support belongs in generic
TaskVine mechanisms or benchmark drivers, not in a second DataVine runtime.

## Change boundaries

- Scheduler changes may affect readiness, dispatch, completion or retry only.
- Controller changes may affect DataID/replica/result lifecycle only.
- Worker Agent changes may affect local storage, peer movement and data RPC.
- Workflow Store changes may affect IR, generation, task events and replay; it
  must not store result payloads.
- Python and other adaptors serialize and submit; they do not own scheduling or
  recovery policy.

Reject changes that reintroduce Manager `vine_file` ownership for DataVine
outputs, Worker-direct SharedFS persistence, size-based routing, per-file
threads, multi-workflow lanes or old journal-result replay.

## Required workflow

Before editing:

1. inspect branch, HEAD and dirty files;
2. read the relevant source diff, not only generated artifacts;
3. classify the change against the ownership boundary above.

Before handoff:

1. warning-clean native and tool builds;
2. Python compile/static checks and module-boundary test;
3. focused tests for the changed behavior;
4. complete DataVine regression when the environment is available;
5. `git diff --check` and generated-residue/process checks;
6. update only `progress.md`, `acceptance/matrix.md` and the compact handoff.

Do not promote a package or change `/users/jzhou24/graph_optimization/factories`
unless explicitly requested. Do not claim a clean or reproducible release while
the worktree is uncommitted.

## Current ordered work

1. Keep the 21/21 strict-singleton regression as a required release gate; each
   new scenario must use one service and journal per workflow.
2. Profile Manager/Worker allocation and message formatting before replacing
   more Runtime tables. The remaining scheduler time is only about 43 ms per
   50k-task run, so array conversion without profile evidence is low value.
3. Close Git provenance and verify a newly built candidate package before
   promotion.
4. Treat cross-host requested-result durability as a separate future feature
   with an explicit semantic and performance gate.

The million-task dataset and old campaign reports are not prerequisites for
ordinary changes. Use a small exact workflow first; scale only when the change
targets scale behavior.
