# DataVine Ultimate Acceptance

Status: **OPEN — NOT ACCEPTED**

This directory is the executable acceptance contract for DataVine. Component
tests, compilation, and small workflows are necessary but never sufficient.
Final acceptance requires one deterministic Grand Challenge, the complete mode
and fault matrix, explicit resource limits, three architecture reviews, and
reproducible evidence tied to exact commits and factory packages.

## Non-negotiable final gate

DataVine may be declared complete only when all of the following are true:

1. The Grand Challenge completes with exact oracle outputs in full and
   deterministic-failure modes.
2. The accepted workload exercises at least 10,000 logical tasks, at least
   100,000 task-to-data bindings, hot/medium/bulk EData, volatile/durable
   IData, diverse graph regions, worker churn, transfer, persistence,
   repeated recovery, and recovery-aware pruning.
3. DataVine stays within explicit Controller, manager, worker-cache,
   persistence, SharedFS, and retained-storage limits.
4. The same logical Legacy workload fails a defined operational constraint or
   demonstrates a clearly worse scaling variable.
5. Pruning materially reduces retained or peak intermediate storage.
6. Advancing durability frontiers bound measured recovery depth.
7. Every mandatory row in `matrix.md` is PASS with a committed artifact.
8. Reviews A, B, and C have no unresolved critical findings.
9. Results identify exact source commit, environment archive hash, workload
   configuration, failure seed/schedule, output checksums, and commands.
10. No critical TODO, duplicate authority, legacy semantic dependency, or
	unexplained fallback remains.

The required final statement must be proven experimentally:

> Legacy TaskVine could not execute the workload within the defined
> control-plane, I/O, storage, and recovery constraints. DataVine completed it
> correctly under repeated worker churn because work scales with unique data
> and active recoverable lineage rather than repeated task-specific
> provisioning and retained workflow history.

## Mandatory comparison modes

The same logical workload must run in these modes:

1. Legacy TaskVine/DAGVine.
2. DataVine with major optimizations disabled.
3. Full DataVine.
4. Full DataVine with deterministic failures.
5. Full DataVine with pruning disabled.
6. Full DataVine with peer transfer disabled.
7. Full DataVine with prefetch disabled.
8. Full DataVine with safe legacy-equivalent persistence behavior.

Correctness is checked in every mode that completes. Performance claims require
at least three accepted repetitions, with median and variation.

## Grand Challenge workload contract

One configurable workflow must simultaneously contain:

- fine-grained repeated Python code, positional args, kwargs, dependency files,
  nested containers, aliasing, and repeated references;
- multiple outputs, chains, fan-out/fan-in, diamonds, multiple roots/finals,
  heterogeneous branch lengths, and dynamic downstream release;
- tiny hot, medium reusable, large one-time, volatile, and durable data;
- peer transfer, prefetch, controlled persistence, deterministic worker churn,
  repeated recovery, and pruning;
- enough intermediate data that pruning-disabled execution exceeds the
  configured retained-storage budget.

An accepted scale deviation requires a committed resource justification and
must still expose the intended architectural scaling difference.

## Deterministic fault contract

The committed schedule must cover faults before execution, during demand
fetch, before publication, after volatile publication, while persistence is
queued/active, after durability, during peer transfer/prefetch/source
selection, between direct consumption and downstream durability, during
recovery, during a second recovery cycle, and during pruning evaluation.

It must cover worker loss, only-replica and partial-replica loss, stale sources,
corruption, SharedFS failure/overload, Controller timeout, and duplicate,
delayed, or reordered protocol events. Cross-component races are tracked in
`matrix.md`.

## Required commands

Clean build after relevant source changes:

```bash
cd /users/jzhou24/cctools_repo/datavine/taskvine/src
make clean && make -j8 && make install
```

Factory entry point:

```bash
/users/jzhou24/graph_optimization/factories/run_factory.sh
```

The accepted environment is always:

```text
/users/jzhou24/graph_optimization/factories/datavine.tar.gz
```

The final implementation must add one-command clean build, complete regression,
and Grand Challenge entry points. Until those commands and their artifacts
exist, reproducibility remains OPEN.

## Evidence layout

```text
acceptance/
  README.md                 this contract
  matrix.md                 requirement status and evidence index
  reviews/                  Reviews A, B, and C
  configs/                  committed workload and fault schedules
  artifacts/                machine-readable accepted summaries
  scripts/                  one-command acceptance entry points
```

Raw run logs may live outside Git, but every accepted summary and checksum must
be committed.
