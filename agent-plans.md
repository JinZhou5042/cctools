# DataVine maintenance workflow

Read [DATAVINE_MAP.md](DATAVINE_MAP.md) first, then the relevant source and
[production contract](DATAVINE_PRODUCTION.md). The [handoff](DATAVINE_HANDOFF_20260827.md)
owns outstanding work; this file only describes how to make a change.

## Before editing

- Limit maintenance and cleanup to DataVine. Do not modify or delete unrelated
  modules' source, documentation, test inputs, caches or build products, even
  when those products can be rebuilt. A request for another cleanup/review
  round does not expand this scope.
- Shared TaskVine files require a concrete DataVine-related change and user
  authorization covering that change. Do not treat the whole CCTools checkout
  as DataVine-owned. Changes outside DataVine require an explicit user request.
- Inspect branch, HEAD and dirty files; preserve unrelated changes.
- Identify the owner of the affected state transition and trace its callers.
- Keep one logical task per physical attempt, bounded queues and reactor work,
  and separate task readiness from data persistence.
- Use a small exact workflow first. Scale tests are needed when the change
  affects scale behavior; they are not prerequisites for ordinary maintenance.

## Before claiming completion

Use the build commands in [acceptance/README.md](acceptance/README.md), then run
focused behavior tests and the appropriate DataVine regression. Changes in
shared TaskVine code also require a generic TaskVine regression. Report missing
runtime checks explicitly; a build or dry-run does not establish acceptance.

Update the document that owns the changed information:

| Information | Owner |
|---|---|
| Runtime semantics and policy | `DATAVINE_PRODUCTION.md` |
| Source navigation | `DATAVINE_MAP.md` |
| Commands and result navigation | `acceptance/README.md` |
| Verified gates and scope limits | `acceptance/matrix.md` |
| Decision rationale | `progress.md` |
| Outstanding delivery work | `DATAVINE_HANDOFF_20260827.md` |

Accepted campaign evidence remains tied to its recorded source and environment.
Retain counts, timer boundaries, limitations and checksums. Do not promote a
package or modify the factory archive unless the user requests that action.
