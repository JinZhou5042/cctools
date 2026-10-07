# DataVine maintenance, 2026-09-06

## Earlier artifact cleanup

These four cleanup rounds predate the subsequent instruction to restrict work
to DataVine Markdown. Their accepted results and source were retained.

| Round | Generated files removed | Payload bytes | Repository disk use afterward |
|---|---:|---:|---:|
| Run directories and Python caches | 3,766 | 78,926,029 | 567 MiB |
| Object files and compiled TaskVine examples | 401 | 66,896,760 | 501 MiB |
| dttools/Work Queue tests and examples | 20 | 78,785,704 | 426 MiB |
| Peripheral build tools and compatibility copy | 7 | 49,401,480 | 379 MiB |

Payload bytes and allocated disk usage differ. Each round passed all 11
acceptance manifests (500 entries). Source/evidence hash comparisons passed;
rounds 2–4 also passed Make reconstruction dry-runs. No runtime tests were run.

`make -j8` regenerates configured build targets. For removed examples/tests,
use `make -C taskvine/src/examples`, `make -C dttools/src`, or
`make -C work_queue/src` as appropriate. The last round removed `makeflow_viz`,
`makeflow_analyze`, `makeflow_status`, `chirp_benchmark`, `rmonitor_poll_example`,
`deltadb_upgrade_log` and `work_queue_pool`; their owning Makefiles rebuild them.
Runtime services, libraries, tracked test inputs and Git history were retained.
Session inventories are temporary `/tmp/datavine-cleanup-20260906-*.json` files,
not scientific evidence or durable handoff dependencies.

## DataVine documentation consolidation

Only DataVine-related Markdown is in scope for this pass. Unrelated module
files, runtime code, machine results and checksummed campaign reports are not
modified. Responsibilities are defined in [the maintenance workflow](../agent-plans.md).

- The project map owns navigation, and the production contract owns semantics.
- Handoff owns delivery state; progress is now a compact decision record.
- The dynamic pilot diagnosis is consolidated with its correction. Transitional
  protocol guidance is replaced by links to the current contract and client.
- Controller reports retain measurements, scope and limitations; completed
  observability/fault-testing work is no longer presented as pending.
- The acceptance matrix distinguishes historical PASS evidence from new runtime
  verification and avoids causal claims from unmatched performance comparisons.

Validation passed: local Markdown links resolve, all 11 existing manifests
verify (500 entries), and files outside the 16 edited DataVine Markdown files
retain their pre-edit hashes. `git diff --check` passes. This does not claim a
new build, runtime acceptance, commit or package promotion.
