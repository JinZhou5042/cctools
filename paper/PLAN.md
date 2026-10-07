# DataVine paper execution checkpoint

Started 2026-09-06 at HEAD c2e9a85be3e52d2b7f0210487a6632b8c03a413e.
Preserve the starting dirty checkout; provenance/ contains its hashes and diff.

## Deliverable

A complete IEEE LaTeX research manuscript, compiled PDF, measured figures,
reproducible campaigns, and validated DataVine implementation changes. The
paper's contribution and submission evidence are specified in
experiments/claims.md and experiments/next-evidence.md. No submission or public
posting is authorized or performed.

## Completed

- Preparation ablation and opt-in attempt tracing; default behavior preserved.
- Dense worker-pass recall starvation reproduced, diagnosed, fixed, and verified
  in the original remote shape and a deterministic C regression.
- Full DataVine regression 22/22 PASS; 32 ordered attempts in each preparation
  mode; generic TaskVine direct/fork serverless acceptance PASS.
- Explicit worker CPU affinity after detecting that Condor requests are shares,
  not a hard CPU quota. Local worker CPU sets are disjoint.
- Private dependency-preload executor and symmetric TaskVine hoisting control;
  original executable remains unchanged. All 48 scientific preload trials PASS.
- Larger 1,024-task, 1-MiB-per-task hash pipelines: 15 cold + 15 preloaded PASS.
- 24 memory/late-worker recall trials and 6 route-isolation trials PASS.
- Post-fix worker-churn-v3 PASS: 3 actual owned-job removals, 1,024 logical
  completions, 18 resubmitted tasks, 1,042 physical submissions, all 16 sinks.
- NFS import stalls diagnosed with owned-child process-state sampling. Retain
  negative cold results; do not pool cold and preloaded allocations.
- Complete paper, bibliography, numerical generation, vector figures, audit,
  source-delta reconstruction, and reproduction commands implemented.

## Final state: 2026-09-06 07:32 EDT

All five campaigns are terminal PASS: 96 colocated, 48 remote worker-pool,
48 preloaded scientific, 15 cold hash-data, and 15 preloaded hash-data trials.
Final source-delta capture, figure generation, LaTeX build and strict artifact
audit PASS. PDF has 10 body pages and 11 total pages including references.
Final pages were rendered and reviewed. No task-owned batch jobs remain; the
two unrelated held course jobs were preserved. See HANDOFF.md for findings,
all evidence links, source scope and remaining research gates.

## Version history and scope

V1 caught harness/startup restrictions. V2 exposed recall starvation. V3 was
stopped after the CPU-share audit. V4 is the post-fix, CPU-affinity main version.
Earlier incomplete/failed results remain diagnostic evidence. Pressure-v1
failed its actual-recall gate; pressure-v2 corrected the initial queued workload
and passed without relaxing the gate. Churn-v1 lacked required transaction
logging; v2 passed before the fairness fix; v3 revalidated the current binary.

The initial root build rebuilt some unrelated ignored objects before it was
stopped. No unrelated source edit/deletion was made; subsequent builds were
restricted to TaskVine. The reviewable source patch is against the starting
dirty files, not against HEAD. Source hashes verify preserved unrelated dirty
files. Do not claim ignored build objects were untouched.

## Submission constraints and research gates

Official CFP checked September 6: abstract October 1 and paper October 8, 2026
AOE; IEEE 10pt letter, two columns, ten body pages, unlimited references;
double anonymity; full author lists and DOI/direct URLs; AI disclosure and
section citations. Runtime Systems is the intended primary track.

A compiled, audited artifact is not equivalent to a submission-ready result.
Production application, reserved physical-node scale, complete causal
attribution, and closest grouping/prepared-node baselines remain explicit
research gates in experiments/next-evidence.md. Do not relabel small kernels
or opportunistic worker pools to imply those gates are closed.
