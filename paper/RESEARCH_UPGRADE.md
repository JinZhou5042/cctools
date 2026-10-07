# Research upgrade checkpoint

The five primary campaigns are complete: 345 accepted trials, five randomized blocks per configuration. The retained measured runtime passes 25 regression gates, eager/deferred traces, generic serverless execution, and six complete application runs on two platforms. The later configurable service build passes protocol regression with one and 16 RPC/data-plane threads. A fixed-topology durable-output scale-up, remote fan-in scale-out, and matched coupled-versus-separated A/B now provide the background bottleneck figure (`results/bottleneck-scale/summary.json`). The revised PDF compiles and the upgraded artifact audit passes its declared primary-evidence and format checks.

The first innovation now has a separate current-build evidence package: with one scheduling service held fixed, 256 one-MiB requested outputs improve from 26.9 to 63.7 tasks/s median as Controller data workers rise from 1 to 16 (2.37x), with all durable files and bytes verified. Unbatched metadata RPC rises with concurrent connections but reaches a shared-state plateau; this is reported as a boundary, not hidden as unlimited scaling.

See [HANDOFF.md](HANDOFF.md), [results/upgrade-results.md](results/upgrade-results.md), [experiments/claims.md](experiments/claims.md), and [experiments/next-evidence.md](experiments/next-evidence.md).

## Accepted evidence

| Campaign | Trials | Scope |
|---|---:|---|
| upgrade-controls-v4 | 75 | Complete paired blocks; no incompatible grouping |
| upgrade-application-v4 | 15 scored of 20 retained | Full 36.56-million-event sample; all grouping rows excluded |
| upgrade-attribution-v2 | 60 | Cold/preloaded and instrumented/uninstrumented |
| upgrade-grouping-v3 | 75 | Native groups C/4C/8C with compatible start-credit policy |
| upgrade-node-scaling-v3 | 120 | 1/2/4/8 distinct physical hosts, eight CPU shares each |

The original manuscript and earlier checkpoints are archived under provenance/. Exact measured binaries and their source patch are retained under provenance/measured-upgrade-v1/. Final repairs have their own build and regression hashes. No measurement was rewritten to claim it used a later binary.

## Numerical-platform diagnostic

The additional measured-build alternate-platform replay was rejected by the primary reference because it selected two additional events. The unchanged independent reference, run on that second platform with verified local copies of every ROOT input, exactly reproduced the replay's cutflow and histogram. This establishes a platform-specific numerical reference difference rather than a runtime-only discrepancy.

The initial independent run blocked in NFS open-state waits and was stopped; the local-staging version completed. `platform_atlas_manifest.py` now derives a new manifest only from an independent reference with matching source/input hashes. It never learns expected results from runtime output.

The second-platform three-variant replay (Condor 375649) completed 3/3 PASS against the independent platform reference on the retained measured runtime. Its result is `results/upgrade-current-application-intel/summary.json`. The primary five-block results remain unchanged.

## Implementation

The 11-file review patch is `provenance/upgrade-implementation.patch`, verified with a reverse-apply check. It fixes deferred group membership, joins, reference release, later-member failure propagation, grouped start deadlines, explicit invalid executor overrides, and regression binding selection. New tests exercise cold libraries, credits, joins, cancellation, socket-reset recovery and invalid executor paths.

## Resource ownership

The eight-host Condor pool 375635 and TCP broker are stopped. Preserve unrelated held course jobs 251996.0 and 252002.0. The independent reference and final replay completed; the manuscript was rebuilt after the numerical-platform wording correction. Final audit results are in `results/upgrade-artifact-audit.json`.

Prepared-node placement, exclusive 16+ nodes, controller saturation attribution, total sender traffic and prepared physical bytes remain open. Fixed/elastic similarity, cold-start regressions and the hash-data scaling plateau are retained as substantive limits.

Diagnostic paths containing `intel` retain an initial naming mistake. The alternate platform is recorded as AMD EPYC 9334; the observed numerical difference is not attributed to CPU vendor.

Final closure: manuscript rebuild and artifact audit PASS (including both platform replays); no owned campaign jobs or pool/broker processes remain. The two unrelated held course jobs are preserved.
