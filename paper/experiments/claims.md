# Claims and evidence obligations

The upgraded manuscript scores 345 trials in five randomized blocks per configuration. See `results/upgrade-audit.json` and `results/upgrade-results.md`. Historical 222-trial kernel evidence remains separate.

| Claim | Evidence | Interpretation |
|---|---|---|
| Coupled manager/data-plane bottleneck | `results/bottleneck-scale/summary.json`, `figures/bottleneck-motivation.pdf`, six fixed-topology 1-MiB durable-output runs, retained remote fan-in and matched 20k-task A/B | At fixed 4x2 workers, 64 to 1,024 durable objects grow service time 0.87 to 13.64 s; 4 GiB remote fan-in falls 542.8 to 442.9 MiB/s from 16 to 64 endpoints; the matched coupled path carries 64.1 MiB through the manager versus 0.04 MiB after separation and is 16.1x slower by median. These are motivation and attribution results, not an unlimited Controller-capacity claim. |
| End-to-end real scientific analysis | `atlas-inputs-v1/manifest.json`, `upgrade-application-v4` | 16 ROOT files, 9.86 GB, 36,564,144 events; 802 tasks. Exact seven-cut and 60-bin agreement on the measured primary platform. Educational analysis, not production collaboration readiness. |
| Application benefit under matched preloading | Five paired blocks; actual executor hashes checked | DV fixed 49.02 s, elastic 48.16 s, TV 63.51 s. Median paired ratios 1.29 and 1.32. |
| Benefit is not solely a larger function window | `upgrade-controls-v4`, 75 trials | Fixed/elastic/eager, TV C and extended TV 4C on matching CPU sets. Fixed DV remains competitive with feedback. |
| Native grouping comparison | `upgrade-grouping-v3`, 75 trials | Real group assignments; C/4C/8C slots; individual results preserved; zero failed attempts. The optional branch start-recall policy is disabled for every TV variant in this comparison. |
| Physical-host scaling | `upgrade-node-scaling-v3`, 120 trials | Nested 1/2/4/8 distinct hosts, eight CPU shares each, five blocks. Fixed DV CPU case has 2.98 median paired one-to-eight speedup; hash-data scaling is weak. Not exclusive nodes or 16+ scaling. |
| Initialization and instrumentation attribution | `upgrade-attribution-v2`, `upgrade-traces.json` | 60 paired trials and 5,120 validated local-clock traces. Cold DV can regress badly. CPU scopes and instrumentation cost are explicit. |
| Similar scientific work across backends | `upgrade-cpu-table.tex` and raw task records | Task-body CPU is close; worker-lifetime CPU differs. Worker CPU excludes controller/client; no total cluster CPU claim. |
| Correct current implementation | `upgrade-regression-final-v3.json`, `upgrade-mechanism-final` | 25/25 gates, eager/deferred 32 attempts each, generic serverless PASS. Runner verifies the loaded extension and hashes measured inputs. |
| Measured-build full application replay | `upgrade-current-application-amd`, `upgrade-current-application-intel` | Six full-sample runs pass, three on each of two platforms against independent references. Correctness smoke only, not another five-block performance campaign. |
| Worker-loss recovery | Retained `worker-churn-v3` | Three removals, 1,024 identities, 18 resubmissions, 16 sinks. This historical experiment retains its own implementation provenance. |
| Prepared-node assignment | Not implemented | OPEN. Eager preparation and native groups are not a reproduction of WOW/Wasabi. |
| Controller capacity and total data-path cost | `results/controller-decoupling-20260906.json`, `results/bottleneck-scale/summary.json`, manager timers, worker CPU, remote fan-in | Partial but measured: data-worker width gives a 2.37x median gain on 256 MiB durable output and fan-in saturation is visible. Exact sender bytes, prepared physical bytes, and broader fixed-pool sweeps remain OPEN. |
| Heterogeneous numerical portability | alternate-platform replay and independent-reference diagnostic | The unchanged alternate-platform reference exactly reproduces the two-event and bin differences. Platform-specific manifests retain strict independent validation; cross-platform bitwise identity is not claimed. |

All incompatible grouping trials are excluded, including successful ones. The phase graph has no single-parent chains, but singleton groups still reserve Workers; grouping is not a scheduling no-op. Earlier raw metadata used that inaccurate description; the current adapter and manuscript correct it without modifying retained measurements.

No claims of first peer transfer/data plane/overcommit, universal adaptive superiority, arbitrary external-side-effect exactly-once execution, global memory safety, or controller-host-loss durability are made. Artifact PASS verifies its declared evidence and format; it does not certify conference acceptance.

Diagnostic paths containing `intel` retain an initial naming mistake. The alternate platform is recorded as AMD EPYC 9334; the observed numerical difference is not attributed to CPU vendor.
