# DataVine paper research upgrade

Current scope update: DataVine uses one installed Poncho executor; task
environment and executor-path overrides are removed. DataVine task-group
changes, tests, and experiment launchers have been removed. The results and validation below describe the
historical measured build and are not current-checkout acceptance.
Worker admission now uses one elastic policy with preparation on execution
opportunity. Current module ownership and verification are in
`../DATAVINE_MAP.md` and `../acceptance/matrix.md`.

Updated 2026-09-06. The user authorized implementation changes, new experiments, and manuscript revision. The inherited dirty worktree remains intact; no commit or publication was made.

## Results

- **345 accepted trials**, five randomized blocks per configuration: controls 75, application 15, attribution 60, grouping 75, physical-host scaling 120.
- **Full ATLAS analysis:** 16 ROOT files, 9,861,498,743 bytes, 36,564,144 events, 802 tasks, all seven cut counts and 60 histogram bins exact on the measured platform. DV fixed/elastic medians 49.02/48.16 s versus TV 63.51 s; median paired ratios 1.29/1.32, five wins in five blocks.
- **Scale:** nested 1/2/4/8 distinct physical hosts, eight CPU shares each. Fixed DV CPU chains have 2.98 median paired one-to-eight speedup. Hash-data chains plateau and TV remains competitive/faster. No exclusive-node claim.
- **Attribution:** close task-body CPU, lower measured worker-lifetime CPU for DV; explicit controller/client exclusion. 5,120 ordered traces. Cold deployment remains a significant weakness.
- **Validation:** 25/25 gates on the retained measured runtime; eager/deferred and generic serverless PASS; six complete ATLAS replays, three on each of two platforms, PASS against independently generated references. The later configurable service build passes protocol regression with one and 16 RPC/data-plane threads. The background bottleneck evidence retains six fixed-topology durable-output runs, remote fan-in records, and a matched coupled-versus-separated A/B under `results/bottleneck-scale/`.
- **Control/data plane:** current-build 256-task, 1 MiB-output runs hold one scheduling service fixed and raise Controller data workers from 1 to 16, improving median durable-output throughput from 26.9 to 63.7 tasks/s (2.37x); all 256 files and 268 MiB pass byte checks. Metadata RPC measurements expose the shared-state plateau.
- **Paper:** rewritten evaluation, abstract, introduction and limitations, new figures and generated tables. Current PDF is 10 total pages; the artifact auditor enforces the ten-body-page limit.

Read `results/upgrade-artifact-audit.json`, `results/upgrade-results.md`, `experiments/claims.md`, and `experiments/next-evidence.md`. The primary artifact audit passed; the additional alternate-platform numerical diagnostic is recorded separately.

## Implementation changes

Native groups now retain members deferred by library startup or credit limits; joins form group boundaries; cancellation and destruction release owned group references. A later group-member failure propagates to callers so they do not reuse a removed Worker. Grouped tasks waiting for predecessors are exempt from the dispatch-based function-start deadline.

Explicit missing or non-executable executor overrides now fail before service contact instead of silently selecting a different executable. The regression runner loads and verifies the checkout's Python extension, then records source and binary hashes.

The review patch is `provenance/upgrade-implementation.patch` (11 files, reverse-apply check PASS). Exact measured binaries and the measured implementation patch are retained under `provenance/measured-upgrade-v1/`; final repairs have separate regression evidence. Existing campaign files were not rewritten to look like current-build measurements.

## Reproduction

Use `scripts/run_upgrade_campaign.py` for fresh controls, application, attribution or scaling runs. It stages colocated dependencies and full ROOT inputs, verifies hashes, retains the exact remapped manifest and records excluded setup. Fresh runs use the current elastic DataVine policy. See README and `requirements-research.txt`. Use Python 3.10 for the retained research dependency tree; plotting may use the system Python.

`make paper` regenerates figures/numbers and compiles the PDF. `make audit` verifies the upgraded artifact without regenerating plot PDFs. `make audit-legacy` is the earlier audit, whose current-binary assumption intentionally does not apply after this upgrade.

## Diagnostics and exclusions

- The initial grouping implementation stranded tasks and had reference-lifetime errors; reproductions are retained.
- An early new harness generated its executor after service startup. Actual Worker content hashes exposed the silent fallback. Those campaigns are diagnostics only.
- All grouping trials using the incompatible branch start-recall policy are excluded, regardless of observed outcome. Corrected grouping uses a fresh paired campaign.
- The controls continuation retains complete blocks and reruns incomplete blocks in full on the new allocation. Scaling retains successful non-grouping runs and continues the original order on the same persistent pool.
- The original 222-trial kernel study and earlier manuscript remain separate historical evidence.
- The measured-build alternate-platform replay selected 553,458 events against the primary reference's 553,456 and was rejected. This is outside the primary scored comparison. The unchanged independent reference on the second platform exactly reproduced every cut and bin difference. A separate manifest generated from that reference then passed all three measured execution modes. The initial rejected trial remains unchanged; cross-platform bitwise equivalence is not claimed.

## Resources and remaining scope

The eight-host pool and broker have been stopped. Preserve unrelated held course jobs 251996.0 and 252002.0. The independent reference and final three-mode replay have completed. The final replay requested eight CPUs and 10,000 MB, reduced from the initial 16,384 MB request after the primary replay measured 2,442 MB; this is a correctness check outside the scored performance campaigns.

Prepared-node placement, exact total traffic/prepared-byte accounting, exclusive 16+ nodes, controller saturation attribution, and production collaboration readiness remain open. The artifact is much stronger than the earlier kernel-only draft, but these results do not establish a guaranteed top-tier acceptance.

Diagnostic paths containing `intel` retain an initial naming mistake. The alternate platform is recorded as AMD EPYC 9334; the observed numerical difference is not attributed to CPU vendor.

Final closure: manuscript rebuild and artifact audit PASS (including both platform replays); no owned campaign jobs or pool/broker processes remain. The two unrelated held course jobs are preserved.
