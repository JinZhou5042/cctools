# DataVine paper package

Read [the handoff](HANDOFF.md), [the manuscript](build/paper.pdf),
[claim/evidence ledger](experiments/claims.md),
and [research upgrade](RESEARCH_UPGRADE.md). This directory owns the paper, LaTeX source,
figures, scripts, retained measurements, and provenance. `paper/` is the single canonical manuscript directory; do not
create conference-specific copies. Historical campaigns remain evidence for this
manuscript. It is an internal
research artifact; raw logs and paths are not anonymized for public release.

The paper's claim is **separating logical dispatch, physical data readiness,
and worker execution admission**, with explicit lifecycle and recovery ordering.
It does not claim the first data controller, peer transfer, overcommit, or an
implemented adaptive shared-filesystem routing optimizer.

## Contents

| Path | Purpose |
|---|---|
| `paper.tex`, `sections/`, `references.bib` | Complete IEEE conference manuscript |
| `build/paper.pdf` | Compiled paper; refresh after final measurements |
| `figures/` | Vector PDFs, preview PNGs, exact input hashes |
| `scripts/` | Trial/campaign, scientific kernels, plotting, acceptance and audit |
| `experiments/` | Job descriptions, claim/evidence requirements |
| `results/` | Raw accepted and failed trials, generated numbers and CSV |
| `research/positioning.md` | Detailed prior-art positioning; primary source links |
| `research/bibliography-metadata/` | Publisher-deposited metadata used to verify citations |
| `provenance/` | Starting dirty state, owned jobs and environment records |
| `vendor/` | IEEEtran class/style and original license/readme |
| `.deps/` | Ignored local Python dependencies; not a global installation |

## Build the paper

From this directory, with a Python containing NumPy and Matplotlib:

```sh
make paper
make audit
```

The Makefile regenerates all numerical text and figures before running
pdflatex, BibTeX, and two further LaTeX passes. `make audit` fails on missing or
incomplete main campaigns, mismatched executable fingerprints, stale figures,
undefined citations, and layout overflow. Exact historical measured binaries
are retained separately from the final repaired build. During an active upgrade
campaign use `python scripts/summarize_upgrade.py --preview` for a nonterminal check.
A successful artifact audit is not a claim of submission readiness.

Requirements: Python + NumPy + Matplotlib; pdflatex + bibtex; graphicx, booktabs,
amsmath and hyperref. IEEEtran is vendored. Measured kernels use NumPy 1.26.4.
The local DataVine interpreter is Python 3.10; plotting can use Python 3.11.
Do not mix their binary extension paths.

## Build and validate DataVine

From the repository root:

```sh
make -C taskvine/src -j8
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$PWD/test_support/python_modules/python3"
DATAVINE_PYTHON=/groups/dthain/users/jzhou24/miniconda/envs/datavine/bin/python
DATAVINE_GO_BINARY=/users/jzhou24/graph_optimization/factories/datavine_workflow_go-20260811 \
 DATAVINE_TEST_PYTHON="$DATAVINE_PYTHON" \
 DATAVINE_REGRESSION_REPORT="$PWD/paper/results/new-regression.json" \
 bash acceptance/scripts/run_regression.sh
```

The Go binary is a site-specific prerequisite of an existing acceptance gate;
see `acceptance/README.md` for using a Go compiler instead. Current architecture
and local verification are indexed in `DATAVINE_MAP.md` and
`acceptance/matrix.md`. The old eager/deferred diagnostic launcher is archived
under `provenance/legacy-worker-policy-launchers/`.

## Run new experiments

The retained paper measurements used these four campaigns, each with five
randomized blocks:

| Kind | Accepted trials | Scope |
|---|---:|---|
| controls | 75 | CPU chains, hash chains, CPU/I/O DAG; fixed, elastic, eager, TV, TV 4C |
| application | 15 | All 16 ATLAS ROOT files, 802 tasks; fixed, elastic, TV |
| attribution | 60 | Paired cold/preloaded and instrumented/uninstrumented controls |
| scaling | 120 | Nested 1/2/4/8 distinct physical hosts, eight CPU shares each |

Fresh campaign entry points use one DataVine elastic policy. Controls compare
it with stock TaskVine and the existing 4C baseline; application and scaling
compare it with stock TaskVine. Attribution retains instrumentation and
TaskVine preloading controls; DataVine always uses its installed executor.
Fixed/eager DataVine trials above describe the historical
measured build; current campaigns do not reproduce those variants.

Use a Python 3.10 compute allocation with at least eight available CPUs for
colocated experiments. The application needs at least 16 GB of memory and
space for a temporary copy of the 9.86-GB dataset. From the repository root:

```sh
"$DATAVINE_PYTHON" -m pip install --target paper/.deps/research-python3.10 \
 -r paper/requirements-research.txt
"$DATAVINE_PYTHON" paper/scripts/run_upgrade_campaign.py \
 --kind controls --output paper/results/new-controls
"$DATAVINE_PYTHON" paper/scripts/run_upgrade_campaign.py \
 --kind application --output paper/results/new-application
```

The same entry point accepts `--kind attribution`. It invokes
`research_campaign.py` and `research_trial.py` directly; the earlier deployment,
upgrade and staging wrappers are archived with the legacy launchers.
It stages dependencies, records excluded setup time, verifies dataset hashes,
and retains the exact application manifest. All scientific work through durable
sinks remains timed. `--repetitions 1` is a smoke check, not a paper campaign.

The control/data-plane separation can be reproduced with the current service
build by setting `DATAVINE_RPC_THREADS` and `DATAVINE_DATA_THREADS` before
`datavine_workflow serve`. The defaults are one RPC event thread and 16
Controller persistence workers. The current-build evidence in
`results/controller-decoupling-20260906.json` holds one single-threaded
scheduling service fixed, varies the data-worker count from 1 to 16, and checks
all durable bytes and protocol invariants. It also reports the metadata-RPC
plateau caused by shared Controller state; these settings are an experiment
control, not a claim of unlimited scaling.

The background bottleneck figure is generated by
`scripts/plot_bottleneck.py`. Its raw inputs are retained under
`results/bottleneck-scale/raw/` (two fixed-topology repetitions at 64, 256,
and 1,024 one-MiB durable outputs), plus the remote fan-in and matched
20,000-task A/B artifacts. Run `make -C paper figures` to rebuild
`figures/bottleneck-motivation.pdf` and its hashed summary. The figure is
service-pressure and coupling evidence; it does not claim that any Controller
configuration scales without a shared-state limit.

```sh
DATAVINE_RPC_THREADS=1 DATAVINE_DATA_THREADS=16 \
 taskvine/src/tools/datavine_workflow serve /tmp/datavine.journal TOKEN
```

The ATLAS manifest must point to existing local inputs. Its release is
`2025e-13tev-beta`; acquire the `data/GamGam` files through the referenced
ATLAS notebook's `atlasopenmagic` downloader, and verify the retained per-file
SHA256 hashes. The independent oracle source is retained in
`results/atlas-inputs-v1/oracle-source.py`.

The float32 reference is not bitwise identical across all platforms. The second
platform's independent reference reproduced 553,458 selected events instead of
the primary reference's 553,456, including all observed bin differences. On a new platform,
run the unchanged reference and derive a separate manifest before comparing:

```sh
"$DATAVINE_PYTHON" paper/scripts/stage_oracle_on_host.py \
 --output paper/results/new-platform-oracle.json
"$DATAVINE_PYTHON" paper/scripts/platform_atlas_manifest.py \
 --oracle paper/results/new-platform-oracle.json \
 --inputs paper/results/new-platform-oracle.inputs.json \
 --output paper/results/new-platform-inputs.json
"$DATAVINE_PYTHON" paper/scripts/run_upgrade_campaign.py \
 --kind application --manifest paper/results/new-platform-inputs.json \
 --output paper/results/new-platform-application
```

Run those commands within the same compute allocation. Runtime outputs never
become the oracle. The source and dataset hashes must match; all cut counts
and histogram bins remain exact requirements for every trial.
Some diagnostic filenames contain the initial label `intel`; their recorded
hardware is actually AMD EPYC 9334. Filenames were retained for provenance,
and no CPU-vendor explanation is inferred from this comparison.

For scaling, first establish a persistent pool using `scripts/node_pool.py` and
the site-specific `experiments/node-pool-v4.sub` template, with distinct hosts
and explicit CPU affinity. Then pass `--kind scaling --pool /absolute/contact.json`
to the entry point. The contact file contains a temporary control token and is
Git-ignored. The supplied measurements use CPU shares, not exclusive nodes.
Do not rebuild or edit fingerprinted inputs during a campaign. Use the retained
measured binary snapshot to reproduce an old measured version.

The archived `continue_controls.py` and `continue_scaling.py` in
`provenance/legacy-worker-policy-launchers/` document the interrupted
original allocation history. No failed
non-grouping sample was replaced selectively. Incompatible grouping trials are
excluded in their entirety, with a separate corrected paired campaign.

## Historical kernel campaigns

Create an unused output directory. The trial and campaign scripts never
silently overwrite a completed run. Install NumPy into an interpreter-specific
local directory if needed:

```sh
"$DATAVINE_PYTHON" -m pip install --target paper/.deps/python3.10 numpy==1.26.4
"$DATAVINE_PYTHON" paper/scripts/run_trial.py \
 --backend datavine --workload histogram --workers 2 --cores 4 \
 --width 32 --levels 4 --bytes 262144 --output paper/results/new-trial
"$DATAVINE_PYTHON" paper/scripts/run_campaign.py \
 --output paper/results/new-campaign --repetitions 3 \
 --workers 2,4 --cores 4 --batch-type condor
```

The configured repository must already be built; scripts explicitly select its
bindings and native binaries. Factories are found on the configured Python's
PATH. Remote nodes need access to the repository and configured Python paths.
`pinned_worker.py` enforces each worker's logical CPU capacity. Local workers
receive disjoint CPU sets; a local pool larger than available affinity fails.
Remote host placement remains opportunistic and may colocate workers.

`compute_campaign.sh` and `experiments/compute.sub` run the primary 96-trial
colocated study in an eight-core batch allocation, with the entire campaign
restricted to eight CPUs. Site-specific paths and result directory names are
explicit: change them before a new submission. The distributed study uses
`run_campaign.py --batch-type condor` from the submission host. Main campaigns
freeze the kernel, runner, wrapper and executable hashes; do not edit or
rebuild them while a campaign is running.

Additional experiments:

- `pressure_campaign.sh` / `experiments/pressure.sub`: 24 memory-pressure and
  eviction trials using the existing acceptance workload, pinned to two CPUs.
- `run_routes.py`: three paired peer/Controller source-isolation trials. This
  uses the existing route benchmark and validates delivered file lengths.
- `acceptance/scripts/benchmark_worker_churn.py`: worker removal at observed
  completion thresholds. Current validated command and result are in the ledger
  and `results/worker-churn-v3/summary.json`. The script removes only connected
  jobs belonging to its own factory; transaction logging is required.

All primary timings exclude full-pool admission and include graph submission,
sink fetch, and local sink fsync. DataVine additionally performs its Controller
requested-output admission. Intermediate backup is worker-local for matched
runtime comparisons. The separate route test uses admitted background backup.

## Interpreting versions and failures

V1 caught two harness errors and TaskVine's unsupported fork slot/core ratio.
V2 exposed a real dense-dispatch recall starvation defect. V3 checked the fix
but was stopped when the batch CPU configuration proved to be shares without
a hard capacity limit. **V4 is the primary campaign version**, with bounded
worker-pass dispatch and explicit CPU affinity. Earlier results are diagnostic
evidence, not extra repetitions for V4.

TV 1C is stock TaskVine. TV+ 2C/4C are explicitly extended controls: only the
startup guard in a private generated fork-library file is removed. Actual CPU
allocation remains bounded. This extension is not represented as an existing
supported stock feature, nor as DataVine-style adaptive admission.

Scientific content must match across variants. Error and interrupted runs stay
in `results/`; no missing run becomes a zero or is silently replaced. Figures
show medians, individual observations, and full ranges over three repetitions.
The original campaigns include per-call dependency imports. The separate
preloaded campaigns import an explicit common module list once per execution
library. Their timer follows worker connection, not an all-libraries-ready
barrier, so residual setup can remain inside the interval. Output serialization
and identical task telemetry are included for both runtimes. These are numerical miniapplications, not a full
HEP production analysis.

## Manuscript format

The current draft uses IEEE 10pt letter format, two columns, and a ten-body-page
limit including figures and tables, with references outside that limit. These
are the current drafting settings; a target venue is not assigned. Submission
or public posting has not occurred.

Before submission, resolve the explicit scientific-application, reserved-scale,
causal-attribution and closest-baseline gaps in `experiments/claims.md`, review
all authored claims, and prepare an anonymized artifact separately. Raw internal
provenance should not be attached unchanged.

## Scope record

Only DataVine-related source and documentation changes were intended and made.
An initial root build followed the old acceptance command and rebuilt some
unrelated ignored CCTools object files before being stopped. No unrelated source
was edited or deleted; subsequent builds were restricted to TaskVine. The
acceptance build command now reflects that scope. The starting dirty checkout
is retained in provenance and must not be reset.

## Historical controlled dependency deployment

The replacement-executor generation and launch scripts are archived in
`provenance/legacy-executor-launchers/`. They used an executor override in the
measured build; that override has been removed from the current runtime.
Their campaigns retain the original randomized order and fingerprints.

- `experiments/preloaded.submit`: 48 scientific trials, results/preloaded-campaign-v1.
- `experiments/preloaded-data.submit`: 15 larger hash-data trials, results/preloaded-data-v1.
- `scripts/diagnose_process_waits.py`: owned-process state sampling used to find
  NFS open/RPC waits. Diagnostic overhead is excluded from headline campaigns.

Cold and preloaded campaigns used different allocations, including different
CPU models. Compare runtimes within each campaign; do not describe their ratio
as a paired estimate of the preload effect. The preloaded small-DAG comparisons
favor DataVine, but fixed DataVine windows often match its elastic window. This
limits claims about the adaptive policy's unique contribution.

The scoped review patch is `provenance/this-work-source.patch`, reconstructed
against the starting dirty files rather than HEAD. `scripts/capture_scope.py`
checks their original hashes before producing it. `experiments/next-evidence.md`
defines the remaining application/scale/attribution gates for submission.

## Directory migration

The paper package now lives directly in `paper/`. Historical records in
`results/` and `provenance/` retain the original `papers/ipdps2027/` paths and
source hashes as captured. When locating their files, map that old prefix to
`paper/`; do not rewrite the retained measurements or fingerprints.

## Current scope after removing task-group changes

DataVine code, tests, and experiment launchers do not enable or modify TaskVine
task groups. The local group implementation changes and two regression tests
were removed. Fresh campaigns cover controls, application, attribution, and
scaling. Historical group measurements and source patches in `results/` and
`provenance/` are retained records only. The existing PDF and recorded audits
precede this scope change and do not validate the current checkout.

## Unified Poncho executor

The current runtime uses one installed `datavine_executor` and the existing
TaskVine fork-library mechanism inside the Poncho environment. Per-task
environments and executor-path overrides are unsupported. Legacy replacement-
executor launchers are retained under `provenance/legacy-executor-launchers/`
for historical records only. Their preloading results and the existing PDF
predate this change; they do not establish current-build performance.

Worker admission now has one elastic policy and preparation waits for an
execution opportunity. Legacy fixed/eager launchers and interrupted-campaign
continuations live in `provenance/legacy-worker-policy-launchers/`. Raw results,
plots and recorded hashes remain historical evidence.
