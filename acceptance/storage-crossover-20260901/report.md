# DataVine dense storage crossover

Updated: 2026-09-02  
Topology: 32 Workers x 16 cores, 512 available cores  
Status: **PASS; no single monotonic crossover exists**

## Answer

The dense experiment rejects a magic file-size threshold. It measured every
32-KiB step from 32 through 1024 KiB, five randomized samples per size and per
path. The median winner changed **seven times**, at 512, 544, 608, 640, 672,
800 and 832 KiB. A real threshold would yield one stable transition, not seven.

Only three sizes were unanimous SharedFS wins in all five paired samples (128,
320 and 384 KiB), and only one was a unanimous Controller win (832 KiB).
Adjacent sizes frequently reversed because storage state dominated the 32-KiB
increment. Representative medians are:

| Size | Controller `/tmp` | Worker-direct SharedFS | SharedFS paired wins | Median winner |
|---:|---:|---:|---:|---|
| 32 KiB | 1,746 files/s | **2,613 files/s** | 4/5 | SharedFS |
| 128 KiB | 1,025 files/s | **2,533 files/s** | 5/5 | SharedFS |
| 256 KiB | 682 files/s | **2,258 files/s** | 4/5 | SharedFS |
| 384 KiB | 541 files/s | **1,784 files/s** | 5/5 | SharedFS |
| 480 KiB | 406 files/s | **1,255 files/s** | 3/5 | SharedFS |
| 512 KiB | **371 files/s** | 248 files/s | 1/5 | Controller |
| 640 KiB | 342 files/s | **1,076 files/s** | 3/5 | SharedFS |
| 832 KiB | **306 files/s** | 236 files/s | 0/5 | Controller |
| 1024 KiB | **327 files/s** | 248 files/s | 3/5 | Controller |

The paired-win column compares the corresponding campaign and round; the two
paths were run sequentially, not simultaneously. The complete arrays, ranges
and ratios are in `dense-summary.json`.

![Dense crossover: every point, ranges and paired ratios](small_file_crossover_dense_32x16.png)

## Scale and correctness

Two independently seeded campaigns contributed three plus two rounds. Each
path completed 160 phases, 655,360 tasks/files and exactly 354,334,801,920
payload bytes (330 GiB). Across both paths this is 320 phases, 1,310,720 files
and 660 GiB. Both campaigns admitted the exact 32x16 topology; every phase had
zero failed tasks and verified its exact file and byte count. SharedFS phase
samples observed 492--512 peak running tasks and
511--512 peak committed cores; the smallest phases can complete before one
instantaneous stats sample sees all 512 tasks.

Controller measurements also verified 655,360 physical submissions and
completions, one exact physical epoch per phase, Worker-facing `enp11s0f0`
traffic, durable file counts and a fetched sample result. Controller timers
include the small workflow-delta append; SharedFS timers start after task
submission. Factory admission is outside both payload timers.

The size order was independently randomized inside every round. This prevents
one monotonic size sweep from confusing elapsed time with size, while retaining
the real state of a long workflow.

Both accepted SharedFS commands emitted one post-PASS teardown warning because
the Python Manager finalized after its temporary monitor directory disappeared.
It occurred after result JSON and payload cleanup and did not affect a phase.
The harness now finalizes the Manager before temporary-directory cleanup; a
focused local smoke passed without the warning.

## The dominant effects are not file size

Controller requested results remain live for the workflow, so the durable
population grew to 198 GiB in the three-round campaign and 132 GiB in the
independent confirmation campaign. After normalizing every observation by its
same-size median, throughput still correlated -0.559 with execution order.
Aggregate rates fell from 615 to 495 to 424 files/s over the first campaign's
three rounds and from 441 to 380 files/s in the confirmation campaign. The
experiment demonstrates a real long-workflow state effect; it does not by
itself assign all of that effect to one kernel or filesystem mechanism.

SharedFS did not show the same monotonic drift (normalized order correlation
0.112), but it repeatedly switched between fast and slow modes. Across the
dense phases it ranged from 126 to 2,704 files/s. For example, 896 KiB reached
only 126 files/s in one confirmation sample, while the adjacent 928-KiB phase
reached 1,045 files/s. File size cannot explain that reversal. Concurrent NFS
load, completion/fsync waves and server-side state are plausible contributors,
but the campaign measures their combined path rather than claiming a unique
server-side root cause.

![Size-normalized execution-order stability](storage_crossover_phase_stability_32x16.png)

## Decision

Do not implement size-based production routing. Peer transfer remains the
scalable active byte path; Controller `/tmp` remains the requested-result and
fallback path; Worker-direct SharedFS remains a comparison backend. A useful
future policy would need live path-state signals and queue/occupancy feedback,
not a constant number of bytes.

## Dense-campaign limitations

- The uniform primary grid begins at 32 KiB. The separate empty-file matrix
  favors Controller `/tmp`, but it has a different file count and timer and is
  not interpolated into a claimed 0--32-KiB crossing. This does not restore a
  global threshold because the dense grid itself reverses seven times.
- Controller requested results correctly remain durable until workflow seal;
  SharedFS phase directories are verified and removed after each phase. The
  campaign therefore compares the real lifecycle of these two paths, including
  Controller accumulation, rather than pretending to be a symmetric raw-device
  microbenchmark.
- The paths use the same resource request and randomized phase plans but run
  sequentially, so unrelated SharedFS load is not controlled. Five samples per
  size expose this variability but do not turn it into a confidence guarantee.
- Results are specific to this frontend's XFS `/tmp`, the CRC NFS mount and the
  recorded 32x16 Worker placement.

The preliminary sparse campaign below estimated a 192--256 KiB band and a
202.1-KiB calibrated affine crossing. That result was internally consistent
for its short independent runs, but the dense long-workflow data supersedes it
as a routing conclusion. The model omitted the two effects now directly
observed: Controller state accumulation and SharedFS mode switching.

## Preliminary sparse campaign (superseded for routing)

The following three-point measurements and model remain for provenance. They
must not be presented as the current crossover conclusion.

### Repeated measurements

### 128 KiB

| Path | Round 1 | Round 2 | Round 3 | Median | Range |
|---|---:|---:|---:|---:|---:|
| Controller `/tmp` | 2,733 | 3,387 | 2,237 | **2,733** | 2,237--3,387 files/s |
| SharedFS | 2,514 | 2,543 | 2,153 | **2,514** | 2,153--2,543 files/s |

The ranges overlap. Controller won two of the three corresponding path
samples and its median advantage was only 8.7%, so 128 KiB is not a robust
hard-routing boundary.

### 192 KiB

| Path | Round 1 | Round 2 | Round 3 | Median | Range |
|---|---:|---:|---:|---:|---:|
| Controller `/tmp` | 1,495 | 3,440 | 3,900 | **3,440** | 1,495--3,900 files/s |
| SharedFS | 2,481 | 2,511 | 2,363 | **2,481** | 2,363--2,511 files/s |

Controller won the median by 38.7%, but its slow round lost to every SharedFS
round. This is direct evidence that file size alone does not determine the
winner.

### 256 KiB

| Path | Round 1 | Round 2 | Round 3 | Median | Range |
|---|---:|---:|---:|---:|---:|
| Controller `/tmp` | 1,469 | 1,456 | 1,460 | **1,460** | 1,456--1,469 files/s |
| SharedFS | 2,293 | 1,863 | 499 | **1,863** | 499--2,293 files/s |

SharedFS won the median by 27.7%, but its slow tail was 2.92x slower than the
Controller's worst round. A separate 160-KiB pilot showed the same kind of
SharedFS tail at 493 files/s. Those samples are retained, not discarded.

### Theory and experiment

Use the affine mean-time model

\[
T(s) = a + b s,
\]

where `a` is the per-file metadata, scheduling and durability cost and `b` is
the marginal transfer/write cost per byte. The empty-file medians give fixed
costs of 88.17 microseconds/file for Controller and 675.68 microseconds/file
for SharedFS. This correctly predicts that Controller wins for sufficiently
small files.

Using the 8-MiB aggregate bandwidth as `b` predicts a 633.8-KiB crossover. That
initial estimate is quantitatively wrong because it assumes the asymptotic
large-file regime extends linearly through small and medium files. It ignores
the earlier Controller persistence knee and how 512 distributed SharedFS
writers amortize fixed metadata cost.

Calibrating `b` with the empty and 512-KiB endpoints instead gives 2.958
ns/byte for Controller and 0.119 ns/byte for SharedFS. Solving
`T_controller(s) = T_sharedfs(s)` yields 206,959 bytes, or **202.1 KiB**. That
prediction lies inside the independently observed 192--256 KiB crossover band.
Thus the physical model is qualitatively correct, the small-file-calibrated
model is quantitatively consistent with experiment, and the original
large-file extrapolation is not.

The 202.1-KiB intersection remains an expected value, not a routing constant:
the retained round ranges prove that external load and completion/persistence
waves can reverse the winner near the intersection.

### Why the sparse crossover appeared as a band

The two paths scale differently:

- Controller has one frontend network ingress, 16 persistence threads and one
  local XFS device. Its fixed metadata path is efficient, but bytes eventually
  concentrate on one host.
- SharedFS distributes writes from up to 512 Worker tasks, so its normal byte
  slope becomes better as files grow. It also shares a centralized NFS service
  with unrelated cluster users, producing load-dependent latency and severe
  occasional slow tails.
- Per-file `fsync`, task completion waves, Worker placement and concurrent
  external SharedFS traffic make the response non-linear. A threshold inferred
  from only the zero-byte and 8-MiB endpoints would have been wrong.

Thus 256 KiB is an empirical median crossover point for this campaign, not a
portable constant or correctness boundary.

### Timer and route validation

Controller rates conservatively include bounded workflow registration. Its
post-registration medians at 128, 192 and 256 KiB were 2,866, 3,654 and 1,498
files/s; using those values leaves the same 192--256 KiB crossover band.
SharedFS tasks were preloaded before its execution timer.

The initial crossover commands accidentally monitored `eno1` instead of the
Worker-facing `enp11s0f0`. This did not affect execution timing or exact durable
file verification. A separate 256-KiB route gate, excluded from the medians,
received 2,251,212,027 bytes on `enp11s0f0`, wrote 2,149,883,904 process bytes,
and verified exactly 8192 files totaling 2 GiB. Therefore the Controller result
really includes Worker-to-Controller network transfer and local persistence.

### Preliminary recommendation

Do not add static file-size routing to the production runtime from this result.
The current policy remains simpler and more resilient:

1. retain the Worker-local replica;
2. resolve consumers local-first and peer-first;
3. back up asynchronously to the Controller;
4. use Controller `/tmp` for requested durable outputs and fallback;
5. keep Worker-direct SharedFS as a comparison backend, not the default path.

If a future explicit Worker-direct SharedFS mode needs a tuning hint, use
256 KiB only as the beginning of a topology-specific experimental region, not
as a hard guarantee. A conservative operator would require live SharedFS load
feedback or a much larger size margin before routing away from Controller/peer.

### Preliminary evidence boundaries

- 18/18 primary repeated rounds passed.
- Four single-round anchor measurements passed at 160 and 512 KiB.
- One excluded route-validation round passed with the correct NIC counter.
- Zero failed tasks; exact file and byte counts in every accepted round.
- Admission delays of roughly 15 or 310--318 seconds are excluded from payload
  timing and retained in raw JSON.
- The SharedFS payload directory was empty after every round.

All JSON results are under `raw/results/`; supporting diagnostics are under
`raw/logs/`. `summary.json` describes the preliminary sparse campaign and
`dense-summary.json` is the accepted dense machine summary. Verify retained
evidence with `sha256sum -c SHA256SUMS` from this directory.
