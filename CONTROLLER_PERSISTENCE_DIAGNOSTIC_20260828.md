# Controller persistence diagnosis

Date: 2026-08-28

## Result

The measured remote requested-output ceiling is explained by the existing 16
data threads spending most of their active time in `fsync`, followed by a full
SHA-256 reread of every newly durable file. Queue admission, rename, close,
journal commit, Scheduler and metadata RPC are not material limits.

No architecture or production policy changed. Detailed counters are disabled
by default and activate only with `DATAVINE_PERSISTENCE_DIAGNOSTICS=1`.
Persistence still uses the same queue, 16 threads, pull connection, stream,
`fsync`, close, atomic rename, SHA-256 verification and journal/install commit.

## Instrumentation boundary

Each data thread owns a cache-line-aligned counter block. The diagnostic times:

1. enqueue blocking and queue residence;
2. Worker connection-lock wait;
3. connect/request/header and temporary-file open;
4. `link_stream_to_fd`, which combines socket read and local file write;
5. `fsync`, close and rename separately;
6. the post-write SHA-256 file reread;
7. prepared-to-commit wait and journal/install commit.

The initial shared-counter implementation reduced throughput from 547 to about
355 MiB/s because of false sharing. It was rejected. With cache-line-aligned
per-thread counters, the 1-MiB probe-on run reached 548.0 MiB/s versus 547.0
MiB/s probe-off, a 0.18% difference inside noise.

## Complete 1-MiB accounting

The validated run used 4,096 one-MiB outputs, 16 remote Workers and exactly 4
GiB total data. All outputs and bytes were durable, with no failure or retry.

| Active stage | Thread-seconds | Share |
|---|---:|---:|
| `fsync` | 60.340 | 50.56% |
| SHA-256 durable-file reread | 26.071 | 21.84% |
| Worker connection wait | 13.984 | 11.72% |
| socket-read + file-write stream | 12.981 | 10.88% |
| prepared-to-commit wait | 3.269 | 2.74% |
| connect/request/header/open | 1.800 | 1.51% |
| close | 0.438 | 0.37% |
| rename | 0.381 | 0.32% |
| journal/install commit | 0.089 | 0.07% |

The stages total 119.353 thread-seconds. Dividing by 16 gives 7.460 seconds,
versus the measured 7.475-second service window: 99.8% of elapsed time is
accounted for. The data threads are balanced and continuously occupied; there
is no missing Controller stage large enough to explain the ceiling.

The queue peaked at 3,501 jobs, but enqueue blocking totaled only 0.478 ms.
This is a backlog, not queue-processing overhead: tasks produce outputs faster
than the 16 persistence threads can drain them. At larger workflow scale the
4,096-job queue limit becomes the existing backpressure boundary.

## Larger-file behavior

Two 16-MiB diagnostic runs independently attributed 73.27% and 73.35% of
active time to `fsync`; accounted stages covered 97.9% and 97.8% of their
service windows. Stream copy contributed about 6.8%, verification about 13.3%,
and connection wait about 6.3%. The repeated stage distribution confirms that
durable flush becomes more dominant as each individual file grows.

The absolute 16-MiB diagnostic rates were 401.0 and 412.9 MiB/s, while an
adjacent probe-off run reached 581.1 MiB/s. The probe can perturb the timing of
large concurrent `fsync` calls, or the local filesystem can vary sharply
between runs. Therefore the stage proportions are diagnostic evidence, but the
probe-off result remains the production-throughput measurement. The tool does
not silently claim otherwise.

The 64-MiB point was excluded because Condor did not admit the exact 16-Worker
inventory. No partial-inventory result was counted.

## Persistence contract and fault gate

The [production contract](DATAVINE_PRODUCTION.md) owns the required `fsync`,
digest verification and atomic-rename semantics. Removing `fsync` would change
failure behavior; this campaign did not authorize that policy change.

The deterministic Linux `LD_PRELOAD` gate forces ENOSPC/EIO at private result
file `fsync`, verifies no phantom descriptor or leaked temporary file, then
restarts the same journal and commits exactly one result. A second restart
fetches it without a Worker. Production has no fault-injection branch.
Evidence: [persistence faults](acceptance/controller-persistence-faults-20260828.json).

## Follow-up optimization experiments

Temporary `/tmp` microbenchmarks used 16 threads, 4 GiB per point and the same
write -> `fsync` -> verify shape where applicable. They are directional local
experiments, not substitutes for the remote end-to-end measurement above.

- `posix_fallocate` before each write did not reduce `fsync`. In a randomized
  50/50 mixed run it increased aggregate active thread time by 2.1% for 1-MiB
  files and 3.5% for 16-MiB files. Reject it as a throughput optimization; it
  remains useful only if early capacity reservation becomes a separate policy.
- Replacing the durable-file reread with SHA-256 computed during the write was
  effectively neutral for 1-MiB files (-0.7% active time). Three randomized
  16-MiB comparisons changed active time by +5.6%, +2.5% and -1.5%; the median
  was a 2.5% regression. The reread normally hits page cache, while SHA-256
  computation remains mandatory and merely moves into the stream loop. There
  is no measured basis to change the production path.
- `fdatasync` was indistinguishable from `fsync` within run variability. Newly
  grown files still require size metadata for subsequent reads.
- Issuing `sync_file_range(..., SYNC_FILE_RANGE_WRITE)` every 4 MiB and retaining
  the final `fsync` moved waiting into the write loop instead of overlapping it.
  Two 16-MiB mixed comparisons increased active time by 14.8% and 2.1%.

The 1-MiB Amdahl ceilings also bound expectations: eliminating the entire
combined network/write stage would yield at most 1.12x, and eliminating all
verification would yield 1.28x, but the required SHA-256 work cannot actually
be eliminated. Only removing the 50.6% `fsync` share offers a theoretical 2x;
doing so changes the failure contract. At 16 MiB, where `fsync` is about 73.3%,
the same semantic change has a theoretical ceiling near 3.75x.

## Useful implications

- Adding threads is the wrong next move. Previous 32/64-Worker runs regress,
  and this campaign shows the existing 16 threads already cover essentially
  the entire service window with useful work.
- Relaxing or grouping `fsync` could improve speed, but would change the
  requested-output durability contract. That is not an acceptable transparent
  optimization.
- Streaming SHA-256, `fdatasync`, preallocation and pipelined writeback have no
  demonstrated win under matched local contention. Keep the simpler current
  path unless a remote production A/B proves otherwise.
- One pull connection per Worker explains 6-12% connection wait when four
  output-producing tasks complete together on each Worker. It is secondary to
  durability and does not justify adding a connection pool yet.
- `link_stream_to_fd` intentionally remains untouched, so this diagnostic
  cannot separate socket read from local write inside the stream stage. A future
  investigation needs a measured reason to split that stage further.

Machine-readable evidence is
`acceptance/controller-persistence-diagnostic-20260828.json`; fault-gate
evidence is `acceptance/controller-persistence-faults-20260828.json`.
