# DataVine 32x16 storage-path benchmark

Date: 2026-09-01  
Runtime: `production-baseline` at `c2e9a85be3e52d2b7f0210487a6632b8c03a413e`  
Status: **PASS with HTCondor admission noise**

## Executive result

The Controller is efficient for metadata-heavy workloads, while pure peer
transfer is decisively best for bulk movement. Worker-direct SharedFS occupies
the middle only for large sequential files: it avoids the Controller's extra
network-to-local-disk hop, but its centralized metadata path performs poorly
for empty files.

| Workload | Controller network + 16 threads + `/tmp` | Worker-direct SharedFS, 512-way | Pure peer | Winner |
|---|---:|---:|---:|---|
| 50,000 empty files | **11,342 files/s** | 1,480 files/s | 3,119 files/s | Controller |
| 4096 x 8 MiB, 32 GiB | 462 MiB/s | 822 MiB/s | **6095 MiB/s** | Peer |

Values are medians of three complete rounds. Every valid round used exactly 32
Workers x 16 cores, completed exact task/file/byte counts, and had zero task
failures.

The main ratios are:

- Empty-file Controller throughput is 7.66x SharedFS and 3.64x peer.
- Large-file SharedFS throughput is 1.78x Controller.
- Large-file peer throughput is 13.20x Controller and 7.41x SharedFS.

![32x16 storage-path matrix](storage_matrix_32x16.png)

## Experimental contract

All paths used 512 available Worker cores and ran sequentially so campaigns did
not compete with each other.

### Metadata workload

- 50,000 independent zero-byte files per round.
- Three rounds per path.
- The rate is end-to-end completed files divided by the measured path window.

### Big-data workload

- 4096 independent files x 8 MiB = exactly 32 GiB per round.
- Three rounds per path, or 96 GiB of measured payload per path.
- Every durable path performs per-file `fsync` followed by atomic rename. It
  does not claim directory-fsync or host-crash durability.

### Paths

1. **Controller `/tmp`:** 32 producer Workers x 16 cores produce requested
   outputs. Payloads traverse the network to the Data Controller's fixed 16
   data threads and are retained under Controller-local XFS `/tmp`.
2. **Worker-direct SharedFS:** 32 Workers x 16 cores run up to 512 independent
   writers against `/groups/dthain` NFS. There is no software semaphore or
   thread cap. A transferred acceptance helper performs create, write, fsync,
   and atomic rename, and each logical task ID is the unique final filename.
3. **Pure peer:** 16 producer Workers retain replicas while 16 disjoint,
   higher-memory consumer Workers fetch them. Producer Workers cannot execute
   consumer tasks. The measured window covers consumer append, resolution,
   peer transfer, validation, and completion; producer generation and
   Controller background-backup tail are outside it.

The paths intentionally test production-relevant end-to-end topology rather
than isolated `fio`. Their timer boundaries are recorded in every raw JSON.
Controller E2E metadata includes registration and has a 13,157 files/s median
after registration; SharedFS tasks are preloaded before its execution timer.

## Full measurements

### Empty-file metadata

| Path | Round 1 | Round 2 | Round 3 | Median | Range |
|---|---:|---:|---:|---:|---:|
| Controller `/tmp` | 12,302 | 8,824 | 11,342 | **11,342** | 8,824-12,302 files/s |
| SharedFS direct | 1,455 | 1,480 | 1,500 | **1,480** | 1,455-1,500 files/s |
| Peer | 3,119 | 1,030 | 3,234 | **3,119** | 1,030-3,234 files/s |

SharedFS is slow but extremely stable: its full range is only about 3.0% of
the median. Controller varies by about 31% of its median. Peer has a real slow
tail: in its second round, Controller-side completion processing consumed
31.64 seconds, including 31.26 seconds of logical transitions, versus roughly
0.3 seconds in the other two rounds. Thus the 1,030 files/s peer sample is not
a network-bandwidth failure; it is metadata/completion-path variance and is
correctly retained rather than discarded.

The result supports keeping empty and tiny data off SharedFS. Controller-local
metadata admission is the strongest path, and peer is useful but pays a
per-DataID request plus consumer-completion cost even when the payload is empty.

### Big-data movement

| Path | Round 1 | Round 2 | Round 3 | Median | Range |
|---|---:|---:|---:|---:|---:|
| Controller `/tmp` | 483 | 462 | 290 | **462** | 290-483 MiB/s |
| SharedFS direct | 1,070 | 822 | 765 | **822** | 765-1,070 MiB/s |
| Peer | 6,937 | 6,095 | 4,936 | **6,095** | 4,936-6,937 MiB/s |

The Controller's median is consistent with the previously observed single-host
durable drain around 0.48 GiB/s. Its third-round slow tail retained exact 32
GiB network and disk counters, so it is attributed to I/O or remote-producer
contention rather than lost work.

SharedFS is faster for these 8 MiB files because Workers write to storage in
one distributed hop. The Controller path must receive bytes at one frontend,
write them to one local device, verify them, and commit them. This large-file
advantage does not carry over to metadata: the same SharedFS is 7.66x slower
than Controller `/tmp` on empty files.

Peer scales across many Worker NICs and local disks. Its 6.10 GiB/s median is
an aggregate 16-to-16-Worker rate, not a claim about one network interface.
The Controller frontend transmitted only 4.65-5.09 MB during each 32 GiB peer
consumer round. That four-order-of-magnitude separation proves the payload did
not silently fall back through the Controller.

The peer workflow still waited for the default Controller backups before
starting consumers. The post-producer backup tail was 75.3, 111.8, and 89.4
seconds. Those times are not full backup durations because backup overlaps
producer execution, and they are not included in peer movement throughput.

## Controller assessment

The Controller is not a metadata bottleneck at this topology. Its conservative
E2E median is 11.3k empty files/s and its post-registration median is 13.2k/s.
For bytes, the implementation sustains a median 462 MiB/s while receiving and
durably writing 32 GiB. That is a healthy single-host fallback and resilience
path, but it cannot compete with distributed peer bandwidth.

Production policy should remain:

- Worker-local replica first.
- Peer transfer first for consumers.
- Asynchronous Controller backup and Controller source as resilience/fallback.
- Controller-local `/tmp` for requested durable outputs within the current
  Controller-host lifetime boundary.
- No automatic Worker-direct SharedFS production route merely because its
  large-file benchmark is faster; that would sacrifice the small-file result,
  central policy, and the existing failure boundary.

## Correctness and route gates

- 18/18 measured rounds passed.
- Controller: exact physical submissions/completions and exact durable files
  and bytes in all rounds.
- SharedFS: exact completed tasks, files, and bytes; zero failed tasks; all
  rounds observed 512 running tasks and 512 committed cores.
- Peer: exact consumer physical tasks, all backups present before consumers,
  exact 32-Worker inventory, route proof, and zero files after GC.
- Controller persistence diagnostics were deliberately disabled to avoid
  probe overhead. Exact filesystem counts, `/proc` I/O, NIC counters, service
  logs, and physical task identities remain in raw evidence.

Two invalid harness attempts were excluded before measurement completion. The
first detected three overwritten SharedFS filenames caused by non-deterministic
temporary suffix reuse; final filenames now use unique logical task IDs. The
second detected a transient 48-Worker over-admission from two independent
Factories; the final peer round uses one exact 16-proc Condor consumer cluster.
Neither invalid attempt produced an accepted result, and both payload trees and
jobs were cleaned.

## HTCondor admission noise

Campaign wall time was dominated by CRC HTCondor claim activation, not data
movement. Multiple worker waves matched slots and then received
`ACTIVATE_CLAIM ... REFUSED` before `vine_worker` started. Typical exact-pool
admission was either 15-17 seconds or about 311-321 seconds after a retry.

Admission is excluded from every table above and retained separately in raw
JSON/factory logs. This is essential: including it would turn a 5-second peer
transfer into a several-minute number that measures stale pool claims rather
than DataVine.

## Limitations

- This is one CRC topology and one frontend/local disk; it is not a universal
  filesystem or cluster claim.
- The three backends require different orchestration. The report exposes those
  timer boundaries and uses medians rather than presenting them as identical
  microbenchmarks.
- SharedFS direct is an acceptance baseline, not a production DataVine mode.
- `/tmp` durability does not survive Controller-host loss, reboot, or external
  cleanup.
- Empty-file results measure the complete metadata/task lifecycle, not raw
  `open(2)` throughput alone.

## Reproduction and evidence

Raw JSON and logs are in `raw/`. The two reusable harnesses are:

- `acceptance/scripts/benchmark_peer_vs_controller.py`
- `acceptance/scripts/benchmark_sharedfs_direct.py`
- `acceptance/helpers/storage_file_writer.c`
- `acceptance/scripts/render_storage_benchmarks.py`

Verify the retained campaign with:

```sh
cd acceptance/storage-matrix-20260901
sha256sum -c SHA256SUMS
```
