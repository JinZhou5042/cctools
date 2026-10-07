# Controller remote fan-in characterization

Date: 2026-08-28

## Outcome

Simultaneous large requested outputs are the first practical Controller data
plane bottleneck. The campaign's 16-thread Controller sustained about 0.49-0.53
GiB/s from remote Workers into durable Controller-local `/tmp`. The useful
concurrency knee is 16 Workers. Raising fan-in to 32 or 64 Workers does not add
bandwidth; it reduces throughput and increases open descriptors.

This is separate from the previously measured small-file result. Empty-output
and 4-KiB tests stress task completion and filesystem metadata. This campaign
holds every requested-output run at exactly 4 GiB and therefore measures byte
movement under remote fan-in.

## Experiment

Each Condor Worker had 4 cores, 3 GiB memory, 10 GiB disk and no GPU. Every
command task generated zeros locally. Requested runs streamed every declared
output to the Controller, followed by `fsync` and atomic rename in `/tmp`.
Each result validated physical task identity, terminal state, durable file
count, durable bytes, Controller process counters and 10-Gbit `enp11s0f0`
interface counters.

| Payload | Tasks | Workers | Service time | Durable rate | Controller CPU | NIC receive | Peak FDs |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 MiB | 4,096 | 4 | 11.537 s | 355.0 MiB/s | 36.60 s | 4,286.9 MiB | 43 |
| 1 MiB | 4,096 | 16 | 7.546 s | 542.8 MiB/s | 35.68 s | 4,286.7 MiB | 79 |
| 1 MiB | 4,096 | 32 | 8.265 s | 495.6 MiB/s | 35.92 s | 4,288.4 MiB | 128 |
| 1 MiB | 4,096 | 64 | 9.248 s | 442.9 MiB/s | 37.31 s | 4,287.4 MiB | 220 |
| 16 MiB | 256 | 16 | 7.496 s | 546.4 MiB/s | 28.19 s | 4,298.7 MiB | 80 |
| 16 MiB | 256 | 32 | 8.392 s | 488.1 MiB/s | 28.69 s | 4,296.8 MiB | 128 |
| 16 MiB | 256 | 64 | 10.223 s | 400.6 MiB/s | 28.68 s | 4,301.1 MiB | 224 |
| 64 MiB | 64 | 16 | 8.010 s | 511.4 MiB/s | 24.28 s | 4,295.2 MiB | 80 |

The 1-MiB curve falls 8.7% at 32 Workers and 18.4% at 64 Workers relative to
16. The 16-MiB curve falls 10.7% and 26.7%. The same knee at two payload sizes,
plus the stable 16-Worker rate at 1, 16 and 64 MiB, establishes a byte-bandwidth
ceiling rather than a file-count ceiling.

## Fair Worker-local control

A separate A/B used the same 4,096 tasks, 16 Workers, command, and explicit
1-MiB `payload.bin`. Only retention policy changed.

| Policy | Service time | Controller RX | Controller write | Controller CPU | Durable files |
|---|---:|---:|---:|---:|---:|
| Worker-local, unrequested | 5.571 s | 1.44 MiB | 0.57 MiB | 0.59 s | 0 |
| Requested and durable | 8.214 s | 4,287.04 MiB | 4,097.14 MiB | 36.08 s | 4,096 |

Worker-local output therefore behaves as intended: data bytes do not cross the
Controller and do not enter its filesystem. Scheduler and ordinary task
execution complete 735.2 tasks/s in the local-retention control. The requested
case sustains 498.6 MiB/s with the same explicit-file executor; this agrees
with the stdout-based sweep closely enough to confirm the bottleneck.

## What is and is not saturated

- The Controller receives about 4.19 GiB on the NIC per 4-GiB payload run,
  roughly 5% framing and protocol overhead. At the best run this is about 4.6
  Gbit/s, less than half the active 10-Gbit link. The NIC is not the ceiling.
- Controller process CPU is 24-37 CPU-seconds over 7.5-10.2 wall-seconds, or
  roughly 3-5 fully occupied cores. CPU capacity is not exhausted.
- Earlier raw durable writes on this host reached 706-770 MiB/s. Integrated
  remote persistence reaches about 71-77% of that ceiling. The remaining cost
  is the pull protocol, copying, synchronization, per-file durability, and
  contention within the fixed data-thread path.
- Metadata publication already exceeds 80k one-record operations/s, far above
  the observed large-file rate. Scheduler throughput is also irrelevant once
  the workload is byte-bound.

The narrow classification is therefore **requested-output pull plus durable
local write**, not Controller metadata, Manager task transport, Scheduler, CPU,
or raw network capacity. Increasing the current 16 data pthreads would work
against the measured concurrency knee.

## Pure network ceiling

A follow-up removed DataVine, hashing and storage entirely. Temporary
warning-clean C senders in remote Condor jobs generated zero buffers in memory;
the frontend synchronized all TCP connections, discarded received bytes in
memory and validated exact byte counts. A local receiver guard reached 69.5
Gbit/s, so the receiver implementation was not the 10-gigabit limit.

| Streams | Total bytes | Seconds | TCP rate |
|---:|---:|---:|---:|
| 1 | 1 GiB | 0.914 | 9.4008 Gbit/s |
| 4 | 2 GiB | 1.825 | 9.4129 Gbit/s |
| 16 | 4 GiB | 3.651 | 9.4120 Gbit/s |
| 16 | 4 GiB | 3.651 | 9.4120 Gbit/s |
| 16 | 4 GiB | 3.651 | 9.4121 Gbit/s |

The empirical inbound ceiling is therefore 9.412 Gbit/s, or 1,122 MiB/s.
One stream already saturates the path. The best integrated DataVine run used
4.810 Gbit/s at the NIC, 51.1% of this measured usable ceiling, while delivering
546.4 MiB/s of durable payload. Network-only payload headroom is about 2.05x;
after that, the active 10-gigabit port becomes the next limit. The second Intel
X520 port exists but is down with no carrier and was not tested. Evidence is
`acceptance/controller-network-ceiling-20260828.json`.

## New issue found

The first “unrequested” command test used stdout rather than a declared output
file. DataVine correctly did not retain it, but generic TaskVine subsequently
retrieved the 4 GiB stdout stream into Manager: NIC receive was 4,286.7 MiB and
`manager_time_receive_good_us` was 5.958 seconds despite zero durable files.
The explicit-file control reduced NIC receive to 1.44 MiB.

This is narrowly scoped to DataVine command stdout that is neither requested
nor consumed. Consumer data still receives a RETAIN decision and follows the
normal Data Agent path. A future DataVine-only fix should suppress or discard
generic stdout retrieval after the Data Agent returns a no-retain decision;
ordinary TaskVine behavior must remain unchanged.

## Decisions

- Keep 16 Controller data threads.
- The explicit Worker-local control disables persistence; current default
  backup policy is defined in [the contract](DATAVINE_PRODUCTION.md).
- The measured capacity applies to this host and campaign, not every
  deployment. Requested-output drain time must be included when relevant.
- Queue-depth and persistence-stage timing is now complete. It attributes
  50.6% of 1-MiB active work to `fsync` and 21.8% to the post-write SHA-256
  reread; see `CONTROLLER_PERSISTENCE_DIAGNOSTIC_20260828.md`.
- Track the DataVine-only dead-stdout transfer in [the matrix](acceptance/matrix.md);
  this historical observation is not a fresh reproduction.

Machine-readable evidence is
`acceptance/controller-remote-fanin-20260828.json`. Some 4-Worker cases were
excluded after 300-second Condor admission timeouts; one redundant 32-Worker
64-MiB attempt was stopped during partial admission. They are not counted as
DataVine failures.
