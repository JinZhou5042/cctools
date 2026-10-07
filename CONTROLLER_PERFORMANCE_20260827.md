# DataVine Controller Performance, 2026-08-27

This campaign freezes Scheduler work and measures the campaign's single-workflow
Data Controller. The durable machine-readable record is
`acceptance/controller-comprehensive-20260827.json`.

## Result

For the recorded workload, ordinary one-replica metadata was faster than
the approximately 20k task/s execution path measured at that time. Direct C metadata operations
remain above 8.4M/s at ten million records. One-record TCP RPC reaches 111.5k
publish/s and 85.4k resolve/s. The practical bottleneck is requested-output
persistence, not the metadata catalog.

At ten million data IDs, one replica and one waiter, the complete Controller
lifecycle takes 13.48 seconds and peaks at 2.28 GiB RSS. Random resolution falls
from 5.13M/s at 100k records to 1.97M/s at ten million records; this is the first
clear cache-capacity effect.

Four replicas per data ID preserve approximately 9.1M publish/s, but reduce
random resolve at one million IDs from 2.82M/s to 0.85M/s. Replica fan-out is
therefore a real scaling dimension, although the production policy normally
uses one replica unless recovery or reuse requires more.

## RPC latency

Every request in this test carries exactly one record. Independent client
processes avoid the Python GIL, and every sixteenth RPC is timed.

| Connections | Publish/s | Publish p99 | Resolve/s | Resolve p99 |
|---:|---:|---:|---:|---:|
| 1 | 20,795 | 50.9 us | 17,942 | 63.1 us |
| 16 | 87,132 | 294.3 us | 81,931 | 313.5 us |
| 64 | 105,321 | 1.31 ms | 83,752 | 1.46 ms |
| 128 | 111,542 | 2.18 ms | 85,445 | 1.65 ms |

Throughput is nearly saturated by 16 to 64 connections. Higher concurrency
mainly increases queueing latency. Sixteen active RPC connections are the best
latency-throughput operating point; 64 is useful when maximum aggregate
throughput matters more than tail latency.

## Persistence

All integrated persistence tests use Controller-local `/tmp`, individual files,
fsync and atomic rename. They do not batch logical outputs.

| Payload | Best tested topology | Integrated rate |
|---|---:|---:|
| empty | 16 Workers | 9,667 files/s |
| 4 KiB | 16 Workers | 4,848 files/s |
| 1 MiB | 4 Workers | 586 MiB/s three-run mean |
| 64 MiB | 16 Workers | 411 MiB/s |

Sixty-four Workers reduce small-file throughput and raise peak process RSS from
about 244 MiB at 16 Workers to about 804 MiB, with 1,690 observed file
descriptors. There is no reason to increase producer concurrency that far.

The fresh raw `/tmp` direct-write baseline reaches 706 to 770 MiB/s for 64 MiB
files. In the same-host integrated test, bytes are first written to Worker-local
storage and then written durably by the Controller. The expected two-write
limit, approximately 770/2 MiB/s, closely matches the observed 411 MiB/s after
cache and timing effects. Disk write bandwidth is the dominant large-file
bottleneck.

## Worker-local boundary

The requested-output optimization behaves correctly. With 16 Workers:

- 1 MiB unrequested outputs generate at 765 MiB/s and create zero durable files;
- 64 MiB unrequested outputs generate at 1.21 GiB/s and create zero durable files;
- requesting the 64 MiB outputs reduces useful throughput to 411 MiB/s because
  the Controller correctly performs the second durable write.

Late consumers, late and in-flight request promotion, volatile-loss replay,
Worker loss, owner restart, checkpoint resume, retries, deduplication and replica
invariants all pass their focused gates.

## Decision and subsequent closure

The campaign supports retaining 16 Controller data threads and does not justify
a metadata redesign for its normal one-replica workload. It predates the default
background-backup policy; the Worker-local controls above explicitly measure
volatile output behavior, not today's default retention behavior.

The two original follow-ups are complete: per-thread persistence counters and
deterministic ENOSPC/EIO injection. Their evidence is maintained in
[the persistence diagnosis](CONTROLLER_PERSISTENCE_DIAGNOSTIC_20260828.md).
[The acceptance matrix](acceptance/matrix.md) owns remaining open gates.
