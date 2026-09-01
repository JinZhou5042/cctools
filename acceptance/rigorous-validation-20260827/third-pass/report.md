# DataVine third-pass findings

Date: 2026-08-27  
Status: **PASS**

## What changed

The single RPC owner remains unchanged. Each metadata record remains one
request and one response; no task or RPC batching was introduced.

1. A request handler now attempts its small nonblocking response immediately.
   It arms `EPOLLOUT` only when `send` actually returns `EAGAIN`. This removes
   the normal extra epoll round trip and two `epoll_ctl` calls.
2. Idle/terminal connection maintenance is rate-limited to the existing 10 ms
   event-loop resolution instead of scanning every connection after every busy
   epoll turn.
3. The Controller benchmark defaults to process clients so Python's GIL cannot
   masquerade as a Controller limit.
4. Notebook `/proc` inspection now treats any process-exit `OSError` as the
   expected race, matching the benchmark sampler.

## Before and after

The original evidence used one thread-driven run per topology. Final values are
three-run means, 102,400 records per phase and one record per request.

| Driver | Connections | Phase | Original | Final | Service CPU change |
|---|---:|---|---:|---:|---:|
| Python threads | 1 | publish | 19,154/s | 21,328/s | -22.2% |
| Python threads | 1 | resolve | 17,034/s | 19,432/s | -18.2% |
| Python threads | 64 | publish | 19,847/s | 20,059/s | -27.6% |
| Python threads | 64 | resolve | 15,660/s | 15,430/s | -22.3% |

The CPU reduction is the runtime gain. Multi-thread wall rates remain near
20k/s because Python threads serialize the request driver through the GIL.

## Actual single-thread Controller capacity

Independent process clients expose the Controller rather than the driver:

| Connections | Publish mean | Resolve mean | Publish CV |
|---:|---:|---:|---:|
| 16 | 94,169 records/s | 81,389 records/s | 3.0% |
| 64 | 101,068 records/s | 83,954 records/s | 4.1% |
| 128 | 98,695 records/s | 87,330 records/s | 10.4% |

At 64 connections the service uses about 6.87 us CPU per publication and
7.94 us per resolve. Publication stops improving at 128 and becomes less
stable, so roughly 64 active connections is the balanced saturation point on
this 64-core host. The former 20k/s figure was a benchmark-client ceiling, not
a DataVine Controller ceiling.

## Data-path guard

Three exact 2,000 x 4 KiB requested-output runs completed with one physical
TaskVine task per logical task, 2,000 durable files and 8,192,000 bytes each.
The Controller service mean was 1,199.0 files/s versus 1,203.8 files/s in the
previous round (-0.4%). The RPC changes therefore preserve the measured small
file path.

The final affected build was warning-clean. Focused Notebook and Shell tests
passed, followed by the full 18/18 regression.
