# DataVine result-driven dynamic workflow pilot

Date: 2026-08-30

> Superseded diagnosis, 2026-08-31: the graph and old measurements below are
> valid, but the largest measured bottleneck was 477 per-task SharedFS object
> transactions for tiny Python invocation records, not the frontend RPC count.
> The root fix and corrected A/B evidence are in
> `DATAVINE_DYNAMIC_CONTROL_20260831.md`.

## Outcome

The dynamic workflow implementation is correct, but its current remote control
path is not yet competitive. Three alternating fixed-topology Condor pairs
discovered the same 477-task, 445-edge graph at runtime. TaskVine finished in a
median 2.808 seconds and DataVine in 5.230 seconds, so TaskVine was 1.86x faster
for this workload.

| Pair | Order | TaskVine | DataVine | DataVine / TaskVine |
|---:|---|---:|---:|---:|
| 1 | TaskVine, DataVine | 3.226 s | 5.637 s | 1.75x |
| 2 | DataVine, TaskVine | 2.808 s | 5.230 s | 1.86x |
| 3 | TaskVine, DataVine | 2.099 s | 4.656 s | 2.22x |

This does not contradict the static data-intensive 16.09x result. The static
case exposes centralized payload movement. This test deliberately puts a
fine-grained result-to-new-task decision on the critical path.

## Workload contract

The submit process starts with 32 roots and cannot know the final graph. Each
5 ms task returns a deterministic digest and fan-out of one to three. Only then
does the frontend create its children, which consume the completed parent
result. Depth-three leaves stop expansion. The run discovers 477 tasks and 445
edges; no task or edge is semantically batched.

All six formal runs produced the same graph trace
`e1d1352b01ec8989df2a2b761dfc56a3829eca1eb86ade033623b1dd244c2038`,
the same payload hash, exactly 477 physical submissions and completions, zero
failures, zero recoveries and about 2.38517 useful CPU-seconds. The useful CPU
spread across backends and repetitions was below 0.0005%.

Each backend used a fresh process and exactly two Condor Workers with four
cores, 2 GiB memory and 4 GiB disk each. No warmup workflow was allowed because
the production contract is one process per workflow; cold library/runtime cost
is included.

## Bottleneck

DataVine issued 1,441 frontend RPCs per 477-task run: 477 append requests, 477
result-descriptor requests, 483 event-watch requests and four setup/seal
requests. The local 1x4 gate completed in 1.582 seconds versus TaskVine's 2.856
seconds, but the same control loop with remote Workers took a median 5.230
seconds. This reversal isolates the problem to completion/result/append
coordination on a remote critical path, not task CPU or graph correctness.

The native runtime itself was not restarted once per delta: accepted runs used
only a handful of runtime invocations. Scheduler work and journal checkpoint
time were also small. The expensive shape is the frontend protocol:

1. observe task completion;
2. obtain the requested control result;
3. return to Python and decide fan-out;
4. send an append RPC;
5. wait for the new child to traverse the remote execution path.

The correct next optimization is not semantic batching or static graph
prediction. It is one blocking completion stream that delivers task identity
plus the compact control result, followed by one append transaction for that
parent. The existing durable watch journal can remain the recovery/audit path;
it should not require polling and a separate result lookup on every live
completion.

## Harness findings

The older dynamic test was only an eight-task sequential chain. Building the
adaptive gate exposed and corrected four measurement problems:

- TaskVine `futures.wait(FIRST_COMPLETED)` blocks on the first pending Future
  for one second; the gate consumes the Manager completion queue instead.
- the Python wait binding accepts integer-second timeouts, so it cannot express
  a low-latency polling workaround;
- the old comparison pool submitted a warmup workflow before the measured
  workflow, violating strict single-workflow ownership;
- DataVine's bounded event cursor must be consumed continuously while appends
  are generated, including while the initial roots are submitted.

Machine evidence is in
`acceptance/dynamic-adaptive-fixed-ab-20260830.json`. Raw summaries and logs are
under
`/groups/dthain/users/jzhou24/datavine-benchmarks/dynamic-adaptive-20260830/`.
This is a 2x4 pilot, not a large-scale production claim.
