# DataVine dynamic control findings

These are the August 30–31 pilot results, not a fresh measurement of the
current checkout. Current execution frames and result semantics are defined in
[DATAVINE_PRODUCTION.md](DATAVINE_PRODUCTION.md).

## Workload and corrected cause

Each completed result determines one to three children. The depth-three graph
is discovered during execution: 477 tasks and 445 edges from 32 roots, with
5 ms useful CPU work per task. Both backends used two Condor Workers with four
cores, exact physical task counts and matching result traces.

The original DataVine median was 5.230 seconds. Each discovered task persisted
an approximately 250-byte Python invocation as an object: 478 object puts,
including one reusable callable, for about 123 KiB. Object-put time was
3.41–4.11 seconds, including 2.67–3.19 seconds in SharedFS operations. The
1,441 frontend RPCs were extra work, but did not explain most of that penalty.

Inlining small invocation metadata reduced object puts to one and object-put
time to about 9.5 ms. Two corrected remote runs took 1.2236 and 1.2246 seconds.
Their paired TaskVine runs took 2.364 and 4.339 seconds. These are pilot pairs,
not a general speedup or multi-node scaling claim.

The campaign used transitional execution frames. Those labels and the old
object fallback are not supported-adaptor guidance: the current implementation
uses DVP1 source, DVP3 inline callable and DVP4 Controller-RPC reference frames.

## Result stream

The Controller-admitted stream exposes only committed results, independently
of Scheduler completion. A dedicated authenticated connection resumes after a
monotonic sequence reconstructed from the data journal. Small committed
results can be inline; large ones remain descriptors. A terminal frame ends
the subscription. Protocol details belong in the
[adaptor guide](taskvine/examples/DATAVINE_WORKFLOW_ADAPTOR.md).

The 477-task stream run used 483 frontend requests. Remote stream and polling
runs both took about 1.224 seconds: inlining produced the wall-time improvement,
while streaming reduced request traffic. A separate local 10,000-empty-result
gate delivered the complete durable sequence and terminal state using five
client RPCs. Its 1,175.6 results/s measures persistence plus delivery, not the
native no-output scheduler ceiling.

## Evidence and remaining scope

- [Original pilot](acceptance/dynamic-adaptive-fixed-ab-20260830.json)
- [Correction and stream gates](acceptance/dynamic-control-root-fix-20260831.json)
- [Later restart and late-consumer gate](acceptance/dynamic-data-management-20260831.json)
- [Current DVP3/DVP4 and elastic campaign](acceptance/adaptive-window-20260903/REPORT.md)

The retained gates cover inline/descriptor results, disconnect resume, restart
replay and terminal delivery. Python serialization, executor startup and the
result-to-append critical path remain distinct from native scheduler throughput.
No static graph prediction or semantic task batching was used.
