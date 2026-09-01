# DataVine dynamic control-plane root fix

Date: 2026-08-31

Status: implemented and acceptance-tested in the current dirty checkout.
Release provenance is still open.

## Result

The 477-task result-driven Condor workflow fell from the previous DataVine
median of 5.230 seconds to 1.224 seconds in two new runs. The exact graph,
result trace and task count did not change. The new DataVine rate was 389.5 to
389.8 tasks/s. The paired TaskVine runs took 2.364 and 4.339 seconds, so
DataVine was 1.93x to 3.54x faster in these pairs instead of 1.86x slower in
the previous campaign.

This is a root data-path correction, not a larger thread pool or a polling
interval patch.

## Corrected diagnosis

The earlier report correctly identified an inefficient frontend protocol, but
it overstated that protocol as the largest measured wall-time bottleneck. The
decisive evidence was already present in the adaptor profile:

| Old Condor run | Total | Object-put wall | SharedFS portion |
|---|---:|---:|---:|
| pair 1 | 5.637 s | 4.114 s | 3.189 s |
| pair 2 | 5.230 s | 3.999 s | 3.185 s |
| pair 3 | 4.656 s | 3.408 s | 2.673 s |

Every dynamically discovered task created a unique Python invocation of only
about 250 bytes. The adaptor nevertheless treated it as ordinary immutable
data and performed a full Controller object transaction: hash, RPC, SharedFS
directory lookup/create, file write, `fsync`, link/rename and later Worker
read. This happened 477 times. Together with the single reusable function
object there were 478 object puts for only about 123 KiB of control data.

The 1,441 frontend RPCs were a second architectural issue, but local and remote
A/B tests show they were not responsible for the old four-second penalty.

## New control/data boundary

Python invocation metadata is control-plane state, not a workflow file.

- Invocation payloads up to 64 KiB remain inline in Workflow IR and are placed
  directly in the physical task's `function_input` frame.
- The Worker Python executor decodes the new `DVP2` ticket without opening or
  fetching an invocation object.
- The reusable callable object remains one content-addressed `DVP1` object and
  is cached by digest in each Worker executor.
- An invocation above 64 KiB follows the unchanged DVP1 object path. This is a
  compatibility and bounded-frame fallback, not file-size placement policy.
- Generic TaskVine tasks, scheduling and file transfer are unchanged.

The runtime advertises `inline_invocation_max_bytes`; the Python adaptor only
uses DVP2 when that capability is present. Older runtimes therefore continue
to receive the old object form.

In the fixed 477-task run, object puts fell from 478 to one. Object-put wall
time fell from 3.41-4.11 seconds to 9.47-9.53 milliseconds.

## Controller-admitted completion stream

Dynamic control results must not be confused with Scheduler task completion.
The Scheduler still marks a task done and releases ready children immediately;
it never waits for Controller persistence. A result is exposed to the frontend
only after the Controller has pulled it, verified it, called `fsync`, renamed
it and committed its `DATA_READY_BATCH` record.

The new `controller-admitted-sequence-v1` stream has these properties:

1. The client opens one dedicated authenticated socket and sends one stream
   request with its last durable sequence.
2. Controller admission appends `{DataID, attempt}` to a compact array in the
   same order as durable data-journal records.
3. An `eventfd` wakes the native epoll owner immediately; no frontend polling
   request is required.
4. The server emits result identity, producer identity, codec and SHA-256. A
   result up to 64 KiB is included in the frame; larger data remains on the
   existing result path and the frame acts as a descriptor.
5. Disconnect recovery resumes after the last sequence. Manager restart
   reconstructs the same sequence by replaying `DATA_READY_BATCH` records.
6. Terminal workflow state is a final stream frame. Failure and cancellation
   cannot leave a subscriber waiting indefinitely.

The sequence is an append-only array rather than a hash table. It costs 16
bytes per requested-result admission, or about 16 MiB for one million results.
Superseded attempts remain historical sequence entries and are skipped by an
attempt check. This preserves replay order without duplicating the Controller
catalog or adding another ownership table.

For the 477-task workflow, stream mode used 483 total RPC requests: 477
semantic append transactions, one stream subscription and five fixed
authentication/setup/seal requests. The matched local polling path used 1,654
requests. Remote stream and poll both took about 1.224 seconds, proving that
the DVP2 data-path correction produced the wall-time gain while the stream
mainly removes scaling load and protocol coupling at this size.

## Scale and acceptance

The wider local run discovered 948 tasks and 884 edges on one 16-core Worker.
DataVine completed in 1.652 seconds at 573.9 tasks/s; TaskVine took 3.441
seconds at 275.5 tasks/s. DataVine used exactly one stream request and 948
unbatched append transactions.

A separate static pressure gate sent 10,000 requested empty results through
Controller durability and one result stream on a 16-core Worker. It delivered
sequences 1 through 10,000 exactly once, reached terminal `completed`, and
took 8.506 seconds (1,175.6 results/s). The entire client interaction used five
RPC requests: two authentications, capabilities, workflow submit and one stream
subscription. No per-result frontend RPC was issued.

Focused gates cover:

- small DVP2 invocation and exact result;
- greater-than-64-KiB DVP1 invocation fallback;
- empty inline result;
- greater-than-64-KiB descriptor result;
- disconnect and sequence resume;
- service restart, journal replay and sequence resume;
- terminal stream frame;
- lifecycle/retry/restart, production data plane and module boundaries;
- warning-clean native and Python builds.

Compact evidence is
`acceptance/dynamic-control-root-fix-20260831.json`. Raw Condor summaries and
logs are under
`/groups/dthain/users/jzhou24/datavine-benchmarks/root-fix-20260831/`.

## Remaining limits

The workload is genuinely dynamic: each completed result determines one to
three children. One append per discovered child is therefore semantic work,
not an accidental polling cost. At the measured 574 tasks/s, Python
serialization is now the largest adaptor ingest stage, but only about 53 ms
for 948 tasks. Worker execution and the result-to-append dependency chain are
the larger remaining wall-time components.

The current result stream performs a second local read for an inline control
result after Controller durability. That read preserves a simple invariant:
only committed bytes are exposed. Avoiding it would require a deliberately
bounded post-commit buffer with replay fallback and should only be considered
after measurement at substantially higher result rates.

This campaign is a dynamic-control acceptance result, not the previously
planned million-task, 128x16 production-scale claim.
