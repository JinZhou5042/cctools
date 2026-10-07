# DataVine decision record

Current navigation: [project map](DATAVINE_MAP.md). Current delivery work:
[handoff](DATAVINE_HANDOFF_20260827.md). Gate status: [acceptance matrix](acceptance/matrix.md).
This record retains decisions and their evidence; campaign reports own the
measurement details.

| Decision | Reason and evidence |
|---|---|
| One process owns one workflow | Removes competing workflow owners; strict-singleton regression replaced the old multi-workflow harnesses. [Baseline](acceptance/production-baseline-20260901.json) |
| Scheduler completion is independent of persistence | Children become ready on physical success; data availability is resolved at the Worker. [Contract](DATAVINE_PRODUCTION.md) |
| Keep Worker replicas after Controller backup | Peer delivery avoids central payload fan-in. [Route comparison](acceptance/peer-vs-controller-20260831.json) |
| Use Controller-local `/tmp` for requested results | Preserve per-file verification and local persistence; no cross-host durability claim. [Storage evidence](acceptance/controller-local-tmp-20260827/summary.json) |
| Keep 16 Controller data threads | More fan-in did not increase durable throughput. [Remote fan-in](CONTROLLER_REMOTE_FANIN_20260828.md) |
| Retain `fsync` and the existing verification path | Fault injection validates delayed writeback failure handling; screened alternatives had no stable gain. [Diagnosis](CONTROLLER_PERSISTENCE_DIAGNOSTIC_20260828.md) |
| Do not route by file size | Dense repeated measurements had seven winner transitions. [Crossover](acceptance/storage-crossover-20260901/report.md) |
| Keep small invocation records in the control path | Per-task object persistence dominated the old dynamic pilot; result streaming removes repeated frontend polling. [Dynamic correction](DATAVINE_DYNAMIC_CONTROL_20260831.md) |
| Use one static/dynamic DataID lifecycle | Lazy durable-identity hydration fixes late consumers after restart without a new data table. [Lifecycle gate](acceptance/dynamic-data-management-20260831.json) |
| Use Worker-owned elastic admission and queued-call recall | Local oracle comparison, memory guard and eviction tests support the mechanism. [Elastic campaign](acceptance/adaptive-window-20260903/REPORT.md) |
| Keep journal recovery as default | Explicit `recovery:none` isolates process-lifetime no-op throughput without changing persistence policy. [Native campaign](acceptance/million-task-throughput-20260903/REPORT.md) |
| Measure Manager transport before further no-op scaling changes | 32x16 completed 1M tasks but did not exceed historical 8x16 throughput; the comparison is not matched A/B. [Big-pool report](acceptance/million-task-bigpool-20260903/REPORT.md) |
| One installed executor in the Poncho environment | Builtins and Python share `datavine_executor`; no task environment or executable override. |
| Remove unused alternative paths | No task groups, DONE rollback API, single-result path RPC or dense-pool override. Result descriptors and the normal scheduler remain the owners. |
| One production admission policy | Worker-owned elastic admission and preparation on execution opportunity; retired fixed/aggressive/AIMD/eager controls live only in historical evidence. |
| One owner for frontend submission and native validation | Session commit reuses builder append; native validation defines accepted IR. Removed duplicated capability preflight and unused retry filters. |
| Apply the singleton boundary to Worker state | One Agent workflow per Manager connection, released at disconnect; no multi-workflow list. |
| Keep checks that protect actual execution and data | Preserve graph validity, bounded queues/frames, generations, digest verification and durability barriers; simplify duplicate paths and implementation-mirroring tests. [Module review](DATAVINE_MAP.md#source-map) |

Earlier pass-by-pass debugging logs and superseded todo lists have been removed
from this document. Their accepted reports and raw results remain under
`acceptance/`; they should not be read as new measurements of today's checkout.
