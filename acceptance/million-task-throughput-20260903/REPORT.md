# DataVine one-million-task throughput report

Date: 2026-09-03  
Source base: `c2e9a85be3e52d2b7f0210487a6632b8c03a413e` plus the dirty DataVine worktree

## Result

Two exact Condor runs each completed 1,000,000 native builtin no-op tasks on
8 Workers x 16 real cores. Their runtime mean is:

| Boundary | Result |
|---|---:|
| Service throughput | 28,705.193 tasks/s |
| End-to-end throughput, including graph registration | 21,662.867 tasks/s |
| Service throughput range | 28,570.916--28,839.469 tasks/s |
| End-to-end throughput range | 21,603.966--21,721.768 tasks/s |
| Physical submissions / completions | 1,000,000 / 1,000,000 |
| Requested outputs / durable files / backup jobs | 0 / 0 / 0 |
| Workflow journal bytes | 0 |
| Service CPU time | 41.425 s mean |
| Peak frontend RSS | 1,255,559,168 bytes |

Against the original local 8x16 streaming baseline, the Condor mean service
rate is 82.3% higher and mean end-to-end throughput is 39.6% higher. This cross-topology
comparison shows the achieved campaign endpoint, not a code-only A/B: the
baseline's 128 advertised slots shared a 32-core frontend cgroup, whereas the
Condor runs used 128 remote cores. Condor Worker admission took 4.03 s and
314.82 s; resource-queue time is reported separately and is not part of the
workflow end-to-end timer.

The controlled local comparison is the repeated final 2x24 configuration.
Its two one-million-task runs averaged 22,771.3 service tasks/s and 18,169.6
end-to-end tasks/s. Against the original local baseline (15,750.4 service and
15,517.0 end-to-end), that is +44.6% and +17.1%, respectively. Service CPU
fell from 76.52 s to a mean of 44.315 s, a 42.1% reduction.

## Exact workload

![Throughput milestones and local Worker sweep](throughput.png)

- One native C-owned workflow and one Manager owner.
- One physical TaskVine FunctionCall for every logical task.
- Builtin executor payload; independent tasks; no CPU payload.
- No requested result, ordinary input mount, output file, Controller backup,
  or workflow recovery journal in the final mode.
- Staged graph construction in bounded 100,000-task deltas, then seal.
- Worker-first scheduling and the existing per-task protocol; no semantic task
  batching.

This measures the smallest real DataVine task lifecycle. It is not a claim
about shell process throughput, requested-file persistence, or data movement.

## Changes retained

1. Removed dead per-task statistics work when task-info and performance logs
   are disabled.
2. Added an opt-in trusted-submitter completion path that directly finalizes
   successful zero-mount FunctionCalls and updates Worker accounting
   incrementally. Generic TaskVine defaults remain unchanged.
3. Avoided sandbox and stdout-file creation when a validated FunctionCall has
   no input and no retained output. Ordinary command tasks always retain their
   sandbox because their stdout path lives there. The decision remains in the
   Worker data-plane boundary; the generic process layer sees only a
   sandboxless hint.
4. Avoided duplicate Controller output expectation registration and cached
   immutable workflow slot/GC state in the reactor.
5. Reduced executor/Worker small-frame reads, writes, allocation, and parsing.
6. Paired poll entries with their Worker pointer, removing per-message pointer
   formatting, allocation, hash, and free.
7. Added `staged` to the scale harness so a million-task graph can be submitted
   in bounded deltas without executing under open-workflow retention rules.
8. Added explicit `policy.recovery: none`. The default remains `journal`.
   Ephemeral mode keeps the live event window but omits recovery payloads and
   recovery-only task indices.
9. Replaced two million fixed-digest `snprintf` calls with fixed-size copies
   and removed redundant per-event workflow lookup inside validated batches.

## Experiments and rejected conclusions

- A single sealed million-task JSON crashed during submit; bounded staged
  deltas are the safe loading path.
- Staged execution was faster than streaming, but registration no longer
  overlaps execution. Chunk sizes 10k, 25k, 50k, and 100k were tested. 100k
  gave the best observed million-task rate without material RSS growth, but
  repeated runs varied by roughly 10% on the shared frontend.
- Queue multipliers 1, 2, 4, 8, 16, and 32 were tested. Interleaved 1-vs-8
  repeats differed by only 0.6%; the existing default 8 is retained because
  no no-op win justified weakening mixed-I/O headroom.
- Local Worker counts 1, 2, 4, 8, 12, and 16 were tested. Two Workers were
  best because the frontend is limited to 32 CPU cores; 8x16 locally is
  advertised concurrency, not 128 allocated cores.
- At two local Workers, 8, 12, 16, 24, and 32 slots were tested. Two x 24 was
  the best observed region; 2x32 regressed from context-switch pressure.
- The small Worker/executor frame optimization improved the 100k median but
  one initial million-task run regressed 2.3%. It is retained only together
  with the later exact gates; that isolated result is not claimed as a win.

## Bottleneck after this campaign

On real 8x16 Workers, 1,000,000 tasks required 34.84 service seconds on average
and 41.425 service CPU seconds. The remaining ceiling is the serialized Manager
task protocol and per-task lifecycle: send/status processing consumed about
11.72 s and 12.69 s of accumulated Manager timers, while scheduling itself was
only 0.741 s. Graph registration still costs 10.91 s end-to-end and is dominated
by client graph construction/JSON plus server validation, not journal I/O.

The next architectural step, if rates materially above 30k tasks/s are needed,
is a compact native ticket/event wire encoding while preserving one-message,
one-task semantics. Adding more Workers, a deeper priority queue, or batching
user tasks is not supported by this evidence.

## Validation

- Forced full TaskVine build: PASS.
- `git diff --check`: PASS.
- Full DataVine regression suite: PASS, 21/21. This includes workflow IR,
  scheduler, dynamic workflow, Shell/Go adaptors, module boundaries, output
  policy, persistence faults, lifecycle, execution, and service tests.
- The final sandbox boundary fix was exercised by three additional consecutive
  Shell workflow passes, including late-output promotion, in-flight promotion,
  and volatile-replica loss/replay.
- Generic TaskVine Python serverless path: PASS with Manager and Worker pinned
  to the same configured Python environment.
- Focused recovery-policy validator: `journal|none` accepted; other values
  rejected at `$.policy.recovery`.
- Two final Condor runs: each exact 1,000,000/1,000,000; all jobs removed.
