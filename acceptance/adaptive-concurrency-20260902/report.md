# Adaptive FunctionCall concurrency and queued-task recall

## Outcome

The implementation passes local correctness, performance, memory-admission,
late-Worker rebalancing, Worker eviction, static DAG and dynamic-workflow
tests. It is suitable for the next multi-node factory gate. It is not yet a
cluster-scale promotion result.

The useful performance result is workload-selective. A 4-core Worker improved
the median 250-ms sleep-I/O rate from 14.34 to 19.17 tasks/s (1.34x) and the
mixed CPU/I/O rate from 14.74 to 19.17 tasks/s (1.30x). A dedicated
three-repetition CPU control stayed at the 4-slot window and measured 14.57
versus 14.72 tasks/s (1.01x), so the feature did not create fake CPU capacity.

## Production rule

1. DataVine gives each fork executor a dispatch queue of eight times its core
   count. Generic TaskVine retains the old fixed-credit path by default.
2. The Worker owns a smaller execution window. It samples its own Linux
   process tree every two seconds, using cumulative CPU ticks and conservative
   RSS. CPU below 85% permits exponential growth; CPU at or above 95% halves
   the window toward the core count. Memory at or above 80% halves immediately.
3. An explicit per-call memory request is retained on the adaptive path and
   gates admission at 80% of Worker memory. Executor whole-machine reservation
   is excluded from this calculation. Unspecified memory is guarded by the RSS
   feedback and a projected per-running-task bound.
4. Dispatched but unstarted FunctionCalls remain cheap Worker queue entries.
   They do not stage DataVine inputs or create sandboxes until an execution
   slot exists.
5. When ready work is empty and a Worker is idle, the Manager selects the
   donor with the most unstarted calls. A late Worker first receives the same
   executor; the Manager then sends generation-bound recalls only for queued
   calls. A successful recall carries a destination reservation through the
   READY transition and is assigned to that receiver. Running calls are never
   recalled.
6. Worker selection uses the optional dense Worker ring and scans its integer
   entries for the smallest queued-plus-running load. No per-task Worker heap
   or peer work-stealing protocol is introduced.

## Correctness findings fixed during testing

- The runtime set executor mode before installing the library, while the mode
  setter requires `provides_library`; adaptive mode was silently disabled.
- A late Worker had no executor when the first Worker had already drained the
  ready queue. Rebalancing now bootstraps one truly idle receiver first.
- TaskVine's normal READY cleanup cleared the receiver reservation and caused
  the same queued call to ping-pong hundreds of times. The reservation now
  survives recall requeue and is cleared after a successful new commit.
- Worker revoke formerly killed a call even after it crossed the start
  boundary. It now removes only a process still present in `procs_waiting` and
  reports a miss for a running/completed race.
- Counting executor whole-machine memory against the new 80% cap deadlocked
  all adaptive calls. Admission now subtracts that logical reservation and
  counts only call memory.
- Reading every descendant's `smaps_rollup` introduced a measurable CPU-case
  perturbation. The sampler now traverses only `/proc/.../children` and uses
  conservative RSS from `stat`, reducing the three-run CPU control to a 1.01x
  ratio.

## Evidence

The two-repetition matrix is intentionally interpreted with its workload
duration. Noop, 5-ms and random-I/O cases complete before the first two-second
feedback sample, so their ratios are startup-sensitive controls rather than
evidence of adaptive expansion. The stable long-running rows are:

| Workload | Fixed | Adaptive | Ratio |
|---|---:|---:|---:|
| 250-ms sleep I/O | 14.34 tasks/s | 19.17 tasks/s | 1.34x |
| Mixed CPU/I/O | 14.74 tasks/s | 19.17 tasks/s | 1.30x |
| Late second Worker | 6.05 tasks/s | 7.37 tasks/s | 1.22x |
| CPU, dedicated 3-run control | 14.57 tasks/s | 14.72 tasks/s | 1.01x |

Both adaptive late-Worker repetitions completed exactly 32 results, used both
executors, and performed exactly eight successful recalls without a recall
storm. The receiver split was 24/8 in each run.

The memory workload launched 16 calls which each touched 128 MiB on a 2-GiB
Worker. The mathematical 80% admission maximum is floor(1638.4 / 128) = 12;
the measured peak was 11 and the Worker window ranged from 4 to 11.

The eviction test recalled two calls to a late Worker, killed that Worker, saw
one Manager Worker removal, and still returned all 16 exact ordinal results
from the survivor. This covers both recall ownership and ordinary Worker-loss
recomputation.

The native scheduler gate completed 10,000 independent builtin tasks on 8x16
in 0.913 s of runtime (9,179 tasks/s end to end; 17,467 tasks/s inside the
service interval), with exactly 10,000 physical submissions and completions.
A 10,000-task layered DAG with 18,976 edges and width 512 completed at 3,078
tasks/s; measured Scheduler delay was only 1.509 ms total.

Final runtime regressions passed the DataVine scheduler, notebook/static DAG,
result-driven dynamic workflow and generic TaskVine serverless paths. Expected
negative notebook cases still emit task-failure diagnostics before the suite's
final PASS line.

## Boundary

This controller changes admission of future calls; it does not `SIGSTOP` an
already-running function. Started work is the hard ownership boundary. The
80% guarantee is strict when calls declare memory and conservative under RSS
feedback, but an undeclared instantaneous allocation spike can exist until the
next sample. Cluster-scale behavior, heterogeneous Worker sizes and node-level
CPU isolation remain the next acceptance stage.
