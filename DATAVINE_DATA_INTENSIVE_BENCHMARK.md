# DataVine data-intensive benchmark

The executable workload contract is
`acceptance/scripts/data_intensive_workload.py`. One logical task is one
physical TaskVine task; sleeps and task grouping are forbidden.

## Full workload

| Item | Value |
|---|---:|
| Tasks | 1,048,576 |
| Source files | 9,437,184 |
| Output files | 1,048,576 |
| Workflow files | 10,485,760 |
| Scheduler edges | 5,898,240 |
| Workers | 128 |
| Cores per Worker | 16 |
| Stored artifacts | 1.040 TiB |
| Logical read/write path | 3.696 TiB |

There are 64 independent cohorts. Each cohort contains 4,096 A tasks, 10,240
B tasks and 2,048 C tasks. A outputs have 20 B consumers; each B output has one
C consumer. Only C outputs are requested. A/B intermediates must exercise
Worker-local retention, peer movement and GC.

Each task deterministically randomizes file order and `pread` offsets, reads
every input byte, performs 2 ms to 5 s of process-CPU work, and writes a real
non-sparse output. DataVine and TaskVine use the same kernel and sampled sink
digests must match.

## Acceptance rules

A comparative run is valid only when both backends use the same graph, dataset,
worker/core resources and output policy, and both satisfy:

- exact logical and physical task counts;
- exact worker admission before timing;
- exact sampled output digests;
- no exhausted task attempts or unresolved DataIDs;
- recorded process exits and cleanup;
- no grouped noops or cross-configuration throughput ratio.

Use `acceptance/scripts/run_data_intensive_campaign.py` for the full campaign
and `acceptance/scripts/compare_data_intensive_runs.py` for fail-closed
comparison. Dataset generation and verification are handled by
`generate_data_intensive_dataset.py` and the adjacent Condor submit files.

## Practical order

1. Run the topology/kernel contract test.
2. Run a small exact workflow using the production scheduler and data path.
3. Run the bounded recovery gate when recovery code changed.
4. Use the million-task dataset only for a scale-specific question.

The current production decision does not depend on completing another 1M-task
campaign. Current scheduler and Controller results are summarized in
`progress.md`; raw Controller-local `/tmp` evidence is under
`acceptance/controller-local-tmp-20260827/`.
