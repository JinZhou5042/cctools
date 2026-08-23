# DataVine versus TaskVine fixed-core result

Source artifact: `/project01/ndcms/jzhou24/datavine-benchmarks/final-screen-v9-1024-20260820/finalized-summary.json`

Status: **PASS**; scope: **publication-1024-core**.

Resource contract: 64 workers x 16 cores = 1024 cores per backend; batch type `condor`.

A ratio above 1 favors DataVine. A performance conclusion requires a non-null paired 95% interval. Accumulated parallel service times are reported as work amplification and are not treated as wall critical path.

## Gates

- `all_regressions_attributed`: `True`
- `exact_1024_core_admission`: `True`
- `exact_results`: `True`
- `exact_task_counts`: `True`
- `no_unresolved_regression_cases`: `[]`
- `paired_repetitions`: `True`
- `warmups_complete`: `True`
- `zero_worker_churn_in_accepted_runs`: `True`

## Results

| Case | Phase | Topology | Tasks | Edges | CPU ms | In/Out degree | TV s | DV s | DV/TV | 95% CI | Attribution |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| `admission_immediate` | admission | independent | 1024 | 0 | 0 | 0/0 | 8.7982 | 5.2907 | 1.674 | [1.644, 1.772] | NOT_REQUIRED |
| `confirmation_input_broadcast_0b_w128` | confirmation | broadcast | 129 | 128 | 10.0 | 1/128 | 1.0819 | 1.1252 | 0.970 | [0.934, 1.023] | NOT_REQUIRED |
| `cpu_0ms` | cpu | independent | 4096 | 0 | 0 | 0/0 | 36.7487 | 19.6110 | 1.901 | [1.773, 2.008] | NOT_REQUIRED |
| `cpu_10000ms` | cpu | independent | 4096 | 0 | 10000 | 0/0 | 65.7639 | 57.2328 | 1.144 | [1.111, 1.160] | NOT_REQUIRED |
| `cpu_1000ms` | cpu | independent | 4096 | 0 | 1000 | 0/0 | 39.8018 | 21.0005 | 1.918 | [1.723, 1.967] | NOT_REQUIRED |
| `cpu_100ms` | cpu | independent | 4096 | 0 | 100 | 0/0 | 37.4742 | 19.7521 | 1.952 | [1.771, 2.010] | NOT_REQUIRED |
| `cpu_10ms` | cpu | independent | 4096 | 0 | 10 | 0/0 | 36.2793 | 19.3121 | 1.869 | [1.790, 1.928] | NOT_REQUIRED |
| `cpu_1ms` | cpu | independent | 4096 | 0 | 1 | 0/0 | 36.0295 | 19.9929 | 1.816 | [1.673, 1.925] | NOT_REQUIRED |
| `degree_1` | degree | regular-pipeline | 2048 | 1024 | 10.0 | 1/1 | 17.7504 | 8.6552 | 2.056 | [1.844, 2.122] | NOT_REQUIRED |
| `degree_16` | degree | regular-pipeline | 2048 | 16384 | 10.0 | 16/16 | 18.8386 | 9.3553 | 2.099 | [1.927, 2.149] | NOT_REQUIRED |
| `degree_4` | degree | regular-pipeline | 2048 | 4096 | 10.0 | 4/4 | 17.8246 | 8.9679 | 1.999 | [1.959, 2.089] | NOT_REQUIRED |
| `degree_64` | degree | regular-pipeline | 2048 | 65536 | 10.0 | 64/64 | 25.2295 | 15.4871 | 1.548 | [1.491, 1.583] | NOT_REQUIRED |
| `input_broadcast_0b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 8.9614 | 5.5667 | 1.664 | [1.561, 1.696] | NOT_REQUIRED |
| `input_broadcast_1024b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 8.9611 | 5.3231 | 1.744 | [1.656, 1.788] | NOT_REQUIRED |
| `input_broadcast_1048576b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 8.9466 | 5.3746 | 1.678 | [1.602, 1.741] | NOT_REQUIRED |
| `input_broadcast_33554432b` | data | broadcast | 129 | 128 | 10.0 | 1/128 | 1.2621 | 1.3713 | 0.899 | [0.872, 0.966] | result-fetch |
| `input_broadcast_65536b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 9.0084 | 5.5454 | 1.641 | [1.595, 1.731] | NOT_REQUIRED |
| `interaction_c0_p0_d1` | interaction | regular-pipeline | 2048 | 1024 | 0 | 1/1 | 17.4178 | 8.8519 | 2.037 | [1.948, 2.058] | NOT_REQUIRED |
| `interaction_c0_p0_d16` | interaction | regular-pipeline | 2048 | 16384 | 0 | 16/16 | 19.0536 | 9.6375 | 2.029 | [1.986, 2.068] | NOT_REQUIRED |
| `interaction_c0_p0_d64` | interaction | regular-pipeline | 2048 | 65536 | 0 | 64/64 | 24.1927 | 15.2350 | 1.554 | [1.500, 1.629] | NOT_REQUIRED |
| `interaction_c0_p1048576_d1` | interaction | regular-pipeline | 2048 | 1024 | 0 | 1/1 | 20.3017 | 11.7217 | 1.730 | [1.615, 1.847] | NOT_REQUIRED |
| `interaction_c0_p1048576_d16` | interaction | regular-pipeline | 2048 | 16384 | 0 | 16/16 | 21.3657 | 12.3734 | 1.758 | [1.678, 1.795] | NOT_REQUIRED |
| `interaction_c0_p1048576_d64` | interaction | regular-pipeline | 2048 | 65536 | 0 | 64/64 | 29.2808 | 19.4151 | 1.437 | [1.371, 1.515] | NOT_REQUIRED |
| `interaction_c0_p65536_d1` | interaction | regular-pipeline | 2048 | 1024 | 0 | 1/1 | 17.9409 | 9.2000 | 2.012 | [1.730, 2.058] | NOT_REQUIRED |
| `interaction_c0_p65536_d16` | interaction | regular-pipeline | 2048 | 16384 | 0 | 16/16 | 19.7048 | 9.8876 | 1.971 | [1.935, 2.070] | NOT_REQUIRED |
| `interaction_c0_p65536_d64` | interaction | regular-pipeline | 2048 | 65536 | 0 | 64/64 | 25.1343 | 15.8986 | 1.545 | [1.505, 1.635] | NOT_REQUIRED |
| `interaction_c100_p0_d1` | interaction | regular-pipeline | 2048 | 1024 | 100 | 1/1 | 17.7311 | 8.3937 | 2.083 | [1.861, 2.172] | NOT_REQUIRED |
| `interaction_c100_p0_d16` | interaction | regular-pipeline | 2048 | 16384 | 100 | 16/16 | 18.8661 | 9.4454 | 2.003 | [1.897, 2.092] | NOT_REQUIRED |
| `interaction_c100_p0_d64` | interaction | regular-pipeline | 2048 | 65536 | 100 | 64/64 | 26.4813 | 16.0247 | 1.671 | [1.557, 1.709] | NOT_REQUIRED |
| `interaction_c100_p1048576_d1` | interaction | regular-pipeline | 2048 | 1024 | 100 | 1/1 | 20.6219 | 11.1531 | 1.853 | [1.691, 1.919] | NOT_REQUIRED |
| `interaction_c100_p1048576_d16` | interaction | regular-pipeline | 2048 | 16384 | 100 | 16/16 | 22.1570 | 12.7990 | 1.780 | [1.707, 1.809] | NOT_REQUIRED |
| `interaction_c100_p1048576_d64` | interaction | regular-pipeline | 2048 | 65536 | 100 | 64/64 | 29.6219 | 20.8368 | 1.402 | [1.370, 1.459] | NOT_REQUIRED |
| `interaction_c100_p65536_d1` | interaction | regular-pipeline | 2048 | 1024 | 100 | 1/1 | 17.9299 | 8.7870 | 2.003 | [1.931, 2.149] | NOT_REQUIRED |
| `interaction_c100_p65536_d16` | interaction | regular-pipeline | 2048 | 16384 | 100 | 16/16 | 19.4505 | 9.5886 | 1.991 | [1.914, 2.127] | NOT_REQUIRED |
| `interaction_c100_p65536_d64` | interaction | regular-pipeline | 2048 | 65536 | 100 | 64/64 | 25.8430 | 16.4000 | 1.538 | [1.440, 1.595] | NOT_REQUIRED |
| `output_0b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 8.9347 | 5.3169 | 1.691 | [1.648, 1.778] | NOT_REQUIRED |
| `output_1024b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 8.9721 | 5.4906 | 1.695 | [1.518, 1.780] | NOT_REQUIRED |
| `output_1048576b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 11.7450 | 8.3743 | 1.448 | [1.352, 1.506] | NOT_REQUIRED |
| `output_33554432b` | data | independent | 128 | 0 | 10.0 | 0/0 | 13.0082 | 11.2239 | 1.177 | [1.055, 1.274] | NOT_REQUIRED |
| `output_65536b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 9.0239 | 5.5391 | 1.652 | [1.524, 1.714] | NOT_REQUIRED |
| `topology_broadcast1024` | topology | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 8.8916 | 5.6217 | 1.602 | [1.518, 1.697] | NOT_REQUIRED |
| `topology_chains` | topology | regular-pipeline | 4096 | 3072 | 10.0 | 1/1 | 35.6421 | 16.9896 | 2.115 | [2.073, 2.334] | NOT_REQUIRED |
| `topology_dynamic` | topology | dynamic | 8 | 7 | 10.0 | 1/1 | 0.1857 | 1.0073 | 0.180 | [0.149, 0.202] | dynamic-session-roundtrips |
| `topology_fanin16` | topology | fan-in | 1088 | 1024 | 10.0 | 16/1 | 9.1131 | 4.6743 | 2.034 | [1.900, 2.240] | NOT_REQUIRED |
| `topology_fanout16` | topology | fan-out | 1088 | 1024 | 10.0 | 1/16 | 9.6373 | 5.5949 | 1.718 | [1.607, 1.782] | NOT_REQUIRED |
| `topology_heavy_tail` | topology | heavy-tail | 1024 | 0 | mixed-0-1000 | 0/0 | 9.9163 | 5.6066 | 1.795 | [1.636, 1.885] | NOT_REQUIRED |
| `topology_map` | topology | independent | 1024 | 0 | 10.0 | 0/0 | 8.8792 | 5.3636 | 1.675 | [1.612, 1.711] | NOT_REQUIRED |
| `topology_pipeline2` | topology | regular-pipeline | 4096 | 6144 | 10.0 | 2/2 | 35.9924 | 16.7015 | 2.293 | [2.064, 2.343] | NOT_REQUIRED |
| `topology_reduce16` | topology | reduction-tree | 1093 | 1092 | 10.0 | 16/1 | 9.4222 | 4.2344 | 2.209 | [2.065, 2.326] | NOT_REQUIRED |

## DataVine regressions and causes

### input_broadcast_33554432b

DataVine excess: 0.109198 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_object_ingest_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.16641494150373526,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 2.7569851904326264,
      "name": "graph-materialization",
      "seconds": 0.301057
    },
    {
      "fraction_of_gap": 1.1111468878921518,
      "name": "result-fetch",
      "seconds": 0.12133490951964632
    },
    {
      "fraction_of_gap": 0.7485079670381553,
      "name": "publication",
      "seconds": 0.0817355
    },
    {
      "fraction_of_gap": 0.4853021103191199,
      "name": "fixed-control",
      "seconds": 0.052993972522361175
    },
    {
      "fraction_of_gap": 0.07404446253448016,
      "name": "scheduler-submit",
      "seconds": 0.008085499999999999
    },
    {
      "fraction_of_gap": 0.0,
      "name": "dynamic-session-roundtrips",
      "seconds": 0.0
    },
    {
      "fraction_of_gap": 0.0,
      "name": "client-object-ingest",
      "seconds": 0.0
    }
  ],
  "datavine_fetch_seconds": 0.12163662401144393,
  "datavine_ingest_profile_medians": {
    "object_put_parallelism": 16.0,
    "object_put_wall_seconds": 0.0418486985,
    "object_rpc_aggregate_seconds": 0.1683973975,
    "object_sharedfs_aggregate_seconds": 0.237574437,
    "python_serialization_seconds": 0.0048496095
  },
  "datavine_median_seconds": 1.3713301194948144,
  "datavine_stage_medians": {
    "manager_lock_seconds": 3e-06,
    "materialize_seconds": 0.301057,
    "publication_commit_seconds": 0.22755999999999998,
    "publication_prepare_seconds": 0.0066095,
    "publication_queue_seconds": 10.8611885,
    "publish_seconds": 0.075126,
    "python_decode_seconds": 2.147791,
    "python_fsync_seconds": 0.0,
    "python_function_seconds": 1.320324,
    "python_serialize_seconds": 0.087603,
    "setup_seconds": 0.034645999999999996,
    "submission_event_seconds": 0.0008815,
    "submit_seconds": 0.0080825
  },
  "datavine_useful_cpu_seconds": 1.2901677275,
  "datavine_workflow_build_seconds": 0.013715402979869395,
  "datavine_workflow_submit_seconds": 0.09452745798625983,
  "dynamic_session_contract": null,
  "dynamic_session_roundtrip_excess_seconds": 0.0,
  "fetch_excess_seconds": 0.12133490951964632,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.12133490951964632
  },
  "logical_edge_payload_bytes": 4294967296,
  "manager_transfer_excess_seconds": 0.0,
  "parallel_publication_service_seconds": {
    "commit": 0.22755999999999998,
    "decode": 2.147791,
    "fsync": 0.0,
    "function": 1.320324,
    "queue": 10.8611885,
    "serialize": 0.087603,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "runtime_invocations": 1.0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0003017144917976111,
  "taskvine_median_seconds": 1.2621322170016356,
  "taskvine_useful_cpu_seconds": 1.2901330785,
  "terminal_poll_residual_seconds": 0.015103972522361175,
  "useful_cpu_relative_difference": 2.685619804426027e-05,
  "worker_execution_excess_seconds": 7.257065
}
```

### topology_dynamic

DataVine excess: 0.821531 s; classification: `dynamic-session-roundtrips`; status: `CONFIRMED`.

Improvement: keep one WorkflowSession runtime lane resident across append/result cycles; submit appended tasks directly to that lane and seal it once

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_object_ingest_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.20682919999407048,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.2008174858315994,
      "name": "dynamic-session-roundtrips",
      "seconds": 0.9865088469814509
    },
    {
      "fraction_of_gap": 0.2596228111467133,
      "name": "fixed-control",
      "seconds": 0.2132881999940705
    },
    {
      "fraction_of_gap": 0.02853939613309627,
      "name": "graph-materialization",
      "seconds": 0.023446
    },
    {
      "fraction_of_gap": 0.02105519937781425,
      "name": "publication",
      "seconds": 0.017297500000000004
    },
    {
      "fraction_of_gap": 0.0036736286586063526,
      "name": "scheduler-submit",
      "seconds": 0.003018
    },
    {
      "fraction_of_gap": 0.0,
      "name": "client-object-ingest",
      "seconds": 0.0
    },
    {
      "fraction_of_gap": 0.0,
      "name": "result-fetch",
      "seconds": 0.0
    }
  ],
  "datavine_fetch_seconds": 0.0,
  "datavine_ingest_profile_medians": {
    "object_put_parallelism": 0.0,
    "object_put_wall_seconds": 0.0,
    "object_rpc_aggregate_seconds": 0.0,
    "object_sharedfs_aggregate_seconds": 0.0,
    "python_serialization_seconds": 0.0
  },
  "datavine_median_seconds": 1.007253903488163,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0,
    "materialize_seconds": 0.023446,
    "publication_commit_seconds": 0.008806999999999999,
    "publication_prepare_seconds": 0.00041099999999999996,
    "publication_queue_seconds": 0.13911800000000002,
    "publish_seconds": 0.016886500000000002,
    "python_decode_seconds": 0.0019545,
    "python_fsync_seconds": 0.0,
    "python_function_seconds": 0.08129049999999999,
    "python_serialize_seconds": 0.000509,
    "setup_seconds": 0.10412149999999999,
    "submission_event_seconds": 6.85e-05,
    "submit_seconds": 0.003018
  },
  "datavine_useful_cpu_seconds": 0.0100015115,
  "datavine_workflow_build_seconds": 0.0,
  "datavine_workflow_submit_seconds": 0.002641008497448638,
  "dynamic_session_contract": "sequential append/result dependency with one final seal",
  "dynamic_session_roundtrip_excess_seconds": 0.9865088469814509,
  "fetch_excess_seconds": 0.0,
  "largest_measured_component": {
    "name": "dynamic-session-roundtrips",
    "seconds": 0.9865088469814509
  },
  "logical_edge_payload_bytes": 7168,
  "manager_transfer_excess_seconds": 0.0,
  "parallel_publication_service_seconds": {
    "commit": 0.008806999999999999,
    "decode": 0.0019545,
    "fsync": 0.0,
    "function": 0.08129049999999999,
    "queue": 0.13911800000000002,
    "serialize": 0.000509,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1024,
  "runtime_invocations": 9.0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0,
  "taskvine_median_seconds": 0.18572285599657334,
  "taskvine_useful_cpu_seconds": 0.010000827,
  "terminal_poll_residual_seconds": 0.09839569999407052,
  "useful_cpu_relative_difference": 6.843965534614762e-05,
  "worker_execution_excess_seconds": 0.068052
}
```

