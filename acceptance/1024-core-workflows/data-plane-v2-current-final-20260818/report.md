# DataVine versus TaskVine fixed-core result

Source artifact: `acceptance/1024-core-workflows/data-plane-v2-current-final-20260818/compact-summary.json`

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
| `admission_immediate` | admission | independent | 1024 | 0 | 0 | 0/0 | 1.1037 | 1.3442 | 0.843 | [0.768, 0.868] | result-fetch |
| `confirmation_input_broadcast_0b_w128` | confirmation | broadcast | 129 | 128 | 10.0 | 1/128 | 0.2144 | 0.3669 | 0.502 | [0.464, 0.637] | fixed-control |
| `cpu_0ms` | cpu | independent | 4096 | 0 | 0 | 0/0 | 5.0329 | 4.9247 | 1.022 | [0.971, 1.093] | NOT_REQUIRED |
| `cpu_10000ms` | cpu | independent | 4096 | 0 | 10000 | 0/0 | 43.5358 | 44.5778 | 0.977 | [0.971, 0.981] | result-fetch+publication+graph-materialization |
| `cpu_1000ms` | cpu | independent | 4096 | 0 | 1000 | 0/0 | 7.3111 | 8.4033 | 0.880 | [0.860, 0.899] | result-fetch+scheduler-submit+publication |
| `cpu_100ms` | cpu | independent | 4096 | 0 | 100 | 0/0 | 4.5983 | 4.8288 | 0.958 | [0.877, 1.042] | NOT_REQUIRED |
| `cpu_10ms` | cpu | independent | 4096 | 0 | 10 | 0/0 | 5.1394 | 4.7376 | 1.095 | [1.016, 1.153] | NOT_REQUIRED |
| `cpu_1ms` | cpu | independent | 4096 | 0 | 1 | 0/0 | 4.9326 | 4.8441 | 1.031 | [0.959, 1.095] | NOT_REQUIRED |
| `degree_1` | degree | regular-pipeline | 2048 | 1024 | 10.0 | 1/1 | 2.1238 | 2.4321 | 0.847 | [0.769, 1.002] | NOT_REQUIRED |
| `degree_16` | degree | regular-pipeline | 2048 | 16384 | 10.0 | 16/16 | 3.1706 | 3.6582 | 0.889 | [0.773, 1.023] | NOT_REQUIRED |
| `degree_4` | degree | regular-pipeline | 2048 | 4096 | 10.0 | 4/4 | 2.3670 | 2.7152 | 0.879 | [0.758, 0.957] | fixed-control |
| `degree_64` | degree | regular-pipeline | 2048 | 65536 | 10.0 | 64/64 | 13.1186 | 11.9778 | 1.022 | [0.878, 1.263] | NOT_REQUIRED |
| `input_broadcast_0b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.2463 | 1.4538 | 0.869 | [0.786, 0.904] | result-fetch |
| `input_broadcast_1024b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.2314 | 1.4270 | 0.868 | [0.788, 0.914] | result-fetch |
| `input_broadcast_1048576b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.2468 | 1.4419 | 0.874 | [0.806, 0.930] | result-fetch |
| `input_broadcast_33554432b` | data | broadcast | 129 | 128 | 10.0 | 1/128 | 0.5296 | 0.5382 | 0.961 | [0.783, 1.027] | NOT_REQUIRED |
| `input_broadcast_65536b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.2322 | 1.4308 | 0.861 | [0.798, 0.900] | result-fetch |
| `interaction_c0_p0_d1` | interaction | regular-pipeline | 2048 | 1024 | 0 | 1/1 | 2.1018 | 2.3640 | 0.906 | [0.813, 0.998] | fixed-control |
| `interaction_c0_p0_d16` | interaction | regular-pipeline | 2048 | 16384 | 0 | 16/16 | 3.3713 | 3.5449 | 0.949 | [0.835, 1.001] | NOT_REQUIRED |
| `interaction_c0_p0_d64` | interaction | regular-pipeline | 2048 | 65536 | 0 | 64/64 | 14.1705 | 11.4857 | 1.212 | [1.020, 1.299] | NOT_REQUIRED |
| `interaction_c0_p1048576_d1` | interaction | regular-pipeline | 2048 | 1024 | 0 | 1/1 | 4.8055 | 4.6759 | 1.054 | [0.997, 1.097] | NOT_REQUIRED |
| `interaction_c0_p1048576_d16` | interaction | regular-pipeline | 2048 | 16384 | 0 | 16/16 | 6.3447 | 6.3836 | 0.971 | [0.900, 1.046] | NOT_REQUIRED |
| `interaction_c0_p1048576_d64` | interaction | regular-pipeline | 2048 | 65536 | 0 | 64/64 | 17.5537 | 14.0945 | 1.210 | [1.082, 1.293] | NOT_REQUIRED |
| `interaction_c0_p65536_d1` | interaction | regular-pipeline | 2048 | 1024 | 0 | 1/1 | 2.3450 | 2.6624 | 0.903 | [0.827, 1.015] | NOT_REQUIRED |
| `interaction_c0_p65536_d16` | interaction | regular-pipeline | 2048 | 16384 | 0 | 16/16 | 3.5020 | 3.7937 | 0.948 | [0.830, 1.000] | fixed-control |
| `interaction_c0_p65536_d64` | interaction | regular-pipeline | 2048 | 65536 | 0 | 64/64 | 14.5479 | 11.2828 | 1.225 | [1.080, 1.328] | NOT_REQUIRED |
| `interaction_c100_p0_d1` | interaction | regular-pipeline | 2048 | 1024 | 100 | 1/1 | 2.2038 | 2.4576 | 0.924 | [0.829, 1.029] | NOT_REQUIRED |
| `interaction_c100_p0_d16` | interaction | regular-pipeline | 2048 | 16384 | 100 | 16/16 | 3.5832 | 3.8057 | 0.926 | [0.787, 0.972] | fixed-control |
| `interaction_c100_p0_d64` | interaction | regular-pipeline | 2048 | 65536 | 100 | 64/64 | 12.4266 | 11.2503 | 0.978 | [0.853, 1.146] | NOT_REQUIRED |
| `interaction_c100_p1048576_d1` | interaction | regular-pipeline | 2048 | 1024 | 100 | 1/1 | 4.7237 | 4.7144 | 1.018 | [0.984, 1.069] | NOT_REQUIRED |
| `interaction_c100_p1048576_d16` | interaction | regular-pipeline | 2048 | 16384 | 100 | 16/16 | 6.1478 | 6.3253 | 0.956 | [0.900, 1.038] | NOT_REQUIRED |
| `interaction_c100_p1048576_d64` | interaction | regular-pipeline | 2048 | 65536 | 100 | 64/64 | 15.2999 | 16.0589 | 1.006 | [0.813, 1.111] | NOT_REQUIRED |
| `interaction_c100_p65536_d1` | interaction | regular-pipeline | 2048 | 1024 | 100 | 1/1 | 2.3870 | 2.7411 | 0.937 | [0.854, 1.019] | NOT_REQUIRED |
| `interaction_c100_p65536_d16` | interaction | regular-pipeline | 2048 | 16384 | 100 | 16/16 | 3.7045 | 3.8279 | 0.938 | [0.821, 0.980] | fixed-control |
| `interaction_c100_p65536_d64` | interaction | regular-pipeline | 2048 | 65536 | 100 | 64/64 | 11.9189 | 11.8375 | 0.969 | [0.865, 1.058] | NOT_REQUIRED |
| `output_0b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 1.1515 | 1.3027 | 0.891 | [0.797, 0.941] | result-fetch |
| `output_1024b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 1.1310 | 1.3283 | 0.867 | [0.797, 0.892] | result-fetch |
| `output_1048576b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 3.7587 | 3.4918 | 1.060 | [1.049, 1.083] | NOT_REQUIRED |
| `output_33554432b` | data | independent | 128 | 0 | 10.0 | 0/0 | 8.3430 | 10.3633 | 0.806 | [0.776, 0.830] | result-fetch |
| `output_65536b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 1.3016 | 1.5168 | 0.867 | [0.826, 0.927] | result-fetch |
| `topology_broadcast1024` | topology | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.2384 | 1.4772 | 0.861 | [0.760, 0.884] | result-fetch |
| `topology_chains` | topology | regular-pipeline | 4096 | 3072 | 10.0 | 1/1 | 4.1745 | 4.3818 | 0.964 | [0.857, 1.065] | NOT_REQUIRED |
| `topology_dynamic` | topology | dynamic | 8 | 7 | 10.0 | 1/1 | 0.1186 | 0.4314 | 0.267 | [0.260, 0.278] | fixed-control |
| `topology_fanin16` | topology | fan-in | 1088 | 1024 | 10.0 | 16/1 | 1.1372 | 1.3377 | 0.898 | [0.762, 0.965] | fixed-control |
| `topology_fanout16` | topology | fan-out | 1088 | 1024 | 10.0 | 1/16 | 1.2619 | 1.5651 | 0.847 | [0.762, 0.870] | result-fetch |
| `topology_heavy_tail` | topology | heavy-tail | 1024 | 0 | mixed-0-1000 | 0/0 | 1.8779 | 2.2868 | 0.824 | [0.798, 0.865] | result-fetch+fixed-control+graph-materialization |
| `topology_map` | topology | independent | 1024 | 0 | 10.0 | 0/0 | 1.1918 | 1.3317 | 0.892 | [0.834, 0.942] | result-fetch |
| `topology_pipeline2` | topology | regular-pipeline | 4096 | 6144 | 10.0 | 2/2 | 4.2926 | 4.6891 | 0.934 | [0.834, 1.019] | NOT_REQUIRED |
| `topology_reduce16` | topology | reduction-tree | 1093 | 1092 | 10.0 | 16/1 | 1.1859 | 1.2885 | 0.931 | [0.812, 1.002] | NOT_REQUIRED |

## DataVine regressions and causes

### admission_immediate

DataVine excess: 0.240539 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.03288713448273484,
  "client_wait_fetch_residual_seconds": 0.2397426289873691,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.5951137904333574,
      "name": "result-fetch",
      "seconds": 0.14314811798976734
    },
    {
      "fraction_of_gap": 0.5682502023143526,
      "name": "fixed-control",
      "seconds": 0.13668637547345366
    },
    {
      "fraction_of_gap": 0.31123634029951314,
      "name": "graph-materialization",
      "seconds": 0.0748645
    },
    {
      "fraction_of_gap": 0.2751943784587658,
      "name": "publication",
      "seconds": 0.066195
    },
    {
      "fraction_of_gap": 0.12695234889335041,
      "name": "scheduler-submit",
      "seconds": 0.030536999999999998
    }
  ],
  "datavine_fetch_seconds": 0.1448132894875016,
  "datavine_median_seconds": 1.3442385514936177,
  "datavine_stage_medians": {
    "manager_lock_seconds": 2e-06,
    "materialize_seconds": 0.0748645,
    "publication_commit_seconds": 3.429106,
    "publication_prepare_seconds": 0.06384000000000001,
    "publication_queue_seconds": 0.010571500000000001,
    "publish_seconds": 0.002355,
    "python_decode_seconds": 0.2087475,
    "python_fsync_seconds": 0.3804885,
    "python_function_seconds": 0.103965,
    "python_serialize_seconds": 0.07226199999999999,
    "setup_seconds": 0.0668,
    "submission_event_seconds": 0.0040160000000000005,
    "submit_seconds": 0.030535
  },
  "datavine_useful_cpu_seconds": 0.002397001,
  "fetch_excess_seconds": 0.14314811798976734,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.14314811798976734
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.13357149999999998,
  "parallel_publication_service_seconds": {
    "commit": 3.429106,
    "decode": 0.2087475,
    "fsync": 0.3804885,
    "function": 0.103965,
    "queue": 0.010571500000000001,
    "serialize": 0.07226199999999999,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016651714977342635,
  "taskvine_median_seconds": 1.1036994809983298,
  "taskvine_useful_cpu_seconds": 0.0031641905,
  "terminal_poll_residual_seconds": 0.027484240990718833,
  "useful_cpu_relative_difference": 0.2424599593482124,
  "worker_execution_excess_seconds": 0.0
}
```

### confirmation_input_broadcast_0b_w128

DataVine excess: 0.152430 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.024200300016673282,
  "client_wait_fetch_residual_seconds": 0.11829607700167875,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.5029580108309466,
      "name": "fixed-control",
      "seconds": 0.0766659225274343
    },
    {
      "fraction_of_gap": 0.47469318001837646,
      "name": "result-fetch",
      "seconds": 0.07235751251573674
    },
    {
      "fraction_of_gap": 0.21315020692609865,
      "name": "scheduler-submit",
      "seconds": 0.0324905
    },
    {
      "fraction_of_gap": 0.0971527497074097,
      "name": "graph-materialization",
      "seconds": 0.014809
    },
    {
      "fraction_of_gap": 0.06886108699465365,
      "name": "publication",
      "seconds": 0.010496499999999999
    }
  ],
  "datavine_fetch_seconds": 0.07251911250932608,
  "datavine_median_seconds": 0.3668643364944728,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0261305,
    "materialize_seconds": 0.014809,
    "publication_commit_seconds": 0.394723,
    "publication_prepare_seconds": 0.0077865,
    "publication_queue_seconds": 0.0014069999999999998,
    "publish_seconds": 0.0027099999999999997,
    "python_decode_seconds": 0.039274,
    "python_fsync_seconds": 0.082149,
    "python_function_seconds": 1.313695,
    "python_serialize_seconds": 0.009622,
    "setup_seconds": 0.035299,
    "submission_event_seconds": 0.0007515,
    "submit_seconds": 0.006359999999999999
  },
  "datavine_useful_cpu_seconds": 1.2901378115000002,
  "fetch_excess_seconds": 0.07235751251573674,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.0766659225274343
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.030631500000000002,
  "parallel_publication_service_seconds": {
    "commit": 0.394723,
    "decode": 0.039274,
    "fsync": 0.082149,
    "function": 1.313695,
    "queue": 0.0014069999999999998,
    "serialize": 0.009622,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0001615999935893342,
  "taskvine_median_seconds": 0.2144342710089404,
  "taskvine_useful_cpu_seconds": 1.29017132,
  "terminal_poll_residual_seconds": 0.014528122510761021,
  "useful_cpu_relative_difference": 2.5972132135005714e-05,
  "worker_execution_excess_seconds": 0.2685710000000001
}
```

### cpu_10000ms

DataVine excess: 1.041987 s; classification: `result-fetch+publication+graph-materialization`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; batch retained-output serialization, hashing, fsync and result fetch; compact task/DataID materialization and remove per-edge copies

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.6956982659885469,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.45761619895498734,
      "name": "result-fetch",
      "seconds": 0.47682995548530016
    },
    {
      "fraction_of_gap": 0.25239095729270794,
      "name": "publication",
      "seconds": 0.262988
    },
    {
      "fraction_of_gap": 0.2496677937212976,
      "name": "graph-materialization",
      "seconds": 0.2601505
    },
    {
      "fraction_of_gap": 0.23211406203453475,
      "name": "fixed-control",
      "seconds": 0.24185974648665387
    },
    {
      "fraction_of_gap": 0.166752621387429,
      "name": "scheduler-submit",
      "seconds": 0.17375400000000002
    }
  ],
  "datavine_fetch_seconds": 0.48380602449469734,
  "datavine_median_seconds": 44.577834351483034,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.07198550000000001,
    "materialize_seconds": 0.2601505,
    "publication_commit_seconds": 15.488254,
    "publication_prepare_seconds": 0.2602235,
    "publication_queue_seconds": 0.040295,
    "publish_seconds": 0.0027645,
    "python_decode_seconds": 1.008028,
    "python_fsync_seconds": 1.76653,
    "python_function_seconds": 41125.385038,
    "python_serialize_seconds": 0.599928,
    "setup_seconds": 0.1713385,
    "submission_event_seconds": 0.013215500000000002,
    "submit_seconds": 0.1017685
  },
  "datavine_useful_cpu_seconds": 40960.004426411004,
  "fetch_excess_seconds": 0.47682995548530016,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.47682995548530016
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.601599,
  "parallel_publication_service_seconds": {
    "commit": 15.488254,
    "decode": 1.008028,
    "fsync": 1.76653,
    "function": 41125.385038,
    "queue": 0.040295,
    "serialize": 0.599928,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.006976069009397179,
  "taskvine_median_seconds": 43.53584773349576,
  "taskvine_useful_cpu_seconds": 40960.0053193965,
  "terminal_poll_residual_seconds": 0.04111274648665386,
  "useful_cpu_relative_difference": 2.18014008732517e-08,
  "worker_execution_excess_seconds": 0.0
}
```

### cpu_1000ms

DataVine excess: 1.092188 s; classification: `result-fetch+scheduler-submit+publication`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; bulk native submission/completion draining without semantic batching; batch retained-output serialization, hashing, fsync and result fetch

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.6764130534961392,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.41072577323366105,
      "name": "result-fetch",
      "seconds": 0.44858961398131214
    },
    {
      "fraction_of_gap": 0.253446376088679,
      "name": "scheduler-submit",
      "seconds": 0.27681100000000003
    },
    {
      "fraction_of_gap": 0.23824202900229655,
      "name": "publication",
      "seconds": 0.260205
    },
    {
      "fraction_of_gap": 0.23383527707367077,
      "name": "graph-materialization",
      "seconds": 0.25539199999999995
    },
    {
      "fraction_of_gap": 0.2252626929040629,
      "name": "fixed-control",
      "seconds": 0.24602912950567876
    }
  ],
  "datavine_fetch_seconds": 0.45536438349517994,
  "datavine_median_seconds": 8.403266176494071,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.17662,
    "materialize_seconds": 0.25539199999999995,
    "publication_commit_seconds": 14.315482,
    "publication_prepare_seconds": 0.2575425,
    "publication_queue_seconds": 0.03962,
    "publish_seconds": 0.0026625,
    "python_decode_seconds": 0.935422,
    "python_fsync_seconds": 1.58104,
    "python_function_seconds": 4114.289349000001,
    "python_serialize_seconds": 0.407993,
    "setup_seconds": 0.174485,
    "submission_event_seconds": 0.012917000000000001,
    "submit_seconds": 0.100191
  },
  "datavine_useful_cpu_seconds": 4096.004381326,
  "fetch_excess_seconds": 0.44858961398131214,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.44858961398131214
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.6045435000000001,
  "parallel_publication_service_seconds": {
    "commit": 14.315482,
    "decode": 0.935422,
    "fsync": 1.58104,
    "function": 4114.289349000001,
    "queue": 0.03962,
    "serialize": 0.407993,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0067747695138677955,
  "taskvine_median_seconds": 7.311078533995897,
  "taskvine_useful_cpu_seconds": 4096.090358871999,
  "terminal_poll_residual_seconds": 0.04318412950567874,
  "useful_cpu_relative_difference": 2.0990148767882515e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### degree_4

DataVine excess: 0.348218 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.06314750450837892,
  "client_wait_fetch_residual_seconds": 0.3374365985010285,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6327609214195449,
      "name": "fixed-control",
      "seconds": 0.2203389915247233
    },
    {
      "fraction_of_gap": 0.5679097089533818,
      "name": "result-fetch",
      "seconds": 0.19775660650338978
    },
    {
      "fraction_of_gap": 0.47356774679156993,
      "name": "graph-materialization",
      "seconds": 0.164905
    },
    {
      "fraction_of_gap": 0.2529001960966994,
      "name": "publication",
      "seconds": 0.0880645
    },
    {
      "fraction_of_gap": 0.23045164040299876,
      "name": "scheduler-submit",
      "seconds": 0.0802475
    }
  ],
  "datavine_fetch_seconds": 0.1994854120130185,
  "datavine_median_seconds": 2.7152195329981623,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.002059,
    "materialize_seconds": 0.164905,
    "publication_commit_seconds": 3.1514860000000002,
    "publication_prepare_seconds": 0.0851855,
    "publication_queue_seconds": 0.0229795,
    "publish_seconds": 0.002879,
    "python_decode_seconds": 0.589063,
    "python_fsync_seconds": 0.40414,
    "python_function_seconds": 20.835430000000002,
    "python_serialize_seconds": 0.149629,
    "setup_seconds": 0.108651,
    "submission_event_seconds": 0.008819,
    "submit_seconds": 0.0781885
  },
  "datavine_useful_cpu_seconds": 20.4822151085,
  "fetch_excess_seconds": 0.19775660650338978,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.2203389915247233
  },
  "logical_edge_payload_bytes": 4194304,
  "manager_transfer_excess_seconds": 0.12294000000000005,
  "parallel_publication_service_seconds": {
    "commit": 3.1514860000000002,
    "decode": 0.589063,
    "fsync": 0.40414,
    "function": 20.835430000000002,
    "queue": 0.0229795,
    "serialize": 0.149629,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.001728805509628728,
  "taskvine_median_seconds": 2.367001139500644,
  "taskvine_useful_cpu_seconds": 20.4823012865,
  "terminal_poll_residual_seconds": 0.031587487016344395,
  "useful_cpu_relative_difference": 4.20743737701504e-06,
  "worker_execution_excess_seconds": 0.0
}
```

### input_broadcast_0b

DataVine excess: 0.207493 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.030730853977729566,
  "client_wait_fetch_residual_seconds": 0.23636897849577015,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6956729143651589,
      "name": "result-fetch",
      "seconds": 0.14434759950381704
    },
    {
      "fraction_of_gap": 0.6146652081345496,
      "name": "fixed-control",
      "seconds": 0.12753902798372324
    },
    {
      "fraction_of_gap": 0.40676216307513974,
      "name": "graph-materialization",
      "seconds": 0.08440049999999999
    },
    {
      "fraction_of_gap": 0.3147761437324386,
      "name": "publication",
      "seconds": 0.06531400000000001
    },
    {
      "fraction_of_gap": 0.20894149700487094,
      "name": "scheduler-submit",
      "seconds": 0.043354
    }
  ],
  "datavine_fetch_seconds": 0.14603074600745458,
  "datavine_median_seconds": 1.4538089654961368,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0094615,
    "materialize_seconds": 0.08440049999999999,
    "publication_commit_seconds": 3.456553,
    "publication_prepare_seconds": 0.062782,
    "publication_queue_seconds": 0.010083,
    "publish_seconds": 0.002532,
    "python_decode_seconds": 0.291628,
    "python_fsync_seconds": 0.36662399999999995,
    "python_function_seconds": 10.4326845,
    "python_serialize_seconds": 0.072865,
    "setup_seconds": 0.067363,
    "submission_event_seconds": 0.0039335,
    "submit_seconds": 0.0338925
  },
  "datavine_useful_cpu_seconds": 10.251126893999999,
  "fetch_excess_seconds": 0.14434759950381704,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.14434759950381704
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.181198,
  "parallel_publication_service_seconds": {
    "commit": 3.456553,
    "decode": 0.291628,
    "fsync": 0.36662399999999995,
    "function": 10.4326845,
    "queue": 0.010083,
    "serialize": 0.072865,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016831465036375448,
  "taskvine_median_seconds": 1.2463154775032308,
  "taskvine_useful_cpu_seconds": 10.2514306835,
  "terminal_poll_residual_seconds": 0.019900674005993657,
  "useful_cpu_relative_difference": 2.963386373874056e-05,
  "worker_execution_excess_seconds": 0.21424200000000226
}
```

### input_broadcast_1024b

DataVine excess: 0.195629 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.03247566049685702,
  "client_wait_fetch_residual_seconds": 0.22630145300806404,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6871619897132459,
      "name": "result-fetch",
      "seconds": 0.1344289730041055
    },
    {
      "fraction_of_gap": 0.6669602134362597,
      "name": "fixed-control",
      "seconds": 0.13047691500551445
    },
    {
      "fraction_of_gap": 0.4367547665731707,
      "name": "graph-materialization",
      "seconds": 0.085442
    },
    {
      "fraction_of_gap": 0.3392821153430906,
      "name": "publication",
      "seconds": 0.0663735
    },
    {
      "fraction_of_gap": 0.24804575089491243,
      "name": "scheduler-submit",
      "seconds": 0.048525
    }
  ],
  "datavine_fetch_seconds": 0.1360758165101288,
  "datavine_median_seconds": 1.4269867635157425,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.014007499999999999,
    "materialize_seconds": 0.085442,
    "publication_commit_seconds": 3.3537264999999996,
    "publication_prepare_seconds": 0.063997,
    "publication_queue_seconds": 0.0101335,
    "publish_seconds": 0.0023765,
    "python_decode_seconds": 0.28702249999999996,
    "python_fsync_seconds": 0.362604,
    "python_function_seconds": 10.4313675,
    "python_serialize_seconds": 0.072116,
    "setup_seconds": 0.06296850000000001,
    "submission_event_seconds": 0.0040585,
    "submit_seconds": 0.0345175
  },
  "datavine_useful_cpu_seconds": 10.251098282000001,
  "fetch_excess_seconds": 0.1344289730041055,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.1344289730041055
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.18238249999999998,
  "parallel_publication_service_seconds": {
    "commit": 3.3537264999999996,
    "decode": 0.28702249999999996,
    "fsync": 0.362604,
    "function": 10.4313675,
    "queue": 0.0101335,
    "serialize": 0.072116,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016468435060232878,
  "taskvine_median_seconds": 1.2313575305015547,
  "taskvine_useful_cpu_seconds": 10.251430272,
  "terminal_poll_residual_seconds": 0.025265754508657423,
  "useful_cpu_relative_difference": 3.238474936577061e-05,
  "worker_execution_excess_seconds": 0.4752369999999999
}
```

### input_broadcast_1048576b

DataVine excess: 0.195126 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.018294682493433356,
  "client_wait_fetch_residual_seconds": 0.22638443249913487,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.7515563163875467,
      "name": "result-fetch",
      "seconds": 0.14664809399982914
    },
    {
      "fraction_of_gap": 0.586119127298212,
      "name": "fixed-control",
      "seconds": 0.1143670154863065
    },
    {
      "fraction_of_gap": 0.42067457387196217,
      "name": "graph-materialization",
      "seconds": 0.0820845
    },
    {
      "fraction_of_gap": 0.3356448521535298,
      "name": "publication",
      "seconds": 0.065493
    },
    {
      "fraction_of_gap": 0.2249522107771371,
      "name": "scheduler-submit",
      "seconds": 0.043894
    }
  ],
  "datavine_fetch_seconds": 0.1482716669997899,
  "datavine_median_seconds": 1.4419241525029065,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0093505,
    "materialize_seconds": 0.0820845,
    "publication_commit_seconds": 3.359674,
    "publication_prepare_seconds": 0.0636915,
    "publication_queue_seconds": 0.010537999999999999,
    "publish_seconds": 0.0018015000000000001,
    "python_decode_seconds": 0.978449,
    "python_fsync_seconds": 0.38304000000000005,
    "python_function_seconds": 10.4300405,
    "python_serialize_seconds": 0.0680545,
    "setup_seconds": 0.061524,
    "submission_event_seconds": 0.0040925,
    "submit_seconds": 0.034543500000000005
  },
  "datavine_useful_cpu_seconds": 10.2511007065,
  "fetch_excess_seconds": 0.14664809399982914,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.14664809399982914
  },
  "logical_edge_payload_bytes": 1073741824,
  "manager_transfer_excess_seconds": 0.18948950000000003,
  "parallel_publication_service_seconds": {
    "commit": 3.359674,
    "decode": 0.978449,
    "fsync": 0.38304000000000005,
    "function": 10.4300405,
    "queue": 0.010537999999999999,
    "serialize": 0.0680545,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016235729999607429,
  "taskvine_median_seconds": 1.2467982639936963,
  "taskvine_useful_cpu_seconds": 10.2514272875,
  "terminal_poll_residual_seconds": 0.024707332992873143,
  "useful_cpu_relative_difference": 3.1857124948611486e-05,
  "worker_execution_excess_seconds": 0.27474050000000005
}
```

### input_broadcast_65536b

DataVine excess: 0.198588 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.03745603249990381,
  "client_wait_fetch_residual_seconds": 0.22663619898476078,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6815333064709702,
      "name": "result-fetch",
      "seconds": 0.1353441635001218
    },
    {
      "fraction_of_gap": 0.6539471306091759,
      "name": "fixed-control",
      "seconds": 0.12986588700103943
    },
    {
      "fraction_of_gap": 0.416266871722365,
      "name": "graph-materialization",
      "seconds": 0.0826655
    },
    {
      "fraction_of_gap": 0.32800362130290805,
      "name": "publication",
      "seconds": 0.0651375
    },
    {
      "fraction_of_gap": 0.21568047754113231,
      "name": "scheduler-submit",
      "seconds": 0.042831499999999995
    }
  ],
  "datavine_fetch_seconds": 0.13703444799466524,
  "datavine_median_seconds": 1.4308188514987705,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.008146500000000001,
    "materialize_seconds": 0.0826655,
    "publication_commit_seconds": 3.363772,
    "publication_prepare_seconds": 0.062947,
    "publication_queue_seconds": 0.010065000000000001,
    "publish_seconds": 0.0021904999999999997,
    "python_decode_seconds": 0.354103,
    "python_fsync_seconds": 0.36554200000000003,
    "python_function_seconds": 10.4298015,
    "python_serialize_seconds": 0.06789899999999999,
    "setup_seconds": 0.060961,
    "submission_event_seconds": 0.004149,
    "submit_seconds": 0.034684999999999994
  },
  "datavine_useful_cpu_seconds": 10.2510999135,
  "fetch_excess_seconds": 0.1353441635001218,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.1353441635001218
  },
  "logical_edge_payload_bytes": 67108864,
  "manager_transfer_excess_seconds": 0.18197800000000003,
  "parallel_publication_service_seconds": {
    "commit": 3.363772,
    "decode": 0.354103,
    "fsync": 0.36554200000000003,
    "function": 10.4298015,
    "queue": 0.010065000000000001,
    "serialize": 0.06789899999999999,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.001690284494543448,
  "taskvine_median_seconds": 1.2322311049938435,
  "taskvine_useful_cpu_seconds": 10.251443072499999,
  "terminal_poll_residual_seconds": 0.021559354501135608,
  "useful_cpu_relative_difference": 3.34742140762949e-05,
  "worker_execution_excess_seconds": 0.3742200000000011
}
```

### interaction_c0_p0_d1

DataVine excess: 0.262255 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.039031125998008065,
  "client_wait_fetch_residual_seconds": 0.28225966749819464,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6831920654703062,
      "name": "fixed-control",
      "seconds": 0.17917065400942425
    },
    {
      "fraction_of_gap": 0.5981568069719091,
      "name": "result-fetch",
      "seconds": 0.1568697174952831
    },
    {
      "fraction_of_gap": 0.5867377091156557,
      "name": "graph-materialization",
      "seconds": 0.15387499999999998
    },
    {
      "fraction_of_gap": 0.3367788656020337,
      "name": "publication",
      "seconds": 0.08832199999999998
    },
    {
      "fraction_of_gap": 0.2971094861915725,
      "name": "scheduler-submit",
      "seconds": 0.0779185
    }
  ],
  "datavine_fetch_seconds": 0.15859130249009468,
  "datavine_median_seconds": 2.36403693149623,
  "datavine_stage_medians": {
    "manager_lock_seconds": 3e-06,
    "materialize_seconds": 0.15387499999999998,
    "publication_commit_seconds": 3.405598,
    "publication_prepare_seconds": 0.08599399999999999,
    "publication_queue_seconds": 0.025723000000000003,
    "publish_seconds": 0.002328,
    "python_decode_seconds": 0.489441,
    "python_fsync_seconds": 0.3699035,
    "python_function_seconds": 0.19652999999999998,
    "python_serialize_seconds": 0.136418,
    "setup_seconds": 0.0976705,
    "submission_event_seconds": 0.0087325,
    "submit_seconds": 0.0779155
  },
  "datavine_useful_cpu_seconds": 0.004579147,
  "fetch_excess_seconds": 0.1568697174952831,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.17917065400942425
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.033072500000000005,
  "parallel_publication_service_seconds": {
    "commit": 3.405598,
    "decode": 0.489441,
    "fsync": 0.3699035,
    "function": 0.19652999999999998,
    "queue": 0.025723000000000003,
    "serialize": 0.136418,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0017215849948115647,
  "taskvine_median_seconds": 2.1017817574902438,
  "taskvine_useful_cpu_seconds": 0.006061107499999999,
  "terminal_poll_residual_seconds": 0.026571028011416198,
  "useful_cpu_relative_difference": 0.24450325291211203,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c0_p65536_d16

DataVine excess: 0.291743 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.1541592429857701,
  "client_wait_fetch_residual_seconds": 0.4736999804802684,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.2183680230637606,
      "name": "fixed-control",
      "seconds": 0.355449813981329
    },
    {
      "fraction_of_gap": 0.9753003080955476,
      "name": "result-fetch",
      "seconds": 0.2845366149849724
    },
    {
      "fraction_of_gap": 0.7219121382660088,
      "name": "graph-materialization",
      "seconds": 0.21061249999999998
    },
    {
      "fraction_of_gap": 0.3062905803367512,
      "name": "publication",
      "seconds": 0.089358
    },
    {
      "fraction_of_gap": 0.29044955975631864,
      "name": "scheduler-submit",
      "seconds": 0.0847365
    }
  ],
  "datavine_fetch_seconds": 0.2864610084943706,
  "datavine_median_seconds": 3.7937216139835073,
  "datavine_stage_medians": {
    "manager_lock_seconds": 3.5e-06,
    "materialize_seconds": 0.21061249999999998,
    "publication_commit_seconds": 3.788753,
    "publication_prepare_seconds": 0.0855365,
    "publication_queue_seconds": 0.0398055,
    "publish_seconds": 0.0038215,
    "python_decode_seconds": 1.8601489999999998,
    "python_fsync_seconds": 0.6482574999999999,
    "python_function_seconds": 0.2921475,
    "python_serialize_seconds": 0.40007899999999996,
    "setup_seconds": 0.14551550000000002,
    "submission_event_seconds": 0.008632500000000001,
    "submit_seconds": 0.084733
  },
  "datavine_useful_cpu_seconds": 0.004780395999999999,
  "fetch_excess_seconds": 0.2845366149849724,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.355449813981329
  },
  "logical_edge_payload_bytes": 1073741824,
  "manager_transfer_excess_seconds": 0.2893905,
  "parallel_publication_service_seconds": {
    "commit": 3.788753,
    "decode": 1.8601489999999998,
    "fsync": 0.6482574999999999,
    "function": 0.2921475,
    "queue": 0.0398055,
    "serialize": 0.40007899999999996,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 67108864,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0019243935093982145,
  "taskvine_median_seconds": 3.5019790474907495,
  "taskvine_useful_cpu_seconds": 0.005494489,
  "terminal_poll_residual_seconds": 0.03718357099555891,
  "useful_cpu_relative_difference": 0.12996531615587922,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c100_p0_d16

DataVine excess: 0.222467 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.16041131351084914,
  "client_wait_fetch_residual_seconds": 0.4016573499911549,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.6834478513069095,
      "name": "fixed-control",
      "seconds": 0.3745122160066643
    },
    {
      "fraction_of_gap": 0.9233646264759888,
      "name": "graph-materialization",
      "seconds": 0.2054185
    },
    {
      "fraction_of_gap": 0.919899059755131,
      "name": "result-fetch",
      "seconds": 0.20464752448606305
    },
    {
      "fraction_of_gap": 0.4040120580441812,
      "name": "scheduler-submit",
      "seconds": 0.0898795
    },
    {
      "fraction_of_gap": 0.3955546379738634,
      "name": "publication",
      "seconds": 0.087998
    }
  ],
  "datavine_fetch_seconds": 0.20658683798683342,
  "datavine_median_seconds": 3.805679438999505,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0096445,
    "materialize_seconds": 0.2054185,
    "publication_commit_seconds": 3.1944895,
    "publication_prepare_seconds": 0.0855365,
    "publication_queue_seconds": 0.025775,
    "publish_seconds": 0.0024615,
    "python_decode_seconds": 0.8246325,
    "python_fsync_seconds": 0.459118,
    "python_function_seconds": 206.093402,
    "python_serialize_seconds": 0.16811500000000001,
    "setup_seconds": 0.14861200000000002,
    "submission_event_seconds": 0.00858,
    "submit_seconds": 0.080235
  },
  "datavine_useful_cpu_seconds": 204.8022561225,
  "fetch_excess_seconds": 0.20464752448606305,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.3745122160066643
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.1736855,
  "parallel_publication_service_seconds": {
    "commit": 3.1944895,
    "decode": 0.8246325,
    "fsync": 0.459118,
    "function": 206.093402,
    "queue": 0.025775,
    "serialize": 0.16811500000000001,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0019393135007703677,
  "taskvine_median_seconds": 3.5832120690029114,
  "taskvine_useful_cpu_seconds": 204.80238444600002,
  "terminal_poll_residual_seconds": 0.045401402495815146,
  "useful_cpu_relative_difference": 6.265722948220337e-07,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c100_p65536_d16

DataVine excess: 0.123404 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.14451464399462566,
  "client_wait_fetch_residual_seconds": 0.4741196740027145,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 2.881239830159602,
      "name": "fixed-control",
      "seconds": 0.3555567994846925
    },
    {
      "fraction_of_gap": 2.311770993998161,
      "name": "result-fetch",
      "seconds": 0.2852820119878743
    },
    {
      "fraction_of_gap": 1.6247475154578956,
      "name": "graph-materialization",
      "seconds": 0.2005005
    },
    {
      "fraction_of_gap": 0.7481153563249501,
      "name": "scheduler-submit",
      "seconds": 0.0923205
    },
    {
      "fraction_of_gap": 0.7238495493317572,
      "name": "publication",
      "seconds": 0.089326
    }
  ],
  "datavine_fetch_seconds": 0.2871998684859136,
  "datavine_median_seconds": 3.8279224284924567,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.011557999999999999,
    "materialize_seconds": 0.2005005,
    "publication_commit_seconds": 3.5733525,
    "publication_prepare_seconds": 0.0868955,
    "publication_queue_seconds": 0.031348,
    "publish_seconds": 0.0024305,
    "python_decode_seconds": 1.9610945,
    "python_fsync_seconds": 0.5998775000000001,
    "python_function_seconds": 206.1969815,
    "python_serialize_seconds": 0.4539165,
    "setup_seconds": 0.15125100000000002,
    "submission_event_seconds": 0.008384,
    "submit_seconds": 0.0807625
  },
  "datavine_useful_cpu_seconds": 204.8022369675,
  "fetch_excess_seconds": 0.2852820119878743,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.3555567994846925
  },
  "logical_edge_payload_bytes": 1073741824,
  "manager_transfer_excess_seconds": 0.26790450000000005,
  "parallel_publication_service_seconds": {
    "commit": 3.5733525,
    "decode": 1.9610945,
    "fsync": 0.5998775000000001,
    "function": 206.1969815,
    "queue": 0.031348,
    "serialize": 0.4539165,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 67108864,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0019178564980393276,
  "taskvine_median_seconds": 3.704518331491272,
  "taskvine_useful_cpu_seconds": 204.8024011835,
  "terminal_poll_residual_seconds": 0.039458155490066815,
  "useful_cpu_relative_difference": 8.018265364700509e-07,
  "worker_execution_excess_seconds": 0.0
}
```

### output_0b

DataVine excess: 0.151192 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.02481783648545388,
  "client_wait_fetch_residual_seconds": 0.22633725650572217,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.8954902961130281,
      "name": "result-fetch",
      "seconds": 0.13539063349890057
    },
    {
      "fraction_of_gap": 0.8274908915357367,
      "name": "fixed-control",
      "seconds": 0.12510969298706115
    },
    {
      "fraction_of_gap": 0.479414780778286,
      "name": "graph-materialization",
      "seconds": 0.0724835
    },
    {
      "fraction_of_gap": 0.4411388512720681,
      "name": "publication",
      "seconds": 0.0666965
    },
    {
      "fraction_of_gap": 0.44043775423946757,
      "name": "scheduler-submit",
      "seconds": 0.0665905
    }
  ],
  "datavine_fetch_seconds": 0.13708831700205337,
  "datavine_median_seconds": 1.3026885665021837,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.035833000000000004,
    "materialize_seconds": 0.0724835,
    "publication_commit_seconds": 3.448155,
    "publication_prepare_seconds": 0.0632965,
    "publication_queue_seconds": 0.010482499999999999,
    "publish_seconds": 0.0034000000000000002,
    "python_decode_seconds": 0.22794350000000002,
    "python_fsync_seconds": 0.3904395,
    "python_function_seconds": 10.4094625,
    "python_serialize_seconds": 0.076904,
    "setup_seconds": 0.064177,
    "submission_event_seconds": 0.0036095,
    "submit_seconds": 0.0307575
  },
  "datavine_useful_cpu_seconds": 10.2410925595,
  "fetch_excess_seconds": 0.13539063349890057,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.13539063349890057
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.13784200000000002,
  "parallel_publication_service_seconds": {
    "commit": 3.448155,
    "decode": 0.22794350000000002,
    "fsync": 0.3904395,
    "function": 10.4094625,
    "queue": 0.010482499999999999,
    "serialize": 0.076904,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016976835031528026,
  "taskvine_median_seconds": 1.15149694099091,
  "taskvine_useful_cpu_seconds": 10.24114418,
  "terminal_poll_residual_seconds": 0.027289856501607257,
  "useful_cpu_relative_difference": 5.0405012459224185e-06,
  "worker_execution_excess_seconds": 0.0
}
```

### output_1024b

DataVine excess: 0.197224 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.03790055352146737,
  "client_wait_fetch_residual_seconds": 0.24005864601327198,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.7965131287977067,
      "name": "result-fetch",
      "seconds": 0.1570914615149377
    },
    {
      "fraction_of_gap": 0.6829121434433162,
      "name": "fixed-control",
      "seconds": 0.134686627026151
    },
    {
      "fraction_of_gap": 0.3625345796415867,
      "name": "graph-materialization",
      "seconds": 0.0715005
    },
    {
      "fraction_of_gap": 0.32860614362125573,
      "name": "publication",
      "seconds": 0.064809
    },
    {
      "fraction_of_gap": 0.30765027033843,
      "name": "scheduler-submit",
      "seconds": 0.060676
    }
  ],
  "datavine_fetch_seconds": 0.1587751560145989,
  "datavine_median_seconds": 1.328263916511787,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0314445,
    "materialize_seconds": 0.0715005,
    "publication_commit_seconds": 3.531379,
    "publication_prepare_seconds": 0.0628085,
    "publication_queue_seconds": 0.0101955,
    "publish_seconds": 0.0020005,
    "python_decode_seconds": 0.223391,
    "python_fsync_seconds": 0.3660065,
    "python_function_seconds": 10.407334500000001,
    "python_serialize_seconds": 0.077224,
    "setup_seconds": 0.06463350000000001,
    "submission_event_seconds": 0.0038155,
    "submit_seconds": 0.0292315
  },
  "datavine_useful_cpu_seconds": 10.241090122,
  "fetch_excess_seconds": 0.1570914615149377,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.1570914615149377
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.13904399999999997,
  "parallel_publication_service_seconds": {
    "commit": 3.531379,
    "decode": 0.223391,
    "fsync": 0.3660065,
    "function": 10.407334500000001,
    "queue": 0.0101955,
    "serialize": 0.077224,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016836944996612146,
  "taskvine_median_seconds": 1.131039971500286,
  "taskvine_useful_cpu_seconds": 10.2411444575,
  "terminal_poll_residual_seconds": 0.02311107350468361,
  "useful_cpu_relative_difference": 5.30560819906847e-06,
  "worker_execution_excess_seconds": 0.0
}
```

### output_33554432b

DataVine excess: 2.020323 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.02908862401091028,
  "client_wait_fetch_residual_seconds": 4.947280784003331,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 2.4356715703241556,
      "name": "result-fetch",
      "seconds": 4.920844295018469
    },
    {
      "fraction_of_gap": 1.6633114192081893,
      "name": "scheduler-submit",
      "seconds": 3.360427
    },
    {
      "fraction_of_gap": 0.7557470708359358,
      "name": "publication",
      "seconds": 1.5268535
    },
    {
      "fraction_of_gap": 0.030425754446691246,
      "name": "fixed-control",
      "seconds": 0.061469864005805855
    },
    {
      "fraction_of_gap": 0.006806336017872672,
      "name": "graph-materialization",
      "seconds": 0.013751
    }
  ],
  "datavine_fetch_seconds": 4.921117643010803,
  "datavine_median_seconds": 10.36331337899901,
  "datavine_stage_medians": {
    "manager_lock_seconds": 3.354295,
    "materialize_seconds": 0.013751,
    "publication_commit_seconds": 15.6218505,
    "publication_prepare_seconds": 0.007977000000000001,
    "publication_queue_seconds": 109.0444155,
    "publish_seconds": 1.5188765000000002,
    "python_decode_seconds": 0.031066999999999997,
    "python_fsync_seconds": 17.203958,
    "python_function_seconds": 1.8681640000000002,
    "python_serialize_seconds": 4.386971,
    "setup_seconds": 0.0166415,
    "submission_event_seconds": 0.000673,
    "submit_seconds": 0.006132
  },
  "datavine_useful_cpu_seconds": 1.280134269,
  "fetch_excess_seconds": 4.920844295018469,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 4.920844295018469
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 3.7654110000000003,
  "parallel_publication_service_seconds": {
    "commit": 15.6218505,
    "decode": 0.031066999999999997,
    "fsync": 17.203958,
    "function": 1.8681640000000002,
    "queue": 109.0444155,
    "serialize": 4.386971,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 4294967296,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.00027334799233358353,
  "taskvine_median_seconds": 8.342989968004986,
  "taskvine_useful_cpu_seconds": 1.2801573705,
  "terminal_poll_residual_seconds": 0.013061739994895571,
  "useful_cpu_relative_difference": 1.8045828218013388e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### output_65536b

DataVine excess: 0.215181 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.03679156201542355,
  "client_wait_fetch_residual_seconds": 0.334903686516583,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.1664540134192505,
      "name": "result-fetch",
      "seconds": 0.25099866349773947
    },
    {
      "fraction_of_gap": 0.5881927592923241,
      "name": "fixed-control",
      "seconds": 0.12656786702516773
    },
    {
      "fraction_of_gap": 0.3601550506255422,
      "name": "scheduler-submit",
      "seconds": 0.0774985
    },
    {
      "fraction_of_gap": 0.34556965056762806,
      "name": "graph-materialization",
      "seconds": 0.07436000000000001
    },
    {
      "fraction_of_gap": 0.30104897745792014,
      "name": "publication",
      "seconds": 0.06478
    }
  ],
  "datavine_fetch_seconds": 0.2528459900058806,
  "datavine_median_seconds": 1.5168214900040766,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0466375,
    "materialize_seconds": 0.07436000000000001,
    "publication_commit_seconds": 4.000336,
    "publication_prepare_seconds": 0.061963000000000004,
    "publication_queue_seconds": 0.17600149999999998,
    "publish_seconds": 0.002817,
    "python_decode_seconds": 0.223078,
    "python_fsync_seconds": 0.5022665,
    "python_function_seconds": 10.452423,
    "python_serialize_seconds": 0.212201,
    "setup_seconds": 0.057563,
    "submission_event_seconds": 0.0038374999999999998,
    "submit_seconds": 0.030861
  },
  "datavine_useful_cpu_seconds": 10.2410825005,
  "fetch_excess_seconds": 0.25099866349773947,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.25099866349773947
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.23132199999999997,
  "parallel_publication_service_seconds": {
    "commit": 4.000336,
    "decode": 0.223078,
    "fsync": 0.5022665,
    "function": 10.452423,
    "queue": 0.17600149999999998,
    "serialize": 0.212201,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 67108864,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0018473265081411228,
  "taskvine_median_seconds": 1.3016405564994784,
  "taskvine_useful_cpu_seconds": 10.241149077500001,
  "terminal_poll_residual_seconds": 0.02271180500974418,
  "useful_cpu_relative_difference": 6.50093065710472e-06,
  "worker_execution_excess_seconds": 0.0
}
```

### topology_broadcast1024

DataVine excess: 0.238744 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.034926150998217054,
  "client_wait_fetch_residual_seconds": 0.24764644900604615,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6643515427744531,
      "name": "result-fetch",
      "seconds": 0.15861000849690754
    },
    {
      "fraction_of_gap": 0.5659839751787091,
      "name": "fixed-control",
      "seconds": 0.13512533249687309
    },
    {
      "fraction_of_gap": 0.3432126757372947,
      "name": "graph-materialization",
      "seconds": 0.08194
    },
    {
      "fraction_of_gap": 0.2769010882838534,
      "name": "publication",
      "seconds": 0.06610850000000001
    },
    {
      "fraction_of_gap": 0.20449720357803355,
      "name": "scheduler-submit",
      "seconds": 0.0488225
    }
  ],
  "datavine_fetch_seconds": 0.1602491654921323,
  "datavine_median_seconds": 1.4771744209865574,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.015505,
    "materialize_seconds": 0.08194,
    "publication_commit_seconds": 3.3247025,
    "publication_prepare_seconds": 0.06374550000000001,
    "publication_queue_seconds": 0.010354499999999999,
    "publish_seconds": 0.0023629999999999996,
    "python_decode_seconds": 0.284995,
    "python_fsync_seconds": 0.397363,
    "python_function_seconds": 10.430746,
    "python_serialize_seconds": 0.0721725,
    "setup_seconds": 0.0625815,
    "submission_event_seconds": 0.0040355,
    "submit_seconds": 0.0333175
  },
  "datavine_useful_cpu_seconds": 10.251094573,
  "fetch_excess_seconds": 0.15861000849690754,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.15861000849690754
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.18221900000000002,
  "parallel_publication_service_seconds": {
    "commit": 3.3247025,
    "decode": 0.284995,
    "fsync": 0.397363,
    "function": 10.430746,
    "queue": 0.010354499999999999,
    "serialize": 0.0721725,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016391569952247664,
  "taskvine_median_seconds": 1.2384303250000812,
  "taskvine_useful_cpu_seconds": 10.251446078499999,
  "terminal_poll_residual_seconds": 0.027896181498656025,
  "useful_cpu_relative_difference": 3.4288382078759135e-05,
  "worker_execution_excess_seconds": 0.26165599999999856
}
```

### topology_dynamic

DataVine excess: 0.312784 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.20534371149555408,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6823592686131364,
      "name": "fixed-control",
      "seconds": 0.21343090249654462
    },
    {
      "fraction_of_gap": 0.05030312202579429,
      "name": "publication",
      "seconds": 0.015733999999999998
    },
    {
      "fraction_of_gap": 0.020680421051471363,
      "name": "scheduler-submit",
      "seconds": 0.0064685
    },
    {
      "fraction_of_gap": 0.008171779579123569,
      "name": "graph-materialization",
      "seconds": 0.002556
    },
    {
      "fraction_of_gap": 0.0,
      "name": "result-fetch",
      "seconds": 0.0
    }
  ],
  "datavine_fetch_seconds": 0.0,
  "datavine_median_seconds": 0.4314326540043112,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0045695,
    "materialize_seconds": 0.002556,
    "publication_commit_seconds": 0.019525499999999998,
    "publication_prepare_seconds": 0.000653,
    "publication_queue_seconds": 0.0001505,
    "publish_seconds": 0.015080999999999997,
    "python_decode_seconds": 0.0023500000000000005,
    "python_fsync_seconds": 0.0052625,
    "python_function_seconds": 0.081644,
    "python_serialize_seconds": 0.000821,
    "setup_seconds": 0.06386049999999999,
    "submission_event_seconds": 5.7e-05,
    "submit_seconds": 0.001899
  },
  "datavine_useful_cpu_seconds": 0.010001659,
  "fetch_excess_seconds": 0.0,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.21343090249654462
  },
  "logical_edge_payload_bytes": 7168,
  "manager_transfer_excess_seconds": 0.0018260000000000001,
  "parallel_publication_service_seconds": {
    "commit": 0.019525499999999998,
    "decode": 0.0023500000000000005,
    "fsync": 0.0052625,
    "function": 0.081644,
    "queue": 0.0001505,
    "serialize": 0.000821,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1024,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0,
  "taskvine_median_seconds": 0.11864888698619325,
  "taskvine_useful_cpu_seconds": 0.010000882499999999,
  "terminal_poll_residual_seconds": 0.13967490249654463,
  "useful_cpu_relative_difference": 7.763712000185963e-05,
  "worker_execution_excess_seconds": 0.02710549999999999
}
```

### topology_fanin16

DataVine excess: 0.200514 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.04312897349882405,
  "client_wait_fetch_residual_seconds": 0.18312179301628845,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.7383964976764056,
      "name": "fixed-control",
      "seconds": 0.14805893501462042
    },
    {
      "fraction_of_gap": 0.44189003930829157,
      "name": "result-fetch",
      "seconds": 0.08860519899462815
    },
    {
      "fraction_of_gap": 0.2612209857495223,
      "name": "graph-materialization",
      "seconds": 0.052378499999999995
    },
    {
      "fraction_of_gap": 0.19358734984467307,
      "name": "scheduler-submit",
      "seconds": 0.038817000000000004
    },
    {
      "fraction_of_gap": 0.1307937717244093,
      "name": "publication",
      "seconds": 0.026226000000000003
    }
  ],
  "datavine_fetch_seconds": 0.08869200649496634,
  "datavine_median_seconds": 1.337667526997393,
  "datavine_stage_medians": {
    "manager_lock_seconds": 3e-06,
    "materialize_seconds": 0.052378499999999995,
    "publication_commit_seconds": 0.1912955,
    "publication_prepare_seconds": 0.024258500000000002,
    "publication_queue_seconds": 0.043371,
    "publish_seconds": 0.0019675,
    "python_decode_seconds": 0.2634655,
    "python_fsync_seconds": 0.042137,
    "python_function_seconds": 11.057758,
    "python_serialize_seconds": 0.0804665,
    "setup_seconds": 0.07008500000000001,
    "submission_event_seconds": 0.003945499999999999,
    "submit_seconds": 0.038814
  },
  "datavine_useful_cpu_seconds": 10.881161645,
  "fetch_excess_seconds": 0.08860519899462815,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.14805893501462042
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.0,
  "parallel_publication_service_seconds": {
    "commit": 0.1912955,
    "decode": 0.2634655,
    "fsync": 0.042137,
    "function": 11.057758,
    "queue": 0.043371,
    "serialize": 0.0804665,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 65536,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 8.680750033818185e-05,
  "taskvine_median_seconds": 1.1371533920028014,
  "taskvine_useful_cpu_seconds": 10.8812057935,
  "terminal_poll_residual_seconds": 0.026787461515796362,
  "useful_cpu_relative_difference": 4.057316885346458e-06,
  "worker_execution_excess_seconds": 0.0
}
```

### topology_fanout16

DataVine excess: 0.303202 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.029132342009688728,
  "client_wait_fetch_residual_seconds": 0.25440527100573296,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.5416435964805387,
      "name": "result-fetch",
      "seconds": 0.16422729851910844
    },
    {
      "fraction_of_gap": 0.4303146430523334,
      "name": "fixed-control",
      "seconds": 0.13047216250850352
    },
    {
      "fraction_of_gap": 0.31822703146722536,
      "name": "graph-materialization",
      "seconds": 0.09648699999999999
    },
    {
      "fraction_of_gap": 0.22326386630470796,
      "name": "publication",
      "seconds": 0.067694
    },
    {
      "fraction_of_gap": 0.15162675211332602,
      "name": "scheduler-submit",
      "seconds": 0.0459735
    }
  ],
  "datavine_fetch_seconds": 0.16588606750883628,
  "datavine_median_seconds": 1.565099736006232,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.004826,
    "materialize_seconds": 0.09648699999999999,
    "publication_commit_seconds": 3.3069984999999997,
    "publication_prepare_seconds": 0.065538,
    "publication_queue_seconds": 0.0111625,
    "publish_seconds": 0.002156,
    "python_decode_seconds": 0.3358175,
    "python_fsync_seconds": 0.44839249999999997,
    "python_function_seconds": 11.0854795,
    "python_serialize_seconds": 0.0845175,
    "setup_seconds": 0.0710755,
    "submission_event_seconds": 0.004758,
    "submit_seconds": 0.041147500000000004
  },
  "datavine_useful_cpu_seconds": 10.881154693,
  "fetch_excess_seconds": 0.16422729851910844,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.16422729851910844
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.180178,
  "parallel_publication_service_seconds": {
    "commit": 3.3069984999999997,
    "decode": 0.3358175,
    "fsync": 0.44839249999999997,
    "function": 11.0854795,
    "queue": 0.0111625,
    "serialize": 0.0845175,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016587689897278324,
  "taskvine_median_seconds": 1.2618979635008145,
  "taskvine_useful_cpu_seconds": 10.881376813,
  "terminal_poll_residual_seconds": 0.019955320498814766,
  "useful_cpu_relative_difference": 2.04128580249823e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### topology_heavy_tail

DataVine excess: 0.408838 s; classification: `result-fetch+fixed-control+graph-materialization`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; reduce RPC polling, workflow setup, checkpoint and completion projection; compact task/DataID materialization and remove per-edge copies

Evidence:

```json
{
  "build_submit_excess_seconds": 0.04491895448882133,
  "client_wait_fetch_residual_seconds": 0.244534807506253,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.3915801279483244,
      "name": "result-fetch",
      "seconds": 0.16009266150649637
    },
    {
      "fraction_of_gap": 0.33524530786839846,
      "name": "fixed-control",
      "seconds": 0.1370608714886047
    },
    {
      "fraction_of_gap": 0.17482249218427057,
      "name": "graph-materialization",
      "seconds": 0.071474
    },
    {
      "fraction_of_gap": 0.1626391691077693,
      "name": "publication",
      "seconds": 0.066493
    },
    {
      "fraction_of_gap": 0.14693611065542278,
      "name": "scheduler-submit",
      "seconds": 0.060073
    }
  ],
  "datavine_fetch_seconds": 0.16259559549507685,
  "datavine_median_seconds": 2.2867806190042756,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.029540999999999998,
    "materialize_seconds": 0.071474,
    "publication_commit_seconds": 2.954763,
    "publication_prepare_seconds": 0.0646475,
    "publication_queue_seconds": 0.010974,
    "publish_seconds": 0.0018455,
    "python_decode_seconds": 0.2256545,
    "python_fsync_seconds": 0.4002785,
    "python_function_seconds": 140.00919249999998,
    "python_serialize_seconds": 0.0801765,
    "setup_seconds": 0.058658,
    "submission_event_seconds": 0.00357,
    "submit_seconds": 0.030532
  },
  "datavine_useful_cpu_seconds": 139.349270492,
  "fetch_excess_seconds": 0.16009266150649637,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.16009266150649637
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.13766799999999998,
  "parallel_publication_service_seconds": {
    "commit": 2.954763,
    "decode": 0.2256545,
    "fsync": 0.4002785,
    "function": 140.00919249999998,
    "queue": 0.010974,
    "serialize": 0.0801765,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0025029339885804802,
  "taskvine_median_seconds": 1.877943065512227,
  "taskvine_useful_cpu_seconds": 139.3530517205,
  "terminal_poll_residual_seconds": 0.0238424169997834,
  "useful_cpu_relative_difference": 2.7134163574594348e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### topology_map

DataVine excess: 0.139870 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.031794428010471165,
  "client_wait_fetch_residual_seconds": 0.25295041950426433,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.2146892987268525,
      "name": "result-fetch",
      "seconds": 0.16989874650607817
    },
    {
      "fraction_of_gap": 0.8976225351149008,
      "name": "fixed-control",
      "seconds": 0.12555057800498792
    },
    {
      "fraction_of_gap": 0.5211370115375207,
      "name": "graph-materialization",
      "seconds": 0.0728915
    },
    {
      "fraction_of_gap": 0.47783970332090797,
      "name": "scheduler-submit",
      "seconds": 0.06683549999999999
    },
    {
      "fraction_of_gap": 0.46954987028306744,
      "name": "publication",
      "seconds": 0.065676
    }
  ],
  "datavine_fetch_seconds": 0.17160553150461055,
  "datavine_median_seconds": 1.3316926085099112,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.037371,
    "materialize_seconds": 0.0728915,
    "publication_commit_seconds": 3.5228975,
    "publication_prepare_seconds": 0.063467,
    "publication_queue_seconds": 0.010439,
    "publish_seconds": 0.002209,
    "python_decode_seconds": 0.22295199999999998,
    "python_fsync_seconds": 0.41967350000000003,
    "python_function_seconds": 10.409439,
    "python_serialize_seconds": 0.077183,
    "setup_seconds": 0.0656745,
    "submission_event_seconds": 0.003831,
    "submit_seconds": 0.029464499999999998
  },
  "datavine_useful_cpu_seconds": 10.2410895915,
  "fetch_excess_seconds": 0.16989874650607817,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.16989874650607817
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.1344345,
  "parallel_publication_service_seconds": {
    "commit": 3.5228975,
    "decode": 0.22295199999999998,
    "fsync": 0.41967350000000003,
    "function": 10.409439,
    "queue": 0.010439,
    "serialize": 0.077183,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0017067849985323846,
  "taskvine_median_seconds": 1.1918224814871792,
  "taskvine_useful_cpu_seconds": 10.241148476,
  "terminal_poll_residual_seconds": 0.018648649994516764,
  "useful_cpu_relative_difference": 5.749794579931503e-06,
  "worker_execution_excess_seconds": 0.0
}
```

