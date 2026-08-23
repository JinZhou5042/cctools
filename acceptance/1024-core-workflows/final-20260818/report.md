# DataVine versus TaskVine fixed-core result

Source artifact: `acceptance/1024-core-workflows/final-20260818/compact-summary.json`

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
| `admission_immediate` | admission | independent | 1024 | 0 | 0 | 0/0 | 1.1432 | 1.3891 | 0.836 | [0.798, 0.899] | result-fetch |
| `confirmation_input_broadcast_0b_w128` | confirmation | broadcast | 129 | 128 | 10.0 | 1/128 | 0.2324 | 0.4750 | 0.511 | [0.444, 0.597] | result-fetch+fixed-control |
| `cpu_0ms` | cpu | independent | 4096 | 0 | 0 | 0/0 | 5.2472 | 4.6236 | 1.138 | [1.102, 1.186] | NOT_REQUIRED |
| `cpu_10000ms` | cpu | independent | 4096 | 0 | 10000 | 0/0 | 43.6188 | 44.3975 | 0.983 | [0.981, 0.988] | result-fetch |
| `cpu_1000ms` | cpu | independent | 4096 | 0 | 1000 | 0/0 | 7.5223 | 8.2776 | 0.904 | [0.876, 0.915] | result-fetch |
| `cpu_100ms` | cpu | independent | 4096 | 0 | 100 | 0/0 | 4.8596 | 4.7439 | 1.011 | [0.969, 1.174] | NOT_REQUIRED |
| `cpu_10ms` | cpu | independent | 4096 | 0 | 10 | 0/0 | 5.4198 | 4.6459 | 1.151 | [1.122, 1.229] | NOT_REQUIRED |
| `cpu_1ms` | cpu | independent | 4096 | 0 | 1 | 0/0 | 5.2251 | 4.6555 | 1.118 | [1.067, 1.199] | NOT_REQUIRED |
| `degree_1` | degree | regular-pipeline | 2048 | 1024 | 10.0 | 1/1 | 2.2359 | 2.3925 | 0.931 | [0.877, 0.976] | result-fetch |
| `degree_16` | degree | regular-pipeline | 2048 | 16384 | 10.0 | 16/16 | 3.6006 | 3.5399 | 1.035 | [0.942, 1.115] | NOT_REQUIRED |
| `degree_4` | degree | regular-pipeline | 2048 | 4096 | 10.0 | 4/4 | 2.4693 | 2.5651 | 0.963 | [0.833, 1.041] | NOT_REQUIRED |
| `degree_64` | degree | regular-pipeline | 2048 | 65536 | 10.0 | 64/64 | 16.4523 | 10.4305 | 1.486 | [1.117, 1.663] | NOT_REQUIRED |
| `input_broadcast_0b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.3288 | 1.4367 | 0.922 | [0.856, 0.950] | result-fetch |
| `input_broadcast_1024b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.2803 | 1.4377 | 0.907 | [0.847, 0.935] | result-fetch |
| `input_broadcast_1048576b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.3923 | 1.4258 | 0.947 | [0.909, 1.019] | NOT_REQUIRED |
| `input_broadcast_33554432b` | data | broadcast | 129 | 128 | 10.0 | 1/128 | 0.5468 | 0.8058 | 0.671 | [0.620, 0.710] | zero-payload-control:result-fetch+fixed-control+payload-path-residual |
| `input_broadcast_65536b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.3109 | 1.3960 | 0.919 | [0.874, 0.952] | result-fetch |
| `interaction_c0_p0_d1` | interaction | regular-pipeline | 2048 | 1024 | 0 | 1/1 | 2.2885 | 2.2388 | 1.028 | [0.937, 1.059] | NOT_REQUIRED |
| `interaction_c0_p0_d16` | interaction | regular-pipeline | 2048 | 16384 | 0 | 16/16 | 3.8446 | 3.5544 | 1.087 | [0.976, 1.151] | NOT_REQUIRED |
| `interaction_c0_p0_d64` | interaction | regular-pipeline | 2048 | 65536 | 0 | 64/64 | 16.8578 | 10.6830 | 1.532 | [1.313, 1.740] | NOT_REQUIRED |
| `interaction_c0_p1048576_d1` | interaction | regular-pipeline | 2048 | 1024 | 0 | 1/1 | 5.0130 | 6.0714 | 0.830 | [0.770, 0.878] | result-fetch |
| `interaction_c0_p1048576_d16` | interaction | regular-pipeline | 2048 | 16384 | 0 | 16/16 | 6.7830 | 7.7420 | 0.863 | [0.764, 0.926] | result-fetch |
| `interaction_c0_p1048576_d64` | interaction | regular-pipeline | 2048 | 65536 | 0 | 64/64 | 19.4607 | 16.3977 | 1.159 | [1.070, 1.343] | NOT_REQUIRED |
| `interaction_c0_p65536_d1` | interaction | regular-pipeline | 2048 | 1024 | 0 | 1/1 | 2.5288 | 2.7199 | 0.917 | [0.882, 1.004] | NOT_REQUIRED |
| `interaction_c0_p65536_d16` | interaction | regular-pipeline | 2048 | 16384 | 0 | 16/16 | 4.0661 | 3.8251 | 1.037 | [0.931, 1.146] | NOT_REQUIRED |
| `interaction_c0_p65536_d64` | interaction | regular-pipeline | 2048 | 65536 | 0 | 64/64 | 17.8035 | 10.5255 | 1.654 | [1.260, 1.735] | NOT_REQUIRED |
| `interaction_c100_p0_d1` | interaction | regular-pipeline | 2048 | 1024 | 100 | 1/1 | 2.3329 | 2.3153 | 1.025 | [0.957, 1.146] | NOT_REQUIRED |
| `interaction_c100_p0_d16` | interaction | regular-pipeline | 2048 | 16384 | 100 | 16/16 | 3.9268 | 3.3775 | 1.152 | [0.965, 1.174] | NOT_REQUIRED |
| `interaction_c100_p0_d64` | interaction | regular-pipeline | 2048 | 65536 | 100 | 64/64 | 13.4724 | 8.9796 | 1.422 | [1.311, 1.797] | NOT_REQUIRED |
| `interaction_c100_p1048576_d1` | interaction | regular-pipeline | 2048 | 1024 | 100 | 1/1 | 4.9132 | 5.8852 | 0.817 | [0.776, 0.919] | result-fetch |
| `interaction_c100_p1048576_d16` | interaction | regular-pipeline | 2048 | 16384 | 100 | 16/16 | 6.5030 | 6.9600 | 0.933 | [0.875, 0.963] | result-fetch |
| `interaction_c100_p1048576_d64` | interaction | regular-pipeline | 2048 | 65536 | 100 | 64/64 | 16.1383 | 13.3540 | 1.226 | [1.099, 1.411] | NOT_REQUIRED |
| `interaction_c100_p65536_d1` | interaction | regular-pipeline | 2048 | 1024 | 100 | 1/1 | 2.4574 | 2.5681 | 0.949 | [0.912, 0.998] | result-fetch |
| `interaction_c100_p65536_d16` | interaction | regular-pipeline | 2048 | 16384 | 100 | 16/16 | 3.8234 | 3.6960 | 1.046 | [0.930, 1.182] | NOT_REQUIRED |
| `interaction_c100_p65536_d64` | interaction | regular-pipeline | 2048 | 65536 | 100 | 64/64 | 12.9988 | 9.0409 | 1.368 | [1.259, 1.778] | NOT_REQUIRED |
| `output_0b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 1.1886 | 1.3933 | 0.850 | [0.824, 0.882] | result-fetch |
| `output_1024b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 1.1921 | 1.3625 | 0.877 | [0.833, 0.923] | result-fetch |
| `output_1048576b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 3.6530 | 5.1022 | 0.730 | [0.683, 0.843] | result-fetch |
| `output_33554432b` | data | independent | 128 | 0 | 10.0 | 0/0 | 8.1706 | 18.6010 | 0.429 | [0.422, 0.453] | result-fetch |
| `output_65536b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 1.3935 | 1.6678 | 0.848 | [0.806, 0.869] | result-fetch |
| `topology_broadcast1024` | topology | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.3687 | 1.4668 | 0.920 | [0.837, 0.958] | result-fetch |
| `topology_chains` | topology | regular-pipeline | 4096 | 3072 | 10.0 | 1/1 | 4.4392 | 3.8594 | 1.136 | [1.061, 1.210] | NOT_REQUIRED |
| `topology_dynamic` | topology | dynamic | 8 | 7 | 10.0 | 1/1 | 0.1314 | 0.4811 | 0.274 | [0.245, 0.288] | fixed-control |
| `topology_fanin16` | topology | fan-in | 1088 | 1024 | 10.0 | 16/1 | 1.2663 | 1.2008 | 1.019 | [0.941, 1.153] | NOT_REQUIRED |
| `topology_fanout16` | topology | fan-out | 1088 | 1024 | 10.0 | 1/16 | 1.3101 | 1.5438 | 0.839 | [0.827, 0.880] | result-fetch |
| `topology_heavy_tail` | topology | heavy-tail | 1024 | 0 | mixed-0-1000 | 0/0 | 1.9640 | 2.3070 | 0.843 | [0.812, 0.878] | result-fetch |
| `topology_map` | topology | independent | 1024 | 0 | 10.0 | 0/0 | 1.2013 | 1.3675 | 0.881 | [0.848, 0.928] | result-fetch |
| `topology_pipeline2` | topology | regular-pipeline | 4096 | 6144 | 10.0 | 2/2 | 4.5958 | 4.0145 | 1.150 | [1.003, 1.203] | NOT_REQUIRED |
| `topology_reduce16` | topology | reduction-tree | 1093 | 1092 | 10.0 | 16/1 | 1.3072 | 1.1399 | 1.106 | [0.962, 1.280] | NOT_REQUIRED |

## DataVine regressions and causes

### admission_immediate

DataVine excess: 0.245896 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.3510201800102237,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.9156356892884688,
      "name": "result-fetch",
      "seconds": 0.22515141899930313
    },
    {
      "fraction_of_gap": 0.5047709401880814,
      "name": "fixed-control",
      "seconds": 0.12412130149849783
    },
    {
      "fraction_of_gap": 0.256925389141592,
      "name": "publication",
      "seconds": 0.063177
    },
    {
      "fraction_of_gap": 0.20718490708972484,
      "name": "graph-materialization",
      "seconds": 0.050946
    },
    {
      "fraction_of_gap": 0.11753329014643225,
      "name": "scheduler-submit",
      "seconds": 0.028901
    }
  ],
  "datavine_fetch_seconds": 0.22683527099434286,
  "datavine_median_seconds": 1.3891428235074272,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0006305,
    "materialize_seconds": 0.050946,
    "publication_commit_seconds": 3.1958085,
    "publication_prepare_seconds": 0.061734,
    "publication_queue_seconds": 0.0094795,
    "publish_seconds": 0.001443,
    "python_decode_seconds": 0.18704500000000002,
    "python_fsync_seconds": 5.5167745,
    "python_function_seconds": 0.0889195,
    "python_serialize_seconds": 0.052319000000000004,
    "setup_seconds": 0.081257,
    "submission_event_seconds": 0.004193499999999999,
    "submit_seconds": 0.0282705
  },
  "datavine_useful_cpu_seconds": 0.0020223515,
  "fetch_excess_seconds": 0.22515141899930313,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.22515141899930313
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.2699725,
  "parallel_publication_service_seconds": {
    "commit": 3.1958085,
    "decode": 0.18704500000000002,
    "fsync": 5.5167745,
    "function": 0.0889195,
    "queue": 0.0094795,
    "serialize": 0.052319000000000004,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016838519950397313,
  "taskvine_median_seconds": 1.1432465334946755,
  "taskvine_useful_cpu_seconds": 0.0031215525000000003,
  "terminal_poll_residual_seconds": 0.03124430149849783,
  "useful_cpu_relative_difference": 0.3521327928971241,
  "worker_execution_excess_seconds": 2.7029050000000012
}
```

### confirmation_input_broadcast_0b_w128

DataVine excess: 0.242598 s; classification: `result-fetch+fixed-control`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.006338914507068694,
  "client_wait_fetch_residual_seconds": 0.2040048694892507,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.4186000079464598,
      "name": "result-fetch",
      "seconds": 0.10155161598231643
    },
    {
      "fraction_of_gap": 0.39469450062171374,
      "name": "fixed-control",
      "seconds": 0.09575218250496312
    },
    {
      "fraction_of_gap": 0.046723344027330464,
      "name": "graph-materialization",
      "seconds": 0.011335
    },
    {
      "fraction_of_gap": 0.03849780957593762,
      "name": "publication",
      "seconds": 0.0093395
    },
    {
      "fraction_of_gap": 0.029268557941425667,
      "name": "scheduler-submit",
      "seconds": 0.007100499999999999
    }
  ],
  "datavine_fetch_seconds": 0.10172005949425511,
  "datavine_median_seconds": 0.4750311889947625,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0006605000000000001,
    "materialize_seconds": 0.011335,
    "publication_commit_seconds": 0.36631,
    "publication_prepare_seconds": 0.007521,
    "publication_queue_seconds": 0.001279,
    "publish_seconds": 0.0018185,
    "python_decode_seconds": 0.037746,
    "python_fsync_seconds": 1.48361,
    "python_function_seconds": 1.3112135,
    "python_serialize_seconds": 0.007782,
    "setup_seconds": 0.060289999999999996,
    "submission_event_seconds": 0.0008179999999999999,
    "submit_seconds": 0.0064399999999999995
  },
  "datavine_useful_cpu_seconds": 1.2901239015,
  "fetch_excess_seconds": 0.10155161598231643,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.10155161598231643
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.04283,
  "parallel_publication_service_seconds": {
    "commit": 0.36631,
    "decode": 0.037746,
    "fsync": 1.48361,
    "function": 1.3112135,
    "queue": 0.001279,
    "serialize": 0.007782,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.00016844351193867624,
  "taskvine_median_seconds": 0.2324329709954327,
  "taskvine_useful_cpu_seconds": 1.290155501,
  "terminal_poll_residual_seconds": 0.026074767997894432,
  "useful_cpu_relative_difference": 2.4492783990540976e-05,
  "worker_execution_excess_seconds": 1.3419409999999998
}
```

### cpu_10000ms

DataVine excess: 0.778675 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.9411322994946474,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.8757597004227284,
      "name": "result-fetch",
      "seconds": 0.6819320064969361
    },
    {
      "fraction_of_gap": 0.36540286048637716,
      "name": "fixed-control",
      "seconds": 0.28452999802447665
    },
    {
      "fraction_of_gap": 0.35112604290521043,
      "name": "publication",
      "seconds": 0.27341299999999996
    },
    {
      "fraction_of_gap": 0.24233479862410787,
      "name": "graph-materialization",
      "seconds": 0.18869999999999998
    },
    {
      "fraction_of_gap": 0.22936404363680798,
      "name": "scheduler-submit",
      "seconds": 0.17859999999999998
    }
  ],
  "datavine_fetch_seconds": 0.69215946199256,
  "datavine_median_seconds": 44.39747374599392,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.07764,
    "materialize_seconds": 0.18869999999999998,
    "publication_commit_seconds": 14.005885,
    "publication_prepare_seconds": 0.2698965,
    "publication_queue_seconds": 0.036109,
    "publish_seconds": 0.0035165,
    "python_decode_seconds": 0.9770415,
    "python_fsync_seconds": 19.992506,
    "python_function_seconds": 41109.2875605,
    "python_serialize_seconds": 0.462398,
    "setup_seconds": 0.1912885,
    "submission_event_seconds": 0.015352,
    "submit_seconds": 0.10096
  },
  "datavine_useful_cpu_seconds": 40960.004188733496,
  "fetch_excess_seconds": 0.6819320064969361,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.6819320064969361
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 1.096747,
  "parallel_publication_service_seconds": {
    "commit": 14.005885,
    "decode": 0.9770415,
    "fsync": 19.992506,
    "function": 41109.2875605,
    "queue": 0.036109,
    "serialize": 0.462398,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.010227455495623872,
  "taskvine_median_seconds": 43.618798949508346,
  "taskvine_useful_cpu_seconds": 40960.0050201435,
  "terminal_poll_residual_seconds": 0.05905299802447672,
  "useful_cpu_relative_difference": 2.029809333859965e-08,
  "worker_execution_excess_seconds": 0.0
}
```

### cpu_1000ms

DataVine excess: 0.755243 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.9060983680069707,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.8750001116114109,
      "name": "result-fetch",
      "seconds": 0.6608381104888394
    },
    {
      "fraction_of_gap": 0.3539810951008314,
      "name": "fixed-control",
      "seconds": 0.2673419065106243
    },
    {
      "fraction_of_gap": 0.3276963172759104,
      "name": "publication",
      "seconds": 0.2474905
    },
    {
      "fraction_of_gap": 0.24859665831565034,
      "name": "graph-materialization",
      "seconds": 0.187751
    },
    {
      "fraction_of_gap": 0.24382402512115994,
      "name": "scheduler-submit",
      "seconds": 0.1841465
    }
  ],
  "datavine_fetch_seconds": 0.6685634854948148,
  "datavine_median_seconds": 8.27757664550154,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0827155,
    "materialize_seconds": 0.187751,
    "publication_commit_seconds": 14.090685,
    "publication_prepare_seconds": 0.244833,
    "publication_queue_seconds": 0.035824,
    "publish_seconds": 0.0026575,
    "python_decode_seconds": 0.9209165,
    "python_fsync_seconds": 16.104215,
    "python_function_seconds": 4112.109362499999,
    "python_serialize_seconds": 0.38097400000000003,
    "setup_seconds": 0.1868785,
    "submission_event_seconds": 0.015196999999999999,
    "submit_seconds": 0.101431
  },
  "datavine_useful_cpu_seconds": 4096.004442861,
  "fetch_excess_seconds": 0.6608381104888394,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.6608381104888394
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 1.075876,
  "parallel_publication_service_seconds": {
    "commit": 14.090685,
    "decode": 0.9209165,
    "fsync": 16.104215,
    "function": 4112.109362499999,
    "queue": 0.035824,
    "serialize": 0.38097400000000003,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.007725375005975366,
  "taskvine_median_seconds": 7.52233318699291,
  "taskvine_useful_cpu_seconds": 4096.0798272745,
  "terminal_poll_residual_seconds": 0.04864190651062428,
  "useful_cpu_relative_difference": 1.8404039149272507e-05,
  "worker_execution_excess_seconds": 7.0185355000003256
}
```

### degree_1

DataVine excess: 0.156552 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.4054597414968387,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.5632457125940074,
      "name": "result-fetch",
      "seconds": 0.24472934598452412
    },
    {
      "fraction_of_gap": 1.108165573480298,
      "name": "fixed-control",
      "seconds": 0.1734856100071277
    },
    {
      "fraction_of_gap": 0.7075186091407927,
      "name": "graph-materialization",
      "seconds": 0.1107635
    },
    {
      "fraction_of_gap": 0.542381855220843,
      "name": "publication",
      "seconds": 0.084911
    },
    {
      "fraction_of_gap": 0.5233306853701032,
      "name": "scheduler-submit",
      "seconds": 0.08192849999999999
    }
  ],
  "datavine_fetch_seconds": 0.24640447150159162,
  "datavine_median_seconds": 2.3924983510078164,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.002796,
    "materialize_seconds": 0.1107635,
    "publication_commit_seconds": 3.1118629999999996,
    "publication_prepare_seconds": 0.0828795,
    "publication_queue_seconds": 0.0401885,
    "publish_seconds": 0.0020315,
    "python_decode_seconds": 0.4361935,
    "python_fsync_seconds": 5.811529999999999,
    "python_function_seconds": 20.7751685,
    "python_serialize_seconds": 0.10240850000000001,
    "setup_seconds": 0.1160185,
    "submission_event_seconds": 0.009663999999999999,
    "submit_seconds": 0.0791325
  },
  "datavine_useful_cpu_seconds": 20.481928724,
  "fetch_excess_seconds": 0.24472934598452412,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.24472934598452412
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.24545699999999998,
  "parallel_publication_service_seconds": {
    "commit": 3.1118629999999996,
    "decode": 0.4361935,
    "fsync": 5.811529999999999,
    "function": 20.7751685,
    "queue": 0.0401885,
    "serialize": 0.10240850000000001,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016751255170674995,
  "taskvine_median_seconds": 2.2359462849999545,
  "taskvine_useful_cpu_seconds": 20.482107818,
  "terminal_poll_residual_seconds": 0.039265610007127694,
  "useful_cpu_relative_difference": 8.743924287057243e-06,
  "worker_execution_excess_seconds": 2.158079999999998
}
```

### input_broadcast_0b

DataVine excess: 0.107882 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.34503101051064394,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.8354462426230391,
      "name": "result-fetch",
      "seconds": 0.198010753505514
    },
    {
      "fraction_of_gap": 1.2026254722248622,
      "name": "fixed-control",
      "seconds": 0.12974107898679377
    },
    {
      "fraction_of_gap": 0.5961956462776782,
      "name": "publication",
      "seconds": 0.0643185
    },
    {
      "fraction_of_gap": 0.5560451228380494,
      "name": "graph-materialization",
      "seconds": 0.059987
    },
    {
      "fraction_of_gap": 0.3910632247795863,
      "name": "scheduler-submit",
      "seconds": 0.0421885
    }
  ],
  "datavine_fetch_seconds": 0.19967458200699184,
  "datavine_median_seconds": 1.4367112915060716,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.008903,
    "materialize_seconds": 0.059987,
    "publication_commit_seconds": 3.237699,
    "publication_prepare_seconds": 0.0622945,
    "publication_queue_seconds": 0.009284,
    "publish_seconds": 0.002024,
    "python_decode_seconds": 0.2886645,
    "python_fsync_seconds": 5.873412500000001,
    "python_function_seconds": 10.418815500000001,
    "python_serialize_seconds": 0.0576225,
    "setup_seconds": 0.0832445,
    "submission_event_seconds": 0.0048015,
    "submit_seconds": 0.033285499999999996
  },
  "datavine_useful_cpu_seconds": 10.250986651,
  "fetch_excess_seconds": 0.198010753505514,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.198010753505514
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.2741265,
  "parallel_publication_service_seconds": {
    "commit": 3.237699,
    "decode": 0.2886645,
    "fsync": 5.873412500000001,
    "function": 10.418815500000001,
    "queue": 0.009284,
    "serialize": 0.0576225,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016638285014778376,
  "taskvine_median_seconds": 1.328829758989741,
  "taskvine_useful_cpu_seconds": 10.251239871500001,
  "terminal_poll_residual_seconds": 0.03443907898679377,
  "useful_cpu_relative_difference": 2.470145106107573e-05,
  "worker_execution_excess_seconds": 3.2731434999999998
}
```

### input_broadcast_1024b

DataVine excess: 0.157389 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.33400372150730806,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.3286125841401089,
      "name": "result-fetch",
      "seconds": 0.2091085410065716
    },
    {
      "fraction_of_gap": 0.7908848160791955,
      "name": "fixed-control",
      "seconds": 0.12447629351757747
    },
    {
      "fraction_of_gap": 0.4038410647477639,
      "name": "publication",
      "seconds": 0.06356
    },
    {
      "fraction_of_gap": 0.3938530510003837,
      "name": "graph-materialization",
      "seconds": 0.061988
    },
    {
      "fraction_of_gap": 0.31081974460239514,
      "name": "scheduler-submit",
      "seconds": 0.048919500000000005
    }
  ],
  "datavine_fetch_seconds": 0.2107926675089402,
  "datavine_median_seconds": 1.4377350165013922,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.014040500000000001,
    "materialize_seconds": 0.061988,
    "publication_commit_seconds": 3.2573594999999997,
    "publication_prepare_seconds": 0.061564,
    "publication_queue_seconds": 0.0093265,
    "publish_seconds": 0.001996,
    "python_decode_seconds": 0.284516,
    "python_fsync_seconds": 5.703098499999999,
    "python_function_seconds": 10.416232,
    "python_serialize_seconds": 0.056942000000000006,
    "setup_seconds": 0.08266599999999999,
    "submission_event_seconds": 0.004711,
    "submit_seconds": 0.034879
  },
  "datavine_useful_cpu_seconds": 10.250977071000001,
  "fetch_excess_seconds": 0.2091085410065716,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.2091085410065716
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.27365900000000004,
  "parallel_publication_service_seconds": {
    "commit": 3.2573594999999997,
    "decode": 0.284516,
    "fsync": 5.703098499999999,
    "function": 10.416232,
    "queue": 0.0093265,
    "serialize": 0.056942000000000006,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016841265023685992,
  "taskvine_median_seconds": 1.280346366489539,
  "taskvine_useful_cpu_seconds": 10.2512823665,
  "terminal_poll_residual_seconds": 0.029680793517577464,
  "useful_cpu_relative_difference": 2.9781200935052043e-05,
  "worker_execution_excess_seconds": 3.3730720000000005
}
```

### input_broadcast_33554432b

DataVine excess: 0.258967 s; classification: `zero-payload-control:result-fetch+fixed-control+payload-path-residual`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; reduce RPC polling, workflow setup, checkpoint and completion projection; reduce parent-payload decode/materialization and preserve worker-local reuse

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.13479902151176798,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.28903956463992103,
      "name": "result-fetch",
      "seconds": 0.07485171500593424
    },
    {
      "fraction_of_gap": 0.21582937387430204,
      "name": "fixed-control",
      "seconds": 0.05589268999652096
    },
    {
      "fraction_of_gap": 0.06970385622963449,
      "name": "scheduler-submit",
      "seconds": 0.018051
    },
    {
      "fraction_of_gap": 0.0454691101381611,
      "name": "graph-materialization",
      "seconds": 0.011775
    },
    {
      "fraction_of_gap": 0.03638880334496485,
      "name": "publication",
      "seconds": 0.0094235
    }
  ],
  "datavine_fetch_seconds": 0.0751161049993243,
  "datavine_median_seconds": 0.8057901935098926,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.011755,
    "materialize_seconds": 0.011775,
    "publication_commit_seconds": 0.32869349999999997,
    "publication_prepare_seconds": 0.007763,
    "publication_queue_seconds": 0.001398,
    "publish_seconds": 0.0016605,
    "python_decode_seconds": 1.993465,
    "python_fsync_seconds": 1.4098095,
    "python_function_seconds": 1.3185635,
    "python_serialize_seconds": 0.0415895,
    "setup_seconds": 0.032799499999999995,
    "submission_event_seconds": 0.0007975,
    "submit_seconds": 0.006296
  },
  "datavine_useful_cpu_seconds": 1.2901195699999999,
  "fetch_excess_seconds": 0.07485171500593424,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.07485171500593424
  },
  "logical_edge_payload_bytes": 4294967296,
  "manager_transfer_excess_seconds": 0.15156,
  "matched_zero_payload_control": {
    "case_id": "confirmation_input_broadcast_0b_w128",
    "causal_constraint": "same graph, CPU, degree and sinks; only payload bytes change",
    "control_absolute_excess_seconds": 0.2425982179993298,
    "fraction_reproduced_without_payload": 0.9367919400028147,
    "payload_independent_control_seconds": 0.2425982179993298,
    "payload_specific_fraction": 0.0632080599971853,
    "payload_specific_residual_seconds": 0.016368803000659682,
    "target_absolute_excess_seconds": 0.2589670209999895
  },
  "parallel_publication_service_seconds": {
    "commit": 0.32869349999999997,
    "decode": 1.993465,
    "fsync": 1.4098095,
    "function": 1.3185635,
    "queue": 0.001398,
    "serialize": 0.0415895,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0002643899933900684,
  "taskvine_median_seconds": 0.5468231725099031,
  "taskvine_useful_cpu_seconds": 1.2901560385,
  "terminal_poll_residual_seconds": 0.02007868999652096,
  "useful_cpu_relative_difference": 2.8266735892236098e-05,
  "worker_execution_excess_seconds": 1.1285319999999999
}
```

### input_broadcast_65536b

DataVine excess: 0.085092 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.3136729214855805,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 2.4505536509102432,
      "name": "result-fetch",
      "seconds": 0.20852288746391423
    },
    {
      "fraction_of_gap": 1.3317964736460595,
      "name": "fixed-control",
      "seconds": 0.11332542998835442
    },
    {
      "fraction_of_gap": 0.7518143254832698,
      "name": "graph-materialization",
      "seconds": 0.0639735
    },
    {
      "fraction_of_gap": 0.7395100182501031,
      "name": "publication",
      "seconds": 0.0629265
    },
    {
      "fraction_of_gap": 0.7270823153856584,
      "name": "scheduler-submit",
      "seconds": 0.061869
    }
  ],
  "datavine_fetch_seconds": 0.21014378497784492,
  "datavine_median_seconds": 1.396033115001046,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0264355,
    "materialize_seconds": 0.0639735,
    "publication_commit_seconds": 3.213735,
    "publication_prepare_seconds": 0.0610435,
    "publication_queue_seconds": 0.0092355,
    "publish_seconds": 0.0018830000000000001,
    "python_decode_seconds": 0.3499355,
    "python_fsync_seconds": 5.8029209999999996,
    "python_function_seconds": 10.414818499999999,
    "python_serialize_seconds": 0.052268999999999996,
    "setup_seconds": 0.0728815,
    "submission_event_seconds": 0.0049485,
    "submit_seconds": 0.0354335
  },
  "datavine_useful_cpu_seconds": 10.250982628500001,
  "fetch_excess_seconds": 0.20852288746391423,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.20852288746391423
  },
  "logical_edge_payload_bytes": 67108864,
  "manager_transfer_excess_seconds": 0.273315,
  "parallel_publication_service_seconds": {
    "commit": 3.213735,
    "decode": 0.3499355,
    "fsync": 5.8029209999999996,
    "function": 10.414818499999999,
    "queue": 0.0092355,
    "serialize": 0.052268999999999996,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016208975139306858,
  "taskvine_median_seconds": 1.310940961484448,
  "taskvine_useful_cpu_seconds": 10.251236159000001,
  "terminal_poll_residual_seconds": 0.028595929988354418,
  "useful_cpu_relative_difference": 2.473170026208248e-05,
  "worker_execution_excess_seconds": 3.1921360000000014
}
```

### interaction_c0_p1048576_d1

DataVine excess: 1.058347 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 2.34633872100324,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 2.078754405439915,
      "name": "result-fetch",
      "seconds": 2.2000428100145655
    },
    {
      "fraction_of_gap": 0.16393620667544076,
      "name": "fixed-control",
      "seconds": 0.17350133900066922
    },
    {
      "fraction_of_gap": 0.10434718865331527,
      "name": "graph-materialization",
      "seconds": 0.11043549999999999
    },
    {
      "fraction_of_gap": 0.10104203346400299,
      "name": "publication",
      "seconds": 0.1069375
    },
    {
      "fraction_of_gap": 0.09002815654455798,
      "name": "scheduler-submit",
      "seconds": 0.095281
    }
  ],
  "datavine_fetch_seconds": 2.2022245495172683,
  "datavine_median_seconds": 6.071394658996724,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0168845,
    "materialize_seconds": 0.11043549999999999,
    "publication_commit_seconds": 7.1131495000000005,
    "publication_prepare_seconds": 0.083482,
    "publication_queue_seconds": 37.74585999999999,
    "publish_seconds": 0.0234555,
    "python_decode_seconds": 1.049485,
    "python_fsync_seconds": 45.4746645,
    "python_function_seconds": 0.8753095,
    "python_serialize_seconds": 3.322435,
    "setup_seconds": 0.1159385,
    "submission_event_seconds": 0.010082,
    "submit_seconds": 0.07839650000000001
  },
  "datavine_useful_cpu_seconds": 0.0040295339999999995,
  "fetch_excess_seconds": 2.2000428100145655,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 2.2000428100145655
  },
  "logical_edge_payload_bytes": 1073741824,
  "manager_transfer_excess_seconds": 1.868281,
  "parallel_publication_service_seconds": {
    "commit": 7.1131495000000005,
    "decode": 1.049485,
    "fsync": 45.4746645,
    "function": 0.8753095,
    "queue": 37.74585999999999,
    "serialize": 3.322435,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1073741824,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0021817395027028397,
  "taskvine_median_seconds": 5.013047985499725,
  "taskvine_useful_cpu_seconds": 0.0060626924999999995,
  "terminal_poll_residual_seconds": 0.037073339000669225,
  "useful_cpu_relative_difference": 0.3353557021076032,
  "worker_execution_excess_seconds": 41.9642615
}
```

### interaction_c0_p1048576_d16

DataVine excess: 0.958968 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 2.399941730488924,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 2.27924992331655,
      "name": "result-fetch",
      "seconds": 2.18572690396104
    },
    {
      "fraction_of_gap": 0.24344871972968468,
      "name": "fixed-control",
      "seconds": 0.23345944251421222
    },
    {
      "fraction_of_gap": 0.21465427290560682,
      "name": "scheduler-submit",
      "seconds": 0.2058465
    },
    {
      "fraction_of_gap": 0.1682934798293392,
      "name": "graph-materialization",
      "seconds": 0.161388
    },
    {
      "fraction_of_gap": 0.11776153450310657,
      "name": "publication",
      "seconds": 0.1129295
    }
  ],
  "datavine_fetch_seconds": 2.1888719174748985,
  "datavine_median_seconds": 7.741972026502481,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.121909,
    "materialize_seconds": 0.161388,
    "publication_commit_seconds": 8.125231,
    "publication_prepare_seconds": 0.0845765,
    "publication_queue_seconds": 31.675711,
    "publish_seconds": 0.028353,
    "python_decode_seconds": 10.579791499999999,
    "python_fsync_seconds": 48.618799,
    "python_function_seconds": 0.917046,
    "python_serialize_seconds": 3.541562,
    "setup_seconds": 0.16354649999999998,
    "submission_event_seconds": 0.0102,
    "submit_seconds": 0.0839375
  },
  "datavine_useful_cpu_seconds": 0.0041554439999999995,
  "fetch_excess_seconds": 2.18572690396104,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 2.18572690396104
  },
  "logical_edge_payload_bytes": 17179869184,
  "manager_transfer_excess_seconds": 2.2030095,
  "parallel_publication_service_seconds": {
    "commit": 8.125231,
    "decode": 10.579791499999999,
    "fsync": 48.618799,
    "function": 0.917046,
    "queue": 31.675711,
    "serialize": 3.541562,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1073741824,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.00314501351385843,
  "taskvine_median_seconds": 6.783004393510055,
  "taskvine_useful_cpu_seconds": 0.005709637,
  "terminal_poll_residual_seconds": 0.04652794251421222,
  "useful_cpu_relative_difference": 0.27220522075221254,
  "worker_execution_excess_seconds": 47.8525525
}
```

### interaction_c100_p1048576_d1

DataVine excess: 0.971958 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 2.319420933497498,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 2.2276666883881773,
      "name": "result-fetch",
      "seconds": 2.1651982329931343
    },
    {
      "fraction_of_gap": 0.18043174172138662,
      "name": "fixed-control",
      "seconds": 0.17537205650531523
    },
    {
      "fraction_of_gap": 0.1465356680783532,
      "name": "publication",
      "seconds": 0.1424265
    },
    {
      "fraction_of_gap": 0.11415412146122751,
      "name": "graph-materialization",
      "seconds": 0.110953
    },
    {
      "fraction_of_gap": 0.07941804899113691,
      "name": "scheduler-submit",
      "seconds": 0.07719100000000001
    }
  ],
  "datavine_fetch_seconds": 2.168574376992183,
  "datavine_median_seconds": 5.885167339001782,
  "datavine_stage_medians": {
    "manager_lock_seconds": 4e-06,
    "materialize_seconds": 0.110953,
    "publication_commit_seconds": 6.853624,
    "publication_prepare_seconds": 0.082889,
    "publication_queue_seconds": 111.06866350000001,
    "publish_seconds": 0.05953749999999999,
    "python_decode_seconds": 1.1759089999999999,
    "python_fsync_seconds": 38.182333,
    "python_function_seconds": 206.68748349999998,
    "python_serialize_seconds": 3.013348,
    "setup_seconds": 0.118413,
    "submission_event_seconds": 0.009670999999999999,
    "submit_seconds": 0.077187
  },
  "datavine_useful_cpu_seconds": 204.8020118805,
  "fetch_excess_seconds": 2.1651982329931343,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 2.1651982329931343
  },
  "logical_edge_payload_bytes": 1073741824,
  "manager_transfer_excess_seconds": 1.896157,
  "parallel_publication_service_seconds": {
    "commit": 6.853624,
    "decode": 1.1759089999999999,
    "fsync": 38.182333,
    "function": 206.68748349999998,
    "queue": 111.06866350000001,
    "serialize": 3.013348,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1073741824,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0033761439990485087,
  "taskvine_median_seconds": 4.913209440506762,
  "taskvine_useful_cpu_seconds": 204.80238026900003,
  "terminal_poll_residual_seconds": 0.03724955650531525,
  "useful_cpu_relative_difference": 1.7987510669433937e-06,
  "worker_execution_excess_seconds": 38.290449499999994
}
```

### interaction_c100_p1048576_d16

DataVine excess: 0.456931 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 2.4026678655005402,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 4.802891502709012,
      "name": "result-fetch",
      "seconds": 2.194591532505001
    },
    {
      "fraction_of_gap": 0.5148435782532346,
      "name": "fixed-control",
      "seconds": 0.23524815348459033
    },
    {
      "fraction_of_gap": 0.3472316180138994,
      "name": "graph-materialization",
      "seconds": 0.158661
    },
    {
      "fraction_of_gap": 0.24380578923486823,
      "name": "scheduler-submit",
      "seconds": 0.1114025
    },
    {
      "fraction_of_gap": 0.217191285943782,
      "name": "publication",
      "seconds": 0.09924150000000001
    }
  ],
  "datavine_fetch_seconds": 2.1982434569945326,
  "datavine_median_seconds": 6.9599547294928925,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.028961,
    "materialize_seconds": 0.158661,
    "publication_commit_seconds": 7.3856015,
    "publication_prepare_seconds": 0.08416950000000001,
    "publication_queue_seconds": 57.032576000000006,
    "publish_seconds": 0.015071999999999999,
    "python_decode_seconds": 11.651928999999999,
    "python_fsync_seconds": 26.297067,
    "python_function_seconds": 206.702864,
    "python_serialize_seconds": 3.0403445,
    "setup_seconds": 0.1636205,
    "submission_event_seconds": 0.009829000000000001,
    "submit_seconds": 0.0824415
  },
  "datavine_useful_cpu_seconds": 204.802039073,
  "fetch_excess_seconds": 2.194591532505001,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 2.194591532505001
  },
  "logical_edge_payload_bytes": 17179869184,
  "manager_transfer_excess_seconds": 1.9362964999999999,
  "parallel_publication_service_seconds": {
    "commit": 7.3856015,
    "decode": 11.651928999999999,
    "fsync": 26.297067,
    "function": 206.702864,
    "queue": 57.032576000000006,
    "serialize": 3.0403445,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1073741824,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.003651924489531666,
  "taskvine_median_seconds": 6.503023413999472,
  "taskvine_useful_cpu_seconds": 204.802407828,
  "terminal_poll_residual_seconds": 0.04819165348459031,
  "useful_cpu_relative_difference": 1.8005403545869026e-06,
  "worker_execution_excess_seconds": 27.265288500000025
}
```

### interaction_c100_p65536_d1

DataVine excess: 0.110617 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.5773919575050268,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 3.787849794353378,
      "name": "result-fetch",
      "seconds": 0.41900104850356
    },
    {
      "fraction_of_gap": 1.5914081421982693,
      "name": "fixed-control",
      "seconds": 0.17603699100534337
    },
    {
      "fraction_of_gap": 1.0075926445459895,
      "name": "graph-materialization",
      "seconds": 0.111457
    },
    {
      "fraction_of_gap": 0.7835088931738574,
      "name": "publication",
      "seconds": 0.08666949999999998
    },
    {
      "fraction_of_gap": 0.7039913671196258,
      "name": "scheduler-submit",
      "seconds": 0.07787350000000001
    }
  ],
  "datavine_fetch_seconds": 0.42083044099854305,
  "datavine_median_seconds": 2.5680643880041316,
  "datavine_stage_medians": {
    "manager_lock_seconds": 3e-06,
    "materialize_seconds": 0.111457,
    "publication_commit_seconds": 3.4154725,
    "publication_prepare_seconds": 0.08183399999999999,
    "publication_queue_seconds": 6.767316,
    "publish_seconds": 0.0048354999999999995,
    "python_decode_seconds": 0.595559,
    "python_fsync_seconds": 6.059858,
    "python_function_seconds": 205.9770555,
    "python_serialize_seconds": 0.403483,
    "setup_seconds": 0.12004100000000001,
    "submission_event_seconds": 0.010159000000000001,
    "submit_seconds": 0.07787050000000001
  },
  "datavine_useful_cpu_seconds": 204.80203871999998,
  "fetch_excess_seconds": 0.41900104850356,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.41900104850356
  },
  "logical_edge_payload_bytes": 67108864,
  "manager_transfer_excess_seconds": 0.37573049999999997,
  "parallel_publication_service_seconds": {
    "commit": 3.4154725,
    "decode": 0.595559,
    "fsync": 6.059858,
    "function": 205.9770555,
    "queue": 6.767316,
    "serialize": 0.403483,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 67108864,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0018293924949830398,
  "taskvine_median_seconds": 2.457447264503571,
  "taskvine_useful_cpu_seconds": 204.80236224150002,
  "terminal_poll_residual_seconds": 0.03701749100534335,
  "useful_cpu_relative_difference": 1.579676603800121e-06,
  "worker_execution_excess_seconds": 3.3980165000000113
}
```

### output_0b

DataVine excess: 0.204766 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.35477333200406935,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.0656666408051267,
      "name": "result-fetch",
      "seconds": 0.21821245951286983
    },
    {
      "fraction_of_gap": 0.6727772109183456,
      "name": "fixed-control",
      "seconds": 0.13776200199697075
    },
    {
      "fraction_of_gap": 0.3128153688497595,
      "name": "publication",
      "seconds": 0.064054
    },
    {
      "fraction_of_gap": 0.24496968377552084,
      "name": "graph-materialization",
      "seconds": 0.0501615
    },
    {
      "fraction_of_gap": 0.16472937219644732,
      "name": "scheduler-submit",
      "seconds": 0.033731
    }
  ],
  "datavine_fetch_seconds": 0.2198586705053458,
  "datavine_median_seconds": 1.3933208945236402,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0044545,
    "materialize_seconds": 0.0501615,
    "publication_commit_seconds": 3.2823089999999997,
    "publication_prepare_seconds": 0.0620445,
    "publication_queue_seconds": 0.009194,
    "publish_seconds": 0.0020095,
    "python_decode_seconds": 0.2194875,
    "python_fsync_seconds": 5.191411,
    "python_function_seconds": 10.4081635,
    "python_serialize_seconds": 0.057370000000000004,
    "setup_seconds": 0.0898615,
    "submission_event_seconds": 0.004204,
    "submit_seconds": 0.0292765
  },
  "datavine_useful_cpu_seconds": 10.2409827005,
  "fetch_excess_seconds": 0.21821245951286983,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.21821245951286983
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.27336900000000003,
  "parallel_publication_service_seconds": {
    "commit": 3.2823089999999997,
    "decode": 0.2194875,
    "fsync": 5.191411,
    "function": 10.4081635,
    "queue": 0.009194,
    "serialize": 0.057370000000000004,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016462109924759716,
  "taskvine_median_seconds": 1.188554740496329,
  "taskvine_useful_cpu_seconds": 10.2410903735,
  "terminal_poll_residual_seconds": 0.036266501996970735,
  "useful_cpu_relative_difference": 1.0513821875753649e-05,
  "worker_execution_excess_seconds": 3.985613500000003
}
```

### output_1024b

DataVine excess: 0.170456 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.3351184595148759,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.3299077362428806,
      "name": "result-fetch",
      "seconds": 0.2266909705067519
    },
    {
      "fraction_of_gap": 0.6455677327641728,
      "name": "fixed-control",
      "seconds": 0.11004099899560775
    },
    {
      "fraction_of_gap": 0.3720428684070712,
      "name": "publication",
      "seconds": 0.063417
    },
    {
      "fraction_of_gap": 0.34454606275205846,
      "name": "scheduler-submit",
      "seconds": 0.058730000000000004
    },
    {
      "fraction_of_gap": 0.3153567398298233,
      "name": "graph-materialization",
      "seconds": 0.0537545
    }
  ],
  "datavine_fetch_seconds": 0.22826283751055598,
  "datavine_median_seconds": 1.3625490764970891,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0285475,
    "materialize_seconds": 0.0537545,
    "publication_commit_seconds": 3.2598314999999998,
    "publication_prepare_seconds": 0.061478500000000005,
    "publication_queue_seconds": 0.0092145,
    "publish_seconds": 0.0019385000000000001,
    "python_decode_seconds": 0.2158195,
    "python_fsync_seconds": 5.661958500000001,
    "python_function_seconds": 10.408951,
    "python_serialize_seconds": 0.057877,
    "setup_seconds": 0.0742375,
    "submission_event_seconds": 0.004374,
    "submit_seconds": 0.0301825
  },
  "datavine_useful_cpu_seconds": 10.240994702,
  "fetch_excess_seconds": 0.2266909705067519,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.2266909705067519
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.272654,
  "parallel_publication_service_seconds": {
    "commit": 3.2598314999999998,
    "decode": 0.2158195,
    "fsync": 5.661958500000001,
    "function": 10.408951,
    "queue": 0.0092145,
    "serialize": 0.057877,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0015718670038040727,
  "taskvine_median_seconds": 1.1920929130137665,
  "taskvine_useful_cpu_seconds": 10.241077556,
  "terminal_poll_residual_seconds": 0.024243498995607737,
  "useful_cpu_relative_difference": 8.090359588368461e-06,
  "worker_execution_excess_seconds": 4.679364999999997
}
```

### output_1048576b

DataVine excess: 1.449171 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 2.286528977492913,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.4838966946587533,
      "name": "result-fetch",
      "seconds": 2.150419610989047
    },
    {
      "fraction_of_gap": 0.15544614590760938,
      "name": "scheduler-submit",
      "seconds": 0.225268
    },
    {
      "fraction_of_gap": 0.07983146398837614,
      "name": "fixed-control",
      "seconds": 0.11568941851039609
    },
    {
      "fraction_of_gap": 0.06456554775229652,
      "name": "publication",
      "seconds": 0.0935665
    },
    {
      "fraction_of_gap": 0.03691835614558219,
      "name": "graph-materialization",
      "seconds": 0.053501
    }
  ],
  "datavine_fetch_seconds": 2.1526167139963945,
  "datavine_median_seconds": 5.102184462506557,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.1951675,
    "materialize_seconds": 0.053501,
    "publication_commit_seconds": 6.8452175,
    "publication_prepare_seconds": 0.062296000000000004,
    "publication_queue_seconds": 119.8018625,
    "publish_seconds": 0.0312705,
    "python_decode_seconds": 0.236122,
    "python_fsync_seconds": 57.640437500000004,
    "python_function_seconds": 10.8396945,
    "python_serialize_seconds": 1.7378490000000002,
    "setup_seconds": 0.0757705,
    "submission_event_seconds": 0.0042179999999999995,
    "submit_seconds": 0.030100500000000002
  },
  "datavine_useful_cpu_seconds": 10.241003181,
  "fetch_excess_seconds": 2.150419610989047,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 2.150419610989047
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 1.918715,
  "parallel_publication_service_seconds": {
    "commit": 6.8452175,
    "decode": 0.236122,
    "fsync": 57.640437500000004,
    "function": 10.8396945,
    "queue": 119.8018625,
    "serialize": 1.7378490000000002,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1073741824,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.002197103007347323,
  "taskvine_median_seconds": 3.6530137630034005,
  "taskvine_useful_cpu_seconds": 10.241080666999999,
  "terminal_poll_residual_seconds": 0.02595391851039608,
  "useful_cpu_relative_difference": 7.566193697531999e-06,
  "worker_execution_excess_seconds": 59.94047350000001
}
```

### output_33554432b

DataVine excess: 10.430457 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.013980710005853325,
  "client_wait_fetch_residual_seconds": 9.29420301600028,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.8878623526214006,
      "name": "result-fetch",
      "seconds": 9.260809809493367
    },
    {
      "fraction_of_gap": 0.06595991152723178,
      "name": "publication",
      "seconds": 0.687992
    },
    {
      "fraction_of_gap": 0.04655400187711113,
      "name": "scheduler-submit",
      "seconds": 0.4855795
    },
    {
      "fraction_of_gap": 0.006515878410301547,
      "name": "fixed-control",
      "seconds": 0.06796358751041401
    },
    {
      "fraction_of_gap": 0.0010221987707754528,
      "name": "graph-materialization",
      "seconds": 0.010662000000000001
    }
  ],
  "datavine_fetch_seconds": 9.261330822992022,
  "datavine_median_seconds": 18.60102864250075,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.4792825,
    "materialize_seconds": 0.010662000000000001,
    "publication_commit_seconds": 15.6027565,
    "publication_prepare_seconds": 0.0152845,
    "publication_queue_seconds": 113.563654,
    "publish_seconds": 0.6727075,
    "python_decode_seconds": 0.027361,
    "python_fsync_seconds": 44.4222325,
    "python_function_seconds": 2.099613,
    "python_serialize_seconds": 117.6401225,
    "setup_seconds": 0.031003999999999997,
    "submission_event_seconds": 0.0007340000000000001,
    "submit_seconds": 0.0062970000000000005
  },
  "datavine_useful_cpu_seconds": 1.280120868,
  "fetch_excess_seconds": 9.260809809493367,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 9.260809809493367
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 8.376836999999998,
  "parallel_publication_service_seconds": {
    "commit": 15.6027565,
    "decode": 0.027361,
    "fsync": 44.4222325,
    "function": 2.099613,
    "queue": 113.563654,
    "serialize": 117.6401225,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 4294967296,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0005210134986555204,
  "taskvine_median_seconds": 8.17057195949019,
  "taskvine_useful_cpu_seconds": 1.280135575,
  "terminal_poll_residual_seconds": 0.01932487750456069,
  "useful_cpu_relative_difference": 1.1488626898020704e-05,
  "worker_execution_excess_seconds": 102.5444315
}
```

### output_65536b

DataVine excess: 0.274223 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.5095581444977233,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.420802088548445,
      "name": "result-fetch",
      "seconds": 0.3896169094950892
    },
    {
      "fraction_of_gap": 0.4473457644358302,
      "name": "fixed-control",
      "seconds": 0.12267259150306598
    },
    {
      "fraction_of_gap": 0.23235815815963928,
      "name": "scheduler-submit",
      "seconds": 0.063718
    },
    {
      "fraction_of_gap": 0.22610048215910664,
      "name": "publication",
      "seconds": 0.062002
    },
    {
      "fraction_of_gap": 0.18555504473957132,
      "name": "graph-materialization",
      "seconds": 0.0508835
    }
  ],
  "datavine_fetch_seconds": 0.39157375249487814,
  "datavine_median_seconds": 1.6677515509945806,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.034158999999999995,
    "materialize_seconds": 0.0508835,
    "publication_commit_seconds": 3.4046705,
    "publication_prepare_seconds": 0.059898,
    "publication_queue_seconds": 3.6305015000000003,
    "publish_seconds": 0.002104,
    "python_decode_seconds": 0.2151775,
    "python_fsync_seconds": 7.6788264999999996,
    "python_function_seconds": 10.446805000000001,
    "python_serialize_seconds": 0.20132699999999998,
    "setup_seconds": 0.0766825,
    "submission_event_seconds": 0.0043064999999999996,
    "submit_seconds": 0.029559000000000002
  },
  "datavine_useful_cpu_seconds": 10.240994406,
  "fetch_excess_seconds": 0.3896169094950892,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.3896169094950892
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.39300450000000003,
  "parallel_publication_service_seconds": {
    "commit": 3.4046705,
    "decode": 0.2151775,
    "fsync": 7.6788264999999996,
    "function": 10.446805000000001,
    "queue": 3.6305015000000003,
    "serialize": 0.20132699999999998,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 67108864,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0019568429997889325,
  "taskvine_median_seconds": 1.3935283409955446,
  "taskvine_useful_cpu_seconds": 10.241079408000001,
  "terminal_poll_residual_seconds": 0.034667591503065975,
  "useful_cpu_relative_difference": 8.300101641053526e-06,
  "worker_execution_excess_seconds": 6.8169945
}
```

### topology_broadcast1024

DataVine excess: 0.098072 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.33380576451086813,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 2.2999061278886983,
      "name": "result-fetch",
      "seconds": 0.22555610399285797
    },
    {
      "fraction_of_gap": 1.2570646860034302,
      "name": "fixed-control",
      "seconds": 0.12328268949925622
    },
    {
      "fraction_of_gap": 0.6520065069633746,
      "name": "publication",
      "seconds": 0.0639435
    },
    {
      "fraction_of_gap": 0.6421616864190951,
      "name": "graph-materialization",
      "seconds": 0.062978
    },
    {
      "fraction_of_gap": 0.5136335010636901,
      "name": "scheduler-submit",
      "seconds": 0.050373
    }
  ],
  "datavine_fetch_seconds": 0.22718148650892545,
  "datavine_median_seconds": 1.4667984180123312,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0153965,
    "materialize_seconds": 0.062978,
    "publication_commit_seconds": 3.2184135,
    "publication_prepare_seconds": 0.0619675,
    "publication_queue_seconds": 0.0096685,
    "publish_seconds": 0.001976,
    "python_decode_seconds": 0.28265399999999996,
    "python_fsync_seconds": 5.7648735,
    "python_function_seconds": 10.413531500000001,
    "python_serialize_seconds": 0.0580525,
    "setup_seconds": 0.0789695,
    "submission_event_seconds": 0.0049305,
    "submit_seconds": 0.0349765
  },
  "datavine_useful_cpu_seconds": 10.250977748499999,
  "fetch_excess_seconds": 0.22555610399285797,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.22555610399285797
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.270704,
  "parallel_publication_service_seconds": {
    "commit": 3.2184135,
    "decode": 0.28265399999999996,
    "fsync": 5.7648735,
    "function": 10.413531500000001,
    "queue": 0.0096685,
    "serialize": 0.0580525,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.001625382516067475,
  "taskvine_median_seconds": 1.3687265440094052,
  "taskvine_useful_cpu_seconds": 10.2512577875,
  "terminal_poll_residual_seconds": 0.03233268949925622,
  "useful_cpu_relative_difference": 2.7317525888674614e-05,
  "worker_execution_excess_seconds": 3.208218500000001
}
```

### topology_dynamic

DataVine excess: 0.349697 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.2415213410029635,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.7410597598848105,
      "name": "fixed-control",
      "seconds": 0.25914621699699947
    },
    {
      "fraction_of_gap": 0.0463744609715492,
      "name": "publication",
      "seconds": 0.016217
    },
    {
      "fraction_of_gap": 0.03342324103320387,
      "name": "scheduler-submit",
      "seconds": 0.011688
    },
    {
      "fraction_of_gap": 0.013654686510399423,
      "name": "graph-materialization",
      "seconds": 0.004775
    },
    {
      "fraction_of_gap": 0.0,
      "name": "result-fetch",
      "seconds": 0.0
    }
  ],
  "datavine_fetch_seconds": 0.0,
  "datavine_median_seconds": 0.4810651970037725,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0051695,
    "materialize_seconds": 0.004775,
    "publication_commit_seconds": 0.0194455,
    "publication_prepare_seconds": 0.0006479999999999999,
    "publication_queue_seconds": 0.0001475,
    "publish_seconds": 0.015569,
    "python_decode_seconds": 0.0013925,
    "python_fsync_seconds": 0.0032045,
    "python_function_seconds": 0.080766,
    "python_serialize_seconds": 0.000329,
    "setup_seconds": 0.11747099999999999,
    "submission_event_seconds": 6.4e-05,
    "submit_seconds": 0.0065185
  },
  "datavine_useful_cpu_seconds": 0.010000765500000001,
  "fetch_excess_seconds": 0.0,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.25914621699699947
  },
  "logical_edge_payload_bytes": 7168,
  "manager_transfer_excess_seconds": 0.0024495000000000003,
  "parallel_publication_service_seconds": {
    "commit": 0.0194455,
    "decode": 0.0013925,
    "fsync": 0.0032045,
    "function": 0.080766,
    "queue": 0.0001475,
    "serialize": 0.000329,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1024,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0,
  "taskvine_median_seconds": 0.13136841001687571,
  "taskvine_useful_cpu_seconds": 0.010001426,
  "terminal_poll_residual_seconds": 0.13177671699699944,
  "useful_cpu_relative_difference": 6.604058261284807e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### topology_fanout16

DataVine excess: 0.233764 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.37558527300074235,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.0269744727276358,
      "name": "result-fetch",
      "seconds": 0.2400699405116029
    },
    {
      "fraction_of_gap": 0.5845023472584172,
      "name": "fixed-control",
      "seconds": 0.13663576599185362
    },
    {
      "fraction_of_gap": 0.3180682796354814,
      "name": "graph-materialization",
      "seconds": 0.074353
    },
    {
      "fraction_of_gap": 0.27482813908223613,
      "name": "publication",
      "seconds": 0.064245
    },
    {
      "fraction_of_gap": 0.2037479871795132,
      "name": "scheduler-submit",
      "seconds": 0.047629000000000005
    }
  ],
  "datavine_fetch_seconds": 0.24169950700888876,
  "datavine_median_seconds": 1.5438179215125274,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0045195,
    "materialize_seconds": 0.074353,
    "publication_commit_seconds": 3.1666495,
    "publication_prepare_seconds": 0.062162,
    "publication_queue_seconds": 0.010138000000000001,
    "publish_seconds": 0.0020829999999999998,
    "python_decode_seconds": 0.2822015,
    "python_fsync_seconds": 6.3358445,
    "python_function_seconds": 11.048799500000001,
    "python_serialize_seconds": 0.058106500000000005,
    "setup_seconds": 0.085514,
    "submission_event_seconds": 0.0056765,
    "submit_seconds": 0.0431095
  },
  "datavine_useful_cpu_seconds": 10.8810208965,
  "fetch_excess_seconds": 0.2400699405116029,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.2400699405116029
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.2792675,
  "parallel_publication_service_seconds": {
    "commit": 3.1666495,
    "decode": 0.2822015,
    "fsync": 6.3358445,
    "function": 11.048799500000001,
    "queue": 0.010138000000000001,
    "serialize": 0.058106500000000005,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016295664972858503,
  "taskvine_median_seconds": 1.3100536489946535,
  "taskvine_useful_cpu_seconds": 10.8811164385,
  "terminal_poll_residual_seconds": 0.03870526599185359,
  "useful_cpu_relative_difference": 8.780532819276314e-06,
  "worker_execution_excess_seconds": 4.915524000000001
}
```

### topology_heavy_tail

DataVine excess: 0.343020 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.3492056814789446,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6925584062784019,
      "name": "result-fetch",
      "seconds": 0.23756112100090832
    },
    {
      "fraction_of_gap": 0.312461953785786,
      "name": "fixed-control",
      "seconds": 0.1071805804948183
    },
    {
      "fraction_of_gap": 0.19033605161064088,
      "name": "publication",
      "seconds": 0.065289
    },
    {
      "fraction_of_gap": 0.15639630199200555,
      "name": "graph-materialization",
      "seconds": 0.053647
    },
    {
      "fraction_of_gap": 0.14124264982591156,
      "name": "scheduler-submit",
      "seconds": 0.048449
    }
  ],
  "datavine_fetch_seconds": 0.2399285089923069,
  "datavine_median_seconds": 2.307045626497711,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.01779,
    "materialize_seconds": 0.053647,
    "publication_commit_seconds": 2.9293205,
    "publication_prepare_seconds": 0.0631385,
    "publication_queue_seconds": 0.010088,
    "publish_seconds": 0.0021505,
    "python_decode_seconds": 0.205323,
    "python_fsync_seconds": 4.3144925,
    "python_function_seconds": 139.976605,
    "python_serialize_seconds": 0.0594085,
    "setup_seconds": 0.07291500000000001,
    "submission_event_seconds": 0.004451,
    "submit_seconds": 0.030659
  },
  "datavine_useful_cpu_seconds": 139.349112606,
  "fetch_excess_seconds": 0.23756112100090832,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.23756112100090832
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.26301250000000004,
  "parallel_publication_service_seconds": {
    "commit": 2.9293205,
    "decode": 0.205323,
    "fsync": 4.3144925,
    "function": 139.976605,
    "queue": 0.010088,
    "serialize": 0.0594085,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.002367387991398573,
  "taskvine_median_seconds": 1.964026007000939,
  "taskvine_useful_cpu_seconds": 139.351863794,
  "terminal_poll_residual_seconds": 0.022411580494818284,
  "useful_cpu_relative_difference": 1.9742742759850267e-05,
  "worker_execution_excess_seconds": 3.175844999999981
}
```

### topology_map

DataVine excess: 0.166173 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.33581555298746657,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.387868217571597,
      "name": "result-fetch",
      "seconds": 0.23062618299445603
    },
    {
      "fraction_of_gap": 0.6800066661360618,
      "name": "fixed-control",
      "seconds": 0.11299872699452097
    },
    {
      "fraction_of_gap": 0.38298346710575376,
      "name": "publication",
      "seconds": 0.0636415
    },
    {
      "fraction_of_gap": 0.35535562839229606,
      "name": "scheduler-submit",
      "seconds": 0.0590505
    },
    {
      "fraction_of_gap": 0.3198324020961644,
      "name": "graph-materialization",
      "seconds": 0.0531475
    }
  ],
  "datavine_fetch_seconds": 0.23225872400507797,
  "datavine_median_seconds": 1.3674827080103569,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.02883,
    "materialize_seconds": 0.0531475,
    "publication_commit_seconds": 3.263274,
    "publication_prepare_seconds": 0.061629,
    "publication_queue_seconds": 0.0095545,
    "publish_seconds": 0.0020125,
    "python_decode_seconds": 0.21839799999999998,
    "python_fsync_seconds": 5.6265719999999995,
    "python_function_seconds": 10.4095425,
    "python_serialize_seconds": 0.058677,
    "setup_seconds": 0.075796,
    "submission_event_seconds": 0.0042765,
    "submit_seconds": 0.030220499999999997
  },
  "datavine_useful_cpu_seconds": 10.2409874175,
  "fetch_excess_seconds": 0.23062618299445603,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.23062618299445603
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.2753,
  "parallel_publication_service_seconds": {
    "commit": 3.263274,
    "decode": 0.21839799999999998,
    "fsync": 5.6265719999999995,
    "function": 10.4095425,
    "queue": 0.0095545,
    "serialize": 0.058677,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016325410106219351,
  "taskvine_median_seconds": 1.2013097385060973,
  "taskvine_useful_cpu_seconds": 10.2410771645,
  "terminal_poll_residual_seconds": 0.026189226994520975,
  "useful_cpu_relative_difference": 8.763433627043685e-06,
  "worker_execution_excess_seconds": 4.517873000000002
}
```

