# DataVine versus TaskVine fixed-core result

Source artifact: `acceptance/1024-core-workflows/data-plane-v2-final-20260818/compact-summary.json`

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
| `admission_immediate` | admission | independent | 1024 | 0 | 0 | 0/0 | 1.0018 | 1.3655 | 0.740 | [0.723, 0.783] | result-fetch+fixed-control |
| `confirmation_input_broadcast_0b_w128` | confirmation | broadcast | 129 | 128 | 10.0 | 1/128 | 0.1725 | 0.3431 | 0.521 | [0.411, 0.638] | fixed-control+result-fetch+scheduler-submit |
| `cpu_0ms` | cpu | independent | 4096 | 0 | 0 | 0/0 | 4.6763 | 4.9092 | 0.958 | [0.925, 0.988] | result-fetch |
| `cpu_10000ms` | cpu | independent | 4096 | 0 | 10000 | 0/0 | 43.3774 | 44.6746 | 0.972 | [0.970, 0.974] | result-fetch+publication+graph-materialization |
| `cpu_1000ms` | cpu | independent | 4096 | 0 | 1000 | 0/0 | 7.1559 | 8.5794 | 0.843 | [0.834, 0.853] | result-fetch+scheduler-submit+fixed-control |
| `cpu_100ms` | cpu | independent | 4096 | 0 | 100 | 0/0 | 4.2578 | 5.0040 | 0.869 | [0.810, 0.929] | result-fetch |
| `cpu_10ms` | cpu | independent | 4096 | 0 | 10 | 0/0 | 4.9099 | 4.8691 | 1.008 | [0.988, 1.029] | NOT_REQUIRED |
| `cpu_1ms` | cpu | independent | 4096 | 0 | 1 | 0/0 | 4.6460 | 4.9737 | 0.937 | [0.920, 1.029] | NOT_REQUIRED |
| `degree_1` | degree | regular-pipeline | 2048 | 1024 | 10.0 | 1/1 | 1.8923 | 2.4735 | 0.766 | [0.739, 0.860] | result-fetch+fixed-control+graph-materialization |
| `degree_16` | degree | regular-pipeline | 2048 | 16384 | 10.0 | 16/16 | 2.8320 | 3.4319 | 0.840 | [0.789, 0.871] | fixed-control |
| `degree_4` | degree | regular-pipeline | 2048 | 4096 | 10.0 | 4/4 | 2.0909 | 2.6989 | 0.796 | [0.736, 0.827] | fixed-control+result-fetch+graph-materialization |
| `degree_64` | degree | regular-pipeline | 2048 | 65536 | 10.0 | 64/64 | 10.3998 | 10.9233 | 0.960 | [0.895, 1.034] | NOT_REQUIRED |
| `input_broadcast_0b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.1075 | 1.4632 | 0.763 | [0.750, 0.808] | result-fetch+fixed-control |
| `input_broadcast_1024b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.1097 | 1.4503 | 0.773 | [0.756, 0.826] | result-fetch |
| `input_broadcast_1048576b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.1681 | 1.4472 | 0.836 | [0.788, 0.888] | result-fetch |
| `input_broadcast_33554432b` | data | broadcast | 129 | 128 | 10.0 | 1/128 | 0.5083 | 0.5233 | 0.929 | [0.839, 0.981] | fixed-control |
| `input_broadcast_65536b` | data | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.1168 | 1.4640 | 0.783 | [0.762, 0.825] | result-fetch+fixed-control |
| `interaction_c0_p0_d1` | interaction | regular-pipeline | 2048 | 1024 | 0 | 1/1 | 1.9172 | 2.4099 | 0.814 | [0.752, 0.854] | result-fetch+fixed-control+graph-materialization |
| `interaction_c0_p0_d16` | interaction | regular-pipeline | 2048 | 16384 | 0 | 16/16 | 2.9819 | 3.4993 | 0.848 | [0.804, 0.908] | fixed-control |
| `interaction_c0_p0_d64` | interaction | regular-pipeline | 2048 | 65536 | 0 | 64/64 | 11.8647 | 10.4683 | 1.100 | [0.982, 1.203] | NOT_REQUIRED |
| `interaction_c0_p1048576_d1` | interaction | regular-pipeline | 2048 | 1024 | 0 | 1/1 | 4.5995 | 5.5417 | 0.829 | [0.766, 0.914] | result-fetch |
| `interaction_c0_p1048576_d16` | interaction | regular-pipeline | 2048 | 16384 | 0 | 16/16 | 5.5525 | 7.0100 | 0.814 | [0.758, 0.920] | result-fetch |
| `interaction_c0_p1048576_d64` | interaction | regular-pipeline | 2048 | 65536 | 0 | 64/64 | 14.0375 | 14.1293 | 1.013 | [0.943, 1.169] | NOT_REQUIRED |
| `interaction_c0_p65536_d1` | interaction | regular-pipeline | 2048 | 1024 | 0 | 1/1 | 2.1733 | 2.7789 | 0.806 | [0.763, 0.849] | result-fetch |
| `interaction_c0_p65536_d16` | interaction | regular-pipeline | 2048 | 16384 | 0 | 16/16 | 3.2490 | 3.7779 | 0.839 | [0.788, 0.918] | result-fetch |
| `interaction_c0_p65536_d64` | interaction | regular-pipeline | 2048 | 65536 | 0 | 64/64 | 11.4003 | 11.4270 | 0.981 | [0.909, 1.151] | NOT_REQUIRED |
| `interaction_c100_p0_d1` | interaction | regular-pipeline | 2048 | 1024 | 100 | 1/1 | 2.0714 | 2.4522 | 0.861 | [0.791, 0.907] | result-fetch |
| `interaction_c100_p0_d16` | interaction | regular-pipeline | 2048 | 16384 | 100 | 16/16 | 3.0991 | 3.6875 | 0.824 | [0.787, 0.881] | fixed-control |
| `interaction_c100_p0_d64` | interaction | regular-pipeline | 2048 | 65536 | 100 | 64/64 | 9.4328 | 11.1249 | 0.862 | [0.772, 0.880] | fixed-control |
| `interaction_c100_p1048576_d1` | interaction | regular-pipeline | 2048 | 1024 | 100 | 1/1 | 4.5338 | 5.6664 | 0.795 | [0.748, 0.826] | result-fetch |
| `interaction_c100_p1048576_d16` | interaction | regular-pipeline | 2048 | 16384 | 100 | 16/16 | 5.3923 | 6.7186 | 0.815 | [0.758, 0.871] | result-fetch |
| `interaction_c100_p1048576_d64` | interaction | regular-pipeline | 2048 | 65536 | 100 | 64/64 | 12.0919 | 15.1479 | 0.816 | [0.780, 0.863] | result-fetch |
| `interaction_c100_p65536_d1` | interaction | regular-pipeline | 2048 | 1024 | 100 | 1/1 | 2.1945 | 2.7012 | 0.805 | [0.755, 0.833] | result-fetch |
| `interaction_c100_p65536_d16` | interaction | regular-pipeline | 2048 | 16384 | 100 | 16/16 | 3.0701 | 3.7731 | 0.811 | [0.747, 0.852] | result-fetch |
| `interaction_c100_p65536_d64` | interaction | regular-pipeline | 2048 | 65536 | 100 | 64/64 | 9.3251 | 11.5012 | 0.819 | [0.725, 0.848] | fixed-control+result-fetch+graph-materialization+scheduler-submit+publication |
| `output_0b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 1.0865 | 1.3755 | 0.787 | [0.768, 0.804] | result-fetch |
| `output_1024b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 1.0782 | 1.3479 | 0.802 | [0.785, 0.810] | result-fetch |
| `output_1048576b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 3.5545 | 4.4618 | 0.814 | [0.759, 0.906] | result-fetch |
| `output_33554432b` | data | independent | 128 | 0 | 10.0 | 0/0 | 8.3459 | 16.6821 | 0.501 | [0.492, 0.510] | result-fetch |
| `output_65536b` | data | independent | 1024 | 0 | 10.0 | 0/0 | 1.2349 | 1.6574 | 0.753 | [0.730, 0.790] | result-fetch |
| `topology_broadcast1024` | topology | broadcast | 1025 | 1024 | 10.0 | 1/1024 | 1.1496 | 1.5017 | 0.787 | [0.743, 0.846] | result-fetch |
| `topology_chains` | topology | regular-pipeline | 4096 | 3072 | 10.0 | 1/1 | 3.7340 | 4.3691 | 0.872 | [0.840, 0.908] | graph-materialization+fixed-control |
| `topology_dynamic` | topology | dynamic | 8 | 7 | 10.0 | 1/1 | 0.1208 | 0.4308 | 0.279 | [0.266, 0.289] | fixed-control |
| `topology_fanin16` | topology | fan-in | 1088 | 1024 | 10.0 | 16/1 | 0.9685 | 1.2643 | 0.795 | [0.745, 0.877] | fixed-control+result-fetch+graph-materialization |
| `topology_fanout16` | topology | fan-out | 1088 | 1024 | 10.0 | 1/16 | 1.1856 | 1.6355 | 0.726 | [0.688, 0.782] | result-fetch+fixed-control+graph-materialization |
| `topology_heavy_tail` | topology | heavy-tail | 1024 | 0 | mixed-0-1000 | 0/0 | 1.8131 | 2.3269 | 0.788 | [0.757, 0.795] | result-fetch+fixed-control+graph-materialization+publication |
| `topology_map` | topology | independent | 1024 | 0 | 10.0 | 0/0 | 1.0775 | 1.3879 | 0.804 | [0.757, 0.814] | result-fetch |
| `topology_pipeline2` | topology | regular-pipeline | 4096 | 6144 | 10.0 | 2/2 | 3.8747 | 4.8020 | 0.845 | [0.804, 0.908] | graph-materialization+fixed-control+result-fetch |
| `topology_reduce16` | topology | reduction-tree | 1093 | 1092 | 10.0 | 16/1 | 1.0740 | 1.2507 | 0.825 | [0.764, 0.940] | fixed-control |

## DataVine regressions and causes

### admission_immediate

DataVine excess: 0.363705 s; classification: `result-fetch+fixed-control`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.04822542650799733,
  "client_wait_fetch_residual_seconds": 0.25648874550667683,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.4926543149302878,
      "name": "result-fetch",
      "seconds": 0.17918082801043056
    },
    {
      "fraction_of_gap": 0.38329149442328575,
      "name": "fixed-control",
      "seconds": 0.13940502550929226
    },
    {
      "fraction_of_gap": 0.19818397839564306,
      "name": "graph-materialization",
      "seconds": 0.07208049999999999
    },
    {
      "fraction_of_gap": 0.17584306904493383,
      "name": "publication",
      "seconds": 0.063955
    },
    {
      "fraction_of_gap": 0.0881772362724658,
      "name": "scheduler-submit",
      "seconds": 0.0320705
    }
  ],
  "datavine_fetch_seconds": 0.1808284150174586,
  "datavine_median_seconds": 1.3655274610064225,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0014759999999999999,
    "materialize_seconds": 0.07208049999999999,
    "publication_commit_seconds": 3.3993425,
    "publication_prepare_seconds": 0.062017,
    "publication_queue_seconds": 0.0102625,
    "publish_seconds": 0.001938,
    "python_decode_seconds": 0.2188405,
    "python_fsync_seconds": 1.819053,
    "python_function_seconds": 0.1049475,
    "python_serialize_seconds": 0.073373,
    "setup_seconds": 0.056593500000000005,
    "submission_event_seconds": 0.0039985,
    "submit_seconds": 0.0305945
  },
  "datavine_useful_cpu_seconds": 0.002512578,
  "fetch_excess_seconds": 0.17918082801043056,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.17918082801043056
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.152972,
  "parallel_publication_service_seconds": {
    "commit": 3.3993425,
    "decode": 0.2188405,
    "fsync": 1.819053,
    "function": 0.1049475,
    "queue": 0.0102625,
    "serialize": 0.073373,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016475870070280507,
  "taskvine_median_seconds": 1.0018224804953206,
  "taskvine_useful_cpu_seconds": 0.0028648249999999997,
  "terminal_poll_residual_seconds": 0.02543109900129492,
  "useful_cpu_relative_difference": 0.12295585245172029,
  "worker_execution_excess_seconds": 0.0
}
```

### confirmation_input_broadcast_0b_w128

DataVine excess: 0.170573 s; classification: `fixed-control+result-fetch+scheduler-submit`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection; add batched/multi-result fetch and avoid one RPC/decode per requested DataID; bulk native submission/completion draining without semantic batching

Evidence:

```json
{
  "build_submit_excess_seconds": 0.029306529992027208,
  "client_wait_fetch_residual_seconds": 0.09408436951408815,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.43828036077149246,
      "name": "fixed-control",
      "seconds": 0.07475898749554438
    },
    {
      "fraction_of_gap": 0.3034127787473569,
      "name": "result-fetch",
      "seconds": 0.05175416049314663
    },
    {
      "fraction_of_gap": 0.1774778097740578,
      "name": "scheduler-submit",
      "seconds": 0.030273
    },
    {
      "fraction_of_gap": 0.08723515374881843,
      "name": "graph-materialization",
      "seconds": 0.01488
    },
    {
      "fraction_of_gap": 0.05473009259557085,
      "name": "publication",
      "seconds": 0.009335499999999998
    }
  ],
  "datavine_fetch_seconds": 0.05192940100096166,
  "datavine_median_seconds": 0.3430684249906335,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.02359,
    "materialize_seconds": 0.01488,
    "publication_commit_seconds": 0.3690175,
    "publication_prepare_seconds": 0.007842499999999999,
    "publication_queue_seconds": 0.001361,
    "publish_seconds": 0.001493,
    "python_decode_seconds": 0.042199,
    "python_fsync_seconds": 0.089252,
    "python_function_seconds": 1.315677,
    "python_serialize_seconds": 0.010110000000000001,
    "setup_seconds": 0.027598499999999998,
    "submission_event_seconds": 0.0007735,
    "submit_seconds": 0.006683
  },
  "datavine_useful_cpu_seconds": 1.2901420235,
  "fetch_excess_seconds": 0.05175416049314663,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.07475898749554438
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.034112500000000004,
  "parallel_publication_service_seconds": {
    "commit": 0.3690175,
    "decode": 0.042199,
    "fsync": 0.089252,
    "function": 1.315677,
    "queue": 0.001361,
    "serialize": 0.010110000000000001,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0001752405078150332,
  "taskvine_median_seconds": 0.1724949880153872,
  "taskvine_useful_cpu_seconds": 1.290144411,
  "terminal_poll_residual_seconds": 0.015132457503517174,
  "useful_cpu_relative_difference": 1.8505680291033229e-06,
  "worker_execution_excess_seconds": 0.386101
}
```

### cpu_0ms

DataVine excess: 0.232919 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.8358843559976827,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 2.659768250284964,
      "name": "result-fetch",
      "seconds": 0.6195112460118253
    },
    {
      "fraction_of_gap": 1.102843546485891,
      "name": "graph-materialization",
      "seconds": 0.2568735
    },
    {
      "fraction_of_gap": 1.099020333199407,
      "name": "publication",
      "seconds": 0.255983
    },
    {
      "fraction_of_gap": 1.0070721460267713,
      "name": "fixed-control",
      "seconds": 0.23456649651412484
    },
    {
      "fraction_of_gap": 0.48704646069845936,
      "name": "scheduler-submit",
      "seconds": 0.1134425
    }
  ],
  "datavine_fetch_seconds": 0.6268974135018652,
  "datavine_median_seconds": 4.909195046508103,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.007529500000000001,
    "materialize_seconds": 0.2568735,
    "publication_commit_seconds": 14.354045,
    "publication_prepare_seconds": 0.2537765,
    "publication_queue_seconds": 0.040357000000000004,
    "publish_seconds": 0.0022065,
    "python_decode_seconds": 0.8538135,
    "python_fsync_seconds": 5.826473,
    "python_function_seconds": 0.4087445,
    "python_serialize_seconds": 0.2856955,
    "setup_seconds": 0.1589425,
    "submission_event_seconds": 0.014141,
    "submit_seconds": 0.10591300000000001
  },
  "datavine_useful_cpu_seconds": 0.0099671165,
  "fetch_excess_seconds": 0.6195112460118253,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.6195112460118253
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.5537015000000001,
  "parallel_publication_service_seconds": {
    "commit": 14.354045,
    "decode": 0.8538135,
    "fsync": 5.826473,
    "function": 0.4087445,
    "queue": 0.040357000000000004,
    "serialize": 0.2856955,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.007386167490039952,
  "taskvine_median_seconds": 4.676275788995554,
  "taskvine_useful_cpu_seconds": 0.0114286005,
  "terminal_poll_residual_seconds": 0.046828996514124865,
  "useful_cpu_relative_difference": 0.12787952470645908,
  "worker_execution_excess_seconds": 0.0
}
```

### cpu_10000ms

DataVine excess: 1.297137 s; classification: `result-fetch+publication+graph-materialization`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; batch retained-output serialization, hashing, fsync and result fetch; compact task/DataID materialization and remove per-edge copies

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.846664720009187,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.48025060954513665,
      "name": "result-fetch",
      "seconds": 0.6229506810050225
    },
    {
      "fraction_of_gap": 0.19800072271037644,
      "name": "publication",
      "seconds": 0.256834
    },
    {
      "fraction_of_gap": 0.19182095759654266,
      "name": "graph-materialization",
      "seconds": 0.24881799999999998
    },
    {
      "fraction_of_gap": 0.18169384207948958,
      "name": "fixed-control",
      "seconds": 0.23568174700504813
    },
    {
      "fraction_of_gap": 0.1433750220278767,
      "name": "scheduler-submit",
      "seconds": 0.185977
    }
  ],
  "datavine_fetch_seconds": 0.631743356003426,
  "datavine_median_seconds": 44.67456234851852,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.07807449999999999,
    "materialize_seconds": 0.24881799999999998,
    "publication_commit_seconds": 14.087327,
    "publication_prepare_seconds": 0.253931,
    "publication_queue_seconds": 0.0384685,
    "publish_seconds": 0.002903,
    "python_decode_seconds": 1.0574295,
    "python_fsync_seconds": 20.0573975,
    "python_function_seconds": 41123.034199,
    "python_serialize_seconds": 0.5853805,
    "setup_seconds": 0.161231,
    "submission_event_seconds": 0.0140125,
    "submit_seconds": 0.1079025
  },
  "datavine_useful_cpu_seconds": 40960.0045741075,
  "fetch_excess_seconds": 0.6229506810050225,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.6229506810050225
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.6147585,
  "parallel_publication_service_seconds": {
    "commit": 14.087327,
    "decode": 1.0574295,
    "fsync": 20.0573975,
    "function": 41123.034199,
    "queue": 0.0384685,
    "serialize": 0.5853805,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.008792674998403527,
  "taskvine_median_seconds": 43.377425668993965,
  "taskvine_useful_cpu_seconds": 40960.0047563815,
  "terminal_poll_residual_seconds": 0.04351474700504809,
  "useful_cpu_relative_difference": 4.4500483459852035e-09,
  "worker_execution_excess_seconds": 0.0
}
```

### cpu_1000ms

DataVine excess: 1.423504 s; classification: `result-fetch+scheduler-submit+fixed-control`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; bulk native submission/completion draining without semantic batching; reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.03116794049856253,
  "client_wait_fetch_residual_seconds": 0.8386034345016609,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.4343815498870237,
      "name": "result-fetch",
      "seconds": 0.6183437449944904
    },
    {
      "fraction_of_gap": 0.2393706452348325,
      "name": "scheduler-submit",
      "seconds": 0.340745
    },
    {
      "fraction_of_gap": 0.18423676690597562,
      "name": "fixed-control",
      "seconds": 0.26226172001077697
    },
    {
      "fraction_of_gap": 0.18029949579314858,
      "name": "publication",
      "seconds": 0.256657
    },
    {
      "fraction_of_gap": 0.1743248011161254,
      "name": "graph-materialization",
      "seconds": 0.24815199999999998
    }
  ],
  "datavine_fetch_seconds": 0.6252633584808791,
  "datavine_median_seconds": 8.57936526699632,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.235014,
    "materialize_seconds": 0.24815199999999998,
    "publication_commit_seconds": 14.520018,
    "publication_prepare_seconds": 0.2540295,
    "publication_queue_seconds": 0.038713,
    "publish_seconds": 0.0026274999999999996,
    "python_decode_seconds": 1.004149,
    "python_fsync_seconds": 16.880025500000002,
    "python_function_seconds": 4114.2419735,
    "python_serialize_seconds": 0.43192949999999997,
    "setup_seconds": 0.1588,
    "submission_event_seconds": 0.015279500000000001,
    "submit_seconds": 0.105731
  },
  "datavine_useful_cpu_seconds": 4096.0046169345005,
  "fetch_excess_seconds": 0.6183437449944904,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.6183437449944904
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.625608,
  "parallel_publication_service_seconds": {
    "commit": 14.520018,
    "decode": 1.004149,
    "fsync": 16.880025500000002,
    "function": 4114.2419735,
    "queue": 0.038713,
    "serialize": 0.43192949999999997,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.006919613486388698,
  "taskvine_median_seconds": 7.155861563500366,
  "taskvine_useful_cpu_seconds": 4096.1121040985,
  "terminal_poll_residual_seconds": 0.041342279512214475,
  "useful_cpu_relative_difference": 2.6241265196781254e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### cpu_100ms

DataVine excess: 0.746201 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.006287074997089803,
  "client_wait_fetch_residual_seconds": 0.8004093059980495,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.7963697851139859,
      "name": "result-fetch",
      "seconds": 0.5942517345101805
    },
    {
      "fraction_of_gap": 0.3511219982306641,
      "name": "graph-materialization",
      "seconds": 0.2620075
    },
    {
      "fraction_of_gap": 0.34282798356677247,
      "name": "publication",
      "seconds": 0.2558185
    },
    {
      "fraction_of_gap": 0.33116257587570985,
      "name": "fixed-control",
      "seconds": 0.24711376397942114
    },
    {
      "fraction_of_gap": 0.21425400475224857,
      "name": "scheduler-submit",
      "seconds": 0.1598765
    }
  ],
  "datavine_fetch_seconds": 0.6017061224847566,
  "datavine_median_seconds": 5.004019803003757,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.052290500000000004,
    "materialize_seconds": 0.2620075,
    "publication_commit_seconds": 14.0643005,
    "publication_prepare_seconds": 0.253776,
    "publication_queue_seconds": 0.038860000000000006,
    "publish_seconds": 0.0020425,
    "python_decode_seconds": 1.003952,
    "python_fsync_seconds": 10.3341615,
    "python_function_seconds": 412.1539975,
    "python_serialize_seconds": 0.356348,
    "setup_seconds": 0.1638175,
    "submission_event_seconds": 0.015491,
    "submit_seconds": 0.107586
  },
  "datavine_useful_cpu_seconds": 409.60457235850004,
  "fetch_excess_seconds": 0.5942517345101805,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.5942517345101805
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.596272,
  "parallel_publication_service_seconds": {
    "commit": 14.0643005,
    "decode": 1.003952,
    "fsync": 10.3341615,
    "function": 412.1539975,
    "queue": 0.038860000000000006,
    "serialize": 0.356348,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.007454387974576093,
  "taskvine_median_seconds": 4.257819048507372,
  "taskvine_useful_cpu_seconds": 409.6044478665,
  "terminal_poll_residual_seconds": 0.046023688982331334,
  "useful_cpu_relative_difference": 3.039321542180646e-07,
  "worker_execution_excess_seconds": 0.0
}
```

### degree_1

DataVine excess: 0.581204 s; classification: `result-fetch+fixed-control+graph-materialization`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; reduce RPC polling, workflow setup, checkpoint and completion projection; compact task/DataID materialization and remove per-edge copies

Evidence:

```json
{
  "build_submit_excess_seconds": 0.02907226950628683,
  "client_wait_fetch_residual_seconds": 0.3518309355037268,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.37205268721907697,
      "name": "result-fetch",
      "seconds": 0.21623857402300928
    },
    {
      "fraction_of_gap": 0.3016236779541356,
      "name": "fixed-control",
      "seconds": 0.1753049400069845
    },
    {
      "fraction_of_gap": 0.26748345501317056,
      "name": "graph-materialization",
      "seconds": 0.1554625
    },
    {
      "fraction_of_gap": 0.15152592537989804,
      "name": "publication",
      "seconds": 0.08806749999999999
    },
    {
      "fraction_of_gap": 0.14189849965005197,
      "name": "scheduler-submit",
      "seconds": 0.082472
    }
  ],
  "datavine_fetch_seconds": 0.21793451851408463,
  "datavine_median_seconds": 2.473513968012412,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0004955,
    "materialize_seconds": 0.1554625,
    "publication_commit_seconds": 3.226999,
    "publication_prepare_seconds": 0.0859055,
    "publication_queue_seconds": 0.041205,
    "publish_seconds": 0.002162,
    "python_decode_seconds": 0.5224934999999999,
    "python_fsync_seconds": 0.799245,
    "python_function_seconds": 20.792273,
    "python_serialize_seconds": 0.14382,
    "setup_seconds": 0.100195,
    "submission_event_seconds": 0.0090945,
    "submit_seconds": 0.08197650000000001
  },
  "datavine_useful_cpu_seconds": 20.482286499,
  "fetch_excess_seconds": 0.21623857402300928,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.21623857402300928
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.05989550000000002,
  "parallel_publication_service_seconds": {
    "commit": 3.226999,
    "decode": 0.5224934999999999,
    "fsync": 0.799245,
    "function": 20.792273,
    "queue": 0.041205,
    "serialize": 0.14382,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016959444910753518,
  "taskvine_median_seconds": 1.8923097959923325,
  "taskvine_useful_cpu_seconds": 20.4820195435,
  "terminal_poll_residual_seconds": 0.029384170500697637,
  "useful_cpu_relative_difference": 1.3033481394376222e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### degree_16

DataVine excess: 0.599881 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.17353515498689376,
  "client_wait_fetch_residual_seconds": 0.4237982929995666,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6270615307526298,
      "name": "fixed-control",
      "seconds": 0.3761622554806203
    },
    {
      "fraction_of_gap": 0.4132457789204781,
      "name": "result-fetch",
      "seconds": 0.2478982629982056
    },
    {
      "fraction_of_gap": 0.32144712345085125,
      "name": "graph-materialization",
      "seconds": 0.19283
    },
    {
      "fraction_of_gap": 0.14319591675563892,
      "name": "publication",
      "seconds": 0.0859005
    },
    {
      "fraction_of_gap": 0.13610617648681936,
      "name": "scheduler-submit",
      "seconds": 0.0816475
    }
  ],
  "datavine_fetch_seconds": 0.24972870449710172,
  "datavine_median_seconds": 3.431888175997301,
  "datavine_stage_medians": {
    "manager_lock_seconds": 3.5e-06,
    "materialize_seconds": 0.19283,
    "publication_commit_seconds": 3.187298,
    "publication_prepare_seconds": 0.08389350000000001,
    "publication_queue_seconds": 0.0341145,
    "publish_seconds": 0.002007,
    "python_decode_seconds": 0.8543890000000001,
    "python_fsync_seconds": 0.4237465,
    "python_function_seconds": 20.8535085,
    "python_serialize_seconds": 0.150068,
    "setup_seconds": 0.1415835,
    "submission_event_seconds": 0.0091195,
    "submit_seconds": 0.081644
  },
  "datavine_useful_cpu_seconds": 20.482202508,
  "fetch_excess_seconds": 0.2478982629982056,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.3761622554806203
  },
  "logical_edge_payload_bytes": 16777216,
  "manager_transfer_excess_seconds": 0.1783285,
  "parallel_publication_service_seconds": {
    "commit": 3.187298,
    "decode": 0.8543890000000001,
    "fsync": 0.4237465,
    "function": 20.8535085,
    "queue": 0.0341145,
    "serialize": 0.150068,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.001830441498896107,
  "taskvine_median_seconds": 2.832007244011038,
  "taskvine_useful_cpu_seconds": 20.482013488,
  "terminal_poll_residual_seconds": 0.04217710049372658,
  "useful_cpu_relative_difference": 9.228499714652821e-06,
  "worker_execution_excess_seconds": 0.0
}
```

### degree_4

DataVine excess: 0.607981 s; classification: `fixed-control+result-fetch+graph-materialization`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection; add batched/multi-result fetch and avoid one RPC/decode per requested DataID; compact task/DataID materialization and remove per-edge copies

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0735351589974016,
  "client_wait_fetch_residual_seconds": 0.3610325485231969,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.37317018406807523,
      "name": "fixed-control",
      "seconds": 0.22688038000501232
    },
    {
      "fraction_of_gap": 0.37069751385193656,
      "name": "result-fetch",
      "seconds": 0.22537704350543208
    },
    {
      "fraction_of_gap": 0.2664121102398323,
      "name": "graph-materialization",
      "seconds": 0.1619735
    },
    {
      "fraction_of_gap": 0.14129060058479984,
      "name": "publication",
      "seconds": 0.085902
    },
    {
      "fraction_of_gap": 0.13360039310378388,
      "name": "scheduler-submit",
      "seconds": 0.08122650000000001
    }
  ],
  "datavine_fetch_seconds": 0.22704406949924305,
  "datavine_median_seconds": 2.698877230519429,
  "datavine_stage_medians": {
    "manager_lock_seconds": 4.5e-06,
    "materialize_seconds": 0.1619735,
    "publication_commit_seconds": 3.1556699999999998,
    "publication_prepare_seconds": 0.0839575,
    "publication_queue_seconds": 0.0364425,
    "publish_seconds": 0.0019445,
    "python_decode_seconds": 0.6326195,
    "python_fsync_seconds": 1.059156,
    "python_function_seconds": 20.840386000000002,
    "python_serialize_seconds": 0.1576155,
    "setup_seconds": 0.10466800000000001,
    "submission_event_seconds": 0.009085,
    "submit_seconds": 0.081222
  },
  "datavine_useful_cpu_seconds": 20.482248586,
  "fetch_excess_seconds": 0.22537704350543208,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.22688038000501232
  },
  "logical_edge_payload_bytes": 4194304,
  "manager_transfer_excess_seconds": 0.13691849999999997,
  "parallel_publication_service_seconds": {
    "commit": 3.1556699999999998,
    "decode": 0.6326195,
    "fsync": 1.059156,
    "function": 20.840386000000002,
    "queue": 0.0364425,
    "serialize": 0.1576155,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016670259938109666,
  "taskvine_median_seconds": 2.0908962350076763,
  "taskvine_useful_cpu_seconds": 20.482020259000002,
  "terminal_poll_residual_seconds": 0.03180022100761071,
  "useful_cpu_relative_difference": 1.1147555359453801e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### input_broadcast_0b

DataVine excess: 0.355729 s; classification: `result-fetch+fixed-control`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.04367245899629779,
  "client_wait_fetch_residual_seconds": 0.2655430755078476,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.4918376068022185,
      "name": "result-fetch",
      "seconds": 0.1749608569953125
    },
    {
      "fraction_of_gap": 0.39665479253106856,
      "name": "fixed-control",
      "seconds": 0.1411015779857617
    },
    {
      "fraction_of_gap": 0.22345245833669855,
      "name": "graph-materialization",
      "seconds": 0.07948849999999999
    },
    {
      "fraction_of_gap": 0.18128692308541933,
      "name": "publication",
      "seconds": 0.06448899999999999
    },
    {
      "fraction_of_gap": 0.16850050106532244,
      "name": "scheduler-submit",
      "seconds": 0.0599405
    }
  ],
  "datavine_fetch_seconds": 0.17662486449989956,
  "datavine_median_seconds": 1.463215099502122,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.025228,
    "materialize_seconds": 0.07948849999999999,
    "publication_commit_seconds": 3.3157555,
    "publication_prepare_seconds": 0.0627025,
    "publication_queue_seconds": 0.0097615,
    "publish_seconds": 0.0017865,
    "python_decode_seconds": 0.2950435,
    "python_fsync_seconds": 0.37480199999999997,
    "python_function_seconds": 10.433029999999999,
    "python_serialize_seconds": 0.07176199999999999,
    "setup_seconds": 0.0647785,
    "submission_event_seconds": 0.004098,
    "submit_seconds": 0.0347125
  },
  "datavine_useful_cpu_seconds": 10.25106984,
  "fetch_excess_seconds": 0.1749608569953125,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.1749608569953125
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.1915615,
  "parallel_publication_service_seconds": {
    "commit": 3.3157555,
    "decode": 0.2950435,
    "fsync": 0.37480199999999997,
    "function": 10.433029999999999,
    "queue": 0.0097615,
    "serialize": 0.07176199999999999,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016640075045870617,
  "taskvine_median_seconds": 1.1074861870001769,
  "taskvine_useful_cpu_seconds": 10.25109838,
  "terminal_poll_residual_seconds": 0.023249618989463927,
  "useful_cpu_relative_difference": 2.784091903391912e-06,
  "worker_execution_excess_seconds": 0.9140939999999986
}
```

### input_broadcast_1024b

DataVine excess: 0.340588 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.03962150450388435,
  "client_wait_fetch_residual_seconds": 0.26609664550945633,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.5295508795971536,
      "name": "result-fetch",
      "seconds": 0.18035883648553863
    },
    {
      "fraction_of_gap": 0.38858803742767656,
      "name": "fixed-control",
      "seconds": 0.1323485410051076
    },
    {
      "fraction_of_gap": 0.22747551476646882,
      "name": "graph-materialization",
      "seconds": 0.0774755
    },
    {
      "fraction_of_gap": 0.19106205071481702,
      "name": "publication",
      "seconds": 0.06507349999999999
    },
    {
      "fraction_of_gap": 0.17262630319181096,
      "name": "scheduler-submit",
      "seconds": 0.0587945
    }
  ],
  "datavine_fetch_seconds": 0.18201427749590948,
  "datavine_median_seconds": 1.4503258119948441,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0253555,
    "materialize_seconds": 0.0774755,
    "publication_commit_seconds": 3.3658395,
    "publication_prepare_seconds": 0.06254399999999999,
    "publication_queue_seconds": 0.009981,
    "publish_seconds": 0.0025294999999999996,
    "python_decode_seconds": 0.297549,
    "python_fsync_seconds": 0.365699,
    "python_function_seconds": 10.432789,
    "python_serialize_seconds": 0.0706415,
    "setup_seconds": 0.0614885,
    "submission_event_seconds": 0.004118,
    "submit_seconds": 0.033438999999999997
  },
  "datavine_useful_cpu_seconds": 10.251076335,
  "fetch_excess_seconds": 0.18035883648553863,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.18035883648553863
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.19161500000000004,
  "parallel_publication_service_seconds": {
    "commit": 3.3658395,
    "decode": 0.297549,
    "fsync": 0.365699,
    "function": 10.432789,
    "queue": 0.009981,
    "serialize": 0.0706415,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016554410103708506,
  "taskvine_median_seconds": 1.109737507009413,
  "taskvine_useful_cpu_seconds": 10.2511099635,
  "terminal_poll_residual_seconds": 0.021504036501223234,
  "useful_cpu_relative_difference": 3.280474028531613e-06,
  "worker_execution_excess_seconds": 0.9393090000000015
}
```

### input_broadcast_1048576b

DataVine excess: 0.279144 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.003180604981025681,
  "client_wait_fetch_residual_seconds": 0.24696769801764656,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6011478834808407,
      "name": "result-fetch",
      "seconds": 0.16780683200340718
    },
    {
      "fraction_of_gap": 0.3277184411844803,
      "name": "fixed-control",
      "seconds": 0.09148064048039731
    },
    {
      "fraction_of_gap": 0.28898166011325516,
      "name": "graph-materialization",
      "seconds": 0.0806675
    },
    {
      "fraction_of_gap": 0.23165999347585847,
      "name": "publication",
      "seconds": 0.0646665
    },
    {
      "fraction_of_gap": 0.20379265738609478,
      "name": "scheduler-submit",
      "seconds": 0.0568875
    }
  ],
  "datavine_fetch_seconds": 0.1695168360020034,
  "datavine_median_seconds": 1.4472149460052606,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.021969000000000002,
    "materialize_seconds": 0.0806675,
    "publication_commit_seconds": 3.3788755000000004,
    "publication_prepare_seconds": 0.062365000000000004,
    "publication_queue_seconds": 0.009803,
    "publish_seconds": 0.0023014999999999997,
    "python_decode_seconds": 0.976701,
    "python_fsync_seconds": 0.44845650000000004,
    "python_function_seconds": 10.4320795,
    "python_serialize_seconds": 0.066286,
    "setup_seconds": 0.060384,
    "submission_event_seconds": 0.0043685,
    "submit_seconds": 0.0349185
  },
  "datavine_useful_cpu_seconds": 10.251032554,
  "fetch_excess_seconds": 0.16780683200340718,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.16780683200340718
  },
  "logical_edge_payload_bytes": 1073741824,
  "manager_transfer_excess_seconds": 0.18421949999999998,
  "parallel_publication_service_seconds": {
    "commit": 3.3788755000000004,
    "decode": 0.976701,
    "fsync": 0.44845650000000004,
    "function": 10.4320795,
    "queue": 0.009803,
    "serialize": 0.066286,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0017100039985962212,
  "taskvine_median_seconds": 1.168070933999843,
  "taskvine_useful_cpu_seconds": 10.251091280499999,
  "terminal_poll_residual_seconds": 0.017874535499371624,
  "useful_cpu_relative_difference": 5.728804708885169e-06,
  "worker_execution_excess_seconds": 1.5405960000000007
}
```

### input_broadcast_33554432b

DataVine excess: 0.015011 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.019456689013168216,
  "client_wait_fetch_residual_seconds": 0.07919221797460782,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 3.38274285336604,
      "name": "fixed-control",
      "seconds": 0.05077739050312154
    },
    {
      "fraction_of_gap": 3.112322598493213,
      "name": "result-fetch",
      "seconds": 0.046718188998056576
    },
    {
      "fraction_of_gap": 1.0299309199401259,
      "name": "graph-materialization",
      "seconds": 0.01546
    },
    {
      "fraction_of_gap": 0.6800475310963006,
      "name": "publication",
      "seconds": 0.010208000000000002
    },
    {
      "fraction_of_gap": 0.5061051228192197,
      "name": "scheduler-submit",
      "seconds": 0.0075970000000000005
    }
  ],
  "datavine_fetch_seconds": 0.046913587997551076,
  "datavine_median_seconds": 0.523299878987018,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0011475,
    "materialize_seconds": 0.01546,
    "publication_commit_seconds": 0.3295465,
    "publication_prepare_seconds": 0.008306000000000001,
    "publication_queue_seconds": 0.0014675,
    "publish_seconds": 0.001902,
    "python_decode_seconds": 1.7630135,
    "python_fsync_seconds": 0.066713,
    "python_function_seconds": 1.3210905,
    "python_serialize_seconds": 0.043044,
    "setup_seconds": 0.0152975,
    "submission_event_seconds": 0.0007535,
    "submit_seconds": 0.0064495
  },
  "datavine_useful_cpu_seconds": 1.2901282285,
  "fetch_excess_seconds": 0.046718188998056576,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.05077739050312154
  },
  "logical_edge_payload_bytes": 4294967296,
  "manager_transfer_excess_seconds": 0.059563000000000005,
  "parallel_publication_service_seconds": {
    "commit": 0.3295465,
    "decode": 1.7630135,
    "fsync": 0.066713,
    "function": 1.3210905,
    "queue": 0.0014675,
    "serialize": 0.043044,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.00019539899949450046,
  "taskvine_median_seconds": 0.5082891635101987,
  "taskvine_useful_cpu_seconds": 1.2901439485,
  "terminal_poll_residual_seconds": 0.013204201489953327,
  "useful_cpu_relative_difference": 1.2184686846935466e-05,
  "worker_execution_excess_seconds": 0.09410050000000059
}
```

### input_broadcast_65536b

DataVine excess: 0.347161 s; classification: `result-fetch+fixed-control`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.04454229048860725,
  "client_wait_fetch_residual_seconds": 0.27183807749512046,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.49845737141911833,
      "name": "result-fetch",
      "seconds": 0.1730452035117196
    },
    {
      "fraction_of_gap": 0.41520157578666383,
      "name": "fixed-control",
      "seconds": 0.14414199749084933
    },
    {
      "fraction_of_gap": 0.22934139416147,
      "name": "graph-materialization",
      "seconds": 0.07961850000000001
    },
    {
      "fraction_of_gap": 0.19013341628410668,
      "name": "publication",
      "seconds": 0.066007
    },
    {
      "fraction_of_gap": 0.15427114360488894,
      "name": "scheduler-submit",
      "seconds": 0.05355700000000001
    }
  ],
  "datavine_fetch_seconds": 0.17469308299769182,
  "datavine_median_seconds": 1.4639851114916382,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.018582500000000002,
    "materialize_seconds": 0.07961850000000001,
    "publication_commit_seconds": 3.3677915,
    "publication_prepare_seconds": 0.0637185,
    "publication_queue_seconds": 0.009694,
    "publish_seconds": 0.0022884999999999997,
    "python_decode_seconds": 0.3650985,
    "python_fsync_seconds": 0.3671085,
    "python_function_seconds": 10.436051500000001,
    "python_serialize_seconds": 0.0680405,
    "setup_seconds": 0.061103500000000005,
    "submission_event_seconds": 0.004191500000000001,
    "submit_seconds": 0.034974500000000006
  },
  "datavine_useful_cpu_seconds": 10.251063101,
  "fetch_excess_seconds": 0.1730452035117196,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.1730452035117196
  },
  "logical_edge_payload_bytes": 67108864,
  "manager_transfer_excess_seconds": 0.19160999999999997,
  "parallel_publication_service_seconds": {
    "commit": 3.3677915,
    "decode": 0.3650985,
    "fsync": 0.3671085,
    "function": 10.436051500000001,
    "queue": 0.009694,
    "serialize": 0.0680405,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016478794859722257,
  "taskvine_median_seconds": 1.1168236219964456,
  "taskvine_useful_cpu_seconds": 10.251095822,
  "terminal_poll_residual_seconds": 0.028439207002242062,
  "useful_cpu_relative_difference": 3.1919514331294148e-06,
  "worker_execution_excess_seconds": 1.0803744999999978
}
```

### interaction_c0_p0_d1

DataVine excess: 0.492682 s; classification: `result-fetch+fixed-control+graph-materialization`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; reduce RPC polling, workflow setup, checkpoint and completion projection; compact task/DataID materialization and remove per-edge copies

Evidence:

```json
{
  "build_submit_excess_seconds": 0.04537554949638434,
  "client_wait_fetch_residual_seconds": 0.3304925185008356,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.405174756586108,
      "name": "result-fetch",
      "seconds": 0.19962240949098486
    },
    {
      "fraction_of_gap": 0.38626679499598077,
      "name": "fixed-control",
      "seconds": 0.19030679248910967
    },
    {
      "fraction_of_gap": 0.31021414093865857,
      "name": "graph-materialization",
      "seconds": 0.152837
    },
    {
      "fraction_of_gap": 0.17961272309678794,
      "name": "publication",
      "seconds": 0.08849200000000002
    },
    {
      "fraction_of_gap": 0.16866651987320863,
      "name": "scheduler-submit",
      "seconds": 0.083099
    }
  ],
  "datavine_fetch_seconds": 0.20132414350518957,
  "datavine_median_seconds": 2.4098889084853,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.002164,
    "materialize_seconds": 0.152837,
    "publication_commit_seconds": 3.3741145,
    "publication_prepare_seconds": 0.08627950000000001,
    "publication_queue_seconds": 0.03934,
    "publish_seconds": 0.0022125,
    "python_decode_seconds": 0.5070445,
    "python_fsync_seconds": 1.3061055,
    "python_function_seconds": 0.20201249999999998,
    "python_serialize_seconds": 0.13838050000000002,
    "setup_seconds": 0.1002425,
    "submission_event_seconds": 0.008974,
    "submit_seconds": 0.080935
  },
  "datavine_useful_cpu_seconds": 0.004785555,
  "fetch_excess_seconds": 0.19962240949098486,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.19962240949098486
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.05841350000000001,
  "parallel_publication_service_seconds": {
    "commit": 3.3741145,
    "decode": 0.5070445,
    "fsync": 1.3061055,
    "function": 0.20201249999999998,
    "queue": 0.03934,
    "serialize": 0.13838050000000002,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0017017340142047033,
  "taskvine_median_seconds": 1.9172066615137737,
  "taskvine_useful_cpu_seconds": 0.0053984689999999995,
  "terminal_poll_residual_seconds": 0.028620742992725323,
  "useful_cpu_relative_difference": 0.11353478180573033,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c0_p0_d16

DataVine excess: 0.517418 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.14684019997366704,
  "client_wait_fetch_residual_seconds": 0.41445156348944256,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6496654850033152,
      "name": "fixed-control",
      "seconds": 0.3361484739714888
    },
    {
      "fraction_of_gap": 0.4510045863746186,
      "name": "result-fetch",
      "seconds": 0.23335779253102373
    },
    {
      "fraction_of_gap": 0.38573278138809697,
      "name": "graph-materialization",
      "seconds": 0.199585
    },
    {
      "fraction_of_gap": 0.1689224899569333,
      "name": "publication",
      "seconds": 0.08740350000000001
    },
    {
      "fraction_of_gap": 0.16505714154509774,
      "name": "scheduler-submit",
      "seconds": 0.0854035
    }
  ],
  "datavine_fetch_seconds": 0.23525983802392147,
  "datavine_median_seconds": 3.4993052485078806,
  "datavine_stage_medians": {
    "manager_lock_seconds": 3e-06,
    "materialize_seconds": 0.199585,
    "publication_commit_seconds": 3.295141,
    "publication_prepare_seconds": 0.084886,
    "publication_queue_seconds": 0.047933,
    "publish_seconds": 0.0025174999999999998,
    "python_decode_seconds": 0.7907725,
    "python_fsync_seconds": 1.178651,
    "python_function_seconds": 0.22270600000000002,
    "python_serialize_seconds": 0.14592349999999998,
    "setup_seconds": 0.14180199999999998,
    "submission_event_seconds": 0.0089325,
    "submit_seconds": 0.08540049999999999
  },
  "datavine_useful_cpu_seconds": 0.004679808,
  "fetch_excess_seconds": 0.23335779253102373,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.3361484739714888
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.19024049999999998,
  "parallel_publication_service_seconds": {
    "commit": 3.295141,
    "decode": 0.7907725,
    "fsync": 1.178651,
    "function": 0.22270600000000002,
    "queue": 0.047933,
    "serialize": 0.14592349999999998,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0019020454928977415,
  "taskvine_median_seconds": 2.9818874670017976,
  "taskvine_useful_cpu_seconds": 0.0050959479999999995,
  "terminal_poll_residual_seconds": 0.02933127399782176,
  "useful_cpu_relative_difference": 0.08166095886378742,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c0_p1048576_d1

DataVine excess: 0.942223 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 2.186886309991828,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 2.190801440630765,
      "name": "result-fetch",
      "seconds": 2.064223398498143
    },
    {
      "fraction_of_gap": 0.16506231336333071,
      "name": "graph-materialization",
      "seconds": 0.15552549999999998
    },
    {
      "fraction_of_gap": 0.1586702895905852,
      "name": "fixed-control",
      "seconds": 0.1495027884978299
    },
    {
      "fraction_of_gap": 0.1585542995293039,
      "name": "publication",
      "seconds": 0.1493935
    },
    {
      "fraction_of_gap": 0.08548401406746467,
      "name": "scheduler-submit",
      "seconds": 0.080545
    }
  ],
  "datavine_fetch_seconds": 2.066218724983628,
  "datavine_median_seconds": 5.5416878325195285,
  "datavine_stage_medians": {
    "manager_lock_seconds": 4e-06,
    "materialize_seconds": 0.15552549999999998,
    "publication_commit_seconds": 6.838405,
    "publication_prepare_seconds": 0.08315,
    "publication_queue_seconds": 56.258846500000004,
    "publish_seconds": 0.06624350000000001,
    "python_decode_seconds": 1.0359775,
    "python_fsync_seconds": 3.968503,
    "python_function_seconds": 0.934085,
    "python_serialize_seconds": 2.577079,
    "setup_seconds": 0.1048065,
    "submission_event_seconds": 0.0089765,
    "submit_seconds": 0.080541
  },
  "datavine_useful_cpu_seconds": 0.0046626535,
  "fetch_excess_seconds": 2.064223398498143,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 2.064223398498143
  },
  "logical_edge_payload_bytes": 1073741824,
  "manager_transfer_excess_seconds": 1.0961764999999999,
  "parallel_publication_service_seconds": {
    "commit": 6.838405,
    "decode": 1.0359775,
    "fsync": 3.968503,
    "function": 0.934085,
    "queue": 56.258846500000004,
    "serialize": 2.577079,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1073741824,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0019953264854848385,
  "taskvine_median_seconds": 4.5994648814958055,
  "taskvine_useful_cpu_seconds": 0.0052661949999999996,
  "terminal_poll_residual_seconds": 0.02733978849782992,
  "useful_cpu_relative_difference": 0.11460675117423486,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c0_p1048576_d16

DataVine excess: 1.457443 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.10330884801805951,
  "client_wait_fetch_residual_seconds": 2.219841630988144,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.4121306039233052,
      "name": "result-fetch",
      "seconds": 2.0580994860065402
    },
    {
      "fraction_of_gap": 0.21062119915449773,
      "name": "fixed-control",
      "seconds": 0.30696833601482926
    },
    {
      "fraction_of_gap": 0.14592717453638007,
      "name": "graph-materialization",
      "seconds": 0.2126805
    },
    {
      "fraction_of_gap": 0.06937253707915164,
      "name": "publication",
      "seconds": 0.1011065
    },
    {
      "fraction_of_gap": 0.06554219789972832,
      "name": "scheduler-submit",
      "seconds": 0.095524
    }
  ],
  "datavine_fetch_seconds": 2.0601712454954395,
  "datavine_median_seconds": 7.009962268988602,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.008747,
    "materialize_seconds": 0.2126805,
    "publication_commit_seconds": 7.167376,
    "publication_prepare_seconds": 0.085232,
    "publication_queue_seconds": 39.940878,
    "publish_seconds": 0.0158745,
    "python_decode_seconds": 11.439748999999999,
    "python_fsync_seconds": 22.1109805,
    "python_function_seconds": 1.0010475,
    "python_serialize_seconds": 2.6677475,
    "setup_seconds": 0.14131749999999998,
    "submission_event_seconds": 0.0091605,
    "submit_seconds": 0.08677699999999999
  },
  "datavine_useful_cpu_seconds": 0.004636572,
  "fetch_excess_seconds": 2.0580994860065402,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 2.0580994860065402
  },
  "logical_edge_payload_bytes": 17179869184,
  "manager_transfer_excess_seconds": 1.1896015,
  "parallel_publication_service_seconds": {
    "commit": 7.167376,
    "decode": 11.439748999999999,
    "fsync": 22.1109805,
    "function": 1.0010475,
    "queue": 39.940878,
    "serialize": 2.6677475,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1073741824,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0020717594888992608,
  "taskvine_median_seconds": 5.5525195365044056,
  "taskvine_useful_cpu_seconds": 0.0051845945000000004,
  "terminal_poll_residual_seconds": 0.04083798799676974,
  "useful_cpu_relative_difference": 0.10570209492757826,
  "worker_execution_excess_seconds": 19.1428915
}
```

### interaction_c0_p65536_d1

DataVine excess: 0.605634 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.03206286199565511,
  "client_wait_fetch_residual_seconds": 0.5063703595019867,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6040343293781684,
      "name": "result-fetch",
      "seconds": 0.3658235409966437
    },
    {
      "fraction_of_gap": 0.29491728146609375,
      "name": "fixed-control",
      "seconds": 0.17861184200920627
    },
    {
      "fraction_of_gap": 0.25219699963414466,
      "name": "graph-materialization",
      "seconds": 0.152739
    },
    {
      "fraction_of_gap": 0.14218413066731905,
      "name": "publication",
      "seconds": 0.0861115
    },
    {
      "fraction_of_gap": 0.13389446635981922,
      "name": "scheduler-submit",
      "seconds": 0.08109100000000001
    }
  ],
  "datavine_fetch_seconds": 0.3677212975017028,
  "datavine_median_seconds": 2.778906712002936,
  "datavine_stage_medians": {
    "manager_lock_seconds": 3.5e-06,
    "materialize_seconds": 0.152739,
    "publication_commit_seconds": 3.4718875000000002,
    "publication_prepare_seconds": 0.0825505,
    "publication_queue_seconds": 4.303457,
    "publish_seconds": 0.003561,
    "python_decode_seconds": 0.5497955,
    "python_fsync_seconds": 1.1885620000000001,
    "python_function_seconds": 0.275933,
    "python_serialize_seconds": 0.406116,
    "setup_seconds": 0.10189100000000001,
    "submission_event_seconds": 0.009068,
    "submit_seconds": 0.0810875
  },
  "datavine_useful_cpu_seconds": 0.0046923195,
  "fetch_excess_seconds": 0.3658235409966437,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.3658235409966437
  },
  "logical_edge_payload_bytes": 67108864,
  "manager_transfer_excess_seconds": 0.17160300000000003,
  "parallel_publication_service_seconds": {
    "commit": 3.4718875000000002,
    "decode": 0.5497955,
    "fsync": 1.1885620000000001,
    "function": 0.275933,
    "queue": 4.303457,
    "serialize": 0.406116,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 67108864,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0018977565050590783,
  "taskvine_median_seconds": 2.1732730200019432,
  "taskvine_useful_cpu_seconds": 0.005340513,
  "terminal_poll_residual_seconds": 0.027901980013551153,
  "useful_cpu_relative_difference": 0.1213728905818598,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c0_p65536_d16

DataVine excess: 0.528921 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.1496725544857327,
  "client_wait_fetch_residual_seconds": 0.5843680875026369,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.7673737108196553,
      "name": "result-fetch",
      "seconds": 0.4058800325146876
    },
    {
      "fraction_of_gap": 0.6525621591580272,
      "name": "fixed-control",
      "seconds": 0.3451537974815526
    },
    {
      "fraction_of_gap": 0.39581529111766633,
      "name": "graph-materialization",
      "seconds": 0.209355
    },
    {
      "fraction_of_gap": 0.16570150968176367,
      "name": "publication",
      "seconds": 0.08764300000000001
    },
    {
      "fraction_of_gap": 0.16376360194898626,
      "name": "scheduler-submit",
      "seconds": 0.086618
    }
  ],
  "datavine_fetch_seconds": 0.4078465695056366,
  "datavine_median_seconds": 3.7779048634984065,
  "datavine_stage_medians": {
    "manager_lock_seconds": 3e-06,
    "materialize_seconds": 0.209355,
    "publication_commit_seconds": 3.459048,
    "publication_prepare_seconds": 0.08211750000000001,
    "publication_queue_seconds": 3.253931,
    "publish_seconds": 0.0055255,
    "python_decode_seconds": 1.8360275000000001,
    "python_fsync_seconds": 0.615089,
    "python_function_seconds": 0.2919345,
    "python_serialize_seconds": 0.4129745,
    "setup_seconds": 0.1401885,
    "submission_event_seconds": 0.0094255,
    "submit_seconds": 0.086615
  },
  "datavine_useful_cpu_seconds": 0.004559355500000001,
  "fetch_excess_seconds": 0.4058800325146876,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.4058800325146876
  },
  "logical_edge_payload_bytes": 1073741824,
  "manager_transfer_excess_seconds": 0.28049949999999996,
  "parallel_publication_service_seconds": {
    "commit": 3.459048,
    "decode": 1.8360275000000001,
    "fsync": 0.615089,
    "function": 0.2919345,
    "queue": 3.253931,
    "serialize": 0.4129745,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 67108864,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0019665369909489527,
  "taskvine_median_seconds": 3.2489839129993925,
  "taskvine_useful_cpu_seconds": 0.0049819554999999995,
  "terminal_poll_residual_seconds": 0.03543624299581993,
  "useful_cpu_relative_difference": 0.08482612901700923,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c100_p0_d1

DataVine excess: 0.380769 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.33809653201234435,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.5426844652597397,
      "name": "result-fetch",
      "seconds": 0.20663767350197304
    },
    {
      "fraction_of_gap": 0.38439924402978765,
      "name": "fixed-control",
      "seconds": 0.1463674944964844
    },
    {
      "fraction_of_gap": 0.38266461308588956,
      "name": "graph-materialization",
      "seconds": 0.145707
    },
    {
      "fraction_of_gap": 0.2269142038463974,
      "name": "publication",
      "seconds": 0.08640199999999999
    },
    {
      "fraction_of_gap": 0.21104502168924993,
      "name": "scheduler-submit",
      "seconds": 0.0803595
    }
  ],
  "datavine_fetch_seconds": 0.2083443525043549,
  "datavine_median_seconds": 2.4521840190136572,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0046025,
    "materialize_seconds": 0.145707,
    "publication_commit_seconds": 3.4951245,
    "publication_prepare_seconds": 0.08373,
    "publication_queue_seconds": 0.085256,
    "publish_seconds": 0.002672,
    "python_decode_seconds": 0.5855085,
    "python_fsync_seconds": 2.3409440000000004,
    "python_function_seconds": 205.97551049999998,
    "python_serialize_seconds": 0.1727215,
    "setup_seconds": 0.10142,
    "submission_event_seconds": 0.008862,
    "submit_seconds": 0.075757
  },
  "datavine_useful_cpu_seconds": 204.80225492,
  "fetch_excess_seconds": 0.20663767350197304,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.20663767350197304
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.068326,
  "parallel_publication_service_seconds": {
    "commit": 3.4951245,
    "decode": 0.5855085,
    "fsync": 2.3409440000000004,
    "function": 205.97551049999998,
    "queue": 0.085256,
    "serialize": 0.1727215,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0017066790023818612,
  "taskvine_median_seconds": 2.071414554011426,
  "taskvine_useful_cpu_seconds": 204.802192627,
  "terminal_poll_residual_seconds": 0.029003994496484387,
  "useful_cpu_relative_difference": 3.041616900618622e-07,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c100_p0_d16

DataVine excess: 0.588474 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.15911560000677127,
  "client_wait_fetch_residual_seconds": 0.4322455785107585,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.614170542301447,
      "name": "fixed-control",
      "seconds": 0.36142320900227864
    },
    {
      "fraction_of_gap": 0.42078375412869784,
      "name": "result-fetch",
      "seconds": 0.247620171008748
    },
    {
      "fraction_of_gap": 0.3455558020389415,
      "name": "graph-materialization",
      "seconds": 0.2033505
    },
    {
      "fraction_of_gap": 0.1611397427694827,
      "name": "scheduler-submit",
      "seconds": 0.09482650000000001
    },
    {
      "fraction_of_gap": 0.15135340900614175,
      "name": "publication",
      "seconds": 0.08906750000000001
    }
  ],
  "datavine_fetch_seconds": 0.2494901580066653,
  "datavine_median_seconds": 3.687536491008359,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0113925,
    "materialize_seconds": 0.2033505,
    "publication_commit_seconds": 3.1301115,
    "publication_prepare_seconds": 0.086398,
    "publication_queue_seconds": 0.0539335,
    "publish_seconds": 0.0026695,
    "python_decode_seconds": 0.8228275,
    "python_fsync_seconds": 0.45480149999999997,
    "python_function_seconds": 206.0632915,
    "python_serialize_seconds": 0.16683,
    "setup_seconds": 0.146799,
    "submission_event_seconds": 0.0089335,
    "submit_seconds": 0.08343400000000001
  },
  "datavine_useful_cpu_seconds": 204.80224825,
  "fetch_excess_seconds": 0.247620171008748,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.36142320900227864
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.191928,
  "parallel_publication_service_seconds": {
    "commit": 3.1301115,
    "decode": 0.8228275,
    "fsync": 0.45480149999999997,
    "function": 206.0632915,
    "queue": 0.0539335,
    "serialize": 0.16683,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0018699869979172945,
  "taskvine_median_seconds": 3.099062795008649,
  "taskvine_useful_cpu_seconds": 204.802173349,
  "terminal_poll_residual_seconds": 0.0354606089955074,
  "useful_cpu_relative_difference": 3.6572352425314055e-07,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c100_p0_d64

DataVine excess: 1.692195 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.4421855934924679,
  "client_wait_fetch_residual_seconds": 0.6714268545007998,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.503812983498903,
      "name": "fixed-control",
      "seconds": 0.8525495649947988
    },
    {
      "fraction_of_gap": 0.17692085522235593,
      "name": "graph-materialization",
      "seconds": 0.2993845
    },
    {
      "fraction_of_gap": 0.1652890904534933,
      "name": "result-fetch",
      "seconds": 0.2797012915107189
    },
    {
      "fraction_of_gap": 0.057893462833141146,
      "name": "scheduler-submit",
      "seconds": 0.09796700000000001
    },
    {
      "fraction_of_gap": 0.05083192237436808,
      "name": "publication",
      "seconds": 0.0860175
    }
  ],
  "datavine_fetch_seconds": 0.2817034865001915,
  "datavine_median_seconds": 11.124948304001009,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.004044000000000001,
    "materialize_seconds": 0.2993845,
    "publication_commit_seconds": 3.044918,
    "publication_prepare_seconds": 0.083755,
    "publication_queue_seconds": 0.0636125,
    "publish_seconds": 0.0022624999999999998,
    "python_decode_seconds": 3.6891825000000003,
    "python_fsync_seconds": 0.48718,
    "python_function_seconds": 206.1632775,
    "python_serialize_seconds": 0.15215299999999998,
    "setup_seconds": 0.30835100000000004,
    "submission_event_seconds": 0.008399,
    "submit_seconds": 0.093923
  },
  "datavine_useful_cpu_seconds": 204.80230623350002,
  "fetch_excess_seconds": 0.2797012915107189,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.8525495649947988
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.482261,
  "parallel_publication_service_seconds": {
    "commit": 3.044918,
    "decode": 3.6891825000000003,
    "fsync": 0.48718,
    "function": 206.1632775,
    "queue": 0.0636125,
    "serialize": 0.15215299999999998,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0020021949894726276,
  "taskvine_median_seconds": 9.432753793502343,
  "taskvine_useful_cpu_seconds": 204.8022409605,
  "terminal_poll_residual_seconds": 0.07826147150233087,
  "useful_cpu_relative_difference": 3.1871223141370145e-07,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c100_p1048576_d1

DataVine excess: 1.132590 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 2.1911228009825274,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.8148195831052942,
      "name": "result-fetch",
      "seconds": 2.0554458900005557
    },
    {
      "fraction_of_gap": 0.28636099390511777,
      "name": "publication",
      "seconds": 0.3243295
    },
    {
      "fraction_of_gap": 0.1344622026127976,
      "name": "graph-materialization",
      "seconds": 0.1522905
    },
    {
      "fraction_of_gap": 0.12309349646717972,
      "name": "fixed-control",
      "seconds": 0.13941442100064827
    },
    {
      "fraction_of_gap": 0.06998033177964302,
      "name": "scheduler-submit",
      "seconds": 0.079259
    }
  ],
  "datavine_fetch_seconds": 2.057386888496694,
  "datavine_median_seconds": 5.666351364983711,
  "datavine_stage_medians": {
    "manager_lock_seconds": 3e-06,
    "materialize_seconds": 0.1522905,
    "publication_commit_seconds": 6.789733999999999,
    "publication_prepare_seconds": 0.08165549999999999,
    "publication_queue_seconds": 172.5399005,
    "publish_seconds": 0.242674,
    "python_decode_seconds": 1.249511,
    "python_fsync_seconds": 6.8457775000000005,
    "python_function_seconds": 206.76340950000002,
    "python_serialize_seconds": 2.7361120000000003,
    "setup_seconds": 0.09658249999999999,
    "submission_event_seconds": 0.009282,
    "submit_seconds": 0.079256
  },
  "datavine_useful_cpu_seconds": 204.8022284765,
  "fetch_excess_seconds": 2.0554458900005557,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 2.0554458900005557
  },
  "logical_edge_payload_bytes": 1073741824,
  "manager_transfer_excess_seconds": 1.1205315,
  "parallel_publication_service_seconds": {
    "commit": 6.789733999999999,
    "decode": 1.249511,
    "fsync": 6.8457775000000005,
    "function": 206.76340950000002,
    "queue": 172.5399005,
    "serialize": 2.7361120000000003,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1073741824,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0019409984961384907,
  "taskvine_median_seconds": 4.533761707512895,
  "taskvine_useful_cpu_seconds": 204.802195066,
  "terminal_poll_residual_seconds": 0.026223921000648298,
  "useful_cpu_relative_difference": 1.6313543196187138e-07,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c100_p1048576_d16

DataVine excess: 1.326249 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.11415306650451384,
  "client_wait_fetch_residual_seconds": 2.2300797629954965,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.566510825099736,
      "name": "result-fetch",
      "seconds": 2.077583195990883
    },
    {
      "fraction_of_gap": 0.23849325496413346,
      "name": "fixed-control",
      "seconds": 0.3163014075176316
    },
    {
      "fraction_of_gap": 0.14638391470337778,
      "name": "graph-materialization",
      "seconds": 0.1941415
    },
    {
      "fraction_of_gap": 0.08102250131149785,
      "name": "publication",
      "seconds": 0.107456
    },
    {
      "fraction_of_gap": 0.061928422693629,
      "name": "scheduler-submit",
      "seconds": 0.0821325
    }
  ],
  "datavine_fetch_seconds": 2.0796335070044734,
  "datavine_median_seconds": 6.718561105502886,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.000785,
    "materialize_seconds": 0.1941415,
    "publication_commit_seconds": 7.04537,
    "publication_prepare_seconds": 0.083078,
    "publication_queue_seconds": 67.7100235,
    "publish_seconds": 0.024378,
    "python_decode_seconds": 11.444886,
    "python_fsync_seconds": 21.1649405,
    "python_function_seconds": 206.829031,
    "python_serialize_seconds": 2.6866415,
    "setup_seconds": 0.1393545,
    "submission_event_seconds": 0.0086725,
    "submit_seconds": 0.0813475
  },
  "datavine_useful_cpu_seconds": 204.80219014850002,
  "fetch_excess_seconds": 2.077583195990883,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 2.077583195990883
  },
  "logical_edge_payload_bytes": 17179869184,
  "manager_transfer_excess_seconds": 1.2078855000000002,
  "parallel_publication_service_seconds": {
    "commit": 7.04537,
    "decode": 11.444886,
    "fsync": 21.1649405,
    "function": 206.829031,
    "queue": 67.7100235,
    "serialize": 2.6866415,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1073741824,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0020503110135905445,
  "taskvine_median_seconds": 5.392312245487119,
  "taskvine_useful_cpu_seconds": 204.8021827205,
  "terminal_poll_residual_seconds": 0.04091534101311778,
  "useful_cpu_relative_difference": 3.62691434019468e-08,
  "worker_execution_excess_seconds": 21.75304
}
```

### interaction_c100_p1048576_d64

DataVine excess: 3.056004 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.3960483819828369,
  "client_wait_fetch_residual_seconds": 2.495047708495295,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6847572214538739,
      "name": "result-fetch",
      "seconds": 2.0926208474993473
    },
    {
      "fraction_of_gap": 0.26664907114788383,
      "name": "fixed-control",
      "seconds": 0.8148806434865554
    },
    {
      "fraction_of_gap": 0.18693201617545735,
      "name": "scheduler-submit",
      "seconds": 0.571265
    },
    {
      "fraction_of_gap": 0.10079600489890794,
      "name": "graph-materialization",
      "seconds": 0.308033
    },
    {
      "fraction_of_gap": 0.02975421441680859,
      "name": "publication",
      "seconds": 0.090929
    }
  ],
  "datavine_fetch_seconds": 2.0954345969948918,
  "datavine_median_seconds": 15.147916221496416,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.47663,
    "materialize_seconds": 0.308033,
    "publication_commit_seconds": 8.33251,
    "publication_prepare_seconds": 0.08150299999999999,
    "publication_queue_seconds": 40.487475,
    "publish_seconds": 0.009426,
    "python_decode_seconds": 46.855568000000005,
    "python_fsync_seconds": 25.506147,
    "python_function_seconds": 206.971959,
    "python_serialize_seconds": 4.3223265,
    "setup_seconds": 0.3213005,
    "submission_event_seconds": 0.0087735,
    "submit_seconds": 0.094635
  },
  "datavine_useful_cpu_seconds": 204.80230661550002,
  "fetch_excess_seconds": 2.0926208474993473,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 2.0926208474993473
  },
  "logical_edge_payload_bytes": 68719476736,
  "manager_transfer_excess_seconds": 1.5332965,
  "parallel_publication_service_seconds": {
    "commit": 8.33251,
    "decode": 46.855568000000005,
    "fsync": 25.506147,
    "function": 206.971959,
    "queue": 40.487475,
    "serialize": 4.3223265,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1073741824,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.002813749495544471,
  "taskvine_median_seconds": 12.091912163508823,
  "taskvine_useful_cpu_seconds": 204.802181132,
  "terminal_poll_residual_seconds": 0.07237376150371855,
  "useful_cpu_relative_difference": 6.127055017195319e-07,
  "worker_execution_excess_seconds": 31.850112499999966
}
```

### interaction_c100_p65536_d1

DataVine excess: 0.506791 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.04491027299081907,
  "client_wait_fetch_residual_seconds": 0.49165600498904094,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.7199150229022934,
      "name": "result-fetch",
      "seconds": 0.3648464489815524
    },
    {
      "fraction_of_gap": 0.3708181217987736,
      "name": "fixed-control",
      "seconds": 0.18792728398814518
    },
    {
      "fraction_of_gap": 0.29774799124153517,
      "name": "graph-materialization",
      "seconds": 0.150896
    },
    {
      "fraction_of_gap": 0.17329431915224117,
      "name": "publication",
      "seconds": 0.087824
    },
    {
      "fraction_of_gap": 0.1638772220244165,
      "name": "scheduler-submit",
      "seconds": 0.0830515
    }
  ],
  "datavine_fetch_seconds": 0.3666643449978437,
  "datavine_median_seconds": 2.701249353005551,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.004965,
    "materialize_seconds": 0.150896,
    "publication_commit_seconds": 3.4589485,
    "publication_prepare_seconds": 0.0825555,
    "publication_queue_seconds": 8.8867565,
    "publish_seconds": 0.005268500000000001,
    "python_decode_seconds": 0.6398385,
    "python_fsync_seconds": 1.1880205,
    "python_function_seconds": 206.05750999999998,
    "python_serialize_seconds": 0.464967,
    "setup_seconds": 0.096495,
    "submission_event_seconds": 0.009016,
    "submit_seconds": 0.0780865
  },
  "datavine_useful_cpu_seconds": 204.80223623199998,
  "fetch_excess_seconds": 0.3648464489815524,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.3648464489815524
  },
  "logical_edge_payload_bytes": 67108864,
  "manager_transfer_excess_seconds": 0.192743,
  "parallel_publication_service_seconds": {
    "commit": 3.4589485,
    "decode": 0.6398385,
    "fsync": 1.1880205,
    "function": 206.05750999999998,
    "queue": 8.8867565,
    "serialize": 0.464967,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 67108864,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0018178960162913427,
  "taskvine_median_seconds": 2.1944583604927175,
  "taskvine_useful_cpu_seconds": 204.8022055525,
  "terminal_poll_residual_seconds": 0.029949510997326123,
  "useful_cpu_relative_difference": 1.4980061033637687e-07,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c100_p65536_d16

DataVine excess: 0.703028 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.16175911050231662,
  "client_wait_fetch_residual_seconds": 0.5692356780002535,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.5468440041486091,
      "name": "result-fetch",
      "seconds": 0.3844467394956155
    },
    {
      "fraction_of_gap": 0.5106868840295189,
      "name": "fixed-control",
      "seconds": 0.3590272655069092
    },
    {
      "fraction_of_gap": 0.2738346885989493,
      "name": "graph-materialization",
      "seconds": 0.1925135
    },
    {
      "fraction_of_gap": 0.13135177784405447,
      "name": "scheduler-submit",
      "seconds": 0.092344
    },
    {
      "fraction_of_gap": 0.12128176884241944,
      "name": "publication",
      "seconds": 0.0852645
    }
  ],
  "datavine_fetch_seconds": 0.3863970149977831,
  "datavine_median_seconds": 3.7731138464878313,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.010903,
    "materialize_seconds": 0.1925135,
    "publication_commit_seconds": 3.388254,
    "publication_prepare_seconds": 0.0824405,
    "publication_queue_seconds": 2.0105355,
    "publish_seconds": 0.002824,
    "python_decode_seconds": 1.842108,
    "python_fsync_seconds": 0.5691915,
    "python_function_seconds": 206.127592,
    "python_serialize_seconds": 0.45849850000000003,
    "setup_seconds": 0.1376505,
    "submission_event_seconds": 0.0087775,
    "submit_seconds": 0.081441
  },
  "datavine_useful_cpu_seconds": 204.80213618,
  "fetch_excess_seconds": 0.3844467394956155,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.3844467394956155
  },
  "logical_edge_payload_bytes": 1073741824,
  "manager_transfer_excess_seconds": 0.2795215,
  "parallel_publication_service_seconds": {
    "commit": 3.388254,
    "decode": 1.842108,
    "fsync": 0.5691915,
    "function": 206.127592,
    "queue": 2.0105355,
    "serialize": 0.45849850000000003,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 67108864,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0019502755021676421,
  "taskvine_median_seconds": 3.0700856765179196,
  "taskvine_useful_cpu_seconds": 204.80221694,
  "terminal_poll_residual_seconds": 0.03974065500459256,
  "useful_cpu_relative_difference": 3.943316689128934e-07,
  "worker_execution_excess_seconds": 0.0
}
```

### interaction_c100_p65536_d64

DataVine excess: 2.176026 s; classification: `fixed-control+result-fetch+graph-materialization+scheduler-submit+publication`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection; add batched/multi-result fetch and avoid one RPC/decode per requested DataID; compact task/DataID materialization and remove per-edge copies; bulk native submission/completion draining without semantic batching; batch retained-output serialization, hashing, fsync and result fetch

Evidence:

```json
{
  "build_submit_excess_seconds": 0.428589226474287,
  "client_wait_fetch_residual_seconds": 0.8255309354935569,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.38992895854278,
      "name": "fixed-control",
      "seconds": 0.848495558967078
    },
    {
      "fraction_of_gap": 0.1975848675330963,
      "name": "result-fetch",
      "seconds": 0.4299498125183163
    },
    {
      "fraction_of_gap": 0.1415038227717079,
      "name": "graph-materialization",
      "seconds": 0.30791599999999997
    },
    {
      "fraction_of_gap": 0.04783536554167321,
      "name": "scheduler-submit",
      "seconds": 0.104091
    },
    {
      "fraction_of_gap": 0.03864360963691894,
      "name": "publication",
      "seconds": 0.0840895
    }
  ],
  "datavine_fetch_seconds": 0.43197655701078475,
  "datavine_median_seconds": 11.501169194511021,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.008003,
    "materialize_seconds": 0.30791599999999997,
    "publication_commit_seconds": 3.430717,
    "publication_prepare_seconds": 0.08126749999999999,
    "publication_queue_seconds": 0.9683350000000001,
    "publish_seconds": 0.002822,
    "python_decode_seconds": 7.5292845,
    "python_fsync_seconds": 0.6781360000000001,
    "python_function_seconds": 206.2054995,
    "python_serialize_seconds": 0.47967150000000003,
    "setup_seconds": 0.314954,
    "submission_event_seconds": 0.0087915,
    "submit_seconds": 0.096088
  },
  "datavine_useful_cpu_seconds": 204.80231014150002,
  "fetch_excess_seconds": 0.4299498125183163,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.848495558967078
  },
  "logical_edge_payload_bytes": 4294967296,
  "manager_transfer_excess_seconds": 0.568694,
  "parallel_publication_service_seconds": {
    "commit": 3.430717,
    "decode": 7.5292845,
    "fsync": 0.6781360000000001,
    "function": 206.2054995,
    "queue": 0.9683350000000001,
    "serialize": 0.47967150000000003,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 67108864,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.002026744492468424,
  "taskvine_median_seconds": 9.325143176494748,
  "taskvine_useful_cpu_seconds": 204.80219639749998,
  "terminal_poll_residual_seconds": 0.08022883249279111,
  "useful_cpu_relative_difference": 5.553843604277205e-07,
  "worker_execution_excess_seconds": 0.0
}
```

### output_0b

DataVine excess: 0.289061 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.04578277851396706,
  "client_wait_fetch_residual_seconds": 0.2642649264910836,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6189159890207837,
      "name": "result-fetch",
      "seconds": 0.17890428748796694
    },
    {
      "fraction_of_gap": 0.48907411567567205,
      "name": "fixed-control",
      "seconds": 0.14137210501250339
    },
    {
      "fraction_of_gap": 0.24889582229315932,
      "name": "graph-materialization",
      "seconds": 0.071946
    },
    {
      "fraction_of_gap": 0.24368584385970232,
      "name": "scheduler-submit",
      "seconds": 0.07044
    },
    {
      "fraction_of_gap": 0.22392528820770605,
      "name": "publication",
      "seconds": 0.06472800000000001
    }
  ],
  "datavine_fetch_seconds": 0.1806609304912854,
  "datavine_median_seconds": 1.375533140002517,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0385325,
    "materialize_seconds": 0.071946,
    "publication_commit_seconds": 3.421112,
    "publication_prepare_seconds": 0.062616,
    "publication_queue_seconds": 0.009717,
    "publish_seconds": 0.002112,
    "python_decode_seconds": 0.2316495,
    "python_fsync_seconds": 1.7961925,
    "python_function_seconds": 10.403564,
    "python_serialize_seconds": 0.0766445,
    "setup_seconds": 0.06382650000000001,
    "submission_event_seconds": 0.0040095,
    "submit_seconds": 0.0319075
  },
  "datavine_useful_cpu_seconds": 10.241138952,
  "fetch_excess_seconds": 0.17890428748796694,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.17890428748796694
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.15549850000000004,
  "parallel_publication_service_seconds": {
    "commit": 3.421112,
    "decode": 0.2316495,
    "fsync": 1.7961925,
    "function": 10.403564,
    "queue": 0.009717,
    "serialize": 0.0766445,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 0,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0017566430033184588,
  "taskvine_median_seconds": 1.0864724424900487,
  "taskvine_useful_cpu_seconds": 10.241009331499999,
  "terminal_poll_residual_seconds": 0.022494326498536332,
  "useful_cpu_relative_difference": 1.265684418583817e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### output_1024b

DataVine excess: 0.269659 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.037075304993777536,
  "client_wait_fetch_residual_seconds": 0.2595741559885303,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6683584017107725,
      "name": "result-fetch",
      "seconds": 0.18022915899928194
    },
    {
      "fraction_of_gap": 0.46084076785566963,
      "name": "fixed-control",
      "seconds": 0.12427006799138432
    },
    {
      "fraction_of_gap": 0.2636603315905318,
      "name": "graph-materialization",
      "seconds": 0.0710985
    },
    {
      "fraction_of_gap": 0.24304358702520956,
      "name": "publication",
      "seconds": 0.06553899999999999
    },
    {
      "fraction_of_gap": 0.2269547757478195,
      "name": "scheduler-submit",
      "seconds": 0.061200500000000005
    }
  ],
  "datavine_fetch_seconds": 0.18192595450091176,
  "datavine_median_seconds": 1.3478930019919062,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.030365,
    "materialize_seconds": 0.0710985,
    "publication_commit_seconds": 3.4363885,
    "publication_prepare_seconds": 0.06315699999999999,
    "publication_queue_seconds": 0.0099745,
    "publish_seconds": 0.002382,
    "python_decode_seconds": 0.22970449999999998,
    "python_fsync_seconds": 1.5133975,
    "python_function_seconds": 10.4034385,
    "python_serialize_seconds": 0.0772945,
    "setup_seconds": 0.058387,
    "submission_event_seconds": 0.003987,
    "submit_seconds": 0.030835500000000002
  },
  "datavine_useful_cpu_seconds": 10.2411340025,
  "fetch_excess_seconds": 0.18022915899928194,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.18022915899928194
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.15216999999999997,
  "parallel_publication_service_seconds": {
    "commit": 3.4363885,
    "decode": 0.22970449999999998,
    "fsync": 1.5133975,
    "function": 10.4034385,
    "queue": 0.0099745,
    "serialize": 0.0772945,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.001696795501629822,
  "taskvine_median_seconds": 1.0782335520052584,
  "taskvine_useful_cpu_seconds": 10.2410357405,
  "terminal_poll_residual_seconds": 0.019674762997606787,
  "useful_cpu_relative_difference": 9.594835882201946e-06,
  "worker_execution_excess_seconds": 0.0
}
```

### output_1048576b

DataVine excess: 0.907308 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.024664586497237906,
  "client_wait_fetch_residual_seconds": 2.0969261445050433,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 2.2321947627964653,
      "name": "result-fetch",
      "seconds": 2.0252883979846956
    },
    {
      "fraction_of_gap": 0.20402771581618126,
      "name": "publication",
      "seconds": 0.185116
    },
    {
      "fraction_of_gap": 0.19122225320781513,
      "name": "scheduler-submit",
      "seconds": 0.1734975
    },
    {
      "fraction_of_gap": 0.12649139031351325,
      "name": "fixed-control",
      "seconds": 0.11476666351728695
    },
    {
      "fraction_of_gap": 0.08209835189596587,
      "name": "graph-materialization",
      "seconds": 0.0744885
    }
  ],
  "datavine_fetch_seconds": 2.0272912239888683,
  "datavine_median_seconds": 4.461802197503857,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.1410045,
    "materialize_seconds": 0.0744885,
    "publication_commit_seconds": 6.7888795,
    "publication_prepare_seconds": 0.061091000000000006,
    "publication_queue_seconds": 95.613067,
    "publish_seconds": 0.124025,
    "python_decode_seconds": 0.2334705,
    "python_fsync_seconds": 6.0786245,
    "python_function_seconds": 10.809286499999999,
    "python_serialize_seconds": 1.3494305,
    "setup_seconds": 0.056581,
    "submission_event_seconds": 0.0040565,
    "submit_seconds": 0.032493
  },
  "datavine_useful_cpu_seconds": 10.241138250999999,
  "fetch_excess_seconds": 2.0252883979846956,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 2.0252883979846956
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 1.313779,
  "parallel_publication_service_seconds": {
    "commit": 6.7888795,
    "decode": 0.2334705,
    "fsync": 6.0786245,
    "function": 10.809286499999999,
    "queue": 95.613067,
    "serialize": 1.3494305,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1073741824,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0020028260041726753,
  "taskvine_median_seconds": 3.5544940935069462,
  "taskvine_useful_cpu_seconds": 10.2410007085,
  "terminal_poll_residual_seconds": 0.023095577020049052,
  "useful_cpu_relative_difference": 1.3430391879122307e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### output_33554432b

DataVine excess: 8.336226 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.02802434001932852,
  "client_wait_fetch_residual_seconds": 9.168935213489837,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 1.0966957595805036,
      "name": "result-fetch",
      "seconds": 9.142303743501543
    },
    {
      "fraction_of_gap": 0.3513072927365762,
      "name": "publication",
      "seconds": 2.928577
    },
    {
      "fraction_of_gap": 0.13793555922916315,
      "name": "scheduler-submit",
      "seconds": 1.149862
    },
    {
      "fraction_of_gap": 0.007214390152902894,
      "name": "fixed-control",
      "seconds": 0.06014078701935862
    },
    {
      "fraction_of_gap": 0.0017050881226471743,
      "name": "graph-materialization",
      "seconds": 0.014214000000000001
    }
  ],
  "datavine_fetch_seconds": 9.142592448493815,
  "datavine_median_seconds": 16.682138363001286,
  "datavine_stage_medians": {
    "manager_lock_seconds": 1.143367,
    "materialize_seconds": 0.014214000000000001,
    "publication_commit_seconds": 15.4868025,
    "publication_prepare_seconds": 0.008125,
    "publication_queue_seconds": 203.027938,
    "publish_seconds": 2.920452,
    "python_decode_seconds": 0.030525499999999997,
    "python_fsync_seconds": 18.7743575,
    "python_function_seconds": 1.9096305,
    "python_serialize_seconds": 4.8732855,
    "setup_seconds": 0.0131555,
    "submission_event_seconds": 0.0007459999999999999,
    "submit_seconds": 0.006495
  },
  "datavine_useful_cpu_seconds": 1.2801373835,
  "fetch_excess_seconds": 9.142303743501543,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 9.142303743501543
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 4.3118229999999995,
  "parallel_publication_service_seconds": {
    "commit": 15.4868025,
    "decode": 0.030525499999999997,
    "fsync": 18.7743575,
    "function": 1.9096305,
    "queue": 203.027938,
    "serialize": 4.8732855,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 4294967296,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0002887049922719598,
  "taskvine_median_seconds": 8.345912327989936,
  "taskvine_useful_cpu_seconds": 1.2801267505,
  "terminal_poll_residual_seconds": 0.0159574470000301,
  "useful_cpu_relative_difference": 8.306139744912495e-06,
  "worker_execution_excess_seconds": 0.0
}
```

### output_65536b

DataVine excess: 0.422482 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.04468083498068154,
  "client_wait_fetch_residual_seconds": 0.4359994829931138,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.8216308195769878,
      "name": "result-fetch",
      "seconds": 0.3471246254775906
    },
    {
      "fraction_of_gap": 0.33047868000324215,
      "name": "fixed-control",
      "seconds": 0.13962145198437848
    },
    {
      "fraction_of_gap": 0.17064021251404402,
      "name": "graph-materialization",
      "seconds": 0.0720925
    },
    {
      "fraction_of_gap": 0.1614540327482292,
      "name": "scheduler-submit",
      "seconds": 0.06821150000000001
    },
    {
      "fraction_of_gap": 0.15423479845662,
      "name": "publication",
      "seconds": 0.0651615
    }
  ],
  "datavine_fetch_seconds": 0.34901603148318827,
  "datavine_median_seconds": 1.657408105005743,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0361815,
    "materialize_seconds": 0.0720925,
    "publication_commit_seconds": 3.4811205,
    "publication_prepare_seconds": 0.0619875,
    "publication_queue_seconds": 6.351691000000001,
    "publish_seconds": 0.003174,
    "python_decode_seconds": 0.2278345,
    "python_fsync_seconds": 1.5680445,
    "python_function_seconds": 10.4451565,
    "python_serialize_seconds": 0.22181099999999998,
    "setup_seconds": 0.0590385,
    "submission_event_seconds": 0.004184,
    "submit_seconds": 0.03203
  },
  "datavine_useful_cpu_seconds": 10.241133452,
  "fetch_excess_seconds": 0.3471246254775906,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.3471246254775906
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.2593345,
  "parallel_publication_service_seconds": {
    "commit": 3.4811205,
    "decode": 0.2278345,
    "fsync": 1.5680445,
    "function": 10.4451565,
    "queue": 6.351691000000001,
    "serialize": 0.22181099999999998,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 67108864,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0018914060055976734,
  "taskvine_median_seconds": 1.234925626005861,
  "taskvine_useful_cpu_seconds": 10.241036112,
  "terminal_poll_residual_seconds": 0.026178617003696947,
  "useful_cpu_relative_difference": 9.504807300498187e-06,
  "worker_execution_excess_seconds": 0.0
}
```

### topology_broadcast1024

DataVine excess: 0.352094 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.039342710995697416,
  "client_wait_fetch_residual_seconds": 0.2846603654986592,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.5658184228612873,
      "name": "result-fetch",
      "seconds": 0.1992215380014386
    },
    {
      "fraction_of_gap": 0.38634512435703033,
      "name": "fixed-control",
      "seconds": 0.13602998199412417
    },
    {
      "fraction_of_gap": 0.22434603953269952,
      "name": "graph-materialization",
      "seconds": 0.078991
    },
    {
      "fraction_of_gap": 0.1859827560068809,
      "name": "publication",
      "seconds": 0.06548350000000001
    },
    {
      "fraction_of_gap": 0.1769510890357831,
      "name": "scheduler-submit",
      "seconds": 0.0623035
    }
  ],
  "datavine_fetch_seconds": 0.20087627200700808,
  "datavine_median_seconds": 1.501721851003822,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0285675,
    "materialize_seconds": 0.078991,
    "publication_commit_seconds": 3.3085545,
    "publication_prepare_seconds": 0.06322900000000001,
    "publication_queue_seconds": 0.0099555,
    "publish_seconds": 0.0022545,
    "python_decode_seconds": 0.2950195,
    "python_fsync_seconds": 0.5320865,
    "python_function_seconds": 10.431908,
    "python_serialize_seconds": 0.0721575,
    "setup_seconds": 0.06260650000000001,
    "submission_event_seconds": 0.0041265,
    "submit_seconds": 0.033736
  },
  "datavine_useful_cpu_seconds": 10.2510561995,
  "fetch_excess_seconds": 0.1992215380014386,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.1992215380014386
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.1928595,
  "parallel_publication_service_seconds": {
    "commit": 3.3085545,
    "decode": 0.2950195,
    "fsync": 0.5320865,
    "function": 10.431908,
    "queue": 0.0099555,
    "serialize": 0.0721575,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016547340055694804,
  "taskvine_median_seconds": 1.149627380495076,
  "taskvine_useful_cpu_seconds": 10.2510955165,
  "terminal_poll_residual_seconds": 0.02434527099842676,
  "useful_cpu_relative_difference": 3.835394952230562e-06,
  "worker_execution_excess_seconds": 1.3248770000000007
}
```

### topology_chains

DataVine excess: 0.635132 s; classification: `graph-materialization+fixed-control`; status: `CONFIRMED`.

Improvement: compact task/DataID materialization and remove per-edge copies; reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.44995420700609956,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.4433230810395069,
      "name": "graph-materialization",
      "seconds": 0.2815685
    },
    {
      "fraction_of_gap": 0.38828764710294156,
      "name": "fixed-control",
      "seconds": 0.24661375651127368
    },
    {
      "fraction_of_gap": 0.3816429792973767,
      "name": "result-fetch",
      "seconds": 0.242393517983146
    },
    {
      "fraction_of_gap": 0.28578407776800824,
      "name": "scheduler-submit",
      "seconds": 0.18151050000000002
    },
    {
      "fraction_of_gap": 0.20710353407350135,
      "name": "publication",
      "seconds": 0.13153800000000002
    }
  ],
  "datavine_fetch_seconds": 0.2440040919900639,
  "datavine_median_seconds": 4.36911878400133,
  "datavine_stage_medians": {
    "manager_lock_seconds": 1.1e-05,
    "materialize_seconds": 0.2815685,
    "publication_commit_seconds": 3.1466529999999997,
    "publication_prepare_seconds": 0.128917,
    "publication_queue_seconds": 0.041348499999999996,
    "publish_seconds": 0.002621,
    "python_decode_seconds": 1.1173739999999999,
    "python_fsync_seconds": 1.2745395,
    "python_function_seconds": 41.577291,
    "python_serialize_seconds": 0.28338949999999996,
    "setup_seconds": 0.1753405,
    "submission_event_seconds": 0.0191995,
    "submit_seconds": 0.1814995
  },
  "datavine_useful_cpu_seconds": 40.9645690535,
  "fetch_excess_seconds": 0.242393517983146,
  "largest_measured_component": {
    "name": "graph-materialization",
    "seconds": 0.2815685
  },
  "logical_edge_payload_bytes": 3145728,
  "manager_transfer_excess_seconds": 0.0,
  "parallel_publication_service_seconds": {
    "commit": 3.1466529999999997,
    "decode": 1.1173739999999999,
    "fsync": 1.2745395,
    "function": 41.577291,
    "queue": 0.041348499999999996,
    "serialize": 0.28338949999999996,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016105740069178864,
  "taskvine_median_seconds": 3.7339871789881727,
  "taskvine_useful_cpu_seconds": 40.9640767425,
  "terminal_poll_residual_seconds": 0.041595756511273674,
  "useful_cpu_relative_difference": 1.2017970928954996e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### topology_dynamic

DataVine excess: 0.309971 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.0,
  "client_wait_fetch_residual_seconds": 0.21964163750780932,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.7453240858278279,
      "name": "fixed-control",
      "seconds": 0.23102906500092701
    },
    {
      "fraction_of_gap": 0.049340054112260986,
      "name": "publication",
      "seconds": 0.015293999999999999
    },
    {
      "fraction_of_gap": 0.021106793777263474,
      "name": "scheduler-submit",
      "seconds": 0.006542500000000001
    },
    {
      "fraction_of_gap": 0.004608491388738384,
      "name": "graph-materialization",
      "seconds": 0.0014285
    },
    {
      "fraction_of_gap": 0.0,
      "name": "result-fetch",
      "seconds": 0.0
    }
  ],
  "datavine_fetch_seconds": 0.0,
  "datavine_median_seconds": 0.43078234550193883,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.004986000000000001,
    "materialize_seconds": 0.0014285,
    "publication_commit_seconds": 0.019614500000000003,
    "publication_prepare_seconds": 0.000649,
    "publication_queue_seconds": 0.000136,
    "publish_seconds": 0.014644999999999998,
    "python_decode_seconds": 0.0014735,
    "python_fsync_seconds": 0.0024275,
    "python_function_seconds": 0.08092949999999999,
    "python_serialize_seconds": 0.0005239999999999999,
    "setup_seconds": 0.06642250000000001,
    "submission_event_seconds": 6.25e-05,
    "submit_seconds": 0.0015565
  },
  "datavine_useful_cpu_seconds": 0.0100010505,
  "fetch_excess_seconds": 0.0,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.23102906500092701
  },
  "logical_edge_payload_bytes": 7168,
  "manager_transfer_excess_seconds": 0.0021405,
  "parallel_publication_service_seconds": {
    "commit": 0.019614500000000003,
    "decode": 0.0014735,
    "fsync": 0.0024275,
    "function": 0.08092949999999999,
    "queue": 0.000136,
    "serialize": 0.0005239999999999999,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1024,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0,
  "taskvine_median_seconds": 0.12081105999823194,
  "taskvine_useful_cpu_seconds": 0.0100011895,
  "terminal_poll_residual_seconds": 0.15471906500092703,
  "useful_cpu_relative_difference": 1.3898346791618628e-05,
  "worker_execution_excess_seconds": 0.0022585000000000105
}
```

### topology_fanin16

DataVine excess: 0.295807 s; classification: `fixed-control+result-fetch+graph-materialization`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection; add batched/multi-result fetch and avoid one RPC/decode per requested DataID; compact task/DataID materialization and remove per-edge copies

Evidence:

```json
{
  "build_submit_excess_seconds": 0.05281097651459277,
  "client_wait_fetch_residual_seconds": 0.17287100149393453,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.4991910587390562,
      "name": "fixed-control",
      "seconds": 0.14766436601316557
    },
    {
      "fraction_of_gap": 0.26810953414607636,
      "name": "result-fetch",
      "seconds": 0.07930876102182083
    },
    {
      "fraction_of_gap": 0.17340680117596477,
      "name": "graph-materialization",
      "seconds": 0.051295
    },
    {
      "fraction_of_gap": 0.131012649891294,
      "name": "scheduler-submit",
      "seconds": 0.0387545
    },
    {
      "fraction_of_gap": 0.08963267231464139,
      "name": "publication",
      "seconds": 0.026514000000000003
    }
  ],
  "datavine_fetch_seconds": 0.07938919901789632,
  "datavine_median_seconds": 1.264286252000602,
  "datavine_stage_medians": {
    "manager_lock_seconds": 3.5e-06,
    "materialize_seconds": 0.051295,
    "publication_commit_seconds": 0.1705815,
    "publication_prepare_seconds": 0.0245835,
    "publication_queue_seconds": 0.0452865,
    "publish_seconds": 0.0019305,
    "python_decode_seconds": 0.2678935,
    "python_fsync_seconds": 0.0406,
    "python_function_seconds": 11.052114499999998,
    "python_serialize_seconds": 0.081367,
    "setup_seconds": 0.066195,
    "submission_event_seconds": 0.0038915,
    "submit_seconds": 0.038751
  },
  "datavine_useful_cpu_seconds": 10.881205123,
  "fetch_excess_seconds": 0.07930876102182083,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.14766436601316557
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.0,
  "parallel_publication_service_seconds": {
    "commit": 0.1705815,
    "decode": 0.2678935,
    "fsync": 0.0406,
    "function": 11.052114499999998,
    "queue": 0.0452865,
    "serialize": 0.081367,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 65536,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 8.043799607548863e-05,
  "taskvine_median_seconds": 0.9684789384918986,
  "taskvine_useful_cpu_seconds": 10.881056840500001,
  "terminal_poll_residual_seconds": 0.02075288949857279,
  "useful_cpu_relative_difference": 1.3627396811610714e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### topology_fanout16

DataVine excess: 0.449851 s; classification: `result-fetch+fixed-control+graph-materialization`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; reduce RPC polling, workflow setup, checkpoint and completion projection; compact task/DataID materialization and remove per-edge copies

Evidence:

```json
{
  "build_submit_excess_seconds": 0.035466854518745095,
  "client_wait_fetch_residual_seconds": 0.28612559099733176,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.4253140559738094,
      "name": "result-fetch",
      "seconds": 0.19132814797922038
    },
    {
      "fraction_of_gap": 0.3022542758189177,
      "name": "fixed-control",
      "seconds": 0.13596952651570748
    },
    {
      "fraction_of_gap": 0.20522107566582692,
      "name": "graph-materialization",
      "seconds": 0.09231900000000001
    },
    {
      "fraction_of_gap": 0.1457347729023922,
      "name": "publication",
      "seconds": 0.065559
    },
    {
      "fraction_of_gap": 0.09978404926924725,
      "name": "scheduler-submit",
      "seconds": 0.044888
    }
  ],
  "datavine_fetch_seconds": 0.19296196498908103,
  "datavine_median_seconds": 1.6354526784998598,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0032034999999999998,
    "materialize_seconds": 0.09231900000000001,
    "publication_commit_seconds": 3.240995,
    "publication_prepare_seconds": 0.0636005,
    "publication_queue_seconds": 0.0104525,
    "publish_seconds": 0.0019584999999999997,
    "python_decode_seconds": 0.34846699999999997,
    "python_fsync_seconds": 4.386638,
    "python_function_seconds": 11.087031499999998,
    "python_serialize_seconds": 0.087837,
    "setup_seconds": 0.068935,
    "submission_event_seconds": 0.0047445000000000005,
    "submit_seconds": 0.0416845
  },
  "datavine_useful_cpu_seconds": 10.8812176755,
  "fetch_excess_seconds": 0.19132814797922038,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.19132814797922038
  },
  "logical_edge_payload_bytes": 1048576,
  "manager_transfer_excess_seconds": 0.19040200000000002,
  "parallel_publication_service_seconds": {
    "commit": 3.240995,
    "decode": 0.34846699999999997,
    "fsync": 4.386638,
    "function": 11.087031499999998,
    "queue": 0.0104525,
    "serialize": 0.087837,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016338170098606497,
  "taskvine_median_seconds": 1.1856012209900655,
  "taskvine_useful_cpu_seconds": 10.8810361825,
  "terminal_poll_residual_seconds": 0.021460171996962374,
  "useful_cpu_relative_difference": 1.667947516647191e-05,
  "worker_execution_excess_seconds": 5.3004265
}
```

### topology_heavy_tail

DataVine excess: 0.513771 s; classification: `result-fetch+fixed-control+graph-materialization+publication`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID; reduce RPC polling, workflow setup, checkpoint and completion projection; compact task/DataID materialization and remove per-edge copies; batch retained-output serialization, hashing, fsync and result fetch

Evidence:

```json
{
  "build_submit_excess_seconds": 0.04246325750136748,
  "client_wait_fetch_residual_seconds": 0.26444388248405326,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.3685255030453066,
      "name": "result-fetch",
      "seconds": 0.1893377749947831
    },
    {
      "fraction_of_gap": 0.2539010950513582,
      "name": "fixed-control",
      "seconds": 0.13044705999588035
    },
    {
      "fraction_of_gap": 0.13702014739845372,
      "name": "graph-materialization",
      "seconds": 0.07039699999999999
    },
    {
      "fraction_of_gap": 0.1279051943426849,
      "name": "publication",
      "seconds": 0.06571400000000001
    },
    {
      "fraction_of_gap": 0.1023860507351288,
      "name": "scheduler-submit",
      "seconds": 0.052603
    }
  ],
  "datavine_fetch_seconds": 0.19096220399660524,
  "datavine_median_seconds": 2.3269031169911614,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0214955,
    "materialize_seconds": 0.07039699999999999,
    "publication_commit_seconds": 2.9508254999999997,
    "publication_prepare_seconds": 0.0634755,
    "publication_queue_seconds": 0.01025,
    "publish_seconds": 0.0022385,
    "python_decode_seconds": 0.2319695,
    "python_fsync_seconds": 1.9127275,
    "python_function_seconds": 139.98481149999998,
    "python_serialize_seconds": 0.07973050000000001,
    "setup_seconds": 0.053614499999999995,
    "submission_event_seconds": 0.0039065,
    "submit_seconds": 0.0311075
  },
  "datavine_useful_cpu_seconds": 139.349458625,
  "fetch_excess_seconds": 0.1893377749947831,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.1893377749947831
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.15300000000000002,
  "parallel_publication_service_seconds": {
    "commit": 2.9508254999999997,
    "decode": 0.2319695,
    "fsync": 1.9127275,
    "function": 139.98481149999998,
    "queue": 0.01025,
    "serialize": 0.07973050000000001,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.0016244290018221363,
  "taskvine_median_seconds": 1.8131319575186353,
  "taskvine_useful_cpu_seconds": 139.3539063985,
  "terminal_poll_residual_seconds": 0.024663302494512873,
  "useful_cpu_relative_difference": 3.19171067028216e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### topology_map

DataVine excess: 0.310446 s; classification: `result-fetch`; status: `CONFIRMED`.

Improvement: add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.03474755147180986,
  "client_wait_fetch_residual_seconds": 0.2816955085050473,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.6132582470086572,
      "name": "result-fetch",
      "seconds": 0.19038336799712852
    },
    {
      "fraction_of_gap": 0.4135570921653108,
      "name": "fixed-control",
      "seconds": 0.12838700897962696
    },
    {
      "fraction_of_gap": 0.2267159975857835,
      "name": "graph-materialization",
      "seconds": 0.070383
    },
    {
      "fraction_of_gap": 0.21106913743101682,
      "name": "scheduler-submit",
      "seconds": 0.0655255
    },
    {
      "fraction_of_gap": 0.2101237224118868,
      "name": "publication",
      "seconds": 0.065232
    }
  ],
  "datavine_fetch_seconds": 0.19211600349808577,
  "datavine_median_seconds": 1.3879227145080222,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.0352495,
    "materialize_seconds": 0.070383,
    "publication_commit_seconds": 3.5166345000000003,
    "publication_prepare_seconds": 0.0626535,
    "publication_queue_seconds": 0.0098765,
    "publish_seconds": 0.0025785,
    "python_decode_seconds": 0.22739199999999998,
    "python_fsync_seconds": 1.189308,
    "python_function_seconds": 10.4042955,
    "python_serialize_seconds": 0.07671700000000001,
    "setup_seconds": 0.06418399999999999,
    "submission_event_seconds": 0.0039605000000000005,
    "submit_seconds": 0.030276
  },
  "datavine_useful_cpu_seconds": 10.24113298,
  "fetch_excess_seconds": 0.19038336799712852,
  "largest_measured_component": {
    "name": "result-fetch",
    "seconds": 0.19038336799712852
  },
  "logical_edge_payload_bytes": 0,
  "manager_transfer_excess_seconds": 0.15644700000000003,
  "parallel_publication_service_seconds": {
    "commit": 3.5166345000000003,
    "decode": 0.22739199999999998,
    "fsync": 1.189308,
    "function": 10.4042955,
    "queue": 0.0098765,
    "serialize": 0.07671700000000001,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.001732635500957258,
  "taskvine_median_seconds": 1.0774770434945822,
  "taskvine_useful_cpu_seconds": 10.240998082499999,
  "terminal_poll_residual_seconds": 0.0203249575078171,
  "useful_cpu_relative_difference": 1.317212658639056e-05,
  "worker_execution_excess_seconds": 0.0
}
```

### topology_pipeline2

DataVine excess: 0.927315 s; classification: `graph-materialization+fixed-control+result-fetch`; status: `CONFIRMED`.

Improvement: compact task/DataID materialization and remove per-edge copies; reduce RPC polling, workflow setup, checkpoint and completion projection; add batched/multi-result fetch and avoid one RPC/decode per requested DataID

Evidence:

```json
{
  "build_submit_excess_seconds": 0.005297855008393526,
  "client_wait_fetch_residual_seconds": 0.5017231630107648,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.31457375772064133,
      "name": "graph-materialization",
      "seconds": 0.291709
    },
    {
      "fraction_of_gap": 0.29749997261730804,
      "name": "fixed-control",
      "seconds": 0.2758762210206063
    },
    {
      "fraction_of_gap": 0.293404318475427,
      "name": "result-fetch",
      "seconds": 0.27207825903315097
    },
    {
      "fraction_of_gap": 0.2027563200068467,
      "name": "scheduler-submit",
      "seconds": 0.18801899999999996
    },
    {
      "fraction_of_gap": 0.1405315173165066,
      "name": "publication",
      "seconds": 0.13031700000000002
    }
  ],
  "datavine_fetch_seconds": 0.27380448451731354,
  "datavine_median_seconds": 4.801967393490486,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.000377,
    "materialize_seconds": 0.291709,
    "publication_commit_seconds": 3.1829175,
    "publication_prepare_seconds": 0.1283765,
    "publication_queue_seconds": 0.039971999999999994,
    "publish_seconds": 0.0019405,
    "python_decode_seconds": 1.1676145,
    "python_fsync_seconds": 2.2838975,
    "python_function_seconds": 41.6414425,
    "python_serialize_seconds": 0.289025,
    "setup_seconds": 0.18963049999999998,
    "submission_event_seconds": 0.0192565,
    "submit_seconds": 0.18764199999999998
  },
  "datavine_useful_cpu_seconds": 40.964134467,
  "fetch_excess_seconds": 0.27207825903315097,
  "largest_measured_component": {
    "name": "graph-materialization",
    "seconds": 0.291709
  },
  "logical_edge_payload_bytes": 6291456,
  "manager_transfer_excess_seconds": 0.0,
  "parallel_publication_service_seconds": {
    "commit": 3.1829175,
    "decode": 1.1676145,
    "fsync": 2.2838975,
    "function": 41.6414425,
    "queue": 0.039971999999999994,
    "serialize": 0.289025,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1048576,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 0.001726225484162569,
  "taskvine_median_seconds": 3.8746522794972407,
  "taskvine_useful_cpu_seconds": 40.9640806965,
  "terminal_poll_residual_seconds": 0.050430866012212805,
  "useful_cpu_relative_difference": 1.312623852606639e-06,
  "worker_execution_excess_seconds": 0.0
}
```

### topology_reduce16

DataVine excess: 0.176690 s; classification: `fixed-control`; status: `CONFIRMED`.

Improvement: reduce RPC polling, workflow setup, checkpoint and completion projection

Evidence:

```json
{
  "build_submit_excess_seconds": 0.05193964649515692,
  "client_wait_fetch_residual_seconds": 0.15935176750190183,
  "critical_path_candidates": [
    {
      "fraction_of_gap": 0.8411296267242723,
      "name": "fixed-control",
      "seconds": 0.1486187980070291
    },
    {
      "fraction_of_gap": 0.40521478950376655,
      "name": "result-fetch",
      "seconds": 0.0715972105099354
    },
    {
      "fraction_of_gap": 0.27681889319791897,
      "name": "graph-materialization",
      "seconds": 0.048911
    },
    {
      "fraction_of_gap": 0.2368165228288222,
      "name": "scheduler-submit",
      "seconds": 0.041843
    },
    {
      "fraction_of_gap": 0.13689549158003853,
      "name": "publication",
      "seconds": 0.024188
    }
  ],
  "datavine_fetch_seconds": 0.07160705850401428,
  "datavine_median_seconds": 1.2506923755136086,
  "datavine_stage_medians": {
    "manager_lock_seconds": 0.000647,
    "materialize_seconds": 0.048911,
    "publication_commit_seconds": 0.007344,
    "publication_prepare_seconds": 0.0221135,
    "publication_queue_seconds": 0.0097595,
    "publish_seconds": 0.0020745,
    "python_decode_seconds": 0.2697545,
    "python_fsync_seconds": 0.0024939999999999997,
    "python_function_seconds": 11.103157,
    "python_serialize_seconds": 0.081403,
    "setup_seconds": 0.064699,
    "submission_event_seconds": 0.0040015,
    "submit_seconds": 0.041195999999999997
  },
  "datavine_useful_cpu_seconds": 10.931210053000001,
  "fetch_excess_seconds": 0.0715972105099354,
  "largest_measured_component": {
    "name": "fixed-control",
    "seconds": 0.1486187980070291
  },
  "logical_edge_payload_bytes": 1118208,
  "manager_transfer_excess_seconds": 0.0,
  "parallel_publication_service_seconds": {
    "commit": 0.007344,
    "decode": 0.2697545,
    "fsync": 0.0024939999999999997,
    "function": 11.103157,
    "queue": 0.0097595,
    "serialize": 0.081403,
    "wall_gap_attribution_allowed": false
  },
  "requested_payload_bytes": 1024,
  "separated_fetch_timing": true,
  "taskvine_fetch_seconds": 9.847994078882039e-06,
  "taskvine_median_seconds": 1.0740028459986206,
  "taskvine_useful_cpu_seconds": 10.931077936,
  "terminal_poll_residual_seconds": 0.023872651511872178,
  "useful_cpu_relative_difference": 1.2086219124956555e-05,
  "worker_execution_excess_seconds": 0.0
}
```

