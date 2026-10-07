# DataVine dynamic pilot, 2026-08-30

The original 2x4 Condor pilot discovered the same 477-task, 445-edge graph on
both backends, with exact physical completion and matching results. DataVine
was slower in those three pairs. Its initial RPC-only bottleneck explanation
was superseded by the measured invocation-object persistence diagnosis.

The combined findings, corrected cause and scope are maintained in
[the dynamic control report](DATAVINE_DYNAMIC_CONTROL_20260831.md).
[The original machine evidence](acceptance/dynamic-adaptive-fixed-ab-20260830.json)
is retained unchanged. This pilot is not a current performance baseline.
