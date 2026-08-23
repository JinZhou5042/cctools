# DataVine production contract

Updated: 2026-08-23

Status: **PRODUCTION V1 FROZEN**

The authoritative source checkpoint is the annotated Git tag
`datavine-production-v1-20260823`. The tag covers the runtime implementation,
tests, benchmark tooling, retained acceptance artifacts, this contract and the
complete handoff checksum manifest.

DataVine has one supported production contract. New workflows, deltas, RPC
clients, journals, Python executors, worker tickets, output manifests and native
builtins all use v1. The active implementation does not negotiate or emit
historical DataVine protocol generations.

| Boundary | Production value |
|---|---|
| Workflow schema | `datavine.workflow/v1` |
| Delta schema | `datavine.workflow-delta/v1` |
| RPC framing | version `1` |
| Journal framing | version `1` |
| Command executor | version `1` |
| Python source executor | `source-v1` |
| Python callable executor | `callable-v1` |
| Python worker ticket | `DVP1` |
| Python output manifest | `DVM1` |
| Native builtin executor | `builtin-v1` / `DVB1` |
| Native runtime ticket | `DVT1` |
| Durable result key | `DVE1` |
| Result descriptor batch | `DVR1` |

The v1 Python callable is the current object-backed contract: function and
invocation bytes are content-addressed immutable objects, workers resolve them
from the configured object root, outputs use explicit DataIDs and attempts, and
only requested outputs are durable. There is no inline-callable compatibility
ticket and no alternate output-manifest parser.

Codec versions such as `python/cloudpickle` version `3`, scientific artifact
schema versions and immutable acceptance-directory names describe their own
external formats or historical evidence. They are not alternate DataVine
production protocols and are not renamed by this freeze.

Historical journals or Workflow IR that contain removed executor or ticket
generations are not migrated. They fail closed. A future incompatible change
must introduce an explicit migration tool before changing this production
contract.

The source regression passed 13/13 after the freeze. Retained evidence is
`acceptance/production-v1-regression-20260823.json`; source and document hashes
are in `acceptance/production-v1.sha256`.

The promoted production environment is
`/users/jzhou24/graph_optimization/factories/datavine.tar.gz`, SHA-256
`6019adc524f86bf4d14b984e8a6f07928cf08964ec2a6a19031e36829f50adcd`.
`poncho_package_run` verified Python 3.10.20, cloudpickle 3.1.2, the production
builder value, protocol markers and exact hashes for the service, native
executor, Python executor and worker. The active-path 1x2x10k smoke passed at
exactly 10,000 submissions/completions and 3,820.6 Runtime tasks/s. The prior
package is retained as `datavine.pre-production-v1-rollback-20260823.tar.gz`,
SHA-256 `32e1361324df69be2db88258565f3ad393d2eb16ff9228e99387b895d9abba6d`.
