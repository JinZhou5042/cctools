# DataVine handoff

Updated: 2026-09-06. This filename is retained for existing references.

Start with [DATAVINE_MAP.md](DATAVINE_MAP.md). Runtime semantics live in
[DATAVINE_PRODUCTION.md](DATAVINE_PRODUCTION.md); build commands and evidence
navigation live in [acceptance/README.md](acceptance/README.md).

## Source and delivery state

The working branch is `production-baseline`, based on `c2e9a85be`. Runtime,
tests and benchmark work remain uncommitted. Inspect Git before continuing;
preserve those changes. Historical PASS records do not establish a clean
release or a fresh runtime test.

The current implementation includes Worker-local elastic FunctionCall
admission, generation-bound queued-call recall, native lifecycle optimizations
and DVP3/DVP4 Controller-RPC invocation handling. The September 3 elastic
campaign records 21/21 DataVine regression and a generic serverless PASS.
Current gate limits are in [the matrix](acceptance/matrix.md).

The previously verified package is
`/users/jzhou24/graph_optimization/factories/datavine.production-baseline-b481c3768.tar.gz`.
It predates the current elastic/RPC implementation. The canonical
`datavine.tar.gz` has not been replaced by this work.

## Next work

1. Review and commit the current source/test changes; verify a fresh build and
   runtime suite before preparing a matching candidate package.
2. Evaluate repeated multi-node workloads with actual CPU, I/O and dependency
   work. The elastic campaign has one remote scale point, not a scaling curve.
3. If native no-op throughput is the target, repeat 8x16 and 32x16 under the
   same code and stable allocation, then profile Manager send/status work.
4. Keep the dead command-stdout transfer issue visible until a focused runtime
   test establishes its status. Cross-host Controller durability remains outside
   the current contract.

The [maintenance record](acceptance/maintenance-20260906.md) identifies removed
build artifacts and their rebuild commands. Accepted campaign data is retained.
