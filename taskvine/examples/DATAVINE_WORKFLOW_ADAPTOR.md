# DataVine workflow adaptor contract

An adaptor builds `datavine.workflow/v1`, performs capability negotiation, and
sends transactions to one workflow service process per workflow. It must not
schedule tasks, construct TaskVine tasks,
project completions, retry work, or implement persistence/pruning policy.

The JSON contract is
[`datavine_workflow_v1.schema.json`](../src/manager/datavine_workflow_v1.schema.json).
The cross-language golden documents and canonical SHA-1 digests are in
[`datavine_workflow_ir_fixtures.json`](../test/datavine_workflow_ir_fixtures.json).

## DVC1 transport

Connect to the advertised `tcp://host:port` endpoint. All integers are unsigned
big-endian unless stated otherwise. Every connection must authenticate first.

Request header, 20 bytes:

| Offset | Size | Meaning |
| ---: | ---: | --- |
| 0 | 4 | magic `0x44564331` (`DVC1`) |
| 4 | 2 | protocol version, currently `1` |
| 6 | 2 | opcode |
| 8 | 4 | payload byte length, at most 64 MiB |
| 12 | 8 | monotonically increasing client request ID |

Response header, 24 bytes:

| Offset | Size | Meaning |
| ---: | ---: | --- |
| 0 | 4 | `DVC1` magic |
| 4 | 2 | protocol version |
| 6 | 2 | echoed opcode |
| 8 | 4 | status: 0 OK, 1 invalid, 2 unauthorized, 3 rejected, 4 internal, 5 not found |
| 12 | 4 | body byte length |
| 16 | 8 | echoed request ID |

Workflow opcodes are: auth `1`, submit `20`, append `21`, seal `22`, describe
`23`, watch `24`, cancel `25`, capabilities `26`, fetch-result `27`, and
result-info `28`. Dynamic clients also use frontier `29`, object-put/get
`30`/`31`, result descriptors `36`, wait-terminal `37`, and result-stream `38`.
The complete opcode definition is in
[`vine_datavine_protocol.h`](../src/datavine/vine_datavine_protocol.h).
First send auth with the token bytes, then capabilities with
an empty payload. Reject an unsupported schema or executor before mutation.

Submit payload is canonical Workflow IR JSON. Append payload is
`u16 id_length, u16 reserved, u64 expected_generation, id bytes, snapshot JSON`.
Seal uses the same fixed prefix without JSON. Describe/cancel use
`u16 id_length, id bytes`. Result/result-info use
`u16 id_length, u16 reserved, u64 DataID, id bytes`. Watch uses
`u16 id_length, u16 limit, u64 after_event_id, id bytes`.

Submit/append/seal/describe/cancel return `DWI1`: state at offset 4,
generation/event ID and graph counters through offset 56, streaming at 56,
WorkflowID length at 60, 40-byte canonical digest at 64, and WorkflowID bytes
at 104. Watch returns a count followed by fixed 76-byte events. Fetch-result
returns exact result bytes. Result-info returns JSON containing SHA-256, size,
attempt, producer TaskID/output slot, requested status, and codec.

The minimal direct implementation is
[`datavine_workflow_go.go`](datavine_workflow_go.go). The POSIX-shell example
uses the stable CLI in [`datavine_workflow_shell.sh`](datavine_workflow_shell.sh).

## Dynamic clients and object transport

Check advertised capabilities before submitting. Current callable invocations
use DVP3 for small inline payloads and DVP4 signed Controller-RPC references for
large payloads; source execution uses DVP1. Do not copy transitional frame
labels or Controller-local path assumptions from old benchmark reports.
[The production contract](../../DATAVINE_PRODUCTION.md) owns those semantics.

For `controller-admitted-sequence-v1`, open a dedicated authenticated socket
and send opcode 38 with
`u16 id_length, u32 inline_max_bytes, u64 after_sequence, id bytes`.
`inline_max_bytes` is at most 65536. Responses use the normal 24-byte header
and a `DVS1` body. Resume with the last consumed sequence after disconnect;
consume the terminal frame. These are durable Controller result admissions,
not Scheduler completion events.

Use the Python client's
[`WorkflowResultStream`](../src/bindings/python3/ndcctools/taskvine/datavine/workflow_client.py)
as the maintained frame-decoding reference. The Go and shell examples above
cover basic workflow submission; they are not a complete dynamic protocol
implementation. Metadata recovery and requested-result persistence are separate
policies; adaptors must not infer result durability from task completion.
