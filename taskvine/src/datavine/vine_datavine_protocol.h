/* Language-neutral DataVine workflow protocol. */
#ifndef VINE_DATAVINE_PROTOCOL_H
#define VINE_DATAVINE_PROTOCOL_H

#include <stdint.h>

static inline uint16_t vine_datavine_get_u16(const unsigned char *buffer)
{
	return (uint16_t)(((uint16_t)buffer[0] << 8) | buffer[1]);
}

static inline uint32_t vine_datavine_get_u32(const unsigned char *buffer)
{
	return ((uint32_t)buffer[0] << 24) | ((uint32_t)buffer[1] << 16) |
	       ((uint32_t)buffer[2] << 8) | buffer[3];
}

static inline uint64_t vine_datavine_get_u64(const unsigned char *buffer)
{
	return ((uint64_t)vine_datavine_get_u32(buffer) << 32) |
	       vine_datavine_get_u32(buffer + 4);
}

static inline void vine_datavine_put_u16(unsigned char *buffer, uint16_t value)
{
	buffer[0] = (unsigned char)(value >> 8);
	buffer[1] = (unsigned char)value;
}

static inline void vine_datavine_put_u32(unsigned char *buffer, uint32_t value)
{
	buffer[0] = (unsigned char)(value >> 24);
	buffer[1] = (unsigned char)(value >> 16);
	buffer[2] = (unsigned char)(value >> 8);
	buffer[3] = (unsigned char)value;
}

static inline void vine_datavine_put_u64(unsigned char *buffer, uint64_t value)
{
	vine_datavine_put_u32(buffer, (uint32_t)(value >> 32));
	vine_datavine_put_u32(buffer + 4, (uint32_t)value);
}

#define VINE_DATAVINE_RPC_MAGIC UINT32_C(0x44564331)
#define VINE_DATAVINE_RPC_VERSION 1
#define VINE_DATAVINE_PYTHON_TICKET_MAGIC "DVP1"
#define VINE_DATAVINE_OUTPUT_MANIFEST_MAGIC "DVM1"
#define VINE_DATAVINE_RPC_REQUEST_HEADER 20
#define VINE_DATAVINE_RPC_RESPONSE_HEADER 24
#define VINE_DATAVINE_RPC_MAX_PAYLOAD (64U * 1024U * 1024U)
#define VINE_DATAVINE_AGENT_HELLO_MAGIC "DVA1"
#define VINE_DATAVINE_TASK_SPEC_MAGIC "DVT2"
#define VINE_DATAVINE_AGENT_MAX_BATCH 1024U
#define VINE_DATAVINE_AGENT_HOST_MAX 64U
#define VINE_DATAVINE_AGENT_HELLO_SIZE 120U
#define VINE_DATAVINE_AGENT_BATCH_HEADER 40U
#define VINE_DATAVINE_AGENT_PUBLISH_RECORD 64U
#define VINE_DATAVINE_AGENT_RESOLVE_RECORD 16U
#define VINE_DATAVINE_AGENT_RESOLVE_REPLY 152U
#define VINE_DATAVINE_AGENT_FAULT_RECORD 40U
#define VINE_DATAVINE_AGENT_RELEASE_RECORD 24U
#define VINE_DATAVINE_TASK_SPEC_HEADER 144U
#define VINE_DATAVINE_TASK_SPEC_INPUT 24U
#define VINE_DATAVINE_TASK_SPEC_OUTPUT 32U

enum vine_datavine_task_input_kind {
	VINE_DATAVINE_TASK_INPUT_GENERATED = 0,
	VINE_DATAVINE_TASK_INPUT_URI = 1,
	VINE_DATAVINE_TASK_INPUT_URI_EPHEMERAL = 2,
	VINE_DATAVINE_TASK_INPUT_LOCAL_FILE = 3,
};

enum vine_datavine_task_output_flags {
	VINE_DATAVINE_TASK_OUTPUT_RETAIN = 1,
	VINE_DATAVINE_TASK_OUTPUT_REQUESTED = 2,
};

enum vine_datavine_agent_wire_resolve_status {
	VINE_DATAVINE_AGENT_WIRE_UNKNOWN = 0,
	VINE_DATAVINE_AGENT_WIRE_PENDING = 1,
	VINE_DATAVINE_AGENT_WIRE_AVAILABLE = 2,
	VINE_DATAVINE_AGENT_WIRE_DEAD = 3,
};

enum vine_datavine_agent_fault_flags {
	/* The reporting Worker failed to fetch the replica identified by the
	 * record's worker/session/token tuple. */
	VINE_DATAVINE_AGENT_FAULT_REMOTE = 1,
};

enum vine_datavine_rpc_opcode {
	VINE_DATAVINE_RPC_AUTH = 1,
	/* Values 2-19 are reserved after removing unused prototype operations. */
	VINE_DATAVINE_RPC_WORKFLOW_SUBMIT = 20,
	VINE_DATAVINE_RPC_WORKFLOW_APPEND = 21,
	VINE_DATAVINE_RPC_WORKFLOW_SEAL = 22,
	VINE_DATAVINE_RPC_WORKFLOW_DESCRIBE = 23,
	VINE_DATAVINE_RPC_WORKFLOW_WATCH = 24,
	VINE_DATAVINE_RPC_WORKFLOW_CANCEL = 25,
	VINE_DATAVINE_RPC_WORKFLOW_CAPABILITIES = 26,
	VINE_DATAVINE_RPC_WORKFLOW_FETCH_RESULT = 27,
	VINE_DATAVINE_RPC_WORKFLOW_RESULT_INFO = 28,
	VINE_DATAVINE_RPC_WORKFLOW_FRONTIER = 29,
	VINE_DATAVINE_RPC_OBJECT_PUT = 30,
	VINE_DATAVINE_RPC_OBJECT_GET = 31,
	VINE_DATAVINE_RPC_OBJECT_PATH = 32,
	/* 33 is reserved after removing redundant SharedFS registration. */
	VINE_DATAVINE_RPC_WORKFLOW_RESULT_PATH = 34,
	VINE_DATAVINE_RPC_RESULT_PERSISTED = 35,
	VINE_DATAVINE_RPC_WORKFLOW_RESULT_DESCRIPTORS = 36,
	VINE_DATAVINE_RPC_WORKFLOW_WAIT_TERMINAL = 37,
	/* Runtime-v2 Worker Data Agent metadata. Payload bytes are forbidden. */
	VINE_DATAVINE_RPC_AGENT_HELLO = 40,
	VINE_DATAVINE_RPC_AGENT_DATA_READY = 41,
	VINE_DATAVINE_RPC_AGENT_RESOLVE = 42,
	VINE_DATAVINE_RPC_AGENT_DATA_FAULT = 43,
	VINE_DATAVINE_RPC_AGENT_HEARTBEAT = 44,
	VINE_DATAVINE_RPC_AGENT_PERSISTED = 45,
};

enum vine_datavine_rpc_status {
	VINE_DATAVINE_RPC_OK = 0,
	VINE_DATAVINE_RPC_INVALID = 1,
	VINE_DATAVINE_RPC_UNAUTHORIZED = 2,
	VINE_DATAVINE_RPC_REJECTED = 3,
	VINE_DATAVINE_RPC_INTERNAL = 4,
	VINE_DATAVINE_RPC_NOT_FOUND = 5,
};

#endif
