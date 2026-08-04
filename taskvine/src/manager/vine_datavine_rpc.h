#ifndef VINE_DATAVINE_RPC_H
#define VINE_DATAVINE_RPC_H

#include <stddef.h>
#include <stdint.h>

#define VINE_DATAVINE_RPC_MAGIC UINT32_C(0x44564331)
#define VINE_DATAVINE_RPC_VERSION 1
#define VINE_DATAVINE_RPC_REQUEST_HEADER 20
#define VINE_DATAVINE_RPC_RESPONSE_HEADER 24

enum vine_datavine_rpc_opcode {
	VINE_DATAVINE_RPC_AUTH = 1,
	VINE_DATAVINE_RPC_PING = 2,
	VINE_DATAVINE_RPC_ALLOCATE_BATCH = 3,
	VINE_DATAVINE_RPC_PUBLISH_BATCH = 4,
};

enum vine_datavine_rpc_status {
	VINE_DATAVINE_RPC_OK = 0,
	VINE_DATAVINE_RPC_INVALID = 1,
	VINE_DATAVINE_RPC_UNAUTHORIZED = 2,
	VINE_DATAVINE_RPC_REJECTED = 3,
	VINE_DATAVINE_RPC_INTERNAL = 4,
};

struct vine_datavine_rpc_server;

struct vine_datavine_rpc_server *vine_datavine_rpc_server_create(
		const char *host, int port, const char *token, int threads,
		int64_t maximum_data_id);
void vine_datavine_rpc_server_delete(struct vine_datavine_rpc_server *server);
int vine_datavine_rpc_server_port(const struct vine_datavine_rpc_server *server);

uint32_t vine_datavine_rpc_get_u32(const unsigned char *buffer);
uint64_t vine_datavine_rpc_get_u64(const unsigned char *buffer);
void vine_datavine_rpc_put_u32(unsigned char *buffer, uint32_t value);
void vine_datavine_rpc_put_u64(unsigned char *buffer, uint64_t value);

#endif
