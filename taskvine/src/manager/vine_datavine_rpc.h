#ifndef VINE_DATAVINE_RPC_H
#define VINE_DATAVINE_RPC_H

#include <stddef.h>
#include <stdint.h>

#include "vine_datavine_directory.h"
#include "vine_datavine_journal.h"

#define VINE_DATAVINE_RPC_MAGIC UINT32_C(0x44564331)
#define VINE_DATAVINE_RPC_VERSION 1
#define VINE_DATAVINE_RPC_REQUEST_HEADER 20
#define VINE_DATAVINE_RPC_RESPONSE_HEADER 24

enum vine_datavine_rpc_opcode {
	VINE_DATAVINE_RPC_AUTH = 1,
	VINE_DATAVINE_RPC_PING = 2,
	VINE_DATAVINE_RPC_ALLOCATE_BATCH = 3,
	VINE_DATAVINE_RPC_PUBLISH_BATCH = 4,
	VINE_DATAVINE_RPC_CLAIM_WORKER = 5,
	VINE_DATAVINE_RPC_PUBLISH_OUTPUTS = 6,
	VINE_DATAVINE_RPC_DISCONNECT_WORKER = 7,
	VINE_DATAVINE_RPC_REPORT_REPLICA = 8,
	VINE_DATAVINE_RPC_RESOLVE_SOURCE = 9,
	VINE_DATAVINE_RPC_RELEASE_SOURCE = 10,
	VINE_DATAVINE_RPC_REGISTER_EDATA = 11,
	VINE_DATAVINE_RPC_GET_EDATA = 12,
	VINE_DATAVINE_RPC_INVALIDATE_REPLICA = 13,
};

enum vine_datavine_rpc_status {
	VINE_DATAVINE_RPC_OK = 0,
	VINE_DATAVINE_RPC_INVALID = 1,
	VINE_DATAVINE_RPC_UNAUTHORIZED = 2,
	VINE_DATAVINE_RPC_REJECTED = 3,
	VINE_DATAVINE_RPC_INTERNAL = 4,
	VINE_DATAVINE_RPC_NOT_FOUND = 5,
};

struct vine_datavine_rpc_server;

struct vine_datavine_rpc_server *vine_datavine_rpc_server_create(
		const char *host, int port, const char *token, int threads,
		int64_t maximum_data_id, const char *journal_path);
void vine_datavine_rpc_server_delete(struct vine_datavine_rpc_server *server);
int vine_datavine_rpc_server_port(const struct vine_datavine_rpc_server *server);
void vine_datavine_rpc_server_get_metrics(
		struct vine_datavine_rpc_server *server,
		struct vine_datavine_directory_metrics *result);
int64_t vine_datavine_rpc_server_replica_active_leases(
		struct vine_datavine_rpc_server *server, char kind, int64_t data_id,
		const char *replica_id);
void vine_datavine_rpc_server_get_journal_metrics(
		struct vine_datavine_rpc_server *server,
		struct vine_datavine_journal_metrics *result);

uint32_t vine_datavine_rpc_get_u32(const unsigned char *buffer);
uint64_t vine_datavine_rpc_get_u64(const unsigned char *buffer);
void vine_datavine_rpc_put_u32(unsigned char *buffer, uint32_t value);
void vine_datavine_rpc_put_u64(unsigned char *buffer, uint64_t value);

#endif
