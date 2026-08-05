#ifndef VINE_DATAVINE_RPC_H
#define VINE_DATAVINE_RPC_H

#include <stddef.h>
#include <stdint.h>

#include "vine_datavine_directory.h"
#include "vine_datavine_journal.h"
#include "vine_datavine_protocol.h"

struct vine_datavine_rpc_server;

struct vine_datavine_rpc_server *vine_datavine_rpc_server_create(
		const char *host, int port, const char *token, int threads,
		int64_t maximum_data_id, uint64_t maximum_edata_bytes,
		const char *journal_path);
void vine_datavine_rpc_server_delete(struct vine_datavine_rpc_server *server);
int vine_datavine_rpc_server_port(const struct vine_datavine_rpc_server *server);
void vine_datavine_rpc_server_get_metrics(
		struct vine_datavine_rpc_server *server,
		struct vine_datavine_directory_metrics *result);
int vine_datavine_rpc_server_snapshot_replicas(
		struct vine_datavine_rpc_server *server, char kind, int64_t data_id,
		struct vine_datavine_replica_snapshot **result, size_t *count);
void vine_datavine_rpc_server_get_journal_metrics(
		struct vine_datavine_rpc_server *server,
		struct vine_datavine_journal_metrics *result);

uint32_t vine_datavine_rpc_get_u32(const unsigned char *buffer);
uint64_t vine_datavine_rpc_get_u64(const unsigned char *buffer);
void vine_datavine_rpc_put_u32(unsigned char *buffer, uint32_t value);
void vine_datavine_rpc_put_u64(unsigned char *buffer, uint64_t value);

#endif
