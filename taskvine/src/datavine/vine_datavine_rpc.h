/* DataVine RPC API. */
#ifndef VINE_DATAVINE_RPC_H
#define VINE_DATAVINE_RPC_H

#include <stddef.h>
#include <stdint.h>

#include "vine_datavine_protocol.h"
#include "vine_datavine_workflow_store.h"

struct vine_datavine_rpc_server;
struct vine_datavine_data_controller;

struct vine_datavine_rpc_server *vine_datavine_rpc_server_create(
		const char *host, int port, const char *token, int threads,
		const char *workflow_journal_path);
void vine_datavine_rpc_server_delete(struct vine_datavine_rpc_server *server);
int vine_datavine_rpc_server_port(const struct vine_datavine_rpc_server *server);
struct vine_datavine_workflow_store *vine_datavine_rpc_server_workflow_store(
		struct vine_datavine_rpc_server *server);
struct vine_datavine_data_controller *vine_datavine_rpc_server_data_controller(
		struct vine_datavine_rpc_server *server);

#endif
