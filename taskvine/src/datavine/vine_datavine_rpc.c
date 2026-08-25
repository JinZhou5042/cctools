/* DataVine RPC implementation.
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_datavine_rpc.h"
#include "vine_datavine_data_controller.h"
#include "vine_datavine_replica_table.h"
#include "vine_datavine_workflow_store.h"
#include "jx.h"
#include "jx_print.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define DATAVINE_RPC_EVENTS 128
#define DATAVINE_RPC_MAX_CONNECTIONS 1024
#define DATAVINE_RPC_IDLE_SECONDS 30

struct vine_datavine_rpc_server;

struct rpc_connection {
	int fd;
	int authorized;
	char object_digest[65];
	time_t last_activity;
	unsigned char header[VINE_DATAVINE_RPC_REQUEST_HEADER];
	size_t header_used;
	unsigned char *payload;
	size_t payload_size;
	size_t payload_used;
	unsigned char *response;
	size_t response_size;
	size_t response_used;
	int waiting_terminal;
	uint16_t waiting_opcode;
	uint64_t waiting_request_id;
	char waiting_workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	int agent;
	uint64_t agent_workflow_slot;
	uint32_t agent_worker_slot;
	uint64_t agent_session_epoch;
	uint64_t agent_sequence;
	struct rpc_connection *next;
};

static int lowercase_hex(const unsigned char *value, size_t size)
{
	for (size_t index = 0; index < size; index++) {
		if (!((value[index] >= '0' && value[index] <= '9') ||
					(value[index] >= 'a' && value[index] <= 'f')))
			return 0;
	}
	return 1;
}

struct rpc_thread {
	struct vine_datavine_rpc_server *server;
	pthread_t id;
	int epoll_fd;
	struct rpc_connection *connections;
};

struct vine_datavine_rpc_server {
	int listen_fd;
	int port;
	char *token;
	size_t token_length;
	int thread_count;
	struct rpc_thread *threads;
	struct vine_datavine_workflow_store *workflow_store;
	struct vine_datavine_data_controller *data_controller;
	atomic_int stopping;
	atomic_size_t active_connections;
};

static int validate_object_ticket(struct vine_datavine_rpc_server *server,
		const unsigned char *ticket, size_t ticket_size,
		char object_digest[65])
{
	/* lowercase sha256 + lowercase HMAC-SHA256. */
	if (ticket_size != 128 || !lowercase_hex(ticket, 128))
		return 0;
	unsigned char message[84] = "datavine-object-v1:";
	memcpy(message + 19, ticket, 64);
	unsigned char signature[EVP_MAX_MD_SIZE];
	unsigned int signature_size = 0;
	if (!HMAC(EVP_sha256(), server->token, (int)server->token_length, message, 83, signature, &signature_size) || signature_size != 32)
		return 0;
	unsigned char encoded[64];
	static const unsigned char hexadecimal[] = "0123456789abcdef";
	for (size_t index = 0; index < 32; index++) {
		encoded[index * 2] = hexadecimal[signature[index] >> 4];
		encoded[index * 2 + 1] = hexadecimal[signature[index] & 15];
	}
	if (CRYPTO_memcmp(encoded, ticket + 64, 64))
		return 0;
	memcpy(object_digest, ticket, 64);
	object_digest[64] = 0;
	return 1;
}

static int authorize_object(struct vine_datavine_rpc_server *server,
		struct rpc_connection *connection)
{
	/* Compatibility auth frame: DVO1 + one object ticket. */
	if (connection->payload_size != 132 ||
			memcmp(connection->payload, "DVO1", 4))
		return 0;
	return validate_object_ticket(server, connection->payload + 4, 128, connection->object_digest);
}

struct vine_datavine_workflow_store *vine_datavine_rpc_server_workflow_store(
		struct vine_datavine_rpc_server *server)
{
	return server ? server->workflow_store : 0;
}

struct vine_datavine_data_controller *vine_datavine_rpc_server_data_controller(
		struct vine_datavine_rpc_server *server)
{
	return server ? server->data_controller : 0;
}

static int set_nonblocking(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);
	return flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

static uint16_t decode_workflow_id(const unsigned char *payload, size_t size,
		size_t header_size, int trailing,
		char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1])
{
	if (size < header_size)
		return 0;
	uint16_t id_size = vine_datavine_get_u16(payload);
	if (!id_size || id_size > VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX ||
			(trailing ? size <= header_size + id_size
				  : size != header_size + id_size))
		return 0;
	memcpy(workflow_id, payload + header_size, id_size);
	workflow_id[id_size] = 0;
	return id_size;
}

static void connection_delete(struct rpc_thread *thread, struct rpc_connection *connection)
{
	if (!connection) {
		return;
	}
	struct rpc_connection **cursor = &thread->connections;
	while (*cursor && *cursor != connection) {
		cursor = &(*cursor)->next;
	}
	if (*cursor) {
		*cursor = connection->next;
	}
	if (connection->agent)
		vine_datavine_data_controller_agent_session_lost(
				thread->server->data_controller,
				connection->agent_workflow_slot,
				connection->agent_worker_slot,
				connection->agent_session_epoch);
	close(connection->fd);
	atomic_fetch_sub(&thread->server->active_connections, 1);
	free(connection->payload);
	free(connection->response);
	free(connection);
}

static int agent_header(
		struct rpc_connection *connection, const unsigned char *payload,
		size_t payload_size, size_t record_size, uint32_t *count)
{
	if (!connection->agent || payload_size < VINE_DATAVINE_AGENT_BATCH_HEADER)
		return 0;
	uint32_t records = vine_datavine_get_u32(payload + 32);
	if (!records || records > VINE_DATAVINE_AGENT_MAX_BATCH ||
			vine_datavine_get_u64(payload) !=
				connection->agent_workflow_slot ||
			vine_datavine_get_u32(payload + 8) !=
				connection->agent_worker_slot ||
			vine_datavine_get_u32(payload + 12) ||
			vine_datavine_get_u64(payload + 16) !=
				connection->agent_session_epoch ||
			vine_datavine_get_u32(payload + 36) ||
			records > (SIZE_MAX - VINE_DATAVINE_AGENT_BATCH_HEADER) /
				record_size ||
			payload_size != VINE_DATAVINE_AGENT_BATCH_HEADER +
				(size_t)records * record_size)
		return 0;
	*count = records;
	return 1;
}

static uint32_t agent_hello(struct vine_datavine_rpc_server *server,
		struct rpc_connection *connection, const unsigned char *payload,
		size_t payload_size, unsigned char **result, size_t *result_size)
{
	if (connection->agent || payload_size != VINE_DATAVINE_AGENT_HELLO_SIZE ||
			memcmp(payload, VINE_DATAVINE_AGENT_HELLO_MAGIC, 4) ||
			vine_datavine_get_u32(payload + 36) == UINT32_MAX ||
			!vine_datavine_get_u64(payload + 40) ||
			!vine_datavine_get_u16(payload + 48) ||
			!vine_datavine_get_u16(payload + 50) ||
			vine_datavine_get_u16(payload + 50) >=
				VINE_DATAVINE_AGENT_HOST_MAX ||
			vine_datavine_get_u32(payload + 116))
		return VINE_DATAVINE_RPC_INVALID;
	uint64_t workflow_slot = 0;
	uint32_t worker_slot = vine_datavine_get_u32(payload + 36);
	uint64_t session_epoch = vine_datavine_get_u64(payload + 40);
	uint16_t host_size = vine_datavine_get_u16(payload + 50);
	char host[VINE_DATAVINE_AGENT_HOST_MAX];
	memcpy(host, payload + 52, host_size);
	host[host_size] = 0;
	if (!vine_datavine_data_controller_agent_hello(server->data_controller,
			payload + 4, &worker_slot, session_epoch, host,
			vine_datavine_get_u16(payload + 48), &workflow_slot))
		return VINE_DATAVINE_RPC_REJECTED;
	*result = calloc(1, 16);
	if (!*result)
		return VINE_DATAVINE_RPC_INTERNAL;
	vine_datavine_put_u64(*result, workflow_slot);
	vine_datavine_put_u32(*result + 8, worker_slot);
	*result_size = 16;
	connection->agent = 1;
	connection->agent_workflow_slot = workflow_slot;
	connection->agent_worker_slot = worker_slot;
	connection->agent_session_epoch = session_epoch;
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t agent_data_ready(struct vine_datavine_rpc_server *server,
		struct rpc_connection *connection, const unsigned char *payload,
		size_t payload_size)
{
	uint32_t count = 0;
	if (!agent_header(connection, payload, payload_size,
			VINE_DATAVINE_AGENT_PUBLISH_RECORD, &count))
		return VINE_DATAVINE_RPC_INVALID;
	uint64_t sequence = vine_datavine_get_u64(payload + 24);
	if (!sequence)
		return VINE_DATAVINE_RPC_INVALID;
	if (sequence <= connection->agent_sequence)
		return VINE_DATAVINE_RPC_OK;
	struct vine_datavine_publish_record records[VINE_DATAVINE_AGENT_MAX_BATCH];
	for (uint32_t index = 0; index < count; index++) {
		const unsigned char *record = payload +
			VINE_DATAVINE_AGENT_BATCH_HEADER +
			(size_t)index * VINE_DATAVINE_AGENT_PUBLISH_RECORD;
		uint64_t data_id = vine_datavine_get_u64(record);
		uint32_t generation = vine_datavine_get_u32(record + 8);
		uint32_t flags = vine_datavine_get_u32(record + 12);
		uint64_t size = vine_datavine_get_u64(record + 16);
		uint64_t object_token = vine_datavine_get_u64(record + 24);
		if (!data_id || !generation || !object_token || flags & ~UINT32_C(1))
			return VINE_DATAVINE_RPC_REJECTED;
		records[index].data_id = data_id;
		records[index].generation = generation;
		records[index].size = size;
		records[index].object_token = object_token;
		records[index].requested = flags & 1;
		memcpy(records[index].digest, record + 32, 32);
	}
	if (!vine_datavine_data_controller_agent_publish_batch(
			server->data_controller, connection->agent_workflow_slot,
			records, count, connection->agent_worker_slot,
			connection->agent_session_epoch))
		return VINE_DATAVINE_RPC_REJECTED;
	connection->agent_sequence = sequence;
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t agent_resolve(struct vine_datavine_rpc_server *server,
		struct rpc_connection *connection, const unsigned char *payload,
		size_t payload_size, unsigned char **result, size_t *result_size)
{
	uint32_t count = 0;
	if (!agent_header(connection, payload, payload_size,
			VINE_DATAVINE_AGENT_RESOLVE_RECORD, &count))
		return VINE_DATAVINE_RPC_INVALID;
	*result_size = (size_t)count * VINE_DATAVINE_AGENT_RESOLVE_REPLY;
	*result = calloc(1, *result_size);
	if (!*result)
		return VINE_DATAVINE_RPC_INTERNAL;
	for (uint32_t index = 0; index < count; index++) {
		const unsigned char *record = payload +
			VINE_DATAVINE_AGENT_BATCH_HEADER +
			(size_t)index * VINE_DATAVINE_AGENT_RESOLVE_RECORD;
		unsigned char *reply = *result +
			(size_t)index * VINE_DATAVINE_AGENT_RESOLVE_REPLY;
		uint64_t data_id = vine_datavine_get_u64(record);
		uint32_t generation = vine_datavine_get_u32(record + 8);
		if (!data_id || vine_datavine_get_u32(record + 12)) {
			free(*result);
			*result = 0;
			*result_size = 0;
			return VINE_DATAVINE_RPC_INVALID;
		}
		struct vine_datavine_agent_replica replica;
		size_t replica_count = 0;
		uint64_t size = 0;
		unsigned char digest[32] = {0};
		int persisted = 0;
		enum vine_datavine_agent_resolve_status status =
			vine_datavine_data_controller_agent_resolve(
				server->data_controller,
				connection->agent_workflow_slot, data_id, generation,
				&replica, 1, &replica_count, &size, digest, &persisted);
		vine_datavine_put_u32(reply, (uint32_t)status);
		vine_datavine_put_u32(reply + 4,
			replica_count ? replica.generation : generation);
		vine_datavine_put_u64(reply + 8, data_id);
		vine_datavine_put_u64(reply + 16, size);
		memcpy(reply + 24, digest, sizeof(digest));
		if (replica_count) {
			vine_datavine_put_u32(reply + 56, replica.worker_slot);
			vine_datavine_put_u32(reply + 60, persisted ? 1 : 0);
			vine_datavine_put_u64(reply + 64, replica.session_epoch);
			vine_datavine_put_u64(reply + 72, replica.object_token);
			size_t host_size = strlen(replica.host);
			if (!host_size || host_size >= VINE_DATAVINE_AGENT_HOST_MAX ||
					!replica.port) {
				free(*result);
				*result = 0;
				*result_size = 0;
				return VINE_DATAVINE_RPC_INTERNAL;
			}
			vine_datavine_put_u16(reply + 80, replica.port);
			vine_datavine_put_u16(reply + 82, (uint16_t)host_size);
			memcpy(reply + 84, replica.host, host_size);
		}
	}
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t agent_data_fault(struct vine_datavine_rpc_server *server,
		struct rpc_connection *connection, const unsigned char *payload,
		size_t payload_size)
{
	uint32_t count = 0;
	if (!agent_header(connection, payload, payload_size,
			VINE_DATAVINE_AGENT_FAULT_RECORD, &count))
		return VINE_DATAVINE_RPC_INVALID;
	uint64_t sequence = vine_datavine_get_u64(payload + 24);
	if (!sequence)
		return VINE_DATAVINE_RPC_INVALID;
	if (sequence <= connection->agent_sequence)
		return VINE_DATAVINE_RPC_OK;
	for (uint32_t index = 0; index < count; index++) {
		const unsigned char *record = payload +
			VINE_DATAVINE_AGENT_BATCH_HEADER +
			(size_t)index * VINE_DATAVINE_AGENT_FAULT_RECORD;
		uint64_t data_id = vine_datavine_get_u64(record);
		uint32_t generation = vine_datavine_get_u32(record + 8);
		uint32_t flags = vine_datavine_get_u32(record + 12);
		uint64_t object_token = vine_datavine_get_u64(record + 16);
		uint32_t remote_worker_slot = vine_datavine_get_u32(record + 24);
		uint64_t remote_session_epoch = vine_datavine_get_u64(record + 32);
		if (!data_id || !generation || !object_token ||
				flags > VINE_DATAVINE_AGENT_FAULT_REMOTE ||
				(flags == VINE_DATAVINE_AGENT_FAULT_REMOTE &&
				 (!remote_worker_slot || !remote_session_epoch)))
			return VINE_DATAVINE_RPC_REJECTED;
		if (flags == VINE_DATAVINE_AGENT_FAULT_REMOTE)
			vine_datavine_data_controller_agent_fault(
					server->data_controller,
					connection->agent_workflow_slot, data_id, generation,
					remote_worker_slot, remote_session_epoch, object_token);
		else
			vine_datavine_data_controller_agent_fault(
					server->data_controller,
					connection->agent_workflow_slot, data_id, generation,
					connection->agent_worker_slot,
					connection->agent_session_epoch, object_token);
		/* Exact faults are idempotent. The owning session may already have been
		 * invalidated before a failed peer transfer reports the same replica. */
	}
	connection->agent_sequence = sequence;
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t agent_heartbeat(struct vine_datavine_rpc_server *server,
		struct rpc_connection *connection, const unsigned char *payload,
		size_t payload_size, unsigned char **result, size_t *result_size)
{
	if (payload_size != 8)
		return VINE_DATAVINE_RPC_INVALID;
	struct vine_datavine_agent_release releases[VINE_DATAVINE_AGENT_MAX_BATCH];
	size_t count = 0;
	if (!vine_datavine_data_controller_agent_take_releases(
			server->data_controller, connection->agent_workflow_slot,
			connection->agent_worker_slot, connection->agent_session_epoch,
			vine_datavine_get_u64(payload), releases,
			VINE_DATAVINE_AGENT_MAX_BATCH, &count))
		return VINE_DATAVINE_RPC_REJECTED;
	*result_size = 16 + count * VINE_DATAVINE_AGENT_RELEASE_RECORD;
	*result = calloc(1, *result_size);
	if (!*result)
		return VINE_DATAVINE_RPC_INTERNAL;
	if (count)
		vine_datavine_put_u64(*result, releases[count - 1].sequence);
	vine_datavine_put_u32(*result + 8, (uint32_t)count);
	for (size_t index = 0; index < count; index++) {
		unsigned char *record = *result + 16 +
				index * VINE_DATAVINE_AGENT_RELEASE_RECORD;
		vine_datavine_put_u64(record, releases[index].data_id);
		vine_datavine_put_u32(record + 8, releases[index].generation);
		vine_datavine_put_u32(record + 12, 0);
		vine_datavine_put_u64(record + 16, releases[index].object_token);
	}
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t agent_persisted(struct vine_datavine_rpc_server *server,
		struct rpc_connection *connection, const unsigned char *payload,
		size_t payload_size)
{
	if (payload_size != 56 || !vine_datavine_get_u64(payload) ||
			!vine_datavine_get_u32(payload + 8) ||
			vine_datavine_get_u32(payload + 12))
		return VINE_DATAVINE_RPC_INVALID;
	return vine_datavine_data_controller_agent_persisted(
			server->data_controller, connection->agent_workflow_slot,
			vine_datavine_get_u64(payload), vine_datavine_get_u32(payload + 8),
			vine_datavine_get_u64(payload + 16), payload + 24)
			? VINE_DATAVINE_RPC_OK : VINE_DATAVINE_RPC_REJECTED;
}

static int epoll_update(int epoll_fd, struct rpc_connection *connection, uint32_t events)
{
	struct epoll_event event = {.events = events, .data.ptr = connection};
	return epoll_ctl(epoll_fd, EPOLL_CTL_MOD, connection->fd, &event) == 0;
}

static int response_create(struct rpc_connection *connection, uint16_t opcode,
		uint32_t status, uint64_t request_id, const unsigned char *payload, size_t payload_size)
{
	if (payload_size > UINT32_MAX) {
		return 0;
	}
	connection->response_size = VINE_DATAVINE_RPC_RESPONSE_HEADER + payload_size;
	connection->response = malloc(connection->response_size);
	if (!connection->response) {
		return 0;
	}
	unsigned char *header = connection->response;
	vine_datavine_put_u32(header, VINE_DATAVINE_RPC_MAGIC);
	header[4] = 0;
	header[5] = VINE_DATAVINE_RPC_VERSION;
	header[6] = (unsigned char)(opcode >> 8);
	header[7] = (unsigned char)opcode;
	vine_datavine_put_u32(header + 8, status);
	vine_datavine_put_u32(header + 12, (uint32_t)payload_size);
	vine_datavine_put_u64(header + 16, request_id);
	if (payload_size) {
		memcpy(header + VINE_DATAVINE_RPC_RESPONSE_HEADER, payload, payload_size);
	}
	return 1;
}

static void request_clear(struct rpc_connection *connection)
{
	free(connection->payload);
	connection->payload = 0;
	connection->header_used = 0;
	connection->payload_size = 0;
	connection->payload_used = 0;
}

static uint32_t workflow_status(const struct vine_datavine_workflow_error *error)
{
	if (!error)
		return VINE_DATAVINE_RPC_INTERNAL;
	if (error->code == VINE_DATAVINE_WORKFLOW_REFERENCE)
		return VINE_DATAVINE_RPC_NOT_FOUND;
	if (error->code == VINE_DATAVINE_WORKFLOW_DUPLICATE ||
			error->code == VINE_DATAVINE_WORKFLOW_VALUE ||
			error->code == VINE_DATAVINE_WORKFLOW_LIMIT)
		return VINE_DATAVINE_RPC_REJECTED;
	return VINE_DATAVINE_RPC_INVALID;
}

static int encode_workflow_error(const struct vine_datavine_workflow_error *error,
		unsigned char **result, size_t *result_size)
{
	size_t path_size = strlen(error->path);
	size_t message_size = strlen(error->message);
	if (path_size > UINT16_MAX || message_size > UINT16_MAX)
		return 0;
	*result_size = 8 + path_size + message_size;
	*result = malloc(*result_size);
	if (!*result) {
		*result_size = 0;
		return 0;
	}
	vine_datavine_put_u32(*result, (uint32_t)error->code);
	vine_datavine_put_u16(*result + 4, (uint16_t)path_size);
	vine_datavine_put_u16(*result + 6, (uint16_t)message_size);
	memcpy(*result + 8, error->path, path_size);
	memcpy(*result + 8 + path_size, error->message, message_size);
	return 1;
}

static int encode_workflow_info(const struct vine_datavine_workflow_info *info,
		unsigned char **result, size_t *result_size)
{
	size_t id_size = strlen(info->workflow_id);
	if (id_size > UINT16_MAX)
		return 0;
	*result_size = 104 + id_size;
	*result = calloc(1, *result_size);
	if (!*result) {
		*result_size = 0;
		return 0;
	}
	memcpy(*result, "DWI1", 4);
	vine_datavine_put_u32(*result + 4, (uint32_t)info->state);
	vine_datavine_put_u64(*result + 8, info->generation);
	vine_datavine_put_u64(*result + 16, info->event_id);
	vine_datavine_put_u64(*result + 24, info->summary.tasks);
	vine_datavine_put_u64(*result + 32, info->summary.data);
	vine_datavine_put_u64(*result + 40, info->summary.edges);
	vine_datavine_put_u64(*result + 48, info->summary.requested_outputs);
	vine_datavine_put_u32(*result + 56, (uint32_t)info->summary.streaming);
	vine_datavine_put_u16(*result + 60, (uint16_t)id_size);
	memcpy(*result + 64, info->digest, VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH);
	memcpy(*result + 104, info->workflow_id, id_size);
	return 1;
}

static uint32_t workflow_result(int valid,
		const struct vine_datavine_workflow_info *info,
		const struct vine_datavine_workflow_error *error,
		unsigned char **result, size_t *result_size)
{
	if (valid) {
		if (encode_workflow_info(info, result, result_size))
			return VINE_DATAVINE_RPC_OK;
		return VINE_DATAVINE_RPC_INTERNAL;
	}
	if (!encode_workflow_error(error, result, result_size))
		return VINE_DATAVINE_RPC_INTERNAL;
	return workflow_status(error);
}

static uint32_t workflow_submit(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	if (!server->workflow_store || !size)
		return VINE_DATAVINE_RPC_INVALID;
	struct vine_datavine_workflow_info info;
	struct vine_datavine_workflow_error error;
	int valid = vine_datavine_workflow_store_submit(server->workflow_store,
			(const char *)payload,
			size,
			&info,
			&error);
	return workflow_result(valid, &info, &error, result, result_size);
}

static uint32_t workflow_append(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	uint16_t id_size = decode_workflow_id(payload, size, 12, 1, workflow_id);
	if (!server->workflow_store || !id_size)
		return VINE_DATAVINE_RPC_INVALID;
	uint64_t generation = vine_datavine_get_u64(payload + 4);
	struct vine_datavine_workflow_info info;
	struct vine_datavine_workflow_error error;
	const char *document = (const char *)payload + 12 + id_size;
	size_t document_size = size - 12 - id_size;
	int valid = vine_datavine_workflow_store_append_delta(
			server->workflow_store, workflow_id, generation, document, document_size, &info, &error);
	return workflow_result(valid, &info, &error, result, result_size);
}

static uint32_t workflow_transition(struct vine_datavine_rpc_server *server,
		uint16_t opcode, const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	size_t header_size = opcode == VINE_DATAVINE_RPC_WORKFLOW_SEAL ? 12 : 2;
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	if (!server->workflow_store ||
			!decode_workflow_id(payload, size, header_size, 0, workflow_id))
		return VINE_DATAVINE_RPC_INVALID;
	struct vine_datavine_workflow_info info;
	struct vine_datavine_workflow_error error;
	int valid;
	if (opcode == VINE_DATAVINE_RPC_WORKFLOW_SEAL) {
		valid = vine_datavine_workflow_store_seal(server->workflow_store,
				workflow_id,
				vine_datavine_get_u64(payload + 4),
				&info,
				&error);
	} else {
		valid = vine_datavine_workflow_store_cancel(server->workflow_store,
				workflow_id,
				&info,
				&error);
	}
	return workflow_result(valid, &info, &error, result, result_size);
}

static uint32_t workflow_describe(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	if (!server->workflow_store ||
			!decode_workflow_id(payload, size, 2, 0, workflow_id))
		return VINE_DATAVINE_RPC_INVALID;
	struct vine_datavine_workflow_info info;
	if (!vine_datavine_workflow_store_describe(server->workflow_store,
				workflow_id,
				&info))
		return VINE_DATAVINE_RPC_NOT_FOUND;
	if (encode_workflow_info(&info, result, result_size))
		return VINE_DATAVINE_RPC_OK;
	return VINE_DATAVINE_RPC_INTERNAL;
}

static uint32_t workflow_frontier(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	if (!server->workflow_store ||
			!decode_workflow_id(payload, size, 2, 0, workflow_id))
		return VINE_DATAVINE_RPC_INVALID;
	uint64_t maximum_task_id, maximum_data_id, maximum_tasks, maximum_edges;
	if (!vine_datavine_workflow_store_frontier(server->workflow_store,
				workflow_id,
				&maximum_task_id,
				&maximum_data_id,
				&maximum_tasks,
				&maximum_edges))
		return VINE_DATAVINE_RPC_NOT_FOUND;
	*result = malloc(32);
	if (!*result)
		return VINE_DATAVINE_RPC_INTERNAL;
	vine_datavine_put_u64(*result, maximum_task_id);
	vine_datavine_put_u64(*result + 8, maximum_data_id);
	vine_datavine_put_u64(*result + 16, maximum_tasks);
	vine_datavine_put_u64(*result + 24, maximum_edges);
	*result_size = 32;
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t workflow_watch(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	if (!server->workflow_store ||
			!decode_workflow_id(payload, size, 12, 0, workflow_id))
		return VINE_DATAVINE_RPC_INVALID;
	uint16_t capacity = vine_datavine_get_u16(payload + 2);
	if (!capacity || capacity > 1024)
		return VINE_DATAVINE_RPC_INVALID;
	struct vine_datavine_workflow_event *events = calloc(capacity, sizeof(*events));
	if (!events)
		return VINE_DATAVINE_RPC_INTERNAL;
	size_t count = vine_datavine_workflow_store_watch(server->workflow_store,
			workflow_id,
			vine_datavine_get_u64(payload + 4),
			events,
			capacity);
	*result_size = 4 + count * 76;
	*result = calloc(1, *result_size);
	if (!*result) {
		free(events);
		*result_size = 0;
		return VINE_DATAVINE_RPC_INTERNAL;
	}
	vine_datavine_put_u32(*result, (uint32_t)count);
	for (size_t i = 0; i < count; i++) {
		unsigned char *record = *result + 4 + i * 76;
		vine_datavine_put_u32(record, (uint32_t)events[i].type);
		vine_datavine_put_u64(record + 4, events[i].event_id);
		vine_datavine_put_u64(record + 12, events[i].generation);
		memcpy(record + 20, events[i].digest, VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH);
		vine_datavine_put_u64(record + 60, (uint64_t)events[i].task_id);
		vine_datavine_put_u32(record + 68, events[i].attempt);
		vine_datavine_put_u32(record + 72, (uint32_t)events[i].result);
	}
	free(events);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t workflow_fetch_result(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	if (!server->workflow_store ||
			!decode_workflow_id(payload, size, 12, 0, workflow_id))
		return VINE_DATAVINE_RPC_INVALID;
	char *data = 0;
	if (!vine_datavine_data_controller_fetch_result(server->data_controller,
				workflow_id,
				vine_datavine_get_u64(payload + 4),
				&data,
				result_size) &&
			!vine_datavine_workflow_store_legacy_fetch_result(server->workflow_store,
					workflow_id,
					vine_datavine_get_u64(payload + 4),
					&data,
					result_size))
		return VINE_DATAVINE_RPC_NOT_FOUND;
	*result = (unsigned char *)data;
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t workflow_result_info(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	if (!server->workflow_store ||
			!decode_workflow_id(payload, size, 12, 0, workflow_id))
		return VINE_DATAVINE_RPC_INVALID;
	struct vine_datavine_workflow_result_info info;
	if (!vine_datavine_data_controller_result_info(server->data_controller,
				workflow_id,
				vine_datavine_get_u64(payload + 4),
				&info) &&
			!vine_datavine_workflow_store_legacy_result_info(server->workflow_store,
					workflow_id,
					vine_datavine_get_u64(payload + 4),
					&info))
		return VINE_DATAVINE_RPC_NOT_FOUND;
	struct jx *document = jx_objectv(
			"workflow_id", jx_string(workflow_id), "data_id", jx_integer((jx_int_t)info.data_id), "size", jx_integer((jx_int_t)info.size), "sha256", jx_string(info.sha256), "attempt", jx_integer(info.attempt), "producer_task_id", jx_integer(info.producer_task_id), "producer_output_index", jx_integer(info.producer_output_index), "requested", jx_boolean(info.requested), "codec", jx_objectv("name", jx_string(info.codec_name), "version", jx_string(info.codec_version), NULL), NULL);
	char *encoded = document ? jx_print_string(document) : 0;
	jx_delete(document);
	if (!encoded)
		return VINE_DATAVINE_RPC_INTERNAL;
	*result = (unsigned char *)encoded;
	*result_size = strlen(encoded);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t workflow_result_path(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	char path[PATH_MAX];
	if (!server->data_controller ||
			!decode_workflow_id(payload, size, 12, 0, workflow_id) ||
			!vine_datavine_data_controller_result_path(server->data_controller,
					workflow_id,
					vine_datavine_get_u64(payload + 4),
					path,
					sizeof(path)))
		return VINE_DATAVINE_RPC_NOT_FOUND;
	*result_size = strlen(path);
	*result = malloc(*result_size);
	if (!*result)
		return VINE_DATAVINE_RPC_INTERNAL;
	memcpy(*result, path, *result_size);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t workflow_result_descriptors(
		struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	uint16_t id_size = decode_workflow_id(payload, size, 8, 1, workflow_id);
	uint32_t count = size >= 8 ? vine_datavine_get_u32(payload + 4) : 0;
	if (!server->data_controller || !id_size || !count ||
			count > (VINE_DATAVINE_RPC_MAX_PAYLOAD - 8U - id_size) / 8U ||
			size != 8U + id_size + (size_t)count * 8U)
		return VINE_DATAVINE_RPC_INVALID;
	uint64_t *data_ids = calloc(count, sizeof(*data_ids));
	struct vine_datavine_workflow_result_info *infos =
			calloc(count, sizeof(*infos));
	char **paths = calloc(count, sizeof(*paths));
	if (!data_ids || !infos || !paths) {
		free(paths);
		free(infos);
		free(data_ids);
		return VINE_DATAVINE_RPC_INTERNAL;
	}
	for (uint32_t index = 0; index < count; index++)
		data_ids[index] = vine_datavine_get_u64(
				payload + 8 + id_size + (size_t)index * 8);
	int found = vine_datavine_data_controller_result_descriptors(
			server->data_controller, workflow_id, data_ids, count, infos, paths);
	uint32_t status = found ? VINE_DATAVINE_RPC_OK : VINE_DATAVINE_RPC_NOT_FOUND;
	size_t encoded_size = 8;
	for (uint32_t index = 0; status == VINE_DATAVINE_RPC_OK && index < count;
			index++) {
		size_t path_size = strlen(paths[index]);
		if (!path_size || path_size > UINT16_MAX ||
				encoded_size > VINE_DATAVINE_RPC_MAX_PAYLOAD - 88U - path_size)
			status = VINE_DATAVINE_RPC_INTERNAL;
		else
			encoded_size += 88U + path_size;
	}
	if (status == VINE_DATAVINE_RPC_OK) {
		*result = calloc(1, encoded_size);
		if (!*result)
			status = VINE_DATAVINE_RPC_INTERNAL;
	}
	if (status == VINE_DATAVINE_RPC_OK) {
		memcpy(*result, "DVR1", 4);
		vine_datavine_put_u32(*result + 4, count);
		size_t offset = 8;
		for (uint32_t index = 0; index < count; index++) {
			size_t path_size = strlen(paths[index]);
			vine_datavine_put_u64(*result + offset, infos[index].data_id);
			vine_datavine_put_u64(*result + offset + 8, infos[index].size);
			vine_datavine_put_u32(*result + offset + 16, infos[index].attempt);
			vine_datavine_put_u16(*result + offset + 20,
					(uint16_t)path_size);
			memcpy(*result + offset + 24, infos[index].sha256, 64);
			memcpy(*result + offset + 88, paths[index], path_size);
			offset += 88 + path_size;
		}
		*result_size = encoded_size;
	}
	for (uint32_t index = 0; index < count; index++)
		free(paths[index]);
	free(paths);
	free(infos);
	free(data_ids);
	return status;
}

static uint32_t workflow_capabilities(
		struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t payload_size,
		unsigned char **result, size_t *result_size)
{
	const char *object_root = vine_datavine_data_controller_object_root(
			server->data_controller);
	if (payload_size || !object_root)
		return VINE_DATAVINE_RPC_INVALID;
	struct jx *document = jx_objectv(
			"schema_versions", jx_arrayv(jx_string(VINE_DATAVINE_WORKFLOW_SCHEMA_NAME), jx_string(VINE_DATAVINE_WORKFLOW_DELTA_SCHEMA_NAME), NULL), "executor_kinds", jx_arrayv(jx_string("command"), jx_string("python"), jx_string("taskvine"), NULL), "digest", jx_string("sha1"), "append", jx_string("delta-cas-v1"), "results", jx_string("durable-bytes"), "frontier", jx_boolean(1), "wait_terminal", jx_boolean(1), "result_identity", jx_string("sha256+attempt+producer+codec"), "selective_results", jx_boolean(1), "object_store", jx_string("sharedfs-single-file-sha256-v1"), "object_max_bytes", jx_integer(67108800), "object_root", jx_string(object_root), "physical_submission_window", jx_integer(VINE_DATAVINE_WORKFLOW_SUBMISSION_WINDOW), "physical_recovery_reserve", jx_integer(VINE_DATAVINE_WORKFLOW_RECOVERY_RESERVE), NULL);
	char *encoded = document ? jx_print_string(document) : 0;
	jx_delete(document);
	if (!encoded)
		return VINE_DATAVINE_RPC_INTERNAL;
	*result = (unsigned char *)encoded;
	*result_size = strlen(encoded);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t object_put(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	if (!server->data_controller || size < 64)
		return VINE_DATAVINE_RPC_INVALID;
	char digest[65];
	memcpy(digest, payload, 64);
	digest[64] = 0;
	int deduplicated = 0;
	if (!vine_datavine_data_controller_put_object(server->data_controller,
				digest,
				payload + 64,
				size - 64,
				&deduplicated))
		return VINE_DATAVINE_RPC_REJECTED;
	*result = malloc(8);
	if (!*result)
		return VINE_DATAVINE_RPC_INTERNAL;
	vine_datavine_put_u32(*result, deduplicated ? 1U : 0U);
	vine_datavine_put_u32(*result + 4, (uint32_t)(size - 64));
	*result_size = 8;
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t object_get(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	if (!server->data_controller || size != 64)
		return VINE_DATAVINE_RPC_INVALID;
	char digest[65];
	memcpy(digest, payload, 64);
	digest[64] = 0;
	if (!vine_datavine_data_controller_get_object(server->data_controller,
				digest,
				result,
				result_size))
		return VINE_DATAVINE_RPC_NOT_FOUND;
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t object_path(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	if (!server->data_controller || size != 64)
		return VINE_DATAVINE_RPC_INVALID;
	char digest[65];
	char path[PATH_MAX];
	memcpy(digest, payload, 64);
	digest[64] = 0;
	if (!vine_datavine_data_controller_object_path(
				server->data_controller, digest, path, sizeof(path)))
		return VINE_DATAVINE_RPC_REJECTED;
	*result_size = strlen(path);
	*result = malloc(*result_size);
	if (!*result)
		return VINE_DATAVINE_RPC_INTERNAL;
	memcpy(*result, path, *result_size);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t result_persisted(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size)
{
	if (!server->data_controller || size < 42)
		return VINE_DATAVINE_RPC_INVALID;
	size_t path_size = vine_datavine_get_u16(payload);
	if (!path_size || size != 42 + path_size || path_size >= PATH_MAX)
		return VINE_DATAVINE_RPC_INVALID;
	char path[PATH_MAX];
	char digest[65];
	memcpy(path, payload + 42, path_size);
	path[path_size] = 0;
	for (size_t index = 0; index < 32; index++)
		snprintf(digest + index * 2, 3, "%02x", payload[10 + index]);
	return vine_datavine_data_controller_result_persisted(
				   server->data_controller, path, vine_datavine_get_u64(payload + 2), digest)
				   ? VINE_DATAVINE_RPC_OK
				   : VINE_DATAVINE_RPC_REJECTED;
}

static uint32_t dispatch_request(struct vine_datavine_rpc_server *server,
		uint16_t opcode, const unsigned char *payload, size_t payload_size,
		unsigned char **result, size_t *result_size)
{
	uint32_t status = VINE_DATAVINE_RPC_OK;
	if (opcode == VINE_DATAVINE_RPC_WORKFLOW_SUBMIT) {
		status = workflow_submit(server, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_WORKFLOW_APPEND) {
		status = workflow_append(server, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_WORKFLOW_SEAL ||
			opcode == VINE_DATAVINE_RPC_WORKFLOW_CANCEL) {
		status = workflow_transition(server, opcode, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_WORKFLOW_DESCRIBE) {
		status = workflow_describe(server, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_WORKFLOW_FRONTIER) {
		status = workflow_frontier(server, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_WORKFLOW_WATCH) {
		status = workflow_watch(server, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_WORKFLOW_FETCH_RESULT) {
		status = workflow_fetch_result(server, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_WORKFLOW_RESULT_INFO) {
		status = workflow_result_info(server, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_WORKFLOW_RESULT_PATH) {
		status = workflow_result_path(server, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_WORKFLOW_RESULT_DESCRIPTORS) {
		status = workflow_result_descriptors(server, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_OBJECT_PUT) {
		status = object_put(server, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_OBJECT_GET) {
		status = object_get(server, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_OBJECT_PATH) {
		status = object_path(server, payload, payload_size, result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_RESULT_PERSISTED) {
		status = result_persisted(server, payload, payload_size);
	} else if (opcode == VINE_DATAVINE_RPC_WORKFLOW_CAPABILITIES) {
		status = workflow_capabilities(server, payload, payload_size, result, result_size);
	} else {
		status = VINE_DATAVINE_RPC_INVALID;
	}
	return status;
}

static int process_request(struct vine_datavine_rpc_server *server, struct rpc_connection *connection)
{
	const unsigned char *header = connection->header;
	uint16_t opcode = (uint16_t)((header[6] << 8) | header[7]);
	uint64_t request_id = vine_datavine_get_u64(header + 12);
	uint32_t status;
	unsigned char *result = 0;
	size_t result_size = 0;
	if (!connection->authorized) {
		char object_digest[65];
		if (opcode == VINE_DATAVINE_RPC_AGENT_HELLO) {
			status = agent_hello(server, connection, connection->payload,
					connection->payload_size, &result, &result_size);
			if (status == VINE_DATAVINE_RPC_OK)
				connection->authorized = 1;
		} else if (opcode == VINE_DATAVINE_RPC_OBJECT_GET &&
				validate_object_ticket(server, connection->payload, connection->payload_size, object_digest)) {
			status = object_get(server, (const unsigned char *)object_digest, 64, &result, &result_size);
		} else if (opcode != VINE_DATAVINE_RPC_AUTH) {
			status = VINE_DATAVINE_RPC_UNAUTHORIZED;
		} else if (connection->payload_size == server->token_length &&
				!CRYPTO_memcmp(connection->payload, server->token, server->token_length)) {
			connection->authorized = 1;
			status = VINE_DATAVINE_RPC_OK;
		} else if (authorize_object(server, connection)) {
			connection->authorized = 1;
			status = VINE_DATAVINE_RPC_OK;
		} else {
			status = VINE_DATAVINE_RPC_UNAUTHORIZED;
		}
	} else if (connection->object_digest[0] &&
			(opcode != VINE_DATAVINE_RPC_OBJECT_GET ||
					connection->payload_size != 64 ||
					CRYPTO_memcmp(connection->payload,
							connection->object_digest,
							64))) {
		status = VINE_DATAVINE_RPC_UNAUTHORIZED;
	} else if (opcode == VINE_DATAVINE_RPC_AGENT_HELLO) {
		status = agent_hello(server, connection, connection->payload,
				connection->payload_size, &result, &result_size);
	} else if (connection->agent &&
			opcode == VINE_DATAVINE_RPC_AGENT_DATA_READY) {
		status = agent_data_ready(server, connection, connection->payload,
				connection->payload_size);
	} else if (connection->agent &&
			opcode == VINE_DATAVINE_RPC_AGENT_RESOLVE) {
		status = agent_resolve(server, connection, connection->payload,
				connection->payload_size, &result, &result_size);
	} else if (connection->agent &&
			opcode == VINE_DATAVINE_RPC_AGENT_DATA_FAULT) {
		status = agent_data_fault(server, connection, connection->payload,
				connection->payload_size);
	} else if (connection->agent &&
			opcode == VINE_DATAVINE_RPC_AGENT_HEARTBEAT &&
			connection->payload_size == 8) {
		status = agent_heartbeat(server, connection, connection->payload,
				connection->payload_size, &result, &result_size);
	} else if (connection->agent &&
			opcode == VINE_DATAVINE_RPC_AGENT_PERSISTED) {
		status = agent_persisted(server, connection, connection->payload,
				connection->payload_size);
	} else if (connection->agent) {
		status = VINE_DATAVINE_RPC_INVALID;
	} else if (opcode >= VINE_DATAVINE_RPC_AGENT_HELLO &&
			opcode <= VINE_DATAVINE_RPC_AGENT_PERSISTED) {
		status = VINE_DATAVINE_RPC_REJECTED;
	} else if (opcode == VINE_DATAVINE_RPC_WORKFLOW_WAIT_TERMINAL) {
		char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
		struct vine_datavine_workflow_info info;
		if (!decode_workflow_id(connection->payload, connection->payload_size, 2, 0, workflow_id)) {
			status = VINE_DATAVINE_RPC_INVALID;
		} else if (!vine_datavine_workflow_store_describe(
					   server->workflow_store, workflow_id, &info)) {
			status = VINE_DATAVINE_RPC_NOT_FOUND;
		} else if (info.state != VINE_DATAVINE_WORKFLOW_COMPLETED &&
				info.state != VINE_DATAVINE_WORKFLOW_FAILED &&
				info.state != VINE_DATAVINE_WORKFLOW_CANCELLED) {
			connection->waiting_terminal = 1;
			connection->waiting_opcode = opcode;
			connection->waiting_request_id = request_id;
			snprintf(connection->waiting_workflow_id,
					sizeof(connection->waiting_workflow_id),
					"%s",
					workflow_id);
			return 2;
		} else if (encode_workflow_info(&info, &result, &result_size)) {
			status = VINE_DATAVINE_RPC_OK;
		} else {
			status = VINE_DATAVINE_RPC_INTERNAL;
		}
	} else {
		status = dispatch_request(server, opcode, connection->payload, connection->payload_size, &result, &result_size);
	}
	int created = response_create(connection, opcode, status, request_id, result, result_size);
	free(result);
	return created;
}

static int connection_read(struct rpc_thread *thread, struct rpc_connection *connection)
{
	if (connection->waiting_terminal)
		return 0;
	while (connection->header_used < sizeof(connection->header)) {
		ssize_t count = recv(connection->fd, connection->header + connection->header_used, sizeof(connection->header) - connection->header_used, 0);
		if (count > 0) {
			connection->last_activity = time(0);
			connection->header_used += (size_t)count;
			continue;
		}
		return count == 0 ? 0 : errno == EAGAIN || errno == EWOULDBLOCK;
	}
	if (!connection->payload && !connection->payload_size) {
		if (vine_datavine_get_u32(connection->header) != VINE_DATAVINE_RPC_MAGIC || connection->header[4] != 0 || connection->header[5] != VINE_DATAVINE_RPC_VERSION) {
			return 0;
		}
		connection->payload_size = vine_datavine_get_u32(connection->header + 8);
		if (connection->payload_size > VINE_DATAVINE_RPC_MAX_PAYLOAD) {
			return 0;
		}
		if (connection->payload_size) {
			connection->payload = malloc(connection->payload_size);
			if (!connection->payload) {
				return 0;
			}
		}
	}
	while (connection->payload_used < connection->payload_size) {
		ssize_t count = recv(connection->fd, connection->payload + connection->payload_used, connection->payload_size - connection->payload_used, 0);
		if (count > 0) {
			connection->last_activity = time(0);
			connection->payload_used += (size_t)count;
			continue;
		}
		return count == 0 ? 0 : errno == EAGAIN || errno == EWOULDBLOCK;
	}
	int processed = process_request(thread->server, connection);
	if (!processed) {
		return 0;
	}
	if (processed == 2) {
		request_clear(connection);
		return epoll_update(thread->epoll_fd, connection, EPOLLIN | EPOLLRDHUP);
	}
	return epoll_update(thread->epoll_fd, connection, EPOLLOUT | EPOLLRDHUP);
}

static int connection_write(struct rpc_thread *thread, struct rpc_connection *connection)
{
	while (connection->response_used < connection->response_size) {
		ssize_t count = send(connection->fd, connection->response + connection->response_used, connection->response_size - connection->response_used, MSG_NOSIGNAL);
		if (count > 0) {
			connection->last_activity = time(0);
			connection->response_used += (size_t)count;
			continue;
		}
		if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			return 1;
		}
		return 0;
	}
	request_clear(connection);
	free(connection->response);
	connection->response = 0;
	connection->response_size = 0;
	connection->response_used = 0;
	return epoll_update(thread->epoll_fd, connection, EPOLLIN | EPOLLRDHUP);
}

static int wake_terminal_waiter(
		struct rpc_thread *thread, struct rpc_connection *connection)
{
	struct vine_datavine_workflow_info info;
	if (!vine_datavine_workflow_store_describe(thread->server->workflow_store,
				connection->waiting_workflow_id,
				&info)) {
		connection->waiting_terminal = 0;
		connection->last_activity = time(0);
		return response_create(connection, connection->waiting_opcode, VINE_DATAVINE_RPC_NOT_FOUND, connection->waiting_request_id, 0, 0) &&
			   epoll_update(thread->epoll_fd, connection, EPOLLOUT | EPOLLRDHUP);
	}
	if (info.state != VINE_DATAVINE_WORKFLOW_COMPLETED &&
			info.state != VINE_DATAVINE_WORKFLOW_FAILED &&
			info.state != VINE_DATAVINE_WORKFLOW_CANCELLED)
		return 1;
	unsigned char *result = 0;
	size_t result_size = 0;
	uint32_t status = encode_workflow_info(&info, &result, &result_size)
					  ? VINE_DATAVINE_RPC_OK
					  : VINE_DATAVINE_RPC_INTERNAL;
	connection->waiting_terminal = 0;
	connection->last_activity = time(0);
	int valid = response_create(connection, connection->waiting_opcode, status, connection->waiting_request_id, result, result_size) &&
			epoll_update(thread->epoll_fd, connection, EPOLLOUT | EPOLLRDHUP);
	free(result);
	return valid;
}

static void accept_connections(struct rpc_thread *thread)
{
	for (;;) {
		int fd = accept(thread->server->listen_fd, 0, 0);
		if (fd < 0) {
			return;
		}
		if (!set_nonblocking(fd)) {
			close(fd);
			continue;
		}
		if (atomic_fetch_add(&thread->server->active_connections, 1) >=
				DATAVINE_RPC_MAX_CONNECTIONS) {
			atomic_fetch_sub(&thread->server->active_connections, 1);
			close(fd);
			continue;
		}
		int enabled = 1;
		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
		setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enabled, sizeof(enabled));
		struct rpc_connection *connection = calloc(1, sizeof(*connection));
		if (!connection) {
			atomic_fetch_sub(&thread->server->active_connections, 1);
			close(fd);
			continue;
		}
		connection->fd = fd;
		connection->last_activity = time(0);
		connection->next = thread->connections;
		thread->connections = connection;
		struct epoll_event event = {.events = EPOLLIN | EPOLLRDHUP, .data.ptr = connection};
		if (epoll_ctl(thread->epoll_fd, EPOLL_CTL_ADD, fd, &event)) {
			connection_delete(thread, connection);
		}
	}
}

static void *rpc_thread_main(void *arg)
{
	struct rpc_thread *thread = arg;
	struct epoll_event events[DATAVINE_RPC_EVENTS];
	while (!atomic_load(&thread->server->stopping)) {
		int count = epoll_wait(thread->epoll_fd, events, DATAVINE_RPC_EVENTS, 10);
		for (int i = 0; i < count; i++) {
			if (!events[i].data.ptr) {
				accept_connections(thread);
				continue;
			}
			struct rpc_connection *connection = events[i].data.ptr;
			int keep = !(events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP));
			if (keep && (events[i].events & EPOLLIN)) {
				keep = connection_read(thread, connection);
			}
			if (keep && (events[i].events & EPOLLOUT)) {
				keep = connection_write(thread, connection);
			}
			if (!keep) {
				epoll_ctl(thread->epoll_fd, EPOLL_CTL_DEL, connection->fd, 0);
				connection_delete(thread, connection);
			}
		}
		time_t now = time(0);
		struct rpc_connection *connection = thread->connections;
		while (connection) {
			struct rpc_connection *next = connection->next;
			if (connection->waiting_terminal &&
					!wake_terminal_waiter(thread, connection)) {
				epoll_ctl(thread->epoll_fd, EPOLL_CTL_DEL, connection->fd, 0);
				connection_delete(thread, connection);
			} else if (!connection->waiting_terminal && !connection->agent &&
					now - connection->last_activity >
							DATAVINE_RPC_IDLE_SECONDS) {
				epoll_ctl(thread->epoll_fd, EPOLL_CTL_DEL, connection->fd, 0);
				connection_delete(thread, connection);
			}
			connection = next;
		}
	}
	while (thread->connections) {
		connection_delete(thread, thread->connections);
	}
	return 0;
}

static int listen_socket(const char *host, int port, int *bound_port)
{
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		return -1;
	}
	int enabled = 1;
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
	struct sockaddr_in address = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
	if (!host || !strcmp(host, "0.0.0.0")) {
		address.sin_addr.s_addr = htonl(INADDR_ANY);
	} else if (inet_pton(AF_INET, host, &address.sin_addr) != 1) {
		close(fd);
		return -1;
	}
	if (bind(fd, (struct sockaddr *)&address, sizeof(address)) || listen(fd, 4096) || !set_nonblocking(fd)) {
		close(fd);
		return -1;
	}
	socklen_t length = sizeof(address);
	if (getsockname(fd, (struct sockaddr *)&address, &length)) {
		close(fd);
		return -1;
	}
	*bound_port = ntohs(address.sin_port);
	return fd;
}

struct vine_datavine_rpc_server *vine_datavine_rpc_server_create(
		const char *host, int port, const char *token, int threads,
		const char *workflow_journal_path)
{
	if (!token || !token[0] || threads < 1 || !workflow_journal_path ||
			!workflow_journal_path[0]) {
		return 0;
	}
	struct vine_datavine_rpc_server *server = calloc(1, sizeof(*server));
	if (!server) {
		return 0;
	}
	server->listen_fd = -1;
	server->token = strdup(token);
	server->token_length = strlen(token);
	server->thread_count = 0;
	server->threads = calloc((size_t)threads, sizeof(*server->threads));
	server->workflow_store = vine_datavine_workflow_store_open(workflow_journal_path);
	server->data_controller = vine_datavine_data_controller_open(
			workflow_journal_path, 4, vine_datavine_workflow_store_journal(server->workflow_store));
	server->listen_fd = listen_socket(host, port, &server->port);
	if (!server->token || !server->threads || !server->workflow_store ||
			!server->data_controller ||
			server->listen_fd < 0) {
		vine_datavine_rpc_server_delete(server);
		return 0;
	}
	for (int i = 0; i < threads; i++) {
		struct rpc_thread *thread = &server->threads[i];
		thread->server = server;
		thread->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
		struct epoll_event event = {.events = EPOLLIN | EPOLLEXCLUSIVE, .data.ptr = 0};
		if (thread->epoll_fd < 0 || epoll_ctl(thread->epoll_fd, EPOLL_CTL_ADD, server->listen_fd, &event) || pthread_create(&thread->id, 0, rpc_thread_main, thread)) {
			if (thread->epoll_fd >= 0) {
				close(thread->epoll_fd);
				thread->epoll_fd = -1;
			}
			vine_datavine_rpc_server_delete(server);
			return 0;
		}
		server->thread_count++;
	}
	return server;
}

void vine_datavine_rpc_server_delete(struct vine_datavine_rpc_server *server)
{
	if (!server) {
		return;
	}
	atomic_store(&server->stopping, 1);
	if (server->listen_fd >= 0) {
		close(server->listen_fd);
	}
	for (int i = 0; i < server->thread_count; i++) {
		if (server->threads[i].id) {
			pthread_join(server->threads[i].id, 0);
		}
		if (server->threads[i].epoll_fd >= 0) {
			close(server->threads[i].epoll_fd);
		}
	}
	vine_datavine_data_controller_close(server->data_controller);
	vine_datavine_workflow_store_close(server->workflow_store);
	free(server->threads);
	free(server->token);
	free(server);
}

int vine_datavine_rpc_server_port(const struct vine_datavine_rpc_server *server)
{
	return server ? server->port : 0;
}
