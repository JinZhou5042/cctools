/*
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_datavine_rpc.h"
#include "vine_datavine_directory.h"
#include "vine_datavine_index.h"
#include "vine_datavine_journal.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#define DATAVINE_RPC_EVENTS 128
#define DATAVINE_RPC_ALLOCATE_SIZE 24U
#define DATAVINE_RPC_PUBLICATION_SIZE 88U
#define DATAVINE_RPC_MUTATION_SHARDS 256

struct vine_datavine_rpc_server;

struct rpc_connection {
	int fd;
	int authorized;
	unsigned char header[VINE_DATAVINE_RPC_REQUEST_HEADER];
	size_t header_used;
	unsigned char *payload;
	size_t payload_size;
	size_t payload_used;
	unsigned char *response;
	size_t response_size;
	size_t response_used;
	struct rpc_connection *next;
};

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
	struct vine_datavine_index *index;
	struct vine_datavine_directory *directory;
	struct vine_datavine_journal *journal;
	pthread_rwlock_t topology_lock;
	int topology_lock_initialized;
	pthread_mutex_t publication_locks[DATAVINE_RPC_MUTATION_SHARDS];
	int publication_locks_initialized;
	atomic_int stopping;
};

void vine_datavine_rpc_server_get_metrics(
		struct vine_datavine_rpc_server *server,
		struct vine_datavine_directory_metrics *result)
{
	if (server && result) {
		vine_datavine_directory_get_metrics(server->directory, result);
	}
}

int vine_datavine_rpc_server_snapshot_replicas(
		struct vine_datavine_rpc_server *server, char kind, int64_t data_id,
		struct vine_datavine_replica_snapshot **result, size_t *count)
{
	return server && vine_datavine_directory_snapshot_replicas(
					 server->directory, kind, data_id, result, count);
}

void vine_datavine_rpc_server_get_journal_metrics(
		struct vine_datavine_rpc_server *server,
		struct vine_datavine_journal_metrics *result)
{
	vine_datavine_journal_get_metrics(server ? server->journal : 0, result);
}

uint32_t vine_datavine_rpc_get_u32(const unsigned char *buffer)
{
	uint32_t value;
	memcpy(&value, buffer, sizeof(value));
	return ntohl(value);
}

uint64_t vine_datavine_rpc_get_u64(const unsigned char *buffer)
{
	return ((uint64_t)vine_datavine_rpc_get_u32(buffer) << 32) | vine_datavine_rpc_get_u32(buffer + 4);
}

void vine_datavine_rpc_put_u32(unsigned char *buffer, uint32_t value)
{
	value = htonl(value);
	memcpy(buffer, &value, sizeof(value));
}

void vine_datavine_rpc_put_u64(unsigned char *buffer, uint64_t value)
{
	vine_datavine_rpc_put_u32(buffer, (uint32_t)(value >> 32));
	vine_datavine_rpc_put_u32(buffer + 4, (uint32_t)value);
}

static int set_nonblocking(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);
	return flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

static uint16_t get_u16(const unsigned char *buffer)
{
	return (uint16_t)((buffer[0] << 8) | buffer[1]);
}

static void put_u16(unsigned char *buffer, uint16_t value)
{
	buffer[0] = (unsigned char)(value >> 8);
	buffer[1] = (unsigned char)value;
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
	close(connection->fd);
	free(connection->payload);
	free(connection->response);
	free(connection);
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
	vine_datavine_rpc_put_u32(header, VINE_DATAVINE_RPC_MAGIC);
	header[4] = 0;
	header[5] = VINE_DATAVINE_RPC_VERSION;
	header[6] = (unsigned char)(opcode >> 8);
	header[7] = (unsigned char)opcode;
	vine_datavine_rpc_put_u32(header + 8, status);
	vine_datavine_rpc_put_u32(header + 12, (uint32_t)payload_size);
	vine_datavine_rpc_put_u64(header + 16, request_id);
	if (payload_size) {
		memcpy(header + VINE_DATAVINE_RPC_RESPONSE_HEADER, payload, payload_size);
	}
	return 1;
}

static uint32_t allocate_batch(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size, unsigned char result[4])
{
	if (size < 4) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	uint32_t count = vine_datavine_rpc_get_u32(payload);
	if (count > (VINE_DATAVINE_RPC_MAX_PAYLOAD - 4) / DATAVINE_RPC_ALLOCATE_SIZE || size != 4 + (size_t)count * DATAVINE_RPC_ALLOCATE_SIZE) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	for (uint32_t i = 0; i < count; i++) {
		const unsigned char *record = payload + 4 + (size_t)i * DATAVINE_RPC_ALLOCATE_SIZE;
		int64_t data_id = (int64_t)vine_datavine_rpc_get_u64(record);
		int64_t task_id = (int64_t)vine_datavine_rpc_get_u64(record + 8);
		int32_t output = (int32_t)vine_datavine_rpc_get_u32(record + 16);
		if (!vine_datavine_index_allocate(server->index, data_id, task_id, output)) {
			vine_datavine_rpc_put_u32(result, i);
			return VINE_DATAVINE_RPC_REJECTED;
		}
	}
	vine_datavine_rpc_put_u32(result, count);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t publish_batch(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size, unsigned char result[4])
{
	if (size < 4) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	uint32_t count = vine_datavine_rpc_get_u32(payload);
	if (count > (VINE_DATAVINE_RPC_MAX_PAYLOAD - 4) / DATAVINE_RPC_PUBLICATION_SIZE || size != 4 + (size_t)count * DATAVINE_RPC_PUBLICATION_SIZE) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	for (uint32_t i = 0; i < count; i++) {
		const unsigned char *record = payload + 4 + (size_t)i * DATAVINE_RPC_PUBLICATION_SIZE;
		int64_t data_id = (int64_t)vine_datavine_rpc_get_u64(record);
		int32_t attempt = (int32_t)vine_datavine_rpc_get_u32(record + 8);
		int64_t bytes = (int64_t)vine_datavine_rpc_get_u64(record + 16);
		char hash[65];
		memcpy(hash, record + 24, 64);
		hash[64] = 0;
		if (!vine_datavine_index_put(server->index, data_id, attempt, hash, bytes)) {
			vine_datavine_rpc_put_u32(result, i);
			return VINE_DATAVINE_RPC_REJECTED;
		}
	}
	vine_datavine_rpc_put_u32(result, count);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t register_edata(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size,
		unsigned char **result, size_t *result_size)
{
	if (size < 4) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	uint32_t count = vine_datavine_rpc_get_u32(payload);
	if (count > (VINE_DATAVINE_RPC_MAX_PAYLOAD - 4) / 8) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	*result_size = 4 + (size_t)count * 8;
	*result = malloc(*result_size);
	if (!*result) {
		*result_size = 0;
		return VINE_DATAVINE_RPC_INTERNAL;
	}
	vine_datavine_rpc_put_u32(*result, count);
	size_t offset = 4;
	for (uint32_t i = 0; i < count; i++) {
		if (size - offset < 144) {
			free(*result);
			*result = 0;
			*result_size = 0;
			return VINE_DATAVINE_RPC_INVALID;
		}
		uint32_t flags = vine_datavine_rpc_get_u32(payload + offset);
		uint32_t metadata_size = vine_datavine_rpc_get_u32(payload + offset + 4);
		uint64_t data_size = vine_datavine_rpc_get_u64(payload + offset + 8);
		size_t inline_size = flags == 1 ? (size_t)data_size : 0;
		if (flags > 1 || data_size > SIZE_MAX || metadata_size > size - offset - 144 || inline_size > size - offset - 144 - metadata_size) {
			free(*result);
			*result = 0;
			*result_size = 0;
			return VINE_DATAVINE_RPC_INVALID;
		}
		const unsigned char *content_hash = payload + offset + 16;
		const unsigned char *serialized_hash = payload + offset + 80;
		char content[65];
		char serialized[65];
		memcpy(content, content_hash, 64);
		memcpy(serialized, serialized_hash, 64);
		content[64] = 0;
		serialized[64] = 0;
		const unsigned char *metadata = payload + offset + 144;
		const unsigned char *data = metadata + metadata_size;
		int64_t data_id = 0;
		if (!vine_datavine_index_register_edata(server->index, content, serialized, metadata, metadata_size, flags ? data : 0, inline_size, data_size, flags, &data_id)) {
			free(*result);
			*result = 0;
			*result_size = 0;
			return VINE_DATAVINE_RPC_REJECTED;
		}
		vine_datavine_rpc_put_u64(*result + 4 + (size_t)i * 8, (uint64_t)data_id);
		offset += 144 + metadata_size + inline_size;
	}
	if (offset != size) {
		free(*result);
		*result = 0;
		*result_size = 0;
		return VINE_DATAVINE_RPC_INVALID;
	}
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t get_edata(struct vine_datavine_rpc_server *server,
		const unsigned char *request, size_t size,
		unsigned char **result, size_t *result_size)
{
	if (size != 8 && size != 12) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	uint32_t options = size == 12 ? vine_datavine_rpc_get_u32(request + 8) : 0;
	if (options > 3) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	int allow_shared = options & 1;
	int include_payload = !(options & 2);
	char content_hash[65];
	char serialized_hash[65];
	unsigned char *payload = 0;
	size_t payload_size = 0;
	uint64_t serialized_size = 0;
	int cache_globally = 0;
	int64_t data_id = (int64_t)vine_datavine_rpc_get_u64(request);
	int found = vine_datavine_index_get_edata(
			server->index, data_id, content_hash, serialized_hash, &payload, &payload_size, &serialized_size, &cache_globally, allow_shared, include_payload);
	if (!found) {
		return VINE_DATAVINE_RPC_NOT_FOUND;
	}
	if (payload_size > VINE_DATAVINE_RPC_MAX_PAYLOAD - 148) {
		free(payload);
		return VINE_DATAVINE_RPC_INTERNAL;
	}
	*result_size = 148 + payload_size;
	*result = malloc(*result_size);
	if (!*result) {
		free(payload);
		*result_size = 0;
		return VINE_DATAVINE_RPC_INTERNAL;
	}
	vine_datavine_rpc_put_u64(*result, serialized_size);
	vine_datavine_rpc_put_u64(*result + 8, payload_size);
	vine_datavine_rpc_put_u32(*result + 16, cache_globally);
	memcpy(*result + 20, content_hash, 64);
	memcpy(*result + 84, serialized_hash, 64);
	if (payload_size) {
		memcpy(*result + 148, payload, payload_size);
	}
	free(payload);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t mark_edata_shared(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size, unsigned char result[4])
{
	if (size < 4) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	uint32_t count = vine_datavine_rpc_get_u32(payload);
	if (count > (VINE_DATAVINE_RPC_MAX_PAYLOAD - 4) / 8 || size != 4 + (size_t)count * 8) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	for (uint32_t i = 0; i < count; i++) {
		int64_t data_id = (int64_t)vine_datavine_rpc_get_u64(payload + 4 + (size_t)i * 8);
		if (!vine_datavine_index_mark_edata_shared(server->index, data_id)) {
			vine_datavine_rpc_put_u32(result, i);
			return VINE_DATAVINE_RPC_REJECTED;
		}
	}
	vine_datavine_rpc_put_u32(result, count);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t claim_worker(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size, unsigned char result[8])
{
	if (size < 4) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	uint16_t worker_length = get_u16(payload);
	uint16_t endpoint_length = get_u16(payload + 2);
	if (!worker_length || worker_length > VINE_DATAVINE_WORKER_ID_MAX || !endpoint_length || endpoint_length > VINE_DATAVINE_ENDPOINT_MAX || size != 4 + (size_t)worker_length + endpoint_length) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	char worker_id[VINE_DATAVINE_WORKER_ID_MAX + 1];
	char endpoint[VINE_DATAVINE_ENDPOINT_MAX + 1];
	memcpy(worker_id, payload + 4, worker_length);
	worker_id[worker_length] = 0;
	memcpy(endpoint, payload + 4 + worker_length, endpoint_length);
	endpoint[endpoint_length] = 0;
	struct vine_datavine_worker_record worker;
	if (!vine_datavine_directory_claim_worker(server->directory, worker_id, endpoint, &worker)) {
		return VINE_DATAVINE_RPC_REJECTED;
	}
	vine_datavine_rpc_put_u64(result, worker.epoch);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t publish_outputs(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size, unsigned char **result, size_t *result_size)
{
	if (size < 16) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	uint16_t worker_length = get_u16(payload);
	uint16_t endpoint_length = get_u16(payload + 2);
	uint64_t worker_epoch = vine_datavine_rpc_get_u64(payload + 4);
	uint32_t count = vine_datavine_rpc_get_u32(payload + 12);
	if (!worker_length || worker_length > VINE_DATAVINE_WORKER_ID_MAX || !endpoint_length || endpoint_length > VINE_DATAVINE_ENDPOINT_MAX || !worker_epoch || count > (VINE_DATAVINE_RPC_MAX_PAYLOAD - 16 - worker_length - endpoint_length) / DATAVINE_RPC_PUBLICATION_SIZE || size != 16 + (size_t)worker_length + endpoint_length + (size_t)count * DATAVINE_RPC_PUBLICATION_SIZE) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	char worker_id[VINE_DATAVINE_WORKER_ID_MAX + 1];
	char endpoint[VINE_DATAVINE_ENDPOINT_MAX + 1];
	memcpy(worker_id, payload + 16, worker_length);
	worker_id[worker_length] = 0;
	memcpy(endpoint, payload + 16 + worker_length, endpoint_length);
	endpoint[endpoint_length] = 0;
	*result_size = 4 + (size_t)count * 8;
	*result = malloc(*result_size);
	if (!*result) {
		*result_size = 0;
		return VINE_DATAVINE_RPC_INTERNAL;
	}
	vine_datavine_rpc_put_u32(*result, count);
	for (uint32_t i = 0; i < count; i++) {
		const unsigned char *record = payload + 16 + worker_length + endpoint_length + (size_t)i * DATAVINE_RPC_PUBLICATION_SIZE;
		int64_t data_id = (int64_t)vine_datavine_rpc_get_u64(record);
		int32_t attempt = (int32_t)vine_datavine_rpc_get_u32(record + 8);
		int32_t tier = (int32_t)vine_datavine_rpc_get_u32(record + 12);
		int64_t bytes = (int64_t)vine_datavine_rpc_get_u64(record + 16);
		char hash[65];
		memcpy(hash, record + 24, 64);
		hash[64] = 0;
		char replica_id[VINE_DATAVINE_REPLICA_ID_MAX + 1];
		int length = snprintf(replica_id, sizeof(replica_id), "taskvine-%s-i-%lld", worker_id, (long long)data_id);
		struct vine_datavine_replica_record replica;
		if (length < 1 || length > VINE_DATAVINE_REPLICA_ID_MAX || (tier != VINE_DATAVINE_WORKER_DRAM && tier != VINE_DATAVINE_WORKER_DISK) || !vine_datavine_index_put(server->index, data_id, attempt, hash, bytes) || !vine_datavine_directory_publish_replica(server->directory, 'i', data_id, replica_id, attempt, tier, hash, bytes, worker_id, worker_epoch, endpoint, &replica)) {
			vine_datavine_rpc_put_u32(*result, i);
			*result_size = 4;
			return VINE_DATAVINE_RPC_REJECTED;
		}
		vine_datavine_rpc_put_u64(*result + 4 + (size_t)i * 8, replica.generation);
	}
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t disconnect_worker(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size)
{
	if (size < 12) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	uint16_t worker_length = get_u16(payload);
	uint64_t epoch = vine_datavine_rpc_get_u64(payload + 4);
	if (!worker_length || worker_length > VINE_DATAVINE_WORKER_ID_MAX || !epoch || size != 12 + (size_t)worker_length) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	char worker_id[VINE_DATAVINE_WORKER_ID_MAX + 1];
	memcpy(worker_id, payload + 12, worker_length);
	worker_id[worker_length] = 0;
	if (!vine_datavine_directory_disconnect_worker(server->directory, worker_id, epoch)) {
		return VINE_DATAVINE_RPC_REJECTED;
	}
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t report_replica(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size, unsigned char result[8])
{
	if (size < 104) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	char kind = (char)payload[0];
	int32_t tier = payload[1];
	uint16_t worker_length = get_u16(payload + 2);
	uint16_t replica_length = get_u16(payload + 4);
	uint16_t endpoint_length = get_u16(payload + 6);
	uint64_t worker_epoch = vine_datavine_rpc_get_u64(payload + 8);
	int64_t data_id = (int64_t)vine_datavine_rpc_get_u64(payload + 16);
	int32_t attempt = (int32_t)vine_datavine_rpc_get_u32(payload + 24);
	int64_t bytes = (int64_t)vine_datavine_rpc_get_u64(payload + 32);
	if ((kind != 'e' && kind != 'i') || (tier != VINE_DATAVINE_WORKER_DRAM && tier != VINE_DATAVINE_WORKER_DISK) || !worker_length || worker_length > VINE_DATAVINE_WORKER_ID_MAX || !replica_length || replica_length > VINE_DATAVINE_REPLICA_ID_MAX || !endpoint_length || endpoint_length > VINE_DATAVINE_ENDPOINT_MAX || size != 104 + (size_t)worker_length + replica_length + endpoint_length) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	char hash[65];
	char worker_id[VINE_DATAVINE_WORKER_ID_MAX + 1];
	char replica_id[VINE_DATAVINE_REPLICA_ID_MAX + 1];
	char endpoint[VINE_DATAVINE_ENDPOINT_MAX + 1];
	memcpy(hash, payload + 40, 64);
	hash[64] = 0;
	memcpy(worker_id, payload + 104, worker_length);
	worker_id[worker_length] = 0;
	memcpy(replica_id, payload + 104 + worker_length, replica_length);
	replica_id[replica_length] = 0;
	memcpy(endpoint, payload + 104 + worker_length + replica_length, endpoint_length);
	endpoint[endpoint_length] = 0;
	if (kind == 'e') {
		if (!vine_datavine_index_validate_edata(server->index, data_id, hash, bytes)) {
			return VINE_DATAVINE_RPC_REJECTED;
		}
	} else {
		struct vine_datavine_data logical;
		if (!vine_datavine_index_get(server->index, data_id, &logical) || logical.attempt != attempt || logical.size != bytes || strcmp(logical.content_hash, hash)) {
			return VINE_DATAVINE_RPC_REJECTED;
		}
	}
	struct vine_datavine_replica_record replica;
	if (!vine_datavine_directory_publish_replica(server->directory, kind, data_id, replica_id, attempt, tier, hash, bytes, worker_id, worker_epoch, endpoint, &replica)) {
		return VINE_DATAVINE_RPC_REJECTED;
	}
	vine_datavine_rpc_put_u64(result, replica.generation);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t resolve_source(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size, unsigned char **result, size_t *result_size)
{
	if (size < 24) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	char kind = (char)payload[0];
	uint16_t destination_length = get_u16(payload + 2);
	uint16_t transfer_length = get_u16(payload + 4);
	uint16_t excluded_length = get_u16(payload + 6);
	uint64_t destination_epoch = vine_datavine_rpc_get_u64(payload + 8);
	int64_t data_id = (int64_t)vine_datavine_rpc_get_u64(payload + 16);
	if ((kind != 'e' && kind != 'i') || !destination_length || destination_length > VINE_DATAVINE_WORKER_ID_MAX || !transfer_length || transfer_length > VINE_DATAVINE_TRANSFER_ID_MAX || excluded_length > VINE_DATAVINE_WORKER_ID_MAX || size != 24 + (size_t)destination_length + transfer_length + excluded_length) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	const unsigned char *strings = payload + 24;
	char destination[VINE_DATAVINE_WORKER_ID_MAX + 1];
	char transfer[VINE_DATAVINE_TRANSFER_ID_MAX + 1];
	char excluded[VINE_DATAVINE_WORKER_ID_MAX + 1];
	memcpy(destination, strings, destination_length);
	destination[destination_length] = 0;
	memcpy(transfer, strings + destination_length, transfer_length);
	transfer[transfer_length] = 0;
	memcpy(excluded, strings + destination_length + transfer_length, excluded_length);
	excluded[excluded_length] = 0;
	struct vine_datavine_source_record source;
	int resolved = vine_datavine_directory_resolve_source(server->directory, kind, data_id, destination, destination_epoch, transfer, excluded_length ? excluded : 0, &source);
	if (resolved < 0) {
		return VINE_DATAVINE_RPC_REJECTED;
	}
	if (!resolved) {
		return VINE_DATAVINE_RPC_NOT_FOUND;
	}
	uint16_t worker_length = (uint16_t)strlen(source.replica.worker_id);
	uint16_t replica_length = (uint16_t)strlen(source.replica.replica_id);
	uint16_t endpoint_length = (uint16_t)strlen(source.endpoint);
	uint16_t response_transfer_length = (uint16_t)strlen(source.transfer_id);
	*result_size = 112 + (size_t)worker_length + replica_length + endpoint_length + response_transfer_length;
	*result = calloc(1, *result_size);
	if (!*result) {
		*result_size = 0;
		vine_datavine_directory_release_source(server->directory, transfer, 0);
		return VINE_DATAVINE_RPC_INTERNAL;
	}
	vine_datavine_rpc_put_u64(*result, source.replica.generation);
	vine_datavine_rpc_put_u32(*result + 8, (uint32_t)source.replica.attempt);
	vine_datavine_rpc_put_u32(*result + 12, (uint32_t)source.replica.tier);
	vine_datavine_rpc_put_u64(*result + 16, (uint64_t)source.replica.size);
	vine_datavine_rpc_put_u32(*result + 24, source.replica.active_leases);
	vine_datavine_rpc_put_u64(*result + 32, source.replica.worker_epoch);
	put_u16(*result + 40, worker_length);
	put_u16(*result + 42, replica_length);
	put_u16(*result + 44, endpoint_length);
	put_u16(*result + 46, response_transfer_length);
	memcpy(*result + 48, source.replica.content_hash, 64);
	unsigned char *output = *result + 112;
	memcpy(output, source.replica.worker_id, worker_length);
	memcpy(output + worker_length, source.replica.replica_id, replica_length);
	memcpy(output + worker_length + replica_length, source.endpoint, endpoint_length);
	memcpy(output + worker_length + replica_length + endpoint_length,
			source.transfer_id,
			response_transfer_length);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t release_source(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size)
{
	if (size < 8) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	int success = payload[0] != 0;
	uint16_t transfer_length = get_u16(payload + 4);
	if (!transfer_length || transfer_length > VINE_DATAVINE_TRANSFER_ID_MAX || size != 8 + (size_t)transfer_length) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	char transfer[VINE_DATAVINE_TRANSFER_ID_MAX + 1];
	memcpy(transfer, payload + 8, transfer_length);
	transfer[transfer_length] = 0;
	int released = vine_datavine_directory_release_source(
			server->directory, transfer, success);
	if (released < 0) {
		return VINE_DATAVINE_RPC_REJECTED;
	}
	if (!released) {
		return VINE_DATAVINE_RPC_NOT_FOUND;
	}
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t change_replica(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size, int operation)
{
	if (size < 16) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	char kind = (char)payload[0];
	uint16_t replica_length = get_u16(payload + 2);
	int64_t data_id = (int64_t)vine_datavine_rpc_get_u64(payload + 8);
	if (!replica_length || replica_length > VINE_DATAVINE_REPLICA_ID_MAX || size != 16 + (size_t)replica_length) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	char replica[VINE_DATAVINE_REPLICA_ID_MAX + 1];
	memcpy(replica, payload + 16, replica_length);
	replica[replica_length] = 0;
	int changed;
	if (operation == 1) {
		changed = vine_datavine_directory_restore_replica(
				server->directory, kind, data_id, replica);
	} else if (operation == 2) {
		changed = vine_datavine_directory_confirm_replica_pruned(
				server->directory, kind, data_id, replica);
	} else {
		changed = vine_datavine_directory_invalidate_replica(
				server->directory, kind, data_id, replica);
	}
	if (changed < 0) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	return changed ? VINE_DATAVINE_RPC_OK : VINE_DATAVINE_RPC_NOT_FOUND;
}

static uint32_t dispatch_request(struct vine_datavine_rpc_server *server,
		uint16_t opcode, const unsigned char *payload, size_t payload_size,
		unsigned char result[8], unsigned char **dynamic_result,
		size_t *result_size)
{
	uint32_t status = VINE_DATAVINE_RPC_OK;
	if (opcode == VINE_DATAVINE_RPC_PING) {
		status = payload_size ? VINE_DATAVINE_RPC_INVALID : VINE_DATAVINE_RPC_OK;
	} else if (opcode == VINE_DATAVINE_RPC_ALLOCATE_BATCH) {
		status = allocate_batch(server, payload, payload_size, result);
		*result_size = 4;
	} else if (opcode == VINE_DATAVINE_RPC_PUBLISH_BATCH) {
		status = publish_batch(server, payload, payload_size, result);
		*result_size = 4;
	} else if (opcode == VINE_DATAVINE_RPC_CLAIM_WORKER) {
		status = claim_worker(server, payload, payload_size, result);
		*result_size = status == VINE_DATAVINE_RPC_OK ? 8 : 0;
	} else if (opcode == VINE_DATAVINE_RPC_PUBLISH_OUTPUTS) {
		status = publish_outputs(server, payload, payload_size, dynamic_result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_DISCONNECT_WORKER) {
		status = disconnect_worker(server, payload, payload_size);
	} else if (opcode == VINE_DATAVINE_RPC_REPORT_REPLICA) {
		status = report_replica(server, payload, payload_size, result);
		*result_size = status == VINE_DATAVINE_RPC_OK ? 8 : 0;
	} else if (opcode == VINE_DATAVINE_RPC_RESOLVE_SOURCE) {
		status = resolve_source(server, payload, payload_size, dynamic_result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_RELEASE_SOURCE) {
		status = release_source(server, payload, payload_size);
	} else if (opcode == VINE_DATAVINE_RPC_REGISTER_EDATA) {
		status = register_edata(server, payload, payload_size, dynamic_result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_GET_EDATA) {
		status = get_edata(server, payload, payload_size, dynamic_result, result_size);
	} else if (opcode == VINE_DATAVINE_RPC_INVALIDATE_REPLICA) {
		status = change_replica(server, payload, payload_size, 0);
	} else if (opcode == VINE_DATAVINE_RPC_RESTORE_REPLICA) {
		status = change_replica(server, payload, payload_size, 1);
	} else if (opcode == VINE_DATAVINE_RPC_CONFIRM_REPLICA_PRUNED) {
		status = change_replica(server, payload, payload_size, 2);
	} else if (opcode == VINE_DATAVINE_RPC_MARK_EDATA_SHARED) {
		status = mark_edata_shared(server, payload, payload_size, result);
		*result_size = 4;
	} else {
		status = VINE_DATAVINE_RPC_INVALID;
	}
	return status;
}

static int persistent_opcode(uint16_t opcode)
{
	return opcode == VINE_DATAVINE_RPC_ALLOCATE_BATCH || opcode == VINE_DATAVINE_RPC_PUBLISH_BATCH || opcode == VINE_DATAVINE_RPC_CLAIM_WORKER || opcode == VINE_DATAVINE_RPC_PUBLISH_OUTPUTS || opcode == VINE_DATAVINE_RPC_DISCONNECT_WORKER || opcode == VINE_DATAVINE_RPC_REPORT_REPLICA || opcode == VINE_DATAVINE_RPC_REGISTER_EDATA || opcode == VINE_DATAVINE_RPC_INVALIDATE_REPLICA || opcode == VINE_DATAVINE_RPC_RESTORE_REPLICA || opcode == VINE_DATAVINE_RPC_CONFIRM_REPLICA_PRUNED || opcode == VINE_DATAVINE_RPC_MARK_EDATA_SHARED;
}

static int durable_opcode(uint16_t opcode)
{
	return opcode == VINE_DATAVINE_RPC_REGISTER_EDATA;
}

static int replay_request(void *context, uint16_t opcode,
		const unsigned char *payload, size_t payload_size)
{
	struct vine_datavine_rpc_server *server = context;
	unsigned char result[8];
	unsigned char *dynamic_result = 0;
	size_t result_size = 0;
	uint32_t status = persistent_opcode(opcode)
					  ? dispatch_request(server, opcode, payload, payload_size, result, &dynamic_result, &result_size)
					  : VINE_DATAVINE_RPC_INVALID;
	free(dynamic_result);
	return status == VINE_DATAVINE_RPC_OK;
}

static pthread_mutex_t *publication_lock(
		struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size)
{
	uint64_t data_id = 0;
	if (size >= 16) {
		uint16_t worker_length = get_u16(payload);
		uint16_t endpoint_length = get_u16(payload + 2);
		uint32_t count = vine_datavine_rpc_get_u32(payload + 12);
		size_t offset = 16 + (size_t)worker_length + endpoint_length;
		if (count && offset <= size && size - offset >= DATAVINE_RPC_PUBLICATION_SIZE) {
			data_id = vine_datavine_rpc_get_u64(payload + offset);
		}
	}
	data_id *= UINT64_C(11400714819323198485);
	return &server->publication_locks[
		data_id % DATAVINE_RPC_MUTATION_SHARDS];
}

static int process_request(struct vine_datavine_rpc_server *server, struct rpc_connection *connection)
{
	const unsigned char *header = connection->header;
	uint16_t opcode = (uint16_t)((header[6] << 8) | header[7]);
	uint64_t request_id = vine_datavine_rpc_get_u64(header + 12);
	uint32_t status;
	unsigned char result[8];
	unsigned char *dynamic_result = 0;
	size_t result_size = 0;
	if (!connection->authorized) {
		if (opcode != VINE_DATAVINE_RPC_AUTH || connection->payload_size != server->token_length || memcmp(connection->payload, server->token, server->token_length)) {
			status = VINE_DATAVINE_RPC_UNAUTHORIZED;
		} else {
			connection->authorized = 1;
			status = VINE_DATAVINE_RPC_OK;
		}
	} else {
		int persistent = server->journal && persistent_opcode(opcode);
		int publishing = persistent
				&& opcode == VINE_DATAVINE_RPC_PUBLISH_OUTPUTS;
		pthread_mutex_t *shard = 0;
		if (publishing) {
			pthread_rwlock_rdlock(&server->topology_lock);
			shard = publication_lock(
				server, connection->payload, connection->payload_size);
			pthread_mutex_lock(shard);
		} else if (persistent) {
			pthread_rwlock_wrlock(&server->topology_lock);
		}
		status = dispatch_request(server, opcode, connection->payload, connection->payload_size, result, &dynamic_result, &result_size);
		int journaled = 1;
		if (status == VINE_DATAVINE_RPC_OK && persistent) {
			if (durable_opcode(opcode)) {
				journaled = vine_datavine_journal_commit(server->journal, opcode, connection->payload, connection->payload_size);
			} else {
				journaled = vine_datavine_journal_append(server->journal, opcode, connection->payload, connection->payload_size);
			}
		}
		if (status == VINE_DATAVINE_RPC_OK && !journaled) {
			status = VINE_DATAVINE_RPC_INTERNAL;
			result_size = 0;
		}
		if (publishing) {
			pthread_mutex_unlock(shard);
			pthread_rwlock_unlock(&server->topology_lock);
		} else if (persistent) {
			pthread_rwlock_unlock(&server->topology_lock);
		}
	}
	int created = response_create(connection, opcode, status, request_id, dynamic_result ? dynamic_result : result, result_size);
	free(dynamic_result);
	return created;
}

static int connection_read(struct rpc_thread *thread, struct rpc_connection *connection)
{
	while (connection->header_used < sizeof(connection->header)) {
		ssize_t count = recv(connection->fd, connection->header + connection->header_used, sizeof(connection->header) - connection->header_used, 0);
		if (count > 0) {
			connection->header_used += (size_t)count;
			continue;
		}
		return count == 0 ? 0 : errno == EAGAIN || errno == EWOULDBLOCK;
	}
	if (!connection->payload && !connection->payload_size) {
		if (vine_datavine_rpc_get_u32(connection->header) != VINE_DATAVINE_RPC_MAGIC || connection->header[4] != 0 || connection->header[5] != VINE_DATAVINE_RPC_VERSION) {
			return 0;
		}
		connection->payload_size = vine_datavine_rpc_get_u32(connection->header + 8);
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
			connection->payload_used += (size_t)count;
			continue;
		}
		return count == 0 ? 0 : errno == EAGAIN || errno == EWOULDBLOCK;
	}
	if (!process_request(thread->server, connection)) {
		return 0;
	}
	return epoll_update(thread->epoll_fd, connection, EPOLLOUT | EPOLLRDHUP);
}

static int connection_write(struct rpc_thread *thread, struct rpc_connection *connection)
{
	while (connection->response_used < connection->response_size) {
		ssize_t count = send(connection->fd, connection->response + connection->response_used, connection->response_size - connection->response_used, MSG_NOSIGNAL);
		if (count > 0) {
			connection->response_used += (size_t)count;
			continue;
		}
		if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			return 1;
		}
		return 0;
	}
	free(connection->payload);
	free(connection->response);
	connection->payload = 0;
	connection->response = 0;
	connection->header_used = 0;
	connection->payload_size = 0;
	connection->payload_used = 0;
	connection->response_size = 0;
	connection->response_used = 0;
	return epoll_update(thread->epoll_fd, connection, EPOLLIN | EPOLLRDHUP);
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
		int enabled = 1;
		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
		struct rpc_connection *connection = calloc(1, sizeof(*connection));
		if (!connection) {
			close(fd);
			continue;
		}
		connection->fd = fd;
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
		int count = epoll_wait(thread->epoll_fd, events, DATAVINE_RPC_EVENTS, 100);
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
		int64_t maximum_data_id, uint64_t maximum_edata_bytes,
		const char *journal_path)
{
	if (!token || !token[0] || threads < 1 || maximum_data_id < 1 || (uint64_t)maximum_data_id > UINT64_MAX / 4) {
		return 0;
	}
	struct vine_datavine_rpc_server *server = calloc(1, sizeof(*server));
	if (!server) {
		return 0;
	}
	server->listen_fd = -1;
	if (pthread_rwlock_init(&server->topology_lock, 0)) {
		free(server);
		return 0;
	}
	server->topology_lock_initialized = 1;
	for (int i = 0; i < DATAVINE_RPC_MUTATION_SHARDS; i++) {
		if (pthread_mutex_init(&server->publication_locks[i], 0)) {
			vine_datavine_rpc_server_delete(server);
			return 0;
		}
		server->publication_locks_initialized++;
	}
	server->token = strdup(token);
	server->token_length = strlen(token);
	server->thread_count = 0;
	server->threads = calloc((size_t)threads, sizeof(*server->threads));
	server->index = vine_datavine_index_create(maximum_data_id, maximum_edata_bytes, 256);
	server->directory = vine_datavine_directory_create(
			1000000, (uint64_t)maximum_data_id * 4, 1000000, 65536, 256);
	server->journal = journal_path && journal_path[0]
					  ? vine_datavine_journal_open(journal_path)
					  : 0;
	server->listen_fd = listen_socket(host, port, &server->port);
	if (!server->token || !server->threads || !server->index || !server->directory || (journal_path && journal_path[0] && !server->journal) || (server->journal && !vine_datavine_journal_replay(server->journal, replay_request, server)) || server->listen_fd < 0) {
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
	vine_datavine_index_delete(server->index);
	vine_datavine_directory_delete(server->directory);
	vine_datavine_journal_close(server->journal);
	for (int i = 0; i < server->publication_locks_initialized; i++) {
		pthread_mutex_destroy(&server->publication_locks[i]);
	}
	if (server->topology_lock_initialized) {
		pthread_rwlock_destroy(&server->topology_lock);
	}
	free(server->threads);
	free(server->token);
	free(server);
}

int vine_datavine_rpc_server_port(const struct vine_datavine_rpc_server *server)
{
	return server ? server->port : 0;
}
