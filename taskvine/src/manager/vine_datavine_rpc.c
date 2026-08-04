/*
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_datavine_rpc.h"
#include "vine_datavine_directory.h"
#include "vine_datavine_index.h"

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
#define DATAVINE_RPC_MAX_PAYLOAD (16U * 1024U * 1024U)
#define DATAVINE_RPC_ALLOCATE_SIZE 24U
#define DATAVINE_RPC_PUBLICATION_SIZE 88U

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
	if (count > (DATAVINE_RPC_MAX_PAYLOAD - 4) / DATAVINE_RPC_ALLOCATE_SIZE || size != 4 + (size_t)count * DATAVINE_RPC_ALLOCATE_SIZE) {
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
	if (count > (DATAVINE_RPC_MAX_PAYLOAD - 4) / DATAVINE_RPC_PUBLICATION_SIZE || size != 4 + (size_t)count * DATAVINE_RPC_PUBLICATION_SIZE) {
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
		const unsigned char *payload, size_t size, unsigned char result[4])
{
	if (size < 4) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	uint32_t count = vine_datavine_rpc_get_u32(payload);
	size_t offset = 4;
	for (uint32_t i = 0; i < count; i++) {
		if (size - offset < 148) {
			return VINE_DATAVINE_RPC_INVALID;
		}
		int64_t data_id = (int64_t)vine_datavine_rpc_get_u64(payload + offset);
		uint32_t metadata_size = vine_datavine_rpc_get_u32(payload + offset + 8);
		uint64_t data_size = vine_datavine_rpc_get_u64(payload + offset + 12);
		if (data_size > SIZE_MAX || metadata_size > size - offset - 148
				|| data_size > size - offset - 148 - metadata_size) {
			return VINE_DATAVINE_RPC_INVALID;
		}
		const unsigned char *content_hash = payload + offset + 20;
		const unsigned char *serialized_hash = payload + offset + 84;
		char content[65];
		char serialized[65];
		memcpy(content, content_hash, 64);
		memcpy(serialized, serialized_hash, 64);
		content[64] = 0;
		serialized[64] = 0;
		const unsigned char *metadata = payload + offset + 148;
		const unsigned char *data = metadata + metadata_size;
		if (!vine_datavine_index_put_edata(server->index, data_id,
				content, serialized, metadata, metadata_size,
				data, (size_t)data_size)) {
			vine_datavine_rpc_put_u32(result, i);
			return VINE_DATAVINE_RPC_REJECTED;
		}
		offset += 148 + metadata_size + (size_t)data_size;
	}
	if (offset != size) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	vine_datavine_rpc_put_u32(result, count);
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t get_edata(struct vine_datavine_rpc_server *server,
		const unsigned char *request, size_t size,
		unsigned char **result, size_t *result_size)
{
	if (size != 8) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	char content_hash[65];
	char serialized_hash[65];
	unsigned char *metadata = 0;
	unsigned char *payload = 0;
	size_t metadata_size = 0;
	size_t payload_size = 0;
	if (!vine_datavine_index_get_edata(server->index,
			(int64_t)vine_datavine_rpc_get_u64(request),
			content_hash, serialized_hash, &metadata, &metadata_size,
			&payload, &payload_size)) {
		return VINE_DATAVINE_RPC_NOT_FOUND;
	}
	if (metadata_size > UINT32_MAX
			|| payload_size > DATAVINE_RPC_MAX_PAYLOAD - 140 - metadata_size) {
		free(metadata);
		free(payload);
		return VINE_DATAVINE_RPC_INTERNAL;
	}
	*result_size = 140 + metadata_size + payload_size;
	*result = malloc(*result_size);
	if (!*result) {
		free(metadata);
		free(payload);
		*result_size = 0;
		return VINE_DATAVINE_RPC_INTERNAL;
	}
	vine_datavine_rpc_put_u32(*result, (uint32_t)metadata_size);
	vine_datavine_rpc_put_u64(*result + 4, payload_size);
	memcpy(*result + 12, content_hash, 64);
	memcpy(*result + 76, serialized_hash, 64);
	if (metadata_size) {
		memcpy(*result + 140, metadata, metadata_size);
	}
	if (payload_size) {
		memcpy(*result + 140 + metadata_size, payload, payload_size);
	}
	free(metadata);
	free(payload);
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
	if (!worker_length || worker_length > VINE_DATAVINE_WORKER_ID_MAX || !endpoint_length || endpoint_length > VINE_DATAVINE_ENDPOINT_MAX || !worker_epoch || count > (DATAVINE_RPC_MAX_PAYLOAD - 16 - worker_length - endpoint_length) / DATAVINE_RPC_PUBLICATION_SIZE || size != 16 + (size_t)worker_length + endpoint_length + (size_t)count * DATAVINE_RPC_PUBLICATION_SIZE) {
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
		if (length < 1 || length > VINE_DATAVINE_REPLICA_ID_MAX || tier != VINE_DATAVINE_WORKER_DISK || !vine_datavine_index_put(server->index, data_id, attempt, hash, bytes) || !vine_datavine_directory_publish_replica(server->directory, 'i', data_id, replica_id, attempt, tier, hash, bytes, worker_id, worker_epoch, endpoint, &replica)) {
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
	if (!vine_datavine_directory_release_source(server->directory, transfer, success)) {
		return VINE_DATAVINE_RPC_REJECTED;
	}
	return VINE_DATAVINE_RPC_OK;
}

static uint32_t invalidate_replica(struct vine_datavine_rpc_server *server,
		const unsigned char *payload, size_t size)
{
	if (size < 16) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	char kind = (char)payload[0];
	uint16_t replica_length = get_u16(payload + 2);
	int64_t data_id = (int64_t)vine_datavine_rpc_get_u64(payload + 8);
	if (!replica_length || replica_length > VINE_DATAVINE_REPLICA_ID_MAX
			|| size != 16 + (size_t)replica_length) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	char replica[VINE_DATAVINE_REPLICA_ID_MAX + 1];
	memcpy(replica, payload + 16, replica_length);
	replica[replica_length] = 0;
	int invalidated = vine_datavine_directory_invalidate_replica(
			server->directory, kind, data_id, replica);
	if (invalidated < 0) {
		return VINE_DATAVINE_RPC_INVALID;
	}
	return invalidated ? VINE_DATAVINE_RPC_OK : VINE_DATAVINE_RPC_NOT_FOUND;
}

static int process_request(struct vine_datavine_rpc_server *server, struct rpc_connection *connection)
{
	const unsigned char *header = connection->header;
	uint16_t opcode = (uint16_t)((header[6] << 8) | header[7]);
	uint64_t request_id = vine_datavine_rpc_get_u64(header + 12);
	uint32_t status = VINE_DATAVINE_RPC_OK;
	unsigned char result[8];
	unsigned char *dynamic_result = 0;
	size_t result_size = 0;
	if (!connection->authorized) {
		if (opcode != VINE_DATAVINE_RPC_AUTH || connection->payload_size != server->token_length || memcmp(connection->payload, server->token, server->token_length)) {
			status = VINE_DATAVINE_RPC_UNAUTHORIZED;
		} else {
			connection->authorized = 1;
		}
	} else if (opcode == VINE_DATAVINE_RPC_PING) {
		status = connection->payload_size ? VINE_DATAVINE_RPC_INVALID : VINE_DATAVINE_RPC_OK;
	} else if (opcode == VINE_DATAVINE_RPC_ALLOCATE_BATCH) {
		status = allocate_batch(server, connection->payload, connection->payload_size, result);
		result_size = 4;
	} else if (opcode == VINE_DATAVINE_RPC_PUBLISH_BATCH) {
		status = publish_batch(server, connection->payload, connection->payload_size, result);
		result_size = 4;
	} else if (opcode == VINE_DATAVINE_RPC_CLAIM_WORKER) {
		status = claim_worker(server, connection->payload, connection->payload_size, result);
		result_size = status == VINE_DATAVINE_RPC_OK ? 8 : 0;
	} else if (opcode == VINE_DATAVINE_RPC_PUBLISH_OUTPUTS) {
		status = publish_outputs(server, connection->payload, connection->payload_size, &dynamic_result, &result_size);
	} else if (opcode == VINE_DATAVINE_RPC_DISCONNECT_WORKER) {
		status = disconnect_worker(server, connection->payload, connection->payload_size);
	} else if (opcode == VINE_DATAVINE_RPC_REPORT_REPLICA) {
		status = report_replica(server, connection->payload, connection->payload_size, result);
		result_size = status == VINE_DATAVINE_RPC_OK ? 8 : 0;
	} else if (opcode == VINE_DATAVINE_RPC_RESOLVE_SOURCE) {
		status = resolve_source(server, connection->payload, connection->payload_size, &dynamic_result, &result_size);
	} else if (opcode == VINE_DATAVINE_RPC_RELEASE_SOURCE) {
		status = release_source(server, connection->payload, connection->payload_size);
	} else if (opcode == VINE_DATAVINE_RPC_REGISTER_EDATA) {
		status = register_edata(server, connection->payload,
				connection->payload_size, result);
		result_size = 4;
	} else if (opcode == VINE_DATAVINE_RPC_GET_EDATA) {
		status = get_edata(server, connection->payload,
				connection->payload_size, &dynamic_result, &result_size);
	} else if (opcode == VINE_DATAVINE_RPC_INVALIDATE_REPLICA) {
		status = invalidate_replica(server, connection->payload,
				connection->payload_size);
	} else {
		status = VINE_DATAVINE_RPC_INVALID;
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
		if (connection->payload_size > DATAVINE_RPC_MAX_PAYLOAD) {
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
		const char *host, int port, const char *token, int threads, int64_t maximum_data_id)
{
	if (!token || !token[0] || threads < 1 || maximum_data_id < 1 || (uint64_t)maximum_data_id > UINT64_MAX / 4) {
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
	server->index = vine_datavine_index_create(maximum_data_id, 256);
	server->directory = vine_datavine_directory_create(
			1000000, (uint64_t)maximum_data_id * 4, 1000000, 65536, 256);
	server->listen_fd = listen_socket(host, port, &server->port);
	if (!server->token || !server->threads || !server->index || !server->directory || server->listen_fd < 0) {
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
	free(server->threads);
	free(server->token);
	free(server);
}

int vine_datavine_rpc_server_port(const struct vine_datavine_rpc_server *server)
{
	return server ? server->port : 0;
}
