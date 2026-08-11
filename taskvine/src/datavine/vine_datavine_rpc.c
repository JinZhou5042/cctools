/* DataVine RPC implementation.
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_datavine_rpc.h"
#include "vine_datavine_data_controller.h"
#include "vine_datavine_workflow_store.h"
#include "jx.h"
#include "jx_print.h"

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
#include <time.h>
#include <unistd.h>

#define DATAVINE_RPC_EVENTS 128
#define DATAVINE_RPC_MAX_CONNECTIONS 1024
#define DATAVINE_RPC_IDLE_SECONDS 30

struct vine_datavine_rpc_server;

struct rpc_connection {
	int fd;
	int authorized;
	time_t last_activity;
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
	struct vine_datavine_workflow_store *workflow_store;
	struct vine_datavine_data_controller *data_controller;
	atomic_int stopping;
	atomic_size_t active_connections;
};

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
	close(connection->fd);
	atomic_fetch_sub(&thread->server->active_connections, 1);
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
	} else if (opcode == VINE_DATAVINE_RPC_WORKFLOW_CAPABILITIES) {
		static const unsigned char capabilities[] =
				"{\"schema_versions\":[\"" VINE_DATAVINE_WORKFLOW_SCHEMA_NAME
				"\",\"" VINE_DATAVINE_WORKFLOW_DELTA_SCHEMA_NAME "\"],"
				"\"executor_kinds\":[\"command\",\"python\",\"taskvine\"],"
				"\"digest\":\"sha1\",\"append\":\"delta-cas-v1\","
				"\"results\":\"durable-bytes\","
				"\"frontier\":true,"
				"\"result_identity\":\"sha256+attempt+producer+codec\","
				"\"selective_results\":true}";
		if (payload_size) {
			status = VINE_DATAVINE_RPC_INVALID;
		} else {
			*result_size = sizeof(capabilities) - 1;
			*result = malloc(*result_size);
			if (*result)
				memcpy(*result, capabilities, *result_size);
			else
				status = VINE_DATAVINE_RPC_INTERNAL;
		}
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
		if (opcode != VINE_DATAVINE_RPC_AUTH || connection->payload_size != server->token_length || memcmp(connection->payload, server->token, server->token_length)) {
			status = VINE_DATAVINE_RPC_UNAUTHORIZED;
		} else {
			connection->authorized = 1;
			status = VINE_DATAVINE_RPC_OK;
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
			connection->last_activity = time(0);
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
		if (atomic_fetch_add(&thread->server->active_connections, 1) >=
				DATAVINE_RPC_MAX_CONNECTIONS) {
			atomic_fetch_sub(&thread->server->active_connections, 1);
			close(fd);
			continue;
		}
		int enabled = 1;
		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
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
		time_t now = time(0);
		struct rpc_connection *connection = thread->connections;
		while (connection) {
			struct rpc_connection *next = connection->next;
			if (now - connection->last_activity >
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
			workflow_journal_path, (size_t)threads);
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
