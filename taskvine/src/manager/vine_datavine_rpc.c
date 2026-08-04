/*
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_datavine_rpc.h"
#include "vine_datavine_index.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdatomic.h>
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
	atomic_int stopping;
};

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

static int process_request(struct vine_datavine_rpc_server *server, struct rpc_connection *connection)
{
	const unsigned char *header = connection->header;
	uint16_t opcode = (uint16_t)((header[6] << 8) | header[7]);
	uint64_t request_id = vine_datavine_rpc_get_u64(header + 12);
	uint32_t status = VINE_DATAVINE_RPC_OK;
	unsigned char result[4];
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
	} else {
		status = VINE_DATAVINE_RPC_INVALID;
	}
	return response_create(connection, opcode, status, request_id, result, result_size);
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
	if (!token || !token[0] || threads < 1) {
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
	server->listen_fd = listen_socket(host, port, &server->port);
	if (!server->token || !server->threads || !server->index || server->listen_fd < 0) {
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
	free(server->threads);
	free(server->token);
	free(server);
}

int vine_datavine_rpc_server_port(const struct vine_datavine_rpc_server *server)
{
	return server ? server->port : 0;
}
