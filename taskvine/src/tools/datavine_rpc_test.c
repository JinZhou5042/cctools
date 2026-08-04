#include "vine_datavine_rpc.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define BATCH 64
#define ALLOCATION_SIZE 24
#define PUBLICATION_SIZE 88

struct client {
	int fd;
	uint64_t request_id;
};

struct publish_args {
	int port;
	int64_t first;
	int64_t count;
	int failed;
};

static double monotonic_seconds(void)
{
	struct timespec value;
	clock_gettime(CLOCK_MONOTONIC, &value);
	return value.tv_sec + value.tv_nsec / 1000000000.0;
}

static int transfer_all(int fd, unsigned char *buffer, size_t size, int writing)
{
	while (size) {
		ssize_t count = writing ? send(fd, buffer, size, MSG_NOSIGNAL) : recv(fd, buffer, size, 0);
		if (count > 0) {
			buffer += count;
			size -= (size_t)count;
			continue;
		}
		if (count < 0 && errno == EINTR) {
			continue;
		}
		return 0;
	}
	return 1;
}

static int request(struct client *client, uint16_t opcode, const unsigned char *payload, uint32_t size)
{
	unsigned char header[VINE_DATAVINE_RPC_REQUEST_HEADER];
	vine_datavine_rpc_put_u32(header, VINE_DATAVINE_RPC_MAGIC);
	header[4] = 0;
	header[5] = VINE_DATAVINE_RPC_VERSION;
	header[6] = (unsigned char)(opcode >> 8);
	header[7] = (unsigned char)opcode;
	vine_datavine_rpc_put_u32(header + 8, size);
	vine_datavine_rpc_put_u64(header + 12, ++client->request_id);
	if (!transfer_all(client->fd, header, sizeof(header), 1) || (size && !transfer_all(client->fd, (unsigned char *)payload, size, 1))) {
		return 0;
	}
	unsigned char response[VINE_DATAVINE_RPC_RESPONSE_HEADER];
	if (!transfer_all(client->fd, response, sizeof(response), 0) || vine_datavine_rpc_get_u32(response) != VINE_DATAVINE_RPC_MAGIC || vine_datavine_rpc_get_u32(response + 8) != VINE_DATAVINE_RPC_OK || vine_datavine_rpc_get_u64(response + 16) != client->request_id) {
		return 0;
	}
	uint32_t response_size = vine_datavine_rpc_get_u32(response + 12);
	unsigned char *body = response_size ? malloc(response_size) : 0;
	int valid = (!response_size || (body && transfer_all(client->fd, body, response_size, 0)));
	free(body);
	return valid;
}

static int client_open(struct client *client, int port)
{
	client->fd = socket(AF_INET, SOCK_STREAM, 0);
	int enabled = 1;
	setsockopt(client->fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
	struct sockaddr_in address = {
			.sin_family = AF_INET,
			.sin_port = htons((uint16_t)port),
			.sin_addr.s_addr = htonl(INADDR_LOOPBACK),
	};
	return client->fd >= 0 && connect(client->fd, (struct sockaddr *)&address, sizeof(address)) == 0 && request(client, VINE_DATAVINE_RPC_AUTH, (const unsigned char *)"test-token", 10);
}

static int allocate_records(int port, int64_t records)
{
	struct client client = {.fd = -1};
	if (!client_open(&client, port)) {
		return 0;
	}
	unsigned char payload[4 + BATCH * ALLOCATION_SIZE];
	for (int64_t first = 1; first <= records; first += BATCH) {
		uint32_t count = (uint32_t)(records - first + 1);
		if (count > BATCH) {
			count = BATCH;
		}
		vine_datavine_rpc_put_u32(payload, count);
		for (uint32_t i = 0; i < count; i++) {
			unsigned char *record = payload + 4 + i * ALLOCATION_SIZE;
			vine_datavine_rpc_put_u64(record, (uint64_t)first + i);
			vine_datavine_rpc_put_u64(record + 8, (uint64_t)first + i);
			vine_datavine_rpc_put_u32(record + 16, 0);
			vine_datavine_rpc_put_u32(record + 20, 0);
		}
		if (!request(&client, VINE_DATAVINE_RPC_ALLOCATE_BATCH, payload, 4 + count * ALLOCATION_SIZE)) {
			close(client.fd);
			return 0;
		}
	}
	close(client.fd);
	return 1;
}

static void *publish_records(void *arg)
{
	struct publish_args *worker = arg;
	struct client client = {.fd = -1};
	if (!client_open(&client, worker->port)) {
		worker->failed = 1;
		return 0;
	}
	const char *hash = "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881";
	unsigned char payload[4 + BATCH * PUBLICATION_SIZE];
	int64_t end = worker->first + worker->count;
	for (int64_t first = worker->first; first < end; first += BATCH) {
		uint32_t count = (uint32_t)(end - first);
		if (count > BATCH) {
			count = BATCH;
		}
		vine_datavine_rpc_put_u32(payload, count);
		for (uint32_t i = 0; i < count; i++) {
			unsigned char *record = payload + 4 + i * PUBLICATION_SIZE;
			vine_datavine_rpc_put_u64(record, (uint64_t)first + i);
			vine_datavine_rpc_put_u32(record + 8, 1);
			vine_datavine_rpc_put_u32(record + 12, 0);
			vine_datavine_rpc_put_u64(record + 16, 1);
			memcpy(record + 24, hash, 64);
		}
		if (!request(&client, VINE_DATAVINE_RPC_PUBLISH_BATCH, payload, 4 + count * PUBLICATION_SIZE)) {
			worker->failed = 1;
			break;
		}
	}
	close(client.fd);
	return 0;
}

int main(int argc, char **argv)
{
	int server_threads = argc > 1 ? atoi(argv[1]) : 1;
	int clients = argc > 2 ? atoi(argv[2]) : 8;
	int64_t records = argc > 3 ? atoll(argv[3]) : 1000000;
	if (server_threads < 1 || clients < 1 || records < clients) {
		return 2;
	}
	struct vine_datavine_rpc_server *server = vine_datavine_rpc_server_create(
			"127.0.0.1", 0, "test-token", server_threads, records);
	if (!server || !allocate_records(vine_datavine_rpc_server_port(server), records)) {
		return 2;
	}
	pthread_t *ids = calloc((size_t)clients, sizeof(*ids));
	struct publish_args *args = calloc((size_t)clients, sizeof(*args));
	int64_t assigned = 0;
	double started = monotonic_seconds();
	for (int i = 0; i < clients; i++) {
		int64_t count = (records - assigned) / (clients - i);
		args[i] = (struct publish_args){vine_datavine_rpc_server_port(server), assigned + 1, count, 0};
		assigned += count;
		pthread_create(&ids[i], 0, publish_records, &args[i]);
	}
	int failed = 0;
	for (int i = 0; i < clients; i++) {
		pthread_join(ids[i], 0);
		failed |= args[i].failed;
	}
	double elapsed = monotonic_seconds() - started;
	printf("records=%lld server_threads=%d clients=%d seconds=%.6f records_per_second=%.0f\n",
			(long long)records,
			server_threads,
			clients,
			elapsed,
			records / elapsed);
	free(ids);
	free(args);
	vine_datavine_rpc_server_delete(server);
	return failed ? 1 : 0;
}
