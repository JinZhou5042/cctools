#include "vine_datavine_rpc.h"
#include "vine_datavine_directory.h"

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

static int request_response(struct client *client, uint16_t opcode,
		const unsigned char *payload, uint32_t size,
		unsigned char **response_body, uint32_t *response_body_size)
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
	*response_body_size = vine_datavine_rpc_get_u32(response + 12);
	*response_body = *response_body_size ? malloc(*response_body_size) : 0;
	return !*response_body_size || (*response_body && transfer_all(client->fd, *response_body, *response_body_size, 0));
}

static int request(struct client *client, uint16_t opcode, const unsigned char *payload, uint32_t size)
{
	unsigned char *body = 0;
	uint32_t body_size = 0;
	int valid = request_response(client, opcode, payload, size, &body, &body_size);
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

static int claim_worker(int port, const char *worker_id)
{
	struct client client = {.fd = -1};
	if (!client_open(&client, port)) {
		return 0;
	}
	const char *endpoint = "http://127.0.0.1:1";
	size_t worker_length = strlen(worker_id);
	size_t endpoint_length = strlen(endpoint);
	unsigned char payload[4 + VINE_DATAVINE_WORKER_ID_MAX + VINE_DATAVINE_ENDPOINT_MAX];
	payload[0] = (unsigned char)(worker_length >> 8);
	payload[1] = (unsigned char)worker_length;
	payload[2] = (unsigned char)(endpoint_length >> 8);
	payload[3] = (unsigned char)endpoint_length;
	memcpy(payload + 4, worker_id, worker_length);
	memcpy(payload + 4 + worker_length, endpoint, endpoint_length);
	unsigned char *body = 0;
	uint32_t body_size = 0;
	int valid = request_response(&client, VINE_DATAVINE_RPC_CLAIM_WORKER, payload, (uint32_t)(4 + worker_length + endpoint_length), &body, &body_size) && body_size == 8 && vine_datavine_rpc_get_u64(body) == 1;
	free(body);
	close(client.fd);
	return valid;
}

static int source_protocol(int port, const char *source_worker)
{
	if (!claim_worker(port, "destination")) {
		return 0;
	}
	struct client client = {.fd = -1};
	if (!client_open(&client, port)) {
		return 0;
	}
	const char *replica_id = "edata-replica";
	uint16_t worker_length = (uint16_t)strlen(source_worker);
	uint16_t replica_length = (uint16_t)strlen(replica_id);
	unsigned char report[104 + 64];
	memset(report, 0, sizeof(report));
	report[0] = 'e';
	report[1] = VINE_DATAVINE_WORKER_DRAM;
	report[2] = (unsigned char)(worker_length >> 8);
	report[3] = (unsigned char)worker_length;
	report[4] = (unsigned char)(replica_length >> 8);
	report[5] = (unsigned char)replica_length;
	vine_datavine_rpc_put_u64(report + 8, 1);
	vine_datavine_rpc_put_u64(report + 16, 42);
	vine_datavine_rpc_put_u32(report + 24, 1);
	vine_datavine_rpc_put_u64(report + 32, 1);
	memcpy(report + 40,
			"2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881",
			64);
	memcpy(report + 104, source_worker, worker_length);
	memcpy(report + 104 + worker_length, replica_id, replica_length);
	if (!request(&client, VINE_DATAVINE_RPC_REPORT_REPLICA, report, 104 + worker_length + replica_length)) {
		close(client.fd);
		return 0;
	}
	const char *destination = "destination";
	const char *transfer = "taskvine:rpc-source-test";
	uint16_t destination_length = (uint16_t)strlen(destination);
	uint16_t transfer_length = (uint16_t)strlen(transfer);
	unsigned char resolve[24 + 64];
	memset(resolve, 0, sizeof(resolve));
	resolve[0] = 'e';
	resolve[2] = (unsigned char)(destination_length >> 8);
	resolve[3] = (unsigned char)destination_length;
	resolve[4] = (unsigned char)(transfer_length >> 8);
	resolve[5] = (unsigned char)transfer_length;
	vine_datavine_rpc_put_u64(resolve + 8, 1);
	vine_datavine_rpc_put_u64(resolve + 16, 42);
	memcpy(resolve + 24, destination, destination_length);
	memcpy(resolve + 24 + destination_length, transfer, transfer_length);
	unsigned char *body = 0;
	uint32_t body_size = 0;
	int valid = request_response(&client, VINE_DATAVINE_RPC_RESOLVE_SOURCE, resolve, 24 + destination_length + transfer_length, &body, &body_size);
	if (valid) {
		valid = body_size >= 112;
	}
	if (valid) {
		uint16_t resolved_worker_length = (uint16_t)((body[40] << 8) | body[41]);
		valid = resolved_worker_length == worker_length && !memcmp(body + 112, source_worker, worker_length);
	}
	free(body);
	unsigned char release[8 + VINE_DATAVINE_TRANSFER_ID_MAX];
	memset(release, 0, sizeof(release));
	release[0] = 1;
	release[4] = (unsigned char)(transfer_length >> 8);
	release[5] = (unsigned char)transfer_length;
	memcpy(release + 8, transfer, transfer_length);
	valid &= request(&client, VINE_DATAVINE_RPC_RELEASE_SOURCE, release, 8 + transfer_length);
	close(client.fd);
	return valid;
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
	char worker_id[32];
	snprintf(worker_id, sizeof(worker_id), "worker-%lld", (long long)worker->first);
	uint16_t worker_length = (uint16_t)strlen(worker_id);
	unsigned char payload[16 + 32 + BATCH * PUBLICATION_SIZE];
	int64_t end = worker->first + worker->count;
	for (int64_t first = worker->first; first < end; first += BATCH) {
		uint32_t count = (uint32_t)(end - first);
		if (count > BATCH) {
			count = BATCH;
		}
		payload[0] = (unsigned char)(worker_length >> 8);
		payload[1] = (unsigned char)worker_length;
		payload[2] = 0;
		payload[3] = 0;
		vine_datavine_rpc_put_u64(payload + 4, 1);
		vine_datavine_rpc_put_u32(payload + 12, count);
		memcpy(payload + 16, worker_id, worker_length);
		for (uint32_t i = 0; i < count; i++) {
			unsigned char *record = payload + 16 + worker_length + i * PUBLICATION_SIZE;
			vine_datavine_rpc_put_u64(record, (uint64_t)first + i);
			vine_datavine_rpc_put_u32(record + 8, 1);
			vine_datavine_rpc_put_u32(record + 12, 0);
			vine_datavine_rpc_put_u64(record + 16, 1);
			memcpy(record + 24, hash, 64);
		}
		if (!request(&client, VINE_DATAVINE_RPC_PUBLISH_OUTPUTS, payload, 16 + worker_length + count * PUBLICATION_SIZE)) {
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
	for (int i = 0; i < clients; i++) {
		int64_t count = (records - assigned) / (clients - i);
		char worker_id[32];
		snprintf(worker_id, sizeof(worker_id), "worker-%lld", (long long)assigned + 1);
		if (!claim_worker(vine_datavine_rpc_server_port(server), worker_id)) {
			return 2;
		}
		assigned += count;
	}
	if (!source_protocol(vine_datavine_rpc_server_port(server), "worker-1")) {
		return 2;
	}
	assigned = 0;
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
