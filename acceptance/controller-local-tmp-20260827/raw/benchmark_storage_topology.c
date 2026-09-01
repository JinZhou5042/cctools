#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

struct header { uint64_t id_be, size_be; };
static sem_t slots;
static const char *sink_root;
static int files_per_phase;
static atomic_ullong sink_files;
static atomic_ullong sink_bytes;
static atomic_int sink_failures;

static uint64_t hton64(uint64_t value)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
	return ((uint64_t)htonl((uint32_t)value) << 32) | htonl((uint32_t)(value >> 32));
#else
	return value;
#endif
}
static uint64_t ntoh64(uint64_t value) { return hton64(value); }

static int io_all(int fd, void *buffer, size_t size, int writing)
{
	char *cursor = buffer;
	while (size) {
		ssize_t count = writing ? write(fd, cursor, size) : read(fd, cursor, size);
		if (count < 0 && errno == EINTR) continue;
		if (count <= 0) return 0;
		cursor += count;
		size -= (size_t)count;
	}
	return 1;
}

static void *serve_connection(void *argument)
{
	int socket_fd = *(int *)argument;
	free(argument);
	char *buffer = malloc(1U << 20);
	for (int phase = 0; buffer && phase < 2; phase++) {
		for (int item = 0; item < files_per_phase; item++) {
			struct header header;
			if (!io_all(socket_fd, &header, sizeof(header), 0)) goto failure;
			uint64_t id = ntoh64(header.id_be), size = ntoh64(header.size_be);
			if (!size || size > (1U << 20)) goto failure;
			sem_wait(&slots);
			char final[4096], temporary[4096];
			int valid = snprintf(final, sizeof(final), "%s/s%d-%020" PRIu64, sink_root, phase, id) < (int)sizeof(final) &&
				snprintf(temporary, sizeof(temporary), "%s/.s%d-%020" PRIu64 ".part", sink_root, phase, id) < (int)sizeof(temporary);
			int output = valid ? open(temporary, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600) : -1;
			uint64_t remaining = size;
			while (valid && remaining) {
				size_t count = remaining < (1U << 20) ? (size_t)remaining : (1U << 20);
				valid = io_all(socket_fd, buffer, count, 0) && io_all(output, buffer, count, 1);
				remaining -= count;
			}
			valid = valid && fsync(output) == 0;
			if (output >= 0 && close(output)) valid = 0;
			if (valid) valid = rename(temporary, final) == 0;
			if (!valid) unlink(temporary);
			sem_post(&slots);
			if (!valid) goto failure;
			atomic_fetch_add(&sink_files, 1);
			atomic_fetch_add(&sink_bytes, size);
		}
		char ack = 'K';
		if (!io_all(socket_fd, &ack, 1, 1)) goto failure;
	}
	free(buffer);
	close(socket_fd);
	return 0;
failure:
	atomic_fetch_add(&sink_failures, 1);
	free(buffer);
	close(socket_fd);
	return 0;
}

static int run_server(int argc, char **argv)
{
	if (argc != 6) return 2;
	sink_root = argv[2];
	int clients = atoi(argv[3]);
	files_per_phase = atoi(argv[4]);
	int concurrency = atoi(argv[5]);
	if (clients < 1 || files_per_phase < 1 || concurrency < 1 || sem_init(&slots, 0, (unsigned)concurrency)) return 2;
	int listener = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	int reuse = 1;
	setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
	struct sockaddr_in address = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_ANY), .sin_port = 0};
	if (listener < 0 || bind(listener, (struct sockaddr *)&address, sizeof(address)) || listen(listener, clients)) return 2;
	socklen_t length = sizeof(address);
	if (getsockname(listener, (struct sockaddr *)&address, &length)) return 2;
	printf("{\"port\":%u}\n", ntohs(address.sin_port));
	fflush(stdout);
	pthread_t *threads = calloc((size_t)clients, sizeof(*threads));
	for (int index = 0; index < clients; index++) {
		int *connection = malloc(sizeof(*connection));
		*connection = accept4(listener, 0, 0, SOCK_CLOEXEC);
		if (*connection < 0 || pthread_create(&threads[index], 0, serve_connection, connection)) return 2;
	}
	close(listener);
	for (int index = 0; index < clients; index++) pthread_join(threads[index], 0);
	printf("{\"files\":%llu,\"bytes\":%llu,\"failures\":%d}\n",
		(unsigned long long)atomic_load(&sink_files), (unsigned long long)atomic_load(&sink_bytes), atomic_load(&sink_failures));
	return atomic_load(&sink_failures) ? 1 : 0;
}

static int wait_path(const char *path)
{
	for (int attempt = 0; attempt < 72000; attempt++) {
		if (!access(path, F_OK)) return 1;
		usleep(100000);
	}
	return 0;
}

static int mark(const char *control, const char *kind, int phase, int worker)
{
	char path[4096];
	if (snprintf(path, sizeof(path), "%s/%s.%d.%d", control, kind, phase, worker) >= (int)sizeof(path)) return 0;
	int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
	return fd >= 0 && !close(fd);
}

static int connect_host(const char *host, const char *port)
{
	struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
	struct addrinfo *addresses = 0;
	if (getaddrinfo(host, port, &hints, &addresses)) return -1;
	int fd = -1;
	for (struct addrinfo *item = addresses; item; item = item->ai_next) {
		fd = socket(item->ai_family, item->ai_socktype | SOCK_CLOEXEC, item->ai_protocol);
		if (fd >= 0 && !connect(fd, item->ai_addr, item->ai_addrlen)) break;
		if (fd >= 0) close(fd);
		fd = -1;
	}
	freeaddrinfo(addresses);
	return fd;
}

static uint64_t item_size(int item) { return item % 10 == 0 ? (1U << 20) : 4096; }

static int stream_phase(int fd, int worker, int files, int stream_phase_id, const void *buffer)
{
	for (int item = 0; item < files; item++) {
		uint64_t id = (uint64_t)worker * (uint64_t)files + (uint64_t)item;
		uint64_t size = item_size(item);
		struct header header = {hton64(id), hton64(size)};
		if (!io_all(fd, &header, sizeof(header), 1) || !io_all(fd, (void *)buffer, (size_t)size, 1)) return 0;
	}
	char ack;
	return io_all(fd, &ack, 1, 0) && ack == 'K' && stream_phase_id >= 0;
}

static int direct_phase(const char *root, int worker, int files, int phase, const void *buffer)
{
	for (int item = 0; item < files; item++) {
		uint64_t id = (uint64_t)worker * (uint64_t)files + (uint64_t)item;
		uint64_t size = item_size(item);
		char final[4096], temporary[4096];
		if (snprintf(final, sizeof(final), "%s/d%d-%020" PRIu64, root, phase, id) >= (int)sizeof(final) ||
			snprintf(temporary, sizeof(temporary), "%s/.d%d-%020" PRIu64 ".part.%ld", root, phase, id, (long)getpid()) >= (int)sizeof(temporary)) return 0;
		int fd = open(temporary, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
		int valid = fd >= 0 && io_all(fd, (void *)buffer, (size_t)size, 1) && fsync(fd) == 0;
		if (fd >= 0 && close(fd)) valid = 0;
		if (valid) valid = rename(temporary, final) == 0;
		if (!valid) { unlink(temporary); return 0; }
	}
	return 1;
}

static int run_client(int argc, char **argv)
{
	if (argc != 9) return 2;
	int worker = atoi(argv[2]), files = atoi(argv[3]);
	const char *control = argv[4], *shared = argv[5], *host = argv[6], *port = argv[7];
	int phases = atoi(argv[8]);
	void *buffer = malloc(1U << 20);
	if (!buffer) return 2;
	memset(buffer, worker + 1, 1U << 20);
	int fd = connect_host(host, port);
	if (fd < 0 || !mark(control, "ready", 0, worker)) return 1;
	for (int phase = 0; phase < phases; phase++) {
		char go[4096];
		if (snprintf(go, sizeof(go), "%s/go.%d", control, phase) >= (int)sizeof(go) || !wait_path(go)) return 1;
		int stream = phase == 0 || phase == 3;
		int valid = stream ? stream_phase(fd, worker, files, phase, buffer) : direct_phase(shared, worker, files, phase, buffer);
		if (!valid || !mark(control, "done", phase, worker)) return 1;
	}
	close(fd);
	free(buffer);
	return 0;
}

int main(int argc, char **argv)
{
	if (argc < 2) return 2;
	if (!strcmp(argv[1], "server")) return run_server(argc, argv);
	if (!strcmp(argv[1], "client")) return run_client(argc, argv);
	return 2;
}
