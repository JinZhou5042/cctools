#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

struct config {
	const char *mode;
	const char *dir;
	uint64_t files;
	uint64_t size;
	int threads;
	int direct;
};

struct worker {
	struct config *cfg;
	int id;
	pthread_barrier_t *barrier;
	atomic_int *failed;
	uint64_t bytes;
	uint64_t checksum;
};

static double now_seconds(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static int transfer_all(int fd, void *buffer, size_t block, uint64_t size,
		int writing, uint64_t *checksum)
{
	uint64_t offset = 0;
	while (offset < size) {
		size_t count = size - offset < block ? (size_t)(size - offset) : block;
		ssize_t result = writing ? write(fd, buffer, count) : read(fd, buffer, count);
		if (result < 0 && errno == EINTR)
			continue;
		if (result <= 0 || (size_t)result != count)
			return 0;
		if (!writing)
			*checksum += ((unsigned char *)buffer)[0] + (uint64_t)result;
		offset += (uint64_t)result;
	}
	return 1;
}

static void *run_worker(void *argument)
{
	struct worker *worker = argument;
	struct config *cfg = worker->cfg;
	size_t block = cfg->size < (1U << 20) ? (size_t)cfg->size : (1U << 20);
	if (block < 4096)
		block = 4096;
	void *buffer = 0;
	if (posix_memalign(&buffer, 4096, block)) {
		atomic_store(worker->failed, 1);
		return 0;
	}
	memset(buffer, (unsigned char)(worker->id + 1), block);
	pthread_barrier_wait(worker->barrier);
	for (uint64_t index = (uint64_t)worker->id; index < cfg->files;
			index += (uint64_t)cfg->threads) {
		char final[4096];
		char temporary[4096];
		if (snprintf(final, sizeof(final), "%s/f%012" PRIu64, cfg->dir, index) >= (int)sizeof(final) ||
				snprintf(temporary, sizeof(temporary), "%s/.f%012" PRIu64 ".part.%d",
					cfg->dir, index, worker->id) >= (int)sizeof(temporary)) {
			atomic_store(worker->failed, 1);
			break;
		}
		int writing = !strcmp(cfg->mode, "write");
		int flags = writing ? O_WRONLY | O_CREAT | O_EXCL : O_RDONLY;
		if (cfg->direct)
			flags |= O_DIRECT;
		int fd = open(writing ? temporary : final, flags | O_CLOEXEC, 0600);
		int valid = fd >= 0 && transfer_all(fd, buffer, block, cfg->size,
				writing, &worker->checksum);
		if (valid && writing)
			valid = fsync(fd) == 0;
		if (fd >= 0 && close(fd))
			valid = 0;
		if (valid && writing)
			valid = rename(temporary, final) == 0;
		if (!valid) {
			fprintf(stderr, "io failure mode=%s file=%" PRIu64 " errno=%d (%s)\n",
				cfg->mode, index, errno, strerror(errno));
			if (writing)
				unlink(temporary);
			atomic_store(worker->failed, 1);
			break;
		}
		worker->bytes += cfg->size;
	}
	free(buffer);
	return 0;
}

int main(int argc, char **argv)
{
	if (argc != 7) {
		fprintf(stderr, "usage: %s write|read DIR FILES BYTES THREADS DIRECT\n", argv[0]);
		return 2;
	}
	struct config cfg = {
		.mode = argv[1], .dir = argv[2], .files = strtoull(argv[3], 0, 10),
		.size = strtoull(argv[4], 0, 10), .threads = atoi(argv[5]), .direct = atoi(argv[6])
	};
	if ((strcmp(cfg.mode, "write") && strcmp(cfg.mode, "read")) || !cfg.files ||
			!cfg.size || cfg.threads < 1 || cfg.threads > 1024)
		return 2;
	pthread_t *threads = calloc((size_t)cfg.threads, sizeof(*threads));
	struct worker *workers = calloc((size_t)cfg.threads, sizeof(*workers));
	pthread_barrier_t barrier;
	atomic_int failed = 0;
	if (!threads || !workers || pthread_barrier_init(&barrier, 0, (unsigned)cfg.threads + 1))
		return 2;
	for (int i = 0; i < cfg.threads; i++) {
		workers[i] = (struct worker){.cfg = &cfg, .id = i, .barrier = &barrier, .failed = &failed};
		if (pthread_create(&threads[i], 0, run_worker, &workers[i]))
			return 2;
	}
	pthread_barrier_wait(&barrier);
	double started = now_seconds();
	uint64_t bytes = 0, checksum = 0;
	for (int i = 0; i < cfg.threads; i++) {
		pthread_join(threads[i], 0);
		bytes += workers[i].bytes;
		checksum += workers[i].checksum;
	}
	double elapsed = now_seconds() - started;
	printf("{\"mode\":\"%s\",\"dir\":\"%s\",\"files\":%" PRIu64 ",\"file_bytes\":%" PRIu64
			",\"threads\":%d,\"direct\":%d,\"elapsed_seconds\":%.9f,"
			"\"files_per_second\":%.3f,\"mib_per_second\":%.3f,\"bytes\":%" PRIu64
			",\"checksum\":%" PRIu64 ",\"status\":\"%s\"}\n",
		cfg.mode, cfg.dir, cfg.files, cfg.size, cfg.threads, cfg.direct, elapsed,
		(double)cfg.files / elapsed, (double)bytes / 1048576.0 / elapsed,
		bytes, checksum, atomic_load(&failed) ? "FAIL" : "PASS");
	return atomic_load(&failed) ? 1 : 0;
}
