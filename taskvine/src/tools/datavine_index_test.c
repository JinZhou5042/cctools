#include "vine_datavine_index.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct worker_args {
	struct vine_datavine_index *index;
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

static void *publish(void *arg)
{
	struct worker_args *worker = arg;
	const char *hash = "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881";
	for (int64_t id = worker->first; id < worker->first + worker->count; id++) {
		struct vine_datavine_data record;
		if (!vine_datavine_index_publish(worker->index, id, 1, hash, 1, &record) || record.data_id != id || record.attempt != 1) {
			worker->failed = 1;
			break;
		}
	}
	return 0;
}

int main(int argc, char **argv)
{
	int threads = argc > 1 ? atoi(argv[1]) : 8;
	int64_t records = argc > 2 ? atoll(argv[2]) : 1000000;
	if (threads < 1 || records < threads) {
		return 2;
	}
	struct vine_datavine_index *index = vine_datavine_index_create(records, 256);
	if (!index) {
		return 2;
	}
	for (int64_t id = 1; id <= records; id++) {
		if (!vine_datavine_index_allocate(index, id, id, 0)) {
			return 2;
		}
	}
	pthread_t *ids = calloc((size_t)threads, sizeof(*ids));
	struct worker_args *args = calloc((size_t)threads, sizeof(*args));
	double started = monotonic_seconds();
	int64_t assigned = 0;
	for (int i = 0; i < threads; i++) {
		int64_t remaining = records - assigned;
		int64_t count = remaining / (threads - i);
		args[i] = (struct worker_args){index, assigned + 1, count, 0};
		assigned += count;
		pthread_create(&ids[i], 0, publish, &args[i]);
	}
	int failed = 0;
	for (int i = 0; i < threads; i++) {
		pthread_join(ids[i], 0);
		failed |= args[i].failed;
	}
	double elapsed = monotonic_seconds() - started;
	const char *first_hash = "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881";
	const char *second_hash = "a1fce4363854ff888cff4b8e7875d600c2682390412a8cf79b37d0b11148b0fa";
	failed |= !vine_datavine_index_put(index, 1, 1, first_hash, 1);
	failed |= !vine_datavine_index_put(index, 1, 2, second_hash, 2);
	failed |= vine_datavine_index_put(index, 1, 1, first_hash, 1);
	failed |= vine_datavine_index_put(index, 1, 2, first_hash, 1);
	struct vine_datavine_data first;
	failed |= !vine_datavine_index_get(index, 1, &first);
	failed |= first.attempt != 2 || first.size != 2 || strcmp(first.content_hash, second_hash);
	struct vine_datavine_index_metrics metrics;
	vine_datavine_index_get_metrics(index, &metrics);
	printf("records=%lld threads=%d seconds=%.6f records_per_second=%.0f\n",
			(long long)records,
			threads,
			elapsed,
			records / elapsed);
	failed |= metrics.allocations != (uint64_t)records;
	failed |= metrics.publications != (uint64_t)records + 1;
	failed |= metrics.idempotent_publications != 1;
	failed |= metrics.stale_rejections != 1;
	failed |= metrics.conflict_rejections != 1;
	free(ids);
	free(args);
	vine_datavine_index_delete(index);
	return failed ? 1 : 0;
}
