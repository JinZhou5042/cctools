#include "vine_datavine_replica_table.h"

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>

struct phase {
	const char *name;
	uint64_t operations;
	double seconds;
};

struct observations {
	uint64_t woken;
	uint64_t released;
	uint64_t lost;
};

static double monotonic_seconds(void)
{
	struct timespec now;
	if (clock_gettime(CLOCK_MONOTONIC, &now)) {
		perror("clock_gettime");
		exit(EXIT_FAILURE);
	}
	return (double)now.tv_sec + (double)now.tv_nsec / 1e9;
}

static uint64_t parse_count(const char *text, const char *name)
{
	char *end = 0;
	errno = 0;
	unsigned long long value = strtoull(text, &end, 10);
	if (errno || !end || *end || !value) {
		fprintf(stderr, "invalid %s: %s\n", name, text);
		exit(EXIT_FAILURE);
	}
	return (uint64_t)value;
}

static void wake_waiter(uint64_t data_id, uint32_t generation,
		uint32_t worker_slot, uint64_t session_epoch, uint64_t request_id,
		uint32_t item_index, void *argument)
{
	(void)data_id;
	(void)generation;
	(void)worker_slot;
	(void)session_epoch;
	(void)request_id;
	(void)item_index;
	((struct observations *)argument)->woken++;
}

static void release_replica(uint64_t data_id, uint32_t generation,
		const struct vine_datavine_replica_view *replica, void *argument)
{
	(void)data_id;
	(void)generation;
	(void)replica;
	((struct observations *)argument)->released++;
}

static void last_replica(uint64_t data_id, uint32_t generation, void *argument)
{
	(void)data_id;
	(void)generation;
	((struct observations *)argument)->lost++;
}

static void record_phase(struct phase *phase, const char *name,
		uint64_t operations, double started)
{
	phase->name = name;
	phase->operations = operations;
	phase->seconds = monotonic_seconds() - started;
}

static uint64_t random_data_id(uint64_t index, uint64_t data_count)
{
	uint64_t value = index + UINT64_C(0x9e3779b97f4a7c15);
	value ^= value >> 30;
	value *= UINT64_C(0xbf58476d1ce4e5b9);
	value ^= value >> 27;
	value *= UINT64_C(0x94d049bb133111eb);
	value ^= value >> 31;
	return value % data_count + 1;
}

int main(int argc, char **argv)
{
	uint64_t data_count = 100000;
	uint64_t workers = 32;
	uint64_t replicas_per_data = 1;
	int waiters = 1;
	int disconnect = 0;
	for (int index = 1; index < argc; index++) {
		if (!strcmp(argv[index], "--data") && index + 1 < argc)
			data_count = parse_count(argv[++index], "data count");
		else if (!strcmp(argv[index], "--workers") && index + 1 < argc)
			workers = parse_count(argv[++index], "worker count");
		else if (!strcmp(argv[index], "--replicas") && index + 1 < argc)
			replicas_per_data = parse_count(argv[++index], "replica count");
		else if (!strcmp(argv[index], "--no-waiters"))
			waiters = 0;
		else if (!strcmp(argv[index], "--scenario") && index + 1 < argc) {
			const char *scenario = argv[++index];
			if (!strcmp(scenario, "disconnect"))
				disconnect = 1;
			else if (strcmp(scenario, "lifecycle")) {
				fprintf(stderr, "scenario must be lifecycle or disconnect\n");
				return EXIT_FAILURE;
			}
		} else {
			fprintf(stderr, "usage: %s [--data N] [--workers N] "
					"[--replicas N] [--no-waiters] "
					"[--scenario lifecycle|disconnect]\n", argv[0]);
			return EXIT_FAILURE;
		}
	}
	if (workers > UINT32_MAX - 1 || replicas_per_data > UINT32_MAX ||
			data_count > UINT64_MAX / replicas_per_data) {
		fprintf(stderr, "benchmark dimensions overflow\n");
		return EXIT_FAILURE;
	}

	struct vine_datavine_replica_table *table =
			vine_datavine_replica_table_create();
	if (!table)
		return EXIT_FAILURE;
	struct observations observations = {0};
	struct phase phases[8];
	size_t phase_count = 0;
	unsigned char digest[32];
	memset(digest, 0xa5, sizeof(digest));
	double total_started = monotonic_seconds();
	double started = monotonic_seconds();
	for (uint64_t worker = 1; worker <= workers; worker++) {
		if (!vine_datavine_replica_table_session_open(
				table, (uint32_t)worker, worker + 1000))
			goto failed;
	}
	record_phase(&phases[phase_count++], "session_open", workers, started);

	started = monotonic_seconds();
	for (uint64_t data_id = 1; data_id <= data_count; data_id++) {
		uint32_t generation = 1;
		if (!vine_datavine_replica_table_expect(
				table, data_id, &generation))
			goto failed;
	}
	record_phase(&phases[phase_count++], "expect", data_count, started);

	if (waiters) {
		started = monotonic_seconds();
		for (uint64_t data_id = 1; data_id <= data_count; data_id++) {
			uint32_t worker = (uint32_t)((data_id - 1) % workers + 1);
			if (!vine_datavine_replica_table_wait(table, data_id, 1,
					worker, (uint64_t)worker + 1000, data_id, 0))
				goto failed;
		}
		record_phase(&phases[phase_count++], "wait", data_count, started);
	}

	uint64_t publications = data_count * replicas_per_data;
	started = monotonic_seconds();
	for (uint64_t replica_index = 0; replica_index < replicas_per_data;
			replica_index++) {
		for (uint64_t data_id = 1; data_id <= data_count; data_id++) {
			uint32_t worker = (uint32_t)(
					(data_id + replica_index - 1) % workers + 1);
			uint64_t object_token = replica_index * data_count + data_id;
			if (!vine_datavine_replica_table_publish(table, data_id, 1,
					4096, digest, worker, (uint64_t)worker + 1000,
					object_token, wake_waiter, &observations))
				goto failed;
		}
	}
	record_phase(&phases[phase_count++], "publish", publications, started);
	if (waiters && observations.woken != data_count)
		goto failed;

	started = monotonic_seconds();
	for (uint64_t index = 0; index < data_count; index++) {
		uint64_t data_id = random_data_id(index, data_count);
		struct vine_datavine_replica_view view;
		size_t count = 0;
		if (vine_datavine_replica_table_resolve(table, data_id, 1,
				&view, 1, &count, 0, 0, 0) !=
				VINE_DATAVINE_RESOLVE_AVAILABLE || count != 1)
			goto failed;
	}
	record_phase(&phases[phase_count++], "resolve_random", data_count, started);

	if (disconnect) {
		started = monotonic_seconds();
		for (uint64_t worker = 1; worker <= workers; worker++) {
			if (!vine_datavine_replica_table_session_lost(table,
					(uint32_t)worker, worker + 1000,
					last_replica, 0, &observations))
				goto failed;
		}
		record_phase(&phases[phase_count++], "session_lost",
				publications, started);
		if (observations.lost != data_count)
			goto failed;
	}

	started = monotonic_seconds();
	for (uint64_t data_id = 1; data_id <= data_count; data_id++) {
		if (!vine_datavine_replica_table_mark_dead(table, data_id, 1,
				release_replica, 0, &observations))
			goto failed;
	}
	record_phase(&phases[phase_count++], "mark_dead", data_count, started);
	if ((!disconnect && observations.released != publications) ||
			(disconnect && observations.released))
		goto failed;

	struct vine_datavine_replica_stats stats;
	if (!vine_datavine_replica_table_stats(table, &stats) ||
			stats.active_data || stats.active_replicas || stats.active_waiters ||
			!vine_datavine_replica_table_check(table))
		goto failed;
	struct rusage usage;
	memset(&usage, 0, sizeof(usage));
	getrusage(RUSAGE_SELF, &usage);
	double total_seconds = monotonic_seconds() - total_started;
	printf("{\"status\":\"PASS\",\"scenario\":\"%s\","
			"\"data\":%" PRIu64 ",\"workers\":%" PRIu64 ","
			"\"replicas_per_data\":%" PRIu64 ",\"waiters\":%s,"
			"\"total_seconds\":%.9f,\"max_rss_kib\":%ld,"
			"\"peak_data\":%" PRIu64 ",\"peak_replicas\":%" PRIu64 ","
			"\"peak_waiters\":%" PRIu64 ",\"phases\":{",
			disconnect ? "disconnect" : "lifecycle", data_count, workers,
			replicas_per_data, waiters ? "true" : "false", total_seconds,
			usage.ru_maxrss, stats.peak_data, stats.peak_replicas,
			stats.peak_waiters);
	for (size_t index = 0; index < phase_count; index++) {
		struct phase *phase = &phases[index];
		printf("%s\"%s\":{\"operations\":%" PRIu64
				",\"seconds\":%.9f,\"operations_per_second\":%.3f}",
				index ? "," : "", phase->name, phase->operations,
				phase->seconds, phase->seconds > 0
					? (double)phase->operations / phase->seconds : 0.0);
	}
	puts("}}");
	vine_datavine_replica_table_delete(table);
	return EXIT_SUCCESS;

failed:
	fprintf(stderr, "replica benchmark invariant failed\n");
	vine_datavine_replica_table_delete(table);
	return EXIT_FAILURE;
}
