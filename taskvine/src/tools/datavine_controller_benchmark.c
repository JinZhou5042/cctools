#include "vine_datavine_data_controller.h"
#include "vine_datavine_journal.h"

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
	const char *failure_step = "arguments";
	uint64_t data_count = 100000;
	uint64_t workers = 32;
	uint64_t replicas_per_data = 1;
	const char *journal_path = 0;
	int disconnect = 0;
	for (int index = 1; index < argc; index++) {
		if (!strcmp(argv[index], "--data") && index + 1 < argc)
			data_count = parse_count(argv[++index], "data count");
		else if (!strcmp(argv[index], "--workers") && index + 1 < argc)
			workers = parse_count(argv[++index], "worker count");
		else if (!strcmp(argv[index], "--replicas") && index + 1 < argc)
			replicas_per_data = parse_count(argv[++index], "replica count");
		else if (!strcmp(argv[index], "--journal") && index + 1 < argc)
			journal_path = argv[++index];
		else if (!strcmp(argv[index], "--scenario") && index + 1 < argc) {
			const char *scenario = argv[++index];
			if (!strcmp(scenario, "disconnect"))
				disconnect = 1;
			else if (strcmp(scenario, "lifecycle")) {
				fprintf(stderr, "scenario must be lifecycle or disconnect\n");
				return EXIT_FAILURE;
			}
		} else {
			fprintf(stderr, "usage: %s --journal PATH [--data N] "
					"[--workers N] [--replicas N] "
					"[--scenario lifecycle|disconnect]\n", argv[0]);
			return EXIT_FAILURE;
		}
	}
	if (!journal_path || workers > UINT32_MAX - 1 ||
			replicas_per_data > UINT32_MAX ||
			data_count > UINT64_MAX / replicas_per_data) {
		fprintf(stderr, "missing journal or benchmark dimensions overflow\n");
		return EXIT_FAILURE;
	}

	struct vine_datavine_journal *journal =
			vine_datavine_journal_open(journal_path);
	struct vine_datavine_data_controller *controller = journal
			? vine_datavine_data_controller_open(journal_path, 0, journal) : 0;
	const char *workflow_id = "controller-benchmark";
	failure_step = "controller_open";
	if (!controller || !vine_datavine_data_controller_configure_object_service(
			controller, "127.0.0.1", 1, "controller-benchmark-token") ||
			!vine_datavine_data_controller_prepare_workflow(
				controller, workflow_id))
		goto failed;
	failure_step = "single_workflow_boundary";
	if (vine_datavine_data_controller_prepare_workflow(
			controller, "controller-benchmark-second"))
		goto failed;
	unsigned char workflow_key[32];
	failure_step = "workflow_key";
	if (!vine_datavine_data_controller_workflow_key(
			controller, workflow_id, workflow_key))
		goto failed;

	struct phase phases[9];
	size_t phase_count = 0;
	unsigned char digest[32];
	memset(digest, 0xa5, sizeof(digest));
	uint64_t workflow_slot = 0;
	double total_started = monotonic_seconds();
	double started = monotonic_seconds();
	failure_step = "agent_hello";
	for (uint64_t worker = 1; worker <= workers; worker++) {
		uint32_t worker_slot = (uint32_t)worker;
		uint64_t slot = 0;
		if (!vine_datavine_data_controller_agent_hello(controller,
				workflow_key, &worker_slot, worker + 1000,
				"127.0.0.1", (uint16_t)(10000 + worker % 50000), &slot) ||
				(slot != workflow_slot && workflow_slot))
			goto failed;
		workflow_slot = slot;
	}
	record_phase(&phases[phase_count++], "agent_hello", workers, started);

	started = monotonic_seconds();
	failure_step = "expect";
	for (uint64_t data_id = 1; data_id <= data_count; data_id++) {
		uint32_t generation = 1;
		if (!vine_datavine_data_controller_agent_expect(controller,
				workflow_id, data_id, &generation))
			goto failed;
	}
	record_phase(&phases[phase_count++], "expect", data_count, started);

	started = monotonic_seconds();
	failure_step = "wait";
	for (uint64_t data_id = 1; data_id <= data_count; data_id++) {
		uint32_t worker = (uint32_t)((data_id - 1) % workers + 1);
		if (!vine_datavine_data_controller_agent_wait(controller,
				workflow_slot, data_id, 1, worker, (uint64_t)worker + 1000,
				data_id, 0))
			goto failed;
	}
	record_phase(&phases[phase_count++], "wait", data_count, started);

	uint64_t publications = data_count * replicas_per_data;
	started = monotonic_seconds();
	failure_step = "publish";
	for (uint64_t replica_index = 0; replica_index < replicas_per_data;
			replica_index++) {
		for (uint64_t data_id = 1; data_id <= data_count; data_id++) {
			uint32_t worker = (uint32_t)(
					(data_id + replica_index - 1) % workers + 1);
			uint64_t object_token = replica_index * data_count + data_id;
			if (!vine_datavine_data_controller_agent_publish(controller,
					workflow_slot, data_id, 1, 4096, digest, worker,
					(uint64_t)worker + 1000, object_token))
				goto failed;
		}
	}
	record_phase(&phases[phase_count++], "publish", publications, started);

	started = monotonic_seconds();
	failure_step = "resolve_random";
	for (uint64_t index = 0; index < data_count; index++) {
		uint64_t data_id = random_data_id(index, data_count);
		struct vine_datavine_agent_replica replica;
		size_t count = 0;
		if (vine_datavine_data_controller_agent_resolve(controller,
				workflow_slot, data_id, 1, &replica, 1, &count, 0, 0, 0) !=
				VINE_DATAVINE_AGENT_AVAILABLE || count != 1 || !replica.port)
			goto failed;
	}
	record_phase(&phases[phase_count++], "resolve_random", data_count, started);

	if (disconnect) {
		started = monotonic_seconds();
		failure_step = "session_lost";
		for (uint64_t worker = 1; worker <= workers; worker++) {
			if (!vine_datavine_data_controller_agent_session_lost(controller,
					workflow_slot, (uint32_t)worker, worker + 1000))
				goto failed;
		}
		record_phase(&phases[phase_count++], "session_lost",
				publications, started);
	}

	started = monotonic_seconds();
	failure_step = "mark_dead";
	for (uint64_t data_id = 1; data_id <= data_count; data_id++) {
		if (!vine_datavine_data_controller_agent_mark_dead(
				controller, workflow_slot, data_id, 1))
			goto failed;
	}
	record_phase(&phases[phase_count++], "mark_dead", data_count, started);

	uint64_t releases_taken = 0;
	if (!disconnect) {
		started = monotonic_seconds();
		failure_step = "take_releases";
		struct vine_datavine_agent_release releases[256];
		for (uint64_t worker = 1; worker <= workers; worker++) {
			uint64_t acknowledged = 0;
			for (;;) {
				size_t count = 0;
				if (!vine_datavine_data_controller_agent_take_releases(controller,
						workflow_slot, (uint32_t)worker, worker + 1000,
						acknowledged, releases, 256, &count))
					goto failed;
				if (!count)
					break;
				acknowledged = releases[count - 1].sequence;
				releases_taken += count;
			}
		}
		record_phase(&phases[phase_count++], "take_releases",
				releases_taken, started);
		if (releases_taken != publications)
			goto failed;
	}

	struct vine_datavine_agent_stats stats;
	failure_step = "final_check";
	if (!vine_datavine_data_controller_agent_stats(
			controller, workflow_slot, &stats) || stats.active_data ||
			stats.active_replicas || stats.active_waiters ||
			!vine_datavine_data_controller_agent_check(controller, workflow_slot))
		goto failed;
	struct rusage usage;
	memset(&usage, 0, sizeof(usage));
	getrusage(RUSAGE_SELF, &usage);
	double total_seconds = monotonic_seconds() - total_started;
	printf("{\"status\":\"PASS\",\"scenario\":\"%s\","
			"\"data\":%" PRIu64 ",\"workers\":%" PRIu64 ","
			"\"replicas_per_data\":%" PRIu64 ","
			"\"total_seconds\":%.9f,\"max_rss_kib\":%ld,"
			"\"peak_data\":%" PRIu64 ",\"peak_replicas\":%" PRIu64 ","
			"\"peak_waiters\":%" PRIu64 ",\"phases\":{",
			disconnect ? "disconnect" : "lifecycle", data_count, workers,
			replicas_per_data, total_seconds, usage.ru_maxrss, stats.peak_data,
			stats.peak_replicas, stats.peak_waiters);
	for (size_t index = 0; index < phase_count; index++) {
		struct phase *phase = &phases[index];
		printf("%s\"%s\":{\"operations\":%" PRIu64
				",\"seconds\":%.9f,\"operations_per_second\":%.3f}",
				index ? "," : "", phase->name, phase->operations,
				phase->seconds, phase->seconds > 0
					? (double)phase->operations / phase->seconds : 0.0);
	}
	puts("}}");
	vine_datavine_data_controller_close(controller);
	vine_datavine_journal_close(journal);
	return EXIT_SUCCESS;

failed:
	fprintf(stderr, "controller benchmark invariant failed at %s\n",
			failure_step);
	vine_datavine_data_controller_close(controller);
	vine_datavine_journal_close(journal);
	return EXIT_FAILURE;
}
