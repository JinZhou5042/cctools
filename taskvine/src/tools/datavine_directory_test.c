#include "vine_datavine_directory.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define STRESS_THREADS 8
#define STRESS_RECORDS_PER_THREAD 10000

struct stress_args {
	struct vine_datavine_directory *directory;
	int thread;
	int failed;
};

static void *hot_source_stress(void *arg)
{
	struct stress_args *worker = arg;
	char destination[32];
	snprintf(destination, sizeof(destination), "destination-%d", worker->thread);
	for (int i = 0; i < STRESS_RECORDS_PER_THREAD; i++) {
		char transfer_id[64];
		snprintf(transfer_id, sizeof(transfer_id), "taskvine:hot-%d-%d", worker->thread, i);
		struct vine_datavine_source_record source;
		int resolved = vine_datavine_directory_resolve_source(worker->directory, 'i', 1, destination, 1, transfer_id, 0, &source);
		if (!resolved || strcmp(source.replica.worker_id, "hot-source") ||
				!vine_datavine_directory_release_source(worker->directory, transfer_id, 1)) {
			worker->failed = 1;
			break;
		}
	}
	return 0;
}

static double monotonic_seconds(void)
{
	struct timespec value;
	clock_gettime(CLOCK_MONOTONIC, &value);
	return value.tv_sec + value.tv_nsec / 1000000000.0;
}

static void *stress(void *arg)
{
	struct stress_args *worker = arg;
	char worker_id[32];
	snprintf(worker_id, sizeof(worker_id), "source-%d", worker->thread);
	for (int i = 0; i < STRESS_RECORDS_PER_THREAD; i++) {
		int64_t data_id = (int64_t)worker->thread * STRESS_RECORDS_PER_THREAD + i + 1;
		char replica_id[64];
		char transfer_id[64];
		snprintf(replica_id, sizeof(replica_id), "r-%lld", (long long)data_id);
		snprintf(transfer_id, sizeof(transfer_id), "taskvine:t-%lld", (long long)data_id);
		struct vine_datavine_replica_record replica;
		struct vine_datavine_source_record source;
		if (!vine_datavine_directory_publish_replica(worker->directory, 'i', data_id, replica_id, 1, VINE_DATAVINE_WORKER_DRAM, "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881", 1, worker_id, 1, "http://127.0.0.1:1", &replica) || !vine_datavine_directory_resolve_source(worker->directory, 'i', data_id, "destination", 1, transfer_id, 0, &source) || strcmp(source.replica.worker_id, worker_id) || !vine_datavine_directory_release_source(worker->directory, transfer_id, 1)) {
			worker->failed = 1;
			break;
		}
	}
	return 0;
}

static int claim(struct vine_datavine_directory *directory, const char *worker, int epoch)
{
	struct vine_datavine_worker_record record;
	return vine_datavine_directory_claim_worker(directory, worker, "http://127.0.0.1:1", &record) && record.epoch == (uint64_t)epoch && record.active;
}

static int publish(struct vine_datavine_directory *directory, const char *worker,
		uint64_t epoch, const char *replica, int attempt, int tier,
		struct vine_datavine_replica_record *record)
{
	return vine_datavine_directory_publish_replica(directory, 'i', 1, replica, attempt, tier, "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881", 1, worker, epoch, "http://127.0.0.1:1", record);
}

int main(void)
{
	int failed = 0;
	struct vine_datavine_directory *directory = vine_datavine_directory_create(10, 100, 10, 100, 32);
	failed |= !directory;
	failed |= !claim(directory, "w1", 1);
	failed |= !claim(directory, "w2", 1);
	failed |= !claim(directory, "w3", 1);
	struct vine_datavine_replica_record first;
	struct vine_datavine_replica_record second;
	failed |= !publish(directory, "w1", 1, "r1", 1, VINE_DATAVINE_WORKER_DISK, &first);
	failed |= !publish(directory, "w2", 1, "r2", 1, VINE_DATAVINE_WORKER_DRAM, &second);
	failed |= first.generation != 1 || second.generation != 1;
	struct vine_datavine_source_record source1;
	struct vine_datavine_source_record source2;
	failed |= !vine_datavine_directory_resolve_source(directory, 'i', 1, "w3", 1, "taskvine:t1", 0, &source1);
	failed |= strcmp(source1.replica.worker_id, "w2");
	struct vine_datavine_replica_snapshot *snapshot = 0;
	size_t snapshot_count = 0;
	failed |= !vine_datavine_directory_snapshot_replicas(
			directory, 'i', 1, &snapshot, &snapshot_count);
	failed |= snapshot_count != 2;
	int snapshot_lease_found = 0;
	for (size_t i = 0; i < snapshot_count; i++) {
		if (!strcmp(snapshot[i].replica.replica_id, "r2") &&
				snapshot[i].replica.active_leases == 1 && snapshot[i].state == 1) {
			snapshot_lease_found = 1;
		}
	}
	failed |= !snapshot_lease_found;
	free(snapshot);
	failed |= vine_datavine_directory_replica_active_leases(
				  directory, 'i', 1, "r2") != 1;
	struct vine_datavine_source_record duplicate;
	failed |= !vine_datavine_directory_resolve_source(directory, 'i', 1, "w3", 1, "taskvine:t1", 0, &duplicate);
	failed |= strcmp(duplicate.replica.replica_id, source1.replica.replica_id);
	failed |= !vine_datavine_directory_resolve_source(directory, 'i', 1, "w3", 1, "taskvine:t2", 0, &source2);
	failed |= strcmp(source2.replica.worker_id, "w1");
	failed |= !vine_datavine_directory_release_source(directory, "taskvine:t2", 1);
	failed |= vine_datavine_directory_replica_active_leases(
				  directory, 'i', 1, "r1") != 0;
	failed |= !vine_datavine_directory_release_source(directory, "taskvine:t2", 1);
	failed |= vine_datavine_directory_release_source(directory, "taskvine:t2", 0) != -1;
	failed |= !vine_datavine_directory_disconnect_worker(directory, "w2", 1);
	failed |= !vine_datavine_directory_disconnect_worker(directory, "w2", 1);
	failed |= vine_datavine_directory_replica_active_leases(
				  directory, 'i', 1, "r2") != 0;
	failed |= !vine_datavine_directory_release_source(directory, "taskvine:t1", 0);
	failed |= !claim(directory, "w2", 2);
	failed |= vine_datavine_directory_disconnect_worker(directory, "w2", 1);
	struct vine_datavine_replica_record replacement;
	failed |= publish(directory, "w2", 1, "r2", 1, VINE_DATAVINE_WORKER_DRAM, &replacement);
	failed |= !publish(directory, "w2", 2, "r2", 1, VINE_DATAVINE_WORKER_DRAM, &replacement);
	failed |= replacement.generation != 2;
	failed |= vine_datavine_directory_invalidate_replica(directory, 'i', 1, "r2") != 1;
	failed |= vine_datavine_directory_invalidate_replica(directory, 'i', 1, "r2") != 0;
	failed |= vine_datavine_directory_restore_replica(directory, 'i', 1, "r2") != 1;
	failed |= vine_datavine_directory_invalidate_replica(directory, 'i', 1, "r2") != 1;
	failed |= vine_datavine_directory_confirm_replica_pruned(directory, 'i', 1, "r2") != 1;
	failed |= vine_datavine_directory_restore_replica(directory, 'i', 1, "r2") != -1;
	failed |= !vine_datavine_directory_resolve_source(directory, 'i', 1, "w3", 1, "taskvine:t3", 0, &source2);
	failed |= strcmp(source2.replica.worker_id, "w1");
	failed |= !vine_datavine_directory_release_source(directory, "taskvine:t3", 1);
	struct vine_datavine_directory_metrics metrics;
	vine_datavine_directory_get_metrics(directory, &metrics);
	failed |= metrics.workers != 3 || metrics.replicas != 3 || metrics.active_leases != 0;
	failed |= metrics.source_selections != 3 || metrics.source_misses != 0;
	failed |= metrics.invalidations != 2 || metrics.restorations != 1 || metrics.prunes != 1;
	failed |= metrics.stale_rejections < 1;
	printf("workers=%llu replicas=%llu leases=%llu selections=%llu stale=%llu\n",
			(unsigned long long)metrics.workers,
			(unsigned long long)metrics.replicas,
			(unsigned long long)metrics.active_leases,
			(unsigned long long)metrics.source_selections,
			(unsigned long long)metrics.stale_rejections);
	vine_datavine_directory_delete(directory);
	directory = vine_datavine_directory_create(
			STRESS_THREADS + 1,
			STRESS_THREADS * STRESS_RECORDS_PER_THREAD,
			STRESS_THREADS * 2,
			1024,
			256);
	failed |= !directory;
	failed |= !claim(directory, "destination", 1);
	for (int i = 0; i < STRESS_THREADS; i++) {
		char worker_id[32];
		snprintf(worker_id, sizeof(worker_id), "source-%d", i);
		failed |= !claim(directory, worker_id, 1);
	}
	pthread_t threads[STRESS_THREADS];
	struct stress_args arguments[STRESS_THREADS];
	double started = monotonic_seconds();
	for (int i = 0; i < STRESS_THREADS; i++) {
		arguments[i] = (struct stress_args){directory, i, 0};
		pthread_create(&threads[i], 0, stress, &arguments[i]);
	}
	for (int i = 0; i < STRESS_THREADS; i++) {
		pthread_join(threads[i], 0);
		failed |= arguments[i].failed;
	}
	double elapsed = monotonic_seconds() - started;
	vine_datavine_directory_get_metrics(directory, &metrics);
	uint64_t operations = STRESS_THREADS * STRESS_RECORDS_PER_THREAD;
	failed |= metrics.replicas != operations || metrics.active_leases != 0;
	printf("concurrent_records=%llu seconds=%.6f records_per_second=%.0f\n",
			(unsigned long long)operations,
			elapsed,
			operations / elapsed);
	vine_datavine_directory_delete(directory);
	directory = vine_datavine_directory_create(
			STRESS_THREADS + 1, 1, STRESS_THREADS * 2, 1024, 256);
	failed |= !directory;
	failed |= !claim(directory, "hot-source", 1);
	struct vine_datavine_replica_record hot_replica;
	failed |= !publish(directory, "hot-source", 1, "hot-replica", 1, VINE_DATAVINE_WORKER_DRAM, &hot_replica);
	for (int i = 0; i < STRESS_THREADS; i++) {
		char destination[32];
		snprintf(destination, sizeof(destination), "destination-%d", i);
		failed |= !claim(directory, destination, 1);
		arguments[i] = (struct stress_args){directory, i, 0};
	}
	started = monotonic_seconds();
	for (int i = 0; i < STRESS_THREADS; i++) {
		pthread_create(&threads[i], 0, hot_source_stress, &arguments[i]);
	}
	for (int i = 0; i < STRESS_THREADS; i++) {
		pthread_join(threads[i], 0);
		failed |= arguments[i].failed;
	}
	elapsed = monotonic_seconds() - started;
	vine_datavine_directory_get_metrics(directory, &metrics);
	failed |= metrics.active_leases != 0;
	printf("hot_source_records=%llu seconds=%.6f records_per_second=%.0f\n",
			(unsigned long long)operations,
			elapsed,
			operations / elapsed);
	vine_datavine_directory_delete(directory);
	return failed ? 1 : 0;
}
