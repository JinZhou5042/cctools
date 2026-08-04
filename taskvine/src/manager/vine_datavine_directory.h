#ifndef VINE_DATAVINE_DIRECTORY_H
#define VINE_DATAVINE_DIRECTORY_H

#include <stddef.h>
#include <stdint.h>

#define VINE_DATAVINE_WORKER_ID_MAX 255
#define VINE_DATAVINE_ENDPOINT_MAX 1023
#define VINE_DATAVINE_REPLICA_ID_MAX 255
#define VINE_DATAVINE_TRANSFER_ID_MAX 127

enum vine_datavine_replica_tier {
	VINE_DATAVINE_WORKER_DRAM = 1,
	VINE_DATAVINE_WORKER_DISK = 2,
};

struct vine_datavine_directory;

struct vine_datavine_worker_record {
	uint64_t epoch;
	int active;
	char worker_id[VINE_DATAVINE_WORKER_ID_MAX + 1];
	char endpoint[VINE_DATAVINE_ENDPOINT_MAX + 1];
};

struct vine_datavine_replica_record {
	char kind;
	int64_t data_id;
	uint64_t generation;
	int32_t attempt;
	int32_t tier;
	int64_t size;
	uint32_t active_leases;
	char content_hash[65];
	char replica_id[VINE_DATAVINE_REPLICA_ID_MAX + 1];
	char worker_id[VINE_DATAVINE_WORKER_ID_MAX + 1];
	uint64_t worker_epoch;
};

struct vine_datavine_source_record {
	struct vine_datavine_replica_record replica;
	char endpoint[VINE_DATAVINE_ENDPOINT_MAX + 1];
	char transfer_id[VINE_DATAVINE_TRANSFER_ID_MAX + 1];
};

struct vine_datavine_replica_snapshot {
	struct vine_datavine_replica_record replica;
	int state;
	char endpoint[VINE_DATAVINE_ENDPOINT_MAX + 1];
};

struct vine_datavine_directory_metrics {
	uint64_t workers;
	uint64_t replicas;
	uint64_t active_leases;
	uint64_t source_selections;
	uint64_t source_misses;
	uint64_t releases;
	uint64_t release_failures;
	uint64_t idempotent_releases;
	uint64_t invalidations;
	uint64_t restorations;
	uint64_t prunes;
	uint64_t stale_rejections;
};

struct vine_datavine_directory *vine_datavine_directory_create(
		uint64_t maximum_workers, uint64_t maximum_replicas,
		uint64_t maximum_active_leases, uint64_t maximum_completed_leases,
		int shards);
void vine_datavine_directory_delete(struct vine_datavine_directory *directory);

int vine_datavine_directory_claim_worker(struct vine_datavine_directory *directory,
		const char *worker_id, const char *endpoint,
		struct vine_datavine_worker_record *result);
int vine_datavine_directory_disconnect_worker(struct vine_datavine_directory *directory,
		const char *worker_id, uint64_t epoch);
int vine_datavine_directory_publish_replica(struct vine_datavine_directory *directory,
		char kind, int64_t data_id, const char *replica_id, int32_t attempt,
		int32_t tier, const char *content_hash, int64_t size,
		const char *worker_id, uint64_t worker_epoch, const char *endpoint,
		struct vine_datavine_replica_record *result);
int vine_datavine_directory_resolve_source(struct vine_datavine_directory *directory,
		char kind, int64_t data_id, const char *destination_worker_id,
		uint64_t destination_worker_epoch, const char *transfer_id,
		const char *excluded_worker_id, struct vine_datavine_source_record *result);
int vine_datavine_directory_release_source(struct vine_datavine_directory *directory,
		const char *transfer_id, int success);
int vine_datavine_directory_invalidate_replica(
		struct vine_datavine_directory *directory, char kind, int64_t data_id,
		const char *replica_id);
int vine_datavine_directory_restore_replica(
		struct vine_datavine_directory *directory, char kind, int64_t data_id,
		const char *replica_id);
int vine_datavine_directory_confirm_replica_pruned(
		struct vine_datavine_directory *directory, char kind, int64_t data_id,
		const char *replica_id);
int64_t vine_datavine_directory_replica_active_leases(
		struct vine_datavine_directory *directory, char kind, int64_t data_id,
		const char *replica_id);
int vine_datavine_directory_snapshot_replicas(
		struct vine_datavine_directory *directory, char kind, int64_t data_id,
		struct vine_datavine_replica_snapshot **result, size_t *count);
void vine_datavine_directory_get_metrics(struct vine_datavine_directory *directory,
		struct vine_datavine_directory_metrics *result);

#endif
