/*
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_datavine_directory.h"

#include "hash_table.h"
#include "itable.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

struct worker {
	struct vine_datavine_worker_record record;
	const char *endpoint;
	atomic_uint_fast64_t epoch;
	atomic_int active;
};

struct replica {
	struct vine_datavine_replica_record record;
	const char *endpoint;
	struct worker *worker;
	atomic_uint_fast64_t active_leases;
	int available;
	int pruned;
	struct replica *next;
};

struct data_entry {
	int32_t latest_attempt;
	struct replica *replicas;
};

struct worker_shard {
	pthread_mutex_t lock;
	struct hash_table *workers;
};

struct lease {
	struct replica *replica;
	int success;
	int completed;
	char transfer_id[VINE_DATAVINE_TRANSFER_ID_MAX + 1];
	char destination_worker_id[VINE_DATAVINE_WORKER_ID_MAX + 1];
	uint64_t destination_worker_epoch;
	struct lease *completed_previous;
	struct lease *completed_next;
};

struct directory_shard {
	pthread_rwlock_t lock;
	struct itable *data;
};

struct lease_shard {
	pthread_mutex_t lock;
	struct hash_table *leases;
	uint64_t completed_leases;
	uint64_t maximum_completed_leases;
	struct lease *completed_head;
	struct lease *completed_tail;
};

struct vine_datavine_directory {
	pthread_mutex_t endpoints_lock;
	int endpoints_lock_initialized;
	struct hash_table *endpoints;
	int shard_count;
	struct worker_shard *worker_shards;
	int initialized_worker_shards;
	struct directory_shard *shards;
	int initialized_shards;
	struct lease_shard *lease_shards;
	int initialized_lease_shards;
	uint64_t maximum_workers;
	uint64_t maximum_replicas;
	uint64_t maximum_active_leases;
	struct vine_datavine_directory_metrics metrics;
};

static int valid_text(const char *value, size_t maximum)
{
	return value && value[0] && strlen(value) <= maximum;
}

static int valid_hash(const char *hash)
{
	if (!hash || strlen(hash) != 64) {
		return 0;
	}
	for (int i = 0; i < 64; i++) {
		if (!((hash[i] >= '0' && hash[i] <= '9') || (hash[i] >= 'a' && hash[i] <= 'f'))) {
			return 0;
		}
	}
	return 1;
}

static uint64_t data_key(char kind, int64_t data_id)
{
	return (uint64_t)data_id | (kind == 'i' ? UINT64_C(1) << 63 : 0);
}

static struct directory_shard *get_shard(struct vine_datavine_directory *directory, uint64_t key)
{
	uint64_t value = key * UINT64_C(11400714819323198485);
	return &directory->shards[value % (uint64_t)directory->shard_count];
}

static struct lease_shard *get_lease_shard(
		struct vine_datavine_directory *directory, const char *transfer_id)
{
	uint64_t hash = UINT64_C(1469598103934665603);
	for (const unsigned char *cursor = (const unsigned char *)transfer_id; *cursor; cursor++) {
		hash = (hash ^ *cursor) * UINT64_C(1099511628211);
	}
	return &directory->lease_shards[hash % (uint64_t)directory->shard_count];
}

static struct worker_shard *get_worker_shard(
		struct vine_datavine_directory *directory, const char *worker_id)
{
	uint64_t hash = UINT64_C(1469598103934665603);
	for (const unsigned char *cursor = (const unsigned char *)worker_id; *cursor; cursor++) {
		hash = (hash ^ *cursor) * UINT64_C(1099511628211);
	}
	return &directory->worker_shards[hash % (uint64_t)directory->shard_count];
}

static struct worker *active_worker(struct vine_datavine_directory *directory,
		const char *worker_id, uint64_t epoch, const char *endpoint,
		const char **stored_endpoint)
{
	struct worker_shard *shard = get_worker_shard(directory, worker_id);
	pthread_mutex_lock(&shard->lock);
	struct worker *worker = hash_table_lookup(shard->workers, worker_id);
	if (!worker || !atomic_load(&worker->active) || atomic_load(&worker->epoch) != epoch || (endpoint && strcmp(worker->endpoint, endpoint))) {
		worker = 0;
	}
	if (worker && stored_endpoint) {
		*stored_endpoint = worker->endpoint;
	}
	pthread_mutex_unlock(&shard->lock);
	return worker;
}

static const char *intern_endpoint_locked(struct vine_datavine_directory *directory, const char *endpoint)
{
	char *stored = hash_table_lookup(directory->endpoints, endpoint);
	if (!stored) {
		stored = strdup(endpoint);
		if (!stored || !hash_table_insert(directory->endpoints, endpoint, stored)) {
			free(stored);
			stored = 0;
		}
	}
	return stored;
}

static void replica_list_delete(struct replica *replica)
{
	while (replica) {
		struct replica *next = replica->next;
		free(replica);
		replica = next;
	}
}

static void data_entry_delete(void *value)
{
	struct data_entry *entry = value;
	replica_list_delete(entry->replicas);
	free(entry);
}

static int complete_lease_locked(struct vine_datavine_directory *directory,
		struct lease_shard *leases, struct lease *lease, int success)
{
	if (lease->completed) {
		int matches = lease->success == !!success;
		if (matches) {
			__sync_fetch_and_add(&directory->metrics.idempotent_releases, 1);
		}
		return matches ? 1 : -1;
	}
	uint_fast64_t active = atomic_load(&lease->replica->active_leases);
	while (active && !atomic_compare_exchange_weak(
					 &lease->replica->active_leases, &active, active - 1)) {
	}
	if (!active) {
		return 0;
	}
	lease->completed = 1;
	lease->success = !!success;
	__sync_fetch_and_add(&directory->metrics.releases, 1);
	if (!success) {
		__sync_fetch_and_add(&directory->metrics.release_failures, 1);
	}
	__sync_fetch_and_sub(&directory->metrics.active_leases, 1);
	lease->completed_previous = leases->completed_tail;
	if (leases->completed_tail) {
		leases->completed_tail->completed_next = lease;
	} else {
		leases->completed_head = lease;
	}
	leases->completed_tail = lease;
	leases->completed_leases++;
	return 1;
}

static void evict_completed_leases_locked(struct lease_shard *leases)
{
	while (leases->completed_leases > leases->maximum_completed_leases) {
		struct lease *lease = leases->completed_head;
		leases->completed_head = lease->completed_next;
		if (leases->completed_head) {
			leases->completed_head->completed_previous = 0;
		} else {
			leases->completed_tail = 0;
		}
		leases->completed_leases--;
		hash_table_remove(leases->leases, lease->transfer_id);
		free(lease);
	}
}

struct vine_datavine_directory *vine_datavine_directory_create(
		uint64_t maximum_workers, uint64_t maximum_replicas,
		uint64_t maximum_active_leases, uint64_t maximum_completed_leases,
		int shards)
{
	if (!maximum_workers || !maximum_replicas || !maximum_active_leases || !maximum_completed_leases || shards < 1) {
		return 0;
	}
	struct vine_datavine_directory *directory = calloc(1, sizeof(*directory));
	if (!directory) {
		return 0;
	}
	if (pthread_mutex_init(&directory->endpoints_lock, 0)) {
		free(directory);
		return 0;
	}
	directory->endpoints_lock_initialized = 1;
	directory->endpoints = hash_table_create(0, 0);
	directory->worker_shards = calloc((size_t)shards, sizeof(*directory->worker_shards));
	directory->shards = calloc((size_t)shards, sizeof(*directory->shards));
	directory->lease_shards = calloc((size_t)shards, sizeof(*directory->lease_shards));
	directory->shard_count = shards;
	directory->maximum_workers = maximum_workers;
	directory->maximum_replicas = maximum_replicas;
	directory->maximum_active_leases = maximum_active_leases;
	if (!directory->endpoints || !directory->worker_shards || !directory->shards || !directory->lease_shards) {
		vine_datavine_directory_delete(directory);
		return 0;
	}
	for (int i = 0; i < shards; i++) {
		directory->worker_shards[i].workers = hash_table_create(0, 0);
		if (!directory->worker_shards[i].workers || pthread_mutex_init(&directory->worker_shards[i].lock, 0)) {
			if (directory->worker_shards[i].workers) {
				hash_table_delete(directory->worker_shards[i].workers);
				directory->worker_shards[i].workers = 0;
			}
			vine_datavine_directory_delete(directory);
			return 0;
		}
		directory->initialized_worker_shards++;
		directory->shards[i].data = itable_create(0);
		if (!directory->shards[i].data) {
			vine_datavine_directory_delete(directory);
			return 0;
		}
		if (pthread_rwlock_init(&directory->shards[i].lock, 0)) {
			itable_delete(directory->shards[i].data);
			directory->shards[i].data = 0;
			vine_datavine_directory_delete(directory);
			return 0;
		}
		directory->initialized_shards++;
		directory->lease_shards[i].leases = hash_table_create(0, 0);
		if (!directory->lease_shards[i].leases || pthread_mutex_init(&directory->lease_shards[i].lock, 0)) {
			if (directory->lease_shards[i].leases) {
				hash_table_delete(directory->lease_shards[i].leases);
				directory->lease_shards[i].leases = 0;
			}
			vine_datavine_directory_delete(directory);
			return 0;
		}
		directory->lease_shards[i].maximum_completed_leases =
				maximum_completed_leases / (uint64_t)shards + ((uint64_t)i < maximum_completed_leases % (uint64_t)shards);
		directory->initialized_lease_shards++;
	}
	return directory;
}

void vine_datavine_directory_delete(struct vine_datavine_directory *directory)
{
	if (!directory) {
		return;
	}
	for (int i = 0; i < directory->initialized_worker_shards; i++) {
		void *value;
		while ((value = hash_table_pop(directory->worker_shards[i].workers))) {
			free(value);
		}
		hash_table_delete(directory->worker_shards[i].workers);
		pthread_mutex_destroy(&directory->worker_shards[i].lock);
	}
	if (directory->endpoints) {
		hash_table_clear(directory->endpoints, free);
		hash_table_delete(directory->endpoints);
	}
	for (int i = 0; i < directory->initialized_lease_shards; i++) {
		void *value;
		while ((value = hash_table_pop(directory->lease_shards[i].leases))) {
			free(value);
		}
		hash_table_delete(directory->lease_shards[i].leases);
		pthread_mutex_destroy(&directory->lease_shards[i].lock);
	}
	for (int i = 0; i < directory->initialized_shards; i++) {
		if (directory->shards[i].data) {
			itable_clear(directory->shards[i].data, data_entry_delete);
			itable_delete(directory->shards[i].data);
		}
		pthread_rwlock_destroy(&directory->shards[i].lock);
	}
	if (directory->endpoints_lock_initialized) {
		pthread_mutex_destroy(&directory->endpoints_lock);
	}
	free(directory->worker_shards);
	free(directory->shards);
	free(directory->lease_shards);
	free(directory);
}

int vine_datavine_directory_claim_worker(struct vine_datavine_directory *directory,
		const char *worker_id, const char *endpoint, struct vine_datavine_worker_record *result)
{
	if (!directory || !result || !valid_text(worker_id, VINE_DATAVINE_WORKER_ID_MAX) || !valid_text(endpoint, VINE_DATAVINE_ENDPOINT_MAX)) {
		return 0;
	}
	struct worker_shard *shard = get_worker_shard(directory, worker_id);
	pthread_mutex_lock(&shard->lock);
	struct worker *worker = hash_table_lookup(shard->workers, worker_id);
	if (worker && atomic_load(&worker->active) && strcmp(worker->record.endpoint, endpoint)) {
		pthread_mutex_unlock(&shard->lock);
		return 0;
	}
	pthread_mutex_lock(&directory->endpoints_lock);
	const char *stored_endpoint = intern_endpoint_locked(directory, endpoint);
	pthread_mutex_unlock(&directory->endpoints_lock);
	if (!stored_endpoint) {
		pthread_mutex_unlock(&shard->lock);
		return 0;
	}
	if (!worker) {
		if (__sync_add_and_fetch(&directory->metrics.workers, 1) > directory->maximum_workers) {
			__sync_fetch_and_sub(&directory->metrics.workers, 1);
			pthread_mutex_unlock(&shard->lock);
			return 0;
		}
		worker = calloc(1, sizeof(*worker));
		if (!worker || !hash_table_insert(shard->workers, worker_id, worker)) {
			free(worker);
			__sync_fetch_and_sub(&directory->metrics.workers, 1);
			pthread_mutex_unlock(&shard->lock);
			return 0;
		}
		worker->record.epoch = 1;
		atomic_store(&worker->epoch, 1);
	} else if (!atomic_load(&worker->active)) {
		worker->record.epoch++;
		atomic_store(&worker->epoch, worker->record.epoch);
	}
	worker->record.active = 1;
	atomic_store(&worker->active, 1);
	strcpy(worker->record.worker_id, worker_id);
	strcpy(worker->record.endpoint, endpoint);
	worker->endpoint = stored_endpoint;
	*result = worker->record;
	pthread_mutex_unlock(&shard->lock);
	return 1;
}

int vine_datavine_directory_disconnect_worker(struct vine_datavine_directory *directory,
		const char *worker_id, uint64_t epoch)
{
	if (!directory || !valid_text(worker_id, VINE_DATAVINE_WORKER_ID_MAX) || !epoch) {
		return 0;
	}
	struct worker_shard *shard = get_worker_shard(directory, worker_id);
	pthread_mutex_lock(&shard->lock);
	struct worker *worker = hash_table_lookup(shard->workers, worker_id);
	if (worker && !atomic_load(&worker->active)) {
		pthread_mutex_unlock(&shard->lock);
		return 1;
	}
	if (worker && atomic_load(&worker->epoch) != epoch) {
		worker = 0;
	}
	if (!worker) {
		__sync_fetch_and_add(&directory->metrics.stale_rejections, 1);
		pthread_mutex_unlock(&shard->lock);
		return 0;
	}
	worker->record.active = 0;
	atomic_store(&worker->active, 0);
	pthread_mutex_unlock(&shard->lock);
	for (int i = 0; i < directory->shard_count; i++) {
		struct lease_shard *leases = &directory->lease_shards[i];
		pthread_mutex_lock(&leases->lock);
		char *key;
		struct lease *lease;
		int iteration;
		HASH_TABLE_ITERATE(leases->leases, iteration, key, lease)
		{
			if (lease->completed) {
				continue;
			}
			int source_matches = !strcmp(lease->replica->record.worker_id, worker_id) && lease->replica->record.worker_epoch == epoch;
			int destination_matches = !strcmp(lease->destination_worker_id, worker_id) && lease->destination_worker_epoch == epoch;
			if (source_matches || destination_matches) {
				complete_lease_locked(directory, leases, lease, 0);
			}
		}
		evict_completed_leases_locked(leases);
		pthread_mutex_unlock(&leases->lock);
	}
	return 1;
}

int vine_datavine_directory_publish_replica(struct vine_datavine_directory *directory,
		char kind, int64_t data_id, const char *replica_id, int32_t attempt,
		int32_t tier, const char *content_hash, int64_t size,
		const char *worker_id, uint64_t worker_epoch, const char *endpoint,
		struct vine_datavine_replica_record *result)
{
	if (!directory || !result || (kind != 'e' && kind != 'i') || data_id < 1 || !valid_text(replica_id, VINE_DATAVINE_REPLICA_ID_MAX) || attempt < 1 || (tier != VINE_DATAVINE_WORKER_DRAM && tier != VINE_DATAVINE_WORKER_DISK) || !valid_hash(content_hash) || size < 0 || !valid_text(worker_id, VINE_DATAVINE_WORKER_ID_MAX) || !worker_epoch || !valid_text(endpoint, VINE_DATAVINE_ENDPOINT_MAX)) {
		return 0;
	}
	const char *stored_endpoint = 0;
	struct worker *source_worker = active_worker(
			directory, worker_id, worker_epoch, endpoint, &stored_endpoint);
	if (!source_worker) {
		__sync_fetch_and_add(&directory->metrics.stale_rejections, 1);
		return 0;
	}
	uint64_t key = data_key(kind, data_id);
	struct directory_shard *shard = get_shard(directory, key);
	pthread_rwlock_wrlock(&shard->lock);
	struct data_entry *entry = itable_lookup(shard->data, key);
	if (!entry) {
		entry = calloc(1, sizeof(*entry));
		if (!entry || !itable_insert(shard->data, key, entry)) {
			free(entry);
			pthread_rwlock_unlock(&shard->lock);
			return 0;
		}
	}
	if (attempt < entry->latest_attempt) {
		__sync_fetch_and_add(&directory->metrics.stale_rejections, 1);
		pthread_rwlock_unlock(&shard->lock);
		return 0;
	}
	if (attempt > entry->latest_attempt) {
		entry->latest_attempt = attempt;
		for (struct replica *item = entry->replicas; item; item = item->next) {
			if (item->record.attempt < attempt) {
				item->available = 0;
			}
		}
	}
	struct replica *replica = 0;
	uint64_t generation = 0;
	for (struct replica *item = entry->replicas; item; item = item->next) {
		if (strcmp(item->record.replica_id, replica_id)) {
			continue;
		}
		if (item->record.generation > generation) {
			generation = item->record.generation;
		}
		if (item->available && (!atomic_load(&item->worker->active) || atomic_load(&item->worker->epoch) != item->record.worker_epoch)) {
			item->available = 0;
		}
		if (item->available) {
			replica = item;
		}
	}
	if (replica && replica->available) {
		int matches = replica->record.attempt == attempt && replica->record.tier == tier && replica->record.size == size && !strcmp(replica->record.content_hash, content_hash) && !strcmp(replica->record.worker_id, worker_id) && replica->record.worker_epoch == worker_epoch && !strcmp(replica->endpoint, endpoint);
		if (!matches) {
			pthread_rwlock_unlock(&shard->lock);
			return 0;
		}
		*result = replica->record;
		result->active_leases = atomic_load(&replica->active_leases);
		pthread_rwlock_unlock(&shard->lock);
		return 1;
	}
	if (!replica) {
		if (__sync_add_and_fetch(&directory->metrics.replicas, 1) > directory->maximum_replicas) {
			__sync_fetch_and_sub(&directory->metrics.replicas, 1);
			pthread_rwlock_unlock(&shard->lock);
			return 0;
		}
		replica = calloc(1, sizeof(*replica));
		if (!replica) {
			__sync_fetch_and_sub(&directory->metrics.replicas, 1);
			pthread_rwlock_unlock(&shard->lock);
			return 0;
		}
		replica->next = entry->replicas;
		entry->replicas = replica;
	}
	replica->available = 1;
	replica->pruned = 0;
	replica->record.kind = kind;
	replica->record.data_id = data_id;
	replica->record.generation = generation + 1;
	replica->record.attempt = attempt;
	replica->record.tier = tier;
	replica->record.size = size;
	replica->record.active_leases = 0;
	atomic_store(&replica->active_leases, 0);
	strcpy(replica->record.content_hash, content_hash);
	strcpy(replica->record.replica_id, replica_id);
	strcpy(replica->record.worker_id, worker_id);
	replica->endpoint = stored_endpoint;
	replica->worker = source_worker;
	replica->record.worker_epoch = worker_epoch;
	*result = replica->record;
	result->active_leases = atomic_load(&replica->active_leases);
	pthread_rwlock_unlock(&shard->lock);
	return 1;
}

int vine_datavine_directory_resolve_source(struct vine_datavine_directory *directory,
		char kind, int64_t data_id, const char *destination_worker_id,
		uint64_t destination_worker_epoch, const char *transfer_id,
		const char *excluded_worker_id, struct vine_datavine_source_record *result)
{
	if (!directory || !result || (kind != 'e' && kind != 'i') || data_id < 1 || !valid_text(destination_worker_id, VINE_DATAVINE_WORKER_ID_MAX) || !destination_worker_epoch || !valid_text(transfer_id, VINE_DATAVINE_TRANSFER_ID_MAX) || (excluded_worker_id && strlen(excluded_worker_id) > VINE_DATAVINE_WORKER_ID_MAX)) {
		return -1;
	}
	if (!active_worker(directory, destination_worker_id, destination_worker_epoch, 0, 0)) {
		__sync_fetch_and_add(&directory->metrics.stale_rejections, 1);
		return -1;
	}
	struct lease_shard *leases = get_lease_shard(directory, transfer_id);
	pthread_mutex_lock(&leases->lock);
	struct lease *lease = hash_table_lookup(leases->leases, transfer_id);
	if (lease) {
		int matches = !lease->completed && lease->replica->record.kind == kind && lease->replica->record.data_id == data_id && !strcmp(lease->destination_worker_id, destination_worker_id) && lease->destination_worker_epoch == destination_worker_epoch;
		if (!matches) {
			pthread_mutex_unlock(&leases->lock);
			return -1;
		}
		if (!atomic_load(&lease->replica->worker->active) || atomic_load(&lease->replica->worker->epoch) != lease->replica->record.worker_epoch) {
			pthread_mutex_unlock(&leases->lock);
			return -1;
		}
		result->replica = lease->replica->record;
		result->replica.active_leases = atomic_load(
				&lease->replica->active_leases);
		strcpy(result->endpoint, lease->replica->endpoint);
		strcpy(result->transfer_id, transfer_id);
		pthread_mutex_unlock(&leases->lock);
		return 1;
	}
	if (__sync_add_and_fetch(&directory->metrics.active_leases, 1) > directory->maximum_active_leases) {
		__sync_fetch_and_sub(&directory->metrics.active_leases, 1);
		pthread_mutex_unlock(&leases->lock);
		return -1;
	}
	uint64_t key = data_key(kind, data_id);
	struct directory_shard *shard = get_shard(directory, key);
	pthread_rwlock_rdlock(&shard->lock);
	struct data_entry *entry = itable_lookup(shard->data, key);
	struct replica *best = 0;
	if (entry) {
		for (struct replica *item = entry->replicas; item; item = item->next) {
			if (!item->available || item->record.attempt != entry->latest_attempt || !atomic_load(&item->worker->active) || atomic_load(&item->worker->epoch) != item->record.worker_epoch || !strcmp(item->record.worker_id, destination_worker_id) || (excluded_worker_id && !strcmp(item->record.worker_id, excluded_worker_id))) {
				continue;
			}
			uint64_t item_leases = atomic_load(&item->active_leases);
			uint64_t best_leases = best ? atomic_load(&best->active_leases) : 0;
			if (!best || item_leases < best_leases || (item_leases == best_leases && item->record.tier < best->record.tier) || (item_leases == best_leases && item->record.tier == best->record.tier && strcmp(item->record.replica_id, best->record.replica_id) < 0)) {
				best = item;
			}
		}
	}
	__sync_fetch_and_add(&directory->metrics.source_selections, 1);
	if (!best) {
		__sync_fetch_and_add(&directory->metrics.source_misses, 1);
		__sync_fetch_and_sub(&directory->metrics.active_leases, 1);
		pthread_rwlock_unlock(&shard->lock);
		pthread_mutex_unlock(&leases->lock);
		return 0;
	}
	lease = calloc(1, sizeof(*lease));
	if (!lease || !hash_table_insert(leases->leases, transfer_id, lease)) {
		free(lease);
		__sync_fetch_and_sub(&directory->metrics.active_leases, 1);
		pthread_rwlock_unlock(&shard->lock);
		pthread_mutex_unlock(&leases->lock);
		return -1;
	}
	lease->replica = best;
	strcpy(lease->transfer_id, transfer_id);
	strcpy(lease->destination_worker_id, destination_worker_id);
	lease->destination_worker_epoch = destination_worker_epoch;
	uint64_t active_leases = atomic_fetch_add(&best->active_leases, 1) + 1;
	result->replica = best->record;
	result->replica.active_leases = active_leases;
	strcpy(result->endpoint, best->endpoint);
	strcpy(result->transfer_id, transfer_id);
	pthread_rwlock_unlock(&shard->lock);
	pthread_mutex_unlock(&leases->lock);
	return 1;
}

int vine_datavine_directory_release_source(struct vine_datavine_directory *directory,
		const char *transfer_id, int success)
{
	if (!directory || !valid_text(transfer_id, VINE_DATAVINE_TRANSFER_ID_MAX)) {
		return 0;
	}
	struct lease_shard *leases = get_lease_shard(directory, transfer_id);
	pthread_mutex_lock(&leases->lock);
	struct lease *lease = hash_table_lookup(leases->leases, transfer_id);
	if (!lease) {
		pthread_mutex_unlock(&leases->lock);
		return 0;
	}
	int result = complete_lease_locked(directory, leases, lease, success);
	evict_completed_leases_locked(leases);
	pthread_mutex_unlock(&leases->lock);
	return result;
}

int vine_datavine_directory_invalidate_replica(
		struct vine_datavine_directory *directory, char kind, int64_t data_id,
		const char *replica_id)
{
	if (!directory || (kind != 'e' && kind != 'i') || data_id < 1 || !valid_text(replica_id, VINE_DATAVINE_REPLICA_ID_MAX)) {
		return -1;
	}
	uint64_t key = data_key(kind, data_id);
	struct directory_shard *shard = get_shard(directory, key);
	pthread_rwlock_wrlock(&shard->lock);
	struct data_entry *entry = itable_lookup(shard->data, key);
	for (struct replica *item = entry ? entry->replicas : 0; item; item = item->next) {
		if (item->available && !strcmp(item->record.replica_id, replica_id)) {
			item->available = 0;
			item->pruned = 0;
			__sync_fetch_and_add(&directory->metrics.invalidations, 1);
			pthread_rwlock_unlock(&shard->lock);
			return 1;
		}
	}
	pthread_rwlock_unlock(&shard->lock);
	return 0;
}

int vine_datavine_directory_restore_replica(
		struct vine_datavine_directory *directory, char kind, int64_t data_id,
		const char *replica_id)
{
	if (!directory || (kind != 'e' && kind != 'i') || data_id < 1 || !valid_text(replica_id, VINE_DATAVINE_REPLICA_ID_MAX)) {
		return -1;
	}
	uint64_t key = data_key(kind, data_id);
	struct directory_shard *shard = get_shard(directory, key);
	pthread_rwlock_wrlock(&shard->lock);
	struct data_entry *entry = itable_lookup(shard->data, key);
	struct replica *latest = 0;
	for (struct replica *item = entry ? entry->replicas : 0; item; item = item->next) {
		if (!strcmp(item->record.replica_id, replica_id) &&
				(!latest || item->record.generation > latest->record.generation)) {
			latest = item;
		}
	}
	if (!latest) {
		pthread_rwlock_unlock(&shard->lock);
		return 0;
	}
	if (latest->available) {
		pthread_rwlock_unlock(&shard->lock);
		return 1;
	}
	if (latest->pruned || latest->record.attempt != entry->latest_attempt ||
			!atomic_load(&latest->worker->active) ||
			atomic_load(&latest->worker->epoch) != latest->record.worker_epoch) {
		pthread_rwlock_unlock(&shard->lock);
		return -1;
	}
	latest->available = 1;
	__sync_fetch_and_add(&directory->metrics.restorations, 1);
	pthread_rwlock_unlock(&shard->lock);
	return 1;
}

int vine_datavine_directory_confirm_replica_pruned(
		struct vine_datavine_directory *directory, char kind, int64_t data_id,
		const char *replica_id)
{
	if (!directory || (kind != 'e' && kind != 'i') || data_id < 1 || !valid_text(replica_id, VINE_DATAVINE_REPLICA_ID_MAX)) {
		return -1;
	}
	uint64_t key = data_key(kind, data_id);
	struct directory_shard *shard = get_shard(directory, key);
	pthread_rwlock_wrlock(&shard->lock);
	struct data_entry *entry = itable_lookup(shard->data, key);
	struct replica *latest = 0;
	for (struct replica *item = entry ? entry->replicas : 0; item; item = item->next) {
		if (!strcmp(item->record.replica_id, replica_id) &&
				(!latest || item->record.generation > latest->record.generation)) {
			latest = item;
		}
	}
	if (!latest) {
		pthread_rwlock_unlock(&shard->lock);
		return 0;
	}
	if (latest->pruned) {
		pthread_rwlock_unlock(&shard->lock);
		return 1;
	}
	if (latest->available || atomic_load(&latest->active_leases)) {
		pthread_rwlock_unlock(&shard->lock);
		return -1;
	}
	latest->pruned = 1;
	__sync_fetch_and_add(&directory->metrics.prunes, 1);
	pthread_rwlock_unlock(&shard->lock);
	return 1;
}

int64_t vine_datavine_directory_replica_active_leases(
		struct vine_datavine_directory *directory, char kind, int64_t data_id,
		const char *replica_id)
{
	if (!directory || (kind != 'e' && kind != 'i') || data_id < 1 || !valid_text(replica_id, VINE_DATAVINE_REPLICA_ID_MAX)) {
		return -1;
	}
	uint64_t key = data_key(kind, data_id);
	struct directory_shard *shard = get_shard(directory, key);
	pthread_rwlock_rdlock(&shard->lock);
	struct data_entry *entry = itable_lookup(shard->data, key);
	for (struct replica *item = entry ? entry->replicas : 0; item; item = item->next) {
		if (!strcmp(item->record.replica_id, replica_id)) {
			int64_t result = (int64_t)atomic_load(&item->active_leases);
			pthread_rwlock_unlock(&shard->lock);
			return result;
		}
	}
	pthread_rwlock_unlock(&shard->lock);
	return -1;
}

int vine_datavine_directory_snapshot_replicas(
		struct vine_datavine_directory *directory, char kind, int64_t data_id,
		struct vine_datavine_replica_snapshot **result, size_t *count)
{
	if (!directory || (kind != 'e' && kind != 'i') || data_id < 1 || !result || !count) {
		return 0;
	}
	*result = 0;
	*count = 0;
	uint64_t key = data_key(kind, data_id);
	struct directory_shard *shard = get_shard(directory, key);
	pthread_rwlock_rdlock(&shard->lock);
	struct data_entry *entry = itable_lookup(shard->data, key);
	size_t length = 0;
	for (struct replica *item = entry ? entry->replicas : 0; item; item = item->next) {
		length++;
	}
	struct vine_datavine_replica_snapshot *records = length ? calloc(length, sizeof(*records)) : 0;
	if (length && !records) {
		pthread_rwlock_unlock(&shard->lock);
		return 0;
	}
	size_t index = 0;
	for (struct replica *item = entry ? entry->replicas : 0; item; item = item->next) {
		records[index].replica = item->record;
		records[index].replica.active_leases = atomic_load(&item->active_leases);
		records[index].state = item->pruned ? 2 : item->available && atomic_load(&item->worker->active) && atomic_load(&item->worker->epoch) == item->record.worker_epoch;
		strcpy(records[index].endpoint, item->endpoint);
		index++;
	}
	pthread_rwlock_unlock(&shard->lock);
	*result = records;
	*count = length;
	return 1;
}

void vine_datavine_directory_get_metrics(struct vine_datavine_directory *directory,
		struct vine_datavine_directory_metrics *result)
{
	if (!directory || !result) {
		return;
	}
	result->workers = __sync_fetch_and_add(&directory->metrics.workers, 0);
	result->replicas = __sync_fetch_and_add(&directory->metrics.replicas, 0);
	result->active_leases = __sync_fetch_and_add(&directory->metrics.active_leases, 0);
	result->source_selections = __sync_fetch_and_add(&directory->metrics.source_selections, 0);
	result->source_misses = __sync_fetch_and_add(&directory->metrics.source_misses, 0);
	result->releases = __sync_fetch_and_add(&directory->metrics.releases, 0);
	result->release_failures = __sync_fetch_and_add(&directory->metrics.release_failures, 0);
	result->idempotent_releases = __sync_fetch_and_add(&directory->metrics.idempotent_releases, 0);
	result->invalidations = __sync_fetch_and_add(&directory->metrics.invalidations, 0);
	result->restorations = __sync_fetch_and_add(&directory->metrics.restorations, 0);
	result->prunes = __sync_fetch_and_add(&directory->metrics.prunes, 0);
	result->stale_rejections = __sync_fetch_and_add(&directory->metrics.stale_rejections, 0);
}
