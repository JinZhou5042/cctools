/*
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_datavine_index.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#define DATAVINE_INDEX_SEGMENT_SHIFT 12
#define DATAVINE_INDEX_SEGMENT_SIZE (1U << DATAVINE_INDEX_SEGMENT_SHIFT)

struct vine_datavine_slot {
	int allocated;
	struct vine_datavine_data data;
	unsigned char *edata_metadata;
	size_t edata_metadata_size;
	unsigned char *edata_payload;
	size_t edata_payload_size;
	char edata_content_hash[65];
	char edata_serialized_hash[65];
};

struct vine_datavine_segment {
	struct vine_datavine_slot slots[DATAVINE_INDEX_SEGMENT_SIZE];
};

struct vine_datavine_index {
	int64_t maximum_data_id;
	size_t segment_count;
	struct vine_datavine_segment **segments;
	pthread_rwlock_t segments_lock;
	int shard_count;
	pthread_mutex_t *shards;
	struct vine_datavine_index_metrics metrics;
};

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

static struct vine_datavine_segment *get_segment(struct vine_datavine_index *index, int64_t data_id, int create)
{
	size_t segment_id = (size_t)data_id >> DATAVINE_INDEX_SEGMENT_SHIFT;
	struct vine_datavine_segment *segment = 0;
	pthread_rwlock_rdlock(&index->segments_lock);
	segment = index->segments[segment_id];
	pthread_rwlock_unlock(&index->segments_lock);
	if (segment || !create) {
		return segment;
	}
	pthread_rwlock_wrlock(&index->segments_lock);
	segment = index->segments[segment_id];
	if (!segment) {
		segment = calloc(1, sizeof(*segment));
		index->segments[segment_id] = segment;
	}
	pthread_rwlock_unlock(&index->segments_lock);
	return segment;
}

static struct vine_datavine_slot *get_slot(struct vine_datavine_index *index, int64_t data_id, int create)
{
	if (!index || data_id < 1 || data_id > index->maximum_data_id) {
		return 0;
	}
	struct vine_datavine_segment *segment = get_segment(index, data_id, create);
	return segment ? &segment->slots[(size_t)data_id & (DATAVINE_INDEX_SEGMENT_SIZE - 1)] : 0;
}

static pthread_mutex_t *get_shard(struct vine_datavine_index *index, int64_t data_id)
{
	uint64_t value = (uint64_t)data_id * UINT64_C(11400714819323198485);
	return &index->shards[value % (uint64_t)index->shard_count];
}

struct vine_datavine_index *vine_datavine_index_create(int64_t maximum_data_id, int shards)
{
	if (maximum_data_id < 1 || shards < 1) {
		return 0;
	}
	struct vine_datavine_index *index = calloc(1, sizeof(*index));
	if (!index) {
		return 0;
	}
	index->maximum_data_id = maximum_data_id;
	index->segment_count = ((size_t)maximum_data_id >> DATAVINE_INDEX_SEGMENT_SHIFT) + 1;
	index->segments = calloc(index->segment_count, sizeof(*index->segments));
	index->shard_count = shards;
	index->shards = calloc((size_t)shards, sizeof(*index->shards));
	if (!index->segments || !index->shards || pthread_rwlock_init(&index->segments_lock, 0)) {
		free(index->segments);
		free(index->shards);
		free(index);
		return 0;
	}
	for (int i = 0; i < shards; i++) {
		if (pthread_mutex_init(&index->shards[i], 0)) {
			for (int j = 0; j < i; j++) {
				pthread_mutex_destroy(&index->shards[j]);
			}
			pthread_rwlock_destroy(&index->segments_lock);
			free(index->segments);
			free(index->shards);
			free(index);
			return 0;
		}
	}
	return index;
}

void vine_datavine_index_delete(struct vine_datavine_index *index)
{
	if (!index) {
		return;
	}
	for (size_t i = 0; i < index->segment_count; i++) {
		struct vine_datavine_segment *segment = index->segments[i];
		if (segment) {
			for (size_t j = 0; j < DATAVINE_INDEX_SEGMENT_SIZE; j++) {
				free(segment->slots[j].edata_metadata);
				free(segment->slots[j].edata_payload);
			}
		}
		free(index->segments[i]);
	}
	for (int i = 0; i < index->shard_count; i++) {
		pthread_mutex_destroy(&index->shards[i]);
	}
	pthread_rwlock_destroy(&index->segments_lock);
	free(index->segments);
	free(index->shards);
	free(index);
}

int vine_datavine_index_allocate(struct vine_datavine_index *index, int64_t data_id, int64_t producer_task_id, int32_t producer_output_index)
{
	if (producer_task_id < 1 || producer_output_index < 0) {
		return 0;
	}
	struct vine_datavine_slot *slot = get_slot(index, data_id, 1);
	if (!slot) {
		return 0;
	}
	pthread_mutex_t *shard = get_shard(index, data_id);
	pthread_mutex_lock(shard);
	if (slot->allocated) {
		int matches = slot->data.producer_task_id == producer_task_id && slot->data.producer_output_index == producer_output_index;
		pthread_mutex_unlock(shard);
		return matches;
	}
	slot->allocated = 1;
	slot->data.data_id = data_id;
	slot->data.producer_task_id = producer_task_id;
	slot->data.producer_output_index = producer_output_index;
	slot->data.revision = 1;
	__sync_fetch_and_add(&index->metrics.allocations, 1);
	pthread_mutex_unlock(shard);
	return 1;
}

int vine_datavine_index_publish(struct vine_datavine_index *index, int64_t data_id, int32_t attempt, const char *content_hash, int64_t size, struct vine_datavine_data *result)
{
	if (attempt < 1 || size < 0 || !valid_hash(content_hash)) {
		return 0;
	}
	struct vine_datavine_slot *slot = get_slot(index, data_id, 0);
	if (!slot) {
		return 0;
	}
	pthread_mutex_t *shard = get_shard(index, data_id);
	pthread_mutex_lock(shard);
	if (!slot->allocated) {
		pthread_mutex_unlock(shard);
		return 0;
	}
	if (attempt < slot->data.attempt) {
		__sync_fetch_and_add(&index->metrics.stale_rejections, 1);
		pthread_mutex_unlock(shard);
		return 0;
	}
	if (attempt == slot->data.attempt && slot->data.content_hash[0]) {
		if (slot->data.size != size || strcmp(slot->data.content_hash, content_hash)) {
			__sync_fetch_and_add(&index->metrics.conflict_rejections, 1);
			pthread_mutex_unlock(shard);
			return 0;
		}
		__sync_fetch_and_add(&index->metrics.idempotent_publications, 1);
	} else {
		slot->data.attempt = attempt;
		slot->data.size = size;
		memcpy(slot->data.content_hash, content_hash, 65);
		slot->data.revision++;
		__sync_fetch_and_add(&index->metrics.publications, 1);
	}
	if (result) {
		*result = slot->data;
	}
	pthread_mutex_unlock(shard);
	return 1;
}

int vine_datavine_index_put(struct vine_datavine_index *index, int64_t data_id, int32_t attempt, const char *content_hash, int64_t size)
{
	return vine_datavine_index_publish(index, data_id, attempt, content_hash, size, 0);
}

int vine_datavine_index_get(struct vine_datavine_index *index, int64_t data_id, struct vine_datavine_data *result)
{
	struct vine_datavine_slot *slot = get_slot(index, data_id, 0);
	if (!slot || !result) {
		return 0;
	}
	pthread_mutex_t *shard = get_shard(index, data_id);
	pthread_mutex_lock(shard);
	int found = slot->allocated;
	if (found) {
		*result = slot->data;
	}
	pthread_mutex_unlock(shard);
	return found;
}

int vine_datavine_index_put_edata(struct vine_datavine_index *index,
		int64_t data_id, const char *content_hash,
		const char *serialized_hash, const unsigned char *metadata,
		size_t metadata_size, const unsigned char *payload,
		size_t payload_size)
{
	if (!valid_hash(content_hash) || !valid_hash(serialized_hash)
			|| (!metadata && metadata_size) || (!payload && payload_size)) {
		return 0;
	}
	struct vine_datavine_slot *slot = get_slot(index, data_id, 1);
	if (!slot) {
		return 0;
	}
	pthread_mutex_t *shard = get_shard(index, data_id);
	pthread_mutex_lock(shard);
	if (slot->edata_content_hash[0]) {
		int matches = slot->edata_metadata_size == metadata_size
			&& slot->edata_payload_size == payload_size
			&& !strcmp(slot->edata_content_hash, content_hash)
			&& !strcmp(slot->edata_serialized_hash, serialized_hash)
			&& (!metadata_size || !memcmp(slot->edata_metadata, metadata, metadata_size))
			&& (!payload_size || !memcmp(slot->edata_payload, payload, payload_size));
		pthread_mutex_unlock(shard);
		return matches;
	}
	unsigned char *metadata_copy = metadata_size ? malloc(metadata_size) : 0;
	unsigned char *payload_copy = payload_size ? malloc(payload_size) : 0;
	if ((metadata_size && !metadata_copy) || (payload_size && !payload_copy)) {
		free(metadata_copy);
		free(payload_copy);
		pthread_mutex_unlock(shard);
		return 0;
	}
	if (metadata_size) {
		memcpy(metadata_copy, metadata, metadata_size);
	}
	if (payload_size) {
		memcpy(payload_copy, payload, payload_size);
	}
	slot->edata_metadata = metadata_copy;
	slot->edata_metadata_size = metadata_size;
	slot->edata_payload = payload_copy;
	slot->edata_payload_size = payload_size;
	memcpy(slot->edata_content_hash, content_hash, 65);
	memcpy(slot->edata_serialized_hash, serialized_hash, 65);
	pthread_mutex_unlock(shard);
	return 1;
}

int vine_datavine_index_get_edata(struct vine_datavine_index *index,
		int64_t data_id, char content_hash[65],
		char serialized_hash[65], unsigned char **metadata,
		size_t *metadata_size, unsigned char **payload,
		size_t *payload_size)
{
	if (!content_hash || !serialized_hash || !metadata || !metadata_size
			|| !payload || !payload_size) {
		return 0;
	}
	struct vine_datavine_slot *slot = get_slot(index, data_id, 0);
	if (!slot) {
		return 0;
	}
	pthread_mutex_t *shard = get_shard(index, data_id);
	pthread_mutex_lock(shard);
	if (!slot->edata_content_hash[0]) {
		pthread_mutex_unlock(shard);
		return 0;
	}
	unsigned char *metadata_copy = slot->edata_metadata_size ? malloc(slot->edata_metadata_size) : 0;
	unsigned char *payload_copy = slot->edata_payload_size ? malloc(slot->edata_payload_size) : 0;
	if ((slot->edata_metadata_size && !metadata_copy) || (slot->edata_payload_size && !payload_copy)) {
		free(metadata_copy);
		free(payload_copy);
		pthread_mutex_unlock(shard);
		return 0;
	}
	if (slot->edata_metadata_size) {
		memcpy(metadata_copy, slot->edata_metadata, slot->edata_metadata_size);
	}
	if (slot->edata_payload_size) {
		memcpy(payload_copy, slot->edata_payload, slot->edata_payload_size);
	}
	memcpy(content_hash, slot->edata_content_hash, 65);
	memcpy(serialized_hash, slot->edata_serialized_hash, 65);
	*metadata = metadata_copy;
	*metadata_size = slot->edata_metadata_size;
	*payload = payload_copy;
	*payload_size = slot->edata_payload_size;
	pthread_mutex_unlock(shard);
	return 1;
}

void vine_datavine_index_get_metrics(struct vine_datavine_index *index,
		struct vine_datavine_index_metrics *result)
{
	if (!index || !result) {
		return;
	}
	*result = index->metrics;
}
