#ifndef VINE_DATAVINE_INDEX_H
#define VINE_DATAVINE_INDEX_H

#include <stdint.h>

struct vine_datavine_index;

struct vine_datavine_data {
	int64_t data_id;
	int64_t producer_task_id;
	int32_t producer_output_index;
	int32_t attempt;
	int64_t size;
	uint64_t revision;
	char content_hash[65];
};

struct vine_datavine_index_metrics {
	uint64_t allocations;
	uint64_t publications;
	uint64_t idempotent_publications;
	uint64_t stale_rejections;
	uint64_t conflict_rejections;
};

struct vine_datavine_index *vine_datavine_index_create(int64_t maximum_data_id, int shards);
void vine_datavine_index_delete(struct vine_datavine_index *index);

int vine_datavine_index_allocate(struct vine_datavine_index *index, int64_t data_id,
		int64_t producer_task_id, int32_t producer_output_index);
int vine_datavine_index_publish(struct vine_datavine_index *index, int64_t data_id,
		int32_t attempt, const char *content_hash, int64_t size,
		struct vine_datavine_data *result);
int vine_datavine_index_put(struct vine_datavine_index *index, int64_t data_id,
		int32_t attempt, const char *content_hash, int64_t size);
int vine_datavine_index_get(struct vine_datavine_index *index, int64_t data_id,
		struct vine_datavine_data *result);
void vine_datavine_index_get_metrics(struct vine_datavine_index *index,
		struct vine_datavine_index_metrics *result);

#endif
