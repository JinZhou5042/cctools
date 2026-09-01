/*
Dense, generation-safe Worker availability ring.

The generic TaskVine Worker hash remains authoritative. This optional index is
created only when Worker-first scheduling is enabled, and it contains no
strings, ownership, or generic scheduling policy.
*/

#include "vine_worker_pool.h"
#include "vine_worker_info.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct worker_slot {
	struct vine_worker_info *worker;
	uint32_t generation;
	uint8_t queued;
};

struct ready_entry {
	uint32_t slot;
	uint32_t generation;
};

struct vine_worker_pool {
	struct worker_slot *slots;
	uint32_t slot_capacity;
	uint32_t slot_highwater;
	uint32_t *free_slots;
	uint32_t free_count;
	struct ready_entry *ready;
	uint32_t ready_capacity;
	uint32_t ready_head;
	uint32_t ready_count;
};

static int reserve_slots(struct vine_worker_pool *pool)
{
	if (pool->free_count || pool->slot_highwater < pool->slot_capacity)
		return 1;
	uint32_t capacity = pool->slot_capacity ? pool->slot_capacity * 2 : 16;
	if (capacity <= pool->slot_capacity)
		return 0;
	struct worker_slot *slots = calloc(capacity, sizeof(*slots));
	uint32_t *free_slots = malloc((size_t)capacity * sizeof(*free_slots));
	if (!slots || !free_slots) {
		free(slots);
		free(free_slots);
		return 0;
	}
	if (pool->slot_highwater)
		memcpy(slots, pool->slots,
				(size_t)pool->slot_highwater * sizeof(*slots));
	if (pool->free_count)
		memcpy(free_slots, pool->free_slots,
				(size_t)pool->free_count * sizeof(*free_slots));
	free(pool->slots);
	free(pool->free_slots);
	pool->slots = slots;
	pool->free_slots = free_slots;
	pool->slot_capacity = capacity;
	return 1;
}

static int reserve_ready(struct vine_worker_pool *pool)
{
	if (pool->ready_count < pool->ready_capacity)
		return 1;
	uint32_t capacity = pool->ready_capacity ? pool->ready_capacity * 2 : 16;
	if (capacity <= pool->ready_capacity)
		return 0;
	struct ready_entry *ready = malloc((size_t)capacity * sizeof(*ready));
	if (!ready)
		return 0;
	for (uint32_t index = 0; index < pool->ready_count; index++)
		ready[index] = pool->ready[(pool->ready_head + index) %
				pool->ready_capacity];
	free(pool->ready);
	pool->ready = ready;
	pool->ready_capacity = capacity;
	pool->ready_head = 0;
	return 1;
}

struct vine_worker_pool *vine_worker_pool_create(void)
{
	return calloc(1, sizeof(struct vine_worker_pool));
}

void vine_worker_pool_delete(struct vine_worker_pool *pool)
{
	if (!pool)
		return;
	for (uint32_t slot = 0; slot < pool->slot_highwater; slot++) {
		struct vine_worker_info *worker = pool->slots[slot].worker;
		if (worker && worker->worker_pool_slot_valid) {
			worker->worker_pool_slot_valid = 0;
			worker->worker_pool_slot = 0;
			worker->worker_pool_generation = 0;
		}
	}
	free(pool->ready);
	free(pool->free_slots);
	free(pool->slots);
	free(pool);
}

int vine_worker_pool_add(struct vine_worker_pool *pool,
		struct vine_worker_info *worker)
{
	if (!pool || !worker)
		return 0;
	if (worker->worker_pool_slot_valid) {
		if (worker->worker_pool_slot < pool->slot_highwater &&
				pool->slots[worker->worker_pool_slot].worker == worker &&
				pool->slots[worker->worker_pool_slot].generation ==
						worker->worker_pool_generation)
			return 1;
		/* Treat stale or foreign slot metadata as unregistered. */
		worker->worker_pool_slot_valid = 0;
		worker->worker_pool_slot = 0;
		worker->worker_pool_generation = 0;
	}
	if (!reserve_slots(pool))
		return 0;
	uint32_t slot = pool->free_count
			? pool->free_slots[--pool->free_count]
			: pool->slot_highwater++;
	struct worker_slot *record = &pool->slots[slot];
	if (!record->generation)
		record->generation = 1;
	record->worker = worker;
	record->queued = 0;
	worker->worker_pool_slot = slot;
	worker->worker_pool_generation = record->generation;
	worker->worker_pool_slot_valid = 1;
	return 1;
}

void vine_worker_pool_remove(struct vine_worker_pool *pool,
		struct vine_worker_info *worker)
{
	if (!pool || !worker || !worker->worker_pool_slot_valid ||
			worker->worker_pool_slot >= pool->slot_highwater)
		return;
	uint32_t slot = worker->worker_pool_slot;
	struct worker_slot *record = &pool->slots[slot];
	if (record->worker != worker ||
			record->generation != worker->worker_pool_generation)
		return;
	record->worker = 0;
	record->queued = 0;
	if (++record->generation == 0)
		record->generation = 1;
	pool->free_slots[pool->free_count++] = slot;
	worker->worker_pool_slot_valid = 0;
	worker->worker_pool_slot = 0;
	worker->worker_pool_generation = 0;
}

int vine_worker_pool_offer(struct vine_worker_pool *pool,
		struct vine_worker_info *worker)
{
	if (!pool || !worker)
		return 0;
	if (!vine_worker_pool_add(pool, worker))
		return 0;
	struct worker_slot *record = &pool->slots[worker->worker_pool_slot];
	if (record->worker != worker || record->queued ||
			record->generation != worker->worker_pool_generation)
		return record->worker == worker && record->queued;
	if (!reserve_ready(pool))
		return 0;
	uint32_t tail = (pool->ready_head + pool->ready_count) %
			pool->ready_capacity;
	pool->ready[tail] = (struct ready_entry){
			worker->worker_pool_slot, worker->worker_pool_generation};
	pool->ready_count++;
	record->queued = 1;
	return 1;
}

struct vine_worker_info *vine_worker_pool_take(struct vine_worker_pool *pool)
{
	while (pool && pool->ready_count) {
		struct ready_entry entry = pool->ready[pool->ready_head];
		pool->ready_head = (pool->ready_head + 1) % pool->ready_capacity;
		pool->ready_count--;
		if (entry.slot >= pool->slot_highwater)
			continue;
		struct worker_slot *record = &pool->slots[entry.slot];
		if (!record->worker || record->generation != entry.generation)
			continue;
		record->queued = 0;
		return record->worker;
	}
	return 0;
}

size_t vine_worker_pool_ready(const struct vine_worker_pool *pool)
{
	return pool ? pool->ready_count : 0;
}
