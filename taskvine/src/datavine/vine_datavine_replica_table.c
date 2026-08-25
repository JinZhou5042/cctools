/* Compact single-writer DataVine worker-replica directory. */

#include "vine_datavine_replica_table.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define DATA_CHUNK_SHIFT 10U
#define DATA_CHUNK_SIZE (1U << DATA_CHUNK_SHIFT)
#define DATA_CHUNK_MASK (DATA_CHUNK_SIZE - 1U)

#define DATA_PRESENT 0x01U
#define DATA_LIVE 0x02U
#define DATA_REQUESTED 0x04U
#define DATA_PERSISTED 0x08U
#define DATA_RECOVERY 0x10U
#define DATA_IDENTITY 0x20U

#define REPLICA_ACTIVE 0x01U
#define WAITER_ACTIVE 0x01U

struct data_record {
	uint64_t size;
	unsigned char digest[32];
	uint32_t generation;
	uint32_t flags;
	uint32_t replica_head;
	uint32_t replica_count;
	uint32_t waiter_head;
	uint32_t waiter_count;
};

struct data_chunk {
	struct data_record records[DATA_CHUNK_SIZE];
};

struct replica_record {
	uint64_t object_token;
	uint64_t session_epoch;
	uint64_t data_id;
	uint32_t generation;
	uint32_t worker_slot;
	uint32_t data_next;
	uint32_t data_previous;
	uint32_t worker_next;
	uint32_t worker_previous;
	uint32_t flags;
};

struct waiter_record {
	uint64_t data_id;
	uint64_t session_epoch;
	uint64_t request_id;
	uint32_t generation;
	uint32_t worker_slot;
	uint32_t item_index;
	uint32_t data_next;
	uint32_t data_previous;
	uint32_t worker_next;
	uint32_t worker_previous;
	uint32_t flags;
};

struct worker_session {
	uint64_t epoch;
	uint32_t replica_head;
	uint32_t waiter_head;
	unsigned char state;
};

struct vine_datavine_replica_table {
	struct data_chunk **chunks;
	size_t chunk_capacity;
	struct replica_record *replicas;
	uint32_t replica_capacity;
	uint32_t replica_highwater;
	uint32_t replica_free;
	struct waiter_record *waiters;
	uint32_t waiter_capacity;
	uint32_t waiter_highwater;
	uint32_t waiter_free;
	struct worker_session *workers;
	uint32_t worker_capacity;
	struct vine_datavine_replica_stats stats;
};

static struct data_record *data_lookup(
		struct vine_datavine_replica_table *table, uint64_t data_id)
{
	if (!table || !data_id)
		return 0;
	uint64_t chunk_index = data_id >> DATA_CHUNK_SHIFT;
	if (chunk_index >= table->chunk_capacity || !table->chunks[chunk_index])
		return 0;
	struct data_record *record =
			&table->chunks[chunk_index]->records[data_id & DATA_CHUNK_MASK];
	return record->flags & DATA_PRESENT ? record : 0;
}

static int reserve_chunks(
		struct vine_datavine_replica_table *table, uint64_t chunk_index)
{
	if (chunk_index > SIZE_MAX / sizeof(*table->chunks) - 1)
		return 0;
	if (chunk_index < table->chunk_capacity)
		return 1;
	size_t capacity = table->chunk_capacity ? table->chunk_capacity : 16;
	while (capacity <= chunk_index) {
		if (capacity > SIZE_MAX / 2)
			return 0;
		capacity *= 2;
	}
	struct data_chunk **next = realloc(
			table->chunks, capacity * sizeof(*next));
	if (!next)
		return 0;
	memset(next + table->chunk_capacity, 0, (capacity - table->chunk_capacity) * sizeof(*next));
	table->chunks = next;
	table->chunk_capacity = capacity;
	return 1;
}

static struct data_record *data_get(
		struct vine_datavine_replica_table *table, uint64_t data_id)
{
	if (!table || !data_id)
		return 0;
	uint64_t chunk_index = data_id >> DATA_CHUNK_SHIFT;
	if (!reserve_chunks(table, chunk_index))
		return 0;
	if (!table->chunks[chunk_index]) {
		table->chunks[chunk_index] = calloc(1, sizeof(struct data_chunk));
		if (!table->chunks[chunk_index])
			return 0;
	}
	struct data_record *record =
			&table->chunks[chunk_index]->records[data_id & DATA_CHUNK_MASK];
	if (!(record->flags & DATA_PRESENT)) {
		record->flags = DATA_PRESENT | DATA_LIVE;
		table->stats.active_data++;
		if (table->stats.active_data > table->stats.peak_data)
			table->stats.peak_data = table->stats.active_data;
	}
	return record;
}

static int reserve_workers(
		struct vine_datavine_replica_table *table, uint32_t worker_slot)
{
	if (worker_slot < table->worker_capacity)
		return 1;
	uint32_t capacity = table->worker_capacity ? table->worker_capacity : 16;
	while (capacity <= worker_slot) {
		if (capacity > UINT32_MAX / 2)
			return 0;
		capacity *= 2;
	}
	struct worker_session *next = realloc(
			table->workers, (size_t)capacity * sizeof(*next));
	if (!next)
		return 0;
	memset(next + table->worker_capacity, 0, (size_t)(capacity - table->worker_capacity) * sizeof(*next));
	table->workers = next;
	table->worker_capacity = capacity;
	return 1;
}

static int reserve_replicas(struct vine_datavine_replica_table *table)
{
	if (table->replica_free)
		return 1;
	if (table->replica_highwater + 1 < table->replica_capacity)
		return 1;
	uint32_t capacity = table->replica_capacity ? table->replica_capacity * 2 : 1024;
	if (capacity <= table->replica_capacity)
		return 0;
	struct replica_record *next = realloc(
			table->replicas, (size_t)capacity * sizeof(*next));
	if (!next)
		return 0;
	memset(next + table->replica_capacity, 0, (size_t)(capacity - table->replica_capacity) * sizeof(*next));
	table->replicas = next;
	table->replica_capacity = capacity;
	return 1;
}

static int reserve_replica_count(struct vine_datavine_replica_table *table,
		size_t count)
{
	size_t available = (size_t)table->replica_capacity -
			(size_t)table->replica_highwater - 1;
	for (uint32_t index = table->replica_free; index;
			index = table->replicas[index].data_next)
		available++;
	if (available >= count)
		return 1;
	uint32_t capacity = table->replica_capacity ? table->replica_capacity : 1024;
	while ((size_t)capacity - (size_t)table->replica_highwater - 1 <
			count - (available - ((size_t)table->replica_capacity -
			(size_t)table->replica_highwater - 1))) {
		if (capacity > UINT32_MAX / 2)
			return 0;
		capacity *= 2;
	}
	struct replica_record *next = realloc(table->replicas,
			(size_t)capacity * sizeof(*next));
	if (!next)
		return 0;
	memset(next + table->replica_capacity, 0,
			(size_t)(capacity - table->replica_capacity) * sizeof(*next));
	table->replicas = next;
	table->replica_capacity = capacity;
	return 1;
}

static uint32_t replica_allocate(struct vine_datavine_replica_table *table)
{
	if (!reserve_replicas(table))
		return 0;
	uint32_t index = table->replica_free;
	if (index) {
		table->replica_free = table->replicas[index].data_next;
	} else {
		index = ++table->replica_highwater;
	}
	memset(&table->replicas[index], 0, sizeof(table->replicas[index]));
	return index;
}

static int reserve_waiters(struct vine_datavine_replica_table *table)
{
	if (table->waiter_free)
		return 1;
	if (table->waiter_highwater + 1 < table->waiter_capacity)
		return 1;
	uint32_t capacity = table->waiter_capacity ? table->waiter_capacity * 2 : 1024;
	if (capacity <= table->waiter_capacity)
		return 0;
	struct waiter_record *next = realloc(
			table->waiters, (size_t)capacity * sizeof(*next));
	if (!next)
		return 0;
	memset(next + table->waiter_capacity, 0, (size_t)(capacity - table->waiter_capacity) * sizeof(*next));
	table->waiters = next;
	table->waiter_capacity = capacity;
	return 1;
}

static uint32_t waiter_allocate(struct vine_datavine_replica_table *table)
{
	if (!reserve_waiters(table))
		return 0;
	uint32_t index = table->waiter_free;
	if (index) {
		table->waiter_free = table->waiters[index].data_next;
	} else {
		index = ++table->waiter_highwater;
	}
	memset(&table->waiters[index], 0, sizeof(table->waiters[index]));
	return index;
}

static int session_matches(struct vine_datavine_replica_table *table,
		uint32_t worker_slot, uint64_t session_epoch)
{
	return table && worker_slot < table->worker_capacity && session_epoch &&
			table->workers[worker_slot].epoch == session_epoch &&
			table->workers[worker_slot].state != VINE_DATAVINE_SESSION_UNUSED;
}

static void replica_remove(struct vine_datavine_replica_table *table,
		uint32_t index)
{
	struct replica_record *replica = &table->replicas[index];
	if (!(replica->flags & REPLICA_ACTIVE))
		return;
	struct data_record *data = data_lookup(table, replica->data_id);
	struct worker_session *worker = replica->worker_slot < table->worker_capacity
							? &table->workers[replica->worker_slot]
							: 0;
	if (data) {
		if (replica->data_previous)
			table->replicas[replica->data_previous].data_next = replica->data_next;
		else
			data->replica_head = replica->data_next;
		if (replica->data_next)
			table->replicas[replica->data_next].data_previous =
					replica->data_previous;
		if (data->replica_count)
			data->replica_count--;
	}
	if (worker) {
		if (replica->worker_previous)
			table->replicas[replica->worker_previous].worker_next =
					replica->worker_next;
		else
			worker->replica_head = replica->worker_next;
		if (replica->worker_next)
			table->replicas[replica->worker_next].worker_previous =
					replica->worker_previous;
	}
	replica->flags = 0;
	replica->data_next = table->replica_free;
	table->replica_free = index;
	if (table->stats.active_replicas)
		table->stats.active_replicas--;
}

static void waiter_remove(struct vine_datavine_replica_table *table,
		uint32_t index)
{
	struct waiter_record *waiter = &table->waiters[index];
	if (!(waiter->flags & WAITER_ACTIVE))
		return;
	struct data_record *data = data_lookup(table, waiter->data_id);
	struct worker_session *worker = waiter->worker_slot < table->worker_capacity
							? &table->workers[waiter->worker_slot]
							: 0;
	if (data) {
		if (waiter->data_previous)
			table->waiters[waiter->data_previous].data_next = waiter->data_next;
		else
			data->waiter_head = waiter->data_next;
		if (waiter->data_next)
			table->waiters[waiter->data_next].data_previous = waiter->data_previous;
		if (data->waiter_count)
			data->waiter_count--;
	}
	if (worker) {
		if (waiter->worker_previous)
			table->waiters[waiter->worker_previous].worker_next = waiter->worker_next;
		else
			worker->waiter_head = waiter->worker_next;
		if (waiter->worker_next)
			table->waiters[waiter->worker_next].worker_previous =
					waiter->worker_previous;
	}
	waiter->flags = 0;
	waiter->data_next = table->waiter_free;
	table->waiter_free = index;
	if (table->stats.active_waiters)
		table->stats.active_waiters--;
}

static void wake_waiters(struct vine_datavine_replica_table *table,
		uint64_t data_id, struct data_record *data,
		vine_datavine_waiter_callback_t callback, void *argument)
{
	uint32_t index = data->waiter_head;
	while (index) {
		struct waiter_record copy = table->waiters[index];
		uint32_t next = copy.data_next;
		if (!copy.generation || copy.generation == data->generation) {
			waiter_remove(table, index);
			if (callback)
				callback(data_id, data->generation, copy.worker_slot, copy.session_epoch, copy.request_id, copy.item_index, argument);
		}
		index = next;
	}
}

struct vine_datavine_replica_table *vine_datavine_replica_table_create(void)
{
	return calloc(1, sizeof(struct vine_datavine_replica_table));
}

void vine_datavine_replica_table_delete(
		struct vine_datavine_replica_table *table)
{
	if (!table)
		return;
	for (size_t index = 0; index < table->chunk_capacity; index++)
		free(table->chunks[index]);
	free(table->chunks);
	free(table->replicas);
	free(table->waiters);
	free(table->workers);
	free(table);
}

int vine_datavine_replica_table_session_open(
		struct vine_datavine_replica_table *table, uint32_t worker_slot,
		uint64_t session_epoch)
{
	if (!table || !session_epoch || !reserve_workers(table, worker_slot))
		return 0;
	struct worker_session *worker = &table->workers[worker_slot];
	if (worker->state != VINE_DATAVINE_SESSION_UNUSED) {
		if (worker->epoch == session_epoch)
			return worker->state == VINE_DATAVINE_SESSION_ACTIVE;
		vine_datavine_replica_table_session_lost(
				table, worker_slot, worker->epoch, 0, 0, 0);
	}
	worker->epoch = session_epoch;
	worker->state = VINE_DATAVINE_SESSION_ACTIVE;
	table->stats.active_sessions++;
	return 1;
}

int vine_datavine_replica_table_session_state(
		struct vine_datavine_replica_table *table, uint32_t worker_slot,
		uint64_t session_epoch, enum vine_datavine_session_state state)
{
	if (!session_matches(table, worker_slot, session_epoch) ||
			state == VINE_DATAVINE_SESSION_UNUSED)
		return 0;
	table->workers[worker_slot].state = (unsigned char)state;
	return 1;
}

int vine_datavine_replica_table_session_active(
		struct vine_datavine_replica_table *table, uint32_t worker_slot,
		uint64_t session_epoch)
{
	return session_matches(table, worker_slot, session_epoch) &&
			table->workers[worker_slot].state == VINE_DATAVINE_SESSION_ACTIVE;
}

int vine_datavine_replica_table_session_lost(
		struct vine_datavine_replica_table *table, uint32_t worker_slot,
		uint64_t session_epoch, vine_datavine_data_callback_t last_replica,
		vine_datavine_waiter_callback_t cancelled_waiter, void *argument)
{
	if (!session_matches(table, worker_slot, session_epoch))
		return 0;
	struct worker_session *worker = &table->workers[worker_slot];
	while (worker->replica_head) {
		uint32_t index = worker->replica_head;
		struct replica_record copy = table->replicas[index];
		struct data_record *data = data_lookup(table, copy.data_id);
		replica_remove(table, index);
		if (data && !data->replica_count && (data->flags & DATA_LIVE) &&
				!(data->flags & DATA_PERSISTED) && last_replica)
			last_replica(copy.data_id, data->generation, argument);
	}
	while (worker->waiter_head) {
		uint32_t index = worker->waiter_head;
		struct waiter_record copy = table->waiters[index];
		waiter_remove(table, index);
		if (cancelled_waiter)
			cancelled_waiter(copy.data_id, copy.generation, copy.worker_slot, copy.session_epoch, copy.request_id, copy.item_index, argument);
	}
	memset(worker, 0, sizeof(*worker));
	if (table->stats.active_sessions)
		table->stats.active_sessions--;
	return 1;
}

int vine_datavine_replica_table_expect(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t *generation, int requested)
{
	if (!table || !data_id || !generation || !*generation)
		return 0;
	struct data_record *data = data_get(table, data_id);
	if (!data || !(data->flags & DATA_LIVE))
		return 0;
	if (data->flags & DATA_IDENTITY) {
		if (*generation > data->generation && !data->replica_count &&
				!(data->flags & DATA_PERSISTED)) {
			data->flags &= ~DATA_IDENTITY;
			data->generation = *generation;
			data->size = 0;
			memset(data->digest, 0, sizeof(data->digest));
		} else {
			*generation = data->generation;
		}
	} else {
		data->generation = *generation;
	}
	if (requested)
		data->flags |= DATA_REQUESTED;
	return 1;
}

int vine_datavine_replica_table_publish(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, uint64_t size, const unsigned char digest[32],
		uint32_t worker_slot, uint64_t session_epoch, uint64_t object_token,
		int requested, vine_datavine_waiter_callback_t wake_waiter,
		void *argument)
{
	if (!table || !data_id || !generation || !digest || !object_token ||
			!session_matches(table, worker_slot, session_epoch) ||
			table->workers[worker_slot].state != VINE_DATAVINE_SESSION_ACTIVE)
		return 0;
	struct data_record *data = data_lookup(table, data_id);
	if (!data || !(data->flags & DATA_LIVE) ||
			data->generation != generation)
		return 0;
	if (data->flags & DATA_IDENTITY) {
		if (data->generation != generation || data->size != size ||
				memcmp(data->digest, digest, sizeof(data->digest)))
			return 0;
	} else {
		data->generation = generation;
		data->size = size;
		memcpy(data->digest, digest, sizeof(data->digest));
		data->flags |= DATA_IDENTITY;
	}
	if (requested)
		data->flags |= DATA_REQUESTED;
	for (uint32_t index = data->replica_head; index;
			index = table->replicas[index].data_next) {
		struct replica_record *known = &table->replicas[index];
		if (known->worker_slot == worker_slot &&
				known->session_epoch == session_epoch &&
				known->object_token == object_token)
			return 1;
	}
	uint32_t index = replica_allocate(table);
	if (!index)
		return 0;
	struct replica_record *replica = &table->replicas[index];
	replica->object_token = object_token;
	replica->session_epoch = session_epoch;
	replica->data_id = data_id;
	replica->generation = generation;
	replica->worker_slot = worker_slot;
	replica->flags = REPLICA_ACTIVE;
	replica->data_next = data->replica_head;
	if (data->replica_head)
		table->replicas[data->replica_head].data_previous = index;
	data->replica_head = index;
	data->replica_count++;
	struct worker_session *worker = &table->workers[worker_slot];
	replica->worker_next = worker->replica_head;
	if (worker->replica_head)
		table->replicas[worker->replica_head].worker_previous = index;
	worker->replica_head = index;
	table->stats.active_replicas++;
	if (table->stats.active_replicas > table->stats.peak_replicas)
		table->stats.peak_replicas = table->stats.active_replicas;
	wake_waiters(table, data_id, data, wake_waiter, argument);
	return 1;
}

int vine_datavine_replica_table_publish_batch(
		struct vine_datavine_replica_table *table,
		const struct vine_datavine_publish_record *records, size_t count,
		uint32_t worker_slot, uint64_t session_epoch,
		vine_datavine_waiter_callback_t wake_waiter, void *argument)
{
	if (!table || !records || !count ||
			!session_matches(table, worker_slot, session_epoch) ||
			table->workers[worker_slot].state != VINE_DATAVINE_SESSION_ACTIVE)
		return 0;
	for (size_t index = 0; index < count; index++) {
		const struct vine_datavine_publish_record *record = &records[index];
		struct data_record *data = data_lookup(table, record->data_id);
		if (!record->data_id || !record->generation ||
				!record->object_token || !data ||
				data->generation != record->generation ||
				((data->flags & DATA_IDENTITY) &&
				 (data->size != record->size ||
				  memcmp(data->digest, record->digest, 32))))
			return 0;
		for (size_t previous = 0; previous < index; previous++) {
			if (records[previous].data_id == record->data_id &&
					(records[previous].generation != record->generation ||
					 records[previous].size != record->size ||
					 memcmp(records[previous].digest, record->digest, 32)))
				return 0;
		}
	}
	size_t live_count = 0;
	for (size_t index = 0; index < count; index++) {
		struct data_record *data = data_lookup(table, records[index].data_id);
		if (data && (data->flags & DATA_LIVE))
			live_count++;
	}
	if (!reserve_replica_count(table, live_count))
		return 0;
	for (size_t index = 0; index < count; index++) {
		const struct vine_datavine_publish_record *record = &records[index];
		struct data_record *data = data_lookup(table, record->data_id);
		if (!data || !(data->flags & DATA_LIVE))
			continue;
		if (!vine_datavine_replica_table_publish(table, record->data_id,
				record->generation, record->size, record->digest, worker_slot,
				session_epoch, record->object_token, record->requested,
				wake_waiter, argument))
			abort();
	}
	return 1;
}

enum vine_datavine_resolve_status vine_datavine_replica_table_resolve(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, struct vine_datavine_replica_view *replicas,
		size_t capacity, size_t *count, uint64_t *size,
		unsigned char digest[32], int *persisted)
{
	if (count)
		*count = 0;
	struct data_record *data = data_lookup(table, data_id);
	if (!data)
		return VINE_DATAVINE_RESOLVE_UNKNOWN;
	if (!(data->flags & DATA_LIVE))
		return VINE_DATAVINE_RESOLVE_DEAD;
	if (!(data->flags & DATA_IDENTITY) ||
			(generation && generation != data->generation) ||
			!data->replica_count)
		return VINE_DATAVINE_RESOLVE_PENDING;
	if (size)
		*size = data->size;
	if (digest)
		memcpy(digest, data->digest, sizeof(data->digest));
	if (persisted)
		*persisted = !!(data->flags & DATA_PERSISTED);
	size_t found = 0;
	for (uint32_t index = data->replica_head; index;
			index = table->replicas[index].data_next) {
		struct replica_record *replica = &table->replicas[index];
		if (!(replica->flags & REPLICA_ACTIVE) ||
				!session_matches(table, replica->worker_slot, replica->session_epoch) ||
				table->workers[replica->worker_slot].state !=
						VINE_DATAVINE_SESSION_ACTIVE)
			continue;
		if (replicas && found < capacity) {
			replicas[found] = (struct vine_datavine_replica_view){
					.worker_slot = replica->worker_slot,
					.session_epoch = replica->session_epoch,
					.object_token = replica->object_token,
					.generation = replica->generation,
			};
		}
		found++;
	}
	if (count)
		*count = found < capacity ? found : capacity;
	return found ? VINE_DATAVINE_RESOLVE_AVAILABLE
			 : VINE_DATAVINE_RESOLVE_PENDING;
}

int vine_datavine_replica_table_wait(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, uint32_t worker_slot, uint64_t session_epoch,
		uint64_t request_id, uint32_t item_index)
{
	if (!table || !data_id || !request_id ||
			!session_matches(table, worker_slot, session_epoch))
		return 0;
	struct data_record *data = data_lookup(table, data_id);
	if (!data || !(data->flags & DATA_LIVE))
		return 0;
	for (uint32_t index = data->waiter_head; index;
			index = table->waiters[index].data_next) {
		struct waiter_record *known = &table->waiters[index];
		if (known->worker_slot == worker_slot &&
				known->session_epoch == session_epoch &&
				known->request_id == request_id &&
				known->item_index == item_index)
			return known->generation == generation;
	}
	uint32_t index = waiter_allocate(table);
	if (!index)
		return 0;
	struct waiter_record *waiter = &table->waiters[index];
	waiter->data_id = data_id;
	waiter->generation = generation;
	waiter->worker_slot = worker_slot;
	waiter->session_epoch = session_epoch;
	waiter->request_id = request_id;
	waiter->item_index = item_index;
	waiter->flags = WAITER_ACTIVE;
	waiter->data_next = data->waiter_head;
	if (data->waiter_head)
		table->waiters[data->waiter_head].data_previous = index;
	data->waiter_head = index;
	data->waiter_count++;
	struct worker_session *worker = &table->workers[worker_slot];
	waiter->worker_next = worker->waiter_head;
	if (worker->waiter_head)
		table->waiters[worker->waiter_head].worker_previous = index;
	worker->waiter_head = index;
	table->stats.active_waiters++;
	if (table->stats.active_waiters > table->stats.peak_waiters)
		table->stats.peak_waiters = table->stats.active_waiters;
	return 1;
}

int vine_datavine_replica_table_fault(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, uint32_t worker_slot, uint64_t session_epoch,
		uint64_t object_token, vine_datavine_data_callback_t last_replica,
		void *argument)
{
	struct data_record *data = data_lookup(table, data_id);
	if (!data || data->generation != generation)
		return 0;
	for (uint32_t index = data->replica_head; index;
			index = table->replicas[index].data_next) {
		struct replica_record *replica = &table->replicas[index];
		if (replica->worker_slot == worker_slot &&
				replica->session_epoch == session_epoch &&
				replica->object_token == object_token) {
			replica_remove(table, index);
			if (!data->replica_count && (data->flags & DATA_LIVE) &&
					!(data->flags & DATA_PERSISTED) && last_replica)
				last_replica(data_id, generation, argument);
			return 1;
		}
	}
	return 0;
}

int vine_datavine_replica_table_set_persisted(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation)
{
	struct data_record *data = data_lookup(table, data_id);
	if (!data || !(data->flags & DATA_IDENTITY) ||
			data->generation != generation)
		return 0;
	data->flags |= DATA_PERSISTED;
	return 1;
}

int vine_datavine_replica_table_set_recovery(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, int active)
{
	struct data_record *data = data_get(table, data_id);
	if (!data ||
			(data->generation && generation && data->generation != generation))
		return 0;
	if (active) {
		if (!(data->flags & DATA_LIVE)) {
			data->flags |= DATA_LIVE;
			table->stats.active_data++;
			if (table->stats.active_data > table->stats.peak_data)
				table->stats.peak_data = table->stats.active_data;
		}
		data->flags |= DATA_RECOVERY;
	} else {
		data->flags &= ~DATA_RECOVERY;
	}
	return 1;
}

int vine_datavine_replica_table_mark_dead(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, vine_datavine_replica_callback_t release_replica,
		vine_datavine_waiter_callback_t cancel_waiter, void *argument)
{
	struct data_record *data = data_lookup(table, data_id);
	if (!data ||
			(generation && data->generation && data->generation != generation))
		return 0;
	if (!(data->flags & DATA_LIVE))
		return 1;
	/* Logical GC is concurrent with physical producer replay. Keep the data
	 * live until Runtime clears DATA_RECOVERY and evaluates current consumers;
	 * otherwise a later replay of the same producer cannot register output. */
	if (data->flags & DATA_RECOVERY)
		return 1;
	data->flags &= ~(DATA_LIVE | DATA_RECOVERY);
	while (data->replica_head) {
		uint32_t index = data->replica_head;
		struct replica_record copy = table->replicas[index];
		struct vine_datavine_replica_view view = {
				.worker_slot = copy.worker_slot,
				.session_epoch = copy.session_epoch,
				.object_token = copy.object_token,
				.generation = copy.generation,
		};
		replica_remove(table, index);
		if (release_replica)
			release_replica(data_id, copy.generation, &view, argument);
	}
	while (data->waiter_head) {
		uint32_t index = data->waiter_head;
		struct waiter_record copy = table->waiters[index];
		waiter_remove(table, index);
		if (cancel_waiter)
			cancel_waiter(data_id, copy.generation, copy.worker_slot, copy.session_epoch, copy.request_id, copy.item_index, argument);
	}
	if (table->stats.active_data)
		table->stats.active_data--;
	return 1;
}

int vine_datavine_replica_table_stats(
		struct vine_datavine_replica_table *table,
		struct vine_datavine_replica_stats *stats)
{
	if (!table || !stats)
		return 0;
	*stats = table->stats;
	return 1;
}

int vine_datavine_replica_table_check(
		struct vine_datavine_replica_table *table)
{
	if (!table)
		return 0;
	uint64_t replicas = 0;
	uint64_t waiters = 0;
	uint64_t sessions = 0;
	for (uint32_t worker_slot = 0; worker_slot < table->worker_capacity;
			worker_slot++) {
		struct worker_session *worker = &table->workers[worker_slot];
		if (worker->state == VINE_DATAVINE_SESSION_UNUSED)
			continue;
		sessions++;
		uint32_t previous = 0;
		for (uint32_t index = worker->replica_head; index;
				index = table->replicas[index].worker_next) {
			if (index > table->replica_highwater ||
					!(table->replicas[index].flags & REPLICA_ACTIVE) ||
					table->replicas[index].worker_slot != worker_slot ||
					table->replicas[index].worker_previous != previous)
				return 0;
			previous = index;
			replicas++;
		}
		previous = 0;
		for (uint32_t index = worker->waiter_head; index;
				index = table->waiters[index].worker_next) {
			if (index > table->waiter_highwater ||
					!(table->waiters[index].flags & WAITER_ACTIVE) ||
					table->waiters[index].worker_slot != worker_slot ||
					table->waiters[index].worker_previous != previous)
				return 0;
			previous = index;
			waiters++;
		}
	}
	return replicas == table->stats.active_replicas &&
			waiters == table->stats.active_waiters &&
			sessions == table->stats.active_sessions;
}
