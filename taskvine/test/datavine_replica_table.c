#include "vine_datavine_replica_table.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

struct observations {
	uint64_t lost[16];
	size_t lost_count;
	uint64_t woken[16];
	size_t woken_count;
	uint64_t released[16];
	size_t released_count;
	size_t cancelled;
};

static void lost_data(uint64_t data_id, uint32_t generation, void *argument)
{
	struct observations *observations = argument;
	assert(generation == 3 || generation == 7 || generation == 9);
	observations->lost[observations->lost_count++] = data_id;
}

static void waiter_event(uint64_t data_id, uint32_t generation,
		uint32_t worker_slot, uint64_t session_epoch, uint64_t request_id,
		uint32_t item_index, void *argument)
{
	struct observations *observations = argument;
	assert(worker_slot == 2);
	assert(session_epoch == 202);
	assert(request_id == 55);
	assert(item_index == 3);
	assert(generation == 7 || generation == 0);
	observations->woken[observations->woken_count++] = data_id;
}

static void cancelled_waiter(uint64_t data_id, uint32_t generation,
		uint32_t worker_slot, uint64_t session_epoch, uint64_t request_id,
		uint32_t item_index, void *argument)
{
	(void)data_id;
	(void)generation;
	(void)worker_slot;
	(void)session_epoch;
	(void)request_id;
	(void)item_index;
	struct observations *observations = argument;
	observations->cancelled++;
}

static void released_replica(uint64_t data_id, uint32_t generation,
		const struct vine_datavine_replica_view *replica, void *argument)
{
	struct observations *observations = argument;
	assert(generation == 7);
	assert(replica->object_token);
	observations->released[observations->released_count++] = data_id;
}

int main(void)
{
	struct vine_datavine_replica_table *table =
			vine_datavine_replica_table_create();
	assert(table);
	assert(vine_datavine_replica_table_session_open(table, 1, 101));
	assert(vine_datavine_replica_table_session_open(table, 2, 202));
	assert(vine_datavine_replica_table_session_open(table, 3, 303));
	assert(vine_datavine_replica_table_check(table));

	unsigned char digest[32];
	unsigned char other[32];
	memset(digest, 0xa5, sizeof(digest));
	memset(other, 0x5a, sizeof(other));
	struct observations observations = {0};

	uint32_t generation = 7;
	assert(vine_datavine_replica_table_expect(table, 5000001, &generation));
	assert(generation == 7);
	assert(vine_datavine_replica_table_wait(
			table, 5000001, 0, 2, 202, 55, 3));
	assert(vine_datavine_replica_table_publish(table, 5000001, 7, 4096, digest, 1, 101, 1001, waiter_event, &observations));
	assert(observations.woken_count == 1 && observations.woken[0] == 5000001);
	/* Exact retransmission is idempotent; conflicting content is rejected. */
	assert(vine_datavine_replica_table_publish(table, 5000001, 7, 4096, digest, 1, 101, 1001, waiter_event, &observations));
	assert(!vine_datavine_replica_table_publish(table, 5000001, 7, 4096, other, 1, 101, 1002, waiter_event, &observations));
	assert(vine_datavine_replica_table_publish(table, 5000001, 7, 4096, digest, 3, 303, 3001, waiter_event, &observations));

	struct vine_datavine_replica_view views[4];
	size_t count = 0;
	uint64_t size = 0;
	unsigned char resolved_digest[32];
	int persisted = -1;
	assert(vine_datavine_replica_table_resolve(table, 5000001, 7, views, 4, &count, &size, resolved_digest, &persisted) ==
			VINE_DATAVINE_RESOLVE_AVAILABLE);
	assert(count == 2 && size == 4096 && !persisted);
	assert(!memcmp(digest, resolved_digest, sizeof(digest)));

	/* One replica fault does not request replay. Losing the last one does. */
	assert(vine_datavine_replica_table_fault(table, 5000001, 7, 1, 101,
			1001, lost_data, &observations));
	assert(observations.lost_count == 0);
	assert(vine_datavine_replica_table_session_lost(table, 3, 303, lost_data, cancelled_waiter, &observations));
	assert(observations.lost_count == 1 && observations.lost[0] == 5000001);
	assert(vine_datavine_replica_table_resolve(table, 5000001, 7, views, 4, &count, &size, resolved_digest, &persisted) ==
			VINE_DATAVINE_RESOLVE_PENDING);

	/* A compact queued bit suppresses duplicate background admission.  Once
	 * persisted, loss of the only Worker replica still resolves AVAILABLE with
	 * zero Worker sources so the Controller fallback can be selected. */
	generation = 3;
	assert(vine_datavine_replica_table_expect(table, 6000001, &generation));
	assert(vine_datavine_replica_table_publish(table, 6000001, 3, 8, digest,
			2, 202, 6001, 0, 0));
	assert(vine_datavine_replica_table_queue_backup(table, 6000001, 3) == 1);
	assert(vine_datavine_replica_table_queue_backup(table, 6000001, 3) == 2);
	assert(vine_datavine_replica_table_clear_backup(table, 6000001, 3));
	assert(vine_datavine_replica_table_queue_backup(table, 6000001, 3) == 1);
	assert(vine_datavine_replica_table_set_persisted(table, 6000001, 3));
	assert(vine_datavine_replica_table_fault(table, 6000001, 3, 2, 202,
			6001, lost_data, &observations));
	count = 4;
	persisted = 0;
	assert(vine_datavine_replica_table_resolve(table, 6000001, 3, views, 4,
			&count, &size, resolved_digest, &persisted) ==
			VINE_DATAVINE_RESOLVE_AVAILABLE);
	assert(count == 0 && persisted == 1 && size == 8);
	assert(observations.lost_count == 1);
	assert(vine_datavine_replica_table_mark_dead(
			table, 6000001, 3, 0, 0, 0));

	/* A stale session cannot publish after reconnect under a new epoch. */
	assert(vine_datavine_replica_table_session_open(table, 1, 111));
	assert(!vine_datavine_replica_table_publish(table, 7000001, 9, 1, digest, 1, 101, 7001, 0, 0));
	generation = 9;
	assert(vine_datavine_replica_table_expect(table, 7000001, &generation));
	assert(vine_datavine_replica_table_publish(table, 7000001, 9, 1, digest, 1, 111, 7001, 0, 0));

	generation = 7;
	assert(vine_datavine_replica_table_expect(table, 42, &generation));
	assert(vine_datavine_replica_table_publish(table, 42, 7, 16, digest, 2, 202, 4200, 0, 0));
	assert(vine_datavine_replica_table_set_persisted(table, 42, 7));

	/* Controller restart keeps the durable catalog but deliberately rebuilds
	 * the dense runtime directory lazily on the first resolve. */
	unsigned char restored_digest[32];
	memset(restored_digest, 0x5a, sizeof(restored_digest));
	assert(vine_datavine_replica_table_restore_persisted(
			table, 77, 9, 1234, restored_digest));
	count = 0;
	size = 0;
	persisted = 0;
	assert(vine_datavine_replica_table_resolve(table, 77, 9, views, 4,
			&count, &size, digest, &persisted) ==
			VINE_DATAVINE_RESOLVE_AVAILABLE);
	assert(count == 0 && size == 1234 && persisted == 1);
	assert(!memcmp(digest, restored_digest, sizeof(digest)));
	assert(vine_datavine_replica_table_restore_persisted(
			table, 77, 9, 1234, restored_digest));
	assert(!vine_datavine_replica_table_restore_persisted(
			table, 77, 10, 1234, restored_digest));
	assert(vine_datavine_replica_table_wait(table, 42, 7, 2, 202, 55, 3));
	assert(vine_datavine_replica_table_mark_dead(table, 42, 7, released_replica, cancelled_waiter, &observations));
	assert(vine_datavine_replica_table_mark_dead(table, 42, 7, released_replica, cancelled_waiter, &observations));
	assert(observations.released_count == 1 && observations.released[0] == 42);
	assert(observations.cancelled == 1);
	assert(vine_datavine_replica_table_resolve(table, 42, 7, views, 4, &count, 0, 0, 0) == VINE_DATAVINE_RESOLVE_DEAD);
	assert(!vine_datavine_replica_table_publish(table, 42, 7, 16, digest, 2, 202, 4200, 0, 0));
	/* Batched publication is asynchronous with respect to task completion.
	 * An exact generation retired in the meantime is a harmless tombstone. */
	struct vine_datavine_publish_record late = {
			.data_id = 42,
			.size = 16,
			.object_token = 4200,
			.generation = 7,
			.requested = 1,
	};
	memcpy(late.digest, digest, sizeof(digest));
	assert(vine_datavine_replica_table_publish_batch(
			table, &late, 1, 2, 202, 0, 0));
	assert(vine_datavine_replica_table_resolve(table, 42, 7, views, 4,
			&count, 0, 0, 0) == VINE_DATAVINE_RESOLVE_DEAD);

	/* One delayed old generation must not poison a valid publication sharing
	 * the same Agent metadata batch. */
	generation = 7;
	assert(vine_datavine_replica_table_expect(table, 9000001, &generation));
	assert(vine_datavine_replica_table_publish(table, 9000001, 7, 64, digest,
			2, 202, 9001, 0, 0));
	assert(vine_datavine_replica_table_mark_dead(
			table, 9000001, 7, 0, 0, 0));
	assert(vine_datavine_replica_table_set_recovery(table, 9000001, 0, 1));
	generation = 8;
	assert(vine_datavine_replica_table_expect(table, 9000001, &generation));
	generation = 1;
	assert(vine_datavine_replica_table_expect(table, 9000002, &generation));
	struct vine_datavine_publish_record mixed[2] = {
		{
			.data_id = 9000001,
			.size = 64,
			.object_token = 9001,
			.generation = 7,
		},
		{
			.data_id = 9000002,
			.size = 64,
			.object_token = 9002,
			.generation = 1,
		},
	};
	memcpy(mixed[0].digest, digest, sizeof(digest));
	memcpy(mixed[1].digest, digest, sizeof(digest));
	assert(vine_datavine_replica_table_publish_batch(
			table, mixed, 2, 2, 202, 0, 0));
	assert(vine_datavine_replica_table_resolve(table, 9000001, 8, views, 4,
			&count, 0, 0, 0) == VINE_DATAVINE_RESOLVE_PENDING);
	assert(vine_datavine_replica_table_resolve(table, 9000002, 1, views, 4,
			&count, 0, 0, 0) == VINE_DATAVINE_RESOLVE_AVAILABLE);
	assert(vine_datavine_replica_table_set_recovery(table, 9000001, 0, 0));
	assert(vine_datavine_replica_table_mark_dead(
			table, 9000001, 8, 0, 0, 0));
	assert(vine_datavine_replica_table_mark_dead(
			table, 9000002, 1, 0, 0, 0));

	/* Recovery may temporarily resurrect an intermediate that logical GC
	 * retired, replace its empty generation, and retire it again. */
	struct vine_datavine_replica_stats stats;
	assert(vine_datavine_replica_table_stats(table, &stats));
	uint64_t active_before_recovery = stats.active_data;
	generation = 7;
	assert(vine_datavine_replica_table_expect(table, 8000001, &generation));
	assert(vine_datavine_replica_table_publish(table, 8000001, 7, 32, digest,
			2, 202, 8001, 0, 0));
	assert(vine_datavine_replica_table_mark_dead(
			table, 8000001, 7, 0, 0, 0));
	assert(vine_datavine_replica_table_set_recovery(table, 8000001, 0, 1));
	assert(vine_datavine_replica_table_set_recovery(table, 8000001, 0, 1));
	assert(vine_datavine_replica_table_mark_dead(
			table, 8000001, 7, 0, 0, 0));
	assert(vine_datavine_replica_table_stats(table, &stats));
	assert(stats.active_data == active_before_recovery + 1);
	assert(vine_datavine_replica_table_resolve(table, 8000001, 0, views, 4,
			&count, 0, 0, 0) == VINE_DATAVINE_RESOLVE_PENDING);
	assert(vine_datavine_replica_table_resolve(table, 8000001, 0, views, 4,
			&count, 0, 0, 0) == VINE_DATAVINE_RESOLVE_PENDING);
	generation = 8;
	assert(vine_datavine_replica_table_expect(table, 8000001, &generation));
	assert(generation == 8);
	assert(vine_datavine_replica_table_publish(table, 8000001, 8, 32, digest,
			2, 202, 8002, 0, 0));
	assert(vine_datavine_replica_table_set_recovery(table, 8000001, 0, 0));
	assert(vine_datavine_replica_table_mark_dead(
			table, 8000001, 8, 0, 0, 0));
	assert(vine_datavine_replica_table_stats(table, &stats));
	assert(stats.active_data == active_before_recovery);

	assert(vine_datavine_replica_table_stats(table, &stats));
	assert(stats.active_sessions == 2);
	assert(stats.active_replicas == 1);
	assert(stats.active_waiters == 0);
	assert(stats.peak_replicas == 2);
	assert(vine_datavine_replica_table_check(table));
	vine_datavine_replica_table_delete(table);
	puts("DataVine replica table PASS");
	return 0;
}
