/* Compact single-writer DataVine worker-replica directory. */
#ifndef VINE_DATAVINE_REPLICA_TABLE_H
#define VINE_DATAVINE_REPLICA_TABLE_H

#include <stddef.h>
#include <stdint.h>

struct vine_datavine_replica_table;

enum vine_datavine_resolve_status {
	VINE_DATAVINE_RESOLVE_UNKNOWN = 0,
	VINE_DATAVINE_RESOLVE_PENDING = 1,
	VINE_DATAVINE_RESOLVE_AVAILABLE = 2,
	VINE_DATAVINE_RESOLVE_DEAD = 3,
};

struct vine_datavine_replica_view {
	uint32_t worker_slot;
	uint64_t session_epoch;
	uint64_t object_token;
	uint32_t generation;
};

struct vine_datavine_replica_stats {
	uint64_t active_data;
	uint64_t active_replicas;
	uint64_t active_waiters;
	uint64_t active_sessions;
	uint64_t peak_data;
	uint64_t peak_replicas;
	uint64_t peak_waiters;
};

struct vine_datavine_publish_record {
	uint64_t data_id;
	uint64_t size;
	uint64_t object_token;
	unsigned char digest[32];
	uint32_t generation;
	uint32_t requested;
};

typedef void (*vine_datavine_data_callback_t)(
		uint64_t data_id, uint32_t generation, void *argument);
typedef void (*vine_datavine_waiter_callback_t)(
		uint64_t data_id, uint32_t generation, uint32_t worker_slot,
		uint64_t session_epoch, uint64_t request_id, uint32_t item_index,
		void *argument);
typedef void (*vine_datavine_replica_callback_t)(
		uint64_t data_id, uint32_t generation,
		const struct vine_datavine_replica_view *replica, void *argument);

/*
The table is intentionally not internally locked. One Controller metadata
event loop owns all mutation. Data and replica records live in geometric
arenas and refer to each other by integer index, so arena growth cannot
invalidate links and no hot-path operation allocates a per-event object.
*/
struct vine_datavine_replica_table *vine_datavine_replica_table_create(void);
void vine_datavine_replica_table_delete(
		struct vine_datavine_replica_table *table);

int vine_datavine_replica_table_session_open(
		struct vine_datavine_replica_table *table, uint32_t worker_slot,
		uint64_t session_epoch);
int vine_datavine_replica_table_session_active(
		struct vine_datavine_replica_table *table, uint32_t worker_slot,
		uint64_t session_epoch);
int vine_datavine_replica_table_session_lost(
		struct vine_datavine_replica_table *table, uint32_t worker_slot,
		uint64_t session_epoch, vine_datavine_data_callback_t last_replica,
		vine_datavine_waiter_callback_t cancelled_waiter, void *argument);

/* Register the exact generation which a valid physical producer may publish.
 * A retry may replace it only before content identity is established. */
int vine_datavine_replica_table_expect(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t *generation);

/* First valid publication fixes size and digest for a generation. Repeated
 * publication is idempotent. A different digest for the same generation is
 * rejected. */
int vine_datavine_replica_table_publish(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, uint64_t size, const unsigned char digest[32],
		uint32_t worker_slot, uint64_t session_epoch, uint64_t object_token,
		vine_datavine_waiter_callback_t wake_waiter,
		void *argument);
/* Validate and reserve the complete bounded batch before making the first
 * record visible. Invalid/conflicting input therefore has no partial effect.
 * A correctly-versioned record which was made DEAD before its asynchronous
 * advertisement arrived is accepted as a tombstone and is not reinserted. */
int vine_datavine_replica_table_publish_batch(
		struct vine_datavine_replica_table *table,
		const struct vine_datavine_publish_record *records, size_t count,
		uint32_t worker_slot, uint64_t session_epoch,
		vine_datavine_waiter_callback_t wake_waiter, void *argument);

enum vine_datavine_resolve_status vine_datavine_replica_table_resolve(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, struct vine_datavine_replica_view *replicas,
		size_t capacity, size_t *count, uint64_t *size,
		unsigned char digest[32], int *persisted);

/* generation zero means wait for the first/current generation. */
int vine_datavine_replica_table_wait(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, uint32_t worker_slot, uint64_t session_epoch,
		uint64_t request_id, uint32_t item_index);

int vine_datavine_replica_table_fault(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, uint32_t worker_slot, uint64_t session_epoch,
		uint64_t object_token, vine_datavine_data_callback_t last_replica,
		void *argument);
int vine_datavine_replica_table_set_persisted(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation);
/* Rebuild the durable half of one DataID after Controller journal replay.
 * This is intentionally lazy: the catalog is authoritative for persisted
 * payloads, while the dense replica table is hydrated only when a Worker
 * resolves the DataID. */
int vine_datavine_replica_table_restore_persisted(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, uint64_t size, const unsigned char digest[32]);
/* Background Controller backup admission is represented by one bit in the
 * dense DataID record.  Return 1 when the caller must enqueue the DataID, 2
 * when it is already queued or persisted, and 0 for an invalid generation. */
int vine_datavine_replica_table_queue_backup(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation);
int vine_datavine_replica_table_clear_backup(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation);
int vine_datavine_replica_table_set_recovery(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, int active);

/* Mark a DataID dead, remove it from resolve immediately, and return every
 * physical replica through release_replica. Repeating the same generation is
 * idempotent. Worker deletion is asynchronous; a later exact batch
 * advertisement is consumed as a tombstone, while stale generations fail. */
int vine_datavine_replica_table_mark_dead(
		struct vine_datavine_replica_table *table, uint64_t data_id,
		uint32_t generation, vine_datavine_replica_callback_t release_replica,
		vine_datavine_waiter_callback_t cancel_waiter, void *argument);

int vine_datavine_replica_table_stats(
		struct vine_datavine_replica_table *table,
		struct vine_datavine_replica_stats *stats);
int vine_datavine_replica_table_check(
		struct vine_datavine_replica_table *table);

#endif
