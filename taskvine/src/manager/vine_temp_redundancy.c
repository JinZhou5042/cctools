#include "vine_temp_redundancy.h"
#include "priority_queue.h"
#include "vine_file.h"
#include "vine_worker_info.h"
#include "vine_file_replica_table.h"
#include "macros.h"
#include "stringtools.h"
#include "vine_manager.h"
#include "debug.h"
#include "random.h"
#include "vine_manager_put.h"
#include "xxmalloc.h"

/*************************************************************/
/* Private Functions */
/*************************************************************/

int is_checkpoint_worker(struct vine_manager *q, struct vine_worker_info *w)
{
	if (!q || !w || !w->features) {
		return 0;
	}

	return hash_table_lookup(w->features, "checkpoint-worker") != NULL;
}

static struct vine_worker_info *get_best_source_worker(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP) {
		return NULL;
	}

	struct set *sources = hash_table_lookup(q->file_worker_table, f->cached_name);
	if (!sources) {
		return NULL;
	}

	struct priority_queue *valid_sources_queue = priority_queue_create(0);
	struct vine_worker_info *w = NULL;
	SET_ITERATE(sources, w)
	{
		/* skip if transfer port is not active or in draining mode */
		if (!w->transfer_port_active || w->draining) {
			continue;
		}
		/* skip if incoming transfer counter is too high */
		if (w->outgoing_xfer_counter >= q->worker_source_max_transfers) {
			continue;
		}
		/* skip if the worker does not have this file */
		struct vine_file_replica *replica = vine_file_replica_table_lookup(w, f->cached_name);
		if (!replica) {
			continue;
		}
		/* skip if the file is not ready */
		if (replica->state != VINE_FILE_REPLICA_STATE_READY) {
			continue;
		}
		/* those with less outgoing_xfer_counter are preferred */
		priority_queue_push(valid_sources_queue, w, -w->outgoing_xfer_counter);
	}

	struct vine_worker_info *best_source = priority_queue_pop(valid_sources_queue);
	priority_queue_delete(valid_sources_queue);

	return best_source;
}

static struct vine_worker_info *get_best_destination_worker(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP) {
		return NULL;
	}

	struct priority_queue *valid_destinations = priority_queue_create(0);

	char *key;
	struct vine_worker_info *w;
	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		/* skip if transfer port is not active or in draining mode */
		if (!w->transfer_port_active || w->draining) {
			continue;
		}
		/* skip if the incoming transfer counter is too high */
		if (w->incoming_xfer_counter >= q->worker_source_max_transfers) {
			continue;
		}
		/* skip if the worker is a checkpoint worker */
		if (is_checkpoint_worker(q, w)) {
			continue;
		}
		/* skip if the worker already has this file */
		struct vine_file_replica *replica = vine_file_replica_table_lookup(w, f->cached_name);
		if (replica) {
			continue;
		}
		/* skip if the worker does not have enough disk space */
		int64_t available_disk_space = (int64_t)MEGABYTES_TO_BYTES(w->resources->disk.total) - w->inuse_cache;
		if ((int64_t)f->size > available_disk_space) {
			continue;
		}
		/* workers with more available disk space are preferred */
		priority_queue_push(valid_destinations, w, available_disk_space);
		switch (q->replica_placement_policy) {
		case VINE_REPLICA_PLACEMENT_POLICY_RANDOM:
			priority_queue_push(valid_destinations, w, random_double());
			break;
		case VINE_REPLICA_PLACEMENT_POLICY_DISK_LOAD:
			priority_queue_push(valid_destinations, w, available_disk_space);
			break;
		case VINE_REPLICA_PLACEMENT_POLICY_TRANSFER_LOAD:
			priority_queue_push(valid_destinations, w, -w->incoming_xfer_counter);
			break;
		}
	}

	struct vine_worker_info *best_destination = priority_queue_pop(valid_destinations);
	priority_queue_delete(valid_destinations);

	return best_destination;
}

static struct vine_worker_info *get_best_checkpoint_worker(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP) {
		return NULL;
	}

	struct priority_queue *valid_checkpoint_workers = priority_queue_create(0);
	char *key;
	struct vine_worker_info *w;
	HASH_TABLE_ITERATE(q->worker_table, key, w)
	{
		/* skip if not a checkpoint worker */
		if (!is_checkpoint_worker(q, w)) {
			continue;
		}
		/* skip if transfer port is not active or in draining mode */
		if (!w->transfer_port_active || w->draining) {
			continue;
		}
		/* skip if incoming transfer counter is too high */
		if (w->incoming_xfer_counter >= q->worker_source_max_transfers) {
			continue;
		}
		/* skip if the worker already has this file */
		struct vine_file_replica *replica = vine_file_replica_table_lookup(w, f->cached_name);
		if (replica) {
			debug(D_NOTICE, "checkpoint worker %s already has file %s", key, f->cached_name);
			continue;
		}
		/* skip if the worker does not have enough disk space */
		int64_t available_disk_space = (int64_t)MEGABYTES_TO_BYTES(w->resources->disk.total) - w->inuse_cache;
		if ((int64_t)f->size > available_disk_space) {
			continue;
		}
		/* workers with less incoming transfer counter are preferred */
		priority_queue_push(valid_checkpoint_workers, w, available_disk_space);
	}

	struct vine_worker_info *best_checkpoint_worker = priority_queue_pop(valid_checkpoint_workers);
	priority_queue_delete(valid_checkpoint_workers);

	return best_checkpoint_worker;
}

int file_has_been_checkpointed(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP) {
		return 0;
	}

	/* check if the file has been checkpointed by any checkpoint worker */
	struct set *source_workers = hash_table_lookup(q->file_worker_table, f->cached_name);
	if (!source_workers) {
		return 0;
	}

	struct vine_worker_info *source_worker;
	SET_ITERATE(source_workers, source_worker)
	{
		/* skip if not a checkpoint worker */
		if (!is_checkpoint_worker(q, source_worker)) {
			continue;
		}
		/* it is already checkpointed */
		return 1;
	}

	return 0;
}

static int replicate_file(struct vine_manager *q, struct vine_file *f, struct vine_worker_info *source_worker, struct vine_worker_info *destination_worker)
{
	if (!q || !f || f->type != VINE_TEMP || !source_worker || !destination_worker) {
		return 0;
	}

	char *source_addr = string_format("%s/%s", source_worker->transfer_url, f->cached_name);
	vine_manager_put_url_now(q, destination_worker, source_worker, source_addr, f);
	free(source_addr);

	return 1;
}

static int consider_replication(struct vine_manager *q)
{
	if (!q) {
		return 0;
	}

	int processed = 0;
	int iter_count = 0;
	int iter_depth = MIN(q->attempt_schedule_depth, priority_queue_size(q->temp_files_to_replicate));
	struct list *skipped = list_create();

	struct vine_file *f;
	while ((f = priority_queue_pop(q->temp_files_to_replicate)) && (iter_count++ < iter_depth)) {
		if (!f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
			continue;
		}

		/* skip if the file has enough replicas or no replicas */
		int current_replica_count = vine_file_replica_count(q, f);
		if (current_replica_count >= q->temp_replica_count || current_replica_count == 0) {
			continue;
		}
		/* skip if the file has no ready replicas */
		int current_ready_replica_count = vine_file_replica_table_count_replicas(q, f->cached_name, VINE_FILE_REPLICA_STATE_READY);
		if (current_ready_replica_count == 0) {
			continue;
		}

		/* if reach here, it means the file needs to be replicated and there is at least one ready replica. */
		struct vine_worker_info *source_worker = get_best_source_worker(q, f);
		if (!source_worker) {
			list_push_tail(skipped, f);
			continue;
		}
		struct vine_worker_info *destination_worker = get_best_destination_worker(q, f);
		if (!destination_worker) {
			list_push_tail(skipped, f);
			continue;
		}

		replicate_file(q, f, source_worker, destination_worker);
		processed++;

		/* push back and keep evaluating the same file with a lower priority, until no more source
		 * or destination workers are available, or the file has enough replicas. */
		enqueue_file_for_replication(q, f);
	}

	while ((f = list_pop_head(skipped))) {
		enqueue_file_for_replication(q, f);
	}
	list_delete(skipped);

	return processed;
}

static int consider_checkpointing(struct vine_manager *q)
{
	if (!q) {
		return 0;
	}

	int processed = 0;
	int iter_count = 0;
	int iter_depth = MIN(q->attempt_schedule_depth, list_size(q->temp_files_to_checkpoint));

	struct vine_file *f;
	while ((f = list_pop_head(q->temp_files_to_checkpoint)) && (iter_count++ < iter_depth)) {
		if (!f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
			continue;
		}

		/* skip if this file has been checkpointed */
		if (file_has_been_checkpointed(q, f)) {
			continue;
		}
		/* skip if no ready source at all */
		int current_ready_replica_count = vine_file_replica_table_count_replicas(q, f->cached_name, VINE_FILE_REPLICA_STATE_READY);
		if (current_ready_replica_count == 0) {
			continue;
		}

		/* get the best source worker */
		struct vine_worker_info *source_worker = get_best_source_worker(q, f);
		if (!source_worker) {
			list_push_tail(q->temp_files_to_checkpoint, f);
			continue;
		}
		/* get the best checkpoint worker */
		struct vine_worker_info *checkpoint_worker = get_best_checkpoint_worker(q, f);
		if (!checkpoint_worker) {
			list_push_tail(q->temp_files_to_checkpoint, f);
			continue;
		}

		replicate_file(q, f, source_worker, checkpoint_worker);
		processed++;
	}

	return processed;
}

/*************************************************************/
/* Public Functions */
/*************************************************************/

int vine_temp_redundancy_replicate_file(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
		return 0;
	}

	return enqueue_file_for_replication(q, f);
}

int vine_temp_redundancy_checkpoint_file(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
		return 0;
	}

	return enqueue_file_for_checkpointing(q, f);
}

int enqueue_file_for_replication(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
		return 0;
	}

	int current_replica_count = vine_file_replica_count(q, f);
	if (current_replica_count == 0 || current_replica_count >= q->temp_replica_count) {
		return 0;
	}

	priority_queue_push(q->temp_files_to_replicate, f, -current_replica_count);

	return 1;
}

int enqueue_file_for_checkpointing(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
		return 0;
	}

	list_push_tail(q->temp_files_to_checkpoint, f);

	return 1;
}

int vine_temp_redundancy_handle_file_lost(struct vine_manager *q, char *cachename)
{
	if (!q || !cachename) {
		return 0;
	}

	struct vine_file *f = hash_table_lookup(q->file_table, cachename);
	if (!f || f->type != VINE_TEMP || f->state != VINE_FILE_STATE_CREATED) {
		return 0;
	}

	enqueue_file_for_replication(q, f);

	// enqueue_file_for_checkpointing(q, f);

	return 1;
}

int vine_temp_redundancy_process(struct vine_manager *q)
{
	if (!q) {
		return 0;
	}

	int processed = 0;
	processed += consider_replication(q);
	processed += consider_checkpointing(q);

	return processed;
}

void vine_set_replica_placement_policy(struct vine_manager *q, vine_replica_placement_policy_t policy)
{
	if (!q) {
		return;
	}

	switch (policy) {
	case VINE_REPLICA_PLACEMENT_POLICY_RANDOM:
		debug(D_VINE, "Setting replica placement policy to RANDOM");
		q->replica_placement_policy = policy;
		break;
	case VINE_REPLICA_PLACEMENT_POLICY_DISK_LOAD:
		debug(D_VINE, "Setting replica placement policy to DISK_LOAD");
		q->replica_placement_policy = policy;
		break;
	case VINE_REPLICA_PLACEMENT_POLICY_TRANSFER_LOAD:
		q->replica_placement_policy = policy;
		break;
	default:
		debug(D_ERROR, "Invalid replica placement policy: %d", policy);
	}
}