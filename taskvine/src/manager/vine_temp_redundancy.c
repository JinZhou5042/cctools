#include "vine_temp_redundancy.h"
#include "priority_queue.h"
#include "vine_file.h"
#include "vine_worker_info.h"
#include "vine_file_replica_table.h"
#include "macros.h"
#include "stringtools.h"
#include "vine_manager.h"
#include "vine_manager_put.h"
#include "xxmalloc.h"

/*************************************************************/
/* Private Functions */
/*************************************************************/

static int is_checkpoint_worker(struct vine_manager *q, struct vine_worker_info *w)
{
    if (!q || !w) {
        return 0;
    }

    char *key;
    LIST_ITERATE(q->checkpoint_worker_list, key)
    {
        if (strcmp(key, w->hashkey) == 0) {
            return 1;
        }
    }
    return 0;
}

static struct vine_worker_info *get_best_source(struct vine_manager *q, struct vine_file *f)
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

static struct vine_worker_info *get_best_destination(struct vine_manager *q, struct vine_file *f)
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
	}

	struct vine_worker_info *best_destination = priority_queue_pop(valid_destinations);
	priority_queue_delete(valid_destinations);

	return best_destination;
}

static int file_has_checkpointed(struct vine_manager *q, struct vine_file *f)
{
	if (!f || f->type != VINE_TEMP || !q->enable_checkpointing || list_size(q->checkpoint_worker_list) == 0) {
		return 0;
	}

    char *key;
    struct vine_worker_info *w;
    LIST_ITERATE(q->checkpoint_worker_list, key)
    {
        w = hash_table_lookup(q->worker_table, key);
        if (!w) {
            continue;
        }
        if (vine_file_replica_table_lookup(w, f->cached_name)) {
            return 1;
        }
    }
    return 0;
}

static int replicate_file(struct vine_manager *q, struct vine_file *f, struct vine_worker_info *source, struct vine_worker_info *destination)
{
	if (!q || !f || f->type != VINE_TEMP || !source || !destination) {
		return 0;
	}

	char *source_addr = string_format("%s/%s", source->transfer_url, f->cached_name);
	vine_manager_put_url_now(q, destination, source, source_addr, f);
	free(source_addr);

	return 1;
}

/*************************************************************/
/* Public Functions */
/*************************************************************/

int vine_temp_redundancy_enqueue_file_for_replication(struct vine_manager *q, char *cachename)
{
	if (!q || !cachename) {
		return 0;
	}

	struct vine_file *f = hash_table_lookup(q->file_table, cachename);
	if (!f || f->type != VINE_TEMP) {
		return 0;
	}

	int current_replica_count = vine_file_replica_count(q, f);
	if (current_replica_count == 0 || current_replica_count >= q->temp_replica_count) {
		return 0;
	}

	list_push_tail(q->temp_files_to_replicate, f);

	return 1;
}

int vine_temp_redundancy_consider_replication(struct vine_manager *q)
{
	if (!q) {
		return 0;
	}

	int processed = 0;
	int iter_count = 0;
	int iter_depth = MIN(q->attempt_schedule_depth, list_size(q->temp_files_to_replicate));

	struct vine_file *f;
	while ((f = list_pop_head(q->temp_files_to_replicate)) && iter_count++ < iter_depth) {
		if (!f || f->type != VINE_TEMP) {
			continue;
		}

		/* skip if the file has been checkpointed */
		if (file_has_checkpointed(q, f)) {
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
		struct vine_worker_info *source = get_best_source(q, f);
		if (!source) {
			list_push_tail(q->temp_files_to_replicate, f);
			continue;
		}
		struct vine_worker_info *destination = get_best_destination(q, f);
		if (!destination) {
			list_push_tail(q->temp_files_to_replicate, f);
			continue;
		}

		replicate_file(q, f, source, destination);
		processed++;
	}

	return processed;
}
