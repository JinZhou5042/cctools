#include "vine_checkpoint.h"

#include "priority_queue.h"
#include "stringtools.h"
#include "vine_worker_info.h"
#include "vine_manager_put.h"
#include "vine_mount.h"
#include "vine_checkpoint_queue.h"
#include "vine_file_replica_table.h"

#define MAX(a, b) ((a) > (b) ? (a) : (b))


int vine_checkpoint_persist(struct vine_manager *q, struct vine_worker_info *source, struct vine_file *f)
{
    if (!q || !f || !source) {
        return 0;
    }

    // printf("Persisting file %s, size: %ld\n", f->cached_name, f->size);

    char *source_addr = string_format("%s/%s", source->transfer_url, f->cached_name);
    vine_manager_put_url_now(q, q->pbb_worker, source_addr, f);
    free(source_addr);
    
    priority_queue_push(q->checkpointed_files, f, -(double)(f->penalty / f->size));

    return 1;
}

struct vine_worker_info *vine_checkpoint_choose_source(struct vine_manager *q, struct vine_file *f)
{
    struct priority_queue *valid_sources = ensure_temp_file_transfer_sources(q, f, 1e6);
    if (!valid_sources) {
        return NULL;
    }

    struct vine_worker_info *source = priority_queue_pop(valid_sources);
    priority_queue_delete(valid_sources);

    return source;
}

int vine_checkpoint_release_pbb(struct vine_manager *q, struct vine_file *f)
{
    if (!q || !f) {
        return 0;
    }

    int64_t total_bytes = (int64_t)(q->pbb_worker->resources->disk.total * 1024 * 1024 * 0.95);
    int64_t actual_inuse_bytes = q->pbb_actual_inuse_cache;
    int64_t pending_transfer_bytes = q->pbb_worker->inuse_cache - q->pbb_actual_inuse_cache;

    int64_t available_bytes = total_bytes - actual_inuse_bytes - pending_transfer_bytes;
    int64_t this_file_size = (int64_t)(f->size);

    if (this_file_size <= available_bytes) {
        return 1;
    }

    // printf("total bytes: %ld, inuse cache: %ld, available bytes: %ld, this file size: %ld\n", total_bytes, actual_inuse_bytes, available_bytes, this_file_size);

    double this_file_cost = (double)f->penalty / f->size;

    double eviction_penalty = 0;
    struct list *candidates = list_create();
    struct vine_file *candidate;

    while ((candidate = priority_queue_pop(q->checkpointed_files))) {
        list_push_tail(candidates, candidate);
        available_bytes += candidate->size;
        eviction_penalty += (double)candidate->penalty / candidate->size;
        /* now, if the capacity suits, we can stop */
        if (available_bytes >= (int64_t)f->size) {
            break;
        }
    }

    if (list_size(candidates) == 0) {
        list_delete(candidates);
        return 0;
    }
    if (available_bytes < (int64_t)f->size) {
        list_delete(candidates);
        return 0;
    }


    // the cost is too high, we don't want to evict for this file
    if (eviction_penalty > this_file_cost) {
        // pop back the files we popped from the checkpointed_files
        LIST_ITERATE(candidates, candidate)
        {
            priority_queue_push(q->checkpointed_files, candidate, -(double)(candidate->penalty / candidate->size));
        }
        list_delete(candidates);
        return 0;
    }
    
    // otherwise, we can evict the selected files for this file
    LIST_ITERATE(candidates, candidate) 
    {
        printf("Evicting file %s, size: %ld\n", candidate->cached_name, candidate->size);
        vine_checkpoint_evict(q, candidate);
    }
    list_delete(candidates);

    return 1;
}

/* Helper function to recursively update downstream files */
static void update_downstream_recovery_metrics(struct vine_manager *q, struct vine_file *f) 
{
    if (!f || f->type != VINE_TEMP || vine_checkpoint_checkpointed(q, f)) {
        return;
    }

    /* calculate new recovery metrics from input files */
    uint64_t critical_time = 0;
    uint64_t total_time = 0;

    char *parent_file_name;
    struct vine_file *parent_file;
    HASH_TABLE_ITERATE(f->parent_temp_files, parent_file_name, parent_file) {
        critical_time = MAX(critical_time, parent_file->recovery_subgraph_critical_time);
        total_time += parent_file->recovery_subgraph_total_time;
    }
    critical_time += f->producer_task_execution_time;
    total_time += f->producer_task_execution_time;

    /* update this file's recovery metrics */
    f->recovery_subgraph_critical_time = critical_time;
    f->recovery_subgraph_total_time = total_time;
    f->penalty = (uint64_t)(0.5 * total_time + 0.5 * critical_time);

    /* add to checkpoint queue for potential checkpointing */
    if (!hash_table_lookup(q->checkpoint_queue, f->cached_name)) {
        hash_table_insert(q->checkpoint_queue, f->cached_name, f);
    }

    /* recursively update all downstream files */
    char *child_file_name;
    struct vine_file *child_file;
    HASH_TABLE_ITERATE(f->child_temp_files, child_file_name, child_file)
    {
        update_downstream_recovery_metrics(q, child_file);
    }
}

int vine_checkpoint_evict(struct vine_manager *q, struct vine_file *f)
{
    if (!q || !f || f->type != VINE_TEMP) {
        return 0;
    }
    if (!q->temp_file_checkpoint) {
        return 0;
    }

    /* skip if not checkpointed */
    if (!vine_checkpoint_checkpointed(q, f)) {
        return 0;
    }

    /* remove from checkpoint storage */
    delete_worker_file(q, q->pbb_worker, f->cached_name, 0, 0);

    /* update this file's recovery metrics after eviction */
    uint64_t critical_time = 0;
    uint64_t total_time = 0;
    struct vine_file *parent_file;
    char *parent_file_name;
    HASH_TABLE_ITERATE(f->parent_temp_files, parent_file_name, parent_file)
    {
        critical_time = MAX(critical_time, parent_file->recovery_subgraph_critical_time);
        total_time += parent_file->recovery_subgraph_total_time;
    }
    critical_time += f->producer_task_execution_time;
    total_time += f->producer_task_execution_time;

    f->recovery_subgraph_critical_time = critical_time;
    f->recovery_subgraph_total_time = total_time;
    f->penalty = (uint64_t)(0.5 * total_time + 0.5 * critical_time);

    /* recursively update all downstream files' recovery metrics */
    struct vine_file *child_file;
    char *child_file_name;
    HASH_TABLE_ITERATE(f->child_temp_files, child_file_name, child_file) {
        update_downstream_recovery_metrics(q, child_file);
    }

    return 1;
}

int vine_checkpoint_checkpointed(struct vine_manager *q, struct vine_file *f)
{
    return vine_file_replica_table_lookup(q->pbb_worker, f->cached_name) != NULL;
}

