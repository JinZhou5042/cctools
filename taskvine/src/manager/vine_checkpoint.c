#include "vine_checkpoint.h"

#include "priority_queue.h"
#include "stringtools.h"
#include "vine_worker_info.h"
#include "vine_manager_put.h"
#include "vine_mount.h"
#include "debug.h"
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

    f->recovery_critical_time = 0;
    f->recovery_total_time = 0;
    f->penalty = 0;

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

    double candidates_penalty = 0;
    int64_t candidates_size = 0;

    struct list *candidates = list_create();
    struct vine_file *candidate;
    while ((candidate = priority_queue_pop(q->checkpointed_files))) {
        list_push_tail(candidates, candidate);
        available_bytes += candidate->size;
        candidates_penalty += candidate->penalty;
        candidates_size += candidate->size;
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

    double this_efficiency = f->penalty / f->size;
    double candidates_efficiency = candidates_penalty / candidates_size;

    // the cost is too high, we don't want to evict for this file
    if (candidates_efficiency > this_efficiency) {
        // pop back the files we popped from the checkpointed_files
        LIST_ITERATE(candidates, candidate)
        {
            priority_queue_push(q->checkpointed_files, candidate, -candidate->penalty / candidate->size);
        }
        list_delete(candidates);
        return 0;
    }
    
    // otherwise, we can evict the selected files for this file
    LIST_ITERATE(candidates, candidate)
    {
        debug(D_VINE | D_NOTICE, "Evicting file %s, size: %ld, penalty: %f\n", candidate->cached_name, candidate->size, candidate->penalty);
        vine_checkpoint_evict(q, candidate);
    }
    list_delete(candidates);

    return 1;
}

/* Get all reachable files in topological order from the given starting file */
static struct list *get_reachable_files_by_topo_order(struct vine_manager *q, struct vine_file *start_file)
{
    if (!start_file || start_file->type != VINE_TEMP || vine_checkpoint_checkpointed(q, start_file)) {
        return list_create();
    }

    /* Define file visit states for topological sort */
    typedef enum {
        VISIT_STATE_UNVISITED = 0,   /* Node hasn't been seen yet */
        VISIT_STATE_IN_PROGRESS = 1, /* Node is being processed (temp mark) */
        VISIT_STATE_COMPLETED = 2    /* Node processing complete (permanent mark) */
    } visit_state_t;

    struct list *result = list_create();
    struct hash_table *visited = hash_table_create(0, 0);
    
    /* Helper structure for DFS */
    struct dfs_state {
        struct vine_file *file;
        struct list *queue; /* List of unprocessed children */
    };

    /* Create a stack for DFS (non-recursive) */
    struct list *stack = list_create();

    /* Add starting node to the stack */
    struct dfs_state *initial = malloc(sizeof(struct dfs_state));
    initial->file = start_file;
    initial->queue = list_create();

    /* Pre-populate with all VINE_TEMP type children */
    char *child_name;
    struct vine_file *child;
    HASH_TABLE_ITERATE(start_file->child_temp_files, child_name, child) {
        if (child && child->type == VINE_TEMP) {
            list_push_tail(initial->queue, child);
        }
    }

    list_push_head(stack, initial);
    hash_table_insert(visited, start_file->cached_name, (void*)VISIT_STATE_IN_PROGRESS);

    while (list_size(stack) > 0) {
        struct dfs_state *current = list_peek_head(stack);
        
        if (list_size(current->queue) == 0) {
            /* All children processed, add to result and mark as completed */
            list_pop_head(stack);
            list_push_tail(result, current->file);
            hash_table_remove(visited, current->file->cached_name);
            hash_table_insert(visited, current->file->cached_name, (void*)VISIT_STATE_COMPLETED);
            list_delete(current->queue);
            free(current);
            continue;
        }

        /* Take next child from queue */
        struct vine_file *next_child = list_pop_head(current->queue);
        
        /* Check if this child has been visited */
        void *visit_state = hash_table_lookup(visited, next_child->cached_name);
        
        if (!visit_state) {
            /* Unvisited node - add to stack and mark as in progress */
            struct dfs_state *child_state = malloc(sizeof(struct dfs_state));
            child_state->file = next_child;
            child_state->queue = list_create();
            
            /* Pre-populate child queue with VINE_TEMP type files only */
            HASH_TABLE_ITERATE(next_child->child_temp_files, child_name, child) {
                if (child && child->type == VINE_TEMP && !vine_checkpoint_checkpointed(q, child)) {
                    list_push_tail(child_state->queue, child);
                }
            }
            
            list_push_head(stack, child_state);
            hash_table_insert(visited, next_child->cached_name, (void*)VISIT_STATE_IN_PROGRESS);
        } else if ((visit_state_t)visit_state == VISIT_STATE_IN_PROGRESS) {
            /* Cycle detected, skip this child (could log cycle warning here) */
            continue;
        }
        /* If completed, we just skip it */
    }
    
    hash_table_delete(visited);
    
    return result;
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
    vine_checkpoint_update_file_penalty(q, f);

    /* update all downstream files' recovery metrics */
    struct list *files_in_topo_order = get_reachable_files_by_topo_order(q, f);
    struct vine_file *current_file;
    
    LIST_ITERATE(files_in_topo_order, current_file) {
        if (!vine_checkpoint_checkpointed(q, current_file)) {
            /* calculate new recovery metrics from input files */
            vine_checkpoint_update_file_penalty(q, current_file);
            
            /* add to checkpoint queue for potential checkpointing */
            if (!hash_table_lookup(q->checkpoint_queue, current_file->cached_name)) {
                hash_table_insert(q->checkpoint_queue, current_file->cached_name, current_file);
            }
        }
    }
    
    list_delete(files_in_topo_order);

    return 1;
}

void vine_checkpoint_update_file_penalty(struct vine_manager *q, struct vine_file *f)
{
    if (!f || f->type != VINE_TEMP) {
        return;
    }

    f->recovery_critical_time = 0;
    f->recovery_total_time = 0;
    f->penalty = 0;

    struct vine_file *parent_file;
    char *parent_file_name;
    HASH_TABLE_ITERATE(f->parent_temp_files, parent_file_name, parent_file) {
        f->recovery_critical_time = MAX(f->recovery_critical_time, parent_file->recovery_critical_time);
        f->recovery_total_time += parent_file->recovery_total_time;
    }

    f->recovery_critical_time += f->producer_task_execution_time;
    f->recovery_total_time += f->producer_task_execution_time;

    f->penalty = (double)(0.1 * f->recovery_total_time) + (double)(0.9 * f->recovery_critical_time);
}

int vine_checkpoint_checkpointed(struct vine_manager *q, struct vine_file *f)
{
    return vine_file_replica_table_lookup(q->pbb_worker, f->cached_name) != NULL;
}

