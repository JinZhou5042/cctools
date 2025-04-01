#include "vine_checkpoint.h"

#include "priority_queue.h"
#include "stringtools.h"
#include "vine_worker_info.h"
#include "vine_manager_put.h"
#include "vine_task.h"
#include "vine_mount.h"
#include "vine_checkpoint_queue.h"


#define MAX(a, b) ((a) > (b) ? (a) : (b))


int vine_checkpoint_persist(struct vine_manager *q, struct vine_worker_info *source, struct vine_file *f)
{
    if (!q || !f || !source) {
        return 0;
    }

    char *source_addr = string_format("%s/%s", source->transfer_url, f->cached_name);
    vine_manager_put_url_now(q, q->pbb_worker, source_addr, f);
    free(source_addr);
    
    priority_queue_push(q->checkpointed_files, f, -(double)(f->recovery_subgraph_cost / f->size));

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

static int64_t vine_checkpoint_pbb_used_bytes(struct vine_manager *q)
{
    if (!q) {
        return 0;
    }
    if (!q->checkpointed_files) {
        return 0;
    }
    if (!q->pbb_worker) {
        return 0;
    }
    if (!q->pbb_worker->resources) {
        return 0;
    }

    int64_t total_checkpointed_bytes = 0;
    int idx = 0;
    struct vine_file *candidate;
    int iter_count = 0;
    int iter_depth = priority_queue_size(q->checkpointed_files);
    PRIORITY_QUEUE_BASE_ITERATE(q->checkpointed_files, idx, candidate, iter_count, iter_depth) 
    {
        total_checkpointed_bytes += candidate->size;
    }
    return total_checkpointed_bytes;
}   

static int64_t vine_checkpoint_pbb_available_bytes(struct vine_manager *q)
{
    return (int64_t)(q->pbb_worker->resources->disk.total * 1024 * 1024) - vine_checkpoint_pbb_used_bytes(q);
}

int vine_checkpoint_pbb_available(struct vine_manager *q, struct vine_file *f)
{
    return (int64_t)(f->size) <= vine_checkpoint_pbb_available_bytes(q);
}

int vine_checkpoint_release_pbb(struct vine_manager *q, struct vine_file *f)
{
    if (!q || !f) {
        return 0;
    }

    if (vine_checkpoint_pbb_available(q, f)) {
        return 1;
    }

    int64_t available_bytes = vine_checkpoint_pbb_available_bytes(q);

    double this_file_cost = (double)f->recovery_subgraph_cost / f->size;

    double total_eviction_cost = 0;
    struct list *candidates = list_create();
    struct vine_file *candidate;

    while ((candidate = priority_queue_pop(q->checkpointed_files))) {
        list_push_tail(candidates, candidate);
        available_bytes += candidate->size;
        total_eviction_cost += (double)candidate->recovery_subgraph_cost / candidate->size;
        /* now, if the capacity suits, we can stop */
        if (available_bytes >= (int64_t)f->size) {
            break;
        }
    }

    printf("recovery_subgraph_cost: %ld, size: %ld, this_file_cost: %f, total_eviction_cost: %f\n", f->recovery_subgraph_cost, f->size, this_file_cost, total_eviction_cost);

    if (list_size(candidates) == 0) {
        list_delete(candidates);
        return 0;
    }
    if (available_bytes < (int64_t)f->size) {
        list_delete(candidates);
        return 0;
    }


    // the cost is too high, we don't want to evict for this file
    // if (total_eviction_cost > this_file_cost) {
    if (0) {
        // pop back the files we popped from the checkpointed_files
        LIST_ITERATE(candidates, candidate)
        {
            priority_queue_push(q->checkpointed_files, candidate, -(double)(candidate->recovery_subgraph_cost / candidate->size));
        }
        list_delete(candidates);
        return 0;
    }
    
    // otherwise, we can evict the selected files for this file
    LIST_ITERATE(candidates, candidate) 
    {
        printf("Evicting %s\n", candidate->cached_name);
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

    struct vine_mount *m;
    struct vine_task *t = itable_lookup(q->tasks, f->producer_task_id);
    if (t->input_mounts) {
        LIST_ITERATE(t->input_mounts, m) {
            if (!m->file || m->file->type != VINE_TEMP) {
                continue;   
            }
            critical_time = MAX(critical_time, m->file->recovery_subgraph_critical_time);
            total_time += m->file->recovery_subgraph_total_time;
        }
    }
    critical_time += t->time_workers_execute_last;
    total_time += t->time_workers_execute_last;

    /* update this file's recovery metrics */
    f->recovery_subgraph_critical_time = critical_time;
    f->recovery_subgraph_total_time = total_time;
    f->recovery_subgraph_cost = (uint64_t)(0.5 * total_time + 0.5 * critical_time);

    /* add to checkpoint queue for potential checkpointing */
    hash_table_insert(q->checkpoint_queue, f->cached_name, f);

    /* recursively update all downstream files */
    int *task_id;
    if (f->consumer_tasks) {
        LIST_ITERATE(f->consumer_tasks, task_id) {
            t = itable_lookup(q->tasks, *task_id);
            struct vine_mount *m;
            LIST_ITERATE(t->output_mounts, m) {
                update_downstream_recovery_metrics(q, m->file);
            }
        }
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
    printf("Removing %s from checkpoint storage\n", f->cached_name);
    delete_worker_file(q, q->pbb_worker, f->cached_name, 0, 0);

    /* update this file's recovery metrics after eviction */
    struct vine_task *t = itable_lookup(q->tasks, f->producer_task_id);
    uint64_t critical_time = 0;
    uint64_t total_time = 0;
    struct vine_mount *m;

    if (t->input_mounts) {
        LIST_ITERATE(t->input_mounts, m) {
            if (!m->file || m->file->type != VINE_TEMP) {
                continue;
            }
            critical_time = MAX(critical_time, m->file->recovery_subgraph_critical_time);
            total_time += m->file->recovery_subgraph_total_time;
        }
    }
    critical_time += t->time_workers_execute_last;
    total_time += t->time_workers_execute_last;

    f->recovery_subgraph_critical_time = critical_time;
    f->recovery_subgraph_total_time = total_time;
    f->recovery_subgraph_cost = (uint64_t)(0.5 * total_time + 0.5 * critical_time);

    /* recursively update all downstream files' recovery metrics */
    printf("Recursively updating recovery metrics for downstream files of %s\n", f->cached_name);

    int *task_id;
    if (f->consumer_tasks) {
        LIST_ITERATE(f->consumer_tasks, task_id) {
            struct vine_mount *m;
            t = itable_lookup(q->tasks, *task_id);
            if (t->output_mounts) {
                LIST_ITERATE(t->output_mounts, m) {
                    if (!m->file || m->file->type != VINE_TEMP) {
                        continue;
                    }
                    update_downstream_recovery_metrics(q, m->file);
                }
            }
        }
    }

    return 1;
}

int vine_checkpoint_checkpointed(struct vine_manager *q, struct vine_file *f)
{
    struct vine_file *replica = hash_table_lookup(q->pbb_worker->current_files, f->cached_name);
    if (replica) {
        return 1;
    }
    return 0;
}

