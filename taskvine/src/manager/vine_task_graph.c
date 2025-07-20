#include "vine_task_graph.h"
#include "taskvine.h"
#include "vine_manager.h"
#include "vine_worker_info.h"
#include "priority_queue.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "priority_queue.h"
#include <math.h>
#include "hash_table.h"
#include "itable.h"
#include "list.h"
#include "vine_task.h"
#include "timestamp.h"
#include "vine_file.h"
#include "set.h"
#include "vine_mount.h"
#include "progress_bar.h"
#include "assert.h"
#include "macros.h"
#include <signal.h>
#include <stdio.h>

static void vine_task_graph_finalize(struct vine_task_graph *tg);
static volatile sig_atomic_t interrupted = 0;

/*************************************************************/
/* Private Functions */
/*************************************************************/

static void handle_sigint(int signal)
{
	interrupted = 1;
}

static void submit_node_task(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	int task_id = vine_task_node_submit(node);
	itable_insert(tg->task_id_to_node, task_id, node);

	return;
}

static void submit_node_ready_children(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	struct vine_task_node *child_node;
	LIST_ITERATE(node->children, child_node)
	{
		/* Remove this parent from the child's pending set if it exists */
		if (child_node->pending_parents) {
			/* Assert that this parent is indeed pending for the child */
			assert(set_lookup(child_node->pending_parents, node));
			set_remove(child_node->pending_parents, node);
		}

		/* If no more parents are pending, submit the child */
		if (!child_node->pending_parents || set_size(child_node->pending_parents) == 0) {
			submit_node_task(tg, child_node);
		}
	}

	return;
}

struct list *get_topological_order(struct vine_task_graph *tg)
{
	if (!tg) {
		return NULL;
	}

	int total_nodes = hash_table_size(tg->nodes);
	struct list *topo_order = list_create();
	struct hash_table *in_degree_map = hash_table_create(0, 0);
	struct priority_queue *pq = priority_queue_create(total_nodes);

	char *key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(tg->nodes, key, node)
	{
		int deg = list_size(node->parents);
		hash_table_insert(in_degree_map, key, (void *)(intptr_t)deg);
		if (deg == 0) {
			priority_queue_push(pq, node, compute_lex_priority(node->node_key));
		}
	}

	while (priority_queue_size(pq) > 0) {
		struct vine_task_node *current = priority_queue_pop(pq);
		list_push_tail(topo_order, current);

		struct vine_task_node *child;
		LIST_ITERATE(current->children, child)
		{
			intptr_t raw_deg = (intptr_t)hash_table_lookup(in_degree_map, child->node_key);
			int deg = (int)raw_deg - 1;

			hash_table_remove(in_degree_map, child->node_key);
			hash_table_insert(in_degree_map, child->node_key, (void *)(intptr_t)deg);

			if (deg == 0) {
				priority_queue_push(pq, child, compute_lex_priority(child->node_key));
			}
		}
	}

	if (list_size(topo_order) != total_nodes) {
		debug(D_VINE, "Error: task graph contains cycles or is malformed.\n");
		debug(D_VINE, "Expected %d nodes, but only sorted %d.\n", total_nodes, list_size(topo_order));

		HASH_TABLE_ITERATE(tg->nodes, key, node)
		{
			intptr_t raw_deg = (intptr_t)hash_table_lookup(in_degree_map, key);
			int deg = (int)raw_deg;
			if (deg > 0) {
				debug(D_VINE, "  Node %s has in-degree %d. Parents:\n", key, deg);
				struct vine_task_node *p;
				LIST_ITERATE(node->parents, p)
				{
					debug(D_VINE, "    -> %s\n", p->node_key);
				}
			}
		}

		list_delete(topo_order);
		exit(1);
	}

	hash_table_delete(in_degree_map);
	priority_queue_delete(pq);
	return topo_order;
}

static struct vine_task_node *get_node_by_task(struct vine_task_graph *tg, struct vine_task *task)
{
	if (!tg || !task) {
		return NULL;
	}

	if (task->type == VINE_TASK_TYPE_STANDARD) {
		return itable_lookup(tg->task_id_to_node, task->task_id);
	} else if (task->type == VINE_TASK_TYPE_RECOVERY) {
		/* note that recovery tasks are not mapped to any node but we still need the original node for pruning,
		 * so we look up the outfile of the task, then map it back to get the original node */
		struct vine_mount *mount;
		LIST_ITERATE(task->output_mounts, mount)
		{
			if (mount->file->original_producer_task_id > 0) {
				return itable_lookup(tg->task_id_to_node, mount->file->original_producer_task_id);
			}
		}
	}

	debug(D_ERROR, "task %d has no original producer task id", task->task_id);

	return NULL;
}

/*************************************************************/
/* Public APIs */
/*************************************************************/

static void handle_checkpoint_worker_stagein(struct vine_task_graph *tg, struct vine_file *f)
{
	if (!tg || !f) {
		return;
	}

	struct vine_task_node *this_node = hash_table_lookup(tg->outfile_cachename_to_node, f->cached_name);
	if (!this_node) {
		return;
	}

	vine_task_node_prune_ancestors(this_node);
}

void vine_task_graph_execute(struct vine_task_graph *tg)
{
	if (!tg) {
		return;
	}

	/* finalize the task graph */
	vine_task_graph_finalize(tg);

	/* enable return recovery tasks */
	vine_enable_return_recovery_tasks(tg->manager);

	signal(SIGINT, handle_sigint);

	/* enqueue those without dependencies */
	char *node_key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		if (!node->pending_parents || set_size(node->pending_parents) == 0) {
			submit_node_task(tg, node);
		}
	}

	struct ProgressBar *pbar = progress_bar_init("Executing Tasks");
	struct ProgressBarPart *regular_tasks_part = progress_bar_part_create("Regular", hash_table_size(tg->nodes));
	struct ProgressBarPart *recovery_tasks_part = progress_bar_part_create("Recovery", 0);
	progress_bar_add_part(pbar, regular_tasks_part);
	progress_bar_add_part(pbar, recovery_tasks_part);

	while (regular_tasks_part->current < regular_tasks_part->total) {
		if (interrupted) {
			break;
		}

		struct vine_task *task = vine_wait(tg->manager, 5);
		progress_bar_update_part_total(pbar, recovery_tasks_part, tg->manager->num_submitted_recovery_tasks);
		if (task) {
			/* get the original node by task id */
			struct vine_task_node *node = get_node_by_task(tg, task);
			if (!node) {
				debug(D_ERROR, "fatal: task %d could not be mapped to a task node, this indicates a serious bug.", task->task_id);
				exit(1);
			}

			/* in case of failure, resubmit this task */
			if (node->task->result != VINE_RESULT_SUCCESS || node->task->exit_code != 0) {
				vine_task_clean(node->task);
				submit_node_task(tg, node);
				continue;
			}

			/* mark the node as completed */
			node->completed = 1;

			/* prune nodes on task completion */
			vine_task_node_prune_ancestors(node);

			/* skip recovery tasks */
			if (task->type == VINE_TASK_TYPE_RECOVERY) {
				progress_bar_advance_part_current(pbar, recovery_tasks_part, 1);
				continue;
			}

			/* set the start time to the submit time of the first regular task */
			if (regular_tasks_part->current == 0) {
				progress_bar_reset_start_time(pbar, task->time_when_submitted);
			}

			/* update critical time */
			vine_task_node_update_critical_time(node, task->time_workers_execute_last);

			/* mark node as completed */
			progress_bar_advance_part_current(pbar, regular_tasks_part, 1);

			/* enqueue the output file for replication or checkpointing */
			switch (node->outfile_type) {
			case VINE_NODE_OUTFILE_TYPE_TEMP:
				vine_task_node_replicate_outfile(node);
				break;
			case VINE_NODE_OUTFILE_TYPE_FILE:
			case VINE_NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
				break;
			}

			/* submit children nodes with dependencies all resolved */
			submit_node_ready_children(tg, node);
		} else {
			progress_bar_advance_part_current(pbar, recovery_tasks_part, 0);
		}

		struct vine_file *new_checkpointed_file;
		while ((new_checkpointed_file = list_pop_head(tg->manager->new_checkpointed_files))) {
			handle_checkpoint_worker_stagein(tg, new_checkpointed_file);
		}
	}

	progress_bar_finish(pbar);
	progress_bar_delete(pbar);

	timestamp_t total_time_spent_on_unlink_local_files = 0;
	timestamp_t total_time_spent_on_prune_ancestors_of_temp_node = 0;
	timestamp_t total_time_spent_on_prune_ancestors_of_persisted_node = 0;
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		total_time_spent_on_unlink_local_files += node->time_spent_on_unlink_local_files;
		total_time_spent_on_prune_ancestors_of_temp_node += node->time_spent_on_prune_ancestors_of_temp_node;
		total_time_spent_on_prune_ancestors_of_persisted_node += node->time_spent_on_prune_ancestors_of_persisted_node;
	}

	return;
}

static struct list *extract_weakly_connected_components(struct vine_task_graph *tg)
{
	if (!tg) {
		return NULL;
	}

    struct set *visited = set_create(0);
    struct list *components = list_create();

	char *node_key;
    struct vine_task_node *node;
    HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
        if (set_lookup(visited, node)) {
            continue;
        }

        struct list *component = list_create();
        struct list *queue = list_create();

        list_push_tail(queue, node);
        set_insert(visited, node);
        list_push_tail(component, node);

        while (list_size(queue) > 0) {
            struct vine_task_node *curr = list_pop_head(queue);

            struct vine_task_node *p;
            LIST_ITERATE(curr->parents, p)
			{
                if (!set_lookup(visited, p)) {
                    list_push_tail(queue, p);
                    set_insert(visited, p);
                    list_push_tail(component, p);
                }
            }

            struct vine_task_node *c;
            LIST_ITERATE(curr->children, c)
			{
                if (!set_lookup(visited, c)) {
                    list_push_tail(queue, c);
                    set_insert(visited, c);
                    list_push_tail(component, c);
                }
            }
        }

        list_push_tail(components, component);
        list_delete(queue);
    }

    set_delete(visited);
    return components;
}

static void compute_eigenvector_centrality(struct vine_task_graph *tg, struct list *topo_order)
{
    int n = list_size(topo_order);
    if (n == 0) {
        return;
    }

	timestamp_t start_time = timestamp_get();

    double *curr = calloc(n, sizeof(double));
    double *next = calloc(n, sizeof(double));
    struct vine_task_node **index_map = malloc(sizeof(*index_map) * n);
    struct hash_table *node_to_index = hash_table_create(0, 0);  // default string-based, but we’ll use pointer cast

    const double damping = 0.85;
    const double base = 1.0 - damping;

    int i = 0;
    struct vine_task_node *node;
    LIST_ITERATE(topo_order, node) {
        curr[i] = 1.0;
        index_map[i] = node;
        int *index_ptr = malloc(sizeof(int));
        *index_ptr = i;
        hash_table_insert(node_to_index, (const char *)node, index_ptr);  // cast node* to const char*
        i++;
    }

    for (int iter = 0; iter < 100; iter++) {
        double norm = 0.0;

        for (int i = 0; i < n; i++) {
            struct vine_task_node *node = index_map[i];
            double sum = 0.0;

            struct vine_task_node *parent;
            LIST_ITERATE(node->parents, parent)
			{
                int *parent_index_ptr = (int *)hash_table_lookup(node_to_index, (const char *)parent);
                sum += curr[*parent_index_ptr];
            }

            next[i] = base + damping * sum;
            norm += next[i] * next[i];
        }

        norm = sqrt(norm);
        if (norm < 1e-10) {
            break;
        }

        double diff = 0.0;
        for (int i = 0; i < n; i++) {
            next[i] /= norm;
            diff += fabs(next[i] - curr[i]);
            curr[i] = next[i];
        }

        if (diff < 1e-6) {
            break;
        }
    }

    for (int i = 0; i < n; i++) {
        index_map[i]->eigenvector_centrality = curr[i];
		printf("node %s eigenvector_centrality: %f\n", index_map[i]->node_key, curr[i]);
    }

    char *key;
    void *value;
    HASH_TABLE_ITERATE(node_to_index, key, value) {
        free(value);
    }
    hash_table_delete(node_to_index);
    free(curr);
    free(next);
    free(index_map);

	timestamp_t end_time = timestamp_get();
	printf("time spent on compute eigenvector centrality: %ld\n", end_time - start_time);
}

static void vine_task_graph_finalize(struct vine_task_graph *tg)
{
	if (!tg) {
		return;
	}

	/* get nodes in topological order */
	struct list *topo_order = get_topological_order(tg);
	if (!topo_order) {
		return;
	}

	char *node_key;
	struct vine_task_node *node;
	struct vine_task_node *parent_node;
	struct vine_task_node *child_node;

	/* compute the upstream and downstream counts for each node */
	struct hash_table *upstream_map = hash_table_create(0, 0);
	struct hash_table *downstream_map = hash_table_create(0, 0);
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		struct set *upstream = set_create(0);
		struct set *downstream = set_create(0);
		hash_table_insert(upstream_map, node_key, upstream);
		hash_table_insert(downstream_map, node_key, downstream);
	}
	LIST_ITERATE(topo_order, node)
	{
		struct set *upstream = hash_table_lookup(upstream_map, node->node_key);
		LIST_ITERATE(node->parents, parent_node)
		{
			struct set *parent_upstream = hash_table_lookup(upstream_map, parent_node->node_key);
			set_union(upstream, parent_upstream);
			set_insert(upstream, parent_node);
		}
	}
	LIST_ITERATE_REVERSE(topo_order, node)
	{
		struct set *downstream = hash_table_lookup(downstream_map, node->node_key);
		LIST_ITERATE(node->children, child_node)
		{
			struct set *child_downstream = hash_table_lookup(downstream_map, child_node->node_key);
			set_union(downstream, child_downstream);
			set_insert(downstream, child_node);
		}
	}
	LIST_ITERATE(topo_order, node)
	{
		node->upstream_subgraph_size = set_size(hash_table_lookup(upstream_map, node->node_key));
		node->downstream_subgraph_size = set_size(hash_table_lookup(downstream_map, node->node_key));
		node->fan_in = list_size(node->parents);
		node->fan_out = list_size(node->children);
		set_delete(hash_table_lookup(upstream_map, node->node_key));
		set_delete(hash_table_lookup(downstream_map, node->node_key));
	}
	hash_table_delete(upstream_map);
	hash_table_delete(downstream_map);

	/* extract weakly connected components */
	struct list *weakly_connected_components = extract_weakly_connected_components(tg);
	struct list *component;
	int component_index = 0;
	debug(D_VINE, "graph has %d weakly connected components\n", list_size(weakly_connected_components));
	LIST_ITERATE(weakly_connected_components, component)
	{
		debug(D_VINE, "component %d size: %d\n", component_index, list_size(component));
		component_index++;
	}
	list_delete(weakly_connected_components);

	/* compute the depth of the node */
	LIST_ITERATE(topo_order, node)
	{
		/* compute the depth of the node */
		node->depth = 0;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (node->depth < parent_node->depth + 1) {
				node->depth = parent_node->depth + 1;
			}
		}
	}

	// compute_eigenvector_centrality(tg, topo_order);

	/* add the parents' outfiles as inputs to the task */
	LIST_ITERATE(topo_order, node)
	{
		LIST_ITERATE(node->parents, parent_node)
		{
			if (parent_node->outfile) {
				vine_task_add_input(node->task, parent_node->outfile, parent_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
			}
		}
	}

	list_delete(topo_order);

	/* initialize pending_parents for all nodes */
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		LIST_ITERATE(node->parents, parent_node)
		{
			if (node->pending_parents) {
				/* Use parent_node->node_key to ensure pointer consistency */
				set_insert(node->pending_parents, parent_node);
			}
		}
	}

	/* print node info */
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		vine_task_node_print_info(node);
	}

	return;
}

struct vine_task_graph *vine_task_graph_create(struct vine_manager *q)
{
	struct vine_task_graph *tg = xxmalloc(sizeof(struct vine_task_graph));
	tg->nodes = hash_table_create(0, 0);
	tg->task_id_to_node = itable_create(0);
	tg->outfile_cachename_to_node = hash_table_create(0, 0);

	tg->library_name = xxstrdup("vine_task_graph_library");
	tg->function_name = xxstrdup("compute_group_keys");
	tg->manager = q;

	/* enable debug system for C code since it uses a separate debug system instance
	 * from the Python bindings. Use the same function that the manager uses. */
	char *debug_tmp = string_format("%s/vine-logs/debug", tg->manager->runtime_directory);
	vine_enable_debug_log(debug_tmp);
	free(debug_tmp);

	return tg;
}

struct vine_task_node *vine_task_graph_create_node(
		struct vine_task_graph *tg,
		const char *node_key,
		const char *outfile_remote_name,
		const char *staging_dir,
		int prune_depth,
		vine_task_node_priority_mode_t priority_mode,
		vine_task_node_outfile_type_t outfile_type)
{
	if (!tg || !node_key) {
		return NULL;
	}

	struct vine_task_node *node = hash_table_lookup(tg->nodes, node_key);
	if (!node) {
		node = vine_task_node_create(tg->manager,
				node_key,
				tg->library_name,
				tg->function_name,
				staging_dir,
				outfile_remote_name,
				prune_depth,
				priority_mode,
				outfile_type);
		hash_table_insert(tg->nodes, node_key, node);
		if (node->outfile) {
			hash_table_insert(tg->outfile_cachename_to_node, node->outfile->cached_name, node);
		}
	}

	return node;
}

void vine_task_graph_add_dependency(struct vine_task_graph *tg, const char *parent_key, const char *child_key)
{
	if (!tg || !parent_key || !child_key) {
		return;
	}

	struct vine_task_node *parent_node = hash_table_lookup(tg->nodes, parent_key);
	struct vine_task_node *child_node = hash_table_lookup(tg->nodes, child_key);
	if (!parent_node) {
		debug(D_ERROR, "parent node %s not found", parent_key);
		exit(1);
	}
	if (!child_node) {
		debug(D_ERROR, "child node %s not found", child_key);
		exit(1);
	}

	list_push_tail(child_node->parents, parent_node);
	list_push_tail(parent_node->children, child_node);
	debug(D_VINE, "added dependency: %s -> %s", parent_key, child_key);
}

const char *vine_task_graph_get_library_name(const struct vine_task_graph *tg)
{
	if (!tg) {
		return NULL;
	}
	return tg->library_name;
}

const char *vine_task_graph_get_function_name(const struct vine_task_graph *tg)
{
	if (!tg) {
		return NULL;
	}
	return tg->function_name;
}

void vine_task_graph_delete(struct vine_task_graph *tg)
{
	if (!tg) {
		return;
	}

	char *node_key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		if (node->infile) {
			vine_prune_file(tg->manager, node->infile);
			hash_table_remove(tg->manager->file_table, node->infile->cached_name);
		}
		if (node->outfile) {
			vine_prune_file(tg->manager, node->outfile);
			hash_table_remove(tg->outfile_cachename_to_node, node->outfile->cached_name);
			hash_table_remove(tg->manager->file_table, node->outfile->cached_name);
		}
		vine_task_node_delete(node);
	}
	free(tg->library_name);
	free(tg->function_name);

	hash_table_delete(tg->nodes);
	itable_delete(tg->task_id_to_node);
	hash_table_delete(tg->outfile_cachename_to_node);
	free(tg);
}
