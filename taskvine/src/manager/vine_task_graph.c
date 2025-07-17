#include "vine_task_graph.h"
#include "taskvine.h"
#include "vine_manager.h"
#include "vine_worker_info.h"
#include "priority_queue.h"
#include <stdlib.h>
#include <string.h>
#include "vine_temp_redundancy.h"
#include <stdint.h>
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "priority_queue.h"
#include <sys/stat.h>
#include "hash_table.h"
#include "vine_file_replica_table.h"
#include "itable.h"
#include "list.h"
#include "vine_task.h"
#include <math.h>
#include "timestamp.h"
#include "vine_file.h"
#include "set.h"
#include "vine_mount.h"
#include "progress_bar.h"
#include "random.h"
#include "assert.h"
#include "macros.h"
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <stdatomic.h>

struct pending_insert_entry {
	struct vine_task_node *target_node;
	struct vine_task_node *source_node;
};

static void submit_node_task(struct vine_task_graph *tg, struct vine_task_node *node);
static void vine_task_graph_finalize(struct vine_task_graph *tg);
static void vine_task_graph_print_node_info(struct vine_task_node *node);
static void vine_task_graph_set_priority_mode(struct vine_task_graph *tg, vine_task_priority_mode_t mode);
static int node_outfile_exists(struct vine_task_graph *tg, struct vine_task_node *node);
static volatile sig_atomic_t interrupted = 0;

/*************************************************************/
/* Private Functions */
/*************************************************************/

static void handle_sigint(int signal)
{
	interrupted = 1;
}

static void submit_node_children(struct vine_task_graph *tg, struct vine_task_node *node)
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

static double compute_lex_priority(const char *key)
{
	double score = 0.0;
	double factor = 1.0;
	for (int i = 0; i < 8 && key[i] != '\0'; i++) {
		score += key[i] * factor;
		factor *= 0.01;
	}
	return -score;
}

struct list *get_topological_order(struct vine_task_graph *graph)
{
	if (!graph) {
		return NULL;
	}

	int total_nodes = hash_table_size(graph->nodes);
	struct list *topo_order = list_create();
	struct hash_table *in_degree_map = hash_table_create(0, 0);
	struct priority_queue *pq = priority_queue_create(total_nodes);

	char *key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(graph->nodes, key, node)
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

		HASH_TABLE_ITERATE(graph->nodes, key, node)
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

static void prune_node(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node || !node->outfile) {
		return;
	}

	/* delete all of the replicas present at remote workers. */
	struct set *source_workers = hash_table_lookup(tg->manager->file_worker_table, node->outfile->cached_name);
	if (source_workers && set_size(source_workers) > 0) {
		struct vine_worker_info *w;
		SET_ITERATE(source_workers, w)
		{
			/* skip if a checkpoint worker */
			if (is_checkpoint_worker(tg->manager, w)) {
				continue;
			}
			delete_worker_file(tg->manager, w, node->outfile->cached_name, 0, 0);
		}
	}
}

static void prune_pre_initialized_node_waiters(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	/* prune stale files based on global prune_depth setting */
	/* this node just finished, now notify others who were waiting on it */
	struct vine_task_node *waiter_node;
	LIST_ITERATE(node->reverse_prune_waiters, waiter_node)
	{
		waiter_node->prune_blocking_children_remaining--;
		if (waiter_node->prune_blocking_children_remaining == 0 && tg->nls_prune_depth > 0) {
			prune_node(tg, waiter_node);
		}
	}

	return;
}

static void find_parents_dfs(struct vine_task_node *node, int remaining_depth, struct list *result, struct set *visited)
{
	if (!node || set_lookup(visited, node)) {
		return;
	}
	set_insert(visited, node);
	if (remaining_depth == 0) {
		list_push_tail(result, node);
		return;
	}
	struct vine_task_node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		find_parents_dfs(parent_node, remaining_depth - 1, result, visited);
	}
}

static struct list *find_parent_nodes_in_depth(struct vine_task_graph *tg, struct vine_task_node *node, int depth)
{
	if (!tg || !node || depth < 0) {
		return NULL;
	}
	struct list *result = list_create();
	struct set *visited = set_create(0);
	find_parents_dfs(node, depth, result, visited);
	set_delete(visited);

	return result;
}

static int node_is_prunable(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node || !node->outfile) {
		return 0;
	}

	/* a file is prunable if its outfile is no longer needed by any child node:
	 * 1. it has no pending dependents
	 * 2. all completed dependents have also completed their corresponding recovery tasks, if any */
	struct vine_task_node *child_node;
	LIST_ITERATE(node->children, child_node)
	{
		/* if a task is not deleted, it means it is still running */
		if (child_node->task && child_node->task->state != VINE_TASK_DONE) {
			return 0;
		}
		/* if the recovery task is running, the parent is not prunable */
		struct vine_task *child_node_recovery_task = child_node->outfile->recovery_task;
		if (child_node_recovery_task && (child_node_recovery_task->state != VINE_TASK_INITIAL && child_node_recovery_task->state != VINE_TASK_DONE)) {
			return 0;
		}
		/* if the output file does not exist, the parent cannot be pruned */
		if (!node_outfile_exists(tg, child_node)) {
			return 0;
		}
	}

	return 1;
}

static void prune_real_time_parent_nodes(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	struct list *parents = find_parent_nodes_in_depth(tg, node, tg->nls_prune_depth);
	struct vine_task_node *parent_node;
	LIST_ITERATE(parents, parent_node)
	{
		if (node_is_prunable(tg, parent_node)) {
			prune_node(tg, parent_node);
		}
	}

	return;
}

static void initialize_pruning(struct vine_task_graph *tg)
{
	if (!tg || tg->prune_algorithm != PRUNE_ALGORITHM_PRE_INITIALIZING) {
		return;
	}

	/* Skip if global prune depth is not set */
	if (tg->nls_prune_depth <= 0) {
		return;
	}

	/* create mappings of nodes to their prune_depth */
	struct list *bfs_nodes = list_create();
	struct list *pending_inserts = list_create(); /* Store pending reverse_prune_waiters inserts */

	/* For each node P, calculate what downstream nodes it needs to wait for based on global prune_depth */
	char *node_key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		node->prune_blocking_children_remaining = 0;

		/* Fast path for prune_depth=1: only check direct children */
		if (tg->nls_prune_depth == 1) {
			struct vine_task_node *child_node;
			LIST_ITERATE(node->children, child_node)
			{
				node->prune_blocking_children_remaining++;
				list_push_tail(child_node->reverse_prune_waiters, node);
			}
		} else {
			/* General BFS for prune_depth > 1 */
			struct set *visited = set_create(0);

			/* clear pending inserts list */
			while (list_size(pending_inserts) > 0) {
				struct pending_insert_entry *entry = list_pop_head(pending_inserts);
				free(entry);
			}

			/* start BFS from current node's direct children */
			struct vine_task_node *child_node;
			LIST_ITERATE(node->children, child_node)
			{
				if (!set_lookup(visited, child_node)) {
					set_insert(visited, child_node);
					list_push_tail(bfs_nodes, child_node);
					node->prune_blocking_children_remaining++;

					/* store the insert operation for later */
					struct pending_insert_entry *entry = xxmalloc(sizeof(struct pending_insert_entry));
					entry->target_node = child_node;
					entry->source_node = node;
					list_push_tail(pending_inserts, entry);
				}
			}

			/* BFS to expand to deeper levels within prune_depth, starting from the current node's direct children */
			int current_depth = 1;
			while (list_size(bfs_nodes) > 0 && current_depth < tg->nls_prune_depth) {
				int level_size = list_size(bfs_nodes);

				for (int i = 0; i < level_size; i++) {
					struct vine_task_node *current_node = list_pop_head(bfs_nodes);

					/* add current_node's children to next level */
					LIST_ITERATE(current_node->children, child_node)
					{
						if (!set_lookup(visited, child_node)) {
							set_insert(visited, child_node);
							list_push_tail(bfs_nodes, child_node);
							node->prune_blocking_children_remaining++;

							/* store the insert operation for later */
							struct pending_insert_entry *entry = xxmalloc(sizeof(struct pending_insert_entry));
							entry->target_node = child_node;
							entry->source_node = node;
							list_push_tail(pending_inserts, entry);
						}
					}
				}
				current_depth++;
			}

			/* perform all the reverse_prune_waiters inserts */
			struct pending_insert_entry *entry;
			LIST_ITERATE(pending_inserts, entry)
			{
				/* child->reverse_prune_waiters stores parents waiting for this child */
				list_push_tail(entry->target_node->reverse_prune_waiters, entry->source_node);
			}

			/* Clear remaining nodes in bfs_nodes for next iteration */
			while (list_size(bfs_nodes) > 0) {
				list_pop_head(bfs_nodes);
			}

			set_delete(visited);
		}
	}

	/* Clean up pending inserts */
	while (list_size(pending_inserts) > 0) {
		struct pending_insert_entry *entry = list_pop_head(pending_inserts);
		free(entry);
	}

	list_delete(bfs_nodes);
	list_delete(pending_inserts);
}

static void prune_node_on_completion(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	timestamp_t start_time = timestamp_get();

	switch (tg->prune_algorithm) {
	case PRUNE_ALGORITHM_PRE_INITIALIZING:
		prune_pre_initialized_node_waiters(tg, node);
		break;
	case PRUNE_ALGORITHM_REAL_TIME:
		prune_real_time_parent_nodes(tg, node);
		break;
	}

	tg->time_spent_on_file_pruning += timestamp_get() - start_time;
}

static double calculate_task_priority(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return 0.0;
	}

	double priority = 0.0;
	timestamp_t current_time = timestamp_get();

	switch (tg->priority_mode) {
	case VINE_TASK_PRIORITY_MODE_RANDOM:
		priority = random_double();
		break;
	case VINE_TASK_PRIORITY_MODE_DEPTH_FIRST:
		priority = (double)node->depth;
		break;
	case VINE_TASK_PRIORITY_MODE_BREADTH_FIRST:
		priority = -(double)node->depth;
		break;
	case VINE_TASK_PRIORITY_MODE_FIFO:
		priority = -(double)current_time;
		break;
	case VINE_TASK_PRIORITY_MODE_LIFO:
		priority = (double)current_time;
		break;
	case VINE_TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST:
		struct vine_task_node *parent_node;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (parent_node->outfile) {
				priority += (double)vine_file_size(parent_node->outfile);
			}
		}
		break;
	}

	return priority;
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

static void submit_node_task(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	double priority = calculate_task_priority(tg, node);
	vine_task_set_priority(node->task, priority);

	int task_id = vine_submit(tg->manager, node->task);
	itable_insert(tg->task_id_to_node, task_id, node);

	return;
}

static void replicate_node(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	vine_temp_redundancy_replicate_file(tg->manager, node->outfile);
}

static void update_node_critical_time(struct vine_task_node *node, timestamp_t execution_time)
{
	timestamp_t max_parent_critical_time = 0;
	struct vine_task_node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		if (parent_node->critical_time > max_parent_critical_time) {
			max_parent_critical_time = parent_node->critical_time;
		}
	}
	node->critical_time = max_parent_critical_time + execution_time;
}

static void checkpoint_node(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	vine_temp_redundancy_checkpoint_file(tg->manager, node->outfile);
}

/*************************************************************/
/* Public APIs */
/*************************************************************/

void vine_task_graph_node_set_outfile_remote_name(struct vine_task_graph *tg, const char *node_key, const char *remote_name)
{
	if (!tg || !node_key || !remote_name) {
		return;
	}

	struct vine_task_node *node = hash_table_lookup(tg->nodes, node_key);
	if (!node) {
		return;
	}

	if (node->outfile_remote_name) {
		free(node->outfile_remote_name);
	}

	node->outfile_remote_name = xxstrdup(remote_name);
}

static void handle_checkpoint_worker_stagein(struct vine_task_graph *tg, struct vine_file *f)
{
	if (!tg || !f) {
		return;
	}

	/* find the node that corresponds to this cached file */
	struct vine_task_node *this_node = hash_table_lookup(tg->outfile_cachename_to_node, f->cached_name);
	if (!this_node) {
		return;
	}

	timestamp_t start_time = timestamp_get();

	/* Traverse all upstream parents using BFS */
	struct set *visited_nodes = set_create(0);
	struct list *bfs_nodes = list_create();

	/* Start with the current node */
	list_push_tail(bfs_nodes, this_node);
	set_insert(visited_nodes, this_node);

	/* BFS traversal */
	while (list_size(bfs_nodes) > 0) {
		struct vine_task_node *current = list_pop_head(bfs_nodes);

		/* Process all parents of current node */
		struct vine_task_node *parent_node;
		LIST_ITERATE(current->parents, parent_node)
		{
			/* skip inactive parent nodes */
			if (!parent_node->active) {
				continue;
			}
			/* skip if the parent node is already visited */
			if (set_lookup(visited_nodes, parent_node)) {
				continue;
			}
			/* skip if children to the parent node are not all in the visited set */
			int all_active_children_visited = 1;
			struct vine_task_node *child_node;
			LIST_ITERATE(parent_node->children, child_node)
			{
				if (!set_lookup(visited_nodes, child_node) && child_node->active) {
					all_active_children_visited = 0;
					break;
				}
			}
			if (!all_active_children_visited) {
				continue;
			}

			/* add to BFS queue */
			set_insert(visited_nodes, parent_node);
			list_push_tail(bfs_nodes, parent_node);
		}
	}

	/* mark all nodes in the visited set as inactive */
	struct vine_task_node *visited_node;
	SET_ITERATE(visited_nodes, visited_node)
	{
		if (visited_node == this_node) {
			continue;
		}
		visited_node->active = 0;
		vine_prune_file(tg->manager, visited_node->outfile);
	}

	list_delete(bfs_nodes);
	set_delete(visited_nodes);

	tg->time_spent_on_file_pruning += timestamp_get() - start_time;
}

static void vine_task_graph_set_priority_mode(struct vine_task_graph *tg, vine_task_priority_mode_t mode)
{
	if (!tg) {
		return;
	}

	switch (mode) {
	case VINE_TASK_PRIORITY_MODE_RANDOM:
		debug(D_VINE, "Set task priority mode to RANDOM");
		break;
	case VINE_TASK_PRIORITY_MODE_DEPTH_FIRST:
		debug(D_VINE, "Set task priority mode to DEPTH_FIRST");
		break;
	case VINE_TASK_PRIORITY_MODE_BREADTH_FIRST:
		debug(D_VINE, "Set task priority mode to BREADTH_FIRST");
		break;
	case VINE_TASK_PRIORITY_MODE_FIFO:
		debug(D_VINE, "Set task priority mode to FIFO");
		break;
	case VINE_TASK_PRIORITY_MODE_LIFO:
		debug(D_VINE, "Set task priority mode to LIFO");
		break;
	case VINE_TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST:
		debug(D_VINE, "Set task priority mode to LARGEST_INPUT_FIRST");
		break;
	default:
		debug(D_VINE, "Unknown priority mode: %d, falling back to FIFO", mode);
		mode = VINE_TASK_PRIORITY_MODE_FIFO;
		break;
	}

	tg->priority_mode = mode;
}

static int node_outfile_exists(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!node) {
		return 0;
	}
	
	struct stat local_info;

	switch (node->output_store_location) {
	case VINE_OUTPUT_STORE_LOCATION_STAGING_DIR:
		if (stat(node->outfile->source, &local_info) != 0) {
			debug(D_VINE | D_NOTICE, "file %s does not exist on staging directory, resubmitting task %d", node->outfile->cached_name, node->task->task_id);
			return 0;
		}
		break;
	case VINE_OUTPUT_STORE_LOCATION_SHARED_FILE_SYSTEM:
		if (stat(node->outfile_remote_name, &local_info) != 0) {
			debug(D_VINE | D_NOTICE, "file %s does not exist on shared file system, resubmitting task %d", node->outfile_remote_name, node->task->task_id);
			return 0;
		}
		break;
	case VINE_OUTPUT_STORE_LOCATION_CHECKPOINT:
	case VINE_OUTPUT_STORE_LOCATION_TEMP:
		if (vine_file_replica_table_count_replicas(tg->manager, node->outfile->cached_name, VINE_FILE_REPLICA_STATE_READY) == 0) {
			debug(D_VINE | D_NOTICE, "file %s does not exist on NLS, resubmitting task %d", node->outfile->cached_name, node->task->task_id);
			return 0;
		}
		break;
	}

	return 1;
}

static int is_node_completed_successfully(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return 0;
	}

	/* in case of failure, resubmit this task */
	if (node->task && (node->task->result != VINE_RESULT_SUCCESS || node->task->exit_code != 0)) {
		debug(D_VINE | D_NOTICE, "task %d failed with result %d and exit code %d\n", node->task->task_id, node->task->result, node->task->exit_code);
		return 0;
	}

	if (!node_outfile_exists(tg, node)) {
		return 0;
	}

	return 1;
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

			/* skip if the node is not completed successfully */
			if (!is_node_completed_successfully(tg, node)) {
				vine_task_clean(node->task);
				submit_node_task(tg, node);
				continue;
			}

			/* prune nodes on task completion */
			prune_node_on_completion(tg, node);

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
			update_node_critical_time(node, task->time_workers_execute_last);

			/* mark node as completed */
			progress_bar_advance_part_current(pbar, regular_tasks_part, 1);

			/* enqueue the output file for replication or checkpointing */
			switch (node->output_store_location) {
			case VINE_OUTPUT_STORE_LOCATION_TEMP:
				replicate_node(tg, node);
				break;
			case VINE_OUTPUT_STORE_LOCATION_CHECKPOINT:
				checkpoint_node(tg, node);
				break;
			case VINE_OUTPUT_STORE_LOCATION_STAGING_DIR:
			case VINE_OUTPUT_STORE_LOCATION_SHARED_FILE_SYSTEM:
				break;
			}

			/* submit children nodes with dependencies all resolved */
			submit_node_children(tg, node);
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

	printf("time spent on file pruning: %ld\n", tg->time_spent_on_file_pruning);

	return;
}

static void delete_node_task(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!node) {
		return;
	}

	vine_task_delete(node->task);
	node->task = NULL;

	if (node->infile) {
		vine_prune_file(tg->manager, node->infile);
		hash_table_remove(tg->manager->file_table, node->infile->cached_name);
		vine_file_delete(node->infile);
		node->infile = NULL;
	}

	if (node->outfile) {
		vine_prune_file(tg->manager, node->outfile);
		hash_table_remove(tg->outfile_cachename_to_node, node->outfile->cached_name);
		hash_table_remove(tg->manager->file_table, node->outfile->cached_name);
		vine_file_delete(node->outfile);
		node->outfile = NULL;
	}

	return;
}

static void create_node_task(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	node->task = vine_task_create(tg->function_name);
	vine_task_set_priority(node->task, node->depth);
	vine_task_set_library_required(node->task, tg->library_name);

	char *infile_content = string_format("%s", node->node_key);
	node->infile = vine_declare_buffer(tg->manager, infile_content, strlen(infile_content), VINE_CACHE_LEVEL_TASK, VINE_UNLINK_WHEN_DONE);
	free(infile_content);

	vine_task_add_input(node->task, node->infile, "infile", VINE_TRANSFER_ALWAYS);

	switch (node->output_store_location) {
	case VINE_OUTPUT_STORE_LOCATION_STAGING_DIR:
		char *persistent_path = string_format("%s/outputs/%s", tg->staging_dir, node->outfile_remote_name);
		node->outfile = vine_declare_file(tg->manager, persistent_path, VINE_CACHE_LEVEL_WORKFLOW, 0);
		free(persistent_path);
		break;
	case VINE_OUTPUT_STORE_LOCATION_TEMP:
	case VINE_OUTPUT_STORE_LOCATION_CHECKPOINT:
		node->outfile = vine_declare_temp(tg->manager);
		break;
	case VINE_OUTPUT_STORE_LOCATION_SHARED_FILE_SYSTEM:
		/* no explicit output file declaration needed */
		node->outfile = NULL;
		break;
	}

	if (node->outfile) {
		if (!hash_table_lookup(tg->outfile_cachename_to_node, node->outfile->cached_name)) {
			hash_table_insert(tg->outfile_cachename_to_node, node->outfile->cached_name, node);
		}
		vine_task_add_output(node->task, node->outfile, node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
	}

	struct vine_task_node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		if (parent_node->outfile) {
			vine_task_add_input(node->task, parent_node->outfile, parent_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
		}
	}

	return;
}

static void vine_task_graph_finalize(struct vine_task_graph *tg)
{
	if (!tg) {
		return;
	}

	/* enable debug system for C code since it uses a separate debug system instance
	 * from the Python bindings. Use the same function that the manager uses. */
	char *debug_tmp = string_format("%s/vine-logs/debug", tg->manager->runtime_directory);
	vine_enable_debug_log(debug_tmp);
	free(debug_tmp);

	/* get nodes in topological order */
	struct list *topo_order = get_topological_order(tg);
	if (!topo_order) {
		return;
	}

	/* compute depths of all nodes */
	struct vine_task_node *node;
	LIST_ITERATE(topo_order, node)
	{
		node->depth = 0;

		struct vine_task_node *parent;
		LIST_ITERATE(node->parents, parent)
		{
			if (node->depth < parent->depth + 1) {
				node->depth = parent->depth + 1;
			}
		}
	}

	/* create tasks for all nodes */
	LIST_ITERATE(topo_order, node)
	{
		create_node_task(tg, node);
	}

	list_delete(topo_order);

	/* initialize prune depth mappings */
	initialize_pruning(tg);

	/* initialize pending_parents for all nodes */
	char *node_key;
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
	{
		struct vine_task_node *parent_node;
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
		vine_task_graph_print_node_info(node);
	}

	return;
}

struct vine_task_graph *vine_task_graph_create(struct vine_manager *q,
		int nls_prune_depth,
		vine_task_priority_mode_t priority_mode,
		const char *staging_dir)
{
	struct vine_task_graph *tg = xxmalloc(sizeof(struct vine_task_graph));
	tg->nodes = hash_table_create(0, 0);
	tg->task_id_to_node = itable_create(0);
	tg->outfile_cachename_to_node = hash_table_create(0, 0);
	tg->nls_prune_depth = MAX(nls_prune_depth, 0);

	vine_task_graph_set_priority_mode(tg, priority_mode);
	tg->library_name = xxstrdup("task-graph-library");
	tg->function_name = xxstrdup("compute_group_keys");
	tg->time_spent_on_file_pruning = 0;
	tg->manager = q;
	tg->staging_dir = xxstrdup(staging_dir);
	tg->prune_algorithm = PRUNE_ALGORITHM_REAL_TIME;
	debug(D_VINE, "set staging dir to: %s", tg->staging_dir);

	return tg;
}

void vine_task_graph_set_node_outfile_remote_name(struct vine_task_graph *tg, const char *node_key, const char *outfile_remote_name)
{
	if (!tg || !node_key || !outfile_remote_name) {
		return;
	}
	struct vine_task_node *node = hash_table_lookup(tg->nodes, node_key);
	if (!node) {
		return;
	}
	if (node->outfile_remote_name) {
		free(node->outfile_remote_name);
	}
	node->outfile_remote_name = xxstrdup(outfile_remote_name);
}

static void vine_task_graph_print_node_info(struct vine_task_node *node)
{
	if (!node) {
		return;
	}

	debug(D_VINE, "node info %s task_id: %d", node->node_key, node->task->task_id);
	debug(D_VINE, "node info %s depth: %d", node->node_key, node->depth);
	debug(D_VINE, "node info %s outfile remote name: %s", node->node_key, node->outfile_remote_name);

	char *parent_keys = NULL;
	struct vine_task_node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		if (!parent_keys) {
			parent_keys = string_format("%s", parent_node->node_key);
		} else {
			char *tmp = string_format("%s, %s", parent_keys, parent_node->node_key);
			free(parent_keys);
			parent_keys = tmp;
		}
	}
	debug(D_VINE, "node info %s parents: %s", node->node_key, parent_keys);
	free(parent_keys);

	char *child_keys = NULL;
	struct vine_task_node *child_node;
	LIST_ITERATE(node->children, child_node)
	{
		if (!child_keys) {
			child_keys = string_format("%s", child_node->node_key);
		} else {
			char *tmp = string_format("%s, %s", child_keys, child_node->node_key);
			free(child_keys);
			child_keys = tmp;
		}
	}
	debug(D_VINE, "node info %s children: %s", node->node_key, child_keys);
	free(child_keys);

	switch (node->output_store_location) {
	case VINE_OUTPUT_STORE_LOCATION_STAGING_DIR:
		debug(D_VINE, "node info %s output store location: staging directory", node->node_key);
		break;
	case VINE_OUTPUT_STORE_LOCATION_TEMP:
		debug(D_VINE, "node info %s output store location: temp", node->node_key);
		break;
	case VINE_OUTPUT_STORE_LOCATION_CHECKPOINT:
		debug(D_VINE, "node info %s output store location: checkpoint", node->node_key);
		break;
	case VINE_OUTPUT_STORE_LOCATION_SHARED_FILE_SYSTEM:
		debug(D_VINE, "node info %s output store location: shared file system", node->node_key);
		break;
	}

	return;
}

struct vine_task_node *vine_task_graph_create_node(struct vine_task_graph *tg,
		const char *node_key,
		const char *outfile_remote_name,
		vine_task_graph_node_output_store_location_t output_store_location)
{
	if (!tg || !node_key) {
		return NULL;
	}

	struct vine_task_node *node = hash_table_lookup(tg->nodes, node_key);
	if (node) {
		return NULL;
	}

	node = xxmalloc(sizeof(struct vine_task_node));
	node->node_key = xxstrdup(node_key);
	node->outfile_remote_name = xxstrdup(outfile_remote_name);
	node->depth = -1;
	node->parents = list_create();
	node->children = list_create();
	node->prune_blocking_children_remaining = 0;
	node->reverse_prune_waiters = list_create();
	node->pending_parents = set_create(0);
	node->critical_time = 0;
	node->active = 1;
	node->output_store_location = output_store_location;

	hash_table_insert(tg->nodes, node_key, node);

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
		if (node->node_key) {
			free(node->node_key);
		}
		if (node->outfile_remote_name) {
			free(node->outfile_remote_name);
		}
		delete_node_task(tg, node);
		list_delete(node->parents);
		list_delete(node->children);
		list_delete(node->reverse_prune_waiters);
		if (node->pending_parents) {
			set_delete(node->pending_parents);
		}
		free(node);
	}
	free(tg->library_name);
	free(tg->function_name);

	hash_table_delete(tg->nodes);
	itable_delete(tg->task_id_to_node);
	hash_table_delete(tg->outfile_cachename_to_node);
	free(tg);
}
