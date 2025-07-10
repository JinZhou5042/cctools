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
#include "hash_table.h"
#include "itable.h"
#include "list.h"
#include "vine_task.h"
#include "timestamp.h"
#include "vine_mount.h"
#include "progress_bar.h"
#include "random.h"
#include "assert.h"
#include "macros.h"
#include "set.h"
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <stdatomic.h>

struct pending_insert_entry {
	struct vine_task_node *target_node;
	struct vine_task_node *source_node;
};

static void initialize_pruning(struct vine_task_graph *graph);
static double calculate_task_priority(struct vine_manager *m, struct vine_task_node *node);
static void submit_node(struct vine_manager *m, struct vine_task_node *node);

static int is_file_expired(struct vine_manager *m, struct vine_file *f);

static volatile sig_atomic_t interrupted = 0;

/*************************************************************/
/* Private Functions */
/*************************************************************/

static void handle_sigint(int signal)
{
	interrupted = 1;
}

static void submit_node_children(struct vine_manager *m, struct vine_task_node *node)
{
	if (!m || !m->task_graph || !node) {
		return;
	}

	char *child_key;
	struct vine_task_node *child_node;
	HASH_TABLE_ITERATE(node->children, child_key, child_node)
	{
		/* Remove this parent from the child's pending set if it exists */
		if (child_node->pending_parents) {
			/* Assert that this parent is indeed pending for the child */
			assert(set_lookup(child_node->pending_parents, node->node_key));
			set_remove(child_node->pending_parents, node->node_key);
		}

		/* If no more parents are pending, submit the child */
		if (!child_node->pending_parents || set_size(child_node->pending_parents) == 0) {
			submit_node(m, child_node);
		}
	}

	return;
}

static struct vine_task_node *create_node(struct vine_manager *m, const char *node_key)
{
	if (!m || !node_key) {
		return NULL;
	}

	struct vine_task_node *node = hash_table_lookup(m->task_graph->nodes, node_key);
	if (node) {
		return node;
	}

	node = xxmalloc(sizeof(struct vine_task_node));
	node->node_key = xxstrdup(node_key);
	char buf[33];
	random_hex(buf, sizeof(buf));
	node->outfile_remote_name = xxstrdup(buf);
	node->depth = -1;
	node->parents = hash_table_create(0, 0);
	node->children = hash_table_create(0, 0);
	node->prune_blocking_children_remaining = 0;
	node->reverse_prune_waiters = hash_table_create(0, 0);
	node->pending_parents = set_create(0);
	node->critical_time = 0;
	node->needs_checkpointing = 0;
	node->needs_replication = 0;
	node->needs_persistency = 0;
	node->active = 1;

	hash_table_insert(m->task_graph->nodes, node_key, node);
	return node;
}

static struct list *get_topological_order(struct vine_task_graph *graph)
{
	if (!graph) {
		return NULL;
	}

	int total_nodes = hash_table_size(graph->nodes);
	struct list *topo_order = list_create();
	int *in_degree = xxmalloc(total_nodes * sizeof(int));
	char **node_keys = xxmalloc(total_nodes * sizeof(char *));
	struct hash_table *key_to_index = hash_table_create(0, 0);

	/* Build node_keys and in_degree */
	int idx = 0;
	char *node_key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(graph->nodes, node_key, node)
	{
		node_keys[idx] = xxstrdup(node_key);
		in_degree[idx] = hash_table_size(node->parents);
		hash_table_insert(key_to_index, node_keys[idx], (void *)(long)idx);
		idx++;
	}

	int front = 0, back = 0;
	int *queue = xxmalloc(total_nodes * sizeof(int));

	for (int i = 0; i < total_nodes; i++) {
		if (in_degree[i] == 0) {
			queue[back++] = i;
		}
	}

	while (front < back) {
		int i = queue[front++];
		list_push_tail(topo_order, xxstrdup(node_keys[i]));

		struct vine_task_node *curr_node = hash_table_lookup(graph->nodes, node_keys[i]);

		char *child_key;
		struct vine_task_node *child;
		HASH_TABLE_ITERATE(curr_node->children, child_key, child)
		{
			intptr_t jval = (intptr_t)hash_table_lookup(key_to_index, child_key);
			int j = (int)jval;
			in_degree[j]--;
			if (in_degree[j] == 0) {
				queue[back++] = j;
			}
		}
	}

	for (int i = 0; i < total_nodes; i++) {
		free(node_keys[i]);
	}
	free(node_keys);
	free(in_degree);
	free(queue);
	hash_table_delete(key_to_index);

	return topo_order;
}

static void prune_nls_file(struct vine_manager *q, struct vine_file *f)
{
	if (!q || !q->task_graph || !f) {
		return;
	}

	/* delete all of the replicas present at remote workers. */
	struct set *source_workers = hash_table_lookup(q->file_worker_table, f->cached_name);
	if (source_workers && set_size(source_workers) > 0) {
		struct vine_worker_info *w;
		SET_ITERATE(source_workers, w)
		{
			/* skip if a checkpoint worker */
			if (is_checkpoint_worker(q, w)) {
				continue;
			}
			delete_worker_file(q, w, f->cached_name, 0, 0);
		}
	}
}

static int is_file_expired(struct vine_manager *q, struct vine_file *f)
{
	if (!q->task_graph || !f) {
		return 0;
	}

	struct vine_task_node *node = hash_table_lookup(q->task_graph->outfile_cachename_to_node, f->cached_name);
	if (!node) {
		return 0;
	}

	/* a file is expired if its outfile is no longer needed by any child node:
	 * 1. it has no pending dependents
	 * 2. all completed dependents have also completed their corresponding recovery tasks, if any */
	char *parent_key;
	struct vine_task_node *child_node;
	HASH_TABLE_ITERATE(node->children, parent_key, child_node)
	{
		/* if a task is not deleted, it means it is still running */
		if (child_node->task && child_node->task->state != VINE_TASK_DONE) {
			return 0;
		}
		struct vine_task *child_node_recovery_task = child_node->outfile->recovery_task;
		if (child_node_recovery_task && (child_node_recovery_task->state != VINE_TASK_INITIAL && child_node_recovery_task->state != VINE_TASK_DONE)) {
			return 0;
		}
	}

	return 1;
}

static void prune_node_waiters(struct vine_manager *q, struct vine_task_node *node)
{
	if (!q || !q->task_graph || !node) {
		return;
	}

	/* prune stale files based on global prune_depth setting */
	/* this node just finished, now notify others who were waiting on it */
	char *waiter_key;
	struct vine_task_node *waiter_node;
	HASH_TABLE_ITERATE(node->reverse_prune_waiters, waiter_key, waiter_node)
	{
		waiter_node->prune_blocking_children_remaining--;
		if (waiter_node->prune_blocking_children_remaining == 0 && q->task_graph->static_prune_depth > 0) {
			if (waiter_node->needs_checkpointing) {
				continue;
			}
			prune_nls_file(q, waiter_node->outfile);
		}
	}

	return;
}

static void initialize_pruning(struct vine_task_graph *graph)
{
	if (!graph) {
		return;
	}

	/* Skip if global prune depth is not set */
	if (graph->static_prune_depth <= 0) {
		return;
	}

	/* create mappings of nodes to their prune_depth */
	struct list *bfs_nodes = list_create();
	struct list *pending_inserts = list_create(); /* Store pending reverse_prune_waiters inserts */

	/* For each node P, calculate what downstream nodes it needs to wait for based on global prune_depth */
	char *node_key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(graph->nodes, node_key, node)
	{
		node->prune_blocking_children_remaining = 0;

		/* Fast path for prune_depth=1: only check direct children */
		if (graph->static_prune_depth == 1) {
			char *child_key;
			struct vine_task_node *child_node;
			HASH_TABLE_ITERATE(node->children, child_key, child_node)
			{
				node->prune_blocking_children_remaining++;
				hash_table_insert(child_node->reverse_prune_waiters, node->node_key, node);
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
			char *child_key;
			struct vine_task_node *child_node;
			HASH_TABLE_ITERATE(node->children, child_key, child_node)
			{
				if (!set_lookup(visited, child_key)) {
					set_insert(visited, child_key);
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
			while (list_size(bfs_nodes) > 0 && current_depth < graph->static_prune_depth) {
				int level_size = list_size(bfs_nodes);

				for (int i = 0; i < level_size; i++) {
					struct vine_task_node *current_node = list_pop_head(bfs_nodes);

					/* add current_node's children to next level */
					HASH_TABLE_ITERATE(current_node->children, child_key, child_node)
					{
						if (!set_lookup(visited, child_key)) {
							set_insert(visited, child_key);
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
				hash_table_insert(entry->target_node->reverse_prune_waiters, entry->source_node->node_key, entry->source_node);
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

static double calculate_task_priority(struct vine_manager *m, struct vine_task_node *node)
{
	if (!m || !m->task_graph || !node) {
		return 0.0;
	}

	double priority = 0.0;
	timestamp_t current_time = timestamp_get();

	switch (m->task_graph->priority_mode) {
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
		priority = 0.0;
		char *parent_key;
		struct vine_task_node *parent_node;
		HASH_TABLE_ITERATE(node->parents, parent_key, parent_node)
		{
			if (parent_node->outfile) {
				priority += (double)vine_file_size(parent_node->outfile);
			}
		}
		break;
	}

	return priority;
}

static void submit_node(struct vine_manager *m, struct vine_task_node *node)
{
	if (!m || !m->task_graph || !node) {
		return;
	}

	double priority = calculate_task_priority(m, node);
	vine_task_set_priority(node->task, priority);

	int task_id = vine_submit(m, node->task);
	itable_insert(m->task_graph->task_id_to_node, task_id, node);

	return;
}

static void replicate_node(struct vine_manager *q, struct vine_task_node *node)
{
	if (!q || !q->task_graph || !node) {
		return;
	}

	node->outfile->needs_replication = 1;
	vine_temp_redundancy_replicate_file(q, node->outfile);
}

static void update_node_critical_time(struct vine_task_node *node, timestamp_t execution_time)
{
	timestamp_t max_parent_critical_time = 0;
	char *parent_key;
	struct vine_task_node *parent_node;
	HASH_TABLE_ITERATE(node->parents, parent_key, parent_node)
	{
		if (parent_node->critical_time > max_parent_critical_time) {
			max_parent_critical_time = parent_node->critical_time;
		}
	}
	node->critical_time = max_parent_critical_time + execution_time;
}

static void checkpoint_node(struct vine_manager *q, struct vine_task_node *node)
{
	if (!q || !q->task_graph || !node) {
		return;
	}

	node->outfile->needs_checkpointing = 1;
	vine_temp_redundancy_checkpoint_file(q, node->outfile);
}

/*************************************************************/
/* Public APIs */
/*************************************************************/

void handle_checkpoint_worker_stagein(struct vine_manager *m, struct vine_worker_info *w, const char *cachename)
{
	if (!m || !m->task_graph || !w || !cachename) {
		return;
	}

	/* find the node that corresponds to this cached file */
	struct vine_task_node *this_node = hash_table_lookup(m->task_graph->outfile_cachename_to_node, cachename);
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
		char *parent_key;
		struct vine_task_node *parent_node;
		HASH_TABLE_ITERATE(current->parents, parent_key, parent_node)
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
			char *child_key;
			struct vine_task_node *child_node;
			HASH_TABLE_ITERATE(parent_node->children, child_key, child_node)
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
		vine_prune_file(m, visited_node->outfile);
	}

	list_delete(bfs_nodes);
	set_delete(visited_nodes);

	m->time_spent_on_file_pruning += timestamp_get() - start_time;
}

void vine_task_graph_set_static_prune_depth(struct vine_manager *m, int prune_depth)
{
	if (!m || !m->task_graph) {
		return;
	}

	m->task_graph->static_prune_depth = MAX(prune_depth, 0);
}

void vine_task_graph_set_priority_mode(struct vine_manager *m, vine_task_priority_mode_t mode)
{
	if (!m || !m->task_graph) {
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

	m->task_graph->priority_mode = mode;
}

void vine_task_graph_execute(struct vine_manager *m)
{
	if (!m || !m->task_graph) {
		return;
	}

	/* enable return recovery tasks */
	vine_enable_return_recovery_tasks(m);

	signal(SIGINT, handle_sigint);

	/* enqueue those without dependencies */
	char *node_key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(m->task_graph->nodes, node_key, node)
	{
		if (!node->pending_parents || set_size(node->pending_parents) == 0) {
			submit_node(m, node);
		}
	}

	struct ProgressBar *pbar = progress_bar_init("Executing Tasks");
	struct ProgressBarPart *regular_tasks_part = progress_bar_part_create("Regular", hash_table_size(m->task_graph->nodes));
	struct ProgressBarPart *recovery_tasks_part = progress_bar_part_create("Recovery", 0);
	progress_bar_add_part(pbar, regular_tasks_part);
	progress_bar_add_part(pbar, recovery_tasks_part);

	while (regular_tasks_part->current < regular_tasks_part->total) {
		if (interrupted) {
			break;
		}

		struct vine_task *task = vine_wait(m, 15);
		progress_bar_update_part_total(pbar, recovery_tasks_part, m->num_submitted_recovery_tasks);
		if (task) {
			/* skip recovery tasks */
			if (task->type == VINE_TASK_TYPE_RECOVERY) {
				progress_bar_advance_part_current(pbar, recovery_tasks_part, 1);
				continue;
			}

			/* set the start time to the submit time of the first regular task */
			if (regular_tasks_part->current == 0) {
				progress_bar_reset_start_time(pbar, task->time_when_submitted);
			}

			/* get the original node by task id */
			struct vine_task_node *node = itable_lookup(m->task_graph->task_id_to_node, task->task_id);

			/* update critical time */
			update_node_critical_time(node, task->time_workers_execute_last);

			/* mark node as completed */
			progress_bar_advance_part_current(pbar, regular_tasks_part, 1);

			/* enqueue the output file for replication or checkpointing */
			if (node->needs_checkpointing) {
				checkpoint_node(m, node);
			} else if (node->needs_replication) {
				replicate_node(m, node);
			}

			timestamp_t start_time = timestamp_get();
			prune_node_waiters(m, node);
			m->time_spent_on_file_pruning += timestamp_get() - start_time;

			/* submit children nodes with dependencies all resolved */
			submit_node_children(m, node);
		} else {
			progress_bar_advance_part_current(pbar, recovery_tasks_part, 0);
		}
	}

	progress_bar_finish(pbar);
	progress_bar_delete(pbar);

	printf("time spent on file pruning: %ld\n", m->time_spent_on_file_pruning);

	return;
}

void vine_task_graph_finalize(struct vine_manager *m, char *library_name, char *function_name, double persistence_percentage, double checkpoint_percentage)
{
	if (!m || !m->task_graph) {
		return;
	}

	if (persistence_percentage < 0.0) {
		persistence_percentage = 0.0;
	} else if (persistence_percentage > 1.0) {
		persistence_percentage = 1.0;
	}

	if (checkpoint_percentage < 0.0) {
		checkpoint_percentage = 0.0;
	} else if (checkpoint_percentage > 1.0) {
		checkpoint_percentage = 1.0;
	}

	if (persistence_percentage + checkpoint_percentage > 1.0) {
		double total = persistence_percentage + checkpoint_percentage;
		persistence_percentage = persistence_percentage / total;
		checkpoint_percentage = checkpoint_percentage / total;
	}

	/* enable debug system for C code since it uses a separate debug system instance
	 * from the Python bindings. Use the same function that the manager uses. */
	char *debug_tmp = string_format("%s/vine-logs/debug", m->runtime_directory);
	vine_enable_debug_log(debug_tmp);
	free(debug_tmp);

	/* get nodes in topological order */
	struct list *topo_order = get_topological_order(m->task_graph);
	if (!topo_order) {
		return;
	}

	/* compute depths of all nodes */
	char *node_key;
	LIST_ITERATE(topo_order, node_key)
	{
		struct vine_task_node *node = hash_table_lookup(m->task_graph->nodes, node_key);
		node->depth = 0;

		char *parent_key;
		struct vine_task_node *parent;
		HASH_TABLE_ITERATE(node->parents, parent_key, parent)
		{
			if (node->depth < parent->depth + 1) {
				node->depth = parent->depth + 1;
			}
		}
	}

	int total_tasks = list_size(topo_order);
	int tasks_to_persist = (int)(total_tasks * persistence_percentage);
	int tasks_to_checkpoint = (int)(total_tasks * checkpoint_percentage);
	int tasks_to_replicate = total_tasks - tasks_to_persist - tasks_to_checkpoint;

	int task_index = 0;
	LIST_ITERATE(topo_order, node_key)
	{
		struct vine_task_node *current_node = hash_table_lookup(m->task_graph->nodes, node_key);

		if (task_index < tasks_to_replicate) {
			current_node->needs_replication = 1;
		} else if (task_index < tasks_to_replicate + tasks_to_checkpoint) {
			current_node->needs_checkpointing = 1;
		} else {
			current_node->needs_persistency = 1;
		}

		task_index++;
	}

	LIST_ITERATE(topo_order, node_key)
	{
		struct vine_task_node *current_node = hash_table_lookup(m->task_graph->nodes, node_key);

		current_node->task = vine_task_create(function_name);
		vine_task_set_priority(current_node->task, current_node->depth);
		vine_task_set_library_required(current_node->task, library_name);

		char *infile_content = string_format("%s", current_node->node_key);
		current_node->infile = vine_declare_buffer(m, infile_content, strlen(infile_content), VINE_CACHE_LEVEL_TASK, VINE_UNLINK_WHEN_DONE);
		free(infile_content);

		vine_task_add_input(current_node->task, current_node->infile, "infile", VINE_TRANSFER_ALWAYS);

		if (current_node->needs_persistency) {
			char *persistent_path = string_format("/tmp/jzhou/vine-staging/outputs/%s", current_node->outfile_remote_name);
			current_node->outfile = vine_declare_file(m, persistent_path, VINE_CACHE_LEVEL_WORKFLOW, 0);
			free(persistent_path);
		} else {
			current_node->outfile = vine_declare_temp(m);
		}

		hash_table_insert(m->task_graph->outfile_cachename_to_node, current_node->outfile->cached_name, current_node);
		vine_task_add_output(current_node->task, current_node->outfile, current_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);

		char *parent_key;
		struct vine_task_node *parent_node;
		HASH_TABLE_ITERATE(current_node->parents, parent_key, parent_node)
		{
			vine_task_add_input(current_node->task, parent_node->outfile, parent_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
		}
	}

	while ((node_key = list_pop_head(topo_order))) {
		free(node_key);
	}
	list_delete(topo_order);

	/* initialize prune depth mappings */
	initialize_pruning(m->task_graph);

	/* initialize pending_parents for all nodes */
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(m->task_graph->nodes, node_key, node)
	{
		char *parent_key;
		struct vine_task_node *parent_node;
		HASH_TABLE_ITERATE(node->parents, parent_key, parent_node)
		{
			if (node->pending_parents) {
				/* Use parent_node->node_key to ensure pointer consistency */
				set_insert(node->pending_parents, parent_node->node_key);
			}
		}
	}

	return;
}

struct vine_task_graph *vine_task_graph_create()
{
	struct vine_task_graph *graph = xxmalloc(sizeof(struct vine_task_graph));
	graph->nodes = hash_table_create(0, 0);
	graph->task_id_to_node = itable_create(0);
	graph->outfile_cachename_to_node = hash_table_create(0, 0);
	graph->static_prune_depth = 0;
	graph->priority_mode = VINE_TASK_PRIORITY_MODE_FIFO;

	return graph;
}

void vine_task_graph_set_node_outfile_remote_name(struct vine_manager *m, const char *node_key, const char *outfile_remote_name)
{
	if (!m->task_graph || !node_key || !outfile_remote_name) {
		return;
	}
	struct vine_task_node *node = hash_table_lookup(m->task_graph->nodes, node_key);
	if (!node) {
		return;
	}
	if (node->outfile_remote_name) {
		free(node->outfile_remote_name);
	}
	node->outfile_remote_name = xxstrdup(outfile_remote_name);
}

void vine_task_graph_add_dependency(struct vine_manager *m, const char *child_key, const char *parent_key)
{
	if (!m->task_graph || !child_key || !parent_key) {
		return;
	}

	struct vine_task_node *child_node = hash_table_lookup(m->task_graph->nodes, child_key);
	struct vine_task_node *parent_node = hash_table_lookup(m->task_graph->nodes, parent_key);

	if (!child_node) {
		child_node = create_node(m, child_key);
	}
	if (!parent_node) {
		parent_node = create_node(m, parent_key);
	}

	/* check if dependency already exists */
	if (hash_table_lookup(child_node->parents, parent_key)) {
		return;
	}
	hash_table_insert(child_node->parents, parent_key, parent_node);
	hash_table_insert(parent_node->children, child_key, child_node);
	debug(D_VINE, "added dependency: %s -> %s", parent_key, child_key);
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
		hash_table_delete(node->parents);
		hash_table_delete(node->children);
		hash_table_delete(node->reverse_prune_waiters);
		if (node->pending_parents) {
			set_delete(node->pending_parents);
		}
		free(node);
	}

	hash_table_delete(tg->nodes);
	itable_delete(tg->task_id_to_node);
	hash_table_delete(tg->outfile_cachename_to_node);
	free(tg);
}
