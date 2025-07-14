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
#include <sys/stat.h>
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

static void submit_node_task(struct vine_task_graph *tg, struct vine_task_node *node);
static void resubmit_node_task(struct vine_task_graph *tg, struct vine_task_node *node);
static void delete_node_task(struct vine_task_node *node);
static void create_node_task(struct vine_task_graph *tg, struct vine_task_node *node);
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
			submit_node_task(tg, child_node);
		}
	}

	return;
}

static struct vine_task_node *create_node(struct vine_task_graph *tg, const char *node_key)
{
	if (!tg || !node_key) {
		return NULL;
	}

	struct vine_task_node *node = hash_table_lookup(tg->nodes, node_key);
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

	hash_table_insert(tg->nodes, node_key, node);
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

static void prune_nls_file(struct vine_task_graph *tg, struct vine_file *f)
{
	if (!tg || !f) {
		return;
	}

	/* delete all of the replicas present at remote workers. */
	struct set *source_workers = hash_table_lookup(tg->manager->file_worker_table, f->cached_name);
	if (source_workers && set_size(source_workers) > 0) {
		struct vine_worker_info *w;
		SET_ITERATE(source_workers, w)
		{
			/* skip if a checkpoint worker */
			if (is_checkpoint_worker(tg->manager, w)) {
				continue;
			}
			delete_worker_file(tg->manager, w, f->cached_name, 0, 0);
		}
	}
}

static void prune_node_waiters(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	/* prune stale files based on global prune_depth setting */
	/* this node just finished, now notify others who were waiting on it */
	char *waiter_key;
	struct vine_task_node *waiter_node;
	HASH_TABLE_ITERATE(node->reverse_prune_waiters, waiter_key, waiter_node)
	{
		waiter_node->prune_blocking_children_remaining--;
		if (waiter_node->prune_blocking_children_remaining == 0 && tg->nls_prune_depth > 0) {
			if (waiter_node->needs_checkpointing) {
				continue;
			}
			prune_nls_file(tg, waiter_node->outfile);
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
	if (graph->nls_prune_depth <= 0) {
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
		if (graph->nls_prune_depth == 1) {
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
			while (list_size(bfs_nodes) > 0 && current_depth < graph->nls_prune_depth) {
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

static int vine_file_exists_on_local(struct vine_task_graph *tg, struct vine_file *f)
{
	if (!tg || !f || f->type != VINE_FILE) {
		return 0;
	}

	struct stat local_info;

	if (stat(f->source, &local_info) == 0) {
		return 1;
	} else {
		return 0;
	}
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

static void resubmit_node_task(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	delete_node_task(node);
	create_node_task(tg, node);
	submit_node_task(tg, node);
}

static void replicate_node(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	node->outfile->needs_replication = 1;
	vine_temp_redundancy_replicate_file(tg->manager, node->outfile);
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

static void checkpoint_node(struct vine_task_graph *tg, struct vine_task_node *node)
{
	if (!tg || !node) {
		return;
	}

	node->outfile->needs_checkpointing = 1;
	vine_temp_redundancy_checkpoint_file(tg->manager, node->outfile);
}

/*************************************************************/
/* Public APIs */
/*************************************************************/

struct set *handle_checkpoint_worker_stagein(struct vine_task_graph *tg, struct vine_worker_info *w, const char *cachename)
{
	if (!tg || !w || !cachename) {
		return NULL;
	}

	/* find the node that corresponds to this cached file */
	struct vine_task_node *this_node = hash_table_lookup(tg->outfile_cachename_to_node, cachename);
	if (!this_node) {
		return NULL;
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

	struct set *files_to_prune = set_create(0);

	/* mark all nodes in the visited set as inactive */
	struct vine_task_node *visited_node;
	SET_ITERATE(visited_nodes, visited_node)
	{
		if (visited_node == this_node) {
			continue;
		}
		visited_node->active = 0;
		set_insert(files_to_prune, visited_node->outfile);
	}

	list_delete(bfs_nodes);
	set_delete(visited_nodes);

	tg->time_spent_on_file_pruning += timestamp_get() - start_time;

	return files_to_prune;
}

void vine_task_graph_set_nls_prune_depth(struct vine_task_graph *tg, int nls_prune_depth)
{
	if (!tg) {
		return;
	}

	tg->nls_prune_depth = MAX(nls_prune_depth, 0);
}

void vine_task_graph_set_priority_mode(struct vine_task_graph *tg, vine_task_priority_mode_t mode)
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

void vine_task_graph_execute(struct vine_task_graph *tg)
{
	if (!tg) {
		return;
	}

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

		struct vine_task *task = vine_wait(tg->manager, 15);
		progress_bar_update_part_total(pbar, recovery_tasks_part, tg->manager->num_submitted_recovery_tasks);
		if (task) {
			/* skip recovery tasks */
			if (task->type == VINE_TASK_TYPE_RECOVERY) {
				progress_bar_advance_part_current(pbar, recovery_tasks_part, 1);
				continue;
			}

			/* get the original node by task id */
			struct vine_task_node *node = itable_lookup(tg->task_id_to_node, task->task_id);

			/* in case of failure, resubmit this task */
			if (task->result != VINE_RESULT_SUCCESS || task->exit_code != 0) {
				debug(D_VINE | D_NOTICE, "task %d failed, resubmitting", task->task_id);
				resubmit_node_task(tg, node);
				continue;
			}

			/* check if the output file exists on local */
			if (node->outfile->type == VINE_FILE) {
				if (!vine_file_exists_on_local(tg, node->outfile)) {
					debug(D_VINE | D_NOTICE, "file %s does not exist on local, resubmitting", node->outfile->cached_name);
					resubmit_node_task(tg, node);
					continue;
				}
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
			if (node->needs_checkpointing) {
				checkpoint_node(tg, node);
			} else if (node->needs_replication) {
				replicate_node(tg, node);
			}

			timestamp_t start_time = timestamp_get();
			prune_node_waiters(tg, node);
			tg->time_spent_on_file_pruning += timestamp_get() - start_time;

			/* submit children nodes with dependencies all resolved */
			submit_node_children(tg, node);
		} else {
			progress_bar_advance_part_current(pbar, recovery_tasks_part, 0);
		}
	}

	progress_bar_finish(pbar);
	progress_bar_delete(pbar);

	printf("time spent on file pruning: %ld\n", tg->time_spent_on_file_pruning);

	return;
}

static void delete_node_task(struct vine_task_node *node)
{
	if (!node) {
		return;
	}

	vine_task_delete(node->task);
	node->task = NULL;

	if (node->infile) {
		vine_file_delete(node->infile);
		node->infile = NULL;
	}

	if (node->outfile) {
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

	if (node->needs_persistency) {
		char *persistent_path = string_format("/tmp/jzhou/vine-staging/outputs/%s", node->outfile_remote_name);
		node->outfile = vine_declare_file(tg->manager, persistent_path, VINE_CACHE_LEVEL_WORKFLOW, 0);
		free(persistent_path);
	} else {
		node->outfile = vine_declare_temp(tg->manager);
	}

	if (!hash_table_lookup(tg->outfile_cachename_to_node, node->outfile->cached_name)) {
		hash_table_insert(tg->outfile_cachename_to_node, node->outfile->cached_name, node);
	}
	vine_task_add_output(node->task, node->outfile, node->outfile_remote_name, VINE_TRANSFER_ALWAYS);

	char *parent_key;
	struct vine_task_node *parent_node;
	HASH_TABLE_ITERATE(node->parents, parent_key, parent_node)
	{
		vine_task_add_input(node->task, parent_node->outfile, parent_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
	}

	return;
}

void vine_task_graph_finalize(struct vine_task_graph *tg, double nls_percentage, double checkpoint_percentage, double persistence_percentage)
{
	if (!tg) {
		return;
	}

	/* Check if the sum of percentages is valid */
	if (nls_percentage + checkpoint_percentage + persistence_percentage != 1.0) {
		debug(D_ERROR, "nls_percentage (%.2f) + checkpoint_percentage (%.2f) + persistence_percentage (%.2f) != 1.0", nls_percentage, checkpoint_percentage, persistence_percentage);
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
	char *node_key;
	LIST_ITERATE(topo_order, node_key)
	{
		struct vine_task_node *node = hash_table_lookup(tg->nodes, node_key);
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
		struct vine_task_node *current_node = hash_table_lookup(tg->nodes, node_key);

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
		create_node_task(tg, hash_table_lookup(tg->nodes, node_key));
	}

	while ((node_key = list_pop_head(topo_order))) {
		free(node_key);
	}
	list_delete(topo_order);

	/* initialize prune depth mappings */
	initialize_pruning(tg);

	/* initialize pending_parents for all nodes */
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(tg->nodes, node_key, node)
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

struct vine_task_graph *vine_task_graph_create(struct vine_manager *q)
{
	struct vine_task_graph *tg = xxmalloc(sizeof(struct vine_task_graph));
	tg->nodes = hash_table_create(0, 0);
	tg->task_id_to_node = itable_create(0);
	tg->outfile_cachename_to_node = hash_table_create(0, 0);
	tg->nls_prune_depth = 0;
	tg->priority_mode = VINE_TASK_PRIORITY_MODE_FIFO;
	tg->library_name = xxstrdup("task-graph-library");
	tg->function_name = xxstrdup("compute_group_keys");
	tg->time_spent_on_file_pruning = 0;
	tg->manager = q;

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

void vine_task_graph_add_dependency(struct vine_task_graph *tg, const char *parent_key, const char *child_key)
{
	if (!tg || !parent_key || !child_key) {
		return;
	}

	struct vine_task_node *child_node = hash_table_lookup(tg->nodes, child_key);
	struct vine_task_node *parent_node = hash_table_lookup(tg->nodes, parent_key);

	if (!child_node) {
		child_node = create_node(tg, child_key);
	}
	if (!parent_node) {
		parent_node = create_node(tg, parent_key);
	}

	/* check if dependency already exists */
	if (hash_table_lookup(child_node->parents, parent_key)) {
		return;
	}
	hash_table_insert(child_node->parents, parent_key, parent_node);
	hash_table_insert(parent_node->children, child_key, child_node);
	debug(D_VINE, "added dependency: %s -> %s", parent_key, child_key);
}

const char *vine_task_graph_get_library_name(const struct vine_task_graph *tg)
{
	if (!tg) {
		return NULL;
	}
	return tg->library_name;
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
		delete_node_task(node);
		hash_table_delete(node->parents);
		hash_table_delete(node->children);
		hash_table_delete(node->reverse_prune_waiters);
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
