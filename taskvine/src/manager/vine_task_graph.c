#include "vine_task_graph.h"
#include "taskvine.h"
#include "vine_manager.h"
#include <stdlib.h>
#include <string.h>
#include "debug.h"
#include "stringtools.h"
#include <unistd.h>
#include "xxmalloc.h"
#include "hash_table.h"
#include "list.h"
#include "vine_task.h"
#include "timestamp.h"
#include "vine_mount.h"
#include "progress_bar.h"
#include "random.h"
#include "assert.h"
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <stdatomic.h>

static volatile sig_atomic_t interrupted = 0;

static void handle_sigint(int signal)
{
	interrupted = 1;
}

struct vine_task_graph *vine_task_graph_create(struct vine_manager *m)
{
	struct vine_task_graph *graph = xxmalloc(sizeof(struct vine_task_graph));
	graph->nodes = hash_table_create(0, 0);
	graph->task_id_to_node = itable_create(0);
	graph->outfile_cachename_to_node = hash_table_create(0, 0);
	return graph;
}

void vine_task_graph_delete(struct vine_manager *m)
{
	if (!m->task_graph) {
		return;
	}

	char *node_key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(m->task_graph->nodes, node_key, node)
	{
		if (node->node_key) {
			free(node->node_key);
		}
		if (node->outfile_remote_name) {
			free(node->outfile_remote_name);
		}
		hash_table_delete(node->parents);
		hash_table_delete(node->children);
		free(node);
	}

	hash_table_delete(m->task_graph->nodes);
	itable_delete(m->task_graph->task_id_to_node);
	hash_table_delete(m->task_graph->outfile_cachename_to_node);
	free(m->task_graph);
	m->task_graph = NULL;
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
	node->completed = 0;
	hash_table_insert(m->task_graph->nodes, node_key, node);
	return node;
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

void vine_task_graph_finalize(struct vine_manager *m, char *library_name, char *function_name)
{
	if (!m->task_graph || !m) {
		return;
	}

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

	/* process nodes in topological order */
	LIST_ITERATE(topo_order, node_key)
	{
		struct vine_task_node *current_node = hash_table_lookup(m->task_graph->nodes, node_key);

		/* create task for this node */
		current_node->task = vine_task_create(function_name);
		vine_task_set_priority(current_node->task, current_node->depth);
		vine_task_set_library_required(current_node->task, library_name);

		/* create infile for this task, which is the node key */
		char *infile_content = string_format("%s", current_node->node_key);
		current_node->infile = vine_declare_buffer(m, infile_content, strlen(infile_content), VINE_CACHE_LEVEL_TASK, VINE_UNLINK_WHEN_DONE);
		if (!current_node->infile) {
			debug(D_ERROR, "%s: failed to create infile for node %s", __func__, node_key);
		}
		vine_task_add_input(current_node->task, current_node->infile, "infile", VINE_TRANSFER_ALWAYS);
		if (!current_node->infile) {
			debug(D_ERROR, "%s: failed to add infile for node %s", __func__, node_key);
		}

		/* create output file for this task */
		current_node->outfile = vine_declare_temp(m);
		hash_table_insert(m->task_graph->outfile_cachename_to_node, current_node->outfile->cached_name, current_node);
		vine_task_add_output(current_node->task, current_node->outfile, current_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);

		/* add inputs from parent nodes */
		char *parent_key;
		struct vine_task_node *parent_node;
		HASH_TABLE_ITERATE(current_node->parents, parent_key, parent_node)
		{
			if (!parent_node->outfile) {
				debug(D_ERROR, "%s: failed to add output file for node %s", __func__, node_key);
			}
			vine_task_add_input(current_node->task, parent_node->outfile, parent_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
			if (!current_node->infile) {
				debug(D_ERROR, "%s: failed to add infile for node %s", __func__, node_key);
			}
		}
	}

	list_delete(topo_order);

	return;
}

int is_file_expired(struct vine_manager *m, struct vine_file *f)
{
	if (!m->task_graph || !f) {
		return 0;
	}

	struct vine_task_node *node = hash_table_lookup(m->task_graph->outfile_cachename_to_node, f->cached_name);
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

void vine_task_graph_execute(struct vine_manager *m)
{
	if (!m || !m->task_graph) {
		return;
	}

	// Enable debug system for C code since it uses a separate debug system instance
	// from the Python bindings. Use the same function that the manager uses.
	char *debug_tmp = string_format("%s/vine-logs/debug", m->runtime_directory);
	vine_enable_debug_log(debug_tmp);
	free(debug_tmp);

	signal(SIGINT, handle_sigint);

	struct hash_table *pending_parents = hash_table_create(0, 0);
	struct hash_table *pending_children = hash_table_create(0, 0);

	char *node_key;
	struct vine_task_node *node;
	HASH_TABLE_ITERATE(m->task_graph->nodes, node_key, node)
	{
		int parents_count = hash_table_size(node->parents);
		int children_count = hash_table_size(node->children);
		hash_table_insert(pending_parents, node_key, (void *)(intptr_t)parents_count);
		hash_table_insert(pending_children, node_key, (void *)(intptr_t)children_count);
	}

	/* first enqueue those without dependencies */
	int initial_tasks = 0;
	HASH_TABLE_ITERATE(m->task_graph->nodes, node_key, node)
	{
		intptr_t parents_count = (intptr_t)hash_table_lookup(pending_parents, node_key);
		if (parents_count == 0) {
			int task_id = vine_submit(m, node->task);
			itable_insert(m->task_graph->task_id_to_node, task_id, node);
			initial_tasks++;
		}
	}

	int num_all_tasks = hash_table_size(m->task_graph->nodes);
	int num_completed_tasks = 0;

	struct ProgressBar *pbar = progress_bar_init("Executing Task Graph", num_all_tasks, 1);

	while (num_completed_tasks < num_all_tasks) {
		if (interrupted) {
			break;
		}

		struct vine_task *task = vine_wait(m, 5);
		if (task) {
			struct vine_task_node *node = itable_lookup(m->task_graph->task_id_to_node, task->task_id);
			num_completed_tasks++;
			progress_bar_update(pbar, 1);

			/* find the children nodes of this node to see if they are ready to run */
			char *child_key;
			struct vine_task_node *child_node;
			HASH_TABLE_ITERATE(node->children, child_key, child_node)
			{
				intptr_t active_parents_count = (intptr_t)hash_table_lookup(pending_parents, child_key);
				active_parents_count--;
				hash_table_remove(pending_parents, child_key);
				hash_table_insert(pending_parents, child_key, (void *)active_parents_count);
				if (active_parents_count == 0) {
					int task_id = vine_submit(m, child_node->task);
					itable_insert(m->task_graph->task_id_to_node, task_id, child_node);
				}
			}

			/* prune stale files */
			char *parent_key;
			struct vine_task_node *parent_node;
			HASH_TABLE_ITERATE(node->parents, parent_key, parent_node)
			{
				intptr_t pending_children_count = (intptr_t)hash_table_lookup(pending_children, parent_key);
				pending_children_count--;
				hash_table_remove(pending_children, parent_key);
				hash_table_insert(pending_children, parent_key, (void *)pending_children_count);
				if (pending_children_count == 0) {
					if (is_file_expired(m, parent_node->outfile)) {
						vine_prune_file(m, parent_node->outfile);
					}
				}
			}
		}
	}

	progress_bar_finish(pbar);

	hash_table_delete(pending_parents);
	hash_table_delete(pending_children);

	return;
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

	if (!hash_table_lookup(child_node->parents, parent_key)) {
		hash_table_insert(child_node->parents, parent_key, parent_node);
	}
	if (!hash_table_lookup(parent_node->children, child_key)) {
		hash_table_insert(parent_node->children, child_key, child_node);
	}

	debug(D_VINE, "added dependency: %s -> %s", parent_key, child_key);
}

void vine_task_graph_handle_task_done(struct vine_manager *m, struct vine_task *t)
{
	if (!m || !m->task_graph || !t || t->state != VINE_TASK_DONE) {
		return;
	}

	struct vine_task_node *node = itable_lookup(m->task_graph->task_id_to_node, t->task_id);
	if (!node) {
		return;
	}

	char *parent_key;
	struct vine_task_node *parent_node;
	HASH_TABLE_ITERATE(node->parents, parent_key, parent_node)
	{
		if (is_file_expired(m, parent_node->outfile)) {
			vine_prune_file(m, parent_node->outfile);
		}
	}
}