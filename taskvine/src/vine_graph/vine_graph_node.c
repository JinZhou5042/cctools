#include <inttypes.h>
#include <stdlib.h>

#include "debug.h"
#include "list.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "vine_graph_node.h"
#include "vine_file.h"

/*************************************************************/
/* Public APIs */
/*************************************************************/

/**
 * Create a node with empty dependency and file-mount lists.
 * @param node_id Identifier assigned by the graph.
 */
struct vine_graph_node *vine_graph_node_create(uint64_t node_id)
{
	struct vine_graph_node *node = xxmalloc(sizeof(struct vine_graph_node));

	node->is_target = 0;
	node->node_id = node_id;

	node->task = NULL;
	node->task_runner_arg_file = NULL;
	node->outfile = NULL;
	node->outfile_remote_name = string_format("outfile_node_%" PRIu64, node->node_id);

	node->parents = list_create();
	node->children = list_create();
	node->outputs = list_create();
	node->extra_inputs = list_create();
	node->remaining_parents_count = 0;
	node->fired_parents = NULL;
	node->completed = 0;
	node->cut = 0;
	node->released_by_prune_depth = 0;
	node->in_resubmit_queue = 0;
	node->last_failure_time = 0;

	node->depth = -1;

	node->preprocessing_time_us = 0;
	node->postprocessing_time_us = 0;
	node->execution_time_us = 0;

	return node;
}

/** Non-zero if parent->child is already in the adjacency lists (checked via parent's children). */
static int vine_graph_node_dependency_exists(struct vine_graph_node *parent, struct vine_graph_node *child)
{
	struct vine_graph_node *x;
	if (!parent || !child) {
		return 0;
	}
	LIST_ITERATE(parent->children, x)
	{
		if (x == child) {
			return 1;
		}
	}
	return 0;
}

void vine_graph_node_add_output(struct vine_graph_node *node, struct vine_file *file, const char *remote_name)
{
	struct vine_graph_io_mount *mount = xxmalloc(sizeof(*mount));
	mount->file = vine_file_addref(file);
	mount->remote_name = xxstrdup(remote_name);
	list_push_tail(node->outputs, mount);
}

void vine_graph_node_ensure_dependency(struct vine_graph_node *parent, struct vine_graph_node *child)
{
	if (!parent || !child || vine_graph_node_dependency_exists(parent, child)) {
		return;
	}
	list_push_tail(child->parents, parent);
	list_push_tail(parent->children, child);
}

/**
 * Construct the task arguments for the node.
 * @param node Reference to the node object.
 * @return The task arguments in JSON format: {"fn_args": ["node_id"], "fn_kwargs": {}} (string for run_node).
 */
char *vine_graph_node_construct_task_arguments(struct vine_graph_node *node)
{
	if (!node) {
		return NULL;
	}
	return string_format("{\"fn_args\":[\"%" PRIu64 "\"],\"fn_kwargs\":{}}", node->node_id);
}

/**
 * Delete the node and all of its associated resources.
 * @param node Reference to the node object.
 */
void vine_graph_node_delete(struct vine_graph_node *node)
{
	if (!node) {
		return;
	}

	if (node->outfile_remote_name) {
		free(node->outfile_remote_name);
	}

	vine_task_delete(node->task);
	node->task = NULL;

	list_delete(node->parents);
	list_delete(node->children);

	while (list_size(node->extra_inputs) > 0) {
		struct vine_graph_io_mount *m = list_pop_head(node->extra_inputs);
		free(m->remote_name);
		free(m);
	}
	list_delete(node->extra_inputs);
	while (list_size(node->outputs) > 0) {
		struct vine_graph_io_mount *m = list_pop_head(node->outputs);
		vine_file_delete(m->file);
		free(m->remote_name);
		free(m);
	}
	list_delete(node->outputs);

	if (node->fired_parents) {
		set_delete(node->fired_parents);
	}
	free(node);
}
