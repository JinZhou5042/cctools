#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "debug.h"
#include "vine_graph.h"
#include "priority_queue.h"
#include "set.h"
#include "stringtools.h"
#include "uuid.h"
#include "xxmalloc.h"

/*************************************************************/
/* Private Functions */
/*************************************************************/

/**
 * Compute a topological ordering of the executor graph.
 * Call only after all nodes, edges, and metrics have been populated.
 * @param g Reference to the executor graph.
 * @return Nodes in topological order.
 */
static struct list *vine_graph_compute_topological_order(struct vine_graph *g)
{
	if (!g) {
		return NULL;
	}

	int total_nodes = itable_size(g->nodes);
	struct list *topo_order = list_create();
	struct itable *in_degree_map = itable_create(0);
	struct priority_queue *pq = priority_queue_create(total_nodes);

	uint64_t nid;
	struct vine_graph_node *node;
	int iteration;
	ITABLE_ITERATE(g->nodes, iteration, nid, node)
	{
		int deg = list_size(node->parents);
		itable_insert(in_degree_map, nid, (void *)(intptr_t)deg);
		if (deg == 0) {
			priority_queue_push(pq, node, -(double)node->node_id);
		}
	}

	while (priority_queue_size(pq) > 0) {
		struct vine_graph_node *current = priority_queue_pop(pq);
		list_push_tail(topo_order, current);

		struct vine_graph_node *child;
		LIST_ITERATE(current->children, child)
		{
			intptr_t raw_deg = (intptr_t)itable_lookup(in_degree_map, child->node_id);
			int deg = (int)raw_deg - 1;
			itable_insert(in_degree_map, child->node_id, (void *)(intptr_t)deg);

			if (deg == 0) {
				priority_queue_push(pq, child, -(double)child->node_id);
			}
		}
	}

	if (list_size(topo_order) != total_nodes) {
		debug(D_ERROR, "Error: executor graph contains cycles or is malformed.");
		debug(D_ERROR, "Expected %d nodes, but only sorted %d.", total_nodes, list_size(topo_order));

		uint64_t id;
		ITABLE_ITERATE(g->nodes, iteration, id, node)
		{
			intptr_t raw_deg = (intptr_t)itable_lookup(in_degree_map, id);
			int deg = (int)raw_deg;
			if (deg > 0) {
				debug(D_ERROR, "  Node %" PRIu64 " has in-degree %d. Parents:", id, deg);
				struct vine_graph_node *p;
				LIST_ITERATE(node->parents, p)
				{
					debug(D_ERROR, "    -> %" PRIu64, p->node_id);
				}
			}
		}

		list_delete(topo_order);
		itable_delete(in_degree_map);
		priority_queue_delete(pq);
		exit(1);
	}

	itable_delete(in_degree_map);
	priority_queue_delete(pq);
	return topo_order;
}

/**
 * Extract weakly connected components of the executor graph.
 * Currently used for debugging and instrumentation only.
 * @param g Reference to the executor graph.
 * @return List of weakly connected components.
 */
static struct list *vine_graph_extract_weak_components(struct vine_graph *g)
{
	if (!g) {
		return NULL;
	}

	struct set *visited = set_create(0);
	struct list *components = list_create();

	uint64_t nid;
	struct vine_graph_node *node;
	int iteration;
	ITABLE_ITERATE(g->nodes, iteration, nid, node)
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
			struct vine_graph_node *curr = list_pop_head(queue);

			struct vine_graph_node *p;
			LIST_ITERATE(curr->parents, p)
			{
				if (!set_lookup(visited, p)) {
					list_push_tail(queue, p);
					set_insert(visited, p);
					list_push_tail(component, p);
				}
			}

			struct vine_graph_node *c;
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

/*************************************************************/
/* Public APIs */
/*************************************************************/

/** Tune the executor graph.
 * @param g Reference to the executor graph.
 * @param name Reference to the name of the parameter to tune.
 * @param value Reference to the value of the parameter to tune.
 * @return 0 on success, -1 on failure.
 */
int vine_graph_tune(struct vine_graph *g, const char *name, const char *value)
{
	if (!g || !name || !value) {
		return -1;
	}

	if (strcmp(name, "output-dir") == 0) {
		if (mkdir(value, 0777) != 0 && errno != EEXIST) {
			debug(D_ERROR, "failed to mkdir %s (errno=%d)", value, errno);
			return -1;
		}
		free(g->output_dir);
		g->output_dir = xxstrdup(value);

	} else if (strcmp(name, "prune-depth") == 0) {
		int k = atoi(value);
		if (k < 0) {
			debug(D_ERROR, "invalid prune-depth: %s (must be >= 0; 0 disables prune-depth release)", value);
			return -1;
		}
		g->prune_depth = k;

	} else if (strcmp(name, "print-graph-details") == 0) {
		g->print_graph_details = (atoi(value) == 1) ? 1 : 0;
	} else {
		debug(D_ERROR, "invalid parameter name: %s", name);
		return -1;
	}

	return 0;
}

/**
 * Get the outfile remote name of a node in the executor graph.
 * @param g Reference to the executor graph.
 * @param node_id Reference to the node id.
 * @return The outfile remote name.
 */
const char *vine_graph_get_node_outfile_remote_name(const struct vine_graph *g, uint64_t node_id)
{
	if (!g) {
		return NULL;
	}

	struct vine_graph_node *node = itable_lookup(g->nodes, node_id);
	if (!node) {
		return NULL;
	}

	return node->outfile_remote_name;
}

/**
 * Get the task runner library name of the executor graph.
 * @param g Reference to the executor graph.
 * @return The task runner library name.
 */
const char *vine_graph_get_task_runner_library_name(const struct vine_graph *g)
{
	if (!g) {
		return NULL;
	}

	return g->task_runner_library_name;
}

/**
 * Set the task runner function name of the executor graph.
 * @param g Reference to the executor graph.
 * @param task_runner_function_name Reference to the task runner function name.
 */
void vine_graph_set_task_runner_function_name(struct vine_graph *g, const char *task_runner_function_name)
{
	if (!g || !task_runner_function_name) {
		return;
	}

	if (g->task_runner_function_name) {
		free(g->task_runner_function_name);
	}

	g->task_runner_function_name = xxstrdup(task_runner_function_name);
}

/**
 * Compute depth and height for each node.
 * Call after all nodes and dependencies are added.
 * @param g Reference to the executor graph.
 */
void vine_graph_finalize(struct vine_graph *g)
{
	if (!g) {
		return;
	}

	struct list *topo_order = vine_graph_compute_topological_order(g); // required for all metric passes
	if (!topo_order) {
		return;
	}

	struct vine_graph_node *node;
	struct vine_graph_node *parent_node;
	struct vine_graph_node *child_node;

	/* Longest path from any source in topo order. */
	LIST_ITERATE(topo_order, node)
	{
		node->depth = 0;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (node->depth < parent_node->depth + 1) {
				node->depth = parent_node->depth + 1;
			}
		}
	}

	/* Longest path to any sink in reverse topo order. */
	LIST_ITERATE_REVERSE(topo_order, node)
	{
		node->height = 0;
		LIST_ITERATE(node->children, child_node)
		{
			if (node->height < child_node->height + 1) {
				node->height = child_node->height + 1;
			}
		}
	}

	if (g->print_graph_details) {
		// weakly connected components and vine_graph_node_debug_print, debug only
		struct list *weakly_connected_components = vine_graph_extract_weak_components(g);
		struct list *component;
		int component_index = 0;
		debug(D_VINE, "graph has %d weakly connected components\n", list_size(weakly_connected_components));
		LIST_ITERATE(weakly_connected_components, component)
		{
			debug(D_VINE, "component %d size: %d\n", component_index, list_size(component));
			list_delete(component);
			component_index++;
		}
		list_delete(weakly_connected_components);

		LIST_ITERATE(topo_order, node)
		{
			vine_graph_node_debug_print(node);
		}
	}

	list_delete(topo_order);

	return;
}

/**
 * Create a new node and track it in the executor graph.
 * @param g Reference to the executor graph.
 * @return The auto-assigned node id.
 */
uint64_t vine_graph_add_node(struct vine_graph *g)
{
	if (!g) {
		return 0;
	}

	uint64_t candidate_id = itable_size(g->nodes);
	candidate_id += 1; // skip zero, search upward until unused
	while (itable_lookup(g->nodes, candidate_id)) {
		candidate_id++;
	}
	uint64_t node_id = candidate_id;

	struct vine_graph_node *node = vine_graph_node_create(node_id); // defaults to non-target

	if (!node) {
		debug(D_ERROR, "failed to create node %" PRIu64, node_id);
		vine_graph_delete(g);
		exit(1);
	}

	itable_insert(g->nodes, node_id, node);

	return node_id;
}

/**
 * Mark a node as a retrieval target.
 */
void vine_graph_set_target(struct vine_graph *g, uint64_t node_id)
{
	if (!g) {
		return;
	}
	struct vine_graph_node *node = itable_lookup(g->nodes, node_id);
	if (!node) {
		debug(D_ERROR, "node %" PRIu64 " not found", node_id);
		exit(1);
	}

	node->is_target = 1;
}

/**
 * Create a new executor graph using graph-owned path configuration.
 * @param runtime_dir Runtime directory used as the default path root.
 * @return A new executor graph instance.
 */
struct vine_graph *vine_graph_create(const char *runtime_dir)
{
	if (!runtime_dir) {
		return NULL;
	}

	struct vine_graph *g = xxmalloc(sizeof(struct vine_graph));

	g->output_dir = xxstrdup(runtime_dir);	   // default to current working directory

	g->nodes = itable_create(0);
	g->recovery_time_by_node = itable_create(0);
	g->outfile_cachename_to_node = hash_table_create(0, 0);
	g->file_id_to_file = itable_create(0);

	cctools_uuid_t task_runner_library_name_id;
	cctools_uuid_create(&task_runner_library_name_id);
	g->task_runner_library_name = xxstrdup(task_runner_library_name_id.str);

	g->task_runner_function_name = NULL;

	/* Default prune-depth: release a TEMP node as soon as all of its
	 * direct children have completed. Set to 0 via tune("prune-depth") to
	 * disable and rely exclusively on cut-propagation. */
	g->prune_depth = 1;

	g->print_graph_details = 0;

	return g;
}

/**
 * Add a dependency between two nodes in the executor graph. Note that the input-output file relationship
 * is not handled here, because their file names might not have been determined yet.
 * @param g Reference to the executor graph.
 * @param parent_id Reference to the parent node id.
 * @param child_id Reference to the child node id.
 */
void vine_graph_add_dependency(struct vine_graph *g, uint64_t parent_id, uint64_t child_id)
{
	if (!g) {
		return;
	}

	struct vine_graph_node *parent_node = itable_lookup(g->nodes, parent_id);
	struct vine_graph_node *child_node = itable_lookup(g->nodes, child_id);
	if (!parent_node) {
		debug(D_ERROR, "parent node %" PRIu64 " not found", parent_id);
		exit(1);
	}
	if (!child_node) {
		debug(D_ERROR, "child node %" PRIu64 " not found", child_id);
		exit(1);
	}

	vine_graph_node_ensure_dependency(parent_node, child_node);

	return;
}

/**
 * Delete an executor graph instance.
 * @param g Reference to the executor graph.
 */
void vine_graph_delete(struct vine_graph *g)
{
	if (!g) {
		return;
	}

	int iteration;
	uint64_t nid;
	struct vine_graph_node *node;
	ITABLE_ITERATE(g->nodes, iteration, nid, node)
	{
		vine_graph_node_delete(node);
	}

	free(g->task_runner_library_name);
	free(g->task_runner_function_name);
	free(g->output_dir);

	itable_delete(g->nodes);
	itable_clear(g->recovery_time_by_node, free);
	itable_delete(g->recovery_time_by_node);
	hash_table_delete(g->outfile_cachename_to_node);


	itable_delete(g->file_id_to_file);

	free(g);
}
