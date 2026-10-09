#include <inttypes.h>
#include <stdlib.h>

#include "debug.h"
#include "list.h"
#include "vine_graph.h"
#include "xxmalloc.h"

struct vine_graph *vine_graph_create(void)
{
	struct vine_graph *g = malloc(sizeof(*g));
	if (!g) {
		return NULL;
	}
	g->nodes = itable_create(0);
	g->outfile_cachename_to_node = hash_table_create(0, 0);
	g->file_id_to_file = itable_create(0);
	return g;
}

uint64_t vine_graph_add_node(struct vine_graph *g)
{
	if (!g) {
		return 0;
	}
	/* Nodes are never removed, so ids are dense and start from one. */
	uint64_t node_id = itable_size(g->nodes) + 1;
	struct vine_graph_node *node = vine_graph_node_create(node_id);
	if (!node) {
		return 0;
	}
	itable_insert(g->nodes, node_id, node);
	return node_id;
}

struct vine_graph_node *vine_graph_get_node(const struct vine_graph *g, uint64_t node_id)
{
	return g ? itable_lookup(g->nodes, node_id) : NULL;
}

int vine_graph_add_dependency(struct vine_graph *g, uint64_t parent_id, uint64_t child_id)
{
	struct vine_graph_node *parent = vine_graph_get_node(g, parent_id);
	struct vine_graph_node *child = vine_graph_get_node(g, child_id);
	if (!parent || !child) {
		debug(D_ERROR, "dependency %" PRIu64 " -> %" PRIu64 " names an unknown node", parent_id, child_id);
		return -1;
	}
	vine_graph_node_ensure_dependency(parent, child);
	return 0;
}

int vine_graph_finalize(struct vine_graph *g)
{
	if (!g) {
		return -1;
	}

	/* Kahn's algorithm: a node's depth is final when its last parent is visited. */
	struct itable *remaining = itable_create(0);
	struct list *ready = list_create();
	uint64_t node_id;
	struct vine_graph_node *node;
	int iteration;
	ITABLE_ITERATE(g->nodes, iteration, node_id, node)
	{
		node->depth = 0;
		itable_insert(remaining, node_id, (void *)(intptr_t)list_size(node->parents));
		if (list_size(node->parents) == 0) {
			list_push_tail(ready, node);
		}
	}

	int visited = 0;
	while ((node = list_pop_head(ready))) {
		visited++;
		struct vine_graph_node *child;
		LIST_ITERATE(node->children, child)
		{
			if (child->depth < node->depth + 1) {
				child->depth = node->depth + 1;
			}
			intptr_t left = (intptr_t)itable_lookup(remaining, child->node_id) - 1;
			itable_insert(remaining, child->node_id, (void *)left);
			if (left == 0) {
				list_push_tail(ready, child);
			}
		}
	}
	list_delete(ready);
	itable_delete(remaining);

	if (visited != itable_size(g->nodes)) {
		debug(D_ERROR, "graph has a cycle: only %d of %d nodes are reachable in dependency order", visited, itable_size(g->nodes));
		return -1;
	}
	return 0;
}

void vine_graph_delete(struct vine_graph *g)
{
	if (!g) {
		return;
	}
	uint64_t node_id;
	struct vine_graph_node *node;
	int iteration;
	ITABLE_ITERATE(g->nodes, iteration, node_id, node)
	{
		vine_graph_node_delete(node);
	}
	itable_delete(g->nodes);
	hash_table_delete(g->outfile_cachename_to_node);
	itable_delete(g->file_id_to_file);
	free(g);
}
