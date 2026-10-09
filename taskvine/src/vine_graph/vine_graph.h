#ifndef VINE_GRAPH_H
#define VINE_GRAPH_H

/* The DAG of one run: nodes, dependency edges, and the lookups from files to their declarations and producers.
 * It holds no run configuration. The executor owns the graph and is the only interface exposed to Python. */

#include "hash_table.h"
#include "itable.h"

#include "vine_graph_node.h"

struct vine_graph {
	struct itable *nodes;			      /* Maps node id -> owned node. */
	struct hash_table *outfile_cachename_to_node; /* Maps output cache name -> borrowed producer node. */
	struct itable *file_id_to_file;		      /* Maps FileHandle and edata ids -> declared vine_file. */
};

/* Create an empty graph. Return NULL on allocation failure. */
struct vine_graph *vine_graph_create(void);

/* Add a node and return its id, starting from one, or zero on failure. */
uint64_t vine_graph_add_node(struct vine_graph *g);

/* Look up a node by id. Return NULL when the id is unknown. */
struct vine_graph_node *vine_graph_get_node(const struct vine_graph *g, uint64_t node_id);

/* Add a parent -> child edge once. Return zero on success, or -1 when either node is unknown. */
int vine_graph_add_dependency(struct vine_graph *g, uint64_t parent_id, uint64_t child_id);

/* Compute each node's depth, the longest path from a source. Return zero on success, or -1 when the graph has a
 * cycle. */
int vine_graph_finalize(struct vine_graph *g);

/* Delete the graph and its nodes. */
void vine_graph_delete(struct vine_graph *g);

#endif // VINE_GRAPH_H
