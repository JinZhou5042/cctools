#ifndef DAGVINE_GRAPH_H
#define DAGVINE_GRAPH_H

#include <stdint.h>

#include "hash_table.h"
#include "itable.h"
#include "node.h"

struct graph {
	struct itable *nodes;
	struct hash_table *outfile_cachename_to_node;
	struct hash_table *inout_filename_to_cached_name;

	char *checkpoint_dir;
	char *output_dir;
	char *task_runner_library_name;
	char *task_runner_function_name;

	double checkpoint_fraction;
	int prune_depth;

	int print_graph_details;
};

/* Public APIs for operating the executor graph */

/** Create an executor graph and return it.
@param runtime_dir Runtime directory used for default graph output paths.
@return A new executor graph.
*/
struct graph *graph_create(const char *runtime_dir);

/** Create a new node in the executor graph.
@param g Reference to the executor graph.
@return The auto-assigned node id.
*/
uint64_t graph_add_node(struct graph *g);

/** Mark a node as a retrieval target.
@param g Reference to the executor graph.
@param node_id Identifier of the node to mark as target.
*/
void graph_set_target(struct graph *g, uint64_t node_id);

/** Add a dependency between two nodes in the executor graph.
@param g Reference to the executor graph.
@param parent_id Identifier of the parent node.
@param child_id Identifier of the child node.
*/
void graph_add_dependency(struct graph *g, uint64_t parent_id, uint64_t child_id);

/** Finalize the metrics of the executor graph.
@param g Reference to the executor graph.
*/
void graph_finalize(struct graph *g);

/** Get the heavy score of a node in the executor graph.
@param g Reference to the executor graph.
@param node_id Identifier of the node.
@return The heavy score.
*/
double graph_get_node_heavy_score(const struct graph *g, uint64_t node_id);

/** Get the outfile remote name of a node in the executor graph.
@param g Reference to the executor graph.
@param node_id Identifier of the node.
@return The outfile remote name.
*/
const char *graph_get_node_outfile_remote_name(const struct graph *g, uint64_t node_id);

/** Delete an executor graph.
@param g Reference to the executor graph.
*/
void graph_delete(struct graph *g);

/** Get the task runner library name of the executor graph.
@param g Reference to the executor graph.
@return The task runner library name.
*/
const char *graph_get_task_runner_library_name(const struct graph *g);

/** Set the task runner function name of the executor graph.
@param g Reference to the executor graph.
@param task_runner_function_name Reference to the task runner function name.
*/
void graph_set_task_runner_function_name(struct graph *g, const char *task_runner_function_name);

/** Tune the executor graph.
@param g Reference to the executor graph.
@param name Reference to the name of the parameter to tune.
@param value Reference to the value of the parameter to tune.
@return 0 on success, -1 on failure.
*/
int graph_tune(struct graph *g, const char *name, const char *value);

#endif // DAGVINE_GRAPH_H
