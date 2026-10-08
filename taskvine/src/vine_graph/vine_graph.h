#ifndef VINE_GRAPH_H
#define VINE_GRAPH_H

#include "hash_table.h"
#include "itable.h"

#include "vine_graph_node.h"

struct vine_graph {
	struct itable *nodes;
	struct itable *recovery_time_by_node; // node ID -> owned double, completion-time estimate in seconds
	struct hash_table *outfile_cachename_to_node;
	/** Maps FileHandle and execution-data ids to their declared vine_file. */
	struct itable *file_id_to_file;

	char *output_dir;
	char *task_runner_library_name;
	char *task_runner_function_name;

	int prune_depth;

	int print_graph_details;
};

// Public graph API (declarations below)

/** Create an executor graph and return it.
@param runtime_dir Runtime directory used for default graph output paths.
@return A new executor graph.
*/
struct vine_graph *vine_graph_create(const char *runtime_dir);

/** Create a new node in the executor graph.
@param g Reference to the executor graph.
@return The auto-assigned node id.
*/
uint64_t vine_graph_add_node(struct vine_graph *g);

/** Mark a node as a retrieval target.
@param g Reference to the executor graph.
@param node_id Identifier of the node to mark as target.
*/
void vine_graph_set_target(struct vine_graph *g, uint64_t node_id);

/** Add a dependency between two nodes in the executor graph.
@param g Reference to the executor graph.
@param parent_id Identifier of the parent node.
@param child_id Identifier of the child node.
*/
void vine_graph_add_dependency(struct vine_graph *g, uint64_t parent_id, uint64_t child_id);

/** Finalize the metrics of the executor graph.
@param g Reference to the executor graph.
*/
void vine_graph_finalize(struct vine_graph *g);

/** Get the outfile remote name of a node in the executor graph.
@param g Reference to the executor graph.
@param node_id Identifier of the node.
@return The outfile remote name.
*/
const char *vine_graph_get_node_outfile_remote_name(const struct vine_graph *g, uint64_t node_id);

/** Delete an executor graph.
@param g Reference to the executor graph.
*/
void vine_graph_delete(struct vine_graph *g);

/** Get the task runner library name of the executor graph.
@param g Reference to the executor graph.
@return The task runner library name.
*/
const char *vine_graph_get_task_runner_library_name(const struct vine_graph *g);

/** Set the task runner function name of the executor graph.
@param g Reference to the executor graph.
@param task_runner_function_name Reference to the task runner function name.
*/
void vine_graph_set_task_runner_function_name(struct vine_graph *g, const char *task_runner_function_name);

/** Tune the executor graph.
@param g Reference to the executor graph.
@param name Reference to the name of the parameter to tune.
@param value Reference to the value of the parameter to tune.
@return 0 on success, -1 on failure.
*/
int vine_graph_tune(struct vine_graph *g, const char *name, const char *value);

#endif // VINE_GRAPH_H
