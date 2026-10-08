#ifndef VINE_GRAPH_NODE_H
#define VINE_GRAPH_NODE_H

#include <stddef.h>

#include "set.h"
#include "timestamp.h"

#include "list.h"
#include "taskvine.h"

/**
 * One element of @c extra_outputs or @c extra_inputs: a logical filename plus its @c vine_file
 * (declared during graph build; attached to @c vine_task in @c vine_graph_executor_materialize_node).
 */
struct vine_graph_io_mount {
	struct vine_file *file;
	char *remote_name;
};

/** The node object. */
struct vine_graph_node {
	uint64_t node_id; // graph assigned id
	int is_target; // if set, output is retrieved when the task completes

	struct vine_task *task;
	struct vine_file *task_runner_arg_file; // JSON args buffer for the runner
	struct vine_file *outfile;		// Manager-owned output, declared during finalize
	char *outfile_remote_name;

	struct list *parents;
	struct list *children;
	/**
	 * Files tracked by TaskHandle.file(), beyond this node's primary
	 * Python-result outfile. Filled before
	 * @c node->task exists; consumed when building the task at submit / materialize time.
	 */
	struct list *extra_outputs;
	/**
	 * FileHandle and execution-data inputs beyond Python-result dependencies. Same lifecycle
	 * as @c extra_outputs: queued at graph build, wired on @c vine_task at materialize.
	 */
	struct list *extra_inputs;

	int remaining_parents_count; // parents not yet satisfied for scheduling
	struct set *fired_parents;   // parents already counted toward that count
	int completed;
	int cut; // return released by cut, cleared if recovery restores file
	/** Non-zero after this node's temp output was released under @c graph->prune_depth; cleared on recovery. */
	int released_by_prune_depth;
	int in_resubmit_queue;
	timestamp_t last_failure_time; // last enqueue to resubmit queue

	int depth;
	int height;

	/** Latest @c vine_graph_executor_submit_node interval for this node (microseconds); graph total is on @c struct vine_graph_executor. */
	uint64_t preprocessing_time_us;
	/** Latest @c vine_graph_executor_run_completion_postprocess interval for this node (microseconds); graph total on executor. */
	uint64_t postprocessing_time_us;
};

/** Create a new node.
@param node_id Unique node identifier supplied by the owning graph.
@return Newly allocated node instance.
*/
struct vine_graph_node *vine_graph_node_create(uint64_t node_id);

/**
 * Add parent->child if that edge is not already present (idempotent).
 */
void vine_graph_node_ensure_dependency(struct vine_graph_node *parent, struct vine_graph_node *child);

/** Create the task arguments for a node.
@param node Reference to the node.
@return The task arguments in JSON format: {"fn_args": ["node_id"], "fn_kwargs": {}} (string id for run_node).
*/
char *vine_graph_node_construct_task_arguments(struct vine_graph_node *node);

/** Delete a node and release owned resources.
@param node Reference to the node.
*/
void vine_graph_node_delete(struct vine_graph_node *node);

/** Print information about a node.
@param node Reference to the node.
*/
void vine_graph_node_debug_print(struct vine_graph_node *node);

#endif // VINE_GRAPH_NODE_H
