#ifndef NODE_H
#define NODE_H

#include <stdint.h>

#include "vine_task.h"
#include "hash_table.h"
#include "list.h"
#include "set.h"
#include "taskvine.h"

/** The storage type of the node's output file. */
typedef enum {
	NODE_OUTFILE_TYPE_LOCAL = 0,	      /* Node-output file will be stored locally on the local staging directory */
	NODE_OUTFILE_TYPE_TEMP,		      /* Node-output file will be stored in the temporary node-local storage */
	NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM, /* Node-output file will be stored in the persistent shared file system */
} node_outfile_type_t;

/** The node object. */
struct node {
	/* Identity */
	uint64_t node_id; /* Unique identifier assigned by the graph when the node is created. */
	int is_target;	  /* If true, the output of the node is retrieved when the task finishes. */

	/* Task and files */
	struct vine_task *task;
	/* JSON args for the task runner call; parent outputs are separate task inputs. */
	struct vine_file *task_runner_arg_file;
	/* Vine file for TEMP/LOCAL outputs; NULL when the output is PFS-only. */
	struct vine_file *outfile;
	char *outfile_remote_name;
	size_t outfile_size_bytes;
	node_outfile_type_t outfile_type;
	/* Bytes currently credited to the executor graph's pfs_usage_bytes. */
	size_t pfs_credited_bytes;

	/* Graph relationships */
	struct list *parents;
	struct list *children;

	/* Execution and scheduling state */
	/* Parent edges not yet satisfied for scheduling. */
	int remaining_parents_count;
	/* Parent edges already counted for this child. */
	struct set *fired_parents;
	int completed;
	/* Return was released by cut; cleared if recovery recreates the output. */
	int cut;
	/* TEMP return was released by prune-depth; also cleared on recovery. */
	int prune_depth_pruned;
	int retry_attempts_left;
	int in_resubmit_queue;
	/* Time of the last failure that put this node on the retry queue. */
	timestamp_t last_failure_time;

	/* Structural metrics */
	int depth;
	int height;
	int upstream_subgraph_size;
	int downstream_subgraph_size;
	int fan_in;
	int fan_out;
	double heavy_score;

	/* Time metrics */
	timestamp_t critical_path_time;

	timestamp_t submission_time;
	timestamp_t scheduling_time;
	timestamp_t commit_time;
	timestamp_t execution_time;
	timestamp_t retrieval_time;
	timestamp_t postprocessing_time;
};

/** Create a new node.
@param node_id Unique node identifier supplied by the owning graph.
@return Newly allocated node instance.
*/
struct node *node_create(uint64_t node_id);

/** Create the task arguments for a node.
@param node Reference to the node.
@return The task arguments in JSON format: {"fn_args": [node_id], "fn_kwargs": {}}.
*/
char *node_construct_task_arguments(struct node *node);

/** Delete a node and release owned resources.
@param node Reference to the node.
*/
void node_delete(struct node *node);

/** Print information about a node.
@param node Reference to the node.
*/
void node_debug_print(struct node *node);

/** Update the critical path time of a node.
@param node Reference to the node.
@param execution_time Reference to the execution time of the node.
*/
void node_update_critical_path_time(struct node *node, timestamp_t execution_time);

#endif // NODE_H