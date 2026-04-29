#ifndef EXECUTOR_GRAPH_H
#define EXECUTOR_GRAPH_H

#include <stdint.h>

#include "vine_task.h"
#include "hash_table.h"
#include "itable.h"
#include "list.h"
#include "vine_manager.h"
#include "set.h"
#include "node.h"
#include "taskvine.h"
#include "timestamp.h"

/** The task priority algorithm used for executor graph scheduling. */
typedef enum {
	TASK_PRIORITY_MODE_RANDOM = 0,			   /**< Assign random priority to tasks */
	TASK_PRIORITY_MODE_DEPTH_FIRST,			   /**< Prioritize deeper tasks first */
	TASK_PRIORITY_MODE_BREADTH_FIRST,		   /**< Prioritize shallower tasks first */
	TASK_PRIORITY_MODE_FIFO,			   /**< First in, first out priority */
	TASK_PRIORITY_MODE_LIFO,			   /**< Last in, first out priority */
	TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST,		   /**< Prioritize tasks with larger inputs first */
	TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST /**< Prioritize tasks with larger storage footprint first */
} task_priority_mode_t;

/** The executor graph (logical scheduling layer). */
struct executor_graph {
	struct vine_manager *manager;
	struct itable *nodes;
	struct itable *task_id_to_node;
	struct hash_table *outfile_cachename_to_node;
	/* Maps a logical in/out filename (remote_name) to a stable cached_name. */
	struct hash_table *inout_filename_to_cached_name;

	/* Unsuccessful tasks are appended to this list to be resubmitted later. */
	struct list *resubmit_queue;

	/* The directory to store the checkpointed results.
	 * Only intermediate results can be checkpointed, the fraction of intermediate results to checkpoint is controlled by the checkpoint-fraction parameter. */
	char *checkpoint_dir;

	/* Results of target nodes will be stored in this directory.
	 * This dir path can not necessarily be a shared file system directory,
	 * output files will be retrieved through the network instead,
	 * as long as the manager can access it. */
	char *output_dir;

	/* Name of the generated proxy library used to run Python work on workers. */
	char *proxy_library_name;

	/* Entry point inside that library. */
	char *proxy_function_name;

	double checkpoint_fraction; /* 0 - 1, the fraction of intermediate results to checkpoint */

	/* TEMP-only early release knob.
	 * 0 disables it. 1 means children only. Larger values wait for more
	 * descendants to complete, so they are more conservative.
	 * This is weaker than cut and may require later recovery to recompute. */
	int prune_depth;

	/* Time spent in cut propagation, in microseconds. */
	timestamp_t time_spent_on_cut_propagation;

	/* Bytes currently live on the shared file system. */
	uint64_t pfs_usage_bytes;

	task_priority_mode_t task_priority_mode; /* priority mode for task graph task scheduling */
	double failure_injection_step_percent;	 /* 0 - 100, the percentage of steps to inject failure */

	double progress_bar_update_interval_sec; /* update interval for the progress bar in seconds */

	/* The filename of the csv file to store the time metrics of the executor graph. */
	char *time_metrics_filename;

	int enable_debug_log;	 /* whether to enable debug log */
	int print_graph_details; /* whether to print the graph details */

	int max_retry_attempts;	   /* the maximum number of times to retry a task */
	double retry_interval_sec; /* the interval between retries in seconds, 0 means no retry interval */

	timestamp_t time_first_task_dispatched; /* the time when the first task is dispatched */
	timestamp_t time_last_task_retrieved;	/* the time when the last task is retrieved */
	timestamp_t makespan_us;		/* the makespan of the executor graph in microseconds */
	uint64_t completed_recovery_tasks;	/* recovery tasks seen complete in executor_graph_execute */
};

/* Public APIs for operating the executor graph */

/** Create an executor graph and return it.
@param q Reference to the current manager object.
@return A new executor graph.
*/
struct executor_graph *executor_graph_create(struct vine_manager *q);

/** Create a new node in the executor graph.
@param eg Reference to the executor graph.
@return The auto-assigned node id.
*/
uint64_t executor_graph_add_node(struct executor_graph *eg);

/** Mark a node as a retrieval target.
@param eg Reference to the executor graph.
@param node_id Identifier of the node to mark as target.
*/
void executor_graph_set_target(struct executor_graph *eg, uint64_t node_id);

/** Add a dependency between two nodes in the executor graph.
@param eg Reference to the executor graph.
@param parent_id Identifier of the parent node.
@param child_id Identifier of the child node.
*/
void executor_graph_add_dependency(struct executor_graph *eg, uint64_t parent_id, uint64_t child_id);

/** Finalize the metrics of the executor graph.
@param eg Reference to the executor graph.
*/
void executor_graph_finalize(struct executor_graph *eg);

/** Get the heavy score of a node in the executor graph.
@param eg Reference to the executor graph.
@param node_id Identifier of the node.
@return The heavy score.
*/
double executor_graph_get_node_heavy_score(const struct executor_graph *eg, uint64_t node_id);

/** Execute the task graph.
@param eg Reference to the executor graph.
*/
void executor_graph_execute(struct executor_graph *eg);

/** Get the outfile remote name of a node in the executor graph.
@param eg Reference to the executor graph.
@param node_id Identifier of the node.
@return The outfile remote name.
*/
const char *executor_graph_get_node_outfile_remote_name(const struct executor_graph *eg, uint64_t node_id);

/** Get the local outfile source of a node in the executor graph.
@param eg Reference to the executor graph.
@param node_id Identifier of the node.
@return The local outfile source, or NULL if the node does not produce a local file.
*/
const char *executor_graph_get_node_local_outfile_source(const struct executor_graph *eg, uint64_t node_id);

/** Delete an executor graph.
@param eg Reference to the executor graph.
*/
void executor_graph_delete(struct executor_graph *eg);

/** Get the proxy library name of the executor graph.
@param eg Reference to the executor graph.
@return The proxy library name.
*/
const char *executor_graph_get_proxy_library_name(const struct executor_graph *eg);

/** Add an input file to a task. The input file will be declared as a temp file.
@param eg Reference to the executor graph.
@param task_id Identifier of the task.
@param filename Reference to the filename.
*/
void executor_graph_add_task_input(struct executor_graph *eg, uint64_t task_id, const char *filename);

/** Add an output file to a task. The output file will be declared as a temp file.
@param eg Reference to the executor graph.
@param task_id Identifier of the task.
@param filename Reference to the filename.
*/
void executor_graph_add_task_output(struct executor_graph *eg, uint64_t task_id, const char *filename);

/** Set the proxy function name of the executor graph.
@param eg Reference to the executor graph.
@param proxy_function_name Reference to the proxy function name.
*/
void executor_graph_set_proxy_function_name(struct executor_graph *eg, const char *proxy_function_name);

/** Tune the executor graph.
@param eg Reference to the executor graph.
@param name Reference to the name of the parameter to tune.
@param value Reference to the value of the parameter to tune.
@return 0 on success, -1 on failure.
*/
int executor_graph_tune(struct executor_graph *eg, const char *name, const char *value);

/** Get the makespan of the executor graph in microseconds.
@param eg Reference to the executor graph.
@return The makespan in microseconds.
*/
uint64_t executor_graph_get_makespan_us(const struct executor_graph *eg);

/** Get the total number of recovery tasks submitted by the manager.
@param eg Reference to the executor graph.
@return Total submitted recovery tasks.
*/
uint64_t executor_graph_get_total_recovery_tasks(const struct executor_graph *eg);

/** Get the number of recovery tasks completed while executing this graph.
@param eg Reference to the executor graph.
@return Completed recovery tasks.
*/
uint64_t executor_graph_get_completed_recovery_tasks(const struct executor_graph *eg);

#endif // EXECUTOR_GRAPH_H
