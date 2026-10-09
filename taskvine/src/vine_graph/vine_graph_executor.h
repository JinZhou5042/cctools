#ifndef VINE_GRAPH_EXECUTOR_H
#define VINE_GRAPH_EXECUTOR_H

/* Execute one vine_graph run on a TaskVine Manager. The executor owns the graph, the run configuration, and every
 * Manager declaration of the run, and it is the only C interface exposed to Python. Every call that can fail returns
 * an error to the caller instead of terminating the process, so the Python run scope can always clean up. */

#include "vine_graph.h"

#include "priority_queue.h"

#include "taskvine.h"

/* Order in which ready nodes are submitted. */
typedef enum {
	TASK_PRIORITY_MODE_RANDOM = 0,			   /* Random order. */
	TASK_PRIORITY_MODE_DEPTH_FIRST,			   /* Deeper nodes first. */
	TASK_PRIORITY_MODE_BREADTH_FIRST,		   /* Shallower nodes first. */
	TASK_PRIORITY_MODE_FIFO,			   /* Earlier ready time first. */
	TASK_PRIORITY_MODE_LIFO,			   /* Later ready time first. */
	TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST,		   /* Largest total parent output first. */
	TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST /* Largest parent output times parent runtime first. */
} task_priority_mode_t;

/* Python receives an opaque handle. Internal fields remain available only to C. */
struct vine_graph_executor;
#ifndef SWIG
struct vine_graph_executor {
	struct vine_graph *graph;     // DAG executed by this executor
	struct vine_manager *manager; // TaskVine runtime

	char *output_dir;		 // directory for retrieved target files
	char *task_runner_library_name;	 // library that runs every node, installed by Python
	char *task_runner_function_name; // library function that runs one node

	struct itable *task_id_to_node;		 // maps vine task id to graph node after submit
	struct priority_queue *checkpoint_queue; // borrowed temporary outputs awaiting admission, longest producer first
	struct list *checkpoint_completed;	 // borrowed files whose checkpoint the Manager published
	struct list *checkpoint_failed;		 // borrowed files whose checkpoint failed or was preempted, to requeue
	struct set *checkpoint_pending;		 // queued or in-flight files; pruning withdraws them
	uint64_t checkpoint_sequence;		 // completion order used to break equal checkpoint priorities
	struct list *resubmit_queue;		 // nodes waiting for retry

	timestamp_t time_first_task_dispatched;	   // earliest dispatch time among user tasks
	timestamp_t time_last_task_retrieved;	   // latest user task retrieval time
	timestamp_t makespan_us;		   // workflow span in microseconds
	timestamp_t time_spent_on_cut_propagation; // time spent in cut propagation
	uint64_t completed_recovery_tasks;	   // recovery completions seen this run
	/** Sum of @c vine_graph_executor_submit_node preprocessing intervals across all nodes (microseconds). */
	uint64_t total_preprocessing_time_us;
	/** Sum of @c vine_graph_executor_run_completion_postprocess intervals across all completions (microseconds). */
	uint64_t total_postprocessing_time_us;

	task_priority_mode_t task_priority_mode; // schedule order before submit
	int prune_depth;			 // descendant levels that must be durable before release; zero disables
	double progress_bar_update_interval_sec;
};
#endif

/* Create an executor whose nodes run task_runner_function_name in the library task_runner_library_name.
 * Return NULL on failure. */
struct vine_graph_executor *vine_graph_executor_create(struct vine_manager *manager, const char *task_runner_library_name, const char *task_runner_function_name);
/* Cancel the run's tasks, release its Manager declarations, and delete the graph. */
void vine_graph_executor_delete(struct vine_graph_executor *e);

/* Build the graph. Calls that take ids return zero or a non-zero id on success and -1 or zero on failure. */
uint64_t vine_graph_executor_add_node(struct vine_graph_executor *e);
int vine_graph_executor_set_target(struct vine_graph_executor *e, uint64_t node_id);
int vine_graph_executor_add_dependency(struct vine_graph_executor *e, uint64_t parent_id, uint64_t child_id);
/* Place serialized edata in the data service vault for asynchronous delivery.
 * Ordinary frontend files keep their existing path. */
int vine_graph_executor_declare_input_file(struct vine_graph_executor *e, uint64_t file_id, const char *source_path, int vault);
int vine_graph_executor_add_task_output_file(struct vine_graph_executor *e, uint64_t task_id, uint64_t file_id, const char *task_path, int is_target);
int vine_graph_executor_add_task_input_file(struct vine_graph_executor *e, uint64_t task_id, uint64_t file_id, const char *task_path);
/* Validate the graph and declare result outputs after every node, edge, and target is added. Return -1 on a cycle
 * or a failed declaration. */
int vine_graph_executor_finalize(struct vine_graph_executor *e);

/* Query names assigned by the executor. */
const char *vine_graph_executor_get_node_outfile_remote_name(struct vine_graph_executor *e, uint64_t node_id);
const char *vine_graph_executor_get_file_target_path(struct vine_graph_executor *e, uint64_t file_id);

/* Apply one setting such as output-dir, prune-depth, or task-priority-mode. Return -1 for an unknown name or value. */
int vine_graph_executor_tune(struct vine_graph_executor *e, const char *name, const char *value);
/* Run the graph to completion. Return zero on success, or -1 when the run cannot continue, for example when a
 * Manager-local input is lost. */
int vine_graph_executor_execute(struct vine_graph_executor *e);
uint64_t vine_graph_executor_get_makespan_us(const struct vine_graph_executor *e);
uint64_t vine_graph_executor_get_completed_recovery_tasks(const struct vine_graph_executor *e);

#endif // VINE_GRAPH_EXECUTOR_H
