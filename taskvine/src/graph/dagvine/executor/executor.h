#ifndef DAGVINE_EXECUTOR_H
#define DAGVINE_EXECUTOR_H

#include <stdint.h>

#include "graph.h"
#include "itable.h"
#include "list.h"
#include "timestamp.h"
#include "vine_manager.h"

typedef enum {
	TASK_PRIORITY_MODE_RANDOM = 0,
	TASK_PRIORITY_MODE_DEPTH_FIRST,
	TASK_PRIORITY_MODE_BREADTH_FIRST,
	TASK_PRIORITY_MODE_FIFO,
	TASK_PRIORITY_MODE_LIFO,
	TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST,
	TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST
} task_priority_mode_t;

struct executor {
	/* Core ownership */
	struct graph *graph;          /* DAG model executed by this executor. */
	struct vine_manager *manager; /* TaskVine manager used for runtime operations. */

	/* Runtime indexes and queues */
	struct itable *task_id_to_node; /* Submitted TaskVine task id -> graph node. */
	struct list *resubmit_queue;    /* Nodes waiting for retry after a failed attempt. */

	/* Execution timing and counters */
	timestamp_t time_first_task_dispatched;   /* Earliest task dispatch timestamp. */
	timestamp_t time_last_task_retrieved;     /* Latest user task retrieval timestamp. */
	timestamp_t makespan_us;                  /* Current workflow makespan in microseconds. */
	timestamp_t time_spent_on_cut_propagation; /* Time spent releasing upstream data. */
	uint64_t completed_recovery_tasks;        /* Number of completed recovery tasks observed. */
	uint64_t pfs_usage_bytes;                 /* Bytes currently credited to shared FS outputs. */

	/* Runtime tuning */
	task_priority_mode_t task_priority_mode;    /* Priority policy used before task submission. */
	double failure_injection_step_percent;      /* Worker-release interval for failure injection. */
	double progress_bar_update_interval_sec;    /* Progress bar refresh interval. */
	int enable_debug_log;                       /* Whether executor debug logging remains enabled. */
	int max_retry_attempts;                     /* Retry budget assigned to newly created nodes. */
};

struct executor *executor_create(struct vine_manager *manager, struct graph *graph);
struct graph *executor_graph_create(struct vine_manager *manager);
void executor_delete(struct executor *e);
uint64_t executor_add_node(struct executor *e);
void executor_finalize(struct executor *e);
void executor_add_task_input(struct executor *e, uint64_t task_id, const char *filename);
void executor_add_task_output(struct executor *e, uint64_t task_id, const char *filename);
int executor_tune(struct executor *e, const char *name, const char *value);
void executor_execute(struct executor *e);
uint64_t executor_get_makespan_us(const struct executor *e);
uint64_t executor_get_total_recovery_tasks(const struct executor *e);
uint64_t executor_get_completed_recovery_tasks(const struct executor *e);

#endif // DAGVINE_EXECUTOR_H
