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
	struct graph *graph;
	struct vine_manager *manager;
	struct itable *task_id_to_node;
	struct list *resubmit_queue;
	timestamp_t time_first_task_dispatched;
	timestamp_t time_last_task_retrieved;
	timestamp_t makespan_us;
	uint64_t completed_recovery_tasks;
	timestamp_t time_spent_on_cut_propagation;
	uint64_t pfs_usage_bytes;
	task_priority_mode_t task_priority_mode;
	double failure_injection_step_percent;
	double progress_bar_update_interval_sec;
	char *time_metrics_filename;
	int enable_debug_log;
	int max_retry_attempts;
	double retry_interval_sec;
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
