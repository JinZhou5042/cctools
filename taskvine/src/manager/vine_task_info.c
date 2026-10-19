/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_task_info.h"
#include "vine_manager.h"

#include "macros.h"

#include <math.h>

// The default tasks capacity reported before information is available.
// Default capacity also implies 1 core, 1024 MB of disk and 512 memory per task.
#define VINE_DEFAULT_CAPACITY_TASKS 10

// The minimum number of task reports to keep
#define VINE_TASK_INFO_MIN_SIZE 50

struct vine_task_info *vine_task_info_create(struct vine_task *t)
{
	struct vine_task_info *ti = calloc(1, sizeof(*ti));

	ti->transfer_time = (t->time_when_commit_end - t->time_when_commit_start) + (t->time_when_done - t->time_when_retrieval);
	ti->exec_time = t->time_workers_execute_last;
	ti->manager_time = (((t->time_when_done - t->time_when_commit_start) - ti->transfer_time) - ti->exec_time);
	ti->resources = rmsummary_copy(t->resources_allocated, 0);

	return ti;
}

void vine_task_info_delete(struct vine_task_info *ti)
{
	rmsummary_delete(ti->resources);
	free(ti);
}

/* Add a report to the sums of task_info_list (sign 1) or remove it (sign -1). The times are unsigned, so their sums
 * wrap exactly as a pass over the list does, and the resources are whole numbers, which doubles add exactly. */
static void task_info_sums_update(struct vine_manager *q, const struct vine_task_info *ti, int sign)
{
	if (sign > 0) {
		q->task_info_sums.transfer_time += ti->transfer_time;
		q->task_info_sums.exec_time += ti->exec_time;
		q->task_info_sums.manager_time += ti->manager_time;
	} else {
		q->task_info_sums.transfer_time -= ti->transfer_time;
		q->task_info_sums.exec_time -= ti->exec_time;
		q->task_info_sums.manager_time -= ti->manager_time;
	}
	if (ti->resources) {
		q->task_info_sums.cores += sign * ti->resources->cores;
		q->task_info_sums.memory += sign * ti->resources->memory;
		q->task_info_sums.disk += sign * ti->resources->disk;
		q->task_info_sums.gpus += sign * ti->resources->gpus;
	}
}

void vine_task_info_add(struct vine_manager *q, struct vine_task *t)
{
	if (!t->resources_allocated) {
		return;
	}

	/* Each report advances the weighted capacity, which needs only the stored reports, not the worker totals that
	 * vine_get_stats() also computes. */
	vine_task_info_compute_capacity(q, q->stats);

	struct vine_task_info *ti = vine_task_info_create(t);

	list_push_tail(q->task_info_list, ti);
	task_info_sums_update(q, ti, 1);

	// Trim the list, but never below its previous size. The bound belongs to the manager, because several managers
	// may run in threads of one process.
	q->task_info_sums.limit = MAX(MAX(q->task_info_sums.limit, VINE_TASK_INFO_MIN_SIZE), 2 * q->stats->tasks_on_workers);

	while (list_size(q->task_info_list) >= q->task_info_sums.limit) {
		ti = list_pop_head(q->task_info_list);
		task_info_sums_update(q, ti, -1);
		vine_task_info_delete(ti);
	}
}

/*
Compute queue capacity based on stored task reports
and the summary of manager activity.
*/

void vine_task_info_compute_capacity(const struct vine_manager *q, struct vine_stats *s)
{
	struct vine_task_info *capacity = calloc(1, sizeof(*capacity));
	capacity->resources = rmsummary_create(0);

	struct vine_task_info *ti;
	double alpha = 0.05;
	int count = list_size(q->task_info_list);
	int capacity_instantaneous = 0;

	// Compute the average task properties.
	if (count < 1) {
		capacity->resources->cores = 1;
		capacity->resources->memory = 512;
		capacity->resources->disk = 1024;
		capacity->resources->gpus = 0;

		capacity->exec_time = VINE_DEFAULT_CAPACITY_TASKS;
		capacity->transfer_time = 1;

		q->stats->capacity_weighted = VINE_DEFAULT_CAPACITY_TASKS;
		capacity_instantaneous = VINE_DEFAULT_CAPACITY_TASKS;

		count = 1;
	} else {
		// The sums of the task reports available.
		capacity->transfer_time = q->task_info_sums.transfer_time;
		capacity->exec_time = q->task_info_sums.exec_time;
		capacity->manager_time = q->task_info_sums.manager_time;
		capacity->resources->cores = q->task_info_sums.cores;
		capacity->resources->memory = q->task_info_sums.memory;
		capacity->resources->disk = q->task_info_sums.disk;
		capacity->resources->gpus = q->task_info_sums.gpus;

		ti = list_peek_tail(q->task_info_list);
		if (ti->transfer_time > 0) {
			capacity_instantaneous = DIV_INT_ROUND_UP(ti->exec_time, (ti->transfer_time + ti->manager_time));
			q->stats->capacity_weighted = (int)ceil((alpha * (float)capacity_instantaneous) + ((1.0 - alpha) * q->stats->capacity_weighted));
			time_t ts;
			time(&ts);
		}
	}

	capacity->transfer_time = MAX(1, capacity->transfer_time);
	capacity->exec_time = MAX(1, capacity->exec_time);
	capacity->manager_time = MAX(1, capacity->manager_time);

	// Never go below the default capacity
	int64_t ratio = MAX(VINE_DEFAULT_CAPACITY_TASKS, DIV_INT_ROUND_UP(capacity->exec_time, (capacity->transfer_time + capacity->manager_time)));

	q->stats->capacity_tasks = ratio;
	q->stats->capacity_cores = DIV_INT_ROUND_UP(capacity->resources->cores * ratio, count);
	q->stats->capacity_memory = DIV_INT_ROUND_UP(capacity->resources->memory * ratio, count);
	q->stats->capacity_disk = DIV_INT_ROUND_UP(capacity->resources->disk * ratio, count);
	q->stats->capacity_gpus = DIV_INT_ROUND_UP(capacity->resources->gpus * ratio, count);
	q->stats->capacity_instantaneous = DIV_INT_ROUND_UP(capacity_instantaneous, 1);

	vine_task_info_delete(capacity);
}
