/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_schedule.h"
#include "vine_blocklist.h"
#include "vine_factory_info.h"
#include "vine_file.h"
#include "vine_file_replica.h"
#include "vine_file_replica_table.h"
#include "vine_mount.h"
#include "vine_temp.h"

#include "debug.h"
#include "hash_table.h"
#include "list.h"
#include "priority_queue.h"
#include "skip_list.h"
#include "macros.h"
#include "random.h"
#include "rmonitor_types.h"
#include "rmsummary.h"
#include "set.h"

#include <limits.h>
#include <math.h>

/* check whether worker has all fixed locations required for task */
int check_fixed_location_worker(struct vine_manager *m, struct vine_worker_info *w, struct vine_task *t)
{
	int all_present = 1;
	struct vine_mount *mt;
	struct vine_file_replica *replica;

	if (t->has_fixed_locations) {
		LIST_ITERATE(t->input_mounts, mt)
		{
			if (mt->flags & VINE_FIXED_LOCATION) {
				replica = hash_table_lookup(w->current_files, mt->file->cached_name);
				if (!replica) {
					all_present = 0;
					break;
				}
			}
		}
	}

	return all_present;
}

/* Check if queue has entered ramp_down mode (more workers than waiting tasks).
 * @param q The manager structure.
 * @return 1 if in ramp down mode, 0 otherwise.
 */

int vine_schedule_in_ramp_down(struct vine_manager *q)
{
	if (!(q->monitor_mode & VINE_MON_WATCHDOG)) {
		/* if monitoring is not terminating tasks because of resources, ramp down heuristic does not have any
		 * effect. */
		return 0;
	}

	if (!q->ramp_down_heuristic) {
		return 0;
	}

	if (hash_table_size(q->worker_table) > skip_list_size(q->ready_tasks)) {
		return 1;
	}

	return 0;
}

/* Check if worker resources are enough to run the task.
 * Note that empty libraries are not *real* tasks and can be
 * killed as needed to reclaim unused resources and
 * make space for other libraries or tasks.
 * @param q     Manager info structure.
 * @param w     Worker info structure.
 * @param t     Task info structure.
 * @param tr    Chosen task resources.
 * @return 1 if yes, 0 otherwise. */
int check_worker_have_enough_resources(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct rmsummary *tr)
{
	/* Skip if it is a function task. Resource guarantees for function calls are handled at the end of @check_worker_against_task, which calls
	 * @vine_schedule.c:vine_schedule_find_library to locate a suitable library for the function call, it directly returns false if no appropriate library is found  */
	if (t->needs_library) {
		return 1;
	}

	struct vine_resources *worker_net_resources = vine_resources_copy(w->resources);

	/* Subtract resources from libraries that are not running any functions at all.
	 * This matches the assumption in @vine_manager.c:commit_task_to_worker(), where empty libraries are being killed right before a task is committed. */
	uint64_t task_id;
	struct vine_task *ti;
	int iteration;
	ITABLE_ITERATE(w->current_tasks, iteration, task_id, ti)
	{
		if (ti->state == VINE_TASK_RUNNING && ti->provides_library && ti->function_slots_inuse == 0) {
			worker_net_resources->disk.inuse -= ti->current_resource_box->disk;
			worker_net_resources->cores.inuse -= ti->current_resource_box->cores;
			worker_net_resources->memory.inuse -= ti->current_resource_box->memory;
			worker_net_resources->gpus.inuse -= ti->current_resource_box->gpus;
		}
	}

	int ok = 1;
	if (worker_net_resources->disk.inuse + tr->disk > worker_net_resources->disk.total) { /* No overcommit disk */
		ok = 0;
	}

	if ((tr->cores > worker_net_resources->cores.total) ||
			(worker_net_resources->cores.inuse + tr->cores > overcommitted_resource_total(q, worker_net_resources->cores.total))) {
		ok = 0;
	}

	if ((tr->memory > worker_net_resources->memory.total) ||
			(worker_net_resources->memory.inuse + tr->memory > overcommitted_resource_total(q, worker_net_resources->memory.total))) {
		ok = 0;
	}

	if ((tr->gpus > worker_net_resources->gpus.total) || (worker_net_resources->gpus.inuse + tr->gpus > overcommitted_resource_total(q, worker_net_resources->gpus.total))) {
		ok = 0;
	}
	vine_resources_delete(worker_net_resources);
	return ok;
}

/* t->disk only specifies the size of output and ephemeral files. Here we check if the task would fit together with all its input files
 * taking into account that some files may be already at the worker. */
int check_worker_have_enough_disk_with_inputs(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	double available = w->resources->disk.total - MAX(0, t->resources_requested->disk) - w->resources->disk.inuse;

	int64_t missing = 0;
	struct vine_mount *m;
	LIST_ITERATE(t->input_mounts, m)
	{
		if (!hash_table_lookup(w->current_files, m->file->cached_name)) {
			missing += m->file->size;
		}
	}

	/* available is in MB, file sizes are in bytes. Rounding the sum once keeps many small inputs from counting as a
	 * megabyte each. */
	return BYTES_TO_MEGABYTES(missing) <= available;
}

/* Check if this worker has committable resources for any type of task.
 * If it returns false, neither a function task, library task nor a regular task can run on this worker.
 * If it returns true, the worker has either free slots for function calls or sufficient resources for regular tasks.
 * @param q         Manager info structure
 * @param w The worker info structure.
 * @param count_gpus Whether free gpus count as resources for a regular task.
 */
static int check_worker_have_committable_resources(struct vine_manager *q, struct vine_worker_info *w, int count_gpus)
{
	/* Check if there are free slots on any of the running libraries */
	if (w->current_libraries && itable_size(w->current_libraries) > 0) {
		uint64_t task_id;
		struct vine_task *t;
		int iteration;
		ITABLE_ITERATE(w->current_libraries, iteration, task_id, t)
		{
			if (t->function_slots_inuse < t->function_slots_total) {
				return 1;
			}
		}
	}

	/* Check if there are free resources for tasks except function calls */
	int cores_committable = w->resources->cores.total > 0 && (w->resources->cores.inuse < overcommitted_resource_total(q, w->resources->cores.total));
	int gpus_committable = count_gpus && w->resources->gpus.total > 0 && (w->resources->gpus.inuse < overcommitted_resource_total(q, w->resources->gpus.total));
	int memory_committable = w->resources->memory.total > 0 && (w->resources->memory.inuse < overcommitted_resource_total(q, w->resources->memory.total));
	int disk_committable = w->resources->disk.total > 0 && (w->resources->disk.inuse < overcommitted_resource_total(q, w->resources->disk.total));

	/* A regular task has to use both memory and disk */
	if (memory_committable && disk_committable) {
		/* A regular task can use either cores or gpus */
		if (cores_committable || gpus_committable) {
			return 1;
		}
	}

	/* If reach here, no free slots for function calls, and no committable resources for other tasks. */
	return 0;
}

/* Check the conditions under which a worker accepts no task at all, apart from its free resources. */
static int check_worker_is_available(struct vine_manager *q, struct vine_worker_info *w)
{
	/* worker has not reported any resources yet */
	if (w->resources->tag < 0 || w->resources->workers.total < 1 || w->end_time < 0) {
		return 0;
	}

	/* Don't send tasks to this worker if it is in draining mode (no more tasks), or it is going to be removed. */
	/* Probably we don't need to check pending_removal here as the worker shouldn't be in worker's table
	 * anyway, but just in case that changes in the future. */
	if (w->draining || w->pending_removal) {
		return 0;
	}

	// if worker's end time has not been received
	if (w->end_time < 0) {
		return 0;
	}

	/* Don't send tasks if a task recently failed at this worker. */
	if (w->last_failure_time + q->transient_error_interval > timestamp_get()) {
		return 0;
	}

	/* Don't send tasks if the factory is used and has too many connected workers. */
	if (w->factory_name) {
		struct vine_factory_info *f = vine_factory_info_lookup(q, w->factory_name);
		if (f && f->connected_workers > f->max_workers) {
			return 0;
		}
	}

	/* Check if worker is blocked from the manager. */
	if (vine_blocklist_is_blocked(q, w->hostname)) {
		return 0;
	}

	return 1;
}

/* Return true if this worker has room for a task now. */
static int check_worker_accepts_tasks(struct vine_manager *q, struct vine_worker_info *w)
{
	return w->type == VINE_WORKER_TYPE_WORKER && check_worker_is_available(q, w) && check_worker_have_committable_resources(q, w, q->gpus_requested);
}

/* Return true if some worker has room for a task now. Otherwise no ready task can be dispatched, and the manager does
 * not need to try each of them against each worker. Room needs a free function slot or a free core, unless idle GPUs
 * count. */
int vine_schedule_any_worker_accepts_tasks(struct vine_manager *q)
{
	char *key;
	struct vine_worker_info *w;
	int iteration;
	HASH_TABLE_ITERATE(q->worker_table, iteration, key, w)
	{
		if (check_worker_accepts_tasks(q, w)) {
			return 1;
		}
	}
	return 0;
}

/* Return true if the input exists but cannot be transferred to a worker that lacks it now, so only the workers that
 * hold it can run a task that reads it. A temporary file that exists nowhere is not such an input, because a full
 * check of the task must start its recovery. */
static int input_held_without_source(struct vine_manager *q, struct vine_file *f)
{
	if (vine_manager_transfer_source_available(q, f)) {
		return 0;
	}
	return f->type != VINE_TEMP || vine_temp_exists_somewhere(q, f);
}

/* Return true if no worker that holds file f can take a task now: none has room, or, when other is given, none also
 * holds other. */
static int input_holders_cannot_run(struct vine_manager *q, struct vine_file *f, struct vine_file *other)
{
	struct set *holders = hash_table_lookup(q->file_worker_table, f->cached_name);
	if (!holders) {
		return 1;
	}
	struct vine_worker_info *w;
	int iteration;
	SET_ITERATE(holders, iteration, w)
	{
		if (other ? hash_table_lookup(w->current_files, other->cached_name) != NULL : check_worker_accepts_tasks(q, w)) {
			return 0;
		}
	}
	return 1;
}

/* Return true if the inputs that kept this ready task from every worker at its last scheduling attempt still keep it
 * from every worker. An input without a transfer source can reach no worker that lacks it, so the task cannot run
 * when no holder of such an input has room, or when no worker holds both recorded inputs. The check costs one lookup
 * per holder of the recorded inputs instead of a pass over all inputs and workers. */
static int blocking_inputs_still_block(struct vine_manager *q, struct vine_task *t)
{
	struct vine_file *first = t->blocking_inputs[0];
	struct vine_file *second = t->blocking_inputs[1];

	int first_held = input_held_without_source(q, first);
	if (first_held && input_holders_cannot_run(q, first, NULL)) {
		return 1;
	}
	int second_held = second && input_held_without_source(q, second);
	if (second_held && input_holders_cannot_run(q, second, NULL)) {
		return 1;
	}
	return first_held && second_held && input_holders_cannot_run(q, first, second);
}

int vine_schedule_task_is_blocked(struct vine_manager *q, struct vine_task *t)
{
	if (!t->blocking_inputs[0] || !q->peer_transfers_enabled) {
		return 0;
	}
	return blocking_inputs_still_block(q, t);
}

/* Return true if a worker runs a task other than its libraries. */
static int worker_runs_tasks(struct vine_worker_info *w)
{
	return itable_size(w->current_tasks) > itable_size(w->current_libraries);
}

/* Return true if worker w is reserved for a quarantined task other than t. A reservation whose task is no longer ready,
 * or reserved another worker meanwhile, is dropped here. */
static int worker_reserved_for_other(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	if (!w->quarantine_reserved_id || w->quarantine_reserved_id == t->task_id) {
		return 0;
	}
	struct vine_task *reserved = itable_lookup(q->tasks, w->quarantine_reserved_id);
	if (reserved && reserved->state == VINE_TASK_READY && reserved->quarantine_worker == w) {
		return 1;
	}
	w->quarantine_reserved_id = 0;
	return 0;
}

/* Return true if worker w may take task t under the quarantine of tasks charged a lost worker. */
static int check_worker_quarantine(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	if (t->type == VINE_TASK_TYPE_LIBRARY) {
		return 1;
	}
	if (w->quarantine_task_id || worker_reserved_for_other(q, w, t)) {
		return 0;
	}
	if (t->worker_loss_charged && worker_runs_tasks(w)) {
		return 0;
	}
	return 1;
}

/* Check if this task is compatible with this given worker by considering
 * resources availability, features, blocklist, and all other relevant factors.
 * Used by all scheduling methods for basic compatibility.
 * @param q The manager structure.
 * @param w The worker info structure.
 * @param t The task structure.
 * @return 0 if the task is not compatible with the worker, 1 otherwise.
 */

int check_worker_against_task(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	/* THIS FUNCTION SHOULD NOT MODIFY t IN ANY WAY. */
	/* Otherwise library templates are modified during the run. */

	if (!check_worker_is_available(q, w)) {
		return 0;
	}

	if (!check_worker_quarantine(q, w, t)) {
		return 0;
	}

	/* if worker has free resources to use */
	if (!check_worker_have_committable_resources(q, w, 1)) {
		return 0;
	}

	/* Compute the resources to allocate to this task. */
	struct rmsummary *l = vine_manager_choose_resources_for_task(q, w, t);

	if (!check_worker_have_enough_resources(q, w, t, l)) {
		rmsummary_delete(l);
		return 0;
	}
	rmsummary_delete(l);

	// if wall time for worker is specified and there's not enough time for task, then not ok
	if (w->end_time > 0) {
		double current_time = ((double)timestamp_get()) / ONE_SECOND;
		if (t->resources_requested->end > 0 && w->end_time < t->resources_requested->end) {
			return 0;
		}
		if (t->min_running_time > 0 && w->end_time - current_time < t->min_running_time) {
			return 0;
		}
	}

	if (!check_worker_have_enough_disk_with_inputs(q, w, t)) {
		return 0;
	}

	/* If the worker is not the one the task wants. */
	if (t->has_fixed_locations && !check_fixed_location_worker(q, w, t)) {
		return 0;
	}

	/* If the worker has transfer capacity to get this task. */
	if (q->peer_transfers_enabled && !vine_manager_transfer_capacity_available(q, w, t)) {
		return 0;
	}

	/* If the worker doesn't have the features the task requires. */
	if (t->feature_list) {
		if (!w->features) {
			return 0;
		} else {
			char *feature;
			LIST_ITERATE(t->feature_list, feature)
			{
				if (!hash_table_lookup(w->features, feature)) {
					return 0;
				}
			}
		}
	}

	/* Finally check to see if a function task has the needed library task */

	if (t->needs_library) {
		struct vine_task *library = vine_schedule_find_library(q, w, t->needs_library);
		if (library) {
			/* The worker already has the library with a free slot. */
		} else {
			library = vine_manager_find_library_template(q, t->needs_library);
			if (library) {
				if (check_worker_against_task(q, w, library)) {
					/* The library would fit this worker if it was sent. */
				} else {
					/* The library would not fit the worker. */
					return 0;
				}
			} else {
				/* There is no library by that name, yikes! */
				return 0;
			}
		}
	}
	return 1;
}

/* Find a library task running on a specific worker that has an available slot.
 * @return pointer to the library task if there's one, 0 otherwise. */

struct vine_task *vine_schedule_find_library(struct vine_manager *q, struct vine_worker_info *w, const char *library_name)
{
	uint64_t task_id;
	struct vine_task *library_task;
	int iteration;
	ITABLE_ITERATE(w->current_libraries, iteration, task_id, library_task)
	{
		if (!strcmp(library_task->provides_library, library_name) && (library_task->function_slots_inuse < library_task->function_slots_total)) {
			return library_task;
		}
	}

	return 0;
}

/* Count the number of free cores on a worker. */

static int count_worker_free_cores(struct vine_manager *q, struct vine_worker_info *w)
{
	int free_cores = 0;

	/* library tasks may themselves consume many cores but can have free slots */
	uint64_t task_id;
	struct vine_task *t;
	int iteration;
	ITABLE_ITERATE(w->current_libraries, iteration, task_id, t)
	{
		free_cores += t->function_slots_total - t->function_slots_inuse;
	}

	/* count the free cores on the worker */
	free_cores += overcommitted_resource_total(q, w->resources->cores.total) - w->resources->cores.inuse;

	return free_cores;
}

/* Drop the reservation of a quarantined task, if any. */
void vine_schedule_quarantine_release(struct vine_task *t)
{
	struct vine_worker_info *w = t->quarantine_worker;
	if (w && w->quarantine_reserved_id == t->task_id) {
		w->quarantine_reserved_id = 0;
	}
	t->quarantine_worker = NULL;
}

/* Reserve for a quarantined task that found no free worker the available worker with the most free cores that is not
 * reserved and runs no quarantined task. It takes no new task until it runs nothing. */
static void quarantine_reserve(struct vine_manager *q, struct vine_task *t)
{
	struct vine_worker_info *best = NULL;
	int best_free_cores = 0;

	char *key;
	struct vine_worker_info *w;
	int iteration;
	HASH_TABLE_ITERATE(q->worker_table, iteration, key, w)
	{
		if (!w || !w->resources || w->type != VINE_WORKER_TYPE_WORKER || w->quarantine_task_id) {
			continue;
		}
		if (worker_reserved_for_other(q, w, t) || !check_worker_is_available(q, w)) {
			continue;
		}
		int free_cores = count_worker_free_cores(q, w);
		if (!best || free_cores > best_free_cores) {
			best = w;
			best_free_cores = free_cores;
		}
	}

	if (best) {
		best->quarantine_reserved_id = t->task_id;
		t->quarantine_worker = best;
		debug(D_VINE, "task %d was charged a lost worker and waits for %s to run nothing else", t->task_id, best->addrport);
	}
}

void vine_schedule_quarantine_commit(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	if (t->type == VINE_TASK_TYPE_LIBRARY || !t->worker_loss_charged) {
		return;
	}
	vine_schedule_quarantine_release(t);
	w->quarantine_task_id = t->task_id;
}

void vine_schedule_quarantine_reap(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t)
{
	if (w->quarantine_task_id == t->task_id) {
		w->quarantine_task_id = 0;
	}
}

void vine_schedule_quarantine_remove_worker(struct vine_manager *q, struct vine_worker_info *w)
{
	if (w->quarantine_reserved_id) {
		struct vine_task *t = itable_lookup(q->tasks, w->quarantine_reserved_id);
		if (t && t->quarantine_worker == w) {
			t->quarantine_worker = NULL;
		}
		w->quarantine_reserved_id = 0;
	}
}

/* Rank worker w for task t under the selection algorithm a, unless it cannot take the task. */
static void rank_worker_for_task(struct vine_manager *q, struct vine_task *t, struct vine_worker_info *w, int a, struct priority_queue *workers)
{
	/* briefly skip uninitialized workers, more detailed checks are performed in @check_worker_against_task */
	if (!w || !w->resources || w->type != VINE_WORKER_TYPE_WORKER || w->draining) {
		return;
	}

	/* if task groups are enabled, skip workers that are running a group task */
	if (q->task_groups_enabled) {
		if (w->tasks_committed > 0) {
			int iteration_group;
			uint64_t tidg;
			struct vine_task *tg;
			int running_group_task = 0;
			ITABLE_ITERATE(w->current_tasks, iteration_group, tidg, tg)
			{
				if (tg->group_id) {
					running_group_task = 1;
					break;
				}
			}
			if (running_group_task) {
				return;
			}
		}
	}

	/* compute the size of cached and uncached input files on the worker */
	int64_t uncached_input_size = 0;
	int64_t cached_input_size = 0;
	struct vine_mount *m;
	LIST_ITERATE(t->input_mounts, m)
	{
		if (!m || !m->file) {
			continue;
		}

		struct vine_file_replica *replica = vine_file_replica_table_lookup(w, m->file->cached_name);
		if (replica) {
			cached_input_size += m->file->size;
		} else {
			uncached_input_size += m->file->size;
		}
	}

	int64_t available_cache_space_after_task_dispatch = MEGABYTES_TO_BYTES(w->resources->disk.total) - (w->inuse_cache + uncached_input_size);

	/* skip this worker if the available cache space drops below 0 after the task is dispatched */
	if (available_cache_space_after_task_dispatch <= 0) {
		return;
	}

	double priority = 0;

	switch (a) {
	case VINE_SCHEDULE_FILES:
		/* Find the worker that has the largest quantity of cached data needed by this task,
		 * so as to minimize transfer work that must be done by the manager. */
		priority = cached_input_size;
		break;
	case VINE_SCHEDULE_DISK:
		/* Find the worker that will be left with the most disk space if the task is finished there */
		priority = available_cache_space_after_task_dispatch;
		break;
	case VINE_SCHEDULE_WORST:
		/* Find the worker that is the "worst fit" for this task, meaning the worker with the most free cores.
		 * We don't check on memory or disk because if there are no free cores, then the task will not fit anyway. */
		priority = count_worker_free_cores(q, w);
		break;
	case VINE_SCHEDULE_TIME:
		/* Find the worker that produced the fastest runtime of prior tasks. */
		priority = w->total_tasks_complete == 0 ? HUGE_VAL : -(double)(w->total_task_time + w->total_transfer_time) / w->total_tasks_complete;
		break;
	case VINE_SCHEDULE_FCFS:
		/* Deprecated, same as random */
	case VINE_SCHEDULE_RAND:
	default:
		/* Default to random selection. */
		priority = random_double();
		break;
	}

	priority_queue_push(workers, w, priority);
}

/* Return true if some worker in holders also holds file f. */
static int holders_hold_file(struct set *holders, struct vine_file *f)
{
	struct vine_worker_info *w;
	int iteration;
	SET_ITERATE(holders, iteration, w)
	{
		if (hash_table_lookup(w->current_files, f->cached_name)) {
			return 1;
		}
	}
	return 0;
}

/* Select the best worker for this task, based on the current scheduling mode. */

struct vine_worker_info *vine_schedule_task_to_worker(struct vine_manager *q, struct vine_task *t)
{
	if (!q || !t) {
		return NULL;
	}

	t->blocking_inputs[0] = NULL;
	t->blocking_inputs[1] = NULL;

	/* A quarantined task with a reservation takes its worker once that worker runs nothing, or any other worker that
	 * runs nothing first, so a long task on the reserved worker does not hold it back while other workers are free. A
	 * reserved worker that runs nothing and still cannot take the task is released, and the task searches again. */
	if (t->quarantine_worker) {
		struct vine_worker_info *reserved = t->quarantine_worker;
		if (check_worker_against_task(q, reserved, t)) {
			return reserved;
		}
		if (!worker_runs_tasks(reserved)) {
			vine_schedule_quarantine_release(t);
		}
	}

	/* An input without a transfer source now, such as a temporary file whose holders all reached
	 * worker-source-max-transfers, cannot reach a worker that lacks it, whatever the worker. Only the workers that hold
	 * the first such input are considered, and check_worker_against_task() finds any other such input for them. When the
	 * first such input has no holder, or a later one is held by no holder of the first, no worker can take the task.
	 * The task records these inputs, and the dispatch pass skips it while vine_schedule_task_is_blocked() finds that
	 * they still block it. */
	struct vine_file *unsourced = NULL;
	struct set *unsourced_holders = NULL;
	if (q->peer_transfers_enabled) {
		struct vine_mount *m;
		LIST_ITERATE(t->input_mounts, m)
		{
			if (!m || !m->file || vine_manager_transfer_source_available(q, m->file)) {
				continue;
			}
			if (!unsourced) {
				unsourced = m->file;
				unsourced_holders = hash_table_lookup(q->file_worker_table, unsourced->cached_name);
				if (!unsourced_holders || set_size(unsourced_holders) < 1) {
					t->blocking_inputs[0] = unsourced;
					return NULL;
				}
			} else if (!holders_hold_file(unsourced_holders, m->file)) {
				t->blocking_inputs[0] = unsourced;
				t->blocking_inputs[1] = m->file;
				return NULL;
			}
		}
	}

	/* first sort by the strategy-specific criterion, then run @check_worker_against_task on the sorted list */
	struct priority_queue *workers = priority_queue_create(0);
	if (!workers) {
		return NULL;
	}

	int a = t->worker_selection_algorithm;

	if (a == VINE_SCHEDULE_UNSET) {
		a = q->worker_selection_algorithm;
	}

	struct vine_worker_info *w;
	int iteration;
	if (unsourced) {
		SET_ITERATE(unsourced_holders, iteration, w)
		{
			rank_worker_for_task(q, t, w, a, workers);
		}
	} else {
		char *key;
		HASH_TABLE_ITERATE(q->worker_table, iteration, key, w)
		{
			rank_worker_for_task(q, t, w, a, workers);
		}
	}

	struct vine_worker_info *best_worker = NULL;
	while ((w = priority_queue_pop(workers))) {
		if (check_worker_against_task(q, w, t)) {
			best_worker = w;
			break;
		}
	}

	priority_queue_delete(workers);

	/* Only holders of the input without a source were considered, and none could take the task. */
	if (!best_worker && unsourced) {
		t->blocking_inputs[0] = unsourced;
	}

	if (!best_worker && t->worker_loss_charged && !t->quarantine_worker) {
		quarantine_reserve(q, t);
	}

	return best_worker;
}

typedef enum {
	CORES_BIT = (1 << 0),
	MEMORY_BIT = (1 << 1),
	DISK_BIT = (1 << 2),
	GPUS_BIT = (1 << 3),
} vine_resource_bitmask_t;

/*
Compares the resources needed by a task to a given worker.
Returns a bitmask that indicates which resource of the task, if any, cannot
be met by the worker. If the task fits in the worker, it returns 0.
*/

static vine_resource_bitmask_t is_task_larger_than_worker(struct vine_manager *q, struct vine_task *t, struct vine_worker_info *w)
{
	if (w->resources->tag < 0) {
		/* quickly return if worker has not sent its resources yet */
		return 0;
	}

	vine_resource_bitmask_t set = 0;
	struct rmsummary *l = vine_manager_choose_resources_for_task(q, w, t);

	// baseline resurce comparison of worker total resources and a task requested resorces

	if ((double)w->resources->cores.total < l->cores) {
		set = set | CORES_BIT;
	}

	if ((double)w->resources->memory.total < l->memory) {
		set = set | MEMORY_BIT;
	}

	if ((double)w->resources->disk.total < l->disk) {
		set = set | DISK_BIT;
	} else if ((double)w->resources->disk.total - w->resources->disk.inuse < l->disk && itable_size(w->current_tasks) < 1) {
		/* also trigger disk if worker's cache does not allow to run any task anymore */
		set = set | DISK_BIT;
	}

	if ((double)w->resources->gpus.total < l->gpus) {
		set = set | GPUS_BIT;
	}
	rmsummary_delete(l);

	return set;
}

/*
Compares the resources needed by a task to all connected workers.
Returns 0 if there is worker than can fit the task. Otherwise it returns a bitmask
that indicates that there was at least one worker that could not fit that task resource.
*/

static vine_resource_bitmask_t is_task_larger_than_any_worker(struct vine_manager *q, struct vine_task *t)
{
	char *key;
	struct vine_worker_info *w;
	int iteration;

	int bit_set = 0;
	HASH_TABLE_ITERATE(q->worker_table, iteration, key, w)
	{
		vine_resource_bitmask_t new_set = is_task_larger_than_worker(q, t, w);
		if (new_set == 0) {
			// Task could run on a currently connected worker, immediately
			// return
			return 0;
		}

		// Inherit the unfit criteria for this task
		bit_set = bit_set | new_set;
	}

	return bit_set;
}

/*
Determine if there exists a ready task that cannot be satisfied
by *any* connected worker, even if all other tasks finish.
If so, then display a suitable message to the user.
This is quite an expensive function and so is invoked only periodically.
*/

void vine_schedule_check_for_large_tasks(struct vine_manager *q)
{
	struct vine_task *t;
	int unfit_core = 0;
	int unfit_mem = 0;
	int unfit_disk = 0;
	int unfit_gpu = 0;

	struct rmsummary *largest_unfit_task = rmsummary_create(-1);

	struct skip_list_cursor *cur = skip_list_cursor_create(q->ready_tasks);
	skip_list_seek(cur, 0);
	while (skip_list_get(cur, (void **)&t)) {
		// advance cursor to the next task first,
		// in case there are continues, etc. below.
		skip_list_next(cur);

		// check each task against the queue of connected workers
		vine_resource_bitmask_t bit_set = is_task_larger_than_any_worker(q, t);
		if (bit_set) {
			rmsummary_merge_max(largest_unfit_task, vine_manager_task_resources_max(q, t));
			rmsummary_merge_max(largest_unfit_task, vine_manager_task_resources_min(q, t));
		}
		if (bit_set & CORES_BIT) {
			unfit_core++;
		}
		if (bit_set & MEMORY_BIT) {
			unfit_mem++;
		}
		if (bit_set & DISK_BIT) {
			unfit_disk++;
		}
		if (bit_set & GPUS_BIT) {
			unfit_gpu++;
		}
	}
	skip_list_cursor_delete(cur);

	if (unfit_core || unfit_mem || unfit_disk || unfit_gpu) {
		notice(D_VINE, "There are tasks that cannot fit any currently connected worker:\n");
	}

	if (unfit_core) {
		notice(D_VINE, "    %d waiting task(s) need more than %s", unfit_core, rmsummary_resource_to_str("cores", largest_unfit_task->cores, 1));
	}

	if (unfit_mem) {
		notice(D_VINE, "    %d waiting task(s) need more than %s of memory", unfit_mem, rmsummary_resource_to_str("memory", largest_unfit_task->memory, 1));
	}

	if (unfit_disk) {
		notice(D_VINE, "    %d waiting task(s) need more than %s of disk (workers' disk cache may be full)", unfit_disk, rmsummary_resource_to_str("disk", largest_unfit_task->disk, 1));
	}

	if (unfit_gpu) {
		notice(D_VINE, "    %d waiting task(s) need more than %s", unfit_gpu, rmsummary_resource_to_str("gpus", largest_unfit_task->gpus, 1));
	}

	rmsummary_delete(largest_unfit_task);
}

/*
Determine whether there is a worker that can fit the task and that has all its strict inputs.
*/
int vine_schedule_check_fixed_location(struct vine_manager *q, struct vine_task *t)
{
	int iteration;
	char *key;
	struct vine_worker_info *w;

	HASH_TABLE_ITERATE(q->worker_table, iteration, key, w)
	{
		if (check_fixed_location_worker(q, w, t)) {
			return 1;
		}
	}
	debug(D_VINE, "Missing fixed_location dependencies for task: %d", t->task_id);
	return 0;
}
