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
#include "vine_manager_data_service.h"
#include "vine_mount.h"
#include "vine_temp.h"

#include "debug.h"
#include "hash_table.h"
#include "itable.h"
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
#include <stdint.h>

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

static int check_worker_is_available(struct vine_manager *q, struct vine_worker_info *w);

/* Drop the reservation of a quarantined task, if any. */
void vine_schedule_quarantine_release(struct vine_task *t)
{
	struct vine_worker_info *w = t->quarantine_worker;
	if (w && w->quarantine_reserved_id == t->task_id) {
		w->quarantine_reserved_id = 0;
	}
	t->quarantine_worker = NULL;
}

/* Reserve for a quarantined task that found no free worker the worker with the most free cores and slots that is not
 * reserved and runs no quarantined task. It takes no new task until it runs nothing. */
static void quarantine_reserve(struct vine_manager *q, struct vine_task *t)
{
	struct skip_list_cursor *cur = skip_list_cursor_create(q->workers_by_capacity);
	if (!cur) {
		return;
	}
	struct vine_worker_info *w;
	skip_list_seek(cur, 0);
	while (skip_list_get(cur, (void **)&w)) {
		skip_list_next(cur);
		if (!w->quarantine_task_id && !worker_reserved_for_other(q, w, t) && check_worker_is_available(q, w)) {
			w->quarantine_reserved_id = t->task_id;
			t->quarantine_worker = w;
			debug(D_VINE, "task %d was charged a lost worker and waits for %s to run nothing else", t->task_id, w->addrport);
			break;
		}
	}
	skip_list_cursor_delete(cur);
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
 * count, so only workers whose capacity index priority is positive can have it, and the walk of the index stops at the
 * first worker without free cores or slots. A busy run, where no worker has room, then costs O(1) per step instead of
 * a pass over all workers. */
int vine_schedule_any_worker_accepts_tasks(struct vine_manager *q)
{
	struct vine_worker_info *w;
	if (q->gpus_requested) {
		char *key;
		int iteration;
		HASH_TABLE_ITERATE(q->worker_table, iteration, key, w)
		{
			if (check_worker_accepts_tasks(q, w)) {
				return 1;
			}
		}
		return 0;
	}

	int accepts = 0;
	struct skip_list_cursor *cur = skip_list_cursor_create(q->workers_by_capacity);
	if (!cur) {
		return 1;
	}
	skip_list_seek(cur, 0);
	while (skip_list_get(cur, (void **)&w) && w->schedule_capacity > 0) {
		if (check_worker_accepts_tasks(q, w)) {
			accepts = 1;
			break;
		}
		skip_list_next(cur);
	}
	skip_list_cursor_delete(cur);
	return accepts;
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

/* Return true if input f, recorded as blocking, keeps a task from every worker on its own: it is held without a transfer
 * source and no holder has room. The answer is computed once per dispatch pass and blocking serial of the file, so the
 * tasks that wait for the same input share one check. */
static int input_blocks_alone(struct vine_manager *q, struct vine_file *f, int *held)
{
	if (f->blocked_memo_pass != q->blocked_pass || f->blocked_memo_serial != f->blocked_serial) {
		f->blocked_memo_pass = q->blocked_pass;
		f->blocked_memo_serial = f->blocked_serial;
		f->blocked_memo_held = input_held_without_source(q, f);
		f->blocked_memo_alone = f->blocked_memo_held && input_holders_cannot_run(q, f, NULL);
	}
	*held = f->blocked_memo_held;
	return f->blocked_memo_alone;
}

/* Return true if the inputs that kept this ready task from every worker at its last scheduling attempt still keep it
 * from every worker. An input without a transfer source can reach no worker that lacks it, so the task cannot run
 * when no holder of such an input has room, or when no worker holds both recorded inputs. The check costs one lookup
 * per holder of the recorded inputs instead of a pass over all inputs and workers. */
static int blocking_inputs_still_block(struct vine_manager *q, struct vine_task *t)
{
	struct vine_file *first = t->blocking_inputs[0];
	struct vine_file *second = t->blocking_inputs[1];

	int first_held = 0;
	int second_held = 0;
	if (input_blocks_alone(q, first, &first_held)) {
		return 1;
	}
	if (second && input_blocks_alone(q, second, &second_held)) {
		return 1;
	}
	return first_held && second_held && input_holders_cannot_run(q, first, second);
}

/* Register blocking input f with the events that can change whether it blocks, once per blocking serial: the file's
 * own events find it by name, a transfer from its URL source by that URL (blocked_sources), and the events of each of
 * its holders through the holder's set of blocking files. Each entry holds a reference to the file. */
static int register_blocking_input(struct vine_manager *q, struct vine_file *f)
{
	if (f->blocked_registered == f->blocked_serial + 1) {
		return 1;
	}
	if (f->type == VINE_URL && f->source) {
		struct set *files = hash_table_lookup(q->blocked_sources, f->source);
		if (!files) {
			files = set_create(0);
			if (!files) {
				return 0;
			}
			hash_table_insert(q->blocked_sources, f->source, files);
		}
		if (!set_lookup(files, f) && set_insert(files, f)) {
			vine_file_addref(f);
		}
	}
	struct set *holders = hash_table_lookup(q->file_worker_table, f->cached_name);
	if (holders) {
		struct vine_worker_info *w;
		int iteration;
		SET_ITERATE(holders, iteration, w)
		{
			if (!w->blocked_files) {
				w->blocked_files = set_create(0);
				if (!w->blocked_files) {
					return 0;
				}
			}
			if (!set_lookup(w->blocked_files, f) && set_insert(w->blocked_files, f)) {
				vine_file_addref(f);
			}
		}
	}
	f->blocked_registered = f->blocked_serial + 1;
	q->blocked_files_registered++;
	return 1;
}

int vine_schedule_task_waits(struct vine_manager *q, struct vine_task *t)
{
	struct vine_file *first = t->blocking_inputs[0];
	struct vine_file *second = t->blocking_inputs[1];
	return (q->peer_transfers_enabled || t->blocking_vault) && first && t->blocked_epoch == q->blocked_epoch && first->blocked_serial == t->blocked_serials[0] && (!second || second->blocked_serial == t->blocked_serials[1]);
}

int vine_schedule_task_is_blocked(struct vine_manager *q, struct vine_task *t)
{
	if (!t->blocking_inputs[0] || (!q->peer_transfers_enabled && !t->blocking_vault)) {
		return 0;
	}
	if (vine_schedule_task_waits(q, t)) {
		return 1;
	}
	t->blocked_epoch = 0;
	/* A task that waits for an input to come inline into the vault waits until that receive ends, which wakes it. */
	if (t->blocking_vault ? !vine_manager_data_service_receiving_inline(q, t->blocking_inputs[0]) : !blocking_inputs_still_block(q, t)) {
		return 0;
	}

	/* The answer can change only on an event of the recorded inputs or of their holders, so the task waits for one.
	 * A task whose inputs cannot be registered for every such event is checked again in the next pass. */
	for (int i = 0; i < 2; i++) {
		struct vine_file *f = t->blocking_inputs[i];
		if (f && !register_blocking_input(q, f)) {
			return 1;
		}
		t->blocked_serials[i] = f ? f->blocked_serial : 0;
	}
	t->blocked_epoch = q->blocked_epoch;
	return 1;
}

/* Advance the blocking serial of file f, which ends the wait of the tasks blocked by it. Only a file registered at its
 * current serial can have tasks that wait for it, so only its wake counts as an event for the dispatch pass. */
static void wake_one_file(struct vine_manager *q, struct vine_file *f)
{
	if (f->blocked_registered == f->blocked_serial + 1) {
		q->schedule_wakes++;
		if (q->blocked_files_registered > 0) {
			q->blocked_files_registered--;
		}
	}
	f->blocked_serial++;
}

/* Advance the blocking serial of each file in files and empty the set, dropping its references. */
static void wake_files(struct vine_manager *q, struct set *files)
{
	struct vine_file *f;
	while ((f = set_pop(files))) {
		wake_one_file(q, f);
		vine_file_delete(f);
	}
}

void vine_schedule_wake_file(struct vine_manager *q, const char *key)
{
	if (!key) {
		return;
	}
	/* Only a file registered at its current serial has tasks that wait for its next event: a task records the serial
	 * of each input it finds blocking, and registers the input at that serial. While no file is registered, which is
	 * the common case, the event needs no lookup of the file. A registered file that is freed before its next event
	 * stays counted, which costs only lookups. */
	if (q->blocked_files_registered > 0) {
		struct vine_file *f = hash_table_lookup(q->file_table, key);
		if (f) {
			wake_one_file(q, f);
		}
	}
	if (q->blocked_sources && hash_table_size(q->blocked_sources) > 0) {
		struct set *files = hash_table_remove(q->blocked_sources, key);
		if (files) {
			wake_files(q, files);
			set_delete(files);
		}
	}
}

void vine_schedule_wake_declared_file(struct vine_manager *q, struct vine_file *f)
{
	if (!f) {
		return;
	}
	wake_one_file(q, f);
	if (q->blocked_sources && hash_table_size(q->blocked_sources) > 0) {
		struct set *files = hash_table_remove(q->blocked_sources, f->cached_name);
		if (files) {
			wake_files(q, files);
			set_delete(files);
		}
	}
}

void vine_schedule_wake_worker(struct vine_manager *q, struct vine_worker_info *w)
{
	if (w->blocked_files) {
		wake_files(q, w->blocked_files);
	}
}

void vine_schedule_wake_all(struct vine_manager *q)
{
	q->blocked_epoch++;
	q->schedule_wakes++;
}

void vine_schedule_delete_blocked_sources(struct vine_manager *q)
{
	char *source;
	struct set *files;
	int iteration;
	HASH_TABLE_ITERATE(q->blocked_sources, iteration, source, files)
	{
		wake_files(q, files);
	}
	hash_table_clear(q->blocked_sources, (void *)set_delete);
	hash_table_delete(q->blocked_sources);
	q->blocked_sources = NULL;
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

/* Return the part of a worker's VINE_SCHEDULE_FILES priority that does not depend on the task: a fraction below one
 * byte that grows with the worker's free cores and function slots. */
static double free_capacity_priority(struct vine_manager *q, struct vine_worker_info *w)
{
	int free_cores = count_worker_free_cores(q, w);
	return (double)free_cores / (free_cores + 1);
}

/* The capacity index (q->workers_by_capacity) holds every worker of worker_table, ordered by free_capacity_priority(),
 * highest first, and then by the order in which the workers connected. An entry is keyed by (schedule_capacity,
 * -schedule_serial), so it is found and moved in O(log W) without a scan. */

void vine_schedule_capacity_index_add(struct vine_manager *q, struct vine_worker_info *w)
{
	if (w->schedule_serial) {
		return;
	}
	w->schedule_serial = ++q->worker_serial;
	w->schedule_capacity = free_capacity_priority(q, w);
	skip_list_insert(q->workers_by_capacity, w, w->schedule_capacity, -(double)w->schedule_serial);
}

/* Take a worker's entry out of the capacity index. */
static void capacity_index_unlink(struct vine_manager *q, struct vine_worker_info *w)
{
	double priority[] = {w->schedule_capacity, -(double)w->schedule_serial};
	if (!skip_list_remove_by_priority_arr(q->workers_by_capacity, priority)) {
		fatal("worker %s is missing from the capacity index", w->addrport);
	}
}

void vine_schedule_capacity_index_remove(struct vine_manager *q, struct vine_worker_info *w)
{
	/* A holder that leaves may leave a file without a replica, whose readers must then start its recovery. */
	vine_schedule_wake_worker(q, w);
	if (!w->schedule_serial) {
		return;
	}
	capacity_index_unlink(q, w);
	w->schedule_serial = 0;
}

void vine_schedule_capacity_index_update(struct vine_manager *q, struct vine_worker_info *w)
{
	/* Each change of the worker's room for a task is followed by an update, also one that keeps its place. A worker
	 * with room may let a ready task that waits for one of its files run. */
	if (w->blocked_files && set_size(w->blocked_files) > 0 && check_worker_accepts_tasks(q, w)) {
		vine_schedule_wake_worker(q, w);
	}
	if (!w->schedule_serial) {
		return;
	}
	double capacity = free_capacity_priority(q, w);
	if (capacity == w->schedule_capacity) {
		return;
	}
	capacity_index_unlink(q, w);
	w->schedule_capacity = capacity;
	skip_list_insert(q->workers_by_capacity, w, w->schedule_capacity, -(double)w->schedule_serial);
}

void vine_schedule_capacity_index_update_all(struct vine_manager *q)
{
	char *key;
	struct vine_worker_info *w;
	int iteration;
	HASH_TABLE_ITERATE(q->worker_table, iteration, key, w)
	{
		vine_schedule_capacity_index_update(q, w);
	}
}

/* Return true if a worker of worker_table is a candidate for the task before the full check of
 * check_worker_against_task(). The inputs of the task were counted in the worker's scratch space under stamp. */
static int worker_is_candidate(struct vine_manager *q, struct vine_worker_info *w, uint64_t stamp, struct vine_file *unsourced, int64_t input_size)
{
	/* briefly skip uninitialized workers, more detailed checks are performed in @check_worker_against_task */
	if (!w || !w->resources || w->type != VINE_WORKER_TYPE_WORKER || w->draining) {
		return 0;
	}

	/* if task groups are enabled, skip workers that are running a group task */
	if (q->task_groups_enabled && w->tasks_committed > 0) {
		int iteration_group;
		uint64_t tidg;
		struct vine_task *tg;
		ITABLE_ITERATE(w->current_tasks, iteration_group, tidg, tg)
		{
			if (tg->group_id) {
				return 0;
			}
		}
	}

	/* skip this worker if it lacks an input that has no transfer source now */
	if (unsourced && (w->schedule_stamp != stamp || !w->schedule_holds_unsourced)) {
		return 0;
	}

	/* skip this worker if the available cache space drops below 0 after the task is dispatched */
	int64_t cached_input_size = w->schedule_stamp == stamp ? w->schedule_cached_bytes : 0;
	int64_t uncached_input_size = input_size - cached_input_size;
	int64_t available_cache_space_after_task_dispatch = MEGABYTES_TO_BYTES(w->resources->disk.total) - (w->inuse_cache + uncached_input_size);
	if (available_cache_space_after_task_dispatch <= 0) {
		return 0;
	}

	return 1;
}

/* Return the priority of a candidate worker for the task under the selection algorithm a. */
static double worker_priority(struct vine_manager *q, struct vine_worker_info *w, int a, uint64_t stamp, int64_t input_size)
{
	/* the size of cached and uncached input files on the worker */
	int64_t cached_input_size = w->schedule_stamp == stamp ? w->schedule_cached_bytes : 0;
	int64_t uncached_input_size = input_size - cached_input_size;

	switch (a) {
	case VINE_SCHEDULE_FILES:
		/* Find the worker that has the largest quantity of cached data needed by this task,
		 * so as to minimize transfer work that must be done by the manager. Among workers with equal cached
		 * data, such as every worker for a task without cached inputs, the one with the most free cores or
		 * function slots wins, through a fraction below one byte. Otherwise ties go to the same workers in
		 * table order whenever they have room, which piles short tasks and their outputs onto a few workers. */
		return cached_input_size + free_capacity_priority(q, w);
	case VINE_SCHEDULE_DISK:
		/* Find the worker that will be left with the most disk space if the task is finished there */
		return MEGABYTES_TO_BYTES(w->resources->disk.total) - (w->inuse_cache + uncached_input_size);
	case VINE_SCHEDULE_WORST:
		/* Find the worker that is the "worst fit" for this task, meaning the worker with the most free cores.
		 * We don't check on memory or disk because if there are no free cores, then the task will not fit anyway. */
		return count_worker_free_cores(q, w);
	case VINE_SCHEDULE_TIME:
		/* Find the worker that produced the fastest runtime of prior tasks. */
		return w->total_tasks_complete == 0 ? HUGE_VAL : -(double)(w->total_task_time + w->total_transfer_time) / w->total_tasks_complete;
	case VINE_SCHEDULE_FCFS:
		/* Deprecated, same as random */
	case VINE_SCHEDULE_RAND:
	default:
		/* Default to random selection. */
		return random_double();
	}
}

/* Advance the cursor over the capacity index to the next candidate that holds no input of the task and return it, or
 * return NULL at the end of the index. A worker that holds an input is ranked with its cached bytes instead. */
static struct vine_worker_info *next_capacity_candidate(struct vine_manager *q, struct skip_list_cursor *cur, uint64_t stamp, int64_t input_size)
{
	struct vine_worker_info *w;
	while (skip_list_get(cur, (void **)&w)) {
		skip_list_next(cur);
		if (w->schedule_stamp != stamp && worker_is_candidate(q, w, stamp, NULL, input_size)) {
			return w;
		}
	}
	return NULL;
}

/* Return true if a holder of @receiving, an input of @t that comes inline into the vault and has no transfer source
 * until it is published, lacks a large temporary input of @t. Running @t on that holder now would fetch the large
 * input over the network, while the receive takes milliseconds. The cost is one lookup per large input and holder of
 * @receiving, usually its producer alone. */
static int receiving_input_draws_task_away(struct vine_manager *q, struct vine_task *t, struct vine_file *receiving)
{
	struct set *producers = hash_table_lookup(q->file_worker_table, receiving->cached_name);
	struct vine_mount *m;
	LIST_ITERATE(t->input_mounts, m)
	{
		if (!m || !m->file || m->file->type != VINE_TEMP || m->file->size <= VINE_DATA_INLINE_BYTES) {
			continue;
		}
		if (!producers) {
			return 1;
		}
		struct vine_worker_info *w;
		int iteration;
		SET_ITERATE(producers, iteration, w)
		{
			if (!vine_file_replica_table_lookup(w, m->file->cached_name)) {
				return 1;
			}
		}
	}
	return 0;
}

/* Return the first worker that passes check_worker_against_task() in the order of VINE_SCHEDULE_FILES. A worker that
 * holds no input of the task has priority free_capacity_priority(), the order of the capacity index, so only the
 * holders are ranked here, with their cached bytes, and the two orders are merged. Workers are thus tried in the same
 * order as a ranking of every worker, at a cost of O(holders) plus O(log W) per worker tried instead of O(W). A holder
 * may have left worker_table while it is pending removal, which its serial of zero shows. When an input has no transfer
 * source now, only the holders of that input are candidates. */
static struct vine_worker_info *find_worker_by_files(struct vine_manager *q, struct vine_task *t, struct list *holders, uint64_t stamp, struct vine_file *unsourced, int64_t input_size)
{
	/* The queue ranks only holders, so it is created for them when the first one is a candidate. A task whose inputs no
	 * worker holds, such as one that reads only a buffer of its own, then allocates none: the default capacity of 127 entries
	 * was an allocation of 1 KB per call, large enough to make malloc consolidate its fast bins every time. */
	struct priority_queue *ranked = NULL;
	struct vine_worker_info *w;
	LIST_ITERATE(holders, w)
	{
		if (w->schedule_serial && worker_is_candidate(q, w, stamp, unsourced, input_size)) {
			if (!ranked) {
				ranked = priority_queue_create(list_size(holders));
				if (!ranked) {
					return NULL;
				}
			}
			priority_queue_push(ranked, w, worker_priority(q, w, VINE_SCHEDULE_FILES, stamp, input_size));
		}
	}

	struct skip_list_cursor *cur = NULL;
	struct vine_worker_info *next = NULL;
	if (!unsourced) {
		cur = skip_list_cursor_create(q->workers_by_capacity);
		skip_list_seek(cur, 0);
		next = next_capacity_candidate(q, cur, stamp, input_size);
	}

	struct vine_worker_info *best_worker = NULL;
	while (1) {
		if (ranked && priority_queue_size(ranked) > 0 && (!next || priority_queue_get_top_priority(ranked) >= next->schedule_capacity)) {
			w = priority_queue_pop(ranked);
		} else if (next) {
			w = next;
			next = next_capacity_candidate(q, cur, stamp, input_size);
		} else {
			break;
		}
		if (check_worker_against_task(q, w, t)) {
			best_worker = w;
			break;
		}
	}

	if (cur) {
		skip_list_cursor_delete(cur);
	}
	if (ranked) {
		priority_queue_delete(ranked);
	}
	return best_worker;
}

/* The number of holders from which an input with a transfer source is not ranked holder by holder when it is the only
 * input with holders (find_worker_by_wide_input()). Below it, ranking the holders costs little. */
#define VINE_SCHEDULE_WIDE_HOLDERS 32

/* Return the first worker that passes check_worker_against_task() in the order of VINE_SCHEDULE_FILES for a task whose
 * only input with holders is wide, which holders hold. Every holder caches the same bytes of the task, wide->size > 0,
 * so they rank above every other worker and among themselves by free capacity, the order of the capacity index: the
 * index is walked for holders first and then for the others, the order find_worker_by_files() would produce, at
 * O(log W) per worker tried instead of O(holders). An input held by every worker, as a broadcast or fanout one is,
 * made each dispatch O(workers) (74% of the manager thread in a fanout of 100,000 tasks over 400 workers). */
static struct vine_worker_info *find_worker_by_wide_input(struct vine_manager *q, struct vine_task *t, struct vine_file *wide, struct set *holders, uint64_t stamp, int64_t input_size)
{
	struct skip_list_cursor *cur = skip_list_cursor_create(q->workers_by_capacity);
	if (!cur) {
		return NULL;
	}
	struct vine_worker_info *best_worker = NULL;
	for (int holding = 1; holding >= 0 && !best_worker; holding--) {
		struct vine_worker_info *w;
		skip_list_seek(cur, 0);
		while (skip_list_get(cur, (void **)&w)) {
			skip_list_next(cur);
			if (!set_lookup(holders, w) != !holding) {
				continue;
			}
			if (holding) {
				w->schedule_stamp = stamp;
				w->schedule_cached_bytes = wide->size;
				w->schedule_holds_unsourced = 0;
			}
			if (worker_is_candidate(q, w, stamp, NULL, input_size) && check_worker_against_task(q, w, t)) {
				best_worker = w;
				break;
			}
		}
	}
	skip_list_cursor_delete(cur);
	return best_worker;
}

/* Select the best worker for this task, based on the current scheduling mode. */

struct vine_worker_info *vine_schedule_task_to_worker(struct vine_manager *q, struct vine_task *t)
{
	if (!q || !t) {
		return NULL;
	}

	struct list *holders = list_create();
	if (!holders) {
		return NULL;
	}

	int a = t->worker_selection_algorithm;

	if (a == VINE_SCHEDULE_UNSET) {
		a = q->worker_selection_algorithm;
	}

	/* Add up the cached bytes of the task's inputs per worker from the workers that hold each input, which costs one
	 * lookup per input instead of one per input and worker. file_worker_table lists a worker for every replica that
	 * the worker's current_files holds, in any state. Each holder is listed once in holders.
	 *
	 * An input without a transfer source now, such as a temporary file whose holders all reached
	 * worker-source-max-transfers, cannot reach a worker that lacks it, whatever the worker. Only the workers that hold
	 * the first such input are considered, and check_worker_against_task() finds any other such input for them. When the
	 * first such input has no holder, or a later one is held by no holder of the first, no worker can take the task.
	 * The task records these inputs, and the dispatch pass skips it while vine_schedule_task_is_blocked() finds that
	 * they still block it. */
	t->blocking_inputs[0] = NULL;
	t->blocking_inputs[1] = NULL;
	t->blocked_epoch = 0;
	t->blocking_vault = 0;

	/* A quarantined task with a reservation waits for its worker until that worker runs nothing, or takes the worker at
	 * the head of the capacity index if that one runs nothing first, so a long task on the reserved worker does not hold
	 * it back while other workers are free. A reserved worker that runs nothing and still cannot take the task is
	 * released, and the task searches again. */
	if (t->worker_loss_charged && t->quarantine_worker) {
		struct vine_worker_info *reserved = t->quarantine_worker;
		if (check_worker_against_task(q, reserved, t)) {
			list_delete(holders);
			return reserved;
		}
		if (worker_runs_tasks(reserved)) {
			struct vine_worker_info *head = NULL;
			struct skip_list_cursor *cur = skip_list_cursor_create(q->workers_by_capacity);
			if (cur) {
				skip_list_seek(cur, 0);
				skip_list_get(cur, (void **)&head);
				skip_list_cursor_delete(cur);
			}
			list_delete(holders);
			if (head && head != reserved && !worker_runs_tasks(head) && check_worker_against_task(q, head, t)) {
				return head;
			}
			return NULL;
		}
		vine_schedule_quarantine_release(t);
	}

	uint64_t stamp = ++q->schedule_stamp;
	int64_t input_size = 0;
	struct vine_file *unsourced = NULL;
	/* Of the inputs that come inline into the vault, and have no source until then, the one whose receive started
	 * last. Such inputs do not end the pass early, so a task that reads many of them waits for the one that tends to
	 * arrive last, instead of being checked again as each arrives. */
	struct vine_file *receiving = NULL;
	uint64_t receiving_order = 0;
	/* The first input with a transfer source and at least VINE_SCHEDULE_WIDE_HOLDERS holders, whose holders are
	 * visited only if another input has holders or no source (find_worker_by_wide_input()). */
	struct vine_file *wide = NULL;
	struct set *wide_holders = NULL;
	struct vine_mount *m;
	LIST_ITERATE(t->input_mounts, m)
	{
		if (!m || !m->file) {
			continue;
		}
		input_size += m->file->size;
		uint64_t order = vine_manager_data_service_receiving_inline(q, m->file);
		if (order > receiving_order) {
			receiving_order = order;
			receiving = m->file;
		}
		int first_unsourced = 0;
		int other_unsourced = 0;
		if (q->peer_transfers_enabled && !vine_manager_transfer_source_available(q, m->file)) {
			if (!unsourced) {
				unsourced = m->file;
				first_unsourced = 1;
			} else {
				other_unsourced = 1;
			}
		}
		int held_with_unsourced = 0;
		struct set *input_holders = hash_table_lookup(q->file_worker_table, m->file->cached_name);
		if (a == VINE_SCHEDULE_FILES && !wide && !first_unsourced && !other_unsourced && m->file->size > 0 && input_holders && set_size(input_holders) >= VINE_SCHEDULE_WIDE_HOLDERS) {
			wide = m->file;
			wide_holders = input_holders;
			continue;
		}
		if (input_holders) {
			struct vine_worker_info *holder;
			int holder_iteration;
			SET_ITERATE(input_holders, holder_iteration, holder)
			{
				if (holder->schedule_stamp != stamp) {
					holder->schedule_stamp = stamp;
					holder->schedule_cached_bytes = 0;
					holder->schedule_holds_unsourced = 0;
					list_push_tail(holders, holder);
				}
				holder->schedule_cached_bytes += m->file->size;
				held_with_unsourced |= holder->schedule_holds_unsourced;
				holder->schedule_holds_unsourced |= first_unsourced;
			}
		}
		/* Each worker lacks the first input without a source when it has no holder, or lacks one of two such inputs
		 * when no holder of the first holds the second. */
		if (!order && ((first_unsourced && (!input_holders || set_size(input_holders) < 1)) || (other_unsourced && !held_with_unsourced))) {
			t->blocking_inputs[0] = unsourced;
			t->blocking_inputs[1] = first_unsourced ? NULL : m->file;
			list_delete(holders);
			return NULL;
		}
	}

	/* A task that reads a large temporary input and an input still coming inline into the vault waits for that
	 * receive, unless the holders of the inline input hold the large inputs too. Otherwise only those holders could
	 * take it now, usually the small input's producer, which then fetches the large inputs over the network, while
	 * the receive takes milliseconds and its publication wakes the task for the holders of its large inputs. */
	if (receiving && receiving_input_draws_task_away(q, t, receiving)) {
		list_delete(holders);
		t->blocking_inputs[0] = receiving;
		t->blocking_vault = 1;
		return NULL;
	}

	struct vine_worker_info *best_worker = NULL;

	if (wide && list_size(holders) == 0 && !unsourced) {
		best_worker = find_worker_by_wide_input(q, t, wide, wide_holders, stamp, input_size);
	} else if (a == VINE_SCHEDULE_FILES) {
		if (wide) {
			/* Another input has holders or no source, so the wide input's holders are ranked with theirs. */
			struct vine_worker_info *holder;
			int holder_iteration;
			SET_ITERATE(wide_holders, holder_iteration, holder)
			{
				if (holder->schedule_stamp != stamp) {
					holder->schedule_stamp = stamp;
					holder->schedule_cached_bytes = 0;
					holder->schedule_holds_unsourced = 0;
					list_push_tail(holders, holder);
				}
				holder->schedule_cached_bytes += wide->size;
			}
		}
		best_worker = find_worker_by_files(q, t, holders, stamp, unsourced, input_size);
	} else {
		/* Other algorithms rank every worker by the strategy-specific criterion, then run @check_worker_against_task
		 * on the sorted list. */
		struct priority_queue *workers = priority_queue_create(0);
		if (workers) {
			char *key;
			struct vine_worker_info *w;
			int iteration;
			HASH_TABLE_ITERATE(q->worker_table, iteration, key, w)
			{
				if (worker_is_candidate(q, w, stamp, unsourced, input_size)) {
					priority_queue_push(workers, w, worker_priority(q, w, a, stamp, input_size));
				}
			}
			while ((w = priority_queue_pop(workers))) {
				if (check_worker_against_task(q, w, t)) {
					best_worker = w;
					break;
				}
			}
			priority_queue_delete(workers);
		}
	}

	list_delete(holders);

	/* Only holders of the input without a source were considered, and none could take the task. A task with an input
	 * that comes inline into the vault waits for it, whatever else it reads. */
	if (!best_worker && receiving) {
		t->blocking_inputs[0] = receiving;
		t->blocking_vault = 1;
	} else if (!best_worker && unsourced) {
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
