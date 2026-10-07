#include "vine_function_call.h"

#include "vine_task.h"
#include "vine_manager.h"
#include "vine_worker_info.h"

#include "itable.h"
#include "hash_table.h"
#include "list.h"
#include "skip_list.h"
#include "macros.h"
#include "stringtools.h"
#include "timestamp.h"
#include "xxmalloc.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void vine_function_call_task_init(struct vine_task *task)
{
	task->function_slots_requested = -1;
	vine_function_call_task_reset(task);
}

void vine_function_call_task_reset(struct vine_task *task)
{
	task->library_task_id = 0;
	task->function_slots_total = 0;
	task->function_slots_running_total = 0;
	task->function_slots_started = 0;
	task->function_adaptive = 0;
	task->function_slots_inuse = 0;
	task->function_slots_reported_free = 0;
	task->function_credit_generation = 0;
	task->function_window_generation = 0;
	task->function_slot_credit_held = 0;
	task->function_start_acknowledged = 0;
	task->function_start_revoke_pending = 0;
	task->function_rebalance_target = 0;
	task->time_when_function_start_ack = 0;
}

void vine_function_call_task_copy(struct vine_task *target,
		const struct vine_task *source)
{
	target->function_slots_requested = source->function_slots_requested;
	if (source->function_input)
		vine_task_set_function_input(target, source->function_input, source->function_input_length);
	vine_function_call_task_reset(target);
}

void vine_function_call_task_delete(struct vine_task *task)
{
	free(task->function_input);
	free(task->function_rebalance_target);
}

void vine_task_set_function_input(struct vine_task *task,
		const char *buffer, size_t size)
{
	free(task->function_input);
	task->function_input = 0;
	task->function_input_length = 0;
	if (buffer && size) {
		task->function_input = xxmalloc(size);
		memcpy(task->function_input, buffer, size);
		task->function_input_length = size;
	}
}

const char *vine_task_get_function_input(const struct vine_task *task)
{
	return task ? task->function_input : 0;
}

size_t vine_task_get_function_input_size(const struct vine_task *task)
{
	return task ? task->function_input_length : 0;
}

int vine_function_call_handle_info(struct vine_manager *manager,
		struct vine_worker_info *worker, const char *field, const char *value,
		struct vine_task **requeue)
{
	*requeue = 0;
	if (string_prefix_is(field, "function-credit")) {
		int library_id = 0;
		int free_slots = 0;
		int64_t generation = 0;
		if (sscanf(value, "%d %d %" SCNd64, &library_id, &free_slots, &generation) != 3) {
			return 1;
		}
		struct vine_task *library = itable_lookup(worker->current_libraries,
				library_id);
		if (library && free_slots >= 0 && free_slots <= library->function_slots_total &&
				generation > library->function_credit_generation) {
			library->function_credit_generation = generation;
			library->function_slots_reported_free = free_slots;
		}
		return 1;
	}
	if (string_prefix_is(field, "function-window")) {
		int library_id = 0;
		int running_slots = 0;
		int64_t generation = 0;
		if (sscanf(value, "%d %d %" SCNd64, &library_id,
				&running_slots, &generation) != 3)
			return 1;
		struct vine_task *library = itable_lookup(worker->current_libraries,
				library_id);
		if (library && library->function_adaptive && running_slots > 0 &&
				running_slots <= library->function_slots_total &&
				generation > library->function_window_generation) {
			library->function_window_generation = generation;
			library->function_slots_running_total = running_slots;
			manager->function_window_updates++;
			if (!manager->function_window_min ||
					running_slots < manager->function_window_min)
				manager->function_window_min = running_slots;
			if (running_slots > manager->function_window_max)
				manager->function_window_max = running_slots;
		}
		return 1;
	}
	if (string_prefix_is(field, "function-start")) {
		int task_id = 0;
		int library_id = 0;
		int64_t generation = 0;
		if (sscanf(value, "%d %d %" SCNd64, &task_id, &library_id, &generation) != 3) {
			return 1;
		}
		struct vine_task *task = itable_lookup(worker->current_tasks, task_id);
		if (task && task->needs_library && task->library_task_id == library_id &&
				task->function_credit_generation == generation &&
				task->function_slot_credit_held && !task->function_start_acknowledged) {
			task->function_start_acknowledged = 1;
			task->time_when_function_start_ack = timestamp_get();
			if (task->library_task)
				task->library_task->function_slots_started++;
			if (!task->function_start_revoke_pending)
				itable_remove(manager->function_start_grants, task->task_id);
		}
		return 1;
	}
	if (string_prefix_is(field, "function-revoked")) {
		int task_id = 0;
		int library_id = 0;
		int revoked = 0;
		int64_t generation = 0;
		struct vine_task *task = 0;
		if (sscanf(value, "%d %d %" SCNd64 " %d", &task_id, &library_id, &generation, &revoked) == 4)
			task = itable_lookup(worker->current_tasks, task_id);
		if (task && task->needs_library &&
				task->library_task_id == library_id &&
				task->function_credit_generation == generation &&
				task->function_slot_credit_held &&
				task->function_start_revoke_pending) {
			task->function_start_revoke_pending = 0;
			if (revoked) {
				manager->function_recalls_succeeded++;
				itable_remove(manager->function_start_grants, task->task_id);
				if (task->try_count > 0)
					task->try_count--;
				*requeue = task;
			} else {
				manager->function_recalls_missed++;
				free(task->function_rebalance_target);
				task->function_rebalance_target = 0;
				if (task->function_start_acknowledged)
					itable_remove(manager->function_start_grants, task->task_id);
			}
		}
		return 1;
	}
	return 0;
}

void vine_function_call_release_credit(struct vine_worker_info *worker,
		struct vine_task *task)
{
	if (!task || !task->needs_library || !task->library_task ||
			!task->function_slot_credit_held)
		return;
	struct vine_task *library = task->library_task;
	library->function_slots_inuse = MAX(0, library->function_slots_inuse - 1);
	if (task->function_start_acknowledged)
		library->function_slots_started = MAX(0,
				library->function_slots_started - 1);
	task->function_slot_credit_held = 0;
	if (itable_lookup(worker->current_libraries, library->task_id) == library) {
		library->function_slots_reported_free = MIN(
				library->function_slots_total - library->function_slots_inuse,
				library->function_slots_reported_free + 1);
	}
}

static int queued_grants(struct vine_worker_info *worker,
		const char *library_name, struct vine_task **first)
{
	int count = 0;
	uint64_t task_id;
	struct vine_task *task;
	int iteration;
	ITABLE_ITERATE(worker->current_tasks, iteration, task_id, task)
	{
		if (task->needs_library && !strcmp(task->needs_library, library_name) &&
				task->function_slot_credit_held &&
				!task->function_start_acknowledged &&
				!task->function_start_revoke_pending) {
			if (first && !*first)
				*first = task;
			count++;
		}
	}
	return count;
}

static int worker_has_nonlibrary_tasks(struct vine_worker_info *worker)
{
	uint64_t task_id;
	struct vine_task *task;
	int iteration;
	ITABLE_ITERATE(worker->current_tasks, iteration, task_id, task)
	{
		if (!task->provides_library)
			return 1;
	}
	return 0;
}

static int worker_has_library(struct vine_worker_info *worker,
		const char *library_name)
{
	struct vine_task *library;
	LIST_ITERATE(worker->current_libraries_list, library)
	{
		if (!strcmp(library->provides_library, library_name))
			return 1;
	}
	return 0;
}

/* Redistribute only grants which have not crossed the Worker start boundary.
 * This is deliberately scheduler-mediated: Workers never exchange ownership
 * messages, and a failed recall simply leaves the running task in place. */
int vine_function_call_rebalance(struct vine_manager *manager)
{
	if (!manager || manager->function_queue_multiplier <= 1 ||
			manager->function_rebalance_limit < 1 ||
			skip_list_size(manager->ready_tasks))
		return 0;
	uint64_t grant_id;
	struct vine_task *grant;
	int grant_iteration;
	ITABLE_ITERATE(manager->function_start_grants, grant_iteration, grant_id,
			grant)
	{
		if (grant->function_start_revoke_pending)
			return 0;
	}
	/* Pick the most overloaded donor first.  A late Worker may not have the
	 * required executor yet, so the donor's library name is also the bootstrap
	 * key for an empty receiver. */
	struct vine_worker_info *donor = 0;
	struct vine_task *donor_library = 0;
	int donor_queued = 0;
	char *key;
	struct vine_worker_info *worker;
	int iteration;
	HASH_TABLE_ITERATE(manager->worker_table, iteration, key, worker)
	{
		if (!worker || worker->draining)
			continue;
		struct vine_task *library;
		LIST_ITERATE(worker->current_libraries_list, library)
		{
			if (!library->function_adaptive)
				continue;
			int queued = queued_grants(worker, library->provides_library, 0);
			if (queued > donor_queued) {
				donor = worker;
				donor_library = library;
				donor_queued = queued;
			}
		}
	}
	if (!donor || !donor_library)
		return 0;

	struct vine_worker_info *receiver = 0;
	struct vine_task *receiver_library = 0;
	int receiver_need = 0;
	HASH_TABLE_ITERATE(manager->worker_table, iteration, key, worker)
	{
		if (!worker || worker == donor || worker->draining)
			continue;
		struct vine_task *library;
		LIST_ITERATE(worker->current_libraries_list, library)
		{
			if (!library->function_adaptive ||
					strcmp(library->provides_library,
						donor_library->provides_library) ||
					library->function_slots_inuse !=
						library->function_slots_started)
				continue;
			int need = library->function_slots_running_total -
					library->function_slots_started;
			if (need > receiver_need) {
				receiver = worker;
				receiver_library = library;
				receiver_need = need;
			}
		}
	}

	/* No ready receiver means a newly connected Worker has not seen a task and
	 * therefore has not installed the executor.  Seed exactly one idle Worker;
	 * the next pass recalls tasks after its first credit arrives. */
	if (!receiver || receiver_need < 1) {
		HASH_TABLE_ITERATE(manager->worker_table, iteration, key, worker)
		{
			if (!worker || worker == donor || worker->draining ||
					worker_has_nonlibrary_tasks(worker) ||
					worker_has_library(worker,
						donor_library->provides_library))
				continue;
			if (vine_manager_send_library_to_worker(manager, worker,
						donor_library->provides_library))
				return 1;
		}
		return 0;
	}

	int requested = MIN(receiver_need, donor_queued);
	requested = MIN(requested, manager->function_rebalance_limit);
	int recalled = 0;
	manager->function_rebalance_rounds++;
	while (recalled < requested) {
		struct vine_task *task = 0;
		queued_grants(donor, receiver_library->provides_library, &task);
		if (!task)
			break;
		task->function_start_revoke_pending = 1;
		free(task->function_rebalance_target);
		task->function_rebalance_target = xxstrdup(receiver->hashkey);
		vine_manager_send(manager, donor, "revoke %d %d %" PRId64 "\n",
				task->task_id, task->library_task_id,
				task->function_credit_generation);
		recalled++;
	}
	manager->function_recalls_sent += recalled;
	return recalled;
}

void vine_function_call_get_stats(struct vine_manager *manager,
		struct vine_function_call_stats *stats)
{
	if (!stats)
		return;
	memset(stats, 0, sizeof(*stats));
	if (!manager)
		return;
	stats->rebalance_rounds = manager->function_rebalance_rounds;
	stats->recalls_sent = manager->function_recalls_sent;
	stats->recalls_succeeded = manager->function_recalls_succeeded;
	stats->recalls_missed = manager->function_recalls_missed;
	stats->window_updates = manager->function_window_updates;
	stats->window_min = manager->function_window_min;
	stats->window_max = manager->function_window_max;
}

int vine_function_call_expire_grants(struct vine_manager *manager)
{
	if (manager->function_start_timeout <= 0 ||
			!itable_size(manager->function_start_grants))
		return 0;
	timestamp_t now = timestamp_get();
	if (now - manager->time_last_function_start_check <
			manager->function_start_check_interval)
		return 0;
	manager->time_last_function_start_check = now;
	int expired = 0;
	uint64_t task_id;
	struct vine_task *task;
	int iteration;
	ITABLE_ITERATE(manager->function_start_grants, iteration, task_id, task)
	{
		if ((!task->library_task || !task->library_task->function_adaptive) &&
				!task->function_start_acknowledged &&
				!task->function_start_revoke_pending && task->worker &&
				now - task->time_when_commit_end >= manager->function_start_timeout) {
			task->function_start_revoke_pending = 1;
			vine_manager_send(manager, task->worker, "revoke %d %d %" PRId64 "\n", task->task_id, task->library_task_id, task->function_credit_generation);
			expired++;
		}
	}
	return expired;
}

void vine_function_call_prepare_grant(struct vine_manager *manager,
		struct vine_task *task)
{
	struct vine_task *library = task->library_task;
	task->library_task_id = library->task_id;
	library->function_slots_inuse++;
	library->function_slots_reported_free = MAX(0,
			library->function_slots_reported_free - 1);
	task->function_slot_credit_held = 1;
	task->function_credit_generation = ++manager->next_function_grant_generation;
	task->function_start_acknowledged = 0;
	task->function_start_revoke_pending = 0;
	task->time_when_function_start_ack = 0;
}

void vine_function_call_commit_grant(struct vine_manager *manager,
		struct vine_task *task)
{
	itable_insert(manager->function_start_grants, task->task_id, task);
}
