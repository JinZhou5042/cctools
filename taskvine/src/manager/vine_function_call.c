#include "vine_function_call.h"

#include "vine_task.h"
#include "vine_manager.h"
#include "vine_worker_info.h"

#include "itable.h"
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
	task->function_slots_inuse = 0;
	task->function_slots_reported_free = 0;
	task->function_credit_generation = 0;
	task->function_slot_credit_held = 0;
	task->function_start_acknowledged = 0;
	task->function_start_revoke_pending = 0;
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
				task->function_slot_credit_held && !task->function_start_acknowledged &&
				!task->function_start_revoke_pending) {
			task->function_start_acknowledged = 1;
			task->time_when_function_start_ack = timestamp_get();
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
		if (revoked && task && task->needs_library &&
				task->library_task_id == library_id &&
				task->function_credit_generation == generation &&
				task->function_slot_credit_held &&
				task->function_start_revoke_pending) {
			itable_remove(manager->function_start_grants, task->task_id);
			task->function_start_revoke_pending = 0;
			if (task->try_count > 0)
				task->try_count--;
			*requeue = task;
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
	task->function_slot_credit_held = 0;
	if (itable_lookup(worker->current_libraries, library->task_id) == library) {
		library->function_slots_reported_free = MIN(
				library->function_slots_total - library->function_slots_inuse,
				library->function_slots_reported_free + 1);
	}
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
		if (!task->function_start_acknowledged &&
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
