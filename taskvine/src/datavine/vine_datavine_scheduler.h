/* DataVine scheduler API. */
#ifndef VINE_DATAVINE_SCHEDULER_H
#define VINE_DATAVINE_SCHEDULER_H

#include <stddef.h>
#include <stdint.h>

struct vine_datavine_scheduler;

enum vine_datavine_task_state {
	VINE_DATAVINE_TASK_WAITING = 1,
	VINE_DATAVINE_TASK_READY = 2,
	VINE_DATAVINE_TASK_RUNNING = 3,
	VINE_DATAVINE_TASK_DONE = 4,
};

struct vine_datavine_scheduler *vine_datavine_scheduler_create(
		int64_t maximum_task_id, uint64_t maximum_tasks,
		uint64_t maximum_edges);
void vine_datavine_scheduler_delete(
		struct vine_datavine_scheduler *scheduler);

/*
The scheduler is owned by one manager event loop and is not internally locked.
Parents are encoded as consecutive little-endian unsigned 64-bit TaskIDs.
Failed additions are transactional and leave no partially registered task.
*/
int vine_datavine_scheduler_add_task(
		struct vine_datavine_scheduler *scheduler, int64_t task_id,
		const char *buffer, size_t size);
int vine_datavine_scheduler_seal(
		struct vine_datavine_scheduler *scheduler);

/*
Append-only dynamic DAG updates. The base revision must match the current
revision. New edges may only target tasks created by the open update, which
keeps validation and commit proportional to the update rather than the DAG.
vine_datavine_scheduler_add_task() adds a task and its encoded parents to the
open update. Commit is atomic.
*/
uint64_t vine_datavine_scheduler_revision(
		struct vine_datavine_scheduler *scheduler);
int vine_datavine_scheduler_begin_update(
		struct vine_datavine_scheduler *scheduler, uint64_t base_revision);
int vine_datavine_scheduler_commit_update(
		struct vine_datavine_scheduler *scheduler);
void vine_datavine_scheduler_abort_update(
		struct vine_datavine_scheduler *scheduler);

int vine_datavine_scheduler_mark_done(
		struct vine_datavine_scheduler *scheduler, int64_t task_id);
int vine_datavine_scheduler_mark_pending(
		struct vine_datavine_scheduler *scheduler, int64_t task_id);
enum vine_datavine_task_state vine_datavine_scheduler_task_state(
		struct vine_datavine_scheduler *scheduler, int64_t task_id);
/* Roll back one DONE task without disturbing DONE dependents or running work. */
int vine_datavine_scheduler_rollback_done(
		struct vine_datavine_scheduler *scheduler, int64_t task_id);
/* Rebuild from a packed little-endian set of already-DONE TaskIDs. */
int vine_datavine_scheduler_rebuild(
		struct vine_datavine_scheduler *scheduler,
		const char *buffer, size_t size);

/* Claim the smallest READY TaskID, or zero when no task is READY. */
int64_t vine_datavine_scheduler_take(
		struct vine_datavine_scheduler *scheduler);
int vine_datavine_scheduler_complete(
		struct vine_datavine_scheduler *scheduler);

#endif
