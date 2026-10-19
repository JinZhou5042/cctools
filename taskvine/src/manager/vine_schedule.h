/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_SCHEDULE_H
#define VINE_SCHEDULE_H

/*
Implementation of the manager's scheduling algorithm.
A single entry point maps a given task to the best available
worker, taking into account all scheduling priorities and constraints.
This module is private to the manager and should not be invoked by the end user.
*/

#include "vine_manager.h"
#include "vine_task.h"
#include "vine_worker_info.h"

struct vine_worker_info *vine_schedule_task_to_worker( struct vine_manager *q, struct vine_task *t );
void vine_schedule_check_for_large_tasks( struct vine_manager *q );
int vine_schedule_check_fixed_location(struct vine_manager *q, struct vine_task *t);
int vine_schedule_in_ramp_down(struct vine_manager *q);
struct vine_task *vine_schedule_find_library(struct vine_manager *q, struct vine_worker_info *w, const char *library_name);
int check_worker_against_task(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t);
/* Return non-zero when some worker could accept a task now. When none can, no ready task can be dispatched until a
 * worker reports a change, such as a finished task, so a pass over the ready tasks would be wasted. */
int vine_schedule_any_worker_accepts_tasks(struct vine_manager *q);
/* Blocked ready tasks. vine_schedule_task_to_worker() records in a ready task that it found no worker the inputs that
 * kept it from every worker (blocking_inputs). vine_schedule_task_is_blocked() returns non-zero when those inputs still
 * keep it from every worker, so a dispatch pass skips it without looking at its other inputs. Its answer depends only
 * on the recorded inputs and on the workers that hold them, so the task remembers the blocking serial of each recorded
 * input (blocked_serials) and the manager's blocked_epoch, and vine_schedule_task_waits() skips it in O(1) while none
 * of them changed. A recorded input found blocking is registered once per serial with the events that can change the
 * answer, which advance its serial:
 * - vine_schedule_wake_file() with its cached name, when a replica of it is added, becomes ready, or is removed, or its
 *   vault entry is published or removed, and with a source URL when a transfer from it ends at the limit of
 *   file-source-max-transfers (a vault transfer, manager://<cached name>, wakes the cached name; the manager's own
 *   send of a local file ends when its replica becomes ready or is removed, see vine_manager_limits_sends());
 * - vine_schedule_wake_worker() with a holder of it, through the holder's set of blocking files, when the holder frees
 *   a transfer slot at worker-source-max-transfers, may have gained room for a task, ends its suspicion as a source
 *   with a message, reports its transfer port, or leaves;
 * - vine_schedule_wake_all(), which advances blocked_epoch in O(1), when a setting changes, and once a second for the
 *   conditions that change with time alone: recent transfer and task failures, blocklist expiry, draining, and factory
 *   limits.
 * An event thus costs O(1), or O(blocking files) that a worker holds, never a pass over the tasks that wait. A task
 * whose wait ended checks its inputs again when the dispatch pass reaches it, and the check of each input is shared by
 * every task that it blocks within one pass (the file's memo). Entries hold file references, so no entry dangles.
 * A task with an input that comes inline into the vault, and has no source until it is published, records instead the
 * one whose receive started last (blocking_vault). It waits while that receive is in progress, whatever the holders,
 * and the receive's end wakes it by cached name. */
int vine_schedule_task_is_blocked(struct vine_manager *q, struct vine_task *t);
/* Return non-zero, in O(1), while a task found blocked waits for an event that can unblock it. A dispatch pass skips
 * such a task before anything else, so the checks of retrieve_ready_task() reach it once its wait ends, at least
 * once a second. */
int vine_schedule_task_waits(struct vine_manager *q, struct vine_task *t);
void vine_schedule_wake_file(struct vine_manager *q, const char *key);
/* Wake the tasks blocked by declared file f, as vine_schedule_wake_file() does for its cached name, without looking
 * the file up by name. */
void vine_schedule_wake_declared_file(struct vine_manager *q, struct vine_file *f);
void vine_schedule_wake_worker(struct vine_manager *q, struct vine_worker_info *w);
void vine_schedule_wake_all(struct vine_manager *q);
/* Drop the registrations by source URL when the manager is deleted. */
void vine_schedule_delete_blocked_sources(struct vine_manager *q);
/* Quarantine of tasks charged an unexpected loss of a worker (t->worker_loss_charged). The charging rule cannot tell a
 * task that makes its worker exit from a task that shared the worker with it, so after its first charged loss a task
 * runs only on a worker that runs no other task, and that worker takes no other task while it runs. The losses of a
 * task that kills its workers are then its own, and a task that only shared a worker with it runs alone and
 * completes. When no worker is free, the task reserves one, which takes no new task until its tasks end, so a
 * quarantined task never waits for a worker to become free by chance. Committing the task releases the reservation.
 * The checks cost O(1) per worker tried, and finding a worker to reserve walks the capacity index once per task. */
void vine_schedule_quarantine_commit(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t);
void vine_schedule_quarantine_reap(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t);
void vine_schedule_quarantine_remove_worker(struct vine_manager *q, struct vine_worker_info *w);
/* Keep the capacity index, which orders the workers of worker_table by their free cores and function slots so that
 * VINE_SCHEDULE_FILES finds the best worker without a pass over all workers. A worker is added when it enters
 * worker_table and removed when it leaves it. An update moves the worker to its place after its committed resources,
 * its libraries, their slots, or its reported resources changed, and does nothing for a worker outside the index.
 * Updating all workers follows a change of the resource submit multiplier. */
void vine_schedule_capacity_index_add(struct vine_manager *q, struct vine_worker_info *w);
void vine_schedule_capacity_index_remove(struct vine_manager *q, struct vine_worker_info *w);
void vine_schedule_capacity_index_update(struct vine_manager *q, struct vine_worker_info *w);
void vine_schedule_capacity_index_update_all(struct vine_manager *q);
#endif
