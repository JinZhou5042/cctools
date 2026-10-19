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

/* Quarantine of tasks charged an unexpected loss of a worker (t->worker_loss_charged). The charging rule cannot tell a
 * task that makes its worker exit from a task that shared the worker with it, so after its first charged loss a task
 * runs only on a worker that runs no other task, and that worker takes no other task while it runs. The losses of a
 * task that kills its workers are then its own, and a task that only shared a worker with it runs alone and
 * completes. When no worker is free, the task reserves one, which takes no new task until its tasks end, so a
 * quarantined task never waits for a worker to become free by chance. The reservation ends when the task is
 * committed to a worker, when it leaves the ready state, or when its worker is removed. */
void vine_schedule_quarantine_commit(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t);
void vine_schedule_quarantine_reap(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t);
void vine_schedule_quarantine_release(struct vine_task *t);
void vine_schedule_quarantine_remove_worker(struct vine_manager *q, struct vine_worker_info *w);
#endif
