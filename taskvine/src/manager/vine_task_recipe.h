/*
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_TASK_RECIPE_H
#define VINE_TASK_RECIPE_H

/*
This module keeps what the manager needs to recreate a lost temporary file, and creates a recovery task only when the
file is lost and needed. Most temporary files are never lost, so a copy of the producing task for each submitted task
cost memory and time for nothing.

The recovery source of a temporary file is one of:

- The task that last wrote the file (f->recovery_task, a standard task), while anyone other than the files it wrote
  holds it: the application, or the manager while the task is active. The files add a reference to the task, which
  counts them in t->recovery_refs.
- A recipe (f->recovery_recipe), once only those files hold the task: the task's description without its state,
  results, or measurements, which takes the task's own strings and mounts, so the task is freed. A task that the
  application changes while files use it gives them a recipe made by copying first, because the files must be
  recreated as the task was submitted.
- A recovery task (f->recovery_task, of type VINE_TASK_TYPE_RECOVERY), made from the recipe when the file is lost and
  needed. It becomes a recipe again once only its files hold it.

The files written by one task share one source, and so share the recovery task that recreates them together. Every
function here runs on the manager's thread.
*/

#include "vine_file.h"
#include "vine_task.h"

/* The description of a task that recreates its temporary output files. */
struct vine_task_recipe;

/* Make t the recovery source of the temporary file f, replacing the earlier source of f. */
void vine_task_recipe_set_source(struct vine_file *f, struct vine_task *t);

/* Drop the recovery source of f, such as when only the source still refers to f. f may be freed by this call. */
void vine_task_recipe_clear_source(struct vine_file *f);

/* Return a new recovery task that recreates f, which every file of the same source now uses, or NULL when f has no
 * source or the task cannot be created. The caller holds the returned reference and submits the task before it
 * deletes the reference. The source must not be an active task. */
struct vine_task *vine_task_recipe_start_recovery(struct vine_file *f);

/* Give the files whose recovery source is t a recipe of t instead. With take set, only those files hold t, the recipe
 * takes t's strings and mounts, and t must be freed afterwards. Otherwise the recipe copies them and t is unchanged. */
void vine_task_recipe_detach(struct vine_task *t, int take);

/* Return the group id of the source of the temporary file f, or zero. */
int vine_task_recipe_group_id(struct vine_file *f);

#endif
