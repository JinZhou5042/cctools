/*
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_task_recipe.h"
#include "vine_counters.h"
#include "vine_mount.h"

#include "debug.h"
#include "list.h"
#include "rmsummary.h"
#include "xxmalloc.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

/* One mount of a recipe: the file, its name in the task's sandbox, and its flags. The recipe holds a reference to the
 * file. */
struct vine_task_recipe_mount {
	struct vine_file *file;
	char *remote_name;
	vine_mount_flags_t flags;
};

/* The fields of a task that vine_task_copy() copies, which are what a recovery task needs. A recovery task starts
 * from the first resource allocation and without fixed locations, as a copy does. */
struct vine_task_recipe {
	int refcount;				   // Number of files whose recovery_recipe is this recipe.
	int input_count;			   // Input mounts, the first entries of mounts.
	int output_count;			   // Output mounts, the entries after the inputs.
	char *command_line;
	char *needs_library;
	char *provides_library;
	char *tag;
	char *category;
	char *monitor_output_directory;
	struct vine_file *monitor_snapshot_file;   // Also an input mount, which holds its reference.
	struct list *env_list;			   // NULL when the task sets no environment variables.
	struct list *feature_list;		   // NULL when the task requires no features.
	struct rmsummary *resources_requested;	   // NULL when a new task with the same library requests the same.
	vine_task_func_exec_mode_t func_exec_mode;
	int function_slots_requested;
	vine_schedule_t worker_selection_algorithm;
	double user_priority;
	int max_retries;
	int max_forsaken;
	int64_t min_running_time;
	int group_id;
	struct vine_task_recipe_mount mounts[]; // input_count inputs, then output_count outputs.
};

static struct rmsummary *default_resources_plain = NULL;
static struct rmsummary *default_resources_function_call = NULL;
static pthread_once_t default_resources_once = PTHREAD_ONCE_INIT;

/* Create the default resources once for the process. Managers in several threads of one process may make recipes at
 * the same time, and a thread that found one pointer set could read the other before it was. */
static void default_resources_create(void)
{
	default_resources_plain = rmsummary_create(-1);
	default_resources_function_call = rmsummary_create(-1);
	default_resources_function_call->cores = 0;
	default_resources_function_call->memory = 0;
	default_resources_function_call->disk = 0;
	default_resources_function_call->gpus = 0;
}

/* Return true if a new task that requires the given library, or none, requests the same resources as s. Such a
 * recipe keeps no resources of its own: a function call requests no cores, memory, disk, or gpus, and every other
 * resource is unset. */
static int resources_are_default(const struct rmsummary *s, const char *needs_library)
{
	pthread_once(&default_resources_once, default_resources_create);
	return !memcmp(s, needs_library ? default_resources_function_call : default_resources_plain, sizeof(*s));
}

/* Count the mounts of a list that a recipe keeps. Mounts that the manager adds for one attempt, such as the resource
 * monitor's, are not part of the task. */
static int count_mounts(struct list *mounts)
{
	int count = 0;
	struct vine_mount *m;
	LIST_ITERATE(mounts, m)
	{
		if (!(m->flags & VINE_RETRACT_ON_RESET)) {
			count++;
		}
	}
	return count;
}

/* Fill entries from a list of mounts and return the next free entry. With take set, the entries take the mounts' file
 * references and names, and the list is emptied. A substitute source is chosen for each attempt and is not kept. */
static struct vine_task_recipe_mount *fill_mounts(struct vine_task_recipe_mount *entry, struct list *mounts, int take)
{
	struct vine_mount *m;
	if (!take) {
		LIST_ITERATE(mounts, m)
		{
			if (!(m->flags & VINE_RETRACT_ON_RESET)) {
				entry->file = vine_file_addref(m->file);
				entry->remote_name = xxstrdup(m->remote_name);
				entry->flags = m->flags;
				entry++;
			}
		}
		return entry;
	}
	while ((m = list_pop_head(mounts))) {
		if (m->flags & VINE_RETRACT_ON_RESET) {
			vine_mount_delete(m);
			continue;
		}
		entry->file = m->file;
		entry->remote_name = m->remote_name;
		entry->flags = m->flags;
		entry++;
		vine_file_delete(m->substitute);
		free(m);
		vine_counters.mount.deleted++;
	}
	return entry;
}

/* Take a string of a task with take set, or copy it otherwise. */
static char *take_string(char **field, int take)
{
	char *value = *field;
	if (take) {
		*field = NULL;
		return value;
	}
	return value ? xxstrdup(value) : NULL;
}

/* Take a list of strings of a task with take set, or copy it otherwise. An empty list is not kept. */
static struct list *take_list(struct list **field, int take)
{
	struct list *value = *field;
	if (!value || list_size(value) == 0) {
		return NULL;
	}
	if (take) {
		*field = NULL;
		return value;
	}
	return list_copy(value, (void *)xxstrdup);
}

/* Create a recipe of t without references from files. */
static struct vine_task_recipe *vine_task_recipe_create(struct vine_task *t, int take)
{
	int input_count = count_mounts(t->input_mounts);
	int output_count = count_mounts(t->output_mounts);
	struct vine_task_recipe *r = malloc(sizeof(*r) + (input_count + output_count) * sizeof(struct vine_task_recipe_mount));
	if (!r) {
		return NULL;
	}

	r->refcount = 0;
	r->input_count = input_count;
	r->output_count = output_count;
	struct vine_task_recipe_mount *entry = fill_mounts(r->mounts, t->input_mounts, take);
	fill_mounts(entry, t->output_mounts, take);

	r->command_line = take_string(&t->command_line, take);
	r->needs_library = take_string(&t->needs_library, take);
	r->provides_library = take_string(&t->provides_library, take);
	r->tag = take_string(&t->tag, take);
	r->category = take_string(&t->category, take);
	r->monitor_output_directory = take_string(&t->monitor_output_directory, take);
	r->monitor_snapshot_file = t->monitor_snapshot_file;
	r->env_list = take_list(&t->env_list, take);
	r->feature_list = take_list(&t->feature_list, take);

	r->resources_requested = NULL;
	if (t->resources_requested && !resources_are_default(t->resources_requested, r->needs_library)) {
		if (take) {
			r->resources_requested = t->resources_requested;
			t->resources_requested = NULL;
		} else {
			r->resources_requested = rmsummary_copy(t->resources_requested, 0);
		}
	}

	r->func_exec_mode = t->func_exec_mode;
	r->function_slots_requested = t->function_slots_requested;
	r->worker_selection_algorithm = t->worker_selection_algorithm;
	r->user_priority = t->user_priority;
	r->max_retries = t->max_retries;
	r->max_forsaken = t->max_forsaken;
	r->min_running_time = t->min_running_time;
	r->group_id = t->group_id;

	return r;
}

static void vine_task_recipe_free(struct vine_task_recipe *r)
{
	for (int i = 0; i < r->input_count + r->output_count; i++) {
		vine_file_delete(r->mounts[i].file);
		free(r->mounts[i].remote_name);
	}
	free(r->command_line);
	free(r->needs_library);
	free(r->provides_library);
	free(r->tag);
	free(r->category);
	free(r->monitor_output_directory);
	if (r->env_list) {
		list_clear(r->env_list, (void *)free);
		list_delete(r->env_list);
	}
	if (r->feature_list) {
		list_clear(r->feature_list, (void *)free);
		list_delete(r->feature_list);
	}
	rmsummary_delete(r->resources_requested);
	free(r);
}

/* Drop the reference of one file to a recipe. */
static void vine_task_recipe_release(struct vine_task_recipe *r)
{
	if (!r) {
		return;
	}
	r->refcount--;
	if (r->refcount == 0) {
		vine_task_recipe_free(r);
	}
}

/* Return a recovery task made from a recipe, as vine_task_copy() makes it from a task. */
static struct vine_task *vine_task_recipe_make_task(const struct vine_task_recipe *r)
{
	struct vine_task *t = vine_task_create(r->command_line);
	if (!t) {
		return NULL;
	}
	t->type = VINE_TASK_TYPE_RECOVERY;

	for (int i = 0; i < r->input_count + r->output_count; i++) {
		const struct vine_task_recipe_mount *entry = &r->mounts[i];
		struct vine_mount *m = vine_mount_create(entry->file, entry->remote_name, entry->flags, NULL);
		list_push_tail(i < r->input_count ? t->input_mounts : t->output_mounts, m);
	}

	list_delete(t->env_list);
	t->env_list = r->env_list ? list_copy(r->env_list, (void *)xxstrdup) : NULL;
	list_delete(t->feature_list);
	t->feature_list = r->feature_list ? list_copy(r->feature_list, (void *)xxstrdup) : NULL;

	if (r->needs_library) {
		vine_task_set_library_required(t, r->needs_library);
	}
	if (r->provides_library) {
		vine_task_set_library_provided(t, r->provides_library);
	}
	/* As vine_task_set_function_exec_mode() sets it. */
	if (r->func_exec_mode && t->provides_library) {
		t->func_exec_mode = r->func_exec_mode;
	}
	if (r->tag) {
		vine_task_set_tag(t, r->tag);
	}
	if (r->category) {
		vine_task_set_category(t, r->category);
	}
	if (r->monitor_output_directory) {
		vine_task_set_monitor_output(t, r->monitor_output_directory);
	}
	t->monitor_snapshot_file = r->monitor_snapshot_file;
	t->function_slots_requested = r->function_slots_requested;

	vine_task_set_scheduler(t, r->worker_selection_algorithm);
	vine_task_set_priority(t, r->user_priority);
	vine_task_set_retries(t, r->max_retries);
	vine_task_set_max_forsaken(t, r->max_forsaken);
	vine_task_set_time_min(t, r->min_running_time);

	if (r->resources_requested) {
		rmsummary_delete(t->resources_requested);
		t->resources_requested = rmsummary_copy(r->resources_requested, 0);
	}

	/* The task lives as long as the files it can recreate. The resources an attempt measures and is allocated are
	 * created when it is submitted. */
	rmsummary_delete(t->resources_measured);
	rmsummary_delete(t->resources_allocated);
	t->resources_measured = NULL;
	t->resources_allocated = NULL;

	t->group_id = r->group_id;

	return t;
}

void vine_task_recipe_set_source(struct vine_file *f, struct vine_task *t)
{
	struct vine_task *previous_task = f->recovery_task;
	struct vine_task_recipe *previous_recipe = f->recovery_recipe;

	f->recovery_task = vine_task_addref(t);
	t->recovery_refs++;
	f->recovery_recipe = NULL;

	if (previous_task) {
		previous_task->recovery_refs--;
		vine_task_delete(previous_task);
	}
	vine_task_recipe_release(previous_recipe);
}

void vine_task_recipe_clear_source(struct vine_file *f)
{
	struct vine_task *task = f->recovery_task;
	struct vine_task_recipe *recipe = f->recovery_recipe;

	f->recovery_task = NULL;
	f->recovery_recipe = NULL;

	if (task) {
		task->recovery_refs--;
		vine_task_delete(task);
	}
	vine_task_recipe_release(recipe);
}

void vine_task_recipe_detach(struct vine_task *t, int take)
{
	struct vine_task_recipe *r = vine_task_recipe_create(t, take);
	if (!r) {
		/* The files keep the task itself, which only costs memory. */
		debug(D_VINE, "could not allocate the recovery recipe of task %d", t->task_id);
		return;
	}

	for (int i = r->input_count; i < r->input_count + r->output_count; i++) {
		struct vine_file *f = r->mounts[i].file;
		if (f->recovery_task == t) {
			f->recovery_task = NULL;
			f->recovery_recipe = r;
			r->refcount++;
			t->recovery_refs--;
			t->refcount--;
			vine_counters.task.deleted++;
		}
	}

	if (t->recovery_refs != 0) {
		notice(D_VINE, "task %d is still the recovery source of %d files that it does not write", t->task_id, t->recovery_refs);
	}

	if (r->refcount == 0) {
		vine_task_recipe_free(r);
	}
}

struct vine_task *vine_task_recipe_start_recovery(struct vine_file *f)
{
	if (f->recovery_task) {
		vine_task_recipe_detach(f->recovery_task, 0);
	}

	struct vine_task_recipe *r = f->recovery_recipe;
	if (!r) {
		return NULL;
	}

	struct vine_task *t = vine_task_recipe_make_task(r);
	if (!t) {
		return NULL;
	}

	/* The recipe is freed with its last file, after the task added its own references to the files. */
	r->refcount++;
	for (int i = r->input_count; i < r->input_count + r->output_count; i++) {
		struct vine_file *output = r->mounts[i].file;
		if (output->recovery_recipe == r) {
			output->recovery_recipe = NULL;
			output->recovery_task = vine_task_addref(t);
			t->recovery_refs++;
			r->refcount--;
		}
	}
	vine_task_recipe_release(r);

	return t;
}

int vine_task_recipe_group_id(struct vine_file *f)
{
	if (f->recovery_task) {
		return f->recovery_task->group_id;
	}
	if (f->recovery_recipe) {
		return f->recovery_recipe->group_id;
	}
	return 0;
}
