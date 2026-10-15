/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_sandbox.h"
#include "vine_cache.h"
#include "vine_cache_file.h"
#include "vine_file.h"
#include "vine_mount.h"
#include "vine_task.h"
#include "vine_worker.h"

#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "file_link_recursive.h"
#include "stringtools.h"

#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

char *vine_sandbox_full_path(struct vine_process *p, const char *sandbox_name)
{
	return string_format("%s/%s", p->sandbox, sandbox_name);
}

/*
Determine whether all the files needed by this process are present.
Return VINE_STATUS_READY if all are present.
Return VINE_STATUS_PROCESSING if some are not ready.
Return VINE_STATUS_FAILED if some have definitely failed.

Files stay in the cache once present, so each call resumes at the first input that was not ready the last time.
The cache starts the transfers of all inputs on its own.
*/

vine_cache_status_t vine_sandbox_ensure(struct vine_process *p, struct vine_cache *cache, struct link *manager, struct itable *procs_table)
{
	if (!p->input_cursor) {
		p->input_cursor = list_cursor_create(p->task->input_mounts);
		list_seek(p->input_cursor, 0);
	}

	struct vine_mount *m;
	while (list_get(p->input_cursor, (void **)&m)) {
		int processing = 0;
		vine_cache_status_t cache_status = vine_cache_ensure(cache, m->file->cached_name);

		switch (cache_status) {
		case VINE_CACHE_STATUS_PENDING:
		case VINE_CACHE_STATUS_PROCESSING:
		case VINE_CACHE_STATUS_TRANSFERRED:
			processing++;
			break;
		case VINE_CACHE_STATUS_READY:
			break;
		case VINE_CACHE_STATUS_UNKNOWN: {
			struct vine_process *lp;
			int iteration;
			uint64_t task_id;
			int found_file = 0;
			ITABLE_ITERATE(procs_table, iteration, task_id, lp)
			{
				struct vine_mount *lm;
				LIST_ITERATE(lp->task->output_mounts, lm)
				{
					if (strcmp(lm->file->cached_name, m->file->cached_name) == 0) {
						found_file = 1;
						break;
					}
				}
				if (found_file) {
					break;
				}
			}
			if (found_file) {
				processing++;
				break;
			}
		} break;
		case VINE_CACHE_STATUS_FAILED:
			debug(D_VINE, "unavailable in cache for task sandbox: %s", m->file->cached_name);
			return VINE_CACHE_STATUS_FAILED;
		}

		if (processing) {
			return VINE_CACHE_STATUS_PROCESSING;
		}
		list_next(p->input_cursor);
	}

	return VINE_CACHE_STATUS_READY;
}

/*
Ensure that a given input file/dir/object is present in the cache,
(which should have occurred from a prior transfer)
and then link it into the sandbox at the desired location.
*/

static int stage_input_file(struct vine_process *p, struct vine_mount *m, struct vine_file *f, struct vine_cache *cache)
{
	char *cache_path = vine_cache_data_path(cache, f->cached_name);
	char *sandbox_path = vine_sandbox_full_path(p, m->remote_name);

	int result = 0;

	vine_cache_status_t status;
	status = vine_cache_ensure(cache, f->cached_name);
	if (status == VINE_CACHE_STATUS_READY) {
		create_dir_parents(sandbox_path, 0777);
		debug(D_VINE, "input: link %s -> %s", cache_path, sandbox_path);
		if (m->flags & VINE_MOUNT_SYMLINK) {
			/* If the user has requested a symlink, just do that b/c it is faster for large dirs. */
			result = symlink(cache_path, sandbox_path);
			/* Change sense of Unix result to true/false. */
			result = !result;
		} else {
			/* Otherwise recursively hard-link the object into the sandbox. */
			result = file_link_recursive(cache_path, sandbox_path, 1);
		}

		if (!result)
			debug(D_VINE, "couldn't link %s into sandbox as %s: %s", cache_path, sandbox_path, strerror(errno));
	} else {
		debug(D_VINE, "input: %s is not ready in the cache!", f->cached_name);
		result = 0;
	}

	free(cache_path);
	free(sandbox_path);

	return result;
}

/* Create an empty output directory when requested by VINE_MOUNT_MKDIR */

static int create_empty_output_dir(struct vine_process *p, struct vine_mount *m)
{
	char *sandbox_path = vine_sandbox_full_path(p, m->remote_name);

	int result = mkdir(sandbox_path, 0755);
	if (result != 0) {
		debug(D_VINE, "sandbox: couldn't mkdir %s: %s", sandbox_path, strerror(errno));
		free(sandbox_path);
		return 0;
	} else {
		free(sandbox_path);
		return 1;
	}
}

/*
For each input file specified by the process,
stage it into the sandbox directory from the cache.
*/

int vine_sandbox_stagein(struct vine_process *p, struct vine_cache *cache)
{
	struct vine_task *t = p->task;
	int result = 1;

	struct vine_mount *m;

	/* For each input mount, stage it into the sandbox. */

	LIST_ITERATE(t->input_mounts, m)
	{
		result = stage_input_file(p, m, m->file, cache);
		if (!result)
			break;
	}

	/* If any of the output mounts have the MKDIR flag, then create those empty dirs. */

	LIST_ITERATE(t->output_mounts, m)
	{
		if (m->flags & VINE_MOUNT_MKDIR) {
			result = create_empty_output_dir(p, m);
			if (!result)
				break;
		}
	}

	return result;
}

/*
Move a given output file back to the target cache location and inform the manager of the added file. A regular file
takes one stat. A directory is measured in pieces, resuming the measurement kept in the process, until the budget
expires. Return 0 if the output continues in a later call, and 1 once the output was handled, whether it was added to
the cache or not.
*/

static int stage_output_file(struct vine_process *p, struct vine_mount *m, struct vine_file *f, struct vine_cache *cache, struct link *manager, const struct budget *b)
{
	char *sandbox_path = vine_sandbox_full_path(p, m->remote_name);
	int64_t size = 0;

	/* A mode of zero means that the output was not observed yet, because every file type has a nonzero mode. */
	if (!p->output_mode) {
		struct stat info;
		debug(D_VINE, "output: measuring %s", sandbox_path);
		if (stat(sandbox_path, &info) < 0) {
			debug(D_VINE, "output: unable to measure size of %s: %s\n", sandbox_path, strerror(errno));
			free(sandbox_path);
			return 1;
		}
		p->output_mode = info.st_mode;
		p->output_mtime = info.st_mtime;
		size = info.st_size;
		if (S_ISDIR(p->output_mode) && budget_expired(b)) {
			free(sandbox_path);
			return 0;
		}
	}

	if (S_ISDIR(p->output_mode)) {
		int result = path_disk_size_info_get_budget(sandbox_path, b, &p->output_measurement_state, NULL);
		if (result == 0 && !p->output_measurement_state->complete_measurement) {
			free(sandbox_path);
			return 0;
		}
		if (result < 0) {
			debug(D_VINE, "output: unable to measure size of %s: %s\n", sandbox_path, strerror(errno));
		}
		size = p->output_measurement_state->last_byte_size_complete;
		path_disk_size_info_delete_state(p->output_measurement_state);
		p->output_measurement_state = NULL;
		if (result < 0) {
			p->output_mode = 0;
			free(sandbox_path);
			return 1;
		}
	}

	char *cache_path = vine_cache_data_path(cache, f->cached_name);
	timestamp_t transfer_time = p->execution_end - p->execution_start;

	debug(D_VINE, "output: moving %s to %s", sandbox_path, cache_path);
	if (vine_cache_add_file(cache, f->cached_name, sandbox_path, f->cache_level, p->output_mode, size, p->output_mtime, p->execution_start, transfer_time, manager)) {
		f->size = size;
	} else {
		debug(D_VINE, "output: unable to move %s to %s: %s\n", sandbox_path, cache_path, strerror(errno));
	}
	p->output_mode = 0;

	free(cache_path);
	free(sandbox_path);

	return 1;
}

/*
Move all output files of a completed process back into the proper cache location, until the budget expires. Return 1
once every output was handled, or 0 if a later call continues where this one stopped. Each call makes progress even
with an expired budget: it handles one regular file, or it observes a directory output or measures one of its entries.
A process whose outputs are regular files thus completes in one call unless the budget expires between its outputs.

Stage-out deliberately does not fail. If any of the desired outputs was not created, we still want the task to be
marked as completed and sent back to the manager. The manager will handle the consequences of missing output files
when processing the result of the task. Therefore, the task should be reported as done only after this function
returned 1.
*/

int vine_sandbox_stageout(struct vine_process *p, struct vine_cache *cache, struct link *manager, const struct budget *b)
{
	if (!p->output_cursor) {
		p->output_cursor = list_cursor_create(p->task->output_mounts);
		list_seek(p->output_cursor, 0);
	}

	int handled = 0;
	struct vine_mount *m;
	while (list_get(p->output_cursor, (void **)&m)) {
		if (handled && budget_expired(b)) {
			return 0;
		}
		if (!stage_output_file(p, m, m->file, cache, manager, b)) {
			return 0;
		}
		handled++;
		list_next(p->output_cursor);
	}

	list_cursor_destroy(p->output_cursor);
	p->output_cursor = NULL;
	return 1;
}
