/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_file_recovery_subgraph.h"
#include "vine_manager.h"
#include "vine_file.h"
#include "vine_task.h"
#include "vine_mount.h"
#include "vine_file_replica_table.h"

#include "debug.h"
#include "hash_table.h"
#include "xxmalloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static void build_recovery_subgraph_recursive(
		struct vine_manager *q,
		struct vine_file *f,
		struct list *recovery_tasks,
		struct hash_table *task_critical_paths,
		struct hash_table *task_execution_times)
{
	// If the file is in PBB, we can stop recursion for this branch
	if (q->pbb_worker) {
		struct vine_file_replica *replica = vine_file_replica_table_lookup(q->pbb_worker, f->cached_name);
		if (replica && replica->state == VINE_FILE_REPLICA_STATE_READY) {
			return;
		}
	}

	// Get the recovery task for this file
	if (!f->recovery_task) {
		// This file doesn't have a recovery task
		return;
	}

	// Check if the task is already in our list
	struct vine_task *t = f->recovery_task;
	list_first_item(recovery_tasks);
	struct vine_task *list_task;
	while ((list_task = list_next_item(recovery_tasks))) {
		if (list_task->task_id == t->task_id) {
			// Task already in list, no need to process it again
			return;
		}
	}

	// Get the execution time for this task from the producer function
	long current_task_execution_time = f->producer_task_execution_time;

	// Store the execution time in the hash table
	char task_id_str[32];
	sprintf(task_id_str, "%d", t->task_id);

	long *exec_time = malloc(sizeof(long));
	*exec_time = current_task_execution_time;
	hash_table_insert(task_execution_times, xxstrdup(task_id_str), exec_time);

	// Track the maximum path length from inputs
	long max_input_path_length = 0;

	// Process all inputs
	struct vine_mount *m;
	LIST_ITERATE(t->input_mounts, m)
	{
		struct vine_file *input_file = m->file;
		if (!input_file)
			continue;

		// Recursively process each input file
		build_recovery_subgraph_recursive(q, input_file, recovery_tasks, task_critical_paths, task_execution_times);

		// If this input file has a recovery task, check its critical path length
		if (input_file->recovery_task) {
			char input_task_id_str[32];
			sprintf(input_task_id_str, "%d", input_file->recovery_task->task_id);

			long *input_path_length = hash_table_lookup(task_critical_paths, input_task_id_str);
			if (input_path_length && *input_path_length > max_input_path_length) {
				max_input_path_length = *input_path_length;
			}
		}
	}

	// Add this task to the list
	list_push_tail(recovery_tasks, t);

	// Calculate and store this task's critical path length
	long path_length = max_input_path_length + current_task_execution_time;

	long *stored_path_length = malloc(sizeof(long));
	*stored_path_length = path_length;
	hash_table_insert(task_critical_paths, xxstrdup(task_id_str), stored_path_length);

	debug(D_VINE, "Task %d in recovery subgraph for %s: execution time %.3fs, critical path %.3fs", t->task_id, f->cached_name, current_task_execution_time / 1000000.0, path_length / 1000000.0);
}

struct recovery_subgraph *calculate_recovery_subgraph(struct vine_manager *q, const char *cachename)
{
	struct recovery_subgraph *subgraph = malloc(sizeof(struct recovery_subgraph));
	subgraph->tasks = list_create();
	subgraph->critical_path = 0;
	subgraph->total_time = 0;
	subgraph->task_execution_times = hash_table_create(0, 0);

	// Check if the file is in the PBB worker
	if (q->pbb_worker) {
		struct vine_file_replica *replica = vine_file_replica_table_lookup(q->pbb_worker, cachename);
		if (replica && replica->state == VINE_FILE_REPLICA_STATE_READY) {
			// File is in PBB, no recovery tasks needed
			return subgraph;
		}
	}

	// Look up the file
	struct vine_file *f = hash_table_lookup(q->file_table, cachename);
	if (!f) {
		// File not found, can't calculate recovery path
		return subgraph;
	}

	// Create a hash table to track the critical path lengths to each task
	struct hash_table *task_critical_paths = hash_table_create(0, 0);

	// Recursively build the recovery subgraph
	build_recovery_subgraph_recursive(q, f, subgraph->tasks, task_critical_paths, subgraph->task_execution_times);

	// Determine the overall critical path by finding the maximum path length
	char *task_id_str;
	void *path_length_value;

	HASH_TABLE_ITERATE(task_critical_paths, task_id_str, path_length_value)
	{
		long *path_length = (long *)path_length_value;
		if (*path_length > subgraph->critical_path) {
			subgraph->critical_path = *path_length;
		}
	}

	// Calculate the total execution time of all tasks by summing from task_execution_times
	void *exec_time_value;
	HASH_TABLE_ITERATE(subgraph->task_execution_times, task_id_str, exec_time_value)
	{
		long *exec_time = (long *)exec_time_value;
		subgraph->total_time += *exec_time;
	}

	// Clean up the critical path hash table
	hash_table_clear(task_critical_paths, free);
	hash_table_delete(task_critical_paths);

	// Print debug information
	debug(D_VINE, "Recovery subgraph for file %s: %d tasks, critical path: %.3f seconds, total time: %.3f seconds", cachename, list_size(subgraph->tasks), subgraph->critical_path / 1000000.0, subgraph->total_time / 1000000.0);

	return subgraph;
}

int get_recovery_subgraph_size(struct recovery_subgraph *subgraph)
{
	return list_size(subgraph->tasks);
}

void free_recovery_subgraph(struct recovery_subgraph *subgraph)
{
	if (subgraph) {
		if (subgraph->tasks) {
			list_delete(subgraph->tasks);
		}
		if (subgraph->task_execution_times) {
			hash_table_clear(subgraph->task_execution_times, free);
			hash_table_delete(subgraph->task_execution_times);
		}
		free(subgraph);
	}
}

long get_task_execution_time(struct recovery_subgraph *subgraph, int task_id)
{
	if (!subgraph || !subgraph->task_execution_times) {
		return 0;
	}

	char task_id_str[32];
	sprintf(task_id_str, "%d", task_id);

	long *exec_time = hash_table_lookup(subgraph->task_execution_times, task_id_str);
	if (exec_time) {
		return *exec_time;
	}

	return 0;
}