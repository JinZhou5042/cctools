#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "debug.h"
#include "macros.h"
#include "priority_queue.h"
#include "progress_bar.h"
#include "set.h"
#include "stringtools.h"
#include "timestamp.h"
#include "vine_graph.h"
#include "vine_graph_executor.h"
#include "xxmalloc.h"

#include "vine_file.h"
#include "vine_manager.h"
#include "vine_manager_data_service.h"
#include "vine_temp.h"

/* Order in which ready nodes are submitted. */
typedef enum {
	VINE_GRAPH_PRIORITY_LARGEST_INPUT_FIRST = 0, /* Largest total input size first. */
	VINE_GRAPH_PRIORITY_DEPTH_FIRST,	     /* Deeper nodes first. */
	VINE_GRAPH_PRIORITY_FIFO		     /* Earlier ready time first. */
} vine_graph_priority_mode_t;

/* One run of a graph on a Manager. */
struct vine_graph_executor {
	struct vine_graph *graph;     // DAG executed by this executor
	struct vine_manager *manager; // TaskVine runtime

	char *library_name;  // library that runs every node, installed by the frontend
	char *function_name; // library function that runs one node

	struct itable *task_id_to_node;		 // maps Manager task id to graph node after submission
	struct priority_queue *checkpoint_queue; // borrowed idata awaiting admission, longest producer first
	struct list *checkpoint_completed;	 // borrowed files whose checkpoint the Manager published
	struct list *checkpoint_failed;		 // borrowed files whose checkpoint failed or was preempted, to requeue
	struct set *checkpoint_pending;		 // queued or in-flight files; release withdraws them
	uint64_t checkpoint_sequence;		 // completion order used to break equal checkpoint priorities

	struct set *fetch_pending;  // borrowed pinned outputs requested at the Manager and not in the vault yet
	struct itable *vault_paths; // vine_file address -> owned path of a fetched output in the vault
	struct list *finished;	    // borrowed nodes that completed or failed and were not reported yet

	vine_graph_priority_mode_t priority_mode; // order of ready nodes
	int max_retries;			  // failed attempts allowed per node before it fails
	int show_progress_bar;			  // draw a progress bar while the run advances
	double progress_bar_update_interval_sec;

	uint64_t submitted_nodes;	       // nodes linked into the graph
	uint64_t completed_nodes;	       // nodes whose own task succeeded
	uint64_t finished_nodes;	       // nodes that completed or failed
	struct ProgressBar *progress_bar;      // drawn while the run advances, or NULL
	struct ProgressBarPart *node_part;     // completed nodes out of submitted nodes
	struct ProgressBarPart *recovery_part; // completed recovery tasks out of submitted ones

	timestamp_t time_first_task_dispatched; // earliest dispatch time among node tasks
	timestamp_t time_last_task_retrieved;	// latest node task retrieval time
	uint64_t completed_recovery_tasks;	// tasks that recomputed outputs of completed nodes

	char *error; // why the run failed as a whole, or NULL
};

/*************************************************************/
/* Lifecycle and settings */
/*************************************************************/

struct vine_graph_executor *vine_graph_executor_create(struct vine_manager *manager, const char *library_name, const char *function_name)
{
	if (!manager || !library_name || !function_name) {
		return NULL;
	}
	struct vine_graph_executor *e = calloc(1, sizeof(*e));
	if (!e) {
		return NULL;
	}
	e->graph = vine_graph_create();
	if (!e->graph) {
		free(e);
		return NULL;
	}
	e->manager = manager;
	e->library_name = xxstrdup(library_name);
	e->function_name = xxstrdup(function_name);
	e->task_id_to_node = itable_create(0);
	e->checkpoint_queue = priority_queue_create(0);
	e->checkpoint_completed = list_create();
	e->checkpoint_failed = list_create();
	e->checkpoint_pending = set_create(0);
	e->fetch_pending = set_create(0);
	e->vault_paths = itable_create(0);
	e->finished = list_create();
	e->priority_mode = VINE_GRAPH_PRIORITY_LARGEST_INPUT_FIRST;
	e->max_retries = 5;
	e->show_progress_bar = 1;
	e->progress_bar_update_interval_sec = 0.1;
	e->time_first_task_dispatched = UINT64_MAX;
	return e;
}

/* Stop only this run's tasks, including Manager-created recovery tasks, before undeclaring their files. */
static void vine_graph_executor_cancel_tasks(struct vine_graph_executor *e)
{
	struct list *tasks = list_create();
	struct vine_task *task;
	uint64_t id;
	int iteration;
	ITABLE_ITERATE(e->manager->tasks, iteration, id, task)
	{
		int source_id = vine_task_get_recovery_source_task_id(task);
		if (itable_lookup(e->task_id_to_node, source_id > 0 ? (uint64_t)source_id : id)) {
			list_push_tail(tasks, vine_task_addref(task));
			vine_cancel_by_task_id(e->manager, vine_task_get_id(task));
		}
	}
	while ((task = list_pop_head(tasks))) {
		vine_wait_for_task_id(e->manager, vine_task_get_id(task), 0);
		vine_task_delete(task);
	}
	list_delete(tasks);
}

/* Finish the progress bar of a run that stopped advancing. */
static void vine_graph_executor_finish_progress_bar(struct vine_graph_executor *e)
{
	if (e->progress_bar) {
		progress_bar_finish(e->progress_bar);
		progress_bar_delete(e->progress_bar);
		e->progress_bar = NULL;
	}
}

/* Forget the vault path of an output whose vault entry is going away. */
static void vine_graph_executor_forget_vault_path(struct vine_graph_executor *e, struct vine_file *file)
{
	free(itable_remove(e->vault_paths, (uint64_t)(uintptr_t)file));
}

void vine_graph_executor_delete(struct vine_graph_executor *e)
{
	if (!e) {
		return;
	}
	vine_graph_executor_finish_progress_bar(e);
	vine_graph_executor_cancel_tasks(e);
	uint64_t file_id;
	struct vine_file *file;
	int iteration;
	ITABLE_ITERATE(e->graph->files, iteration, file_id, file)
	{
		vine_graph_executor_forget_vault_path(e, file);
		/* Declaring a path twice returns the Manager's existing file with one more reference. Whichever id comes
		 * first undeclares the file, and each other id only drops its reference. */
		if (vine_manager_lookup_file(e->manager, vine_file_cached_name(file)) == file) {
			vine_undeclare_file(e->manager, file);
		} else {
			vine_file_delete(file);
		}
	}
	vine_graph_delete(e->graph);
	itable_delete(e->task_id_to_node);
	priority_queue_delete(e->checkpoint_queue);
	list_delete(e->checkpoint_completed);
	list_delete(e->checkpoint_failed);
	set_delete(e->checkpoint_pending);
	set_delete(e->fetch_pending);
	itable_delete(e->vault_paths);
	list_delete(e->finished);
	free(e->library_name);
	free(e->function_name);
	free(e->error);
	free(e);
}

/* Parse a non-negative integer setting. Return zero, or -1 for an invalid value. */
static int vine_graph_parse_count(const char *name, const char *value, int *count)
{
	char *end;
	long parsed = strtol(value, &end, 10);
	if (end == value || *end || parsed < 0 || parsed > INT_MAX) {
		debug(D_ERROR, "invalid %s: %s (must be a non-negative integer)", name, value);
		return -1;
	}
	*count = (int)parsed;
	return 0;
}

int vine_graph_executor_tune(struct vine_graph_executor *e, const char *name, const char *value)
{
	if (!e || !name || !value) {
		return -1;
	}

	if (strcmp(name, "task-priority-mode") == 0) {
		if (strcmp(value, "largest-input-first") == 0) {
			e->priority_mode = VINE_GRAPH_PRIORITY_LARGEST_INPUT_FIRST;
		} else if (strcmp(value, "depth-first") == 0) {
			e->priority_mode = VINE_GRAPH_PRIORITY_DEPTH_FIRST;
		} else if (strcmp(value, "fifo") == 0) {
			e->priority_mode = VINE_GRAPH_PRIORITY_FIFO;
		} else {
			debug(D_ERROR, "invalid task-priority-mode: %s (use largest-input-first, depth-first, or fifo)", value);
			return -1;
		}
		return 0;
	} else if (strcmp(name, "max-retries") == 0) {
		return vine_graph_parse_count(name, value, &e->max_retries);
	} else if (strcmp(name, "progress-bar") == 0) {
		return vine_graph_parse_count(name, value, &e->show_progress_bar);
	} else if (strcmp(name, "progress-bar-update-interval-sec") == 0) {
		double interval = atof(value);
		e->progress_bar_update_interval_sec = interval > 0 ? interval : 0.1;
		return 0;
	}
	debug(D_ERROR, "invalid vine_graph parameter: %s", name);
	return -1;
}

int vine_graph_executor_check_setting(const char *name, const char *value)
{
	/* Tuning only records settings, so a scratch executor applies the same rules without a Manager. */
	struct vine_graph_executor scratch = {0};
	return vine_graph_executor_tune(&scratch, name, value);
}

/*************************************************************/
/* Building the graph */
/*************************************************************/

uint64_t vine_graph_executor_add_node(struct vine_graph_executor *e)
{
	return e ? vine_graph_add_node(e->graph) : 0;
}

uint64_t vine_graph_executor_declare_file(struct vine_graph_executor *e, const char *source_path, int vault)
{
	if (!e || !source_path) {
		return 0;
	}
	struct vine_file *file = vault ? vine_manager_data_service_declare_file(e->manager, source_path) : vine_declare_file(e->manager, source_path, VINE_CACHE_LEVEL_WORKFLOW, 0);
	return file ? vine_graph_add_file(e->graph, file) : 0;
}

uint64_t vine_graph_executor_add_output(struct vine_graph_executor *e, uint64_t node_id, const char *task_path)
{
	struct vine_graph_node *node = e && task_path ? vine_graph_get_node(e->graph, node_id) : NULL;
	if (!node || node->submitted) {
		return 0;
	}
	struct vine_file *file = vine_declare_temp(e->manager);
	if (!file) {
		return 0;
	}
	uint64_t file_id = vine_graph_add_file(e->graph, file);
	return vine_graph_add_mount(e->graph, node_id, file_id, task_path, 1) == 0 ? file_id : 0;
}

int vine_graph_executor_add_input(struct vine_graph_executor *e, uint64_t node_id, uint64_t file_id, const char *task_path)
{
	if (!e || !task_path) {
		return -1;
	}
	return vine_graph_add_mount(e->graph, node_id, file_id, task_path, 0);
}

/*************************************************************/
/* Node tasks */
/*************************************************************/

/* Record why the run failed as a whole. The first reason wins. */
static void vine_graph_executor_fail_run(struct vine_graph_executor *e, char *error)
{
	debug(D_ERROR, "vine_graph run failed: %s", error);
	if (e->error) {
		free(error);
		return;
	}
	e->error = error;
}

/* Compute the submission priority of a ready node. */
static double vine_graph_executor_priority(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	double priority = 0;
	struct vine_graph_mount *mount;
	switch (e->priority_mode) {
	case VINE_GRAPH_PRIORITY_LARGEST_INPUT_FIRST:
		LIST_ITERATE(node->inputs, mount)
		{
			priority += (double)vine_file_size(mount->file);
		}
		break;
	case VINE_GRAPH_PRIORITY_DEPTH_FIRST:
		priority = node->depth;
		break;
	case VINE_GRAPH_PRIORITY_FIFO:
		priority = -(double)timestamp_get();
		break;
	}
	return priority;
}

static void vine_graph_executor_fail_node(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_graph_node *source, char *error);

/* Submit a new task for a node. The task runs the node function with the shared arguments and every mount.
 * Return zero, or -1 after failing the node. */
static int vine_graph_executor_submit_task(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_task *task = vine_task_create(e->function_name);
	if (!task) {
		vine_graph_executor_fail_node(e, node, node, xxstrdup("could not create its task"));
		return -1;
	}
	vine_task_set_library_required(task, e->library_name);
	int mounted = 1;
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->inputs, mount)
	{
		mounted = mounted && vine_task_add_input(task, mount->file, mount->task_path, VINE_TRANSFER_ALWAYS);
	}
	LIST_ITERATE(node->outputs, mount)
	{
		mounted = mounted && vine_task_add_output(task, mount->file, mount->task_path, VINE_TRANSFER_ALWAYS);
	}
	vine_task_set_priority(task, vine_graph_executor_priority(e, node));
	int task_id = mounted ? vine_submit(e->manager, task) : 0;
	if (task_id <= 0) {
		vine_task_delete(task);
		vine_graph_executor_fail_node(e, node, node, xxstrdup("could not submit its task"));
		return -1;
	}

	/* The Manager keeps its own reference while the task is active. A finished attempt is no longer needed. */
	vine_task_delete(node->task);
	node->task = task;
	itable_insert(e->task_id_to_node, (uint64_t)task_id, node);
	debug(D_VINE, "submitted node %" PRIu64 " as task %d", node->node_id, task_id);
	return 0;
}

/* Map a returned task to its node. A recovery task maps to the node whose task it copies. */
static struct vine_graph_node *vine_graph_executor_node_from_task(struct vine_graph_executor *e, struct vine_task *task)
{
	int task_id = vine_task_get_recovery_source_task_id(task);
	if (task_id <= 0) {
		task_id = vine_task_get_id(task);
	}
	return task_id > 0 ? itable_lookup(e->task_id_to_node, (uint64_t)task_id) : NULL;
}

/* After a node completes, submit each child whose parents have now all completed. The ready children are collected
 * first, because a failed submission walks the node's children again and LIST_ITERATE keeps its position in the list. */
static void vine_graph_executor_submit_children(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct list *ready = list_create();
	struct vine_graph_node *child;
	LIST_ITERATE(node->children, child)
	{
		if (--child->remaining_parents == 0 && !child->task && !child->failed) {
			list_push_tail(ready, child);
		}
	}
	while ((child = list_pop_head(ready))) {
		vine_graph_executor_submit_task(e, child);
	}
	list_delete(ready);
}

/*************************************************************/
/* Release and checkpoints */
/*************************************************************/

/* Return non-zero while a recovery task is recreating one of the node's temporary outputs. */
static int vine_graph_node_is_recovering(const struct vine_graph_node *node)
{
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->outputs, mount)
	{
		if (vine_file_type(mount->file) == VINE_TEMP && vine_file_is_recovering(mount->file)) {
			return 1;
		}
	}
	return 0;
}

/* Return non-zero when the node completed and every output has a readable Manager-local copy. */
static int vine_graph_executor_node_is_anchored(struct vine_graph_executor *e, const struct vine_graph_node *node)
{
	if (!node->completed) {
		return 0;
	}
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->outputs, mount)
	{
		if (!vine_manager_data_service_has_local_file(e->manager, mount->file)) {
			return 0;
		}
	}
	return 1;
}

/* Push a pending file into the checkpoint queue, ordered by producer execution time with the longest first.
 * A fraction below one microsecond breaks ties in queue order without reordering distinct execution times. */
static void vine_graph_executor_push_checkpoint(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_file *file)
{
	double priority = (double)node->execution_time_us + 1.0 / (2.0 + (double)e->checkpoint_sequence++);
	priority_queue_push(e->checkpoint_queue, file, priority);
	debug(D_VINE, "checkpoint queued: %s node %" PRIu64 " runtime_us=%" PRIu64 " priority=%.9f", vine_file_cached_name(file), node->node_id, node->execution_time_us, priority);
}

/* Queue a temporary output for retrieval into the vault unless it is already queued, in flight, or published. */
static void vine_graph_executor_queue_checkpoint(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_file *file)
{
	if (vine_file_type(file) != VINE_TEMP || set_lookup(e->checkpoint_pending, file) || vine_manager_data_service_vault_contains(e->manager, file)) {
		return;
	}
	set_insert(e->checkpoint_pending, file);
	vine_graph_executor_push_checkpoint(e, node, file);
}

/* Withdraw a released file from checkpointing. Queued files leave the queue. In-flight receives are cancelled by the
 * Manager when the file is pruned, and their results are ignored because the file is no longer pending. */
static void vine_graph_executor_forget_checkpoint(struct vine_graph_executor *e, struct vine_file *file)
{
	if (!set_remove(e->checkpoint_pending, file)) {
		return;
	}
	debug(D_VINE, "checkpoint withdrawn: %s", vine_file_cached_name(file));
	int index = priority_queue_find_idx(e->checkpoint_queue, file);
	if (index >= 0) {
		priority_queue_remove(e->checkpoint_queue, index);
	}
}

/* Return non-zero when a lost output of this node would never need the node's parents: the node failed and will never
 * read them, or it completed, is not being recovered, and its outputs are either released or anchored in the vault. */
static int vine_graph_executor_node_is_durable(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (node->failed) {
		return 1;
	}
	return node->completed && !vine_graph_node_is_recovering(node) && (node->released || vine_graph_executor_node_is_anchored(e, node));
}

/* Prune the node's temporary outputs once every submitted child is durable and the frontend pins none of them. A lost
 * output then needs at most its parent, never a chain of released ancestors. A consumer submitted after the release
 * makes TaskVine recompute the output, so pins keep outputs that may still be read. Pruning wins over checkpointing.
 * Return non-zero when the node was released. */
static int vine_graph_executor_try_release(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (node->released || !node->completed) {
		return 0;
	}
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->outputs, mount)
	{
		if (mount->pins > 0) {
			return 0;
		}
	}
	struct vine_graph_node *child;
	LIST_ITERATE(node->children, child)
	{
		if (!vine_graph_executor_node_is_durable(e, child)) {
			return 0;
		}
	}
	LIST_ITERATE(node->outputs, mount)
	{
		vine_graph_executor_forget_checkpoint(e, mount->file);
		vine_graph_executor_forget_vault_path(e, mount->file);
		vine_prune_file(e->manager, mount->file);
	}
	node->released = 1;
	debug(D_VINE, "released node %" PRIu64, node->node_id);
	return 1;
}

/* After a node becomes durable or releasable, release it and walk upstream while ancestors become releasable.
 * A recovery task runs only because a consumer needs its output now, so with release_self unset the node itself is
 * kept and only its ancestors are considered. The consumer's completion releases it later. */
static void vine_graph_executor_release_from(struct vine_graph_executor *e, struct vine_graph_node *start, int release_self)
{
	if (release_self) {
		vine_graph_executor_try_release(e, start);
	}
	struct list *worklist = list_create();
	struct vine_graph_node *parent;
	LIST_ITERATE(start->parents, parent)
	{
		list_push_tail(worklist, parent);
	}
	struct vine_graph_node *node;
	while ((node = list_pop_head(worklist))) {
		if (vine_graph_executor_try_release(e, node)) {
			LIST_ITERATE(node->parents, parent)
			{
				list_push_tail(worklist, parent);
			}
		}
	}
	list_delete(worklist);
}

/* Record a checkpoint or fetch result from the Manager thread. Queue and graph changes wait for the executor loop. */
static void vine_graph_executor_checkpoint_complete(void *argument, struct vine_file *file, int success)
{
	struct vine_graph_executor *e = argument;
	list_push_tail(success ? e->checkpoint_completed : e->checkpoint_failed, file);
	/* The Manager's wait returns, so the executor reports a fetched output and fills the freed slot at once. */
	vine_wake(e->manager);
}

/* Settle reported results, then admit queued files in priority order until the Manager refuses.
 * BUSY means no slot or vault space, so admission stops. A file without a ready source is set aside for this round
 * so it does not block lower-priority files. Results of released files are ignored. */
static void vine_graph_executor_process_checkpoints(struct vine_graph_executor *e)
{
	struct vine_file *file;
	while ((file = list_pop_head(e->checkpoint_completed))) {
		if (set_remove(e->checkpoint_pending, file)) {
			/* A published checkpoint anchors its node, which may let its ancestors be released. */
			vine_graph_executor_release_from(e, vine_graph_get_producer(e->graph, file), 0);
		}
	}
	while ((file = list_pop_head(e->checkpoint_failed))) {
		if (set_lookup(e->checkpoint_pending, file)) {
			vine_graph_executor_push_checkpoint(e, vine_graph_get_producer(e->graph, file), file);
		}
	}

	enum { SKIP_LIMIT = 16 };
	struct vine_file *skipped[SKIP_LIMIT];
	double skipped_priority[SKIP_LIMIT];
	int skips = 0;
	while (skips < SKIP_LIMIT && (file = priority_queue_peek_top(e->checkpoint_queue))) {
		double priority = priority_queue_get_top_priority(e->checkpoint_queue);
		vine_manager_checkpoint_result_t result = vine_manager_data_service_checkpoint(e->manager, file, vine_graph_executor_checkpoint_complete, e);
		if (result == VINE_CHECKPOINT_BUSY) {
			break;
		}
		priority_queue_pop(e->checkpoint_queue);
		if (result == VINE_CHECKPOINT_NO_SOURCE) {
			skipped[skips] = file;
			skipped_priority[skips] = priority;
			skips++;
		}
	}
	for (int i = 0; i < skips; i++) {
		priority_queue_push(e->checkpoint_queue, skipped[i], skipped_priority[i]);
	}
}

/*************************************************************/
/* Failures */
/*************************************************************/

/* Cancel the active task of a node, if any. The Manager returns it later, and the executor ignores it. */
static void vine_graph_executor_cancel_node_task(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (node->task && itable_lookup(e->manager->tasks, (uint64_t)vine_task_get_id(node->task))) {
		vine_cancel_by_task_id(e->manager, vine_task_get_id(node->task));
	}
}

/* Fail a node because of source, the node whose own failure explains it, and fail every unfinished node that depends
 * on it. error is set only when node is the source. A completed node fails when its outputs cannot be restored. Its
 * completed children keep their own outputs. Failed nodes no longer hold back the release of their parents. */
static void vine_graph_executor_fail_node(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_graph_node *source, char *error)
{
	if (node->failed) {
		free(error);
		return;
	}
	if (node == source) {
		node->error = error;
		debug(D_VINE, "node %" PRIu64 " failed: %s", node->node_id, error);
	}
	struct list *failed = list_create();
	struct list *worklist = list_create();
	list_push_tail(worklist, node);
	struct vine_graph_node *current;
	while ((current = list_pop_head(worklist))) {
		if (current->failed) {
			continue;
		}
		current->failed = 1;
		current->failure_source = source;
		current->restoring = 0;
		if (!current->completed) {
			e->finished_nodes++;
		}
		if (current != node) {
			vine_graph_executor_cancel_node_task(e, current);
		}
		list_push_tail(e->finished, current);
		list_push_tail(failed, current);
		struct vine_graph_mount *mount;
		LIST_ITERATE(current->outputs, mount)
		{
			set_remove(e->fetch_pending, mount->file);
		}
		struct vine_graph_node *child;
		LIST_ITERATE(current->children, child)
		{
			if (!child->completed) {
				list_push_tail(worklist, child);
			}
		}
	}
	while ((current = list_pop_head(failed))) {
		vine_graph_executor_release_from(e, current, 0);
	}
	list_delete(worklist);
	list_delete(failed);
}

/* Return the first Manager-local input of a node whose source is no longer readable, or NULL.
 * Edata and user input files are VINE_FILEs that cannot be recomputed. */
static struct vine_file *vine_graph_executor_find_lost_input(struct vine_graph_node *node)
{
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->inputs, mount)
	{
		if (vine_file_type(mount->file) == VINE_FILE && access(vine_file_source(mount->file), R_OK) != 0) {
			return mount->file;
		}
	}
	return NULL;
}

/* Return non-zero when the node function ran and exited with an error, such as an exception. Its outputs are then
 * missing only because of that error. */
static int vine_graph_task_function_failed(struct vine_task *task)
{
	vine_result_t result = vine_task_get_result(task);
	return vine_task_get_exit_code(task) > 0 && (result == VINE_RESULT_SUCCESS || result == VINE_RESULT_OUTPUT_MISSING);
}

/* Return non-zero when a task that succeeded left one of the node's outputs uncreated. A Worker reports a missing
 * temporary output only by invalidating its cache entry, so the task itself still succeeds. Each new task of the node
 * resets its outputs to pending, and the Manager processes the Worker's cache updates before returning the task. */
static int vine_graph_node_output_missing(const struct vine_graph_node *node)
{
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->outputs, mount)
	{
		if (mount->file->state != VINE_FILE_STATE_CREATED) {
			return 1;
		}
	}
	return 0;
}

/* Handle a failed attempt with its result. A function that raised, a lost Manager-local input, or too many failures
 * fail the node and its dependents. Other failures are infrastructure failures: the executor resubmits its own task,
 * while the Manager resubmits a recovery task itself when a consumer needs its output again. */
static void vine_graph_executor_handle_failure(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_task *task, vine_result_t result, int is_recovery)
{
	if (vine_graph_task_function_failed(task)) {
		const char *output = vine_task_get_stdout(task);
		vine_graph_executor_fail_node(e, node, node, string_format("its function failed:\n%s", output ? output : ""));
		return;
	}
	struct vine_file *lost = result == VINE_RESULT_INPUT_MISSING ? vine_graph_executor_find_lost_input(node) : NULL;
	if (lost) {
		vine_graph_executor_fail_node(e, node, node, string_format("its input %s is missing at the Manager", vine_file_source(lost)));
		return;
	}
	if (++node->failures > e->max_retries) {
		vine_graph_executor_fail_node(e, node, node, string_format("it failed %d times; last result: %s", node->failures, vine_result_string(result)));
		return;
	}
	debug(D_VINE, "node %" PRIu64 " %s %d failed (%s), failure %d of at most %d", node->node_id, is_recovery ? "recovery task" : "task", vine_task_get_id(task), vine_result_string(result), node->failures, e->max_retries);
	if (!is_recovery) {
		vine_graph_executor_submit_task(e, node);
	}
}

/* Handle a successful attempt. Every temporary output is queued for the vault. A task that restores the outputs of a
 * completed node runs only because they are needed now, so only its ancestors may be released. The node's own task
 * completes the node, releases what it no longer needs, and submits the children it unblocks. */
static void vine_graph_executor_handle_success(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_task *task, int restores)
{
	node->execution_time_us = (uint64_t)vine_task_get_metric(task, "time_workers_execute_last");
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->outputs, mount)
	{
		vine_graph_executor_queue_checkpoint(e, node, mount->file);
	}

	if (restores) {
		e->completed_recovery_tasks++;
		node->released = 0;
		node->restoring = 0;
		vine_graph_executor_release_from(e, node, 0);
		return;
	}
	e->time_last_task_retrieved = MAX(e->time_last_task_retrieved, (timestamp_t)vine_task_get_metric(task, "time_when_retrieval"));
	node->completed = 1;
	e->completed_nodes++;
	e->finished_nodes++;
	list_push_tail(e->finished, node);
	vine_graph_executor_release_from(e, node, 1);
	vine_graph_executor_submit_children(e, node);
}

/* Handle one task returned by the Manager. */
static void vine_graph_executor_handle_task(struct vine_graph_executor *e, struct vine_task *task)
{
	struct vine_graph_node *node = vine_graph_executor_node_from_task(e, task);
	if (!node) {
		vine_graph_executor_fail_run(e, string_format("task %d does not belong to this graph", vine_task_get_id(task)));
		return;
	}
	if (node->failed) {
		return; // a task cancelled when the node failed
	}
	timestamp_t commit_end = vine_task_get_metric(task, "time_when_commit_end");
	if (commit_end > 0) {
		e->time_first_task_dispatched = MIN(e->time_first_task_dispatched, commit_end);
	}

	int is_recovery = vine_task_get_recovery_source_task_id(task) > 0;
	int restores = is_recovery || node->completed;
	vine_result_t result = vine_task_get_result(task);
	if (result == VINE_RESULT_SUCCESS && vine_task_get_exit_code(task) == 0 && vine_graph_node_output_missing(node)) {
		result = VINE_RESULT_OUTPUT_MISSING;
	}
	if (result != VINE_RESULT_SUCCESS || vine_task_get_exit_code(task) != 0) {
		vine_graph_executor_handle_failure(e, node, task, result, is_recovery);
		return;
	}
	if (!restores && e->progress_bar && e->completed_nodes == 0) {
		progress_bar_set_start_time(e->progress_bar, vine_task_get_metric(task, "time_when_commit_start"));
	}
	vine_graph_executor_handle_success(e, node, task, restores);
	if (e->progress_bar) {
		progress_bar_update_part(e->progress_bar, restores ? e->recovery_part : e->node_part, 1);
	}
}

/*************************************************************/
/* Submission, pins, and fetches */
/*************************************************************/

int vine_graph_executor_submit_node(struct vine_graph_executor *e, uint64_t node_id)
{
	if (!e || e->error || vine_graph_link_node(e->graph, node_id) != 0) {
		return -1;
	}
	e->submitted_nodes++;
	struct vine_graph_node *node = vine_graph_get_node(e->graph, node_id);
	struct vine_graph_node *parent;
	struct vine_graph_node *failed_parent = NULL;
	LIST_ITERATE(node->parents, parent)
	{
		if (parent->failed) {
			failed_parent = parent;
		}
	}
	if (failed_parent) {
		vine_graph_executor_fail_node(e, node, failed_parent->failure_source, NULL);
	} else if (node->remaining_parents == 0) {
		vine_graph_executor_submit_task(e, node);
	}
	return 0;
}

/* Add delta to the pins of a node output. Return the output's mount, or NULL for an unknown output or an unpin
 * without a pin. */
static struct vine_graph_mount *vine_graph_executor_add_pins(struct vine_graph_executor *e, uint64_t file_id, int delta)
{
	struct vine_graph_mount *mount = e ? vine_graph_get_output(e->graph, file_id) : NULL;
	if (!mount || mount->pins + delta < 0) {
		debug(D_ERROR, "file %" PRIu64 " is not a node output, or it is not pinned", file_id);
		return NULL;
	}
	mount->pins += delta;
	return mount;
}

int vine_graph_executor_pin_file(struct vine_graph_executor *e, uint64_t file_id)
{
	return vine_graph_executor_add_pins(e, file_id, 1) ? 0 : -1;
}

int vine_graph_executor_unpin_file(struct vine_graph_executor *e, uint64_t file_id)
{
	struct vine_graph_mount *mount = vine_graph_executor_add_pins(e, file_id, -1);
	if (!mount) {
		return -1;
	}
	if (mount->pins == 0) {
		set_remove(e->fetch_pending, mount->file);
		/* The last pin may have held back the producer and, through it, its ancestors. */
		vine_graph_executor_release_from(e, vine_graph_get_producer(e->graph, mount->file), 1);
	}
	return 0;
}

int vine_graph_executor_fetch_file(struct vine_graph_executor *e, uint64_t file_id)
{
	struct vine_graph_mount *mount = e ? vine_graph_get_output(e->graph, file_id) : NULL;
	struct vine_graph_node *producer = mount ? vine_graph_get_producer(e->graph, mount->file) : NULL;
	if (!producer || !producer->submitted || mount->pins == 0) {
		debug(D_ERROR, "file %" PRIu64 " is not a pinned output of a submitted node", file_id);
		return -1;
	}
	if (!producer->failed && !vine_manager_data_service_vault_contains(e->manager, mount->file)) {
		set_insert(e->fetch_pending, mount->file);
	}
	return 0;
}

/* Advance every pending fetch. A file in the vault is done. A file with a Worker replica is received into the vault
 * with priority over checkpoints and beyond the vault limit. A completed node whose output exists nowhere is run again
 * to restore it, unless a recovery task is already recreating it. */
static void vine_graph_executor_process_fetches(struct vine_graph_executor *e)
{
	struct list *pending = list_create();
	struct vine_file *file;
	int iteration;
	SET_ITERATE(e->fetch_pending, iteration, file)
	{
		list_push_tail(pending, file);
	}
	while ((file = list_pop_head(pending))) {
		if (vine_manager_data_service_vault_contains(e->manager, file)) {
			set_remove(e->fetch_pending, file);
			continue;
		}
		struct vine_graph_node *producer = vine_graph_get_producer(e->graph, file);
		if (!producer->completed || producer->restoring) {
			continue;
		}
		vine_manager_checkpoint_result_t result = vine_manager_data_service_fetch(e->manager, file, vine_graph_executor_checkpoint_complete, e);
		if (result == VINE_CHECKPOINT_NO_SOURCE && !vine_temp_exists_somewhere(e->manager, file) && !vine_file_is_recovering(file)) {
			debug(D_VINE, "node %" PRIu64 " runs again to restore %s", producer->node_id, vine_file_cached_name(file));
			producer->restoring = 1;
			vine_graph_executor_submit_task(e, producer);
		}
	}
	list_delete(pending);
}

const char *vine_graph_executor_get_local_path(struct vine_graph_executor *e, uint64_t file_id)
{
	struct vine_file *file = e ? vine_graph_get_file(e->graph, file_id) : NULL;
	if (!file) {
		return NULL;
	}
	if (!vine_graph_get_producer(e->graph, file)) {
		return vine_file_source(file);
	}
	uint64_t key = (uint64_t)(uintptr_t)file;
	char *path = itable_lookup(e->vault_paths, key);
	if (!path && (path = vine_manager_data_service_vault_path(e->manager, file))) {
		itable_insert(e->vault_paths, key, path);
	}
	return path;
}

/*************************************************************/
/* Advancing the run */
/*************************************************************/

/* Draw a progress bar over the submitted nodes, which grow as the graph grows. */
static void vine_graph_executor_update_progress_bar(struct vine_graph_executor *e)
{
	if (!e->show_progress_bar) {
		return;
	}
	if (!e->progress_bar) {
		e->progress_bar = progress_bar_init("Executing Tasks");
		progress_bar_set_update_interval(e->progress_bar, e->progress_bar_update_interval_sec);
		e->node_part = progress_bar_create_part("User", e->submitted_nodes);
		e->recovery_part = progress_bar_create_part("Recovery", 0);
		progress_bar_bind_part(e->progress_bar, e->node_part);
		progress_bar_bind_part(e->progress_bar, e->recovery_part);
	}
	struct vine_stats stats;
	vine_get_stats(e->manager, &stats);
	progress_bar_set_part_total(e->progress_bar, e->node_part, e->submitted_nodes);
	progress_bar_set_part_total(e->progress_bar, e->recovery_part, stats.tasks_recovery);
}

vine_graph_status_t vine_graph_executor_wait(struct vine_graph_executor *e, int timeout)
{
	if (!e) {
		return VINE_GRAPH_FAILED;
	}
	/* Recovery completions are returned to the executor, which releases ancestors behind them. */
	vine_enable_external_recovery_handling(e->manager);

	time_t stoptime = time(NULL) + MAX(timeout, 0);
	int wait_timeout = 0;
	int fetches = set_size(e->fetch_pending);
	int expired = 0;
	while (!e->error && (e->finished_nodes < e->submitted_nodes || set_size(e->fetch_pending) > 0)) {
		/* TaskVine removes a library that keeps failing to start, and then no node can run. */
		if (!vine_manager_find_library_template(e->manager, e->library_name)) {
			vine_graph_executor_fail_run(e, string_format("library %s failed to start on workers too many times and was removed; check that workers can run it, and set the manager tuning watch-library-logfiles to 1 to keep its logs", e->library_name));
			break;
		}
		vine_graph_executor_process_checkpoints(e);
		vine_graph_executor_process_fetches(e);
		vine_graph_executor_update_progress_bar(e);
		/* The caller learns at once that a node finished or that a fetched output became readable. Results that
		 * arrived during the last Manager wait are settled above before any return. */
		if (expired || list_size(e->finished) > 0 || set_size(e->fetch_pending) < fetches) {
			return VINE_GRAPH_RUNNING;
		}
		/* Drain returned tasks without blocking, then block once for the rest of the timeout. A blocking wait that
		 * returns no task expired or was ended by vine_graph_executor_wake(). */
		struct vine_task *task = vine_wait(e->manager, wait_timeout);
		if (task) {
			vine_graph_executor_handle_task(e, task);
			wait_timeout = 0;
		} else if (wait_timeout > 0 || time(NULL) >= stoptime) {
			expired = 1;
		} else {
			wait_timeout = MAX(stoptime - time(NULL), 1);
		}
	}

	vine_graph_executor_finish_progress_bar(e);
	return e->error ? VINE_GRAPH_FAILED : VINE_GRAPH_DONE;
}

void vine_graph_executor_wake(struct vine_graph_executor *e)
{
	vine_wake(e->manager);
}

/*************************************************************/
/* Queries */
/*************************************************************/

uint64_t vine_graph_executor_next_finished(struct vine_graph_executor *e)
{
	struct vine_graph_node *node = e ? list_pop_head(e->finished) : NULL;
	return node ? node->node_id : 0;
}

vine_graph_node_state_t vine_graph_executor_get_node_state(const struct vine_graph_executor *e, uint64_t node_id)
{
	struct vine_graph_node *node = e ? vine_graph_get_node(e->graph, node_id) : NULL;
	if (node && node->failed) {
		return VINE_GRAPH_NODE_FAILED;
	}
	return node && node->completed ? VINE_GRAPH_NODE_COMPLETED : VINE_GRAPH_NODE_WAITING;
}

uint64_t vine_graph_executor_get_failure_source(const struct vine_graph_executor *e, uint64_t node_id)
{
	struct vine_graph_node *node = e ? vine_graph_get_node(e->graph, node_id) : NULL;
	return node && node->failed ? node->failure_source->node_id : 0;
}

const char *vine_graph_executor_get_node_error(const struct vine_graph_executor *e, uint64_t node_id)
{
	struct vine_graph_node *node = e ? vine_graph_get_node(e->graph, node_id) : NULL;
	return node && node->failed ? node->failure_source->error : NULL;
}

const char *vine_graph_executor_get_error(const struct vine_graph_executor *e)
{
	return e ? e->error : NULL;
}

uint64_t vine_graph_executor_get_makespan_us(const struct vine_graph_executor *e)
{
	if (!e || e->time_last_task_retrieved < e->time_first_task_dispatched) {
		return 0;
	}
	return e->time_last_task_retrieved - e->time_first_task_dispatched;
}

uint64_t vine_graph_executor_get_completed_recovery_tasks(const struct vine_graph_executor *e)
{
	return e ? e->completed_recovery_tasks : 0;
}

int vine_graph_executor_cancel_node(struct vine_graph_executor *e, uint64_t node_id)
{
	struct vine_graph_node *node = e ? vine_graph_get_node(e->graph, node_id) : NULL;
	if (!node || !node->submitted || node->completed || node->failed) {
		return -1;
	}
	vine_graph_executor_cancel_node_task(e, node);
	vine_graph_executor_fail_node(e, node, node, xxstrdup("it was cancelled"));
	return 0;
}
