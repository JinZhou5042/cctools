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

	struct itable *task_id_to_node;		 // maps Manager task id to the id of its graph node after submission
	struct priority_queue *checkpoint_queue; // borrowed idata awaiting admission, longest producer first
	struct list *checkpoint_completed;	 // borrowed files whose checkpoint the Manager published
	struct list *checkpoint_failed;		 // borrowed files whose checkpoint failed, to requeue
	timestamp_t checkpoint_retry_time;	 // when failed files are requeued, so a lost source is not retried at once
	struct set *checkpoint_pending;		 // queued or in-flight files; release withdraws them, and the queue drops
						 // files that are no longer pending when they reach its head
	struct set *checkpoint_parked;		 // pending files with no replica anywhere, queued again once recreated
	uint64_t checkpoint_sequence;		 // completion order used to break equal checkpoint priorities

	struct set *fetch_pending;  // borrowed pinned outputs requested at the Manager and not in the vault yet
	struct list *fetch_ready;   // borrowed pending fetches to try now, queued when one can make progress, maybe twice
	struct itable *vault_paths; // vine_file address -> owned path of a fetched output in the vault
	struct list *finished;	    // nodes that completed or failed and were not reported yet, counted in their refs
	struct list *released;	    // nodes whose outputs were released and were not reported yet, counted in their refs
	struct list *removable;	    // ids of nodes let go by the frontend that may have become removable
	struct list *removed;	    // ids of removed nodes that were not reported yet
	int vault_full_noticed;	    // the user was told once that the vault is full

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
	e->checkpoint_parked = set_create(0);
	e->fetch_pending = set_create(0);
	e->fetch_ready = list_create();
	e->vault_paths = itable_create(0);
	e->finished = list_create();
	e->released = list_create();
	e->removable = list_create();
	e->removed = list_create();
	e->priority_mode = VINE_GRAPH_PRIORITY_LARGEST_INPUT_FIRST;
	e->max_retries = 5;
	e->show_progress_bar = 1;
	e->progress_bar_update_interval_sec = 0.1;
	e->time_first_task_dispatched = UINT64_MAX;
	return e;
}

/* Delete a recovery task that the Manager returned, whose reference the executor receives. */
static void vine_graph_executor_release_returned(struct vine_task *task)
{
	if (task && vine_task_get_recovery_source_task_id(task) > 0) {
		vine_task_delete(task);
	}
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
	/* The Manager puts each cancelled task at the head of its retrieved list, so the newest is collected first. */
	while ((task = list_pop_tail(tasks))) {
		vine_graph_executor_release_returned(vine_wait_for_task_id(e->manager, vine_task_get_id(task), 0));
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
	/* Report entries keep removed nodes until the graph frees the nodes that remain. */
	struct vine_graph_node *node;
	while ((node = list_pop_head(e->finished))) {
		vine_graph_node_unref(node);
	}
	while ((node = list_pop_head(e->released))) {
		vine_graph_node_unref(node);
	}
	vine_graph_delete(e->graph);
	itable_delete(e->task_id_to_node);
	/* Queued files hold references of their own. */
	while ((file = priority_queue_pop(e->checkpoint_queue))) {
		vine_file_delete(file);
	}
	priority_queue_delete(e->checkpoint_queue);
	struct list *queued[] = {e->checkpoint_completed, e->checkpoint_failed, e->fetch_ready};
	for (size_t i = 0; i < sizeof(queued) / sizeof(queued[0]); i++) {
		while ((file = list_pop_head(queued[i]))) {
			vine_file_delete(file);
		}
		list_delete(queued[i]);
	}
	set_delete(e->checkpoint_pending);
	set_delete(e->checkpoint_parked);
	set_delete(e->fetch_pending);
	itable_delete(e->vault_paths);
	list_delete(e->finished);
	list_delete(e->released);
	list_delete(e->removable);
	list_delete(e->removed);
	free(e->library_name);
	free(e->function_name);
	free(e->error);
	free(e);
}

/* Parse a non-negative integer setting. Return NULL, or what the setting accepts for an invalid value. */
static const char *vine_graph_parse_count(const char *value, int *count)
{
	char *end;
	long parsed = strtol(value, &end, 10);
	if (end == value || *end || parsed < 0 || parsed > INT_MAX) {
		return "it must be a non-negative integer";
	}
	*count = (int)parsed;
	return NULL;
}

/* Apply a setting. Return NULL, or a static explanation of what the setting accepts. */
static const char *vine_graph_executor_apply_setting(struct vine_graph_executor *e, const char *name, const char *value)
{
	if (strcmp(name, "task-priority-mode") == 0) {
		if (strcmp(value, "largest-input-first") == 0) {
			e->priority_mode = VINE_GRAPH_PRIORITY_LARGEST_INPUT_FIRST;
		} else if (strcmp(value, "depth-first") == 0) {
			e->priority_mode = VINE_GRAPH_PRIORITY_DEPTH_FIRST;
		} else if (strcmp(value, "fifo") == 0) {
			e->priority_mode = VINE_GRAPH_PRIORITY_FIFO;
		} else {
			return "use largest-input-first, depth-first, or fifo";
		}
		return NULL;
	} else if (strcmp(name, "max-retries") == 0) {
		return vine_graph_parse_count(value, &e->max_retries);
	} else if (strcmp(name, "progress-bar") == 0) {
		return vine_graph_parse_count(value, &e->show_progress_bar);
	} else if (strcmp(name, "progress-bar-update-interval-sec") == 0) {
		double interval = atof(value);
		e->progress_bar_update_interval_sec = interval > 0 ? interval : 0.1;
		return NULL;
	}
	return "it is not a vine_graph setting";
}

int vine_graph_executor_tune(struct vine_graph_executor *e, const char *name, const char *value)
{
	if (!e || !name || !value) {
		return -1;
	}
	const char *problem = vine_graph_executor_apply_setting(e, name, value);
	if (problem) {
		debug(D_VINE, "invalid value %s for %s: %s", value, name, problem);
		return -1;
	}
	return 0;
}

const char *vine_graph_executor_check_setting(const char *name, const char *value)
{
	if (!name || !value) {
		return "a setting needs a name and a value";
	}
	/* Tuning only records settings, so a scratch executor applies the same rules without a Manager. */
	struct vine_graph_executor scratch = {0};
	return vine_graph_executor_apply_setting(&scratch, name, value);
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

int vine_graph_executor_undeclare_file(struct vine_graph_executor *e, uint64_t file_id)
{
	struct vine_file *file = e ? vine_graph_get_file(e->graph, file_id) : NULL;
	if (!file || vine_graph_get_producer(e->graph, file)) {
		return -1;
	}
	vine_graph_remove_file(e->graph, file_id);
	vine_graph_executor_forget_vault_path(e, file);
	/* As in vine_graph_executor_delete, the first id of a path declared twice undeclares the shared file. */
	if (vine_manager_lookup_file(e->manager, vine_file_cached_name(file)) == file) {
		vine_undeclare_file(e->manager, file);
	} else {
		vine_file_delete(file);
	}
	return 0;
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

static void vine_graph_executor_fail_node(struct vine_graph_executor *e, struct vine_graph_node *node, uint64_t source_id, char *error);
static struct vine_graph_node *vine_graph_executor_find_failed_parent(struct vine_graph_executor *e, struct vine_graph_node *node);

/* Queue a node for a report to the frontend. The entry keeps the node's structure until the report. */
static void vine_graph_executor_report(struct list *reports, struct vine_graph_node *node)
{
	node->refs++;
	list_push_tail(reports, node);
}

/* Return the id of the next reported node that was not removed meanwhile, or zero. */
static uint64_t vine_graph_executor_next_report(struct list *reports)
{
	struct vine_graph_node *node;
	while ((node = list_pop_head(reports))) {
		uint64_t node_id = node->removed ? 0 : node->node_id;
		vine_graph_node_unref(node);
		if (node_id) {
			return node_id;
		}
	}
	return 0;
}

/* Submit a new task for a node. The task runs the node function with the shared arguments and every mount. A node
 * that reads an output of a failed parent that exists nowhere fails instead, because its task would wait forever.
 * Return zero, or -1 after failing the node. */
static int vine_graph_executor_submit_task(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph_node *failed_parent = vine_graph_executor_find_failed_parent(e, node);
	if (failed_parent) {
		vine_graph_executor_fail_node(e, node, failed_parent->failure_source_id, NULL);
		return -1;
	}
	struct vine_task *task = vine_task_create(e->function_name);
	if (!task) {
		vine_graph_executor_fail_node(e, node, node->node_id, xxstrdup("could not create its task"));
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
		vine_graph_executor_fail_node(e, node, node->node_id, xxstrdup("could not submit its task"));
		return -1;
	}

	/* The Manager keeps its own reference while the task is active. A finished attempt is no longer needed. */
	vine_task_delete(node->task);
	node->task = task;
	node->task_id = task_id;
	itable_insert(e->task_id_to_node, (uint64_t)task_id, (void *)(uintptr_t)node->node_id);
	debug(D_VINE, "submitted node %" PRIu64 " as task %d", node->node_id, task_id);
	return 0;
}

/* Map a returned task to the id of its node, or zero for a task of no node. A recovery task maps to the node whose task
 * it copies. */
static uint64_t vine_graph_executor_node_id_from_task(struct vine_graph_executor *e, struct vine_task *task)
{
	int task_id = vine_task_get_recovery_source_task_id(task);
	if (task_id <= 0) {
		task_id = vine_task_get_id(task);
	}
	return task_id > 0 ? (uint64_t)(uintptr_t)itable_lookup(e->task_id_to_node, (uint64_t)task_id) : 0;
}

/* After a node completes, submit each child whose parents have now all completed. The ready children are collected
 * first, because a failed submission walks the node's children again and LIST_ITERATE keeps its position in the list. */
static void vine_graph_executor_submit_children(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct list *ready = list_create();
	struct vine_graph_node *child;
	LIST_ITERATE(node->children, child)
	{
		if (--child->remaining_parents == 0 && !child->task_id && !child->failed && !child->removed) {
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
 * A fraction below one microsecond breaks ties in queue order without reordering distinct execution times. Each entry
 * holds a reference to its file, so a file withdrawn when its node is removed stays valid until it leaves the queue. */
static void vine_graph_executor_push_checkpoint(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_file *file)
{
	double priority = (double)node->execution_time_us + 1.0 / (2.0 + (double)e->checkpoint_sequence++);
	priority_queue_push(e->checkpoint_queue, vine_file_addref(file), priority);
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

/* Withdraw a released file from checkpointing. A queued file stays in the queue until it reaches the head, which drops
 * it then, so a withdrawal costs no search. In-flight receives are cancelled by the Manager when the file is pruned,
 * and their results are ignored because the file is no longer pending. */
static void vine_graph_executor_forget_checkpoint(struct vine_graph_executor *e, struct vine_file *file)
{
	set_remove(e->checkpoint_parked, file);
	if (set_remove(e->checkpoint_pending, file)) {
		debug(D_VINE, "checkpoint withdrawn: %s", vine_file_cached_name(file));
	}
}

/* Return non-zero when a lost output of this node would never need the node's parents: the node failed and will never
 * read them, or it completed, is not being recovered, and its outputs are either released or anchored in the vault. */
static int vine_graph_executor_node_is_durable(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (node->failed || node->removed) {
		return 1;
	}
	return node->completed && !vine_graph_node_is_recovering(node) && (node->released || vine_graph_executor_node_is_anchored(e, node));
}

/* Return non-zero when every submitted child of the node is durable. A child that became durable usually stays so, so
 * the check resumes at the first child that was not durable the last time, and each event costs O(1) until that child
 * changes, instead of a pass over every child. Once the position passes the last child, a full pass confirms the
 * result, because a recovery can make an earlier child non-durable again, and moves the position to such a child. */
static int vine_graph_executor_children_durable(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (!node->durable_children) {
		node->durable_children = list_cursor_create(node->children);
		if (!node->durable_children) {
			return 0;
		}
		list_seek(node->durable_children, 0);
	}
	struct vine_graph_node *child;
	while (list_get(node->durable_children, (void **)&child)) {
		if (!vine_graph_executor_node_is_durable(e, child)) {
			return 0;
		}
		list_next(node->durable_children);
	}
	int index = 0;
	LIST_ITERATE(node->children, child)
	{
		if (!vine_graph_executor_node_is_durable(e, child)) {
			list_seek(node->durable_children, index);
			return 0;
		}
		index++;
	}
	return 1;
}

/* Prune the node's temporary outputs once every submitted child is durable and the frontend pins none of them. A lost
 * output then needs at most its parent, never a chain of released ancestors. A consumer submitted after the release
 * makes TaskVine recompute the output, so pins keep outputs that may still be read. Pruning wins over checkpointing.
 * While the vault is full, children cannot become durable, so their parents stay on Workers until downstream nodes
 * complete or the vault has room again. Return non-zero when the node was released. */
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
	if (!vine_graph_executor_children_durable(e, node)) {
		return 0;
	}
	list_cursor_destroy(node->durable_children);
	node->durable_children = NULL;
	LIST_ITERATE(node->outputs, mount)
	{
		vine_graph_executor_forget_checkpoint(e, mount->file);
		vine_graph_executor_forget_vault_path(e, mount->file);
		vine_prune_file(e->manager, mount->file);
	}
	node->released = 1;
	vine_graph_executor_report(e->released, node);
	if (node->let_go) {
		list_push_tail(e->removable, (void *)(uintptr_t)node->node_id);
	}
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

/* Record a checkpoint or fetch result from the Manager thread. Queue and graph changes wait for the executor loop, and
 * the recorded file keeps a reference until then. */
static void vine_graph_executor_checkpoint_complete(void *argument, struct vine_file *file, int success)
{
	struct vine_graph_executor *e = argument;
	file = vine_file_addref(file);
	if (success) {
		list_push_tail(e->checkpoint_completed, file);
		/* The Manager's wait returns, so the executor reports a fetched output and fills the freed slot at once. */
		vine_wake(e->manager);
		return;
	}
	/* Like a transient transfer error, a failed receive is retried after the Manager's transient error interval. A
	 * source that just disconnected otherwise fails every retry until the Manager notices. */
	if (list_size(e->checkpoint_failed) == 0) {
		e->checkpoint_retry_time = timestamp_get() + e->manager->transient_error_interval;
	}
	list_push_tail(e->checkpoint_failed, file);
}

/* Queue a pending fetch to be tried in the next round, when its file may have become receivable. The entry holds a
 * reference to its file. */
static void vine_graph_executor_fetch_ready(struct vine_graph_executor *e, struct vine_file *file)
{
	if (set_lookup(e->fetch_pending, file)) {
		list_push_tail(e->fetch_ready, vine_file_addref(file));
	}
}

/* Tell the user once that the vault is full, so intermediate data stays on Workers longer. */
static void vine_graph_executor_note_vault_full(struct vine_graph_executor *e)
{
	if (!e->vault_full_noticed) {
		e->vault_full_noticed = 1;
		debug(D_NOTICE | D_VINE, "the vault is full, so outputs wait on Workers until the vault has room or the nodes that read them are released, and Worker disk use grows meanwhile; raise the manager tuning vault-disk-limit to keep it near the live frontier");
	}
}

/* Settle reported results, then admit queued files in priority order until the Manager refuses. Failed files return
 * to the queue once their retry time passed. BUSY means no free slot, and FULL means no vault space. Either stops
 * admission. A file without a ready source is set aside for this round so
 * it does not block lower-priority files. Results of released files are ignored. */
static void vine_graph_executor_process_checkpoints(struct vine_graph_executor *e)
{
	struct vine_file *file;
	while ((file = list_pop_head(e->checkpoint_completed))) {
		/* A received file serves both a checkpoint and a fetch of it. */
		set_remove(e->fetch_pending, file);
		if (set_remove(e->checkpoint_pending, file)) {
			/* A published checkpoint anchors its node, which may let its ancestors be released. */
			vine_graph_executor_release_from(e, vine_graph_get_producer(e->graph, file), 0);
		}
		vine_file_delete(file);
	}
	while (timestamp_get() >= e->checkpoint_retry_time && (file = list_pop_head(e->checkpoint_failed))) {
		vine_graph_executor_fetch_ready(e, file);
		if (set_lookup(e->checkpoint_pending, file)) {
			vine_graph_executor_push_checkpoint(e, vine_graph_get_producer(e->graph, file), file);
		}
		vine_file_delete(file);
	}

	enum { SKIP_LIMIT = 16 };
	struct vine_file *skipped[SKIP_LIMIT];
	double skipped_priority[SKIP_LIMIT];
	int skips = 0;
	while (skips < SKIP_LIMIT && (file = priority_queue_peek_top(e->checkpoint_queue))) {
		double priority = priority_queue_get_top_priority(e->checkpoint_queue);
		if (!set_lookup(e->checkpoint_pending, file) || set_lookup(e->checkpoint_parked, file)) {
			/* Withdrawn, or parked by an earlier entry for the same file. */
			vine_file_delete(priority_queue_pop(e->checkpoint_queue));
			continue;
		}
		vine_manager_checkpoint_result_t result = vine_manager_data_service_checkpoint(e->manager, file, vine_graph_executor_checkpoint_complete, e);
		if (result == VINE_CHECKPOINT_BUSY) {
			break;
		}
		if (result == VINE_CHECKPOINT_FULL) {
			vine_graph_executor_note_vault_full(e);
			break;
		}
		priority_queue_pop(e->checkpoint_queue);
		if (result == VINE_CHECKPOINT_NO_SOURCE && vine_temp_exists_somewhere(e->manager, file)) {
			/* The skipped entry keeps its reference and returns to the queue below. */
			skipped[skips] = file;
			skipped_priority[skips] = priority;
			skips++;
			continue;
		}
		if (result == VINE_CHECKPOINT_NO_SOURCE) {
			/* No replica can serve it, so it waits outside the queue until a task recreates it. */
			set_insert(e->checkpoint_parked, file);
		}
		vine_file_delete(file);
	}
	for (int i = 0; i < skips; i++) {
		priority_queue_push(e->checkpoint_queue, skipped[i], skipped_priority[i]);
	}
}

/*************************************************************/
/* Failures */
/*************************************************************/

/* Cancel a task that is still active in the Manager. The Manager returns it later, and the executor ignores it. */
static void vine_graph_executor_cancel_task(struct vine_graph_executor *e, int task_id)
{
	if (task_id > 0 && itable_lookup(e->manager->tasks, (uint64_t)task_id)) {
		vine_cancel_by_task_id(e->manager, task_id);
	}
}

/* Return non-zero when an output of producer exists nowhere the Manager can give it to a task: neither in the vault
 * nor on a Worker. Files of a producer that never completed are not results, whatever its attempts left. An output
 * whose producer runs again is pending, so its copies no longer count until the task succeeds. */
static int vine_graph_executor_output_lost(struct vine_graph_executor *e, struct vine_graph_node *producer, struct vine_file *file)
{
	return !producer->completed || !vine_temp_exists_somewhere(e->manager, file);
}

/* Return a failed parent of a node that wrote an input of the node that exists nowhere, or NULL. Inputs are searched
 * only when some parent failed. */
static struct vine_graph_node *vine_graph_executor_find_failed_parent(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	int parent_failed = 0;
	struct vine_graph_node *parent;
	LIST_ITERATE(node->parents, parent)
	{
		parent_failed = parent_failed || parent->failed;
	}
	if (!parent_failed) {
		return NULL;
	}
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->inputs, mount)
	{
		parent = vine_graph_get_producer(e->graph, mount->file);
		if (parent && parent->failed && vine_graph_executor_output_lost(e, parent, mount->file)) {
			return parent;
		}
	}
	return NULL;
}

/* Return non-zero when a node reads an output of parent that exists nowhere. */
static int vine_graph_executor_reads_lost_output(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_graph_node *parent)
{
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->inputs, mount)
	{
		if (vine_graph_get_producer(e->graph, mount->file) == parent && vine_graph_executor_output_lost(e, parent, mount->file)) {
			return 1;
		}
	}
	return 0;
}

/* Mark a node failed because of the node source_id, cancel its active task and any Manager recovery task of its
 * outputs, and report it, again if it had completed. */
static void vine_graph_executor_mark_failed(struct vine_graph_executor *e, struct vine_graph_node *node, uint64_t source_id)
{
	node->failed = 1;
	node->failure_source_id = source_id;
	node->restoring = 0;
	if (!node->completed) {
		e->finished_nodes++;
	}
	vine_graph_executor_cancel_task(e, node->task_id);
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->outputs, mount)
	{
		set_remove(e->fetch_pending, mount->file);
		if (vine_file_is_recovering(mount->file)) {
			vine_graph_executor_cancel_task(e, vine_task_get_id(mount->file->recovery_task));
		}
	}
	vine_graph_executor_report(e->finished, node);
	if (node->let_go) {
		list_push_tail(e->removable, (void *)(uintptr_t)node->node_id);
	}
}

/* Fail each dependent of a failed node that needs one of its outputs that exists nowhere, with the same failure
 * source, and continue from each dependent that fails. An unfinished node needs its inputs, and so does a completed
 * node that is recreating its outputs, by a restore or a Manager recovery task. Other completed nodes keep their
 * outputs. Failed nodes no longer hold back the release of their parents. */
static void vine_graph_executor_fail_dependents(struct vine_graph_executor *e, struct vine_graph_node *start)
{
	struct list *failed = list_create();
	struct list *worklist = list_create();
	list_push_tail(worklist, start);
	struct vine_graph_node *current;
	while ((current = list_pop_head(worklist))) {
		/* A child that reads only outputs that still exist continues. Its inputs are searched only when some
		 * outputs of the failed node exist and others do not. */
		int lost = 0;
		struct vine_graph_mount *mount;
		LIST_ITERATE(current->outputs, mount)
		{
			lost += vine_graph_executor_output_lost(e, current, mount->file);
		}
		if (lost == 0) {
			continue;
		}
		int all_lost = lost == list_size(current->outputs);
		struct vine_graph_node *child;
		LIST_ITERATE(current->children, child)
		{
			if (child->removed || child->failed) {
				continue;
			}
			if (child->completed && !child->restoring && !vine_graph_node_is_recovering(child)) {
				continue;
			}
			if (all_lost || vine_graph_executor_reads_lost_output(e, child, current)) {
				vine_graph_executor_mark_failed(e, child, current->failure_source_id);
				list_push_tail(failed, child);
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

/* Fail a node because of the node source_id, whose own failure explains it, and the dependents that need its lost
 * outputs. error is set only when node is the source. */
static void vine_graph_executor_fail_node(struct vine_graph_executor *e, struct vine_graph_node *node, uint64_t source_id, char *error)
{
	if (node->failed) {
		free(error);
		return;
	}
	if (node->node_id == source_id) {
		node->error = error;
		debug(D_VINE, "node %" PRIu64 " failed: %s", node->node_id, error);
	}
	vine_graph_executor_mark_failed(e, node, source_id);
	vine_graph_executor_fail_dependents(e, node);
	vine_graph_executor_release_from(e, node, 0);
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
		vine_graph_executor_fail_node(e, node, node->node_id, string_format("its function failed:\n%s", output ? output : ""));
		return;
	}
	struct vine_file *lost = result == VINE_RESULT_INPUT_MISSING ? vine_graph_executor_find_lost_input(node) : NULL;
	if (lost) {
		vine_graph_executor_fail_node(e, node, node->node_id, string_format("its input %s is missing at the Manager", vine_file_source(lost)));
		return;
	}
	if (++node->failures > e->max_retries) {
		vine_graph_executor_fail_node(e, node, node->node_id, string_format("it failed %d times; last result: %s", node->failures, vine_result_string(result)));
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
	if (restores) {
		node->restoring = 0;
	} else {
		node->completed = 1;
	}
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->outputs, mount)
	{
		if (set_remove(e->checkpoint_parked, mount->file)) {
			vine_graph_executor_push_checkpoint(e, node, mount->file);
		}
		vine_graph_executor_queue_checkpoint(e, node, mount->file);
		vine_graph_executor_fetch_ready(e, mount->file);
	}

	if (restores) {
		e->completed_recovery_tasks++;
		node->released = 0;
		/* A Manager recovery task runs for a consumer that needs the output now, and that consumer releases the node
		 * later. A restore that a fetch started runs for the frontend's pin, which may be gone already, so the node is
		 * released as usual once nothing pins it. */
		vine_graph_executor_release_from(e, node, vine_task_get_recovery_source_task_id(task) == 0);
		return;
	}
	e->time_last_task_retrieved = MAX(e->time_last_task_retrieved, (timestamp_t)vine_task_get_metric(task, "time_when_retrieval"));
	e->completed_nodes++;
	e->finished_nodes++;
	vine_graph_executor_report(e->finished, node);
	vine_graph_executor_release_from(e, node, 1);
	vine_graph_executor_submit_children(e, node);
}

/* Handle one task of a node returned by the Manager. */
static void vine_graph_executor_handle_node_task(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_task *task)
{
	int is_recovery = vine_task_get_recovery_source_task_id(task) > 0;
	if (node->failed) {
		/* A task cancelled when the node failed, or a Manager recovery task that a consumer needed. A consumer that
		 * needs an output the recovery task did not recreate fails now. */
		if (is_recovery) {
			vine_graph_executor_fail_dependents(e, node);
		}
		return;
	}
	timestamp_t commit_end = vine_task_get_metric(task, "time_when_commit_end");
	if (commit_end > 0) {
		e->time_first_task_dispatched = MIN(e->time_first_task_dispatched, commit_end);
	}

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

/* Handle one task returned by the Manager. */
static void vine_graph_executor_handle_task(struct vine_graph_executor *e, struct vine_task *task)
{
	uint64_t node_id = vine_graph_executor_node_id_from_task(e, task);
	if (!node_id) {
		vine_graph_executor_fail_run(e, string_format("task %d does not belong to this graph", vine_task_get_id(task)));
		return;
	}
	struct vine_graph_node *node = vine_graph_get_node(e->graph, node_id);
	if (!node) {
		return; // a recovery task or an earlier attempt of a removed node
	}
	struct vine_task *attempt = node->task;
	vine_graph_executor_handle_node_task(e, node, task);
	/* A returned attempt is not needed anymore, and the node keeps only its id. The Manager keeps a copy as the
	 * recovery task of each temporary output, and the caller deletes the reference of a returned recovery task. A
	 * failed attempt that was submitted again was already replaced. */
	if (task == attempt && node->task == attempt) {
		node->task = NULL;
		vine_task_delete(attempt);
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
	struct vine_graph_node *failed_parent;
	if (node->remaining_parents == 0) {
		vine_graph_executor_submit_task(e, node);
	} else if ((failed_parent = vine_graph_executor_find_failed_parent(e, node))) {
		vine_graph_executor_fail_node(e, node, failed_parent->failure_source_id, NULL);
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
		struct vine_graph_node *producer = vine_graph_get_producer(e->graph, mount->file);
		/* A pin on a released node asked for its outputs again, and the frontend took the node back. A node that was not
		 * restored meanwhile is still released, so the frontend hears so again and lets it go. */
		if (producer->released && !producer->restoring) {
			vine_graph_executor_report(e->released, producer);
		}
		/* The last pin may have held back the producer and, through it, its ancestors. */
		vine_graph_executor_release_from(e, producer, 1);
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
		vine_graph_executor_fetch_ready(e, mount->file);
	}
	return 0;
}

/* What became of one fetch that was tried. */
typedef enum {
	VINE_GRAPH_FETCH_DONE = 0, /* Received, being received, not pending, or waiting for a task. It leaves the list. */
	VINE_GRAPH_FETCH_LATER,	   /* A replica exists but cannot serve it yet. It is tried again later. */
	VINE_GRAPH_FETCH_BUSY	   /* The Manager has no free receive slot. It stays first in the list. */
} vine_graph_fetch_result_t;

/* Try one pending fetch: receive it from a Worker replica, or run its completed producer again when no replica
 * exists, unless a recovery task is already recreating it. */
static vine_graph_fetch_result_t vine_graph_executor_fetch_one(struct vine_graph_executor *e, struct vine_file *file)
{
	if (!set_lookup(e->fetch_pending, file)) {
		return VINE_GRAPH_FETCH_DONE;
	}
	if (vine_manager_data_service_vault_contains(e->manager, file)) {
		set_remove(e->fetch_pending, file);
		return VINE_GRAPH_FETCH_DONE;
	}
	struct vine_graph_node *producer = vine_graph_get_producer(e->graph, file);
	if (!producer->completed || producer->restoring) {
		return VINE_GRAPH_FETCH_DONE;
	}
	vine_manager_checkpoint_result_t result = vine_manager_data_service_fetch(e->manager, file, vine_graph_executor_checkpoint_complete, e);
	if (result == VINE_CHECKPOINT_BUSY) {
		return VINE_GRAPH_FETCH_BUSY;
	}
	if (result != VINE_CHECKPOINT_NO_SOURCE || vine_file_is_recovering(file)) {
		return VINE_GRAPH_FETCH_DONE;
	}
	if (vine_temp_exists_somewhere(e->manager, file)) {
		return VINE_GRAPH_FETCH_LATER;
	}
	debug(D_VINE, "node %" PRIu64 " runs again to restore %s", producer->node_id, vine_file_cached_name(file));
	producer->restoring = 1;
	vine_graph_executor_submit_task(e, producer);
	return VINE_GRAPH_FETCH_DONE;
}

/* Try the fetches that can make progress, in the order they became ready, until the Manager has no free receive slot.
 * A file in the vault is done. A file with a Worker replica is received into the vault with priority over checkpoints
 * and beyond the vault limit, and its receive reports back. A completed node whose output exists nowhere is run again
 * to restore it, unless a recovery task is already recreating it, and the restoring task queues the fetch again. A
 * file whose replica cannot serve it yet is tried again later, at most SKIP_LIMIT of them per round. The work of a
 * round depends on the free slots, not on how many fetches are pending. */
static void vine_graph_executor_process_fetches(struct vine_graph_executor *e)
{
	enum { SKIP_LIMIT = 16 };
	int skips = 0;
	struct vine_file *file;
	while (skips < SKIP_LIMIT && (file = list_pop_head(e->fetch_ready))) {
		vine_graph_fetch_result_t result = vine_graph_executor_fetch_one(e, file);
		if (result == VINE_GRAPH_FETCH_BUSY) {
			list_push_head(e->fetch_ready, file);
			break;
		}
		if (result == VINE_GRAPH_FETCH_LATER) {
			list_push_tail(e->fetch_ready, file);
			skips++;
			continue;
		}
		vine_file_delete(file);
	}
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
/* Removing nodes */
/*************************************************************/

/* Return non-zero when a node the frontend dropped can be removed: every child was removed, so no node that remains
 * reads its outputs, and it failed, or it completed and its outputs were released and are not being restored. */
static int vine_graph_executor_node_is_removable(struct vine_graph_node *node)
{
	if (!node->let_go || node->removed || node->live_children > 0) {
		return 0;
	}
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->outputs, mount)
	{
		if (mount->pins > 0) {
			return 0;
		}
	}
	return node->failed || (node->completed && node->released && !node->restoring);
}

/* Cancel a task that is still active in the Manager and collect it at once. */
static void vine_graph_executor_collect_task(struct vine_graph_executor *e, int task_id)
{
	if (task_id > 0 && itable_lookup(e->manager->tasks, (uint64_t)task_id)) {
		vine_cancel_by_task_id(e->manager, task_id);
		vine_graph_executor_release_returned(vine_wait_for_task_id(e->manager, task_id, 0));
	}
}

/* Remove a node: collect its active tasks, undeclare its outputs, and queue its dropped parents, which may be
 * removable once their last child is gone. */
static void vine_graph_executor_remove_node(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	/* The cancelled tasks of a failed node may still be active, and so may a recovery task that no consumer needs
	 * anymore. The Manager returns them at once, before the node's task id no longer maps to the node. */
	vine_graph_executor_collect_task(e, node->task_id);
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->outputs, mount)
	{
		if (vine_file_is_recovering(mount->file)) {
			vine_graph_executor_collect_task(e, vine_task_get_id(mount->file->recovery_task));
		}
	}
	if (node->task_id) {
		/* Earlier attempts keep their entries, so a late recovery task of the node is recognized and ignored. */
		itable_remove(e->task_id_to_node, (uint64_t)node->task_id);
	}
	LIST_ITERATE(node->outputs, mount)
	{
		vine_graph_executor_forget_checkpoint(e, mount->file);
		set_remove(e->fetch_pending, mount->file);
		vine_graph_executor_forget_vault_path(e, mount->file);
		vine_graph_remove_file(e->graph, mount->file_id);
		if (vine_manager_lookup_file(e->manager, vine_file_cached_name(mount->file)) == mount->file) {
			vine_undeclare_file(e->manager, mount->file);
		} else {
			vine_file_delete(mount->file);
		}
	}
	struct vine_graph_node *parent;
	LIST_ITERATE(node->parents, parent)
	{
		if (--parent->live_children == 0 && parent->let_go) {
			list_push_tail(e->removable, (void *)(uintptr_t)parent->node_id);
		}
	}
	uint64_t node_id = node->node_id;
	vine_graph_remove_node(e->graph, node);
	list_push_tail(e->removed, (void *)(uintptr_t)node_id);
	debug(D_VINE, "removed node %" PRIu64, node_id);
}

/* Remove the dropped nodes that became removable, and the parents that their removal makes removable in turn. Nodes
 * are queued by id, so a node removed meanwhile is skipped. */
static void vine_graph_executor_remove_dropped(struct vine_graph_executor *e)
{
	void *id;
	while ((id = list_pop_head(e->removable))) {
		struct vine_graph_node *node = vine_graph_get_node(e->graph, (uint64_t)(uintptr_t)id);
		if (node && vine_graph_executor_node_is_removable(node)) {
			vine_graph_executor_remove_node(e, node);
		}
	}
}

int vine_graph_executor_drop_node(struct vine_graph_executor *e, uint64_t node_id)
{
	struct vine_graph_node *node = e ? vine_graph_get_node(e->graph, node_id) : NULL;
	if (!node || !node->submitted) {
		return -1;
	}
	node->let_go = 1;
	list_push_tail(e->removable, (void *)(uintptr_t)node_id);
	vine_graph_executor_remove_dropped(e);
	return 0;
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
	progress_bar_set_part_total(e->progress_bar, e->node_part, e->submitted_nodes);
	progress_bar_set_part_total(e->progress_bar, e->recovery_part, e->manager->stats->tasks_recovery);
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
	/* With nothing left to do, the wait still runs the Manager until the timeout or a wake, so Workers that connect,
	 * restores that nobody waits for anymore, recovery tasks, and keepalives are handled while the run is idle. */
	int done = 0;
	while (!e->error) {
		done = e->finished_nodes >= e->submitted_nodes && set_size(e->fetch_pending) == 0;
		/* TaskVine removes a library that keeps failing to start, and then no node can run. */
		if (!done && !vine_manager_find_library_template(e->manager, e->library_name)) {
			vine_graph_executor_fail_run(e, string_format("library %s failed to start on workers too many times and was removed; check that workers can run it and use the manager's environment (Python, cloudpickle, ndcctools), and set the manager tuning watch-library-logfiles to 1 to keep its logs", e->library_name));
			break;
		}
		/* Fetches come first, so a receive slot freed during the last wait goes to an output the caller waits for. */
		vine_graph_executor_process_fetches(e);
		vine_graph_executor_process_checkpoints(e);
		vine_graph_executor_remove_dropped(e);
		/* An idle run draws no bar, because each DONE finishes the bar it drew. */
		if (!done) {
			vine_graph_executor_update_progress_bar(e);
		}
		/* The caller learns at once that a node finished or that a fetched output became readable. Results that
		 * arrived during the last Manager wait are settled above before any return. */
		if (list_size(e->finished) > 0 || set_size(e->fetch_pending) < fetches) {
			return VINE_GRAPH_RUNNING;
		}
		if (expired) {
			if (done) {
				break;
			}
			return VINE_GRAPH_RUNNING;
		}
		/* Drain returned tasks without blocking, then block once for the rest of the timeout. A blocking wait that
		 * returns no task expired or was ended by vine_graph_executor_wake(). */
		struct vine_task *task = vine_wait(e->manager, wait_timeout);
		if (task) {
			/* Settle every task the Manager has already returned, so one call reports a batch of finished nodes.
			 * A wait of zero seconds only collects returned tasks. */
			do {
				/* Handling may delete a returned attempt, so whether this is a recovery task is decided first. */
				int recovery = vine_task_get_recovery_source_task_id(task) > 0;
				vine_graph_executor_handle_task(e, task);
				if (recovery) {
					vine_task_delete(task);
				}
			} while ((task = vine_wait(e->manager, 0)));
			wait_timeout = 0;
		} else if (wait_timeout > 0 || time(NULL) >= stoptime) {
			expired = 1;
		} else {
			wait_timeout = MAX(stoptime - time(NULL), 1);
		}
	}

	vine_graph_executor_remove_dropped(e);
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
	return e ? vine_graph_executor_next_report(e->finished) : 0;
}

uint64_t vine_graph_executor_next_released(struct vine_graph_executor *e)
{
	return e ? vine_graph_executor_next_report(e->released) : 0;
}

uint64_t vine_graph_executor_next_removed(struct vine_graph_executor *e)
{
	return e ? (uint64_t)(uintptr_t)list_pop_head(e->removed) : 0;
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
	return node && node->failed ? node->failure_source_id : 0;
}

const char *vine_graph_executor_get_node_error(const struct vine_graph_executor *e, uint64_t node_id)
{
	struct vine_graph_node *node = e ? vine_graph_get_node(e->graph, node_id) : NULL;
	struct vine_graph_node *source = node && node->failed ? vine_graph_get_node(e->graph, node->failure_source_id) : NULL;
	return source ? source->error : NULL;
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
	vine_graph_executor_fail_node(e, node, node->node_id, xxstrdup("it was cancelled"));
	return 0;
}
