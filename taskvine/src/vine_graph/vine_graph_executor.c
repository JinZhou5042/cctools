#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>

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

/* Dropped nodes removed per step of vine_graph_executor_wait(). A frontend that drops many nodes at once removes the
 * rest with vine_graph_executor_remove_dropped() at its own pace, so a step never spends long on removals. */
#define VINE_GRAPH_REMOVALS_PER_STEP 256

/* Bytes of a node's failure message that the debug log keeps. The message ends with the function's output, which may
 * be long, while the frontend reads the whole message. */
#define VINE_GRAPH_ERROR_LOGGED 4096

/* Order in which ready nodes are submitted. */
typedef enum {
	VINE_GRAPH_PRIORITY_LARGEST_INPUT_FIRST = 0, /* Largest total input size first. */
	VINE_GRAPH_PRIORITY_DEPTH_FIRST,	     /* Deeper nodes first. */
	VINE_GRAPH_PRIORITY_FIFO		     /* Earlier ready time first. */
} vine_graph_priority_mode_t;

/* A first-in first-out queue of node ids in one array, which keeps its capacity, at most 8 bytes per id of the longest
 * backlog. A dttools list allocates an item for every entry, which every dropped node paid twice on its way to
 * removal, and the frees of those items added to the allocator work of removal. */
struct vine_graph_id_queue {
	uint64_t *ids; // owned array of the ids, NULL before the first push
	int head;      // index of the oldest id
	int tail;      // index after the newest id
	int capacity;  // ids allocated
};

/* Seconds of one durability measurement window. */
#define VINE_GRAPH_WINDOW_SECONDS 5

/* Seconds a large checkpoint waits by default for the readers of its node (see vine_graph_executor_hold()). */
#define VINE_GRAPH_CHECKPOINT_YIELD_DEFAULT 10

/* The widest band of generations that adaptive cut anchoring uses, which bounds what one loss recomputes. */
#define VINE_GRAPH_CUT_MAX 32

/* Counts of one durability measurement window. Large outputs are those above VINE_DATA_INLINE_BYTES, which the vault
 * receives over connections of their own. Small outputs come inline and are always anchored, because the inline
 * receive costs little and also delivers them with the tasks that read them. */
struct vine_graph_window {
	timestamp_t start;     // when the window began
	timestamp_t sampled;   // when the exposure was last integrated
	double exposure_area;  // integral of exposed outputs over the window, in output-seconds
	uint64_t outputs;      // outputs completed in the window that need the vault to be anchored
	uint64_t large;	       // of which large
	uint64_t large_anchor; // large outputs the vault anchored in the window
	int drained;	       // an admission round of the window left no large output in the queue
};

/* A large output whose checkpoint waits for the readers of its node (see vine_graph_executor_hold()). */
struct vine_graph_hold {
	struct vine_file *file; // a reference to the held file, which a withdrawal or removal leaves valid until expiry
	timestamp_t start;	// when the hold started, so the holds expire in list order whatever checkpoint-yield is
};

/* One run of a graph on a Manager. */
struct vine_graph_executor {
	struct vine_graph *graph;     // DAG executed by this executor
	struct vine_manager *manager; // TaskVine runtime

	char *library_name;  // library that runs every node, installed by the frontend
	char *function_name; // library function that runs one node

	struct itable *task_id_to_node;		 // maps Manager task id to the id of its graph node after submission
	struct priority_queue *checkpoint_queue; // borrowed large idata awaiting admission, longest producer first
	struct priority_queue *checkpoint_small; // the same for idata that comes inline, which other slots receive
	struct list *checkpoint_completed;	 // borrowed files whose checkpoint the Manager published
	struct list *checkpoint_failed;		 // borrowed files whose checkpoint failed, to requeue
	struct list *checkpoint_unsendable;	 // borrowed files whose source cannot send them, which a retry cannot change
	struct list *checkpoint_lost;		 // borrowed files whose vault entry the vault lost, to checkpoint again
	timestamp_t checkpoint_retry_time;	 // when failed files are requeued, so a lost source is not retried at once
	struct set *checkpoint_pending;		 // queued or in-flight files; release withdraws them, and the queue drops
						 // files that are no longer pending when they reach its head
	struct set *checkpoint_parked;		 // pending files with no replica anywhere, queued again once recreated
	uint64_t checkpoint_sequence;		 // completion order used to break equal checkpoint priorities

	struct set *fetch_pending;  // borrowed pinned outputs requested at the Manager and not in the vault yet
	struct list *fetch_ready;   // borrowed pending fetches to try now, queued when one can make progress, maybe twice
	struct list *fetch_small;   // the same for files that come inline, which never wait for a receive slot
	struct itable *vault_paths; // vine_file address -> owned path of a fetched output in the vault
	struct list *fetched;	    // ids of requested files that became readable and were not reported yet
	struct list *fetch_failed;  // ids of requested files that their Workers cannot send, not reported yet
	struct list *finished;	    // nodes that completed or failed and were not reported yet, counted in their refs
	struct list *released;	    // nodes whose outputs were released and were not reported yet, counted in their refs
	struct list *kept;	    // released nodes that ran again and are kept, not reported yet, counted in their refs
	struct vine_graph_id_queue removable; // ids of nodes let go by the frontend that may have become removable
	struct vine_graph_id_queue removed;   // ids of removed nodes that were not reported yet
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

	/* Durability (see vine_graph_executor_get_durability()). */
	int64_t exposed_outputs;		  // outputs of completed nodes that are neither released nor anchored
	struct vine_graph_window window;	  // counts of the current measurement window
	struct vine_graph_durability durability;  // what the last complete window measured
	int cut_setting;			  // checkpoint-cut: -1 adapts the band, 0 anchors every output, k fixes it
	int cut;				  // generations per anchored band of large outputs, or 0 to anchor each
	int overloaded_windows;			  // consecutive windows in which large outputs always waited
	int underloaded_windows;		  // consecutive windows of a cut in which the vault would keep up
	int cut_noticed;			  // the user was told once that anchoring falls behind

	char *shared_root; // the directory given by shared-directory, which the vault keeps large checkpoints in, or NULL

	timestamp_t yield_us; // checkpoint-yield: how long a large checkpoint waits for the readers of its node, or 0
	struct set *held;     // borrowed pending large files whose checkpoint waits for the readers of their node
	struct set *yielded;  // borrowed queued files that waited once, which are admitted when they reach the head again
	struct list *holds;   // owned holds in the order they started

	char *error; // why the run failed as a whole, or NULL
	int tasks_cancelled; // the tasks of a run that failed as a whole were cancelled
};

/*************************************************************/
/* Id queues */
/*************************************************************/

/* Append an id. A full array first drops the ids already popped when they are at least half of it, and otherwise
 * doubles, so each id is moved a constant number of times on average. */
static void vine_graph_id_queue_push(struct vine_graph_id_queue *q, uint64_t id)
{
	if (q->tail == q->capacity) {
		if (q->head > 0 && q->head >= q->capacity / 2) {
			memmove(q->ids, q->ids + q->head, sizeof(*q->ids) * (q->tail - q->head));
			q->tail -= q->head;
			q->head = 0;
		} else {
			q->capacity = q->capacity ? 2 * q->capacity : 64;
			q->ids = xxrealloc(q->ids, sizeof(*q->ids) * q->capacity);
		}
	}
	q->ids[q->tail++] = id;
}

/* Remove and return the oldest id, or zero when the queue is empty. An emptied queue starts again at the front of its
 * array, which it keeps for the next ids. */
static uint64_t vine_graph_id_queue_pop(struct vine_graph_id_queue *q)
{
	if (q->head == q->tail) {
		return 0;
	}
	uint64_t id = q->ids[q->head++];
	if (q->head == q->tail) {
		q->head = q->tail = 0;
	}
	return id;
}

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
	e->checkpoint_small = priority_queue_create(0);
	e->checkpoint_completed = list_create();
	e->checkpoint_failed = list_create();
	e->checkpoint_pending = set_create(0);
	e->checkpoint_parked = set_create(0);
	e->yield_us = VINE_GRAPH_CHECKPOINT_YIELD_DEFAULT * 1000000ULL;
	e->held = set_create(0);
	e->yielded = set_create(0);
	e->holds = list_create();
	e->fetch_pending = set_create(0);
	e->fetch_ready = list_create();
	e->fetch_small = list_create();
	e->vault_paths = itable_create(0);
	e->finished = list_create();
	e->released = list_create();
	e->kept = list_create();
	e->fetched = list_create();
	e->fetch_failed = list_create();
	e->checkpoint_unsendable = list_create();
	e->checkpoint_lost = list_create();
	e->priority_mode = VINE_GRAPH_PRIORITY_LARGEST_INPUT_FIRST;
	e->max_retries = 5;
	e->show_progress_bar = 1;
	e->progress_bar_update_interval_sec = 0.1;
	e->time_first_task_dispatched = UINT64_MAX;
	e->cut_setting = -1;
	e->window.start = e->window.sampled = timestamp_get();
	/* Recovery completions are returned to the executor, which releases ancestors behind them. */
	vine_enable_external_recovery_handling(manager);
	return e;
}

static void vine_graph_executor_undeclare_owned(struct vine_graph_executor *e, struct vine_graph_node *node);

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
	uint64_t node_id;
	ITABLE_ITERATE(e->graph->nodes, iteration, node_id, node)
	{
		vine_graph_executor_undeclare_owned(e, node);
	}
	while ((node = list_pop_head(e->finished))) {
		vine_graph_node_unref(node);
	}
	while ((node = list_pop_head(e->released))) {
		vine_graph_node_unref(node);
	}
	while ((node = list_pop_head(e->kept))) {
		vine_graph_node_unref(node);
	}
	free(e->shared_root);
	vine_graph_delete(e->graph);
	itable_delete(e->task_id_to_node);
	/* Queued files hold references of their own. */
	struct priority_queue *queues[] = {e->checkpoint_queue, e->checkpoint_small};
	for (size_t i = 0; i < sizeof(queues) / sizeof(queues[0]); i++) {
		while ((file = priority_queue_pop(queues[i]))) {
			vine_file_delete(file);
		}
		priority_queue_delete(queues[i]);
	}
	struct list *queued[] = {e->checkpoint_completed, e->checkpoint_failed, e->checkpoint_unsendable, e->checkpoint_lost, e->fetch_ready, e->fetch_small};
	for (size_t i = 0; i < sizeof(queued) / sizeof(queued[0]); i++) {
		while ((file = list_pop_head(queued[i]))) {
			vine_file_delete(file);
		}
		list_delete(queued[i]);
	}
	set_delete(e->checkpoint_pending);
	set_delete(e->checkpoint_parked);
	struct vine_graph_hold *hold;
	while ((hold = list_pop_head(e->holds))) {
		vine_file_delete(hold->file);
		free(hold);
	}
	list_delete(e->holds);
	set_delete(e->held);
	set_delete(e->yielded);
	set_delete(e->fetch_pending);
	itable_delete(e->vault_paths);
	list_delete(e->finished);
	list_delete(e->released);
	list_delete(e->kept);
	free(e->removable.ids);
	free(e->removed.ids);
	list_delete(e->fetched);
	list_delete(e->fetch_failed);
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

/* Apply shared-directory: the vault keeps large checkpoints in a directory of its own inside it, which the Workers that
 * hold them write (vine_set_vault_shared_directory()). A scratch executor of vine_graph_executor_check_setting(), which
 * has no Manager, only checks the directory. */
static const char *vine_graph_executor_set_shared_directory(struct vine_graph_executor *e, const char *value)
{
	struct stat info;
	if (value[0] != '/' || stat(value, &info) != 0 || !S_ISDIR(info.st_mode) || access(value, W_OK | X_OK) != 0) {
		return "it must be the absolute path of a directory that this process can write";
	}
	if (e->shared_root) {
		return strcmp(e->shared_root, value) == 0 ? NULL : "the vault uses one shared directory, and another one was set already";
	}
	if (!e->manager) {
		return NULL;
	}
	if (!vine_set_vault_shared_directory(e->manager, value)) {
		return "the vault could not create its directory in it";
	}
	e->shared_root = xxstrdup(value);
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
	} else if (strcmp(name, "checkpoint-cut") == 0) {
		int cut = -1;
		if (strcmp(value, "auto") != 0 && vine_graph_parse_count(value, &cut)) {
			return "use auto, 0 to anchor every output, or the number of generations per band";
		}
		e->cut_setting = cut;
		e->cut = MAX(cut, 0);
		e->overloaded_windows = 0;
		e->underloaded_windows = 0;
		return NULL;
	} else if (strcmp(name, "checkpoint-yield") == 0) {
		char *end;
		double seconds = strtod(value, &end);
		if (end == value || *end || !(seconds >= 0 && seconds <= 86400)) {
			return "use the seconds, at most 86400, that a large checkpoint waits for the readers of its node, or 0";
		}
		e->yield_us = (timestamp_t)(seconds * 1e6);
		return NULL;
	} else if (strcmp(name, "shared-directory") == 0) {
		return vine_graph_executor_set_shared_directory(e, value);
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

int vine_graph_executor_add_inputs(struct vine_graph_executor *e, uint64_t node_id, int count, const uint64_t *file_ids, const char *const *task_paths)
{
	if (!e || count < 0 || (count > 0 && (!file_ids || !task_paths))) {
		return -1;
	}
	for (int i = 0; i < count; i++) {
		if (vine_graph_executor_add_input(e, node_id, file_ids[i], task_paths[i]) != 0) {
			return -1;
		}
	}
	return 0;
}

int vine_graph_executor_add_input_buffer(struct vine_graph_executor *e, uint64_t node_id, const char *buffer, size_t size, const char *task_path)
{
	struct vine_graph_node *node = e && buffer && task_path ? vine_graph_get_node(e->graph, node_id) : NULL;
	if (!node || node->submitted) {
		return -1;
	}
	/* Below the worker cache level a buffer gets a random cache name, not a hash of its content, so nodes with equal
	 * manifests never share a file whose undeclaration or removal at the end of one task would affect another. */
	struct vine_file *file = vine_declare_buffer(e->manager, buffer, size, VINE_CACHE_LEVEL_TASK, 0);
	if (!file) {
		return -1;
	}
	if (vine_graph_add_owned_input(e->graph, node_id, file, task_path) != 0) {
		vine_undeclare_file(e->manager, file);
		return -1;
	}
	return 0;
}

/* Undeclare the inputs that a node owns, whose mounts keep their own references. */
static void vine_graph_executor_undeclare_owned(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	for (int i = 0; i < node->inputs.count; i++) {
		struct vine_graph_mount *mount = node->inputs.items[i];
		if (mount->file_id == 0) {
			vine_undeclare_file(e->manager, mount->file);
		}
	}
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
		for (int i = 0; i < node->inputs.count; i++) {
			mount = node->inputs.items[i];
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

static void vine_graph_executor_fail_node(struct vine_graph_executor *e, struct vine_graph_node *node, uint64_t source_id, vine_graph_failure_t failure, char *error);
static void vine_graph_executor_reader_start(struct vine_graph_executor *e, struct vine_graph_node *node);
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

static void vine_graph_executor_checkpoint_complete(void *argument, struct vine_file *file, int success);

/* Submit a new task for a node. The task runs the node function with the shared arguments and every mount. A node
 * that reads an output of a failed parent that exists nowhere fails instead, because its task would wait forever.
 * Return zero, or -1 after failing the node. */
static int vine_graph_executor_submit_task(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph_node *failed_parent = vine_graph_executor_find_failed_parent(e, node);
	if (failed_parent) {
		vine_graph_executor_fail_node(e, node, failed_parent->failure_source_id, VINE_GRAPH_FAILURE_NONE, NULL);
		return -1;
	}
	struct vine_task *task = vine_task_create(e->function_name);
	if (!task) {
		vine_graph_executor_fail_node(e, node, node->node_id, VINE_GRAPH_FAILURE_SUBMIT, xxstrdup("could not create its task"));
		return -1;
	}
	vine_task_set_library_required(task, e->library_name);
	/* The Manager moves a task whose worker is lost to another worker, so a task that makes its worker exit would
	 * take every worker in turn. With retries set, it returns once more workers were lost under it than the node may
	 * fail, counting only losses that no loss of other workers explains, so a mass loss of workers fails no node. */
	vine_task_set_retries(task, MAX(e->max_retries, 1));
	int mounted = 1;
	struct vine_graph_mount *mount;
	for (int i = 0; i < node->inputs.count; i++) {
		mount = node->inputs.items[i];
		mounted = mounted && vine_task_add_input(task, mount->file, mount->task_path, VINE_TRANSFER_ALWAYS);
	}
	for (int i = 0; i < node->outputs.count; i++) {
		mount = node->outputs.items[i];
		mounted = mounted && vine_task_add_output(task, mount->file, mount->task_path, VINE_TRANSFER_ALWAYS);
	}
	vine_task_set_priority(task, vine_graph_executor_priority(e, node));
	int task_id = mounted ? vine_submit(e->manager, task) : 0;
	if (task_id <= 0) {
		vine_task_delete(task);
		vine_graph_executor_fail_node(e, node, node->node_id, VINE_GRAPH_FAILURE_SUBMIT, xxstrdup("could not submit its task"));
		return -1;
	}

	/* The Manager keeps its own reference while the task is active. A finished attempt is no longer needed. */
	vine_task_delete(node->task);
	node->task = task;
	node->task_id = task_id;
	itable_insert(e->task_id_to_node, (uint64_t)task_id, (void *)(uintptr_t)node->node_id);
	debug(D_VINE, "submitted node %" PRIu64 " as task %d", node->node_id, task_id);
	vine_graph_executor_reader_start(e, node);

	/* The task waits for a small input that comes inline into the vault instead of fetching it from a peer, so the
	 * checkpoints of its inputs that still wait in the queue start now. The queue entry of such a file stays and finds
	 * the receive in progress or the file in the vault. */
	for (int i = 0; i < node->inputs.count; i++) {
		mount = node->inputs.items[i];
		if (set_lookup(e->checkpoint_pending, mount->file) && !set_lookup(e->checkpoint_parked, mount->file)) {
			vine_manager_data_service_checkpoint_input(e->manager, mount->file, vine_graph_executor_checkpoint_complete, e);
		}
	}
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
 * first, so a failed submission, which walks the node's children again, sees every count already updated. */
static void vine_graph_executor_submit_children(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct list *ready = list_create();
	struct vine_graph_node *child;
	for (int i = 0; i < node->children.count; i++) {
		child = node->children.items[i];
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

/* Return non-zero while a recovery task is recreating the node's temporary outputs. When a task is submitted, the
 * Manager gives each of its temporary outputs the same recovery task, a copy of that task, and every task of a node
 * writes all of the node's outputs, which are temporary. So the outputs of a node share one recovery task, and its
 * first output tells the state of all of them in O(1). */
static int vine_graph_node_is_recovering(const struct vine_graph_node *node)
{
	struct vine_graph_mount *mount = node->outputs.count > 0 ? node->outputs.items[0] : NULL;
	return mount && vine_file_is_recovering(mount->file);
}

/* Return non-zero when the node completed and every output has a readable Manager-local copy, which for a temporary
 * output is its vault entry. The node counts its outputs in the vault, so the check costs O(1). */
static int vine_graph_executor_node_is_anchored(const struct vine_graph_node *node)
{
	return node->completed && node->local_outputs == node->outputs.count;
}

/* Return non-zero for an output that the vault receives over a connection of its own, which bounds how many it
 * anchors per second, rather than inline with the messages of its Worker. */
static int vine_graph_file_is_large(struct vine_file *file)
{
	return vine_file_size(file) > VINE_DATA_INLINE_BYTES;
}

/* Bring the executor's count of exposed outputs up to date with the state of a node: its outputs are exposed while it
 * is completed and neither failed nor removed, and they are neither anchored nor released. Called after every change
 * of that state, any number of times, at O(1). */
static void vine_graph_executor_recount(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	int exposed = 0;
	if (node->completed && !node->released && !node->failed && !node->removed) {
		exposed = node->outputs.count - node->local_outputs;
	}
	e->exposed_outputs += exposed - node->exposed;
	node->exposed = exposed;
}

/* Count an output whose vault entry was just published. A receive publishes only on the Manager thread, right before
 * its callback, and nothing else adds a temporary file to the vault, so the count follows every addition. */
static void vine_graph_executor_count_local(struct vine_graph_executor *e, struct vine_file *file)
{
	struct vine_graph_mount *mount = vine_graph_find_output(e->graph, file);
	if (mount && !mount->local && vine_manager_data_service_vault_contains(e->manager, file)) {
		mount->local = 1;
		mount->node->local_outputs++;
		vine_graph_executor_recount(e, mount->node);
		if (vine_graph_file_is_large(file)) {
			e->window.large_anchor++;
		}
	}
}

/* Stop counting an output whose vault entry is removed: by the executor, when it prunes or undeclares the file, or by
 * the vault, when it finds that a copy in the shared directory was deleted outside it. */
static void vine_graph_executor_uncount_local(struct vine_graph_mount *mount)
{
	if (mount->local) {
		mount->local = 0;
		mount->node->local_outputs--;
	}
}

/* Return the checkpoint queue of a file by its size now: large files wait for a receive slot or a copy slot, and small
 * files for room in the bound of inline receives, so neither kind waits behind the other. */
static struct priority_queue *vine_graph_executor_checkpoint_queue(struct vine_graph_executor *e, struct vine_file *file)
{
	return vine_graph_file_is_large(file) ? e->checkpoint_queue : e->checkpoint_small;
}

/* Push a pending file into its checkpoint queue, ordered by producer execution time with the longest first.
 * A fraction below one microsecond breaks ties in queue order without reordering distinct execution times. Each entry
 * holds a reference to its file, so a file withdrawn when its node is removed stays valid until it leaves the queue. */
static void vine_graph_executor_push_checkpoint(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_file *file)
{
	double priority = (double)node->execution_time_us + 1.0 / (2.0 + (double)e->checkpoint_sequence++);
	priority_queue_push(vine_graph_executor_checkpoint_queue(e, file), vine_file_addref(file), priority);
	debug(D_VINE, "checkpoint queued: %s node %" PRIu64 " runtime_us=%" PRIu64 " priority=%.9f", vine_file_cached_name(file), node->node_id, node->execution_time_us, priority);
}

/* Return non-zero when a node's large outputs are anchored under the current cut: every node while the cut is 0, and
 * otherwise a node on the boundary of its band of cut generations (depth / cut), because a submitted node that reads
 * it lies in a later band, or because nothing reads it yet. Every path in the graph then crosses an anchored node
 * within cut generations, and a lost output recomputes at most its band. A pinned output has a reader outside the
 * graph, the frontend, at a time the graph does not show, so it is on the boundary like a node without readers. Off
 * the boundary, outputs stay exposed until the release rule releases them, which keeps every loss recoverable. */
static int vine_graph_executor_on_cut(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (e->cut == 0 || node->pinned_outputs > 0) {
		return 1;
	}
	int band = node->depth / e->cut;
	int readers = 0;
	for (int i = 0; i < node->children.count; i++) {
		struct vine_graph_node *child = node->children.items[i];
		if (child->removed) {
			continue;
		}
		if (child->depth / e->cut > band) {
			return 1;
		}
		readers++;
	}
	return readers == 0;
}

/* Queue a temporary output for retrieval into the vault unless it is already queued, in flight, or published, or it is
 * a large output off the current cut. */
static void vine_graph_executor_queue_checkpoint(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_file *file)
{
	if (vine_file_type(file) != VINE_TEMP || set_lookup(e->checkpoint_pending, file) || vine_manager_data_service_vault_contains(e->manager, file)) {
		return;
	}
	if (vine_graph_file_is_large(file) && !vine_graph_executor_on_cut(e, node)) {
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
	set_remove(e->held, file);
	set_remove(e->yielded, file);
	set_remove(e->checkpoint_parked, file);
	if (set_remove(e->checkpoint_pending, file)) {
		debug(D_VINE, "checkpoint withdrawn: %s", vine_file_cached_name(file));
	}
}

/* Readers come first. A reader of a large output fetches it from the Worker that produced it, which at the same time
 * sends the output's checkpoint, to the shared directory or to the Manager, through the same network link, so on a
 * host with a slow link the reader waits for both. A large checkpoint therefore waits while its node has children
 * whose tasks were submitted and have not finished (readers), at most checkpoint-yield seconds, and only once: a file
 * that waited is admitted when it reaches the head of the queue again. A node counts its readers when their tasks are
 * submitted and when they complete, fail, or are removed, so each costs O(parents), and an expired hold costs O(1).
 * Small outputs never wait, because they come inline and their readers wait for them in the vault, and neither does
 * an output that the frontend fetches, whose receive does not wait for the checkpoint anyway. The release rule is
 * unchanged: an output whose readers become durable while it waits is released without a checkpoint. */

/* Return the held large files of a node to the checkpoint queue. */
static void vine_graph_executor_unhold(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	for (int i = 0; i < node->outputs.count; i++) {
		struct vine_graph_mount *mount = node->outputs.items[i];
		if (set_remove(e->held, mount->file) && set_lookup(e->checkpoint_pending, mount->file)) {
			vine_graph_executor_push_checkpoint(e, node, mount->file);
		}
	}
}

/* Count a submitted node in the readers of its parents until it finishes. */
static void vine_graph_executor_reader_start(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (node->reading || node->completed) {
		return;
	}
	node->reading = 1;
	for (int i = 0; i < node->parents.count; i++) {
		struct vine_graph_node *parent = node->parents.items[i];
		parent->readers++;
	}
}

/* A reader completed, failed, or was removed. A parent whose readers all finished queues its held files again. */
static void vine_graph_executor_reader_done(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (!node->reading) {
		return;
	}
	node->reading = 0;
	for (int i = 0; i < node->parents.count; i++) {
		struct vine_graph_node *parent = node->parents.items[i];
		if (--parent->readers == 0) {
			vine_graph_executor_unhold(e, parent);
		}
	}
}

/* Queue the held files whose wait is over again. */
static void vine_graph_executor_expire_holds(struct vine_graph_executor *e)
{
	timestamp_t now = timestamp_get();
	struct vine_graph_hold *hold;
	while ((hold = list_peek_head(e->holds)) && hold->start + e->yield_us <= now) {
		list_pop_head(e->holds);
		struct vine_graph_node *producer = vine_graph_get_producer(e->graph, hold->file);
		if (set_remove(e->held, hold->file) && producer && set_lookup(e->checkpoint_pending, hold->file)) {
			vine_graph_executor_push_checkpoint(e, producer, hold->file);
		}
		vine_file_delete(hold->file);
		free(hold);
	}
}

/* Hold a large file taken from the head of its queue, with the entry's reference, when its node has readers that have
 * not finished and the file did not wait before. Return non-zero when it was held. */
static int vine_graph_executor_hold(struct vine_graph_executor *e, struct vine_file *file)
{
	if (e->yield_us == 0 || set_lookup(e->yielded, file) || set_lookup(e->fetch_pending, file)) {
		return 0;
	}
	struct vine_graph_node *producer = vine_graph_get_producer(e->graph, file);
	if (!producer || producer->readers <= 0) {
		return 0;
	}
	struct vine_graph_hold *hold = malloc(sizeof(*hold));
	if (!hold) {
		return 0;
	}
	hold->file = file;
	hold->start = timestamp_get();
	list_push_tail(e->holds, hold);
	set_insert(e->held, file);
	set_insert(e->yielded, file);
	debug(D_VINE, "checkpoint waits for readers: %s node %" PRIu64 " readers=%d", vine_file_cached_name(file), producer->node_id, producer->readers);
	return 1;
}

/* Change the cut at the end of a measurement window. Large outputs always waiting in the queue through two windows,
 * while they complete faster than the vault anchors them, start a cut with bands just wider than that ratio, which
 * the vault can keep up with when few nodes are unread. A cut under which they still wait doubles, and a cut under
 * which the vault keeps up ends once the outputs would no longer outpace it, or halves after half a minute, which
 * probes a capacity that the cut keeps from being measured. */
static void vine_graph_executor_adapt_cut(struct vine_graph_executor *e, int saturated)
{
	if (e->cut_setting >= 0) {
		return;
	}
	if (saturated) {
		e->overloaded_windows++;
		e->underloaded_windows = 0;
	} else {
		e->underloaded_windows++;
		e->overloaded_windows = 0;
	}
	struct vine_graph_durability *d = &e->durability;
	double rho = d->large_capacity > 0 ? d->large_rate / d->large_capacity : (d->large_rate > 0 ? INFINITY : 0);
	int cut = e->cut;
	if (cut == 0 && e->overloaded_windows >= 2 && rho > 1.1) {
		cut = rho < VINE_GRAPH_CUT_MAX - 1 ? MAX((int)ceil(rho) + 1, 2) : VINE_GRAPH_CUT_MAX;
	} else if (cut > 0 && e->overloaded_windows >= 2) {
		cut = MIN(2 * cut, VINE_GRAPH_CUT_MAX);
	} else if (cut > 0 && e->underloaded_windows >= 2 && rho < 0.8) {
		cut = 0;
	} else if (cut > 0 && e->underloaded_windows >= 6) {
		cut = cut >= 4 ? cut / 2 : 0;
	}
	if (cut == e->cut) {
		return;
	}
	e->overloaded_windows = 0;
	e->underloaded_windows = 0;
	debug(D_VINE, "checkpoint cut %d -> %d: large outputs %.1f/s, vault capacity %.1f/s", e->cut, cut, d->large_rate, d->large_capacity);
	if (cut > 0 && !e->cut_noticed) {
		e->cut_noticed = 1;
		debug(D_NOTICE | D_VINE, "large outputs complete faster than the vault anchors them (%.1f/s against %.1f/s), so only the outputs on the boundaries of bands of %d generations are anchored, and a lost output recomputes at most its band; set shared-directory to a file system that every Worker mounts to add its bandwidth to the vault's", d->large_rate, d->large_capacity, cut);
	}
	e->cut = cut;
}

/* Integrate the exposed outputs over time, and close the measurement window when it is over: the rates of completed
 * and anchored outputs, the lag by Little's law, and the vault's capacity for large outputs, measured while some of
 * them always waited for it, and otherwise at least what it anchored. */
static void vine_graph_executor_measure(struct vine_graph_executor *e)
{
	struct vine_graph_window *w = &e->window;
	timestamp_t now = timestamp_get();
	w->exposure_area += (double)e->exposed_outputs * (double)(now - w->sampled) / 1e6;
	w->sampled = now;
	if (now - w->start < VINE_GRAPH_WINDOW_SECONDS * 1000000ULL) {
		return;
	}
	double seconds = (double)(now - w->start) / 1e6;
	struct vine_graph_durability *d = &e->durability;
	d->output_rate = w->outputs / seconds;
	d->large_rate = w->large / seconds;
	d->lag = d->output_rate > 0 ? w->exposure_area / seconds / d->output_rate : 0;
	double anchored = w->large_anchor / seconds;
	int saturated = !w->drained;
	if (saturated) {
		d->large_capacity = d->large_capacity > 0 ? (d->large_capacity + anchored) / 2 : anchored;
	} else {
		d->large_capacity = MAX(d->large_capacity, anchored);
	}
	vine_graph_executor_adapt_cut(e, saturated);
	memset(w, 0, sizeof(*w));
	w->start = w->sampled = now;
}

/* Return non-zero when a lost output of this node would never need the node's parents: the node failed and will never
 * read them, or it completed, is not being recovered, and its outputs are either released or anchored in the vault. */
static int vine_graph_executor_node_is_durable(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (node->failed || node->removed) {
		return 1;
	}
	return node->completed && !vine_graph_node_is_recovering(node) && (node->released || vine_graph_executor_node_is_anchored(node));
}

/* Return non-zero when every submitted child of the node is durable. A child that became durable usually stays so, so
 * the check resumes at the first child that was not durable the last time, and each event costs O(1) until that child
 * changes, instead of a pass over every child. Once the position passes the last child, a full pass confirms the
 * result, because a recovery can make an earlier child non-durable again, and moves the position to such a child. */
static int vine_graph_executor_children_durable(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	while (node->durable_children < node->children.count) {
		if (!vine_graph_executor_node_is_durable(e, node->children.items[node->durable_children])) {
			return 0;
		}
		node->durable_children++;
	}
	for (int i = 0; i < node->children.count; i++) {
		if (!vine_graph_executor_node_is_durable(e, node->children.items[i])) {
			node->durable_children = i;
			return 0;
		}
	}
	return 1;
}

/* Prune the node's temporary outputs once every submitted child is durable and the frontend pins none of them. A lost
 * output then needs at most its parent, never a chain of released ancestors. A consumer submitted after the release
 * makes TaskVine recompute the output, so pins keep outputs that may still be read. Pruning wins over checkpointing.
 * While the vault is full, children cannot become durable, so their parents stay on Workers until downstream nodes
 * complete or the vault has room again. Return non-zero when the node was released. With report set, the frontend
 * hears of the release from vine_graph_executor_next_released(), and a caller that tells the frontend itself clears it. */
static int vine_graph_executor_try_release(struct vine_graph_executor *e, struct vine_graph_node *node, int report)
{
	if (node->released || !node->completed) {
		return 0;
	}
	if (node->pinned_outputs > 0 || node->kept) {
		return 0;
	}
	if (!vine_graph_executor_children_durable(e, node)) {
		return 0;
	}
	node->durable_children = 0;
	for (int i = 0; i < node->outputs.count; i++) {
		struct vine_graph_mount *mount = node->outputs.items[i];
		vine_graph_executor_forget_checkpoint(e, mount->file);
		vine_graph_executor_forget_vault_path(e, mount->file);
		vine_graph_executor_uncount_local(mount);
		vine_prune_file(e->manager, mount->file);
	}
	node->released = 1;
	vine_graph_executor_recount(e, node);
	if (report) {
		vine_graph_executor_report(e->released, node);
	}
	if (node->let_go) {
		vine_graph_id_queue_push(&e->removable, node->node_id);
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
		vine_graph_executor_try_release(e, start, 1);
	}
	/* A parent is released only once every child is durable, so a node that is not durable releases none of them. A
	 * completed node usually is not durable yet, until its checkpoint anchors it or it is released, and every node
	 * that completes would otherwise try each of its parents, which costs a check per edge. */
	if (!vine_graph_executor_node_is_durable(e, start)) {
		return;
	}
	/* A node without parents, such as every call that reads no result, releases nothing upstream, so it allocates no
	 * worklist: the unpin of each of its Futures would otherwise pay for one. */
	if (start->parents.count == 0) {
		return;
	}
	struct list *worklist = list_create();
	for (int i = 0; i < start->parents.count; i++) {
		list_push_tail(worklist, start->parents.items[i]);
	}
	struct vine_graph_node *node;
	while ((node = list_pop_head(worklist))) {
		if (vine_graph_executor_try_release(e, node, 1)) {
			for (int i = 0; i < node->parents.count; i++) {
				list_push_tail(worklist, node->parents.items[i]);
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
	if (success == -2) {
		/* The vault lost a published entry: its copy in the shared directory was deleted outside the vault. */
		list_push_tail(e->checkpoint_lost, file);
		vine_wake(e->manager);
		return;
	}
	if (success < 0) {
		/* The Worker cannot send the file, such as a directory that holds an entry it cannot read, so neither a
		 * checkpoint nor a fetch of it is tried again until its producer runs again. */
		list_push_tail(e->checkpoint_unsendable, file);
		vine_wake(e->manager);
		return;
	}
	if (success) {
		/* The published vault entry anchors the output at once, as the Manager's own queries see it. */
		vine_graph_executor_count_local(e, file);
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

/* Report a requested node output that became readable at the Manager, so the frontend learns its path without asking. */
static void vine_graph_executor_report_fetched(struct vine_graph_executor *e, struct vine_file *file)
{
	struct vine_graph_mount *mount = vine_graph_find_output(e->graph, file);
	if (mount) {
		list_push_tail(e->fetched, (void *)(uintptr_t)mount->file_id);
	}
}

/* Queue a pending fetch to be tried in the next round, when its file may have become receivable, in the list of its
 * size: a small file comes inline, which a fetch never waits for, so it must not wait behind large files that find no
 * receive slot. The entry holds a reference to its file. */
static void vine_graph_executor_fetch_ready(struct vine_graph_executor *e, struct vine_file *file)
{
	if (set_lookup(e->fetch_pending, file)) {
		list_push_tail(vine_graph_file_is_large(file) ? e->fetch_ready : e->fetch_small, vine_file_addref(file));
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

/* Admit files from one checkpoint queue in priority order until the Manager refuses, and return zero when the vault is
 * full. BUSY means no free slot for the file's kind of receive, and a file without a ready source cannot be received
 * now. Such a file is set aside for this round so it does not block lower-priority files, which may have a source, and
 * admission stops after SKIP_LIMIT of them. A file set aside returns to the queue of its size now. */
static int vine_graph_executor_admit_checkpoints(struct vine_graph_executor *e, struct priority_queue *queue)
{
	enum { SKIP_LIMIT = 16 };
	struct vine_file *skipped[SKIP_LIMIT];
	double skipped_priority[SKIP_LIMIT];
	int skips = 0;
	int room = 1;
	struct vine_file *file;
	while (skips < SKIP_LIMIT && (file = priority_queue_peek_top(queue))) {
		double priority = priority_queue_get_top_priority(queue);
		if (!set_lookup(e->checkpoint_pending, file) || set_lookup(e->checkpoint_parked, file)) {
			/* Withdrawn, or parked by an earlier entry for the same file. */
			vine_file_delete(priority_queue_pop(queue));
			continue;
		}
		if (queue == e->checkpoint_queue && vine_graph_executor_hold(e, file)) {
			/* The hold keeps the entry's reference. */
			priority_queue_pop(queue);
			continue;
		}
		vine_manager_checkpoint_result_t result = vine_manager_data_service_checkpoint(e->manager, file, vine_graph_executor_checkpoint_complete, e);
		if (result == VINE_CHECKPOINT_FULL) {
			vine_graph_executor_note_vault_full(e);
			room = 0;
			break;
		}
		priority_queue_pop(queue);
		if (result == VINE_CHECKPOINT_BUSY || (result == VINE_CHECKPOINT_NO_SOURCE && vine_temp_exists_somewhere(e->manager, file))) {
			/* The skipped entry keeps its reference and returns to a queue below. */
			skipped[skips] = file;
			skipped_priority[skips] = priority;
			skips++;
			continue;
		}
		if (result == VINE_CHECKPOINT_NO_SOURCE) {
			/* No replica can serve it, so it waits outside the queue until a task recreates it. */
			set_insert(e->checkpoint_parked, file);
		}
		set_remove(e->yielded, file);
		vine_file_delete(file);
	}
	for (int i = 0; i < skips; i++) {
		priority_queue_push(vine_graph_executor_checkpoint_queue(e, skipped[i]), skipped[i], skipped_priority[i]);
	}
	return room;
}

/* Settle reported results, then admit queued files from each queue in priority order until the Manager refuses.
 * Failed files return to the queue once their retry time passed. FULL means no vault space and stops admission. Large
 * and small files wait in queues of their own, because they wait for different slots: a small output whose producer
 * ran briefly waited behind every large output that found no receive slot, while the inline receives it needs were
 * free. Results of released files are ignored. */
static void vine_graph_executor_process_checkpoints(struct vine_graph_executor *e)
{
	struct vine_file *file;
	while ((file = list_pop_head(e->checkpoint_completed))) {
		/* A received file serves both a checkpoint and a fetch of it. */
		if (set_remove(e->fetch_pending, file)) {
			vine_graph_executor_report_fetched(e, file);
		}
		/* A fetch of a held file publishes it too. */
		set_remove(e->held, file);
		set_remove(e->yielded, file);
		if (set_remove(e->checkpoint_pending, file)) {
			/* A published checkpoint anchors its node, which may let its ancestors be released. */
			vine_graph_executor_release_from(e, vine_graph_get_producer(e->graph, file), 0);
		}
		vine_file_delete(file);
	}
	while ((file = list_pop_head(e->checkpoint_unsendable))) {
		struct vine_graph_mount *mount = vine_graph_find_output(e->graph, file);
		if (set_remove(e->fetch_pending, file) && mount) {
			list_push_tail(e->fetch_failed, (void *)(uintptr_t)mount->file_id);
		}
		set_remove(e->held, file);
		set_remove(e->yielded, file);
		if (set_remove(e->checkpoint_pending, file) && mount) {
			debug(D_NOTICE | D_VINE, "the output %s of a task cannot be sent by its Worker, which cannot read an entry of it, so it stays on Workers without a checkpoint", mount->task_path);
		}
		vine_file_delete(file);
	}
	while ((file = list_pop_head(e->checkpoint_lost))) {
		/* The output is no longer anchored. It is checkpointed again once a replica exists, which the Manager's
		 * recovery task makes when a reader needs it, and a pinned output, which the frontend may read from the vault,
		 * is fetched again, which restores its producer when no replica exists. */
		struct vine_graph_mount *mount = vine_graph_find_output(e->graph, file);
		vine_graph_executor_forget_vault_path(e, file);
		if (mount && mount->local) {
			debug(D_VINE, "the vault lost the output %s, whose copy in the shared directory was deleted, so it is checkpointed again", mount->task_path);
			vine_graph_executor_uncount_local(mount);
			vine_graph_executor_recount(e, mount->node);
			vine_graph_executor_queue_checkpoint(e, mount->node, file);
			if (mount->pins > 0 && !set_lookup(e->fetch_pending, file)) {
				set_insert(e->fetch_pending, file);
				vine_graph_executor_fetch_ready(e, file);
			}
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

	vine_graph_executor_expire_holds(e);
	/* Small files first, because a refusal of either kind says nothing about the other's slots, and the vault takes small
	 * files from memory. A full vault refuses a large file when it refuses a small one. */
	if (vine_graph_executor_admit_checkpoints(e, e->checkpoint_small)) {
		vine_graph_executor_admit_checkpoints(e, e->checkpoint_queue);
	}
	/* A round that leaves no large output waiting shows that the vault keeps up. Entries of withdrawn files count until
	 * they reach the head. */
	if (priority_queue_size(e->checkpoint_queue) == 0) {
		e->window.drained = 1;
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
	for (int i = 0; i < node->parents.count; i++) {
		parent = node->parents.items[i];
		parent_failed = parent_failed || parent->failed;
	}
	if (!parent_failed) {
		return NULL;
	}
	for (int i = 0; i < node->inputs.count; i++) {
		struct vine_graph_mount *mount = node->inputs.items[i];
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
	for (int i = 0; i < node->inputs.count; i++) {
		struct vine_graph_mount *mount = node->inputs.items[i];
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
	vine_graph_executor_reader_done(e, node);
	if (!node->completed) {
		e->finished_nodes++;
	}
	vine_graph_executor_cancel_task(e, node->task_id);
	for (int i = 0; i < node->outputs.count; i++) {
		struct vine_graph_mount *mount = node->outputs.items[i];
		set_remove(e->fetch_pending, mount->file);
		if (vine_file_is_recovering(mount->file)) {
			vine_graph_executor_cancel_task(e, vine_task_get_id(mount->file->recovery_task));
		}
	}
	vine_graph_executor_report(e->finished, node);
	if (node->let_go) {
		vine_graph_id_queue_push(&e->removable, node->node_id);
	}
	vine_graph_executor_recount(e, node);
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
		for (int i = 0; i < current->outputs.count; i++) {
			struct vine_graph_mount *mount = current->outputs.items[i];
			lost += vine_graph_executor_output_lost(e, current, mount->file);
		}
		if (lost == 0) {
			continue;
		}
		int all_lost = lost == current->outputs.count;
		for (int i = 0; i < current->children.count; i++) {
			struct vine_graph_node *child = current->children.items[i];
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
 * outputs. failure and error are set only when node is the source. */
static void vine_graph_executor_fail_node(struct vine_graph_executor *e, struct vine_graph_node *node, uint64_t source_id, vine_graph_failure_t failure, char *error)
{
	if (node->failed) {
		free(error);
		return;
	}
	if (node->node_id == source_id) {
		node->error = error;
		node->failure = failure;
		size_t length = error ? strlen(error) : 0;
		if (length > VINE_GRAPH_ERROR_LOGGED) {
			debug(D_VINE, "node %" PRIu64 " failed: %.*s... (%zu bytes)", node->node_id, VINE_GRAPH_ERROR_LOGGED, error, length);
		} else {
			debug(D_VINE, "node %" PRIu64 " failed: %s", node->node_id, error ? error : "");
		}
	}
	vine_graph_executor_mark_failed(e, node, source_id);
	vine_graph_executor_fail_dependents(e, node);
	vine_graph_executor_release_from(e, node, 0);
}

/* Return the first Manager-local input of a node whose source is no longer readable, or NULL.
 * Edata and user input files are VINE_FILEs that cannot be recomputed. */
static struct vine_file *vine_graph_executor_find_lost_input(struct vine_graph_node *node)
{
	for (int i = 0; i < node->inputs.count; i++) {
		struct vine_graph_mount *mount = node->inputs.items[i];
		if (vine_file_type(mount->file) == VINE_FILE && access(vine_file_source(mount->file), R_OK) != 0) {
			return mount->file;
		}
	}
	return NULL;
}

/* Return non-zero when the node function ran and exited with an error, such as an exception. Its outputs are then
 * missing only because of that error, and its standard output may be longer than the Manager keeps. */
static int vine_graph_task_function_failed(struct vine_task *task)
{
	vine_result_t result = vine_task_get_result(task);
	return vine_task_get_exit_code(task) > 0 && (result == VINE_RESULT_SUCCESS || result == VINE_RESULT_OUTPUT_MISSING || result == VINE_RESULT_STDOUT_MISSING);
}

/* Return the sandbox path of an output that a task that succeeded left uncreated, or NULL. A Worker reports a missing
 * temporary output only by invalidating its cache entry, so the task itself still succeeds. Each new task of the node
 * resets its outputs to pending, and the Manager processes the Worker's cache updates before returning the task. */
static const char *vine_graph_node_output_missing(const struct vine_graph_node *node)
{
	for (int i = 0; i < node->outputs.count; i++) {
		struct vine_graph_mount *mount = node->outputs.items[i];
		if (mount->file->state != VINE_FILE_STATE_CREATED) {
			return mount->task_path;
		}
	}
	return NULL;
}

/* Handle a failed attempt with its result. A function that raised, a lost Manager-local input, or too many failures
 * fail the node and its dependents. Other failures are infrastructure failures: the executor resubmits its own task,
 * while the Manager resubmits a recovery task itself when a consumer needs its output again. missing_output names an
 * output that the task did not create, or is NULL. */
static void vine_graph_executor_handle_failure(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_task *task, vine_result_t result, const char *missing_output, int is_recovery)
{
	if (vine_graph_task_function_failed(task)) {
		const char *output = vine_task_get_stdout(task);
		/* A function call reports the status that waitpid() returned for its process. */
		int status = vine_task_get_exit_code(task);
		if (WIFSIGNALED(status)) {
			vine_graph_executor_fail_node(e, node, node->node_id, VINE_GRAPH_FAILURE_SIGNAL, string_format("its function was killed by signal %d (%s):\n%s", WTERMSIG(status), strsignal(WTERMSIG(status)), output ? output : ""));
		} else {
			vine_graph_executor_fail_node(e, node, node->node_id, VINE_GRAPH_FAILURE_FUNCTION, string_format("its function failed:\n%s", output ? output : ""));
		}
		return;
	}
	if (result == VINE_RESULT_MAX_RETRIES) {
		vine_graph_executor_fail_node(e, node, node->node_id, VINE_GRAPH_FAILURE_WORKERS, string_format("%d workers disconnected while running its task, at times when no other workers were being lost; a task that exceeds the memory of its worker or crashes it makes the worker exit", MAX(e->max_retries, 1) + 1));
		return;
	}
	struct vine_file *lost = result == VINE_RESULT_INPUT_MISSING ? vine_graph_executor_find_lost_input(node) : NULL;
	if (lost) {
		/* A frontend may declare a link of its own to the caller's file, whose path is what the caller knows. */
		char target[PATH_MAX];
		ssize_t length = readlink(vine_file_source(lost), target, sizeof(target) - 1);
		if (length > 0) {
			target[length] = 0;
			vine_graph_executor_fail_node(e, node, node->node_id, VINE_GRAPH_FAILURE_INPUT, string_format("its input %s is missing at the Manager", target));
		} else {
			vine_graph_executor_fail_node(e, node, node->node_id, VINE_GRAPH_FAILURE_INPUT, string_format("its input %s is missing at the Manager", vine_file_source(lost)));
		}
		return;
	}
	if (++node->failures > e->max_retries) {
		if (missing_output) {
			vine_graph_executor_fail_node(e, node, node->node_id, VINE_GRAPH_FAILURE_RETRIES, string_format("it failed %d times; last result: %s: the task did not create its output %s", node->failures, vine_result_string(result), missing_output));
		} else {
			vine_graph_executor_fail_node(e, node, node->node_id, VINE_GRAPH_FAILURE_RETRIES, string_format("it failed %d times; last result: %s", node->failures, vine_result_string(result)));
		}
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
		/* A released node that runs again was asked for after its release, by a fetch or by a consumer submitted
		 * later, or it is an ancestor that such a request needed. Requests of this kind tend to come again for its
		 * neighbours, which would recompute the same ancestors each time, so the node is kept, never released, until
		 * the frontend lets it go (vine_graph_executor_unkeep_node()). It is kept before the release walk below, in
		 * which its consumers may already release it. */
		if (node->released && !node->kept) {
			node->kept = 1;
			vine_graph_executor_report(e->kept, node);
		}
		node->released = 0;
	} else {
		node->completed = 1;
	}
	vine_graph_executor_recount(e, node);
	for (int i = 0; i < node->outputs.count; i++) {
		struct vine_graph_mount *mount = node->outputs.items[i];
		e->window.outputs++;
		e->window.large += vine_graph_file_is_large(mount->file);
		if (set_remove(e->checkpoint_parked, mount->file)) {
			vine_graph_executor_push_checkpoint(e, node, mount->file);
		}
		vine_graph_executor_queue_checkpoint(e, node, mount->file);
		vine_graph_executor_fetch_ready(e, mount->file);
	}

	if (restores) {
		e->completed_recovery_tasks++;
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
	vine_graph_executor_reader_done(e, node);
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
	/* The Manager keeps at most max_task_stdout_storage (1 GB) of a task's standard output and reports the rest
	 * missing. That says nothing about the task's outputs, and running the task again prints as much again. */
	if (result == VINE_RESULT_STDOUT_MISSING && vine_task_get_exit_code(task) == 0) {
		debug(D_VINE, "node %" PRIu64 " task %d printed more standard output than the Manager keeps", node->node_id, vine_task_get_id(task));
		result = VINE_RESULT_SUCCESS;
	}
	const char *missing_output = NULL;
	if (result == VINE_RESULT_SUCCESS && vine_task_get_exit_code(task) == 0) {
		missing_output = vine_graph_node_output_missing(node);
		if (missing_output) {
			result = VINE_RESULT_OUTPUT_MISSING;
		}
	}
	if (result != VINE_RESULT_SUCCESS || vine_task_get_exit_code(task) != 0) {
		vine_graph_executor_handle_failure(e, node, task, result, missing_output, is_recovery);
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
	/* A returned attempt is not needed anymore, and the node keeps only its id. The Manager keeps its own copy of
	 * the task as the recovery task of its temporary outputs, which it runs when an output is lost and needed, and the
	 * caller deletes the reference of a returned recovery task. A failed attempt that was submitted again was already
	 * replaced. */
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
		vine_graph_executor_fail_node(e, node, failed_parent->failure_source_id, VINE_GRAPH_FAILURE_NONE, NULL);
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
	/* The producer counts its pinned outputs, which its release check reads, as a mount gains its first pin or loses
	 * its last one. */
	if ((mount->pins == 0) != (mount->pins + delta == 0)) {
		struct vine_graph_node *producer = mount->node;
		producer->pinned_outputs += mount->pins == 0 ? 1 : -1;
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
		struct vine_graph_node *producer = mount->node;
		/* A pin on a released node asked for its outputs again, and the frontend took the node back. A node that was not
		 * restored meanwhile is still released, so the frontend hears so again, from the result, and lets it go. */
		if (producer->released && !producer->restoring) {
			return 1;
		}
		/* The last pin may have held back the producer and, through it, its ancestors. Pins do not make a node durable,
		 * so its ancestors can be released only when the producer is, and a producer that stays is not walked from:
		 * the last readers of a wide layer would otherwise each walk every parent of the producer they unpin. The
		 * caller hears of the producer's release from the result rather than from a report, which would cost the
		 * frontend another call and a lookup of the node for every Future it drops. Released ancestors are reported. */
		if (vine_graph_executor_try_release(e, producer, 0)) {
			vine_graph_executor_release_from(e, producer, 0);
			return 1;
		}
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
	if (producer->failed) {
		return 0;
	}
	if (vine_manager_data_service_vault_contains(e->manager, mount->file)) {
		vine_graph_executor_report_fetched(e, mount->file);
	} else {
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
		vine_graph_executor_report_fetched(e, file);
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

/* Try the fetches of one list that can make progress, in the order they became ready, until the Manager has no free
 * receive slot. A file whose replica cannot serve it yet is tried again later, at most SKIP_LIMIT of them per round. A
 * file of the small list that the Manager refused for want of a slot has grown, and moves to the large list. */
static void vine_graph_executor_fetch_list(struct vine_graph_executor *e, struct list *ready)
{
	enum { SKIP_LIMIT = 16 };
	int skips = 0;
	struct vine_file *file;
	while (skips < SKIP_LIMIT && (file = list_pop_head(ready))) {
		vine_graph_fetch_result_t result = vine_graph_executor_fetch_one(e, file);
		if (result == VINE_GRAPH_FETCH_BUSY) {
			if (ready == e->fetch_small) {
				list_push_tail(e->fetch_ready, file);
				continue;
			}
			list_push_head(ready, file);
			break;
		}
		if (result == VINE_GRAPH_FETCH_LATER) {
			list_push_tail(ready, file);
			skips++;
			continue;
		}
		vine_file_delete(file);
	}
}

/* Try the pending fetches that can make progress, small files first. A file in the vault is done. A file with a Worker
 * replica is received into the vault with priority over checkpoints and beyond the vault limit, and its receive
 * reports back. A completed node whose output exists nowhere is run again to restore it, unless a recovery task is
 * already recreating it, and the restoring task queues the fetch again. The work of a round depends on the free slots,
 * not on how many fetches are pending. Decided on 2026-10-22: with every output of 1,600 x 64 MiB pinned on the
 * cluster, the 400 results of 51 bytes that the caller waited for were fetched behind the large ones, up to 10 minutes
 * after their tasks completed. */
static void vine_graph_executor_process_fetches(struct vine_graph_executor *e)
{
	vine_graph_executor_fetch_list(e, e->fetch_small);
	vine_graph_executor_fetch_list(e, e->fetch_ready);
}

int vine_graph_executor_get_local_data(struct vine_graph_executor *e, uint64_t file_id, const char **data, int64_t *length)
{
	struct vine_file *file = e ? vine_graph_get_file(e->graph, file_id) : NULL;
	*data = NULL;
	*length = 0;
	if (!file || !vine_graph_get_producer(e->graph, file)) {
		return 0;
	}
	*data = vine_manager_data_service_vault_data(e->manager, file, length, NULL);
	return *data != NULL;
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
	for (int i = 0; i < node->outputs.count; i++) {
		struct vine_graph_mount *mount = node->outputs.items[i];
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
	vine_graph_executor_reader_done(e, node);
	struct vine_graph_mount *mount;
	for (int i = 0; i < node->outputs.count; i++) {
		mount = node->outputs.items[i];
		if (vine_file_is_recovering(mount->file)) {
			vine_graph_executor_collect_task(e, vine_task_get_id(mount->file->recovery_task));
		}
	}
	if (node->task_id) {
		/* Earlier attempts keep their entries, so a late recovery task of the node is recognized and ignored. */
		itable_remove(e->task_id_to_node, (uint64_t)node->task_id);
	}
	e->exposed_outputs -= node->exposed;
	node->exposed = 0;
	for (int i = 0; i < node->outputs.count; i++) {
		mount = node->outputs.items[i];
		vine_graph_executor_forget_checkpoint(e, mount->file);
		set_remove(e->fetch_pending, mount->file);
		vine_graph_executor_forget_vault_path(e, mount->file);
		vine_graph_executor_uncount_local(mount);
		vine_graph_remove_file(e->graph, mount->file_id);
		/* An output is a temporary file that vine_graph_executor_add_output() declared under a random cache name, so
		 * the Manager's table holds this file object and no other declaration shares it, and it is undeclared without
		 * a lookup of its name. */
		vine_undeclare_file(e->manager, mount->file);
	}
	vine_graph_executor_undeclare_owned(e, node);
	for (int i = 0; i < node->parents.count; i++) {
		struct vine_graph_node *parent = node->parents.items[i];
		if (--parent->live_children == 0 && parent->let_go) {
			vine_graph_id_queue_push(&e->removable, parent->node_id);
		}
	}
	uint64_t node_id = node->node_id;
	vine_graph_remove_node(e->graph, node);
	vine_graph_id_queue_push(&e->removed, node_id);
	debug(D_VINE, "removed node %" PRIu64, node_id);
}

int vine_graph_executor_remove_dropped(struct vine_graph_executor *e, int limit)
{
	if (!e) {
		return 0;
	}
	uint64_t id;
	/* Nodes are queued by id, so a node removed meanwhile is skipped. Each entry counts against the limit, so a call
	 * that meets a long chain of dropped ancestors stops and leaves the rest queued. */
	while (limit-- > 0 && (id = vine_graph_id_queue_pop(&e->removable))) {
		struct vine_graph_node *node = vine_graph_get_node(e->graph, id);
		if (node && vine_graph_executor_node_is_removable(node)) {
			vine_graph_executor_remove_node(e, node);
		}
	}
	return e->removable.tail - e->removable.head;
}

int vine_graph_executor_drop_node(struct vine_graph_executor *e, uint64_t node_id)
{
	struct vine_graph_node *node = e ? vine_graph_get_node(e->graph, node_id) : NULL;
	if (!node || !node->submitted) {
		return -1;
	}
	node->let_go = 1;
	vine_graph_id_queue_push(&e->removable, node_id);
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

/* Free the removed entries of the tables that grow with nodes and files, so the memory of removed nodes can be reused
 * (see compact_tables() in vine_manager.c). Without it, a table that only shrinks after a Graph is dropped keeps every removed entry. This runs
 * between the steps of a wait, outside every walk of these tables, and costs amortized O(1) per removal. */
static void vine_graph_executor_compact_tables(struct vine_graph_executor *e)
{
	itable_compact(e->graph->nodes);
	itable_compact(e->graph->files);
	itable_compact(e->graph->producers);
	itable_compact(e->task_id_to_node);
	itable_compact(e->vault_paths);
	set_compact(e->checkpoint_pending);
	set_compact(e->checkpoint_parked);
	set_compact(e->fetch_pending);
	set_compact(e->held);
	set_compact(e->yielded);
}

vine_graph_status_t vine_graph_executor_wait(struct vine_graph_executor *e, int timeout)
{
	if (!e) {
		return VINE_GRAPH_FAILED;
	}
	if (e->error) {
		/* The run stopped as a whole, and none of its tasks can complete, so they are cancelled once. The Manager
		 * still runs until the timeout or a wake, so status requests, keepalives, and Workers are answered until the
		 * frontend deletes the executor. */
		if (!e->tasks_cancelled) {
			vine_graph_executor_cancel_tasks(e);
			e->tasks_cancelled = 1;
		}
		struct vine_task *task = vine_wait(e->manager, MAX(timeout, 0));
		while (task) {
			vine_graph_executor_release_returned(task);
			task = vine_wait(e->manager, 0);
		}
		return VINE_GRAPH_FAILED;
	}
	time_t stoptime = time(NULL) + MAX(timeout, 0);
	int wait_timeout = 0;
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
		/* Fetches come first, so a receive slot freed during the last wait goes to an output the caller waits for.
		 * The requests and unlinks they send leave in one write per worker. */
		vine_manager_send_hold(e->manager);
		vine_graph_executor_process_fetches(e);
		vine_graph_executor_process_checkpoints(e);
		vine_graph_executor_remove_dropped(e, VINE_GRAPH_REMOVALS_PER_STEP);
		vine_manager_send_release(e->manager);
		vine_graph_executor_measure(e);
		vine_graph_executor_compact_tables(e);
		/* An idle run draws no bar, because each DONE finishes the bar it drew. */
		if (!done) {
			vine_graph_executor_update_progress_bar(e);
		}
		/* The caller learns at once that a node finished or that a fetched output became readable. Results that
		 * arrived during the last Manager wait are settled above before any return. */
		if (list_size(e->finished) > 0 || list_size(e->fetched) > 0 || list_size(e->fetch_failed) > 0) {
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
			 * A wait of zero seconds only collects returned tasks, so the unlinks of the files that the batch
			 * releases leave in one write per worker. */
			vine_manager_send_hold(e->manager);
			do {
				/* Handling may delete a returned attempt, so whether this is a recovery task is decided first. */
				int recovery = vine_task_get_recovery_source_task_id(task) > 0;
				vine_graph_executor_handle_task(e, task);
				if (recovery) {
					vine_task_delete(task);
				}
			} while ((task = vine_wait(e->manager, 0)));
			vine_manager_send_release(e->manager);
			wait_timeout = 0;
		} else if (wait_timeout > 0 || time(NULL) >= stoptime) {
			expired = 1;
		} else {
			wait_timeout = MAX(stoptime - time(NULL), 1);
		}
	}

	vine_graph_executor_remove_dropped(e, VINE_GRAPH_REMOVALS_PER_STEP);
	vine_graph_executor_finish_progress_bar(e);
	return e->error ? VINE_GRAPH_FAILED : VINE_GRAPH_DONE;
}

void vine_graph_executor_wake(struct vine_graph_executor *e)
{
	vine_wake(e->manager);
}

void vine_graph_executor_hold_sends(struct vine_graph_executor *e)
{
	if (e) {
		vine_manager_send_hold(e->manager);
	}
}

void vine_graph_executor_release_sends(struct vine_graph_executor *e)
{
	if (e) {
		vine_manager_send_release(e->manager);
	}
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

uint64_t vine_graph_executor_next_kept(struct vine_graph_executor *e)
{
	return e ? vine_graph_executor_next_report(e->kept) : 0;
}

int vine_graph_executor_unkeep_node(struct vine_graph_executor *e, uint64_t node_id)
{
	struct vine_graph_node *node = e ? vine_graph_get_node(e->graph, node_id) : NULL;
	if (!node || !node->kept) {
		debug(D_ERROR, "node %" PRIu64 " is not kept", node_id);
		return -1;
	}
	node->kept = 0;
	/* As for the last unpin, the caller hears of the node's release from the result, and released ancestors are
	 * reported. */
	if (vine_graph_executor_try_release(e, node, 0)) {
		vine_graph_executor_release_from(e, node, 0);
		return 1;
	}
	return 0;
}

uint64_t vine_graph_executor_next_removed(struct vine_graph_executor *e)
{
	return e ? vine_graph_id_queue_pop(&e->removed) : 0;
}

uint64_t vine_graph_executor_next_fetched(struct vine_graph_executor *e)
{
	return e ? (uint64_t)(uintptr_t)list_pop_head(e->fetched) : 0;
}

uint64_t vine_graph_executor_next_fetch_failed(struct vine_graph_executor *e)
{
	return e ? (uint64_t)(uintptr_t)list_pop_head(e->fetch_failed) : 0;
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

vine_graph_failure_t vine_graph_executor_get_node_failure(const struct vine_graph_executor *e, uint64_t node_id)
{
	struct vine_graph_node *node = e ? vine_graph_get_node(e->graph, node_id) : NULL;
	struct vine_graph_node *source = node && node->failed ? vine_graph_get_node(e->graph, node->failure_source_id) : NULL;
	return source ? source->failure : VINE_GRAPH_FAILURE_NONE;
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

struct vine_graph_capacity vine_graph_executor_get_capacity(struct vine_graph_executor *e)
{
	struct vine_graph_capacity capacity = {0, 0};
	if (!e) {
		return capacity;
	}
	struct vine_stats stats;
	vine_get_stats(e->manager, &stats);
	capacity.workers = stats.workers_connected;
	capacity.cores = stats.total_cores;
	return capacity;
}

struct vine_graph_durability vine_graph_executor_get_durability(const struct vine_graph_executor *e)
{
	struct vine_graph_durability durability = {0};
	if (!e) {
		return durability;
	}
	durability = e->durability;
	durability.exposed = e->exposed_outputs;
	durability.pending = set_size(e->checkpoint_pending);
	durability.cut = e->cut;
	return durability;
}

int vine_graph_executor_cancel_node(struct vine_graph_executor *e, uint64_t node_id)
{
	struct vine_graph_node *node = e ? vine_graph_get_node(e->graph, node_id) : NULL;
	if (!node || !node->submitted || node->completed || node->failed) {
		return -1;
	}
	vine_graph_executor_fail_node(e, node, node->node_id, VINE_GRAPH_FAILURE_CANCELLED, xxstrdup("it was cancelled"));
	return 0;
}
