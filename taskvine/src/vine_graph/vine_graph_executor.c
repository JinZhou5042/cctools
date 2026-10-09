#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "debug.h"
#include "vine_graph_executor.h"
#include "macros.h"
#include "priority_queue.h"
#include "progress_bar.h"
#include "random.h"
#include "set.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include "taskvine.h"
#include "vine_manager_data_service.h"
#include "vine_manager.h"
#include "vine_file.h"

static volatile sig_atomic_t interrupted = 0;

static void vine_graph_executor_submit_node(struct vine_graph_executor *e, struct vine_graph_node *node);
static struct vine_task *vine_graph_executor_make_vine_task(struct vine_graph_executor *e);
static void vine_graph_executor_materialize_node(struct vine_graph_executor *e, struct vine_graph_node *node);
static void vine_graph_executor_run_completion_postprocess(struct vine_graph_executor *e, struct vine_graph_node *node, int release_self);

static void vine_graph_io_mount_add(struct list *lst, struct vine_file *f, const char *remote_name)
{
	struct vine_graph_io_mount *m = xxmalloc(sizeof(*m));
	m->file = f;
	m->remote_name = xxstrdup(remote_name);
	list_push_tail(lst, m);
}

/* Undeclare runner infile buffer (before discarding the vine_task). */
static void vine_graph_executor_clear_node_runner_arg(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (!e || !node || !node->task_runner_arg_file) {
		return;
	}
	vine_undeclare_file(e->manager, node->task_runner_arg_file);
	node->task_runner_arg_file = NULL;
}

/* Initialize runtime fields and default tuning values for a new executor. */
static void vine_graph_executor_init_runtime(struct vine_graph_executor *e)
{
	if (!e) {
		return;
	}

	e->task_id_to_node = itable_create(0);
	e->resubmit_queue = list_create();
	e->checkpoint_queue = priority_queue_create(0);
	e->checkpoint_completed = list_create();
	e->checkpoint_failed = list_create();
	e->checkpoint_pending = set_create(0);
	e->checkpoint_sequence = 0;
	e->time_first_task_dispatched = UINT64_MAX; // sentinel until first task commit time
	e->time_last_task_retrieved = 0;
	e->makespan_us = 0;
	e->completed_recovery_tasks = 0;
	e->time_spent_on_cut_propagation = 0;
	e->total_preprocessing_time_us = 0;
	e->total_postprocessing_time_us = 0;
	e->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST;
	e->progress_bar_update_interval_sec = 0.1;
	/* Release a temporary output once its direct children are complete and durable. Zero leaves only cut
	 * propagation. */
	e->prune_depth = 1;
}

static int vine_graph_task_not_submitted(struct vine_task *task)
{
	return !task || vine_task_get_id(task) <= 0;
}

/* Release runtime lookups and queues. Their nodes and files are owned by the graph. */
static void vine_graph_executor_clear_runtime(struct vine_graph_executor *e)
{
	if (!e) {
		return;
	}
	priority_queue_delete(e->checkpoint_queue);
	list_delete(e->checkpoint_completed);
	list_delete(e->checkpoint_failed);
	set_delete(e->checkpoint_pending);
	if (e->task_id_to_node) {
		itable_delete(e->task_id_to_node);
		e->task_id_to_node = NULL;
	}
	if (e->resubmit_queue) {
		list_delete(e->resubmit_queue);
		e->resubmit_queue = NULL;
	}
}

/* The executor owns its graph and Manager declarations for the entire run. */
struct vine_graph_executor *vine_graph_executor_create(struct vine_manager *manager, const char *task_runner_library_name, const char *task_runner_function_name)
{
	if (!manager || !task_runner_library_name || !task_runner_function_name) {
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
	e->output_dir = xxstrdup(vine_get_runtime_directory(manager));
	e->task_runner_library_name = xxstrdup(task_runner_library_name);
	e->task_runner_function_name = xxstrdup(task_runner_function_name);
	vine_graph_executor_init_runtime(e);
	return e;
}

/* Stop only this run's tasks, including Manager-created recovery tasks, before undeclaring inputs. */
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

/* Stop tasks, release Manager declarations, then destroy the graph's own references.
 * Python's run scope owns local paths and decides which returned files survive.
 */
void vine_graph_executor_delete(struct vine_graph_executor *e)
{
	if (!e) {
		return;
	}
	struct vine_graph *g = e->graph;
	vine_graph_executor_cancel_tasks(e);
	uint64_t id;
	int iteration;
	struct vine_graph_node *node;
	ITABLE_ITERATE(g->nodes, iteration, id, node)
	{
		vine_graph_executor_clear_node_runner_arg(e, node);
		struct vine_graph_io_mount *mount;
		LIST_ITERATE(node->outputs, mount)
		{
			vine_undeclare_file(e->manager, mount->file);
		}
	}
	struct vine_file *file;
	ITABLE_ITERATE(g->file_id_to_file, iteration, id, file)
	{
		/* Output declarations were released above. Their mounts still hold live references. */
		if (!hash_table_lookup(g->outfile_cachename_to_node, vine_file_cached_name(file))) {
			vine_undeclare_file(e->manager, file);
		}
	}
	vine_graph_executor_clear_runtime(e);
	vine_graph_delete(g);
	free(e->output_dir);
	free(e->task_runner_library_name);
	free(e->task_runner_function_name);
	free(e);
}

/*
 * Create a new library task (not yet published on the node). The caller attaches IO, then sets
 * node->task only when the task is fully configured (atomic materialize).
 */
static struct vine_task *vine_graph_executor_make_vine_task(struct vine_graph_executor *e)
{
	struct vine_task *t = vine_task_create(e->task_runner_function_name);
	if (!t) {
		return NULL;
	}
	vine_task_set_library_required(t, e->task_runner_library_name);
	return t;
}

/*
 * Attach inputs, outputs, and the infile buffer to a new vine_task at submit time. The node's
 * task pointer stays NULL until that bundle is complete and ready for vine_submit.
 */
static void vine_graph_executor_materialize_node(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}

	/* An existing task already owns its mounts and runner infile. */
	if (node->task) {
		return;
	}

	vine_graph_executor_clear_node_runner_arg(e, node);

	struct vine_task *t = vine_graph_executor_make_vine_task(e);
	if (!t) {
		return;
	}

	void *item;
	LIST_ITERATE(node->outputs, item)
	{
		struct vine_graph_io_mount *m = (struct vine_graph_io_mount *)item;
		vine_task_add_output(t, m->file, m->remote_name, VINE_TRANSFER_ALWAYS);
	}

	struct vine_graph_node *parent_node;
	LIST_ITERATE(node->parents, parent_node)
	{
		if (parent_node && parent_node->outfile) {
			vine_task_add_input(t, parent_node->outfile, parent_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
		}
	}

	LIST_ITERATE(node->extra_inputs, item)
	{
		struct vine_graph_io_mount *m = (struct vine_graph_io_mount *)item;
		if (!vine_task_add_input(t, m->file, m->remote_name, VINE_TRANSFER_ALWAYS)) {
			goto fail_task;
		}
	}

	char *task_arguments = vine_graph_node_construct_task_arguments(node);
	if (!task_arguments) {
		goto fail_task;
	}
	struct vine_file *arg_file =
			vine_declare_buffer(e->manager, task_arguments, strlen(task_arguments), VINE_CACHE_LEVEL_TASK, VINE_UNLINK_WHEN_DONE);
	free(task_arguments);
	if (!arg_file) {
		goto fail_task;
	}
	vine_task_add_input(t, arg_file, "infile", VINE_TRANSFER_ALWAYS);

	node->task = t; // own the initial reference; Manager retains its own reference while submitted
	node->task_runner_arg_file = arg_file;
	return;

fail_task:
	vine_task_delete(t);
}

/* Declare one node output and mount it at task_path. A retrieved target is a local file named local_name in the
 * output directory. Any other output is idata, a temporary file. Return the declared file, or NULL on failure. */
static struct vine_file *vine_graph_executor_declare_output(struct vine_graph_executor *e, struct vine_graph_node *node, const char *task_path, const char *local_name, int is_target)
{
	struct vine_file *file;
	if (is_target) {
		char *path = string_format("%s/%s", e->output_dir, local_name);
		file = vine_declare_file(e->manager, path, VINE_CACHE_LEVEL_WORKFLOW, 0);
		free(path);
	} else {
		file = vine_declare_temp(e->manager);
	}
	if (!file) {
		return NULL;
	}
	vine_graph_node_add_output(node, file, task_path);
	hash_table_insert(e->graph->outfile_cachename_to_node, vine_file_cached_name(file), node);
	return file;
}

/* Allocate the next graph node. Its task and mounts are created when the node is submitted. */
uint64_t vine_graph_executor_add_node(struct vine_graph_executor *e)
{
	return e ? vine_graph_add_node(e->graph) : 0;
}

int vine_graph_executor_set_target(struct vine_graph_executor *e, uint64_t node_id)
{
	struct vine_graph_node *node = e ? vine_graph_get_node(e->graph, node_id) : NULL;
	if (!node) {
		return -1;
	}
	node->is_target = 1;
	return 0;
}

int vine_graph_executor_add_dependency(struct vine_graph_executor *e, uint64_t parent_id, uint64_t child_id)
{
	return e ? vine_graph_add_dependency(e->graph, parent_id, child_id) : -1;
}

const char *vine_graph_executor_get_node_outfile_remote_name(struct vine_graph_executor *e, uint64_t node_id)
{
	struct vine_graph_node *node = e ? vine_graph_get_node(e->graph, node_id) : NULL;
	return node ? node->outfile_remote_name : NULL;
}

/* Compute depths, declare each node's Python-result output, and set the parent counts used for scheduling.
 * The result output is declared here because target marks are known only after the graph is built. */
int vine_graph_executor_finalize(struct vine_graph_executor *e)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || vine_graph_finalize(g) != 0) {
		return -1;
	}

	uint64_t nid;
	struct vine_graph_node *node;
	int iteration;
	ITABLE_ITERATE(g->nodes, iteration, nid, node)
	{
		if (!node->outfile) {
			node->outfile = vine_graph_executor_declare_output(e, node, node->outfile_remote_name, node->outfile_remote_name, node->is_target);
			if (!node->outfile) {
				return -1;
			}
		}
		node->remaining_parents_count = list_size(node->parents);
	}
	return 0;
}

int vine_graph_executor_declare_input_file(struct vine_graph_executor *e, uint64_t file_id, const char *source_path, int vault)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !file_id || !source_path || itable_lookup(g->file_id_to_file, file_id)) {
		return -1;
	}

	struct vine_file *file = vault ? vine_manager_data_service_declare_file(e->manager, source_path) :
		vine_declare_file(e->manager, source_path, VINE_CACHE_LEVEL_WORKFLOW, 0);
	if (!file) {
		return -1;
	}
	itable_insert(g->file_id_to_file, file_id, file);
	return 0;
}

int vine_graph_executor_add_task_output_file(struct vine_graph_executor *e, uint64_t task_id, uint64_t file_id, const char *task_path, int is_target)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !task_id || !file_id || !task_path || itable_lookup(g->file_id_to_file, file_id)) {
		return -1;
	}

	struct vine_graph_node *node = vine_graph_get_node(g, task_id);
	if (!node) {
		return -1;
	}

	const char *base = strrchr(task_path, '/');
	char *local_name = string_format("file-%" PRIu64 "-%s", file_id, base ? base + 1 : task_path);
	struct vine_file *file = vine_graph_executor_declare_output(e, node, task_path, local_name, is_target);
	free(local_name);
	if (!file) {
		return -1;
	}
	itable_insert(g->file_id_to_file, file_id, file);
	return 0;
}

int vine_graph_executor_add_task_input_file(struct vine_graph_executor *e, uint64_t task_id, uint64_t file_id, const char *task_path)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !task_id || !file_id || !task_path) {
		return -1;
	}

	struct vine_graph_node *node = vine_graph_get_node(g, task_id);
	struct vine_file *file = itable_lookup(g->file_id_to_file, file_id);
	if (!node || !file) {
		return -1;
	}

	vine_graph_io_mount_add(node->extra_inputs, file, task_path);
	return 0;
}

const char *vine_graph_executor_get_file_target_path(struct vine_graph_executor *e, uint64_t file_id)
{
	struct vine_graph *g = e ? e->graph : NULL;
	struct vine_file *file = g ? itable_lookup(g->file_id_to_file, file_id) : NULL;
	return file ? vine_file_source(file) : NULL;
}

/* Apply one executor setting. Return zero on success, or -1 for an unknown name or invalid value. */
int vine_graph_executor_tune(struct vine_graph_executor *e, const char *name, const char *value)
{
	if (!e || !name || !value) {
		return -1;
	}

	if (strcmp(name, "output-dir") == 0) {
		if (mkdir(value, 0777) != 0 && errno != EEXIST) {
			debug(D_ERROR, "failed to create output directory %s: %s", value, strerror(errno));
			return -1;
		}
		free(e->output_dir);
		e->output_dir = xxstrdup(value);

	} else if (strcmp(name, "prune-depth") == 0) {
		char *end;
		long depth = strtol(value, &end, 10);
		if (end == value || *end || depth < 0 || depth > INT_MAX) {
			debug(D_ERROR, "invalid prune-depth: %s (must be >= 0; 0 disables prune-depth release)", value);
			return -1;
		}
		e->prune_depth = (int)depth;

	} else if (strcmp(name, "task-priority-mode") == 0) {
		if (strcmp(value, "random") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_RANDOM;
		} else if (strcmp(value, "depth-first") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_DEPTH_FIRST;
		} else if (strcmp(value, "breadth-first") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_BREADTH_FIRST;
		} else if (strcmp(value, "fifo") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_FIFO;
		} else if (strcmp(value, "lifo") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_LIFO;
		} else if (strcmp(value, "largest-input-first") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST;
		} else if (strcmp(value, "largest-storage-footprint-first") == 0) {
			e->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST;
		} else {
			debug(D_ERROR, "invalid priority mode: %s", value);
			return -1;
		}

	} else if (strcmp(name, "progress-bar-update-interval-sec") == 0) {
		double val = atof(value);
		e->progress_bar_update_interval_sec = (val > 0.0) ? val : 0.1;

	} else {
		debug(D_ERROR, "invalid vine_graph parameter: %s", name);
		return -1;
	}

	return 0;
}

/* Set the interrupted flag when SIGINT is received. */
static void vine_graph_executor_handle_sigint(int signal)
{
	interrupted = 1;
}

/* Compute submission priority for a node using the configured scheduling policy. */
static double vine_graph_executor_calculate_task_priority(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!node || !g) {
		return 0;
	}

	double priority = 0;
	timestamp_t current_time = timestamp_get();
	struct vine_graph_node *parent_node;

	switch (e->task_priority_mode) {
	case TASK_PRIORITY_MODE_RANDOM:
		priority = random_double();
		break;
	case TASK_PRIORITY_MODE_DEPTH_FIRST:
		priority = (double)node->depth;
		break;
	case TASK_PRIORITY_MODE_BREADTH_FIRST:
		priority = -(double)node->depth;
		break;
	case TASK_PRIORITY_MODE_FIFO:
		priority = -(double)current_time; // earlier time yields higher priority
		break;
	case TASK_PRIORITY_MODE_LIFO:
		priority = (double)current_time;
		break;
	case TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST:
		LIST_ITERATE(node->parents, parent_node)
		{
			if (!parent_node || !parent_node->outfile) {
				continue;
			}
			priority += (double)vine_file_size(parent_node->outfile);
		}
		break;
	case TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST:
		LIST_ITERATE(node->parents, parent_node)
		{
			if (!parent_node || !parent_node->outfile) {
				continue;
			}
			if (!parent_node->task) {
				continue;
			}
			timestamp_t parent_task_completion_time = vine_task_get_metric(parent_node->task, "time_workers_execute_last");
			priority += (double)vine_file_size(parent_node->outfile) * (double)parent_task_completion_time;
		}
		break;
	}

	return priority;
}

/* Submit the node task if it is still initial, and record the manager task id for later lookup. */
static void vine_graph_executor_submit_node(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}

	timestamp_t t_pre = timestamp_get();

	vine_graph_executor_materialize_node(e, node);

	if (!node->task) {
		debug(D_ERROR, "vine_graph_executor_submit_node: node %" PRIu64 " has no task after materialize", node->node_id);
		goto record_preprocessing;
	}

	if (!vine_graph_task_not_submitted(node->task)) {
		debug(D_VINE,
				"vine_graph_executor_submit_node: skipping node %" PRIu64 " (task already submitted, state=%s, task_id=%d)",
				node->node_id,
				vine_task_get_state(node->task),
				vine_task_get_id(node->task));
		goto record_preprocessing;
	}

	double priority = vine_graph_executor_calculate_task_priority(e, node);
	vine_task_set_priority(node->task, priority);

	int task_id = vine_submit(e->manager, node->task);

	if (task_id <= 0) {
		debug(D_ERROR, "vine_graph_executor_submit_node: failed to submit node %" PRIu64 " (returned task_id=%d)", node->node_id, task_id);
		goto record_preprocessing;
	}

	itable_insert(e->task_id_to_node, (uint64_t)task_id, node); // reverse lookup from vine_wait
	debug(D_VINE, "submitted node %" PRIu64 " with task id %d", node->node_id, task_id);

record_preprocessing: {
	uint64_t dt = (uint64_t)(timestamp_get() - t_pre);
	node->preprocessing_time_us = dt;
	e->total_preprocessing_time_us += dt;
	debug(D_VINE,
			"node %" PRIu64 " preprocessing %" PRIu64 " us, graph cumulative %" PRIu64 " us",
			node->node_id,
			dt,
			e->total_preprocessing_time_us);
}
}

/* Return true when this node is ready to submit. */
static int vine_graph_node_ready_for_submission(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (!e || !node || node->remaining_parents_count != 0 || node->completed) {
		return 0;
	}
	if (node->in_resubmit_queue) {
		return 0;
	}
	if (node->task && !vine_graph_task_not_submitted(node->task)) {
		return 0;
	}
	return 1;
}

/* Submit ready source nodes and enable delivery of recovery tasks to the application. */
static void vine_graph_executor_submit_initial_ready_nodes(struct vine_graph_executor *e)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !e->manager) {
		return;
	}

	uint64_t nid;
	struct vine_graph_node *node;
	int iteration;
	ITABLE_ITERATE(g->nodes, iteration, nid, node)
	{
		if (vine_graph_node_ready_for_submission(e, node)) {
			vine_graph_executor_submit_node(e, node);
		}
	}

	vine_enable_external_recovery_handling(e->manager); // driver must observe recovery completions for cut and prune
}

/* After one parent completes, decrement remaining parents and submit children that become ready. */
static void vine_graph_executor_submit_unblocked_children(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}

	struct vine_graph_node *child_node;
	LIST_ITERATE(node->children, child_node)
	{
		if (!child_node) {
			continue;
		}

		if (!child_node->fired_parents) {
			child_node->fired_parents = set_create(0);
		}
		if (set_lookup(child_node->fired_parents, node)) {
			continue;
		}
		set_insert(child_node->fired_parents, node);

		if (child_node->remaining_parents_count > 0) {
			child_node->remaining_parents_count--;
		}

		if (vine_graph_node_ready_for_submission(e, child_node)) {
			vine_graph_executor_submit_node(e, child_node);
		}
	}
}

/* Map a completed vine_task to the corresponding graph node, including recovery tasks. */
static struct vine_graph_node *vine_graph_executor_node_from_task(struct vine_graph_executor *e, struct vine_task *task)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !task) {
		return NULL;
	}

	/* Recovery completions map to the original producer; regular completions map to themselves. */
	int lookup_task_id = vine_task_get_recovery_source_task_id(task);
	if (lookup_task_id <= 0) {
		lookup_task_id = vine_task_get_id(task);
	}
	if (lookup_task_id > 0) {
		return itable_lookup(e->task_id_to_node, (uint64_t)lookup_task_id);
	}

	debug(D_ERROR, "task %d has no graph node mapping", vine_task_get_id(task));
	return NULL;
}

/* Return non-zero when a temporary output file has a recovery task that is neither initial nor finished. */
static int vine_graph_node_is_mid_recovery(const struct vine_graph_node *n)
{
	struct vine_graph_io_mount *mount;
	LIST_ITERATE(n->outputs, mount)
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
	if (!node || !node->completed || !node->outfile) {
		return 0;
	}
	struct vine_graph_io_mount *mount;
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
	if (!e->manager || vine_file_type(file) != VINE_TEMP || set_lookup(e->checkpoint_pending, file) || vine_manager_data_service_vault_contains(e->manager, file)) {
		return;
	}
	set_insert(e->checkpoint_pending, file);
	vine_graph_executor_push_checkpoint(e, node, file);
}

/* Withdraw a pruned file from checkpointing. Queued files leave the queue. In-flight receives are cancelled by the
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

/* Release intermediate replicas through Manager. Retrieved targets remain available to the caller. */
static void vine_graph_executor_delete_node_output(struct vine_graph_executor *e, struct vine_graph_node *n)
{
	struct vine_graph_io_mount *mount;
	LIST_ITERATE(n->outputs, mount)
	{
		if (vine_file_type(mount->file) == VINE_TEMP) {
			vine_graph_executor_forget_checkpoint(e, mount->file);
			vine_prune_file(e->manager, mount->file);
		}
	}
}

/* Attempt to mark a completed node as cut and delete its return file when all children permit release. */
static int vine_graph_executor_try_cut_node(struct vine_graph_executor *e, struct vine_graph_node *n)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !n || n->cut || !n->completed) {
		return 0;
	}

	struct vine_graph_node *c;
	LIST_ITERATE(n->children, c)
	{
		if ((!vine_graph_executor_node_is_anchored(e, c) && !c->cut) || vine_graph_node_is_mid_recovery(c)) {
			return 0; // wait for anchored, cut, or non-recovery children
		}
	}

	n->cut = 1;
	debug(D_VINE, "cut: node %" PRIu64 " is_target=%d", n->node_id, n->is_target);

	vine_graph_executor_delete_node_output(e, n);

	return 1;
}

/* Walk upstream from a completed node and apply cut propagation along the worklist.
 * With release_self unset, only the ancestors of start are considered. */
static void vine_graph_executor_propagate_cut_from(struct vine_graph_executor *e, struct vine_graph_node *start, int release_self)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !start || !start->completed) {
		return;
	}

	timestamp_t t0 = timestamp_get();
	if (release_self) {
		vine_graph_executor_try_cut_node(e, start);
	}

	/* Upstream BFS: when a node is cut, enqueue its parents for the same check. */
	struct list *worklist = list_create();
	struct vine_graph_node *p;
	LIST_ITERATE(start->parents, p)
	{
		list_push_tail(worklist, p);
	}

	while (list_size(worklist) > 0) {
		struct vine_graph_node *m = list_pop_head(worklist);
		if (vine_graph_executor_try_cut_node(e, m)) {
			LIST_ITERATE(m->parents, p)
			{
				list_push_tail(worklist, p);
			}
		}
	}

	list_delete(worklist);
	e->time_spent_on_cut_propagation += timestamp_get() - t0;
}

/* Return non-zero if every descendant within the given depth bound is complete, not mid-recovery, and durable:
 * anchored in the vault or itself released. Releasing an ancestor of a non-durable output would force a chain of
 * recoveries through already released ancestors when that output is lost. */
static int vine_graph_executor_descendants_durable_within_depth(struct vine_graph_executor *e, struct vine_graph_node *a, int depth)
{
	if (!a || depth <= 0) {
		return 1;
	}

	struct set *visited = set_create(0);
	struct list *current = list_create();
	list_push_tail(current, a);
	set_insert(visited, a);

	int ok = 1;
	/* Expand one child frontier per iteration up to depth hops from a. */
	for (int d = 0; d < depth && ok; d++) {
		struct list *next = list_create();
		struct vine_graph_node *n;
		LIST_ITERATE(current, n)
		{
			struct vine_graph_node *c;
			LIST_ITERATE(n->children, c)
			{
				if (set_lookup(visited, c)) {
					continue;
				}
				set_insert(visited, c);
				if (!c->completed || vine_graph_node_is_mid_recovery(c) ||
						(!c->cut && !c->released_by_prune_depth && !vine_graph_executor_node_is_anchored(e, c))) {
					ok = 0;
					break;
				}
				list_push_tail(next, c);
			}
			if (!ok) {
				break;
			}
		}
		list_delete(current);
		current = next;
	}

	list_delete(current);
	set_delete(visited);
	return ok;
}

/* Release a temporary output when prune-depth constraints and descendant completion are satisfied. */
static void vine_graph_executor_try_prune_depth_release(struct vine_graph_executor *e, struct vine_graph_node *a)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !a) {
		return;
	}
	if (a->released_by_prune_depth) {
		return;
	}
	if (!a->completed) {
		return;
	}
	if (!vine_graph_executor_descendants_durable_within_depth(e, a, e->prune_depth)) {
		return; // wait until descendants within prune_depth layers are durable
	}

	vine_graph_executor_delete_node_output(e, a);
	a->released_by_prune_depth = 1;

	debug(D_VINE, "prune-depth release: node %" PRIu64 " depth=%d", a->node_id, e->prune_depth);
}

/* Apply prune-depth release starting at a node and extending up to k ancestor levels.
 * With release_self unset, only the ancestors of node are considered. */
static void vine_graph_executor_apply_prune_depth_from(struct vine_graph_executor *e, struct vine_graph_node *node, int release_self)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g || !node) {
		return;
	}
	int k = e->prune_depth;
	if (k <= 0) {
		return;
	}

	if (release_self) {
		vine_graph_executor_try_prune_depth_release(e, node);
	}

	struct set *visited = set_create(0);
	struct list *current = list_create();
	list_push_tail(current, node);
	set_insert(visited, node);

	/* Visit new parents up to k levels, trying prune release on each. */
	for (int d = 1; d <= k; d++) {
		struct list *next = list_create();
		struct vine_graph_node *n;
		LIST_ITERATE(current, n)
		{
			struct vine_graph_node *p;
			LIST_ITERATE(n->parents, p)
			{
				if (set_lookup(visited, p)) {
					continue;
				}
				set_insert(visited, p);
				list_push_tail(next, p);
				vine_graph_executor_try_prune_depth_release(e, p);
			}
		}
		list_delete(current);
		current = next;
	}

	list_delete(current);
	set_delete(visited);
}

/*
 * Completion hook after a node finishes: cut propagation, prune-depth handling, and timing.
 * Postprocessing wall time is charged to the node that triggered this call.
 * A recovery task runs only because some consumer needs its output now, so its node is not released here.
 * The consumer's own completion later releases it through the same cut and prune-depth walks.
 */
static void vine_graph_executor_run_completion_postprocess(struct vine_graph_executor *e, struct vine_graph_node *node, int release_self)
{
	if (!e || !node) {
		return;
	}

	timestamp_t t0 = timestamp_get();
	vine_graph_executor_propagate_cut_from(e, node, release_self);
	vine_graph_executor_apply_prune_depth_from(e, node, release_self);
	uint64_t dt = (uint64_t)(timestamp_get() - t0);
	node->postprocessing_time_us = dt;
	e->total_postprocessing_time_us += dt;
	debug(D_VINE,
			"node %" PRIu64 " postprocessing %" PRIu64 " us, graph cumulative %" PRIu64 " us",
			node->node_id,
			dt,
			e->total_postprocessing_time_us);
}

/* Record a checkpoint result from the Manager thread. Queue and graph changes wait for the executor loop. */
static void vine_graph_executor_checkpoint_complete(void *argument, struct vine_file *file, int success)
{
	struct vine_graph_executor *e = argument;
	list_push_tail(success ? e->checkpoint_completed : e->checkpoint_failed, file);
}

/* Settle reported results, then admit queued files in priority order until the Manager refuses.
 * BUSY means no slot or vault space, so admission stops. A file without a ready source is set aside for this round
 * so it does not block lower-priority files. Results of pruned files are ignored. */
static void vine_graph_executor_process_checkpoints(struct vine_graph_executor *e)
{
	struct vine_file *file;
	while ((file = list_pop_head(e->checkpoint_completed))) {
		if (set_remove(e->checkpoint_pending, file)) {
			/* A published checkpoint anchors its node, which may let its ancestors be released. */
			struct vine_graph_node *node = hash_table_lookup(e->graph->outfile_cachename_to_node, vine_file_cached_name(file));
			vine_graph_executor_run_completion_postprocess(e, node, 0);
		}
	}
	while ((file = list_pop_head(e->checkpoint_failed))) {
		if (set_lookup(e->checkpoint_pending, file)) {
			struct vine_graph_node *node = hash_table_lookup(e->graph->outfile_cachename_to_node, vine_file_cached_name(file));
			vine_graph_executor_push_checkpoint(e, node, file);
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

#define RESUBMIT_SCAN_LIMIT 100
#define RESUBMIT_COOLDOWN_USECS ((timestamp_t)1000000)

/* Queue this node for retry after failure. */
static void vine_graph_executor_queue_node_retry(struct vine_graph_executor *e, struct vine_graph_node *node)
{
	if (!e || !e->graph || !node || node->in_resubmit_queue) {
		return;
	}
	node->last_failure_time = timestamp_get();
	list_push_tail(e->resubmit_queue, node);
	node->in_resubmit_queue = 1;
}

/*
 * Process the resubmit queue after cooldown. A ready head is popped, its failed task is torn down,
 * and vine_graph_executor_submit_node runs again. If the head is still cooling off, rotate it to the tail so
 * other nodes behind it are not stuck forever while the driver waits.
 */
static void vine_graph_executor_drain_resubmit_queue(struct vine_graph_executor *e)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g) {
		return;
	}

	timestamp_t now = timestamp_get();
	int queued = list_size(e->resubmit_queue);
	if (queued == 0) {
		return;
	}

	int budget = queued < RESUBMIT_SCAN_LIMIT ? queued : RESUBMIT_SCAN_LIMIT;
	int resubmits = 0;
	int rotations_without_resubmit = 0;

	while (resubmits < budget && list_size(e->resubmit_queue) > 0) {
		struct vine_graph_node *node = list_peek_head(e->resubmit_queue);
		if (!node) {
			break;
		}
		if (now - node->last_failure_time >= RESUBMIT_COOLDOWN_USECS) {
			list_pop_head(e->resubmit_queue);
			node->in_resubmit_queue = 0;

			debug(D_VINE, "Resubmitting node %" PRIu64, node->node_id);
			vine_graph_executor_clear_node_runner_arg(e, node);
			if (node->task) {
				vine_task_delete(node->task);
				node->task = NULL;
			}
			vine_graph_executor_submit_node(e, node);
			resubmits++;
			rotations_without_resubmit = 0;
		} else {
			list_pop_head(e->resubmit_queue);
			list_push_tail(e->resubmit_queue, node);
			rotations_without_resubmit++;
			if (rotations_without_resubmit >= list_size(e->resubmit_queue)) {
				break;
			}
		}
	}
}

/*
 * Verify one output mount after a successful task (VINE_FILE paths must exist).
 */
static int vine_graph_executor_validate_io_mount_or_retry(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_graph_io_mount *mount, struct vine_task *task)
{
	if (!mount || !mount->file) {
		return 1;
	}
	struct vine_file *f = mount->file;

	switch (vine_file_type(f)) {
	case VINE_TEMP:
	case VINE_BUFFER:
		break;
	case VINE_FILE:
		if (vine_file_source(f)) {
			struct stat info;
			if (stat(vine_file_source(f), &info) < 0) {
				debug(D_VINE,
						"Task %d succeeded but missing output file %s (%s)",
						vine_task_get_id(task),
						mount->remote_name ? mount->remote_name : "?",
						vine_file_source(f));
				vine_graph_executor_queue_node_retry(e, node);
				return 0;
			}
		}
		break;
	default:
		break;
	}
	return 1;
}

/*
 * Validate each output mount declared for this graph node.
 */
static int vine_graph_executor_validate_all_declared_outputs_or_retry(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_task *task)
{
	void *item;
	LIST_ITERATE(node->outputs, item)
	{
		if (!vine_graph_executor_validate_io_mount_or_retry(e, node, (struct vine_graph_io_mount *)item, task)) {
			return 0;
		}
	}
	return 1;
}

/* Return the first Manager-local input of a node whose source is no longer readable, or NULL.
 * Edata and user input files are VINE_FILEs that cannot be recomputed, so a missing source fails the run. */
static struct vine_file *vine_graph_executor_find_lost_input(struct vine_graph_node *node)
{
	struct vine_graph_io_mount *mount;
	LIST_ITERATE(node->extra_inputs, mount)
	{
		if (vine_file_type(mount->file) == VINE_FILE && access(vine_file_source(mount->file), R_OK) != 0) {
			return mount->file;
		}
	}
	return NULL;
}

static int vine_graph_executor_validate_task_or_retry(struct vine_graph_executor *e, struct vine_graph_node *node, struct vine_task *task)
{
	/*
	 * Returning zero means the completion is rejected, a retry is enqueued, and the progress
	 * bar must not advance because the node did not succeed yet. Returning -1 means the run must stop.
	 */
	if (vine_task_get_result(task) == VINE_RESULT_INPUT_MISSING) {
		struct vine_file *lost = vine_graph_executor_find_lost_input(node);
		if (lost) {
			debug(D_ERROR, "node %" PRIu64 " input %s is missing at the Manager; stopping the run", node->node_id, vine_file_source(lost));
			return -1;
		}
	}
	if (vine_task_get_result(task) != VINE_RESULT_SUCCESS || vine_task_get_exit_code(task) != 0) {
		debug(D_VINE,
				"Task %d failed (result=%d, exit=%d)",
				vine_task_get_id(task),
				vine_task_get_result(task),
				vine_task_get_exit_code(task));
		vine_graph_executor_queue_node_retry(e, node);
		return 0;
	}

	if (!vine_graph_executor_validate_all_declared_outputs_or_retry(e, node, task)) {
		return 0;
	}

	return 1;
}

/* Return the recorded user-task makespan in microseconds. */
uint64_t vine_graph_executor_get_makespan_us(const struct vine_graph_executor *e)
{
	if (!e) {
		return 0;
	}

	return (uint64_t)e->makespan_us;
}

/* Return the manager's cumulative count of recovery tasks submitted. */
static uint64_t vine_graph_executor_get_total_recovery_tasks(const struct vine_graph_executor *e)
{
	if (!e || !e->manager) {
		return 0;
	}

	struct vine_stats stats;
	vine_get_stats(e->manager, &stats);
	return (uint64_t)stats.tasks_recovery;
}

/* Return how many recovery tasks have completed in the current executor run. */
uint64_t vine_graph_executor_get_completed_recovery_tasks(const struct vine_graph_executor *e)
{
	if (!e) {
		return 0;
	}

	return e->completed_recovery_tasks;
}

/* Main loop: submit work, wait, handle recovery, update graph state. */
int vine_graph_executor_execute(struct vine_graph_executor *e)
{
	struct vine_graph *g = e ? e->graph : NULL;
	if (!g) {
		return -1;
	}
	int failed = 0;

	interrupted = 0;
	void (*previous_sigint_handler)(int) = signal(SIGINT, vine_graph_executor_handle_sigint);

	debug(D_VINE, "start executing executor graph");

	vine_graph_executor_submit_initial_ready_nodes(e);

	struct ProgressBar *pbar = progress_bar_init("Executing Tasks");
	progress_bar_set_update_interval(pbar, e->progress_bar_update_interval_sec);
	e->completed_recovery_tasks = 0;

	struct ProgressBarPart *user_tasks_part = progress_bar_create_part("User", itable_size(g->nodes));
	struct ProgressBarPart *recovery_tasks_part = progress_bar_create_part("Recovery", 0);
	progress_bar_bind_part(pbar, user_tasks_part);
	progress_bar_bind_part(pbar, recovery_tasks_part);

	const uint64_t user_node_total = itable_size(g->nodes);

	int wait_timeout = 1; // short timeout after a result, longer when idle

	/* Count each user node once, independently of retries and recovery completions. */
	uint64_t completed_user_nodes = 0;
	uint64_t node_id;
	struct vine_graph_node *existing_node;
	int iteration;
	ITABLE_ITERATE(g->nodes, iteration, node_id, existing_node)
	{
		if (existing_node->completed) {
			completed_user_nodes++;
		}
	}
	progress_bar_update_part(pbar, user_tasks_part, completed_user_nodes);
	while (completed_user_nodes < user_node_total) {
		if (interrupted) {
			break;
		}

		vine_graph_executor_process_checkpoints(e);
		vine_graph_executor_drain_resubmit_queue(e);
		progress_bar_set_part_total(pbar, recovery_tasks_part, vine_graph_executor_get_total_recovery_tasks(e));

		struct vine_task *task = vine_wait(e->manager, wait_timeout);
		if (task) {
			wait_timeout = 0;

			struct vine_graph_node *node = vine_graph_executor_node_from_task(e, task);
			if (!node) {
				debug(D_ERROR, "task %d does not belong to this graph; stopping the run", vine_task_get_id(task));
				failed = 1;
				break;
			}

			timestamp_t commit_end = vine_task_get_metric(task, "time_when_commit_end");
			if (commit_end > 0) {
				e->time_first_task_dispatched = MIN(e->time_first_task_dispatched, commit_end); // makespan start
			}

			/*
			 * User and recovery progress advances only after outputs validate. Failed tasks enter
			 * the retry path and leave the bar unchanged until a later successful completion.
			 */
			int valid = vine_graph_executor_validate_task_or_retry(e, node, task);
			if (valid < 0) {
				failed = 1;
				break;
			}
			if (!valid) {
				continue;
			}

			/* Every temporary output is retrieved into the vault in producer execution time order. */
			node->execution_time_us = (uint64_t)vine_task_get_metric(task, "time_workers_execute_last");
			struct vine_graph_io_mount *output;
			LIST_ITERATE(node->outputs, output)
			{
				vine_graph_executor_queue_checkpoint(e, node, output->file);
			}

			if (vine_task_get_recovery_source_task_id(task) > 0) {
				e->completed_recovery_tasks++;
				progress_bar_update_part(
						pbar,
						recovery_tasks_part,
						e->completed_recovery_tasks - recovery_tasks_part->current);

				/* Reset cut and prune-depth flags for recovery tasks. */
				node->cut = 0;
				node->released_by_prune_depth = 0;

				/* Release only the inputs this recovery no longer needs. Its own output has a waiting consumer. */
				vine_graph_executor_run_completion_postprocess(e, node, 0);
			} else {
				timestamp_t retrieval_time = (timestamp_t)vine_task_get_metric(task, "time_when_retrieval");
				e->time_last_task_retrieved = MAX(e->time_last_task_retrieved, retrieval_time);
				e->makespan_us = e->time_last_task_retrieved - e->time_first_task_dispatched;

				if (!node->completed) {
					node->completed = 1;
					completed_user_nodes++;
					if (user_tasks_part->current == 0) {
						progress_bar_set_start_time(pbar, vine_task_get_metric(task, "time_when_commit_start"));
					}
					progress_bar_update_part(pbar, user_tasks_part, 1);
				}

				/* Postprocess the node and submit its children. */
				vine_graph_executor_run_completion_postprocess(e, node, 1);
				vine_graph_executor_submit_unblocked_children(e, node);
			}
		} else {
			wait_timeout = 1; // no task ready, wait with default blocking timeout
		}
	}

	progress_bar_finish(pbar);
	progress_bar_delete(pbar);

	debug(D_VINE, "total time spent on cut propagation: %.6f seconds\n", e->time_spent_on_cut_propagation / 1e6);

	signal(SIGINT, previous_sigint_handler);
	if (interrupted) {
		raise(SIGINT); // restore handler first, then honor prior interrupt
	}
	return failed ? -1 : 0;
}
