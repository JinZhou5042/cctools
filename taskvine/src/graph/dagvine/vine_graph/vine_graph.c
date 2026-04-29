#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <assert.h>
#include <math.h>
#include <signal.h>
#include <errno.h>
#include <stdio.h>
#include <sys/stat.h>

#include "priority_queue.h"
#include "list.h"
#include "debug.h"
#include "itable.h"
#include "xxmalloc.h"
#include "stringtools.h"
#include "random.h"
#include "hash_table.h"
#include "set.h"
#include "timestamp.h"
#include "progress_bar.h"
#include "macros.h"
#include "uuid.h"

#include "vine_node.h"
#include "vine_graph.h"
#include "vine_manager.h"
#include "vine_worker_info.h"
#include "vine_task.h"
#include "vine_file.h"
#include "vine_mount.h"
#include "taskvine.h"
#include "vine_temp.h"

static volatile sig_atomic_t interrupted = 0;

/*************************************************************/
/* Private Functions */
/*************************************************************/

/**
 * Handle the SIGINT signal.
 * @param signal Reference to the signal.
 */
static void handle_sigint(int signal)
{
	interrupted = 1;
}

/**
 * Calculate the priority of a node given the priority mode.
 * @param node Reference to the node object.
 * @param priority_mode Reference to the priority mode.
 * @return The priority.
 */
static double calculate_task_priority(struct vine_node *node, task_priority_mode_t priority_mode)
{
	if (!node) {
		return 0;
	}

	double priority = 0;
	timestamp_t current_time = timestamp_get();

	struct vine_node *parent_node;

	switch (priority_mode) {
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
		priority = -(double)current_time;
		break;
	case TASK_PRIORITY_MODE_LIFO:
		priority = (double)current_time;
		break;
	case TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST:
		LIST_ITERATE(node->parents, parent_node)
		{
			if (!parent_node->fn_return_file) {
				continue;
			}
			priority += (double)vine_file_size(parent_node->fn_return_file);
		}
		break;
	case TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST:
		LIST_ITERATE(node->parents, parent_node)
		{
			if (!parent_node->fn_return_file) {
				continue;
			}
			timestamp_t parent_task_completion_time = parent_node->task->time_workers_execute_last;
			priority += (double)vine_file_size(parent_node->fn_return_file) * (double)parent_task_completion_time;
		}
		break;
	}

	return priority;
}

/**
 * Submit a node to the TaskVine manager via the vine graph.
 * @param vg Reference to the vine graph.
 * @param node Reference to the node.
 */
static void submit_node_task(struct vine_graph *vg, struct vine_node *node)
{
	if (!vg || !node) {
		return;
	}

	if (!node->task) {
		debug(D_ERROR, "submit_node_task: node %" PRIu64 " has no task", node->node_id);
		return;
	}

	/* Avoid double-submitting the same task object. This should never be needed
	 * for correctness and leads to task_id mapping corruption if it happens. */
	if (node->task->state != VINE_TASK_INITIAL) {
		debug(D_VINE, "submit_node_task: skipping node %" PRIu64 " (task already submitted, state=%d, task_id=%d)", node->node_id, node->task->state, node->task->task_id);
		return;
	}

	/* calculate the priority of the node */
	double priority = calculate_task_priority(node, vg->task_priority_mode);
	vine_task_set_priority(node->task, priority);

	/* submit the task to the manager */
	timestamp_t time_start = timestamp_get();
	int task_id = vine_submit(vg->manager, node->task);
	node->submission_time = timestamp_get() - time_start;

	if (task_id <= 0) {
		debug(D_ERROR, "submit_node_task: failed to submit node %" PRIu64 " (returned task_id=%d)", node->node_id, task_id);
		return;
	}

	/* insert the task id to the task id to node map */
	itable_insert(vg->task_id_to_node, (uint64_t)task_id, node);

	debug(D_VINE, "submitted node %" PRIu64 " with task id %d", node->node_id, task_id);

	return;
}

/**
 * Submit the children of a node once every dependency has completed.
 * @param vg Reference to the vine graph.
 * @param node Reference to the node.
 */
static void submit_unblocked_children(struct vine_graph *vg, struct vine_node *node)
{
	if (!vg || !node) {
		return;
	}

	struct vine_node *child_node;
	LIST_ITERATE(node->children, child_node)
	{
		if (!child_node) {
			continue;
		}

		/* Edge-fired dependency resolution: each parent->child edge is consumed at most once.
		 * This is critical for recomputation/resubmission, where a parent may "complete" multiple times. */
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

		/* If no more parents are remaining, submit the child (if it is not already done / in-flight). */
		if (child_node->remaining_parents_count == 0 && !child_node->completed && child_node->task &&
				child_node->task->state == VINE_TASK_INITIAL) {
			submit_node_task(vg, child_node);
		}
	}

	return;
}

/**
 * Compute a topological ordering of the vine graph.
 * Call only after all nodes, edges, and metrics have been populated.
 * @param vg Reference to the vine graph.
 * @return Nodes in topological order.
 */
static struct list *get_topological_order(struct vine_graph *vg)
{
	if (!vg) {
		return NULL;
	}

	int total_nodes = itable_size(vg->nodes);
	struct list *topo_order = list_create();
	struct itable *in_degree_map = itable_create(0);
	struct priority_queue *pq = priority_queue_create(total_nodes);

	uint64_t nid;
	struct vine_node *node;
	ITABLE_ITERATE(vg->nodes, nid, node)
	{
		int deg = list_size(node->parents);
		itable_insert(in_degree_map, nid, (void *)(intptr_t)deg);
		if (deg == 0) {
			priority_queue_push(pq, node, -(double)node->node_id);
		}
	}

	while (priority_queue_size(pq) > 0) {
		struct vine_node *current = priority_queue_pop(pq);
		list_push_tail(topo_order, current);

		struct vine_node *child;
		LIST_ITERATE(current->children, child)
		{
			intptr_t raw_deg = (intptr_t)itable_lookup(in_degree_map, child->node_id);
			int deg = (int)raw_deg - 1;
			itable_insert(in_degree_map, child->node_id, (void *)(intptr_t)deg);

			if (deg == 0) {
				priority_queue_push(pq, child, -(double)child->node_id);
			}
		}
	}

	if (list_size(topo_order) != total_nodes) {
		debug(D_ERROR, "Error: vine graph contains cycles or is malformed.");
		debug(D_ERROR, "Expected %d nodes, but only sorted %d.", total_nodes, list_size(topo_order));

		uint64_t id;
		ITABLE_ITERATE(vg->nodes, id, node)
		{
			intptr_t raw_deg = (intptr_t)itable_lookup(in_degree_map, id);
			int deg = (int)raw_deg;
			if (deg > 0) {
				debug(D_ERROR, "  Node %" PRIu64 " has in-degree %d. Parents:", id, deg);
				struct vine_node *p;
				LIST_ITERATE(node->parents, p)
				{
					debug(D_ERROR, "    -> %" PRIu64, p->node_id);
				}
			}
		}

		list_delete(topo_order);
		itable_delete(in_degree_map);
		priority_queue_delete(pq);
		exit(1);
	}

	itable_delete(in_degree_map);
	priority_queue_delete(pq);
	return topo_order;
}

/**
 * Extract weakly connected components of the vine graph.
 * Currently used for debugging and instrumentation only.
 * @param vg Reference to the vine graph.
 * @return List of weakly connected components.
 */
static struct list *extract_weakly_connected_components(struct vine_graph *vg)
{
	if (!vg) {
		return NULL;
	}

	struct set *visited = set_create(0);
	struct list *components = list_create();

	uint64_t nid;
	struct vine_node *node;
	ITABLE_ITERATE(vg->nodes, nid, node)
	{
		if (set_lookup(visited, node)) {
			continue;
		}

		struct list *component = list_create();
		struct list *queue = list_create();

		list_push_tail(queue, node);
		set_insert(visited, node);
		list_push_tail(component, node);

		while (list_size(queue) > 0) {
			struct vine_node *curr = list_pop_head(queue);

			struct vine_node *p;
			LIST_ITERATE(curr->parents, p)
			{
				if (!set_lookup(visited, p)) {
					list_push_tail(queue, p);
					set_insert(visited, p);
					list_push_tail(component, p);
				}
			}

			struct vine_node *c;
			LIST_ITERATE(curr->children, c)
			{
				if (!set_lookup(visited, c)) {
					list_push_tail(queue, c);
					set_insert(visited, c);
					list_push_tail(component, c);
				}
			}
		}

		list_push_tail(components, component);
		list_delete(queue);
	}

	set_delete(visited);
	return components;
}

/**
 * Compute the heavy score of a node in the vine graph.
 * @param node Reference to the node.
 * @return Heavy score.
 */
static double compute_node_heavy_score(struct vine_node *node)
{
	if (!node) {
		return 0;
	}

	double up_score = node->depth * node->upstream_subgraph_size * node->fan_in;
	double down_score = node->height * node->downstream_subgraph_size * node->fan_out;

	return up_score / (down_score + 1);
}

static void assign_node_outfile_local(struct vine_graph *vg, struct vine_node *node)
{
	node->outfile_type = NODE_OUTFILE_TYPE_LOCAL;
	char *local_outfile_path = string_format("%s/%s", vg->output_dir, node->outfile_remote_name);
	node->fn_return_file = vine_declare_file(vg->manager, local_outfile_path, VINE_CACHE_LEVEL_WORKFLOW, 0);
	free(local_outfile_path);
}

static void assign_node_outfile_temp(struct vine_graph *vg, struct vine_node *node)
{
	node->outfile_type = NODE_OUTFILE_TYPE_TEMP;
	node->fn_return_file = vine_declare_temp(vg->manager);
}

/**
 * Compute upstream/downstream subgraph sizes and heavy scores for each node.
 * This is expensive (can approach transitive-closure cost) and should only be
 * invoked when heavy-score-based checkpoint selection is enabled.
 */
static void compute_upstream_downstream_and_heavy_scores(struct vine_graph *vg, struct list *topo_order)
{
	if (!vg || !topo_order) {
		return;
	}

	struct vine_node *node;
	struct vine_node *parent_node;
	struct vine_node *child_node;

	/* compute the upstream and downstream counts for each node */
	struct itable *upstream_map = itable_create(0);
	struct itable *downstream_map = itable_create(0);
	uint64_t nid_tmp;
	ITABLE_ITERATE(vg->nodes, nid_tmp, node)
	{
		struct set *upstream = set_create(0);
		struct set *downstream = set_create(0);
		itable_insert(upstream_map, node->node_id, upstream);
		itable_insert(downstream_map, node->node_id, downstream);
	}

	LIST_ITERATE(topo_order, node)
	{
		struct set *upstream = itable_lookup(upstream_map, node->node_id);
		LIST_ITERATE(node->parents, parent_node)
		{
			struct set *parent_upstream = itable_lookup(upstream_map, parent_node->node_id);
			/* set_union() returns a NEW set (allocates); we want an in-place union. */
			set_insert_set(upstream, parent_upstream);
			set_insert(upstream, parent_node);
		}
	}

	LIST_ITERATE_REVERSE(topo_order, node)
	{
		struct set *downstream = itable_lookup(downstream_map, node->node_id);
		LIST_ITERATE(node->children, child_node)
		{
			struct set *child_downstream = itable_lookup(downstream_map, child_node->node_id);
			/* set_union() returns a NEW set (allocates); we want an in-place union. */
			set_insert_set(downstream, child_downstream);
			set_insert(downstream, child_node);
		}
	}

	LIST_ITERATE(topo_order, node)
	{
		node->upstream_subgraph_size = set_size(itable_lookup(upstream_map, node->node_id));
		node->downstream_subgraph_size = set_size(itable_lookup(downstream_map, node->node_id));
		node->fan_in = list_size(node->parents);
		node->fan_out = list_size(node->children);
		set_delete(itable_lookup(upstream_map, node->node_id));
		set_delete(itable_lookup(downstream_map, node->node_id));
	}

	itable_delete(upstream_map);
	itable_delete(downstream_map);

	/* compute the heavy score for each node */
	LIST_ITERATE(topo_order, node)
	{
		node->heavy_score = compute_node_heavy_score(node);
	}
}

/**
 * Map a task object reported by the manager back to the owning graph node.
 * @param vg Reference to the vine graph.
 * @param task Task handle as returned from the wait API.
 * @return Matching node, or NULL.
 */
static struct vine_node *get_node_by_task(struct vine_graph *vg, struct vine_task *task)
{
	if (!vg || !task) {
		return NULL;
	}

	if (task->type == VINE_TASK_TYPE_STANDARD) {
		/* standard tasks are mapped directly to a node */
		return itable_lookup(vg->task_id_to_node, (uint64_t)task->task_id);
	} else if (task->type == VINE_TASK_TYPE_RECOVERY) {
		/* Recovery tasks are not in the per-node task-id map; recover the
		 * producer by following the file’s original producer id. */
		struct vine_mount *mount;
		LIST_ITERATE(task->output_mounts, mount)
		{
			uint64_t original_producer_task_id = mount->file->original_producer_task_id;
			if (original_producer_task_id > 0) {
				return itable_lookup(vg->task_id_to_node, original_producer_task_id);
			}
		}
	}

	debug(D_ERROR, "task %d has no original producer task id", task->task_id);

	return NULL;
}

/* Shared-FS byte accounting.
 * vg->pfs_usage_bytes should equal the sum of node->pfs_credited_bytes.
 * We update it when a PFS output is observed on success, and when that
 * path is later deleted. */

/* Update the global PFS byte count after stat'ing a node's output. */
static void pfs_account_write(struct vine_graph *vg, struct vine_node *n, size_t new_size)
{
	if (!vg || !n) {
		return;
	}

	size_t prev = n->pfs_credited_bytes;
	if (new_size == prev) {
		return;
	}

	if (new_size > prev) {
		vg->pfs_usage_bytes += (new_size - prev);
	} else {
		vg->pfs_usage_bytes -= (prev - new_size);
	}
	n->pfs_credited_bytes = new_size;

	debug(D_VINE,
			"pfs write: node %" PRIu64 " size=%zu (prev=%zu) usage=%" PRIu64,
			n->node_id,
			new_size,
			prev,
			vg->pfs_usage_bytes);
}

/* Remove this node's credited PFS bytes from the global total. */
static void pfs_account_delete(struct vine_graph *vg, struct vine_node *n)
{
	if (!vg || !n) {
		return;
	}

	size_t credited = n->pfs_credited_bytes;
	if (credited == 0) {
		return;
	}

	vg->pfs_usage_bytes -= credited;
	n->pfs_credited_bytes = 0;

	debug(D_VINE,
			"pfs delete: node %" PRIu64 " size=%zu usage=%" PRIu64,
			n->node_id,
			credited,
			vg->pfs_usage_bytes);
}

/* Cut propagation.
 * anchored(n): completed and on LOCAL/PFS, so recovery does not walk past it.
 * cut(n): completed, and every child is anchored or already cut.
 * Once a non-target node is cut, its return can be deleted. TEMP goes through
 * vine_prune_file(); LOCAL/PFS unlink the path directly. */

/* Return 1 if this node is complete and on durable storage. */
static int node_is_anchored(const struct vine_node *n)
{
	if (!n || !n->completed) {
		return 0;
	}
	return n->outfile_type == NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM || n->outfile_type == NODE_OUTFILE_TYPE_LOCAL;
}

/* TEMP output is being regenerated by recovery; don't treat it as settled yet. */
static int node_is_mid_recovery(const struct vine_node *n)
{
	if (!n || !n->fn_return_file || n->fn_return_file->type != VINE_TEMP) {
		return 0;
	}
	struct vine_task *rt = n->fn_return_file->recovery_task;
	if (!rt) {
		return 0;
	}
	/* INITIAL = never submitted, DONE = last run finished successfully.
	 * Everything else (READY, RUNNING, WAITING_RETRIEVAL, RETRIEVED)
	 * means the manager has (re-)submitted the task and is currently
	 * depending on N's inputs. */
	return rt->state != VINE_TASK_INITIAL && rt->state != VINE_TASK_DONE;
}

/* Delete this node's return path. Caller must already know it is safe. */
static void delete_node_return_file(struct vine_graph *vg, struct vine_node *n)
{
	if (!vg || !n) {
		return;
	}

	switch (n->outfile_type) {
	case NODE_OUTFILE_TYPE_TEMP:
		/* Temp outputs live on workers; go through the manager so every
		 * replica (and cached_name bookkeeping) is released consistently. */
		if (n->fn_return_file) {
			vine_prune_file(vg->manager, n->fn_return_file);
		}
		break;
	case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
		/* For PFS nodes, outfile_remote_name is the full path. Debit
		 * accounting first so the counters stay consistent even if
		 * unlink() fails (the data is considered dispensable and will
		 * be rewritten on any future re-run). */
		pfs_account_delete(vg, n);
		if (n->outfile_remote_name) {
			unlink(n->outfile_remote_name);
		}
		break;
	case NODE_OUTFILE_TYPE_LOCAL:
		/* For LOCAL nodes the physical path is outfile->source
		 * (<output_dir>/<outfile_remote_name>); prefer that over
		 * reconstructing the path ourselves. */
		if (n->fn_return_file && n->fn_return_file->source) {
			unlink(n->fn_return_file->source);
		}
		break;
	}
}

/* Try to cut a node. Returns 1 only on a new transition to cut. */
static int try_cut_node(struct vine_graph *vg, struct vine_node *n)
{
	if (!vg || !n || n->cut || !n->completed) {
		return 0;
	}

	struct vine_node *c;
	LIST_ITERATE(n->children, c)
	{
		/* child must be anchored or cut, and not in the middle of recovery */
		if ((!node_is_anchored(c) && !c->cut) || node_is_mid_recovery(c)) {
			return 0;
		}
	}

	n->cut = 1;

	debug(D_VINE, "cut: node %" PRIu64 " outfile_type=%d is_target=%d", n->node_id, n->outfile_type, n->is_target);

	/* Keep target outputs around. */
	if (!n->is_target) {
		delete_node_return_file(vg, n);
	}

	return 1;
}

/* Re-check `start`, then walk upward until cut state stops changing. */
static void propagate_cut_from(struct vine_graph *vg, struct vine_node *start)
{
	if (!vg || !start || !start->completed) {
		return;
	}

	timestamp_t t0 = timestamp_get();

	try_cut_node(vg, start);

	struct list *worklist = list_create();
	struct vine_node *p;
	LIST_ITERATE(start->parents, p)
	{
		list_push_tail(worklist, p);
	}

	while (list_size(worklist) > 0) {
		struct vine_node *m = list_pop_head(worklist);
		if (try_cut_node(vg, m)) {
			LIST_ITERATE(m->parents, p)
			{
				list_push_tail(worklist, p);
			}
		}
	}

	list_delete(worklist);

	vg->time_spent_on_cut_propagation += timestamp_get() - t0;
}

/* prune-depth release.
 * TEMP only. If a completed non-target node has every descendant within k child
 * hops completed, we may drop its TEMP return before cut would allow it.
 * This is weaker than cut and may force later recovery to recompute the node.
 * LOCAL/PFS are not touched here. */

/* Level-order walk. Returns 0 if any descendant is incomplete or mid-recovery. */
static int all_descendants_within_depth_completed(struct vine_node *a, int depth)
{
	if (!a || depth <= 0) {
		return 1;
	}

	struct set *visited = set_create(0);
	struct list *current = list_create();
	list_push_tail(current, a);
	set_insert(visited, a);

	int ok = 1;
	for (int d = 0; d < depth && ok; d++) {
		struct list *next = list_create();
		struct vine_node *n;
		LIST_ITERATE(current, n)
		{
			struct vine_node *c;
			LIST_ITERATE(n->children, c)
			{
				if (set_lookup(visited, c)) {
					continue;
				}
				set_insert(visited, c);
				/* mid-recovery still counts as unsettled */
				if (!c->completed || node_is_mid_recovery(c)) {
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

/* Early-release one TEMP node if it now meets prune-depth. */
static void try_prune_depth_release(struct vine_graph *vg, struct vine_node *a)
{
	if (!vg || !a) {
		return;
	}
	if (a->prune_depth_pruned) {
		return;
	}
	if (a->outfile_type != NODE_OUTFILE_TYPE_TEMP) {
		return;
	}
	if (a->is_target) {
		return;
	}
	if (!a->completed || !a->fn_return_file) {
		return;
	}
	if (!all_descendants_within_depth_completed(a, vg->prune_depth)) {
		return;
	}

	delete_node_return_file(vg, a);
	a->prune_depth_pruned = 1;

	debug(D_VINE, "prune-depth release: node %" PRIu64 " depth=%d", a->node_id, vg->prune_depth);
}

/* After a completion, re-check this node and ancestors out to k hops. */
static void apply_prune_depth_from(struct vine_graph *vg, struct vine_node *node)
{
	if (!vg || !node) {
		return;
	}
	int k = vg->prune_depth;
	if (k <= 0) {
		return;
	}

	try_prune_depth_release(vg, node);

	struct set *visited = set_create(0);
	struct list *current = list_create();
	list_push_tail(current, node);
	set_insert(visited, node);

	for (int d = 1; d <= k; d++) {
		struct list *next = list_create();
		struct vine_node *n;
		LIST_ITERATE(current, n)
		{
			struct vine_node *p;
			LIST_ITERATE(n->parents, p)
			{
				if (set_lookup(visited, p)) {
					continue;
				}
				set_insert(visited, p);
				list_push_tail(next, p);
				try_prune_depth_release(vg, p);
			}
		}
		list_delete(current);
		current = next;
	}

	list_delete(current);
	set_delete(visited);
}

/* Per-iteration cap on how many queued nodes resubmit processing will handle.
 * Prevents a single pass from monopolizing the main loop when a large batch of
 * tasks fails at once; the remainder is picked up on the next iteration. */
#define RESUBMIT_SCAN_LIMIT 100

/* Minimum delay (in microseconds) between a node's failure and its next retry.
 * Smooths out transient failures (e.g. worker evictions clustered in time) and
 * avoids hot-looping on a deterministically-failing task. */
#define RESUBMIT_COOLDOWN_USECS ((timestamp_t)1000000)

/**
 * Queue a node to be retried later. Idempotent: a node already on the queue
 * is left in place, preserving its original failure timestamp.
 */
static void queue_node_for_retry(struct vine_graph *vg, struct vine_node *node)
{
	if (!vg || !node) {
		return;
	}

	if (node->in_resubmit_queue) {
		return;
	}

	node->last_failure_time = timestamp_get();
	list_push_tail(vg->resubmit_queue, node);
	node->in_resubmit_queue = 1;
}

/* Drain the resubmit queue: reset + resubmit up to RESUBMIT_SCAN_LIMIT nodes,
 * skipping any whose cooldown has not yet elapsed.
 *
 * FIFO invariant: nodes are pushed at enqueue time with monotonically-increasing
 * timestamps, so the head is always the oldest failure. If the head is still in
 * cooldown, every later entry is younger and also in cooldown, so we can stop
 * scanning immediately.
 *
 * When a wait call returns a completed task, the task object is in DONE
 * state, so resetting it for a fresh submit is safe.  Input-missing and
 * temp recovery are handled inside the manager; this path only covers
 * application-level outcomes the manager surfaces as completed tasks
 * (failed results, bad exits, missing artifacts on shared storage, etc.). */
static void drain_resubmit_queue(struct vine_graph *vg)
{
	if (!vg) {
		return;
	}

	timestamp_t now = timestamp_get();
	int queued = list_size(vg->resubmit_queue);
	int budget = queued < RESUBMIT_SCAN_LIMIT ? queued : RESUBMIT_SCAN_LIMIT;

	for (int i = 0; i < budget; i++) {
		struct vine_node *node = list_peek_head(vg->resubmit_queue);
		if (!node) {
			break;
		}
		if (now - node->last_failure_time < RESUBMIT_COOLDOWN_USECS) {
			break;
		}

		list_pop_head(vg->resubmit_queue);
		node->in_resubmit_queue = 0;

		if (--node->retry_attempts_left < 0) {
			debug(D_ERROR, "node %" PRIu64 " has no retries left. Aborting.", node->node_id);
			vine_graph_delete(vg);
			exit(1);
		}

		debug(D_VINE, "Resubmitting node %" PRIu64 " (remaining=%d)", node->node_id, node->retry_attempts_left);
		vine_task_reset(node->task);
		submit_node_task(vg, node);
	}
}

/* Decide whether a just-retrieved task is successful and, on any kind of
 * failure, enqueue its node for resubmission. Returns 1 on success, 0 on
 * failure (caller must skip post-processing and continue the main loop).
 *
 * Two failure modes are handled uniformly:
 *   1. Manager-reported failure: non-SUCCESS result code or non-zero exit.
 *   2. Output-missing failure: for sharedfs outputs, the declared output
 *      file cannot be stat()-ed (worker reported success but no artifact).
 *
 * On success, `outfile_size_bytes` is filled from the on-disk size for PFS
 * returns, or from the in-memory size recorded on the return object for
 * local/temp storage. */
static int validate_task_or_enqueue(struct vine_graph *vg, struct vine_node *node)
{
	struct vine_task *task = node->task;

	if (task->result != VINE_RESULT_SUCCESS || task->exit_code != 0) {
		debug(D_VINE, "Task %d failed (result=%d, exit=%d)", task->task_id, task->result, task->exit_code);
		queue_node_for_retry(vg, node);
		return 0;
	}

	switch (node->outfile_type) {
	case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM: {
		struct stat info;
		if (stat(node->outfile_remote_name, &info) < 0) {
			debug(D_VINE, "Task %d succeeded but missing sharedfs output %s", task->task_id, node->outfile_remote_name);
			queue_node_for_retry(vg, node);
			return 0;
		}
		node->outfile_size_bytes = info.st_size;
		pfs_account_write(vg, node, (size_t)info.st_size);
		break;
	}
	case NODE_OUTFILE_TYPE_LOCAL:
	case NODE_OUTFILE_TYPE_TEMP:
		node->outfile_size_bytes = node->fn_return_file->size;
		break;
	}

	return 1;
}

/*************************************************************/
/* Public APIs */
/*************************************************************/

/** Tune the vine graph.
 *@param vg Reference to the vine graph.
 *@param name Reference to the name of the parameter to tune.
 *@param value Reference to the value of the parameter to tune.
 *@return 0 on success, -1 on failure.
 */
int vine_graph_tune(struct vine_graph *vg, const char *name, const char *value)
{
	if (!vg || !name || !value) {
		return -1;
	}

	if (strcmp(name, "failure-injection-step-percent") == 0) {
		vg->failure_injection_step_percent = atof(value);

	} else if (strcmp(name, "task-priority-mode") == 0) {
		if (strcmp(value, "random") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_RANDOM;
		} else if (strcmp(value, "depth-first") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_DEPTH_FIRST;
		} else if (strcmp(value, "breadth-first") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_BREADTH_FIRST;
		} else if (strcmp(value, "fifo") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_FIFO;
		} else if (strcmp(value, "lifo") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_LIFO;
		} else if (strcmp(value, "largest-input-first") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST;
		} else if (strcmp(value, "largest-storage-footprint-first") == 0) {
			vg->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_STORAGE_FOOTPRINT_FIRST;
		} else {
			debug(D_ERROR, "invalid priority mode: %s", value);
			return -1;
		}

	} else if (strcmp(name, "output-dir") == 0) {
		if (vg->output_dir) {
			free(vg->output_dir);
		}
		if (mkdir(value, 0777) != 0 && errno != EEXIST) {
			debug(D_ERROR, "failed to mkdir %s (errno=%d)", value, errno);
			return -1;
		}
		vg->output_dir = xxstrdup(value);

	} else if (strcmp(name, "prune-depth") == 0) {
		int k = atoi(value);
		if (k < 0) {
			debug(D_ERROR, "invalid prune-depth: %s (must be >= 0; 0 disables prune-depth release)", value);
			return -1;
		}
		vg->prune_depth = k;

	} else if (strcmp(name, "checkpoint-fraction") == 0) {
		double fraction = atof(value);
		if (fraction < 0.0 || fraction > 1.0) {
			debug(D_ERROR, "invalid checkpoint fraction: %s (must be between 0.0 and 1.0)", value);
			return -1;
		}
		vg->checkpoint_fraction = fraction;

	} else if (strcmp(name, "checkpoint-dir") == 0) {
		if (vg->checkpoint_dir) {
			free(vg->checkpoint_dir);
		}
		if (mkdir(value, 0777) != 0 && errno != EEXIST) {
			debug(D_ERROR, "failed to mkdir %s (errno=%d)", value, errno);
			return -1;
		}
		vg->checkpoint_dir = xxstrdup(value);

	} else if (strcmp(name, "progress-bar-update-interval-sec") == 0) {
		double val = atof(value);
		vg->progress_bar_update_interval_sec = (val > 0.0) ? val : 0.1;

	} else if (strcmp(name, "time-metrics-filename") == 0) {
		if (value == NULL || strcmp(value, "0") == 0) {
			vg->time_metrics_filename = NULL;
			return 0;
		}

		if (vg->time_metrics_filename) {
			free(vg->time_metrics_filename);
		}

		vg->time_metrics_filename = xxstrdup(value);

		/** Extract parent directory inline **/
		const char *slash = strrchr(vg->time_metrics_filename, '/');
		if (slash) {
			size_t len = slash - vg->time_metrics_filename;
			char *parent = malloc(len + 1);
			memcpy(parent, vg->time_metrics_filename, len);
			parent[len] = '\0';

			/** Ensure the parent directory exists **/
			if (mkdir(parent, 0777) != 0 && errno != EEXIST) {
				debug(D_ERROR, "failed to mkdir %s (errno=%d)", parent, errno);
				free(parent);
				return -1;
			}
			free(parent);
		}

		/** Truncate or create the file **/
		FILE *fp = fopen(vg->time_metrics_filename, "w");
		if (!fp) {
			debug(D_ERROR, "failed to create file %s (errno=%d)", vg->time_metrics_filename, errno);
			return -1;
		}
		fclose(fp);

	} else if (strcmp(name, "enable-debug-log") == 0) {
		if (vg->enable_debug_log == 0) {
			return -1;
		}
		vg->enable_debug_log = (atoi(value) == 1) ? 1 : 0;
		if (vg->enable_debug_log == 0) {
			debug_flags_clear();
			debug_close();
		}

	} else if (strcmp(name, "max-retry-attempts") == 0) {
		vg->max_retry_attempts = MAX(0, atoi(value));

	} else if (strcmp(name, "print-graph-details") == 0) {
		vg->print_graph_details = (atoi(value) == 1) ? 1 : 0;
	} else {
		debug(D_ERROR, "invalid parameter name: %s", name);
		return -1;
	}

	return 0;
}

/**
 * Get the outfile remote name of a node in the vine graph.
 * @param vg Reference to the vine graph.
 * @param node_id Reference to the node id.
 * @return The outfile remote name.
 */
const char *vine_graph_get_node_outfile_remote_name(const struct vine_graph *vg, uint64_t node_id)
{
	if (!vg) {
		return NULL;
	}

	struct vine_node *node = itable_lookup(vg->nodes, node_id);
	if (!node) {
		return NULL;
	}

	return node->outfile_remote_name;
}

/**
 * Get the proxy library name of the vine graph.
 * @param vg Reference to the vine graph.
 * @return The proxy library name.
 */
const char *vine_graph_get_proxy_library_name(const struct vine_graph *vg)
{
	if (!vg) {
		return NULL;
	}

	return vg->proxy_library_name;
}

/**
 * Add an input file to a task. The input file will be declared as a temp file.
 * @param vg Reference to the vine graph.
 * @param task_id Reference to the task id.
 * @param filename Reference to the filename.
 */
void vine_graph_add_task_input(struct vine_graph *vg, uint64_t task_id, const char *filename)
{
	if (!vg || !task_id || !filename) {
		return;
	}

	struct vine_node *node = itable_lookup(vg->nodes, task_id);
	if (!node) {
		return;
	}

	struct vine_file *f = NULL;
	const char *cached_name = hash_table_lookup(vg->inout_filename_to_cached_name, filename);

	if (cached_name) {
		f = vine_manager_lookup_file(vg->manager, cached_name);
	} else {
		f = vine_declare_temp(vg->manager);
		hash_table_insert(vg->inout_filename_to_cached_name, filename, xxstrdup(f->cached_name));
	}

	vine_task_add_input(node->task, f, filename, VINE_TRANSFER_ALWAYS);
}

/**
 * Add an output file to a task. The output file will be declared as a temp file.
 * @param vg Reference to the vine graph.
 * @param task_id Reference to the task id.
 * @param filename Reference to the filename.
 */
void vine_graph_add_task_output(struct vine_graph *vg, uint64_t task_id, const char *filename)
{
	if (!vg || !task_id || !filename) {
		return;
	}

	struct vine_node *node = itable_lookup(vg->nodes, task_id);
	if (!node) {
		return;
	}

	struct vine_file *f = NULL;
	const char *cached_name = hash_table_lookup(vg->inout_filename_to_cached_name, filename);

	if (cached_name) {
		f = vine_manager_lookup_file(vg->manager, cached_name);
	} else {
		f = vine_declare_temp(vg->manager);
		hash_table_insert(vg->inout_filename_to_cached_name, filename, xxstrdup(f->cached_name));
	}

	vine_task_add_output(node->task, f, filename, VINE_TRANSFER_ALWAYS);
}

/**
 * Set the proxy function name of the vine graph.
 * @param vg Reference to the vine graph.
 * @param proxy_function_name Reference to the proxy function name.
 */
void vine_graph_set_proxy_function_name(struct vine_graph *vg, const char *proxy_function_name)
{
	if (!vg || !proxy_function_name) {
		return;
	}

	if (vg->proxy_function_name) {
		free(vg->proxy_function_name);
	}

	vg->proxy_function_name = xxstrdup(proxy_function_name);
}

/**
 * Get the heavy score of a node in the vine graph.
 * @param vg Reference to the vine graph.
 * @param node_id Reference to the node id.
 * @return The heavy score.
 */
double vine_graph_get_node_heavy_score(const struct vine_graph *vg, uint64_t node_id)
{
	if (!vg) {
		return -1;
	}

	struct vine_node *node = itable_lookup(vg->nodes, node_id);
	if (!node) {
		return -1;
	}

	return node->heavy_score;
}

/**
 * Get the local outfile source of a node in the vine graph, only valid for local output files.
 * The source of a local output file is the path on the local filesystem.
 * @param vg Reference to the vine graph.
 * @param node_id Reference to the node id.
 * @return The local outfile source.
 */
const char *vine_graph_get_node_local_outfile_source(const struct vine_graph *vg, uint64_t node_id)
{
	if (!vg) {
		return NULL;
	}

	struct vine_node *node = itable_lookup(vg->nodes, node_id);
	if (!node) {
		debug(D_ERROR, "node %" PRIu64 " not found", node_id);
		exit(1);
	}

	if (node->outfile_type != NODE_OUTFILE_TYPE_LOCAL) {
		debug(D_ERROR, "node %" PRIu64 " is not a local output file", node_id);
		exit(1);
	}

	return node->fn_return_file->source;
}

/**
 * Compute the topology metrics of the vine graph, including depth, height, upstream and downstream counts,
 * heavy scores, and weakly connected components. Must be called after all nodes and dependencies are added.
 * @param vg Reference to the vine graph.
 */
void vine_graph_finalize(struct vine_graph *vg)
{
	if (!vg) {
		return;
	}

	/* get nodes in topological order */
	struct list *topo_order = get_topological_order(vg);
	if (!topo_order) {
		return;
	}

	struct vine_node *node;
	struct vine_node *parent_node;
	struct vine_node *child_node;

	/* compute the depth of the node */
	LIST_ITERATE(topo_order, node)
	{
		node->depth = 0;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (node->depth < parent_node->depth + 1) {
				node->depth = parent_node->depth + 1;
			}
		}
	}

	/* compute the height of the node */
	LIST_ITERATE_REVERSE(topo_order, node)
	{
		node->height = 0;
		LIST_ITERATE(node->children, child_node)
		{
			if (node->height < child_node->height + 1) {
				node->height = child_node->height + 1;
			}
		}
	}

	int total_nodes = list_size(topo_order);
	int total_target_nodes = 0;
	LIST_ITERATE(topo_order, node)
	{
		if (node->is_target) {
			total_target_nodes++;
		}
	}

	/* Calculate the number of intermediate nodes to checkpoint.
	 * If this is zero, skip heavy-score computation and sorting entirely. */
	int checkpoint_count = (int)((total_nodes - total_target_nodes) * vg->checkpoint_fraction);
	if (checkpoint_count < 0) {
		checkpoint_count = 0;
	}

	if (checkpoint_count > 0) {
		/* Only pay the (large) transitive-closure-like cost when we will use it. */
		compute_upstream_downstream_and_heavy_scores(vg, topo_order);

		/* sort nodes using priority queue */
		struct priority_queue *sorted_nodes = priority_queue_create(total_nodes);
		LIST_ITERATE(topo_order, node)
		{
			priority_queue_push(sorted_nodes, node, node->heavy_score);
		}

		/* assign outfile types to each node */
		int assigned_checkpoint_count = 0;
		while ((node = priority_queue_pop(sorted_nodes))) {
			if (node->is_target) {
				/* declare the return as a normal managed file for retrieval as usual */
				assign_node_outfile_local(vg, node);
				continue;
			}
			if (assigned_checkpoint_count < checkpoint_count) {
				/* checkpointed files will be written directly to the shared file system, no need to manage them in the manager */
				node->outfile_type = NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM;
				char *shared_file_system_outfile_path = string_format("%s/%s", vg->checkpoint_dir, node->outfile_remote_name);
				free(node->outfile_remote_name);
				node->outfile_remote_name = shared_file_system_outfile_path;
				node->fn_return_file = NULL;
				assigned_checkpoint_count++;
			} else {
				/* other nodes will be declared as temp files to leverage node-local storage */
				assign_node_outfile_temp(vg, node);
			}
		}
		priority_queue_delete(sorted_nodes);
	} else {
		/* Fast path: no checkpointing means no heavy-score computation/sorting. */
		LIST_ITERATE(topo_order, node)
		{
			if (node->is_target) {
				assign_node_outfile_local(vg, node);
			} else {
				assign_node_outfile_temp(vg, node);
			}
		}
	}

	/* track outputs for all nodes that carry a return object */
	LIST_ITERATE(topo_order, node)
	{
		if (node->fn_return_file) {
			vine_task_add_output(node->task, node->fn_return_file, node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
		}
	}

	/* extract weakly connected components (could be expensive) */
	if (vg->print_graph_details) {
		struct list *weakly_connected_components = extract_weakly_connected_components(vg);
		struct list *component;
		int component_index = 0;
		debug(D_VINE, "graph has %d weakly connected components\n", list_size(weakly_connected_components));
		LIST_ITERATE(weakly_connected_components, component)
		{
			debug(D_VINE, "component %d size: %d\n", component_index, list_size(component));
			list_delete(component);
			component_index++;
		}
		list_delete(weakly_connected_components);
	}

	/* print the node details if enabled */
	if (vg->print_graph_details) {
		LIST_ITERATE(topo_order, node)
		{
			vine_node_debug_print(node);
		}
	}

	/* create mappings from task IDs and outfile cache names to nodes */
	LIST_ITERATE(topo_order, node)
	{
		if (node->fn_return_file) {
			hash_table_insert(vg->outfile_cachename_to_node, node->fn_return_file->cached_name, node);
		}
	}

	/* add the parents' outfiles as inputs to the task */
	LIST_ITERATE(topo_order, node)
	{
		struct vine_node *parent_node;
		LIST_ITERATE(node->parents, parent_node)
		{
			if (parent_node->fn_return_file) {
				vine_task_add_input(node->task, parent_node->fn_return_file, parent_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
			}
		}
	}

	/* initialize remaining_parents_count for all nodes */
	LIST_ITERATE(topo_order, node)
	{
		node->remaining_parents_count = list_size(node->parents);
	}

	/* enqueue those without unresolved dependencies */
	LIST_ITERATE(topo_order, node)
	{
		if (node->remaining_parents_count == 0) {
			submit_node_task(vg, node);
		}
	}

	/* enable return recovery tasks */
	vine_enable_return_recovery_tasks(vg->manager);

	list_delete(topo_order);

	return;
}

/**
 * Create a new node and track it in the vine graph.
 * @param vg Reference to the vine graph.
 * @return The auto-assigned node id.
 */
uint64_t vine_graph_add_node(struct vine_graph *vg)
{
	if (!vg) {
		return 0;
	}

	/* assign a new id based on current node count, ensure uniqueness */
	uint64_t candidate_id = itable_size(vg->nodes);
	candidate_id += 1;
	while (itable_lookup(vg->nodes, candidate_id)) {
		candidate_id++;
	}
	uint64_t node_id = candidate_id;

	/* create the backing node (defaults to non-target) */
	struct vine_node *node = vine_node_create(node_id);

	if (!node) {
		debug(D_ERROR, "failed to create node %" PRIu64, node_id);
		vine_graph_delete(vg);
		exit(1);
	}

	if (!vg->proxy_function_name) {
		debug(D_ERROR, "proxy function name is not set");
		vine_graph_delete(vg);
		exit(1);
	}

	if (!vg->proxy_library_name) {
		debug(D_ERROR, "proxy library name is not set");
		vine_graph_delete(vg);
		exit(1);
	}

	/* create node task */
	node->task = vine_task_create(vg->proxy_function_name);
	vine_task_set_library_required(node->task, vg->proxy_library_name);
	vine_task_addref(node->task);

	/* JSON args for the library proxy; worker mount name remains "infile". */
	char *task_arguments = vine_node_construct_task_arguments(node);
	node->proxy_arg_file = vine_declare_buffer(vg->manager, task_arguments, strlen(task_arguments), VINE_CACHE_LEVEL_TASK, VINE_UNLINK_WHEN_DONE);
	free(task_arguments);
	vine_task_add_input(node->task, node->proxy_arg_file, "infile", VINE_TRANSFER_ALWAYS);

	node->retry_attempts_left = vg->max_retry_attempts;

	itable_insert(vg->nodes, node_id, node);

	return node_id;
}

/**
 * Mark a node as a retrieval target.
 */
void vine_graph_set_target(struct vine_graph *vg, uint64_t node_id)
{
	if (!vg) {
		return;
	}
	struct vine_node *node = itable_lookup(vg->nodes, node_id);
	if (!node) {
		debug(D_ERROR, "node %" PRIu64 " not found", node_id);
		exit(1);
	}

	node->is_target = 1;
}

/**
 * Create a new vine graph and bind a manager to it.
 * @param q Reference to the manager object.
 * @return A new vine graph instance.
 */
struct vine_graph *vine_graph_create(struct vine_manager *q)
{
	if (!q) {
		return NULL;
	}

	struct vine_graph *vg = xxmalloc(sizeof(struct vine_graph));

	vg->manager = q;

	vg->checkpoint_dir = xxstrdup(vg->manager->runtime_directory); // default to current working directory
	vg->output_dir = xxstrdup(vg->manager->runtime_directory);     // default to current working directory

	vg->nodes = itable_create(0);
	vg->task_id_to_node = itable_create(0);
	vg->outfile_cachename_to_node = hash_table_create(0, 0);
	vg->inout_filename_to_cached_name = hash_table_create(0, 0);
	vg->resubmit_queue = list_create();

	cctools_uuid_t proxy_library_name_id;
	cctools_uuid_create(&proxy_library_name_id);
	vg->proxy_library_name = xxstrdup(proxy_library_name_id.str);

	vg->proxy_function_name = NULL;

	/* Default prune-depth: release a TEMP node as soon as all of its
	 * direct children have completed. Set to 0 via tune("prune-depth") to
	 * disable and rely exclusively on cut-propagation. */
	vg->prune_depth = 1;

	vg->time_spent_on_cut_propagation = 0;

	vg->pfs_usage_bytes = 0;

	vg->print_graph_details = 0;

	vg->task_priority_mode = TASK_PRIORITY_MODE_LARGEST_INPUT_FIRST;
	vg->failure_injection_step_percent = -1.0;

	vg->progress_bar_update_interval_sec = 0.1;

	/* enable debug system for C code since it uses a separate debug system instance
	 * from the Python bindings. Use the same function that the manager uses. */
	char *debug_tmp = string_format("%s/vine-logs/debug", vg->manager->runtime_directory);
	vine_enable_debug_log(debug_tmp);
	free(debug_tmp);

	vg->time_metrics_filename = NULL;

	vg->enable_debug_log = 1;

	vg->max_retry_attempts = 15;

	vg->time_first_task_dispatched = UINT64_MAX;
	vg->time_last_task_retrieved = 0;
	vg->makespan_us = 0;
	vg->completed_recovery_tasks = 0;

	return vg;
}

/**
 * Add a dependency between two nodes in the vine graph. Note that the input-output file relationship
 * is not handled here, because their file names might not have been determined yet.
 * @param vg Reference to the vine graph.
 * @param parent_id Reference to the parent node id.
 * @param child_id Reference to the child node id.
 */
void vine_graph_add_dependency(struct vine_graph *vg, uint64_t parent_id, uint64_t child_id)
{
	if (!vg) {
		return;
	}

	struct vine_node *parent_node = itable_lookup(vg->nodes, parent_id);
	struct vine_node *child_node = itable_lookup(vg->nodes, child_id);
	if (!parent_node) {
		debug(D_ERROR, "parent node %" PRIu64 " not found", parent_id);
		uint64_t nid;
		struct vine_node *node;
		printf("parent_ids:\n");
		ITABLE_ITERATE(vg->nodes, nid, node)
		{
			printf("  %" PRIu64 "\n", node->node_id);
		}
		exit(1);
	}
	if (!child_node) {
		debug(D_ERROR, "child node %" PRIu64 " not found", child_id);
		exit(1);
	}

	list_push_tail(child_node->parents, parent_node);
	list_push_tail(parent_node->children, child_node);

	return;
}

uint64_t vine_graph_get_makespan_us(const struct vine_graph *vg)
{
	if (!vg) {
		return 0;
	}

	return (uint64_t)vg->makespan_us;
}

uint64_t vine_graph_get_total_recovery_tasks(const struct vine_graph *vg)
{
	if (!vg || !vg->manager || !vg->manager->stats) {
		return 0;
	}

	return (uint64_t)vg->manager->stats->tasks_recovery;
}

uint64_t vine_graph_get_completed_recovery_tasks(const struct vine_graph *vg)
{
	if (!vg) {
		return 0;
	}

	return vg->completed_recovery_tasks;
}

/**
 * Execute the vine graph. This must be called after all nodes and dependencies are added and the topology metrics are computed.
 * @param vg Reference to the vine graph.
 */
void vine_graph_execute(struct vine_graph *vg)
{
	if (!vg) {
		return;
	}

	void (*previous_sigint_handler)(int) = signal(SIGINT, handle_sigint);

	debug(D_VINE, "start executing vine graph");

	struct ProgressBar *pbar = progress_bar_init("Executing Tasks");
	progress_bar_set_update_interval(pbar, vg->progress_bar_update_interval_sec);
	vg->completed_recovery_tasks = 0;

	struct ProgressBarPart *user_tasks_part = progress_bar_create_part("User", itable_size(vg->nodes));
	struct ProgressBarPart *recovery_tasks_part = progress_bar_create_part("Recovery", 0);
	progress_bar_bind_part(pbar, user_tasks_part);
	progress_bar_bind_part(pbar, recovery_tasks_part);

	/* calculate steps to inject failure */
	double next_failure_threshold = -1.0;
	if (vg->failure_injection_step_percent > 0) {
		next_failure_threshold = vg->failure_injection_step_percent / 100.0;
	}

	int wait_timeout = 1;
	while (user_tasks_part->current < user_tasks_part->total) {
		if (interrupted) {
			break;
		}

		/* Process graph-level resubmissions (non-INPUT_MISSING retries only;
		 * all temp-file recovery is done inside the manager). */
		drain_resubmit_queue(vg);

		/* Recovery bar tracks manager-created recovery tasks. */
		progress_bar_set_part_total(pbar, recovery_tasks_part, vine_graph_get_total_recovery_tasks(vg));

		struct vine_task *task = vine_wait(vg->manager, wait_timeout);
		if (task) {
			/* retrieve all possible tasks */
			wait_timeout = 0;
			timestamp_t time_when_postprocessing_start = timestamp_get();

			if (task->type == VINE_TASK_TYPE_RECOVERY) {
				vg->completed_recovery_tasks++;
				progress_bar_update_part(
						pbar,
						recovery_tasks_part,
						vg->completed_recovery_tasks - recovery_tasks_part->current);
			}

			/* get the original node by task id */
			struct vine_node *node = get_node_by_task(vg, task);
			if (!node) {
				debug(D_ERROR, "fatal: task %d could not be mapped to a task node, this indicates a serious bug.", task->task_id);
				exit(1);
			}

			if (task->time_when_commit_end > 0) {
				vg->time_first_task_dispatched = MIN(vg->time_first_task_dispatched, task->time_when_commit_end);
			}

			/* Unified success check: handles both manager-reported failures and
			 * sharedfs output-missing failures. On failure the node is already
			 * enqueued for resubmission; skip all post-processing. */
			if (!validate_task_or_enqueue(vg, node)) {
				continue;
			}

			/* update time metrics */
			vg->time_last_task_retrieved = MAX(vg->time_last_task_retrieved, task->time_when_retrieval);
			if (vg->time_last_task_retrieved < vg->time_first_task_dispatched) {
				debug(D_ERROR, "task %d time_last_task_retrieved < time_first_task_dispatched: %" PRIu64 " < %" PRIu64, task->task_id, vg->time_last_task_retrieved, vg->time_first_task_dispatched);
				vg->time_last_task_retrieved = vg->time_first_task_dispatched;
			}
			vg->makespan_us = vg->time_last_task_retrieved - vg->time_first_task_dispatched;

			debug(D_VINE, "Node %" PRIu64 " completed with outfile %s size: %zu bytes", node->node_id, node->outfile_remote_name, node->outfile_size_bytes);

			/* mark the node as completed
			 * Note: a node may complete multiple times due to resubmission/recomputation.
			 * Only the first completion should advance the "User" progress. */
			int first_completion = !node->completed;
			node->completed = 1;
			node->commit_time = task->time_when_commit_end - task->time_when_commit_start;
			node->execution_time = task->time_workers_execute_last;
			node->retrieval_time = task->time_when_done - task->time_when_retrieval;

			/* A recovery completion regenerates a previously-released TEMP
			 * output. Clear any prior cut / prune-depth verdict on this
			 * node so the rules below can re-evaluate from scratch and,
			 * if conditions still hold, release the freshly-produced data
			 * again. First completions are a no-op here because the flags
			 * are already 0 at that point. */
			if (task->type == VINE_TASK_TYPE_RECOVERY) {
				node->cut = 0;
				node->prune_depth_pruned = 0;
			}

			/* Walk the cut-propagation upward from this freshly-completed
			 * node so any upstream data that is no longer needed gets
			 * dropped as early as possible. */
			propagate_cut_from(vg, node);

			/* Separately, apply the prune-depth release window. This is
			 * orthogonal to cut: it targets only TEMP data on workers and
			 * willingly trades some recovery resilience for earlier
			 * worker-local storage reclamation. Controlled by the
			 * `prune-depth` tune knob (0 disables). */
			apply_prune_depth_from(vg, node);

			/* Recovery tasks were already counted in the Recovery bar above; they are
			 * not user-submitted nodes, so skip the user-progress bookkeeping. */
			if (task->type == VINE_TASK_TYPE_RECOVERY) {
				continue;
			}

			if (first_completion) {
				/* set the start time to the submit time of the first user task */
				if (user_tasks_part->current == 0) {
					progress_bar_set_start_time(pbar, task->time_when_commit_start);
				}

				/* update critical time */
				vine_node_update_critical_path_time(node, node->execution_time);

				/* mark this user task as completed */
				progress_bar_update_part(pbar, user_tasks_part, 1);
			}

			/* inject failure */
			if (vg->failure_injection_step_percent > 0) {
				double progress = (double)user_tasks_part->current / (double)user_tasks_part->total;
				if (progress >= next_failure_threshold && release_random_worker(vg->manager)) {
					debug(D_VINE, "released a random worker at %.2f%% (threshold %.2f%%)", progress * 100, next_failure_threshold * 100);
					next_failure_threshold += vg->failure_injection_step_percent / 100.0;
				}
			}

			/* enqueue the output file for replication */
			switch (node->outfile_type) {
			case NODE_OUTFILE_TYPE_TEMP:
				/* replicate the outfile of the temp node */
				vine_temp_queue_for_replication(vg->manager, node->fn_return_file);
				break;
			case NODE_OUTFILE_TYPE_LOCAL:
			case NODE_OUTFILE_TYPE_SHARED_FILE_SYSTEM:
				break;
			}

			/* submit children nodes with dependencies all resolved */
			submit_unblocked_children(vg, node);

			timestamp_t time_when_postprocessing_end = timestamp_get();
			node->postprocessing_time = time_when_postprocessing_end - time_when_postprocessing_start;
		} else {
			wait_timeout = 1;
		}
	}

	progress_bar_finish(pbar);
	progress_bar_delete(pbar);

	debug(D_VINE, "total time spent on cut propagation: %.6f seconds\n", vg->time_spent_on_cut_propagation / 1e6);

	signal(SIGINT, previous_sigint_handler);
	if (interrupted) {
		raise(SIGINT);
	}

	return;
}

/**
 * Delete a vine graph instance.
 * @param vg Reference to the vine graph.
 */
void vine_graph_delete(struct vine_graph *vg)
{
	if (!vg) {
		return;
	}

	uint64_t nid;
	struct vine_node *node;
	ITABLE_ITERATE(vg->nodes, nid, node)
	{
		if (node->proxy_arg_file) {
			vine_prune_file(vg->manager, node->proxy_arg_file);
		}
		delete_node_return_file(vg, node);
		if (node->proxy_arg_file) {
			hash_table_remove(vg->manager->file_table, node->proxy_arg_file->cached_name);
		}
		if (node->fn_return_file) {
			hash_table_remove(vg->outfile_cachename_to_node, node->fn_return_file->cached_name);
			hash_table_remove(vg->manager->file_table, node->fn_return_file->cached_name);
		}
		vine_node_delete(node);
	}

	list_delete(vg->resubmit_queue);

	free(vg->proxy_library_name);
	free(vg->proxy_function_name);

	itable_delete(vg->nodes);
	itable_delete(vg->task_id_to_node);
	hash_table_delete(vg->outfile_cachename_to_node);

	hash_table_clear(vg->inout_filename_to_cached_name, (void *)free);
	hash_table_delete(vg->inout_filename_to_cached_name);

	free(vg);
}
