/* DataVine scheduler implementation.
Copyright (C) 2026- The University of Notre Dame
See the file COPYING for details.
*/

#include "vine_datavine_scheduler.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define NO_EDGE UINT64_MAX

struct scheduler_slot {
	uint32_t initial_dependencies;
	uint32_t remaining_dependencies;
	int64_t heap_index;
	uint64_t first_dependent;
	uint64_t last_dependent;
	uint64_t staged_index;
	uint64_t validation_mark;
	unsigned char registered;
	unsigned char staged;
	unsigned char state;
};

struct scheduler_edge {
	uint64_t child;
	uint64_t next;
};

struct staged_task {
	uint64_t task_id;
	uint64_t first_parent;
	uint32_t parent_count;
};

struct staged_parent {
	uint64_t parent;
	uint64_t child_index;
	uint64_t next;
};

struct vine_datavine_scheduler {
	int64_t maximum_task_id;
	uint64_t maximum_tasks;
	uint64_t maximum_edges;
	struct scheduler_slot *slots;
	struct scheduler_edge *edges;
	uint64_t edge_count;
	uint64_t task_count;
	uint64_t done_count;
	uint64_t *heap;
	uint64_t heap_size;
	uint64_t revision;
	uint64_t validation_epoch;
	int sealed;
	int updating;
	struct staged_task *staged_tasks;
	uint64_t staged_task_count;
	uint64_t staged_task_capacity;
	struct staged_parent *staged_parents;
	uint64_t staged_parent_count;
	uint64_t staged_parent_capacity;
};

static uint64_t read_u64_le(const unsigned char *data)
{
	uint64_t value = 0;
	for (int i = 7; i >= 0; i--) {
		value = (value << 8) | data[i];
	}
	return value;
}

static int valid_task_id(struct vine_datavine_scheduler *scheduler, int64_t task_id)
{
	return scheduler && task_id > 0 && task_id <= scheduler->maximum_task_id && scheduler->slots[task_id].registered;
}

static void heap_swap(struct vine_datavine_scheduler *scheduler, uint64_t a, uint64_t b)
{
	uint64_t task_a = scheduler->heap[a];
	uint64_t task_b = scheduler->heap[b];
	scheduler->heap[a] = task_b;
	scheduler->heap[b] = task_a;
	scheduler->slots[task_a].heap_index = (int64_t)b;
	scheduler->slots[task_b].heap_index = (int64_t)a;
}

static void heap_push(struct vine_datavine_scheduler *scheduler, uint64_t task_id)
{
	struct scheduler_slot *slot = &scheduler->slots[task_id];
	if (slot->heap_index >= 0)
		return;
	uint64_t index = scheduler->heap_size++;
	scheduler->heap[index] = task_id;
	slot->heap_index = (int64_t)index;
	while (index > 0) {
		uint64_t parent = (index - 1) / 2;
		if (scheduler->heap[parent] <= scheduler->heap[index])
			break;
		heap_swap(scheduler, parent, index);
		index = parent;
	}
}

static uint64_t heap_remove(struct vine_datavine_scheduler *scheduler, uint64_t index)
{
	if (index >= scheduler->heap_size)
		return 0;
	uint64_t removed = scheduler->heap[index];
	scheduler->slots[removed].heap_index = -1;
	scheduler->heap_size--;
	if (index == scheduler->heap_size)
		return removed;
	scheduler->heap[index] = scheduler->heap[scheduler->heap_size];
	scheduler->slots[scheduler->heap[index]].heap_index = (int64_t)index;
	while (index > 0) {
		uint64_t parent = (index - 1) / 2;
		if (scheduler->heap[parent] <= scheduler->heap[index])
			break;
		heap_swap(scheduler, parent, index);
		index = parent;
	}
	for (;;) {
		uint64_t left = index * 2 + 1;
		if (left >= scheduler->heap_size)
			break;
		uint64_t right = left + 1;
		uint64_t child = right < scheduler->heap_size && scheduler->heap[right] < scheduler->heap[left]
						 ? right
						 : left;
		if (scheduler->heap[index] <= scheduler->heap[child])
			break;
		heap_swap(scheduler, index, child);
		index = child;
	}
	return removed;
}

static void append_edge(struct vine_datavine_scheduler *scheduler,
		uint64_t parent, uint64_t child)
{
	uint64_t index = scheduler->edge_count++;
	scheduler->edges[index] = (struct scheduler_edge){child, NO_EDGE};
	struct scheduler_slot *slot = &scheduler->slots[parent];
	if (slot->last_dependent == NO_EDGE) {
		slot->first_dependent = index;
	} else {
		scheduler->edges[slot->last_dependent].next = index;
	}
	slot->last_dependent = index;
}

static int reserve_staging(struct vine_datavine_scheduler *scheduler,
		uint64_t tasks, uint64_t parents)
{
	if (tasks > scheduler->staged_task_capacity) {
		uint64_t capacity = scheduler->staged_task_capacity
						    ? scheduler->staged_task_capacity
						    : 16;
		while (capacity < tasks)
			capacity *= 2;
		if (capacity > scheduler->maximum_tasks)
			capacity = scheduler->maximum_tasks;
		struct staged_task *next = realloc(scheduler->staged_tasks,
				(size_t)capacity * sizeof(*next));
		if (!next)
			return 0;
		scheduler->staged_tasks = next;
		scheduler->staged_task_capacity = capacity;
	}
	if (parents > scheduler->staged_parent_capacity) {
		uint64_t capacity = scheduler->staged_parent_capacity
						    ? scheduler->staged_parent_capacity
						    : 32;
		while (capacity < parents)
			capacity *= 2;
		if (capacity > scheduler->maximum_edges)
			capacity = scheduler->maximum_edges;
		struct staged_parent *next = realloc(scheduler->staged_parents,
				(size_t)capacity * sizeof(*next));
		if (!next)
			return 0;
		scheduler->staged_parents = next;
		scheduler->staged_parent_capacity = capacity;
	}
	return 1;
}

static void clear_update(struct vine_datavine_scheduler *scheduler)
{
	for (uint64_t i = 0; i < scheduler->staged_task_count; i++) {
		struct scheduler_slot *slot =
				&scheduler->slots[scheduler->staged_tasks[i].task_id];
		slot->staged = 0;
		slot->staged_index = 0;
	}
	scheduler->staged_task_count = 0;
	scheduler->staged_parent_count = 0;
	scheduler->updating = 0;
}

static void stage_parent(struct vine_datavine_scheduler *scheduler,
		uint64_t child_index, uint64_t parent)
{
	struct staged_task *task = &scheduler->staged_tasks[child_index];
	uint64_t index = scheduler->staged_parent_count++;
	scheduler->staged_parents[index] =
			(struct staged_parent){parent, child_index, task->first_parent};
	task->first_parent = index;
	task->parent_count++;
}

struct vine_datavine_scheduler *vine_datavine_scheduler_create(
		int64_t maximum_task_id, uint64_t maximum_tasks, uint64_t maximum_edges)
{
	if (maximum_task_id < 1 || maximum_tasks < 1)
		return 0;
	struct vine_datavine_scheduler *scheduler = calloc(1, sizeof(*scheduler));
	if (!scheduler)
		return 0;
	scheduler->maximum_task_id = maximum_task_id;
	scheduler->maximum_tasks = maximum_tasks;
	scheduler->maximum_edges = maximum_edges;
	scheduler->slots = calloc((size_t)maximum_task_id + 1, sizeof(*scheduler->slots));
	scheduler->edges = maximum_edges
					   ? calloc((size_t)maximum_edges, sizeof(*scheduler->edges))
					   : 0;
	scheduler->heap = calloc((size_t)maximum_tasks, sizeof(*scheduler->heap));
	if (!scheduler->slots || !scheduler->heap || (maximum_edges && !scheduler->edges)) {
		vine_datavine_scheduler_delete(scheduler);
		return 0;
	}
	for (int64_t task_id = 0; task_id <= maximum_task_id; task_id++) {
		scheduler->slots[task_id].heap_index = -1;
		scheduler->slots[task_id].first_dependent = NO_EDGE;
		scheduler->slots[task_id].last_dependent = NO_EDGE;
	}
	return scheduler;
}

void vine_datavine_scheduler_delete(struct vine_datavine_scheduler *scheduler)
{
	if (!scheduler)
		return;
	free(scheduler->slots);
	free(scheduler->edges);
	free(scheduler->heap);
	free(scheduler->staged_tasks);
	free(scheduler->staged_parents);
	free(scheduler);
}

int vine_datavine_scheduler_add_task(struct vine_datavine_scheduler *scheduler,
		int64_t task_id, const char *buffer, size_t size)
{
	uint64_t parent_count = size / 8;
	if (!scheduler || task_id < 1 || task_id > scheduler->maximum_task_id || size % 8 || (size && !buffer) || parent_count > UINT32_MAX || scheduler->task_count + scheduler->staged_task_count >= scheduler->maximum_tasks || scheduler->slots[task_id].registered || scheduler->slots[task_id].staged || scheduler->edge_count + scheduler->staged_parent_count + parent_count > scheduler->maximum_edges) {
		return 0;
	}
	if (scheduler->sealed && !scheduler->updating) {
		return 0;
	}
	for (uint64_t i = 0; i < parent_count; i++) {
		uint64_t parent = read_u64_le((const unsigned char *)buffer + i * 8);
		if (!parent || parent > (uint64_t)scheduler->maximum_task_id || parent == (uint64_t)task_id) {
			return 0;
		}
	}
	if (scheduler->updating) {
		uint64_t task_count = scheduler->staged_task_count + 1;
		uint64_t edge_count = scheduler->staged_parent_count + parent_count;
		if (!reserve_staging(scheduler, task_count, edge_count))
			return 0;
		uint64_t task_index = scheduler->staged_task_count++;
		scheduler->staged_tasks[task_index] =
				(struct staged_task){(uint64_t)task_id, NO_EDGE, 0};
		scheduler->slots[task_id].staged = 1;
		scheduler->slots[task_id].staged_index = task_index;
		for (uint64_t i = 0; i < parent_count; i++) {
			stage_parent(scheduler, task_index, read_u64_le((const unsigned char *)buffer + i * 8));
		}
		return 1;
	}

	struct scheduler_slot *slot = &scheduler->slots[task_id];
	slot->registered = 1;
	slot->state = VINE_DATAVINE_TASK_WAITING;
	slot->initial_dependencies = (uint32_t)parent_count;
	slot->remaining_dependencies = (uint32_t)parent_count;
	scheduler->task_count++;
	for (uint64_t i = 0; i < parent_count; i++) {
		append_edge(scheduler,
				read_u64_le((const unsigned char *)buffer + i * 8),
				(uint64_t)task_id);
	}
	return 1;
}

static int validate_initial_graph(struct vine_datavine_scheduler *scheduler)
{
	uint32_t *remaining = calloc((size_t)scheduler->maximum_task_id + 1,
			sizeof(*remaining));
	uint64_t *queue = calloc((size_t)scheduler->maximum_tasks, sizeof(*queue));
	uint64_t *seen = calloc((size_t)scheduler->maximum_task_id + 1,
			sizeof(*seen));
	if (!remaining || !queue || !seen) {
		free(remaining);
		free(queue);
		free(seen);
		return 0;
	}
	uint64_t head = 0, tail = 0, visited = 0;
	for (int64_t parent = 1; parent <= scheduler->maximum_task_id; parent++) {
		struct scheduler_slot *slot = &scheduler->slots[parent];
		if (!slot->registered && slot->first_dependent != NO_EDGE) {
			free(remaining);
			free(queue);
			free(seen);
			return 0;
		}
		if (!slot->registered)
			continue;
		remaining[parent] = slot->initial_dependencies;
		if (!remaining[parent])
			queue[tail++] = (uint64_t)parent;
		for (uint64_t edge = slot->first_dependent; edge != NO_EDGE;
				edge = scheduler->edges[edge].next) {
			uint64_t child = scheduler->edges[edge].child;
			if (!scheduler->slots[child].registered || seen[child] == (uint64_t)parent) {
				free(remaining);
				free(queue);
				free(seen);
				return 0;
			}
			seen[child] = (uint64_t)parent;
		}
	}
	while (head < tail) {
		uint64_t parent = queue[head++];
		visited++;
		for (uint64_t edge = scheduler->slots[parent].first_dependent;
				edge != NO_EDGE;
				edge = scheduler->edges[edge].next) {
			uint64_t child = scheduler->edges[edge].child;
			if (--remaining[child] == 0)
				queue[tail++] = child;
		}
	}
	free(remaining);
	free(queue);
	free(seen);
	return visited == scheduler->task_count;
}

int vine_datavine_scheduler_seal(struct vine_datavine_scheduler *scheduler)
{
	if (!scheduler || scheduler->sealed || scheduler->updating || !validate_initial_graph(scheduler)) {
		return 0;
	}
	scheduler->sealed = 1;
	scheduler->revision = 1;
	for (int64_t task_id = 1; task_id <= scheduler->maximum_task_id; task_id++) {
		struct scheduler_slot *slot = &scheduler->slots[task_id];
		if (slot->registered && !slot->remaining_dependencies) {
			slot->state = VINE_DATAVINE_TASK_READY;
			heap_push(scheduler, (uint64_t)task_id);
		}
	}
	return 1;
}

uint64_t vine_datavine_scheduler_revision(struct vine_datavine_scheduler *scheduler)
{
	return scheduler ? scheduler->revision : 0;
}

int vine_datavine_scheduler_begin_update(
		struct vine_datavine_scheduler *scheduler, uint64_t base_revision)
{
	if (!scheduler || !scheduler->sealed || scheduler->updating || base_revision != scheduler->revision) {
		return 0;
	}
	scheduler->updating = 1;
	return 1;
}

static int validate_update(struct vine_datavine_scheduler *scheduler,
		uint32_t *indegree, uint64_t *queue, uint64_t *first_out,
		uint64_t *next_out)
{
	uint64_t head = 0, tail = 0, visited = 0;
	for (uint64_t i = 0; i < scheduler->staged_task_count; i++) {
		first_out[i] = NO_EDGE;
	}
	for (uint64_t i = 0; i < scheduler->staged_task_count; i++) {
		struct staged_task *task = &scheduler->staged_tasks[i];
		if (++scheduler->validation_epoch == 0)
			scheduler->validation_epoch++;
		for (uint64_t edge = task->first_parent; edge != NO_EDGE;
				edge = scheduler->staged_parents[edge].next) {
			uint64_t parent = scheduler->staged_parents[edge].parent;
			if ((!scheduler->slots[parent].registered && !scheduler->slots[parent].staged) || scheduler->slots[parent].validation_mark == scheduler->validation_epoch)
				return 0;
			scheduler->slots[parent].validation_mark = scheduler->validation_epoch;
			if (scheduler->slots[parent].staged) {
				uint64_t parent_index = scheduler->slots[parent].staged_index;
				indegree[i]++;
				next_out[edge] = first_out[parent_index];
				first_out[parent_index] = edge;
			}
		}
		if (!indegree[i])
			queue[tail++] = i;
	}
	while (head < tail) {
		uint64_t completed = queue[head++];
		visited++;
		for (uint64_t edge = first_out[completed]; edge != NO_EDGE;
				edge = next_out[edge]) {
			uint64_t child = scheduler->staged_parents[edge].child_index;
			if (--indegree[child] == 0)
				queue[tail++] = child;
		}
	}
	return visited == scheduler->staged_task_count;
}

int vine_datavine_scheduler_commit_update(struct vine_datavine_scheduler *scheduler)
{
	if (!scheduler || !scheduler->updating || !scheduler->staged_task_count) {
		return 0;
	}
	uint32_t *indegree = calloc((size_t)scheduler->staged_task_count, sizeof(*indegree));
	uint64_t *queue = calloc((size_t)scheduler->staged_task_count, sizeof(*queue));
	uint64_t *first_out = malloc((size_t)scheduler->staged_task_count * sizeof(*first_out));
	uint64_t *next_out = scheduler->staged_parent_count
					     ? malloc((size_t)scheduler->staged_parent_count * sizeof(*next_out))
					     : 0;
	int valid = indegree && queue && first_out && (!scheduler->staged_parent_count || next_out) && validate_update(scheduler, indegree, queue, first_out, next_out);
	free(indegree);
	free(queue);
	free(first_out);
	free(next_out);
	if (!valid) {
		clear_update(scheduler);
		return 0;
	}

	for (uint64_t i = 0; i < scheduler->staged_task_count; i++) {
		struct staged_task *task = &scheduler->staged_tasks[i];
		struct scheduler_slot *slot = &scheduler->slots[task->task_id];
		slot->registered = 1;
		slot->state = VINE_DATAVINE_TASK_WAITING;
		slot->initial_dependencies = task->parent_count;
		slot->remaining_dependencies = 0;
		for (uint64_t edge = task->first_parent; edge != NO_EDGE;
				edge = scheduler->staged_parents[edge].next) {
			uint64_t parent = scheduler->staged_parents[edge].parent;
			if (scheduler->slots[parent].state != VINE_DATAVINE_TASK_DONE) {
				slot->remaining_dependencies++;
			}
			append_edge(scheduler, parent, task->task_id);
		}
		scheduler->task_count++;
	}
	for (uint64_t i = 0; i < scheduler->staged_task_count; i++) {
		struct scheduler_slot *slot =
				&scheduler->slots[scheduler->staged_tasks[i].task_id];
		if (!slot->remaining_dependencies) {
			slot->state = VINE_DATAVINE_TASK_READY;
			heap_push(scheduler, scheduler->staged_tasks[i].task_id);
		}
	}
	scheduler->revision++;
	clear_update(scheduler);
	return 1;
}

void vine_datavine_scheduler_abort_update(struct vine_datavine_scheduler *scheduler)
{
	if (scheduler && scheduler->updating)
		clear_update(scheduler);
}

int64_t vine_datavine_scheduler_take(struct vine_datavine_scheduler *scheduler)
{
	if (!scheduler || !scheduler->sealed || !scheduler->heap_size)
		return 0;
	uint64_t task_id = heap_remove(scheduler, 0);
	scheduler->slots[task_id].state = VINE_DATAVINE_TASK_RUNNING;
	return (int64_t)task_id;
}

int vine_datavine_scheduler_mark_done(struct vine_datavine_scheduler *scheduler, int64_t task_id)
{
	if (!valid_task_id(scheduler, task_id) || !scheduler->sealed || scheduler->slots[task_id].state != VINE_DATAVINE_TASK_RUNNING) {
		return 0;
	}
	scheduler->slots[task_id].state = VINE_DATAVINE_TASK_DONE;
	scheduler->done_count++;
	for (uint64_t edge = scheduler->slots[task_id].first_dependent;
			edge != NO_EDGE;
			edge = scheduler->edges[edge].next) {
		uint64_t child_id = scheduler->edges[edge].child;
		struct scheduler_slot *child = &scheduler->slots[child_id];
		if (!child->remaining_dependencies) {
			return 0;
		}
		child->remaining_dependencies--;
		if (!child->remaining_dependencies && child->state == VINE_DATAVINE_TASK_WAITING) {
			child->state = VINE_DATAVINE_TASK_READY;
			heap_push(scheduler, child_id);
		}
	}
	return 1;
}

int vine_datavine_scheduler_mark_pending(struct vine_datavine_scheduler *scheduler, int64_t task_id)
{
	if (!valid_task_id(scheduler, task_id) || !scheduler->sealed || scheduler->slots[task_id].state != VINE_DATAVINE_TASK_RUNNING) {
		return 0;
	}
	struct scheduler_slot *slot = &scheduler->slots[task_id];
	slot->state = slot->remaining_dependencies ? VINE_DATAVINE_TASK_WAITING
						   : VINE_DATAVINE_TASK_READY;
	if (!slot->remaining_dependencies)
		heap_push(scheduler, (uint64_t)task_id);
	return 1;
}

int vine_datavine_scheduler_rebuild(struct vine_datavine_scheduler *scheduler,
		const char *buffer, size_t size)
{
	if (!scheduler || !scheduler->sealed || scheduler->updating || size % 8 || (size && !buffer)) {
		return 0;
	}
	scheduler->heap_size = 0;
	scheduler->done_count = 0;
	for (int64_t task_id = 1; task_id <= scheduler->maximum_task_id; task_id++) {
		struct scheduler_slot *slot = &scheduler->slots[task_id];
		if (!slot->registered)
			continue;
		slot->state = VINE_DATAVINE_TASK_WAITING;
		slot->remaining_dependencies = slot->initial_dependencies;
		slot->heap_index = -1;
	}
	for (size_t i = 0; i < size / 8; i++) {
		uint64_t task_id = read_u64_le((const unsigned char *)buffer + i * 8);
		if (!valid_task_id(scheduler, (int64_t)task_id) || scheduler->slots[task_id].state == VINE_DATAVINE_TASK_DONE) {
			return 0;
		}
		scheduler->slots[task_id].state = VINE_DATAVINE_TASK_DONE;
		scheduler->done_count++;
	}
	for (int64_t parent = 1; parent <= scheduler->maximum_task_id; parent++) {
		if (!scheduler->slots[parent].registered || scheduler->slots[parent].state != VINE_DATAVINE_TASK_DONE)
			continue;
		for (uint64_t edge = scheduler->slots[parent].first_dependent;
				edge != NO_EDGE;
				edge = scheduler->edges[edge].next) {
			uint64_t child = scheduler->edges[edge].child;
			if (scheduler->slots[child].remaining_dependencies) {
				scheduler->slots[child].remaining_dependencies--;
			}
		}
	}
	/* A recovered output remains authoritative even when an ancestor output
	 * was lost.  Its inputs are irrelevant until that task itself needs to be
	 * replayed, so DONE is intentionally allowed with unresolved parents. */
	for (int64_t task_id = 1; task_id <= scheduler->maximum_task_id; task_id++) {
		struct scheduler_slot *slot = &scheduler->slots[task_id];
		if (slot->registered && slot->state != VINE_DATAVINE_TASK_DONE && !slot->remaining_dependencies) {
			slot->state = VINE_DATAVINE_TASK_READY;
			heap_push(scheduler, (uint64_t)task_id);
		}
	}
	return 1;
}

int vine_datavine_scheduler_complete(struct vine_datavine_scheduler *scheduler)
{
	return scheduler && scheduler->done_count == scheduler->task_count;
}
