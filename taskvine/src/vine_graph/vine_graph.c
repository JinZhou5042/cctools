#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include "debug.h"
#include "list.h"
#include "set.h"
#include "vine_graph.h"
#include "xxmalloc.h"

#include "vine_file.h"

struct vine_graph *vine_graph_create(void)
{
	struct vine_graph *g = malloc(sizeof(*g));
	if (!g) {
		return NULL;
	}
	g->nodes = itable_create(0);
	g->files = itable_create(0);
	g->producers = itable_create(0);
	g->last_node_id = 0;
	g->last_file_id = 0;
	return g;
}

/* Append a pointer to an array. The first pointer stays in the array itself, and storage then doubles when full. */
static void vine_graph_array_push(struct vine_graph_array *array, void *item)
{
	if (array->count == array->capacity) {
		if (array->capacity == 0) {
			array->items = &array->first;
			array->capacity = 1;
		} else if (array->items == &array->first) {
			array->items = xxmalloc(sizeof(*array->items) * 2);
			array->items[0] = array->first;
			array->capacity = 2;
		} else {
			array->capacity *= 2;
			array->items = xxrealloc(array->items, sizeof(*array->items) * array->capacity);
		}
	}
	array->items[array->count++] = item;
}

/* Free an array's storage and leave it empty. */
static void vine_graph_array_clear(struct vine_graph_array *array)
{
	if (array->items != &array->first) {
		free(array->items);
	}
	array->items = NULL;
	array->count = array->capacity = 0;
}

uint64_t vine_graph_add_node(struct vine_graph *g)
{
	struct vine_graph_node *node = calloc(1, sizeof(*node));
	if (!node) {
		return 0;
	}
	/* Ids start from one and are never reused, because the frontend may remove nodes. */
	node->node_id = ++g->last_node_id;
	itable_insert(g->nodes, node->node_id, node);
	return node->node_id;
}

struct vine_graph_node *vine_graph_get_node(const struct vine_graph *g, uint64_t node_id)
{
	return itable_lookup(g->nodes, node_id);
}

uint64_t vine_graph_add_file(struct vine_graph *g, struct vine_file *file)
{
	/* Ids start from one and are never reused, because the frontend may undeclare a file it provided. */
	uint64_t file_id = ++g->last_file_id;
	itable_insert(g->files, file_id, file);
	return file_id;
}

struct vine_file *vine_graph_get_file(const struct vine_graph *g, uint64_t file_id)
{
	return itable_lookup(g->files, file_id);
}

struct vine_file *vine_graph_remove_file(struct vine_graph *g, uint64_t file_id)
{
	return itable_remove(g->files, file_id);
}

/* Mount file on a node that is not submitted yet. file_id is zero for an input the node owns. */
static int vine_graph_mount_file(struct vine_graph *g, struct vine_graph_node *node, struct vine_file *file, uint64_t file_id, const char *task_path, int is_output)
{
	if (is_output && itable_lookup(g->producers, (uint64_t)(uintptr_t)file)) {
		debug(D_ERROR, "file %" PRIu64 " already has a producer", file_id);
		return -1;
	}
	size_t length = strlen(task_path) + 1;
	struct vine_graph_mount *mount = xxcalloc(1, sizeof(*mount) + length);
	mount->file = vine_file_addref(file);
	mount->file_id = file_id;
	memcpy(mount->task_path, task_path, length);
	mount->node = node;
	if (is_output) {
		itable_insert(g->producers, (uint64_t)(uintptr_t)file, mount);
	}
	vine_graph_array_push(is_output ? &node->outputs : &node->inputs, mount);
	return 0;
}

int vine_graph_add_mount(struct vine_graph *g, uint64_t node_id, uint64_t file_id, const char *task_path, int is_output)
{
	struct vine_graph_node *node = vine_graph_get_node(g, node_id);
	struct vine_file *file = vine_graph_get_file(g, file_id);
	if (!node || node->submitted || !file) {
		debug(D_ERROR, "mount of file %" PRIu64 " on node %" PRIu64 " names an unknown file or an unknown or submitted node", file_id, node_id);
		return -1;
	}
	return vine_graph_mount_file(g, node, file, file_id, task_path, is_output);
}

int vine_graph_add_owned_input(struct vine_graph *g, uint64_t node_id, struct vine_file *file, const char *task_path)
{
	struct vine_graph_node *node = vine_graph_get_node(g, node_id);
	if (!node || node->submitted || !file) {
		debug(D_ERROR, "an owned input of node %" PRIu64 " names an unknown or submitted node", node_id);
		return -1;
	}
	return vine_graph_mount_file(g, node, file, 0, task_path, 0);
}

struct vine_graph_mount *vine_graph_find_output(const struct vine_graph *g, struct vine_file *file)
{
	return itable_lookup(g->producers, (uint64_t)(uintptr_t)file);
}

struct vine_graph_node *vine_graph_get_producer(const struct vine_graph *g, struct vine_file *file)
{
	struct vine_graph_mount *mount = vine_graph_find_output(g, file);
	return mount ? mount->node : NULL;
}

struct vine_graph_mount *vine_graph_get_output(const struct vine_graph *g, uint64_t file_id)
{
	struct vine_file *file = vine_graph_get_file(g, file_id);
	return file ? vine_graph_find_output(g, file) : NULL;
}

int vine_graph_link_node(struct vine_graph *g, uint64_t node_id)
{
	struct vine_graph_node *node = vine_graph_get_node(g, node_id);
	if (!node || node->submitted) {
		debug(D_ERROR, "node %" PRIu64 " is unknown or already submitted", node_id);
		return -1;
	}
	/* Collect each producer once, however many of its files the node reads, with one lookup per mount. A node that
	 * reads an output of a node not submitted is left as it was. */
	struct set *linked = set_create(node->inputs.count);
	for (int i = 0; i < node->inputs.count; i++) {
		struct vine_graph_mount *mount = node->inputs.items[i];
		struct vine_graph_node *parent = vine_graph_get_producer(g, mount->file);
		if (!parent || set_lookup(linked, parent)) {
			continue;
		}
		if (!parent->submitted) {
			debug(D_ERROR, "node %" PRIu64 " reads an output of node %" PRIu64 ", which is not submitted", node_id, parent->node_id);
			set_delete(linked);
			vine_graph_array_clear(&node->parents);
			return -1;
		}
		set_insert(linked, parent);
		vine_graph_array_push(&node->parents, parent);
	}
	for (int i = 0; i < node->parents.count; i++) {
		struct vine_graph_node *parent = node->parents.items[i];
		vine_graph_array_push(&parent->children, node);
		parent->live_children++;
		node->refs++;
		if (node->depth < parent->depth + 1) {
			node->depth = parent->depth + 1;
		}
		if (!parent->completed) {
			node->remaining_parents++;
		}
	}
	set_delete(linked);
	node->submitted = 1;
	return 0;
}

static void vine_graph_mounts_delete(struct vine_graph_array *mounts)
{
	for (int i = 0; i < mounts->count; i++) {
		struct vine_graph_mount *mount = mounts->items[i];
		vine_file_delete(mount->file);
		free(mount);
	}
	vine_graph_array_clear(mounts);
}

void vine_graph_node_unref(struct vine_graph_node *node)
{
	if (--node->refs == 0 && node->removed) {
		free(node);
	}
}

/* Drop the entries of a node's children array that point to removed children. Entries of children that remain point
 * to structures that the graph still owns. */
static void vine_graph_node_drop_removed_children(struct vine_graph_node *node)
{
	for (int i = 0; i < node->children.count; i++) {
		struct vine_graph_node *child = node->children.items[i];
		if (child->removed) {
			vine_graph_node_unref(child);
		}
	}
	vine_graph_array_clear(&node->children);
}

/* Free what a node owns. Its children array must hold no entry for a removed child anymore. */
static void vine_graph_node_clear(struct vine_graph_node *node)
{
	vine_task_delete(node->task);
	node->task = NULL;
	free(node->error);
	node->error = NULL;
	vine_graph_mounts_delete(&node->inputs);
	vine_graph_mounts_delete(&node->outputs);
	vine_graph_array_clear(&node->children);
	vine_graph_array_clear(&node->parents);
}

void vine_graph_remove_node(struct vine_graph *g, struct vine_graph_node *node)
{
	for (int i = 0; i < node->outputs.count; i++) {
		struct vine_graph_mount *mount = node->outputs.items[i];
		itable_remove(g->producers, (uint64_t)(uintptr_t)mount->file);
	}
	itable_remove(g->nodes, node->node_id);
	/* Every child was removed before the node, so every entry of its children array points to a removed child. */
	vine_graph_node_drop_removed_children(node);
	vine_graph_node_clear(node);
	node->removed = 1;
	if (node->refs == 0) {
		free(node);
	}
}

void vine_graph_delete(struct vine_graph *g)
{
	if (!g) {
		return;
	}
	/* Removed nodes are freed while every node that remains still exists, and then the nodes that remain. */
	struct list *nodes = list_create();
	uint64_t node_id;
	struct vine_graph_node *node;
	int iteration;
	ITABLE_ITERATE(g->nodes, iteration, node_id, node)
	{
		list_push_tail(nodes, node);
		vine_graph_node_drop_removed_children(node);
	}
	while ((node = list_pop_head(nodes))) {
		vine_graph_node_clear(node);
		free(node);
	}
	list_delete(nodes);
	itable_delete(g->nodes);
	itable_delete(g->files);
	itable_delete(g->producers);
	free(g);
}
