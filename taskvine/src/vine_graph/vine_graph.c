#include <inttypes.h>
#include <stdlib.h>

#include "debug.h"
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
	g->producers = hash_table_create(0, 0);
	return g;
}

uint64_t vine_graph_add_node(struct vine_graph *g)
{
	struct vine_graph_node *node = calloc(1, sizeof(*node));
	if (!node) {
		return 0;
	}
	/* Nodes are never removed, so ids are dense and start from one. */
	node->node_id = itable_size(g->nodes) + 1;
	node->inputs = list_create();
	node->outputs = list_create();
	node->parents = list_create();
	node->children = list_create();
	itable_insert(g->nodes, node->node_id, node);
	return node->node_id;
}

struct vine_graph_node *vine_graph_get_node(const struct vine_graph *g, uint64_t node_id)
{
	return itable_lookup(g->nodes, node_id);
}

uint64_t vine_graph_add_file(struct vine_graph *g, struct vine_file *file)
{
	/* Files are never removed, so ids are dense and start from one. */
	uint64_t file_id = itable_size(g->files) + 1;
	itable_insert(g->files, file_id, file);
	return file_id;
}

struct vine_file *vine_graph_get_file(const struct vine_graph *g, uint64_t file_id)
{
	return itable_lookup(g->files, file_id);
}

int vine_graph_add_mount(struct vine_graph *g, uint64_t node_id, uint64_t file_id, const char *task_path, int is_output)
{
	struct vine_graph_node *node = vine_graph_get_node(g, node_id);
	struct vine_file *file = vine_graph_get_file(g, file_id);
	if (!node || node->submitted || !file) {
		debug(D_ERROR, "mount of file %" PRIu64 " on node %" PRIu64 " names an unknown file or an unknown or submitted node", file_id, node_id);
		return -1;
	}
	if (is_output) {
		if (hash_table_lookup(g->producers, vine_file_cached_name(file))) {
			debug(D_ERROR, "file %" PRIu64 " already has a producer", file_id);
			return -1;
		}
		hash_table_insert(g->producers, vine_file_cached_name(file), node);
	}
	struct vine_graph_mount *mount = xxcalloc(1, sizeof(*mount));
	mount->file = file;
	mount->task_path = xxstrdup(task_path);
	list_push_tail(is_output ? node->outputs : node->inputs, mount);
	return 0;
}

struct vine_graph_node *vine_graph_get_producer(const struct vine_graph *g, struct vine_file *file)
{
	return hash_table_lookup(g->producers, vine_file_cached_name(file));
}

struct vine_graph_mount *vine_graph_get_output(const struct vine_graph *g, uint64_t file_id)
{
	struct vine_file *file = vine_graph_get_file(g, file_id);
	struct vine_graph_node *producer = file ? vine_graph_get_producer(g, file) : NULL;
	struct vine_graph_mount *mount;
	if (producer) {
		LIST_ITERATE(producer->outputs, mount)
		{
			if (mount->file == file) {
				return mount;
			}
		}
	}
	return NULL;
}

int vine_graph_link_node(struct vine_graph *g, uint64_t node_id)
{
	struct vine_graph_node *node = vine_graph_get_node(g, node_id);
	if (!node || node->submitted) {
		debug(D_ERROR, "node %" PRIu64 " is unknown or already submitted", node_id);
		return -1;
	}
	struct vine_graph_mount *mount;
	LIST_ITERATE(node->inputs, mount)
	{
		struct vine_graph_node *parent = vine_graph_get_producer(g, mount->file);
		if (parent && !parent->submitted) {
			debug(D_ERROR, "node %" PRIu64 " reads an output of node %" PRIu64 ", which is not submitted", node_id, parent->node_id);
			return -1;
		}
	}

	/* Link each producer once, however many of its files the node reads. */
	struct set *linked = set_create(0);
	LIST_ITERATE(node->inputs, mount)
	{
		struct vine_graph_node *parent = vine_graph_get_producer(g, mount->file);
		if (!parent || set_lookup(linked, parent)) {
			continue;
		}
		set_insert(linked, parent);
		list_push_tail(node->parents, parent);
		list_push_tail(parent->children, node);
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

static void vine_graph_mounts_delete(struct list *mounts)
{
	struct vine_graph_mount *mount;
	while ((mount = list_pop_head(mounts))) {
		free(mount->task_path);
		free(mount);
	}
	list_delete(mounts);
}

void vine_graph_delete(struct vine_graph *g)
{
	if (!g) {
		return;
	}
	uint64_t node_id;
	struct vine_graph_node *node;
	int iteration;
	ITABLE_ITERATE(g->nodes, iteration, node_id, node)
	{
		vine_task_delete(node->task);
		free(node->error);
		vine_graph_mounts_delete(node->inputs);
		vine_graph_mounts_delete(node->outputs);
		list_delete(node->parents);
		list_delete(node->children);
		free(node);
	}
	itable_delete(g->nodes);
	itable_delete(g->files);
	hash_table_delete(g->producers);
	free(g);
}
