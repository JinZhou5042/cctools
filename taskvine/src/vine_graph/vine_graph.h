#ifndef VINE_GRAPH_H
#define VINE_GRAPH_H

/* Internal to the executor. The DAG of one run in TaskVine terms: each node is one task, and each edge is a file that
 * one node writes and another reads. Dependencies are derived from those files, never declared separately.
 *
 * The graph grows while it runs. Each node is linked when it is submitted, and it may only read outputs of nodes
 * submitted before it, so the graph stays acyclic without a global check. Nothing in the graph depends on nodes that
 * have not been submitted yet.
 *
 * Nodes and files are the graph's stable handles over TaskVine objects. A node outlives the tasks of its attempts, as
 * a file handle refers to a vine_file whose single owner is the Manager's file_table. The graph holds no run
 * configuration. */

#include <stdint.h>

#include "hash_table.h"
#include "itable.h"
#include "list.h"

#include "taskvine.h"

/* A file mounted in a node's task sandbox. */
struct vine_graph_mount {
	struct vine_file *file; // a reference of its own, so undeclaring the file leaves the mount valid
	char *task_path;	// path inside the task sandbox
	int pins;		// on an output, frontend references that keep it from being released
};

/* One node of the graph, executed as one task. */
struct vine_graph_node {
	uint64_t node_id;	// dense id assigned by the graph, starting from one
	struct vine_task *task; // current attempt, or NULL before the first submission

	struct list *inputs;   // mounts the task reads
	struct list *outputs;  // mounts the task writes
	struct list *parents;  // borrowed producers of input files, set by vine_graph_link_node
	struct list *children; // borrowed submitted consumers of output files, which grow as the graph grows

	int submitted;		    // set once the node is linked, after which its mounts are fixed
	int remaining_parents;	    // parents not completed yet
	int depth;		    // longest path from a source, computed by vine_graph_link_node
	int completed;		    // set once the node's own task succeeds
	int released;		    // temporary outputs pruned; cleared when a recovery task restores them
	int restoring;		    // a new task of this completed node is recomputing its outputs
	int failures;		    // failed attempts, bounded by the executor's retry limit
	uint64_t execution_time_us; // latest successful execution time on a Worker

	int failed;				// the node cannot complete, or its outputs cannot be restored
	struct vine_graph_node *failure_source; // borrowed node whose own failure made this one fail
	char *error;				// why the node failed, set only on a failure source
};

/* Nodes, files, and the producer of each output file. */
struct vine_graph {
	struct itable *nodes;	      // node id -> owned node
	struct itable *files;	      // file id -> declared vine_file, borrowed until the executor undeclares it
	uint64_t last_file_id;	      // id given to the latest recorded file
	struct hash_table *producers; // output cache name -> borrowed producer node
};

/* Create an empty graph. Return NULL on allocation failure. */
struct vine_graph *vine_graph_create(void);

/* Add a node and return its id, or zero on failure. */
uint64_t vine_graph_add_node(struct vine_graph *g);

/* Look up a node by id. Return NULL when the id is unknown. */
struct vine_graph_node *vine_graph_get_node(const struct vine_graph *g, uint64_t node_id);

/* Record a declared file and return its id, starting from one. */
uint64_t vine_graph_add_file(struct vine_graph *g, struct vine_file *file);

/* Look up a file by id. Return NULL when the id is unknown. */
struct vine_file *vine_graph_get_file(const struct vine_graph *g, uint64_t file_id);

/* Forget a recorded file and return it, or NULL when the id is unknown. Mounts of the file keep their references. */
struct vine_file *vine_graph_remove_file(struct vine_graph *g, uint64_t file_id);

/* Mount a recorded file at task_path as an input or output of a node that is not submitted yet. A file has at most one
 * producer. Return zero, or -1 for an unknown or submitted node, an unknown file, or a second producer. */
int vine_graph_add_mount(struct vine_graph *g, uint64_t node_id, uint64_t file_id, const char *task_path, int is_output);

/* Return the node that writes a file, or NULL for a file no node writes. */
struct vine_graph_node *vine_graph_get_producer(const struct vine_graph *g, struct vine_file *file);

/* Return the output mount of the file with this id, or NULL for a file no node writes. */
struct vine_graph_mount *vine_graph_get_output(const struct vine_graph *g, uint64_t file_id);

/* Submit a node: link it to the producers of the files it reads, and compute its depth and the number of parents it
 * still waits for. Return zero, or -1 for an unknown or submitted node, or one that reads an output of a node not
 * submitted before it. */
int vine_graph_link_node(struct vine_graph *g, uint64_t node_id);

/* Delete the graph, its nodes, and their tasks. Files are not undeclared. */
void vine_graph_delete(struct vine_graph *g);

#endif // VINE_GRAPH_H
