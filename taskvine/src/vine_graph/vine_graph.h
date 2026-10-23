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
 * configuration.
 *
 * A node the frontend no longer needs is removed once every child was removed, so no node that remains reads an
 * output of a removed one. Its parents still list it until they are removed too, so the node's structure stays, marked
 * as removed, until no entry refers to it. Ids are never reused. */

#include <stdint.h>

#include "itable.h"

#include "taskvine.h"

/* A file mounted in a node's task sandbox, allocated together with its task path. */
struct vine_graph_mount {
	struct vine_file *file;	      // a reference of its own, so undeclaring the file leaves the mount valid
	uint64_t file_id;	      // the graph's id of the file, or zero for an input the node owns
	struct vine_graph_node *node; // borrowed node that mounts the file
	int pins;		      // on an output, frontend references that keep it from being released
	int local;		      // on an output, a vault copy exists and is counted in the node's local_outputs
	char task_path[];	      // path inside the task sandbox
};

/* Pointers that a node keeps in the order they were added: its mounts, parents, or children. A dttools list cost a
 * header, a cursor, and 64 bytes for each entry, a large part of what a node held while the frontend kept it. Most
 * arrays hold one pointer, such as the output of a node or its manifest, which stays in the array itself, so such an
 * array costs no allocation to fill or to free. The array must therefore stay where it is while it holds pointers. */
struct vine_graph_array {
	void **items; // the pointers: NULL while empty, &first while one fits there, else an owned allocation
	int count;    // pointers in use
	int capacity; // pointers that items can hold
	void *first;  // storage of the only pointer while capacity is one
};

/* One node of the graph, executed as one task. */
struct vine_graph_node {
	uint64_t node_id;	// dense id assigned by the graph, starting from one
	struct vine_task *task; // latest attempt until the Manager returns it, then NULL
	int task_id;		// Manager id of the latest attempt, or zero before the first submission

	struct vine_graph_array inputs;	  // owned mounts the task reads
	struct vine_graph_array outputs;  // owned mounts the task writes
	struct vine_graph_array parents;  // borrowed producers of input files, set by vine_graph_link_node
	struct vine_graph_array children; // borrowed submitted consumers of output files, which grow as the graph grows

	unsigned submitted : 1; // set once the node is linked, after which its mounts are fixed
	unsigned completed : 1; // set once the node's own task succeeds
	unsigned released : 1;	// temporary outputs pruned; cleared when a recovery task restores them
	unsigned restoring : 1; // a new task of this completed node is recomputing its outputs
	unsigned failed : 1;	// the node cannot complete, or its outputs cannot be restored
	unsigned let_go : 1;	// the frontend no longer needs the node, which is removed once every child was removed
	unsigned kept : 1;	// ran again after its release, and kept until the frontend lets it go (never released meanwhile)
	unsigned removed : 1;	// removed from the graph, so only the entries counted in refs still hold the structure
	unsigned reading : 1;	// submitted and not finished, so counted in each parent's readers

	int remaining_parents;	    // parents not completed yet
	int depth;		    // longest path from a source, computed by vine_graph_link_node
	int failures;		    // failed attempts, bounded by the executor's retry limit
	int pinned_outputs;	    // outputs with pins, so a release check costs O(1) instead of a pass over outputs
	int local_outputs;	    // outputs with a vault copy, so the anchor check costs O(1) instead of a pass over outputs
	int durable_children;	    // executor's release check: every child before this index was durable when checked
	int exposed;		    // outputs the executor counts as exposed: completed, neither anchored nor released
	int readers;		    // submitted children that have not finished, before which large checkpoints wait
	uint64_t execution_time_us; // latest successful execution time on a Worker

	uint64_t failure_source_id; // the node whose own failure made this one fail
	char *error;		    // why the node failed, set only on a failure source
	int failure;		    // a vine_graph_failure_t: the kind of failure, set only on a failure source

	int live_children; // children not removed yet
	int refs;	   // entries that point to the node in its parents' children arrays and in the executor's lists
};

/* Nodes, files, and the producer of each output file. */
struct vine_graph {
	struct itable *nodes;	  // node id -> owned node that was not removed
	uint64_t last_node_id;	  // id given to the latest node
	struct itable *files;	  // file id -> declared vine_file, borrowed until the executor undeclares it
	uint64_t last_file_id;	  // id given to the latest recorded file
	struct itable *producers; // address of an output's vine_file -> borrowed output mount, whose node is the producer
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

/* Mount a declared file that the graph does not record at task_path as an input of a node that is not submitted yet.
 * The node owns the declaration, which the executor undeclares when it removes the node, so the file needs no id. The
 * mount takes a reference of its own. Return zero, or -1 for an unknown or submitted node. */
int vine_graph_add_owned_input(struct vine_graph *g, uint64_t node_id, struct vine_file *file, const char *task_path);

/* Return the node that writes a file, or NULL for a file no node writes. An output is found by the address of its
 * vine_file, the one that the graph records for it. */
struct vine_graph_node *vine_graph_get_producer(const struct vine_graph *g, struct vine_file *file);

/* Return the output mount of the file with this id, or NULL for a file no node writes. */
struct vine_graph_mount *vine_graph_get_output(const struct vine_graph *g, uint64_t file_id);

/* Return the output mount of a file, or NULL for a file no node writes. */
struct vine_graph_mount *vine_graph_find_output(const struct vine_graph *g, struct vine_file *file);

/* Submit a node: link it to the producers of the files it reads, and compute its depth and the number of parents it
 * still waits for. Return zero, or -1 for an unknown or submitted node, or one that reads an output of a node not
 * submitted before it. */
int vine_graph_link_node(struct vine_graph *g, uint64_t node_id);

/* Remove a node whose children were all removed: delete its task and mounts, forget it as a producer, and leave its
 * parents' children arrays pointing at a structure marked as removed. The caller undeclares the node's outputs and
 * updates its parents first. */
void vine_graph_remove_node(struct vine_graph *g, struct vine_graph_node *node);

/* Drop one entry that points to a node, and free the structure of a removed node that no entry points to anymore. */
void vine_graph_node_unref(struct vine_graph_node *node);

/* Delete the graph, its nodes, and their tasks. Files are not undeclared. The executor drops its own entries first. */
void vine_graph_delete(struct vine_graph *g);

#endif // VINE_GRAPH_H
