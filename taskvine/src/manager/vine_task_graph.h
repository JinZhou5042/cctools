#ifndef VINE_TASK_GRAPH_H
#define VINE_TASK_GRAPH_H

#include "vine_task.h"
#include "hash_table.h"
#include "vine_manager.h"
#include "set.h"

// Node structure for the graph
struct vine_task_node {
    char *node_key;          // Unique node key identifier in the graph
    struct vine_task *task;
    int depth;              // Depth of the node in the graph (0 for root nodes)
    struct hash_table *parents;  // Table of node keys this node depends on
    struct hash_table *children;    // Table of node keys that depend on this node
    int completed;                 // Whether this node has been computed
    int prune_blocking_children_remaining;  // Number of downstream nodes within prune_depth that haven't completed
    struct hash_table *reverse_prune_waiters;  // Table of nodes that are waiting for this node to complete (for pruning)
    struct set *pending_parents;  // Set of parent keys that this node is waiting for
    
    char *outfile_remote_name;   // Output filename for this node
    struct vine_file *infile;  // arguments, a list of keys to compute
    struct vine_file *outfile; // output file for this task
    
    timestamp_t critical_time;
    
    int needs_checkpointing;
    int needs_replication;
    int needs_persistency;

    int active;
};

// Graph structure
struct vine_task_graph {
	struct hash_table *nodes;
	struct itable *task_id_to_node;
	struct hash_table *outfile_cachename_to_node;
	int nls_prune_depth;
    struct vine_manager *manager;
	vine_task_priority_mode_t priority_mode;

    timestamp_t time_spent_on_file_pruning;

    char *library_name;
    char *function_name;
};

struct set *handle_checkpoint_worker_stagein(struct vine_task_graph *tg, struct vine_worker_info *w, const char *cachename);


#endif // VINE_TASK_GRAPH_H
