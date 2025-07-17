#ifndef VINE_TASK_GRAPH_H
#define VINE_TASK_GRAPH_H

#include "vine_task.h"
#include "hash_table.h"
#include "list.h"
#include "vine_manager.h"
#include "set.h"

struct vine_task_node {
    char *node_key;          // Unique node key identifier in the graph
    struct vine_task *task;
    int depth;              // Depth of the node in the graph (0 for root nodes)
    struct list *parents;
    struct list *children;
    int completed;                 // Whether this node has been computed
    int prune_blocking_children_remaining;  // Number of downstream nodes within prune_depth that haven't completed
    struct list *reverse_prune_waiters;  // List of nodes that are waiting for this node to complete (for pruning)
    struct set *pending_parents;  // Set of parent keys that this node is waiting for
    
    char *outfile_remote_name;   // Output filename for this node
    struct vine_file *infile;  // arguments, a list of keys to compute
    struct vine_file *outfile; // output file for this task

    timestamp_t critical_time;
    
    vine_task_graph_node_output_store_location_t output_store_location;

    int active;
};

typedef enum {
	PRUNE_ALGORITHM_PRE_INITIALIZING = 0,
    PRUNE_ALGORITHM_REAL_TIME,
} prune_algorithm_t;

struct vine_task_graph {
	struct hash_table *nodes;
	struct hash_table *outfile_cachename_to_node;
	struct itable *task_id_to_node;
	int nls_prune_depth;
    struct vine_manager *manager;
	vine_task_priority_mode_t priority_mode;
    timestamp_t time_spent_on_file_pruning;
    char *staging_dir;
    prune_algorithm_t prune_algorithm;

    char *library_name;
    char *function_name;
};


#endif // VINE_TASK_GRAPH_H
