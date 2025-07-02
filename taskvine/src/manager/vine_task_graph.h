#ifndef VINE_TASK_GRAPH_H
#define VINE_TASK_GRAPH_H

#include "vine_task.h"
#include "hash_table.h"
#include "vine_manager.h"


// Node structure for the graph
struct vine_task_node {
    char *node_key;          // Unique node key identifier in the graph
    struct vine_task *task;
    int depth;              // Depth of the node in the graph (0 for root nodes)
    struct hash_table *parents;  // Table of node keys this node depends on
    struct hash_table *children;    // Table of node keys that depend on this node
    int completed;                 // Whether this node has been computed
    int prune_depth;               // Prune depth for this node (0 = no pruning, 1+ = prune after depth)
    int prune_blocking_children_remaining;  // Number of downstream nodes within prune_depth that haven't completed
    struct hash_table *reverse_prune_waiters;  // Table of nodes that are waiting for this node to complete (for pruning)
    struct hash_table *pending_parents;  // Table of nodes that are waiting for this node to complete (for pruning)
    
    char *outfile_remote_name;   // Output filename for this node
    struct vine_file *infile;  // arguments, a list of keys to compute
    struct vine_file *outfile; // output file for this task
};

// Graph structure
struct vine_task_graph {
    struct hash_table *nodes;    // Table of nodes indexed by node_key
    struct itable *task_id_to_node; // Table of task ids indexed by node_key
    struct hash_table *outfile_cachename_to_node;
    int prune_algorithm;         // Algorithm used for pruning (vine_task_graph_prune_algorithm_t)
    int static_prune_depth;      // Prune depth for STATIC algorithm
};

struct vine_task_graph *vine_task_graph_create();
void vine_task_graph_delete(struct vine_task_graph *tg);
void vine_task_graph_handle_task_done(struct vine_manager *m, struct vine_task *t);



#endif // VINE_TASK_GRAPH_H
