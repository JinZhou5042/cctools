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
    
    char *outfile_remote_name;   // Output filename for this node
    struct vine_file *infile;  // arguments, a list of keys to compute
    struct vine_file *outfile; // output file for this task
};

// Graph structure
struct vine_task_graph {
    struct hash_table *nodes;    // Table of nodes indexed by node_key
    struct itable *task_id_to_node; // Table of task ids indexed by node_key
    struct hash_table *outfile_cachename_to_node;
};

struct vine_task_graph *vine_task_graph_create(struct vine_manager *m);
void vine_task_graph_delete(struct vine_manager *m);
void vine_task_graph_handle_task_done(struct vine_manager *m, struct vine_task *t);

#endif // VINE_TASK_GRAPH_H
