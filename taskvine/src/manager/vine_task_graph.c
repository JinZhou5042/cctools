#include "vine_task_graph.h"
#include "taskvine.h"
#include "vine_manager.h"
#include <stdlib.h>
#include <string.h>
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "hash_table.h"
#include "list.h"
#include "vine_task.h"
#include "timestamp.h"
#include "vine_mount.h"
#include "random.h"
#include "assert.h"
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <stdatomic.h>

static volatile sig_atomic_t interrupted = 0;

static void handle_sigint(int signal)
{
    interrupted = 1;
}

// Public API to update depths of all nodes in the graph
static void vine_task_graph_update_depths(struct vine_manager *m)
{
    if (!m->task_graph) {
        return;
    }

    // First pass: set all nodes to depth -1 (unvisited)
    char *node_key;
    struct vine_task_node *node;
    hash_table_firstkey(m->task_graph->nodes);
    while (hash_table_nextkey(m->task_graph->nodes, &node_key, (void **)&node)) {
        node->depth = -1;
    }

    // Second pass: find root nodes (nodes with no dependencies)
    hash_table_firstkey(m->task_graph->nodes);
    while (hash_table_nextkey(m->task_graph->nodes, &node_key, (void **)&node)) {
        if (hash_table_size(node->dependencies) == 0) {
            node->depth = 0;
        }
    }

    // Third pass: propagate depths through the graph
    int changed;
    do {
        changed = 0;
        hash_table_firstkey(m->task_graph->nodes);
        while (hash_table_nextkey(m->task_graph->nodes, &node_key, (void **)&node)) {
            if (node->depth == -1) {
                // Find the maximum depth of all dependencies
                int max_dep_depth = -1;

                char *dep_key;
                struct vine_task_node *dep_node;
                hash_table_firstkey(node->dependencies);
                while (hash_table_nextkey(node->dependencies, &dep_key, (void **)&dep_node)) {
                    if (dep_node->depth > max_dep_depth) {
                        max_dep_depth = dep_node->depth;
                    }
                }

                // If all dependencies have valid depths, set this node's depth
                if (max_dep_depth != -1) {
                    node->depth = max_dep_depth + 1;
                    changed = 1;
                }
            }
        }
    } while (changed);

    // Check for cycles (nodes that still have depth -1)
    hash_table_firstkey(m->task_graph->nodes);
    while (hash_table_nextkey(m->task_graph->nodes, &node_key, (void **)&node)) {
        if (node->depth == -1) {
            debug(D_DEBUG, "Warning: Node %s is part of a cycle in the graph", node_key);
        }
    }
}

struct vine_task_graph *vine_task_graph_create(struct vine_manager *m)
{
    struct vine_task_graph *graph = xxmalloc(sizeof(struct vine_task_graph));
    graph->nodes = hash_table_create(0, 0);
    graph->task_id_to_node = itable_create(0);
    graph->outfile_cachename_to_node = hash_table_create(0, 0);
    return graph;
}

void vine_task_graph_delete(struct vine_manager *m)
{
    if (!m->task_graph) {
        return;
    }

    // First collect all node keys
    char **node_keys = NULL;
    int node_count = 0;
    int node_capacity = 0;

    char *node_key;
    struct vine_task_node *node;

    hash_table_firstkey(m->task_graph->nodes);
    while (hash_table_nextkey(m->task_graph->nodes, &node_key, (void **)&node)) {
        if (node_count >= node_capacity) {
            node_capacity = node_capacity ? node_capacity * 2 : 16;
            node_keys = xxrealloc(node_keys, node_capacity * sizeof(char *));
        }
        node_keys[node_count++] = xxstrdup(node_key);
    }

    // Now remove all nodes
    for (int i = 0; i < node_count; i++) {
        vine_task_graph_remove_node(m, node_keys[i]);
        free(node_keys[i]);
    }

    free(node_keys);
    hash_table_delete(m->task_graph->nodes);
    itable_delete(m->task_graph->task_id_to_node);
    hash_table_delete(m->task_graph->outfile_cachename_to_node);
    free(m->task_graph);
}

// Initialize compute-related fields in get_or_create_node
static struct vine_task_node *get_or_create_node(struct vine_task_graph *graph, const char *node_key)
{
    if (!graph || !node_key)
        return NULL;

    struct vine_task_node *node = hash_table_lookup(graph->nodes, node_key);
    if (!node) {
        node = xxmalloc(sizeof(struct vine_task_node));
        node->node_key = xxstrdup(node_key);
        char buf[33];
        random_hex(buf, sizeof(buf));
        node->outfile_remote_name = xxstrdup(buf);
        node->depth = -1;
        node->dependencies = hash_table_create(0, 0);
        node->dependents = hash_table_create(0, 0);
        node->num_pending_dependencies = 0;
        node->num_pending_dependents = 0;
        node->completed = 0;
        hash_table_insert(graph->nodes, node_key, node);
    }
    return node;
}

void vine_task_graph_set_node_outfile_remote_name(struct vine_manager *m, const char *node_key, const char *outfile_remote_name)
{
    if (!m->task_graph || !node_key || !outfile_remote_name) {
        return;
    }
    struct vine_task_node *node = hash_table_lookup(m->task_graph->nodes, node_key);
    if (!node) {
        return;
    }
    if (node->outfile_remote_name) {
        free(node->outfile_remote_name);
    }
    node->outfile_remote_name = xxstrdup(outfile_remote_name);
}

static char **get_topological_order(struct vine_task_graph *graph, int *node_count)
{
    if (!graph || !node_count)
        return NULL;

    int total_nodes = hash_table_size(graph->nodes);
    *node_count = 0;

    char **topo_order = xxmalloc(total_nodes * sizeof(char *));
    int *in_degree = xxmalloc(total_nodes * sizeof(int));
    char **node_keys = xxmalloc(total_nodes * sizeof(char *));
    struct hash_table *key_to_index = hash_table_create(0, 0);

    // Build node_keys and in_degree
    struct vine_task_node *node;
    char *node_key;
    int idx = 0;

    hash_table_firstkey(graph->nodes);
    while (hash_table_nextkey(graph->nodes, &node_key, (void **)&node)) {
        node_keys[idx] = xxstrdup(node_key);
        in_degree[idx] = hash_table_size(node->dependencies);
        hash_table_insert(key_to_index, node_keys[idx], (void *)(long)idx);
        idx++;
    }

    int front = 0, back = 0;
    int *queue = xxmalloc(total_nodes * sizeof(int));

    for (int i = 0; i < total_nodes; i++) {
        if (in_degree[i] == 0) {
            queue[back++] = i;
        }
    }

    while (front < back) {
        int i = queue[front++];
        topo_order[(*node_count)++] = xxstrdup(node_keys[i]);

        struct vine_task_node *curr_node = hash_table_lookup(graph->nodes, node_keys[i]);
        char *child_key;
        struct vine_task_node *child;

        HASH_TABLE_ITERATE(curr_node->dependents, child_key, child) {
            intptr_t jval = (intptr_t)hash_table_lookup(key_to_index, child_key);
            int j = (int)jval;
            in_degree[j]--;
            if (in_degree[j] == 0) {
                queue[back++] = j;
            }
        }
    }

    // Clean up on cycle
    if (*node_count < total_nodes) {
        debug(D_NOTICE, "Cycle detected in task graph");
        for (int i = 0; i < total_nodes; i++) free(node_keys[i]);
        free(node_keys); free(in_degree); free(queue); hash_table_delete(key_to_index);
        for (int i = 0; i < *node_count; i++) free(topo_order[i]);
        free(topo_order);
        *node_count = 0;
        return NULL;
    }

    for (int i = 0; i < total_nodes; i++) free(node_keys[i]);
    free(node_keys); free(in_degree); free(queue); hash_table_delete(key_to_index);

    return topo_order;
}


void vine_task_graph_finalize(struct vine_manager *m, char *library_name, char *function_name)
{
    if (!m->task_graph || !m) {
        return;
    }

    // First update depths of all nodes
    vine_task_graph_update_depths(m);

    // Get nodes in topological order
    int node_count;
    char **topo_order = get_topological_order(m->task_graph, &node_count);
    if (!topo_order) {
        return;
    }

    // Process nodes in topological order
    for (int i = 0; i < node_count; i++) {
        struct vine_task_node *node = hash_table_lookup(m->task_graph->nodes, topo_order[i]);
        // create task for this node
        node->task = vine_task_create(function_name);
        vine_task_set_priority(node->task, node->depth);
        vine_task_set_library_required(node->task, library_name);
        // create infile for this task, which is the node key
        char *infile_content = string_format("%s", node->node_key);
        node->infile = vine_declare_buffer(m, infile_content, strlen(infile_content), VINE_CACHE_LEVEL_TASK, VINE_UNLINK_WHEN_DONE);
        if (!node->infile) {
            debug(D_NOTICE | D_VINE, "%s: failed to create infile for node %s", __func__, topo_order[i]);
        }
        vine_task_add_input(node->task, node->infile, "infile", VINE_TRANSFER_ALWAYS);
        if (!node->infile) {
            debug(D_NOTICE | D_VINE, "%s: failed to add infile for node %s", __func__, topo_order[i]);
        }
        // Create output file for this task
        node->outfile = vine_declare_temp(m);
        hash_table_insert(m->task_graph->outfile_cachename_to_node, node->outfile->cached_name, node);
        vine_task_add_output(node->task, node->outfile, node->outfile_remote_name, VINE_TRANSFER_ALWAYS);

        // Add inputs from parent nodes
        char *parent_key;
        struct vine_task_node *parent_node;
        HASH_TABLE_ITERATE(node->dependencies, parent_key, parent_node)
        {
            if (!parent_node->outfile) {
                debug(D_NOTICE | D_VINE, "%s: failed to add output file for node %s", __func__, topo_order[i]);
            }
            vine_task_add_input(node->task, parent_node->outfile, parent_node->outfile_remote_name, VINE_TRANSFER_ALWAYS);
            if (!node->infile) {
                debug(D_NOTICE | D_VINE, "%s: failed to add infile for node %s", __func__, topo_order[i]);
            }
        }
        // free(infile_content);
    }

    // Clean up topo_order
    for (int i = 0; i < node_count; i++) {
        free(topo_order[i]);
    }
    free(topo_order);

    return;
}

int vine_task_graph_is_file_prunable(struct vine_manager *m, struct vine_file *f)
{
    if (!m->task_graph || !f) {
        return 0;
    }
    struct vine_task_node *node = hash_table_lookup(m->task_graph->outfile_cachename_to_node, f->cached_name);
    if (!node) {
        return 0;
    }
    
    // a file is prunable if its outfile is not needed by any child node
    // 1. it has no pending dependents
    // 2. and, all completed dependents, for their outfiles, their recovery tasks are also completed
    if (node->num_pending_dependents > 0) {
        return 0;
    }
    char *parent_key;
    struct vine_task_node *child_node;
    HASH_TABLE_ITERATE(node->dependents, parent_key, child_node)
    {
        // if a task is not deleted, it means it is still running
        if (child_node->task && child_node->task->state != VINE_TASK_DONE) {
            return 0;
        }
        struct vine_task *child_node_recovery_task = child_node->outfile->recovery_task;
        if (child_node_recovery_task && (child_node_recovery_task->state != VINE_TASK_INITIAL && child_node_recovery_task->state != VINE_TASK_DONE)) {
            return 0;
        }
    }

    return 1;
}

struct vine_task_node *vine_task_graph_get_node_by_outfile_cachename(struct vine_manager *m, const char *cachename)
{
    if (!m->task_graph || !cachename) {
        return NULL;
    }
    return hash_table_lookup(m->task_graph->outfile_cachename_to_node, cachename);
}

struct vine_task_node *vine_task_graph_get_node_by_task_id(struct vine_manager *m, int task_id)
{
    if (!m->task_graph || task_id < 0) {
        return NULL;
    }
    return itable_lookup(m->task_graph->task_id_to_node, task_id);
}

int vine_task_graph_execute(struct vine_manager *m)
{
    if (!m || !m->task_graph)
        return -1;

    // Enable debug system for C code since it uses a separate debug system instance
    // from the Python bindings. Use the same function that the manager uses.
	char *debug_tmp = string_format("%s/vine-logs/debug", m->runtime_directory);
	vine_enable_debug_log(debug_tmp);
	free(debug_tmp);

    signal(SIGINT, handle_sigint);
    
    // first enqueue those without dependencies
    char *node_key;
    struct vine_task_node *node;
    int initial_tasks = 0;
    HASH_TABLE_ITERATE(m->task_graph->nodes, node_key, node)
    {
        if (node->num_pending_dependencies == 0) {
            int task_id = vine_submit(m, node->task);
            itable_insert(m->task_graph->task_id_to_node, task_id, node);
            initial_tasks++;
        }
    }

    int num_all_tasks = hash_table_size(m->task_graph->nodes);
    int num_completed_tasks = 0;
    timestamp_t start_time = timestamp_get();

    while (num_completed_tasks < num_all_tasks) {
        if (interrupted) {
            break;
        }   

        struct vine_task *task = vine_wait(m, 5);
        if (task) {
            struct vine_task_node *node = itable_lookup(m->task_graph->task_id_to_node, task->task_id);
            num_completed_tasks++;

            if (num_completed_tasks % 200 == 0) {
                timestamp_t end_time = timestamp_get();
                double duration = (end_time - start_time) / 1e6;
                printf("completed task count: %d, total time: %fs\n", num_completed_tasks, duration);
            }

            // find the children nodes of this node to see if they are ready to run
            char *child_key;
            struct vine_task_node *child_node;
            HASH_TABLE_ITERATE(node->dependents, child_key, child_node)
            {
                if (--child_node->num_pending_dependencies == 0) {
                    int task_id = vine_submit(m, child_node->task);
                    itable_insert(m->task_graph->task_id_to_node, task_id, child_node);
                }
            }

            // prune stale files
            char *parent_key;
            struct vine_task_node *parent_node;
            HASH_TABLE_ITERATE(node->dependencies, parent_key, parent_node)
            {
                if (--parent_node->num_pending_dependents == 0) {
                    if (vine_task_graph_is_file_prunable(m, parent_node->outfile)) {
                        vine_prune_file(m, parent_node->outfile);
                    }
                }
            }
        }
    }

    return 0;
}

// Update dependency counts when adding a dependency
int vine_task_graph_add_dependency(struct vine_manager *m, const char *dependent_key, const char *dependency_key)
{
    if (!m->task_graph || !dependent_key || !dependency_key)
        return -1;

    struct vine_task_node *dependent = get_or_create_node(m->task_graph, dependent_key);
    struct vine_task_node *dependency = get_or_create_node(m->task_graph, dependency_key);

    if (!dependent || !dependency)
        return -1;

    // Add the dependency
    hash_table_insert(dependent->dependencies, dependency_key, dependency);
    hash_table_insert(dependency->dependents, dependent_key, dependent);

    // Update pending counts
    dependent->num_pending_dependencies++;
    dependency->num_pending_dependents++;

    return 0;
}

void vine_task_graph_remove_node(struct vine_manager *m, const char *node_key)
{
    if (!m->task_graph || !node_key)
        return;

    struct vine_task_node *node = hash_table_lookup(m->task_graph->nodes, node_key);
    if (!node)
        return;

    if (node->node_key) {
        free(node->node_key);
    }
    if (node->outfile_remote_name) {
        free(node->outfile_remote_name);
    }

    // First collect all dependency and dependent keys
    char **dep_keys = NULL;
    char **depent_keys = NULL;
    int dep_count = 0;
    int depent_count = 0;
    int dep_capacity = 0;
    int depent_capacity = 0;

    // Collect dependency keys
    char *dep_key;
    struct vine_task_node *dep_node;
    hash_table_firstkey(node->dependencies);
    while (hash_table_nextkey(node->dependencies, &dep_key, (void **)&dep_node)) {
        if (dep_count >= dep_capacity) {
            dep_capacity = dep_capacity ? dep_capacity * 2 : 16;
            dep_keys = xxrealloc(dep_keys, dep_capacity * sizeof(char *));
        }
        dep_keys[dep_count++] = xxstrdup(dep_key);
    }

    // Collect dependent keys
    hash_table_firstkey(node->dependents);
    while (hash_table_nextkey(node->dependents, &dep_key, (void **)&dep_node)) {
        if (depent_count >= depent_capacity) {
            depent_capacity = depent_capacity ? depent_capacity * 2 : 16;
            depent_keys = xxrealloc(depent_keys, depent_capacity * sizeof(char *));
        }
        depent_keys[depent_count++] = xxstrdup(dep_key);
    }

    // Now remove this node from all its dependencies' dependents lists
    for (int i = 0; i < dep_count; i++) {
        struct vine_task_node *dep_node = hash_table_lookup(m->task_graph->nodes, dep_keys[i]);
        if (dep_node) {
            if (hash_table_remove(dep_node->dependents, node_key) == 0) {
                debug(D_DEBUG, "Failed to remove node %s from dependency %s's dependents", node_key, dep_keys[i]);
            }
        }
        free(dep_keys[i]);
    }

    // Remove this node from all its dependents' dependencies lists
    for (int i = 0; i < depent_count; i++) {
        struct vine_task_node *depent_node = hash_table_lookup(m->task_graph->nodes, depent_keys[i]);
        if (depent_node) {
            if (hash_table_remove(depent_node->dependencies, node_key) == 0) {
                debug(D_DEBUG, "Failed to remove node %s from dependent %s's dependencies", node_key, depent_keys[i]);
            }
        }
        free(depent_keys[i]);
    }

    // Free the collected arrays
    free(dep_keys);
    free(depent_keys);

    // Clean up the node's own tables
    hash_table_delete(node->dependencies);
    hash_table_delete(node->dependents);

    // Remove the node from the graph
    if (hash_table_remove(m->task_graph->nodes, node_key) == 0) {
        debug(D_DEBUG, "Failed to remove node %s from graph", node_key);
    }
    free(node);
}

void vine_task_graph_prune_when_task_done(struct vine_manager *m, struct vine_task *t)
{
    if (!m->task_graph || !t) {
        return;
    }

    struct vine_task_node *node = vine_task_graph_get_node_by_task_id(m, t->task_id);
    if (!node) {
        return;
    }

    char *parent_key;
    struct vine_task_node *parent_node;
    HASH_TABLE_ITERATE(node->dependencies, parent_key, parent_node)
    {
        if (vine_task_graph_is_file_prunable(m, parent_node->outfile)) {
            vine_prune_file(m, parent_node->outfile);
        }
    }
}