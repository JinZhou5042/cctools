/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_FILE_RECOVERY_SUBGRAPH_H
#define VINE_FILE_RECOVERY_SUBGRAPH_H

#include "list.h"
#include "taskvine.h"
#include "timestamp.h"

/*
This module defines the structure and functions for calculating and managing
recovery subgraphs for files in the TaskVine system.
*/

/* Structure to store recovery subgraph */
struct recovery_subgraph {
    struct list *tasks;          /* List of tasks in the recovery subgraph */
    long critical_path;   /* Length of the critical path (longest chain) in microseconds */
    long total_time;      /* Sum of execution times for all tasks in microseconds */
    struct hash_table *task_execution_times; /* Hash table mapping task_id -> execution_time */
};

/* 
 * Calculate the recovery subgraph for a file - the set of tasks needed to regenerate it.
 * Returns a structure containing the tasks and metrics for the recovery subgraph.
 * If the file is in the PBB worker, returns an empty recovery subgraph.
 */
struct recovery_subgraph *calculate_recovery_subgraph(struct vine_manager *q, const char *cachename);

/* Free the recovery subgraph structure */
void free_recovery_subgraph(struct recovery_subgraph *subgraph);

long get_task_execution_time(struct recovery_subgraph *subgraph, int task_id);

/* Get the size of the recovery subgraph */
int get_recovery_subgraph_size(struct recovery_subgraph *subgraph);

#endif