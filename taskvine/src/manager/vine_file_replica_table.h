/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


/*
 * Abstract the files posessed by the workers to a single "table"
 */


#ifndef VINE_FILE_REPLICA_TABLE_H
#define VINE_FILE_REPLICA_TABLE_H

#include "taskvine.h"
#include "set.h"
#include "vine_file_replica.h"
#include "vine_worker_info.h"

int vine_file_replica_table_insert(struct vine_manager *m, struct vine_worker_info *w, const char *cachename, struct vine_file_replica *replica);

struct vine_file_replica *vine_file_replica_table_remove(struct vine_manager *m, struct vine_worker_info *w, const char *cachename);

int vine_file_worker_table_remove(struct vine_manager *m, struct vine_worker_info *w, const char *cachename);

struct vine_file_replica *vine_file_replica_table_lookup(struct vine_worker_info *w, const char *cachename);

struct vine_file_replica *vine_file_replica_table_get_or_create(struct vine_manager *m, struct vine_worker_info *w, const char *cachename, vine_file_type_t type, vine_cache_level_t cache_level, int64_t size, time_t mtime);

struct vine_worker_info *vine_file_replica_table_find_worker(struct vine_manager *q, const char *cachename);
/* Return non-zero when some holder of the file can send it now, as vine_file_replica_table_find_worker() would choose
 * one, without choosing: the first such holder answers, so a file that many workers hold costs O(1) to check. */
int vine_file_replica_table_has_source(struct vine_manager *q, const char *cachename);
/* Return non-zero when some holder of the file may send it apart from its current load: it serves transfers, is not
 * suspect or being removed, had no recent transfer failure, and holds a READY replica. */
int vine_file_replica_table_has_holder(struct vine_manager *q, const char *cachename);

int vine_file_replica_table_count_replicas( struct vine_manager *q, const char *cachename, vine_file_replica_state_t state );

#endif