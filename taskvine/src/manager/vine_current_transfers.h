/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_worker_info.h"
#include "uuid.h"

#define VINE_FILE_SOURCE_MAX_TRANSFERS 1
#define VINE_WORKER_SOURCE_MAX_TRANSFERS 10
/* Checkpoints that one worker sends at once, by default. */
#define VINE_WORKER_SOURCE_MAX_CHECKPOINTS 10

/* Record a transfer of the file cachename from source_url (of source_worker, if a worker) to dest_worker, and return
 * its id. */
char *vine_current_transfers_add(struct vine_manager *q, struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url, const char *cachename);

/* Record a checkpoint of the file cachename that source_worker sends to the data service, and return its id. It counts
 * in the worker's outgoing_checkpoints instead of its transfers to other workers. */
char *vine_current_transfers_add_checkpoint(struct vine_manager *q, struct vine_worker_info *source_worker, const char *source_url, const char *cachename);

int vine_current_transfers_remove(struct vine_manager *q, const char *id);

int vine_current_transfers_set_failure(struct vine_manager *q, char *id, const char *cachename);

void vine_current_transfers_set_success(struct vine_manager *q, char *id);

int vine_current_transfers_url_in_use(struct vine_manager *q, const char *source);

int vine_current_transfers_wipe_worker(struct vine_manager *q, struct vine_worker_info *w);

/* Tell every other worker that is receiving a file from w, which is being removed, to abandon the transfer, so it
 * fails at once instead of after the peer transfer timeout. Return the number of transfers abandoned. */
int vine_current_transfers_abandon_source(struct vine_manager *q, struct vine_worker_info *w);

void vine_current_transfers_print_table(struct vine_manager *q);

void vine_current_transfers_clear( struct vine_manager *q );

int vine_current_transfers_get_table_size(struct vine_manager *q);
