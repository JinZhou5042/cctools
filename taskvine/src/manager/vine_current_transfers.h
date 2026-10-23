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

/* Count the outgoing transfers of worker w with those of the other workers of its host, once it reported its hostname.
 * The workers of one host share its network link, so a choice of source that avoids a busy link counts their transfers
 * together, to other workers and checkpoints alike. */
void vine_current_transfers_attach_host(struct vine_manager *q, struct vine_worker_info *w);

/* Stop counting the transfers of worker w with its host. Its own records are wiped before. */
void vine_current_transfers_detach_host(struct vine_manager *q, struct vine_worker_info *w);

/* When the oldest transfer to a worker from the host of worker w still in progress started, or zero when it sends none
 * or w has no host yet. The number of those transfers, the sizes of their files, and the number of the host's
 * checkpoints go to sends, bytes, and checkpoints. */
timestamp_t vine_current_transfers_host_load(struct vine_worker_info *w, int *sends, int64_t *bytes, int *checkpoints);

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
