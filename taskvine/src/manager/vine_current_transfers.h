/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_worker_info.h"
#include "uuid.h"

#define VINE_FILE_SOURCE_MAX_TRANSFERS 1
#define VINE_WORKER_SOURCE_MAX_TRANSFERS 10

/*
Bytes that one source worker sends at once, beyond the transfer of a single file. Transfers that together fit this
bound take about 50 ms of a 10 Gb/s link and 0.5 s of a 1 Gb/s link, so sharing the link among them delays none much,
and up to worker-source-max-transfers small files still overlap the start of each transfer. A larger file is sent one
copy at a time: every finished copy becomes a source of further copies, so a file read on many workers doubles its
copies each time one copy takes to send, while ten copies at once that split the link take ten times as long each.
*/
#define VINE_WORKER_SOURCE_MAX_BYTES (64LL * 1024 * 1024)

/* Record a transfer of the file cachename from source_url (of source_worker, if a worker) to dest_worker, and return
 * its id. A transfer from a source worker counts the size of the worker's replica of cachename among the bytes it
 * sends at once. */
char *vine_current_transfers_add(struct vine_manager *q, struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url, const char *cachename);

int vine_current_transfers_remove(struct vine_manager *q, const char *id);

/* Return true if source worker w can start sending another file of size bytes now: it sends fewer than
 * worker-source-max-transfers files, and it sends nothing else or the bytes it sends, with this file, stay within
 * VINE_WORKER_SOURCE_MAX_BYTES. A size that is not known counts as zero. */
int vine_current_transfers_source_has_room(struct vine_manager *q, struct vine_worker_info *w, int64_t size);

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
