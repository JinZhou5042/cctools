/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_worker_info.h"
#include "uuid.h"

#define VINE_FILE_SOURCE_MAX_TRANSFERS 1
#define VINE_WORKER_SOURCE_MAX_TRANSFERS 10

char *vine_current_transfers_add(struct vine_manager *q, struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url, struct vine_file *f);

int vine_current_transfers_remove(struct vine_manager *q, const char *id);

int vine_current_transfers_set_failure(struct vine_manager *q, char *id, const char *cachename);

void vine_current_transfers_set_success(struct vine_manager *q, char *id);

int vine_current_transfers_url_in_use(struct vine_manager *q, const char *source);

int vine_current_transfers_wipe_worker(struct vine_manager *q, struct vine_worker_info *w);

void vine_current_transfers_print_table(struct vine_manager *q);

void vine_current_transfers_clear( struct vine_manager *q );

int vine_current_transfers_get_table_size(struct vine_manager *q);

int vine_current_transfers_retry_releases(struct vine_manager *q, int limit);

/* True only for an active worker-to-worker transfer whose lifetime is
 * protected by a Data Controller lease. */
int vine_current_transfers_is_datavine_peer(struct vine_manager *q, const char *id);

/* Validate that the reporting worker is the destination bound to the lease. */
int vine_current_transfers_is_datavine_peer_destination(
		struct vine_manager *q,
		const char *id,
		struct vine_worker_info *destination);

/* Validate destination progress that is nonzero and below the expected size. */
int vine_current_transfers_is_partial_datavine_peer_progress(
		struct vine_manager *q,
		const char *id,
		struct vine_worker_info *destination,
		uint64_t bytes);

const char *vine_current_transfers_peer_source_workerid(
		struct vine_manager *q, const char *id);
const char *vine_current_transfers_cachename(
		struct vine_manager *q, const char *id);
int vine_current_transfers_uses_alternate_peer(
		struct vine_manager *q,
		const char *id,
		const char *excluded_source_workerid);

/* Abruptly lose the source endpoint of an active DataVine peer lease. */
int vine_current_transfers_abort_source(struct vine_manager *q, const char *id);

uint64_t vine_current_transfers_pending_releases(
		struct vine_manager *q);
uint64_t vine_current_transfers_pending_release_capacity(
		struct vine_manager *q);
uint64_t vine_current_transfers_pending_release_high_water(
		struct vine_manager *q);
