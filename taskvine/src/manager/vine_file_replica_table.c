/*
This software is distributed under the GNU General Public License.
Copyright (C) 2022- The University of Notre Dame
See the file COPYING for details.
*/

#include <math.h>

#include "vine_file_replica_table.h"
#include "set.h"
#include "vine_blocklist.h"
#include "vine_current_transfers.h"
#include "vine_file.h"
#include "vine_file_replica.h"
#include "vine_manager.h"
#include "vine_manager_put.h"
#include "vine_worker_info.h"

#include "stringtools.h"

#include "debug.h"
#include "macros.h"

// add a file to the remote file table.
int vine_file_replica_table_insert(struct vine_manager *m, struct vine_worker_info *w, const char *cachename, struct vine_file_replica *replica)
{
	if (hash_table_lookup(w->current_files, cachename)) {
		// delete the previous replcia because the replica's size might have changed
		vine_file_replica_table_remove(m, w, cachename);
	}

	double prev_available = w->resources->disk.total - BYTES_TO_MEGABYTES(w->inuse_cache);

	hash_table_insert(w->current_files, cachename, replica);
	w->inuse_cache += replica->size;

	if (prev_available >= m->current_max_worker->disk) {
		/* the current worker may have been the one with the maximum available space, so we update it. */
		m->current_max_worker->disk = w->resources->disk.total - BYTES_TO_MEGABYTES(w->inuse_cache);
	}

	struct set *workers = hash_table_lookup(m->file_worker_table, cachename);
	if (!workers) {
		workers = set_create(0);
		hash_table_insert(m->file_worker_table, cachename, workers);
	}

	set_insert(workers, w);

	struct set *temp_source_workers = hash_table_lookup(m->file_worker_table, cachename);
	if (!temp_source_workers || set_size(temp_source_workers) == 0) {
		debug(D_ERROR, "WHAT THE HELL, the source worker set is NULL \n");
	}

	return 1;
}

// remove a file from the remote file table.
struct vine_file_replica *vine_file_replica_table_remove(struct vine_manager *m, struct vine_worker_info *w, const char *cachename)
{
	if (!w) {
		// Handle error: invalid pointer
		return 0;
	}

	if (!hash_table_lookup(w->current_files, cachename)) {
		return 0;
	}

	struct vine_file_replica *replica = hash_table_remove(w->current_files, cachename);
	if (replica) {
		w->inuse_cache -= replica->size;
	}

	double available = w->resources->disk.total - BYTES_TO_MEGABYTES(w->inuse_cache);
	if (available > m->current_max_worker->disk) {
		/* the current worker has more space than we knew before for all workers, so we update it. */
		m->current_max_worker->disk = available;
	}

	struct set *workers = hash_table_lookup(m->file_worker_table, cachename);

	if (workers) {
		set_remove(workers, w);
		if (set_size(workers) < 1) {
			hash_table_remove(m->file_worker_table, cachename);
			set_delete(workers);
		}
	}

	return replica;
}

// lookup a file in posession of a specific worker
struct vine_file_replica *vine_file_replica_table_lookup(struct vine_worker_info *w, const char *cachename)
{
	return hash_table_lookup(w->current_files, cachename);
}

// count the number of in-cluster replicas of a file
int vine_file_replica_count(struct vine_manager *m, struct vine_file *f)
{
	struct set *source_workers = hash_table_lookup(m->file_worker_table, f->cached_name);
	if (!source_workers) {
		return 0;
	}
	return set_size(source_workers);
}

// find a worker (randomly) in posession of a specific file, and is ready to transfer it.
struct vine_worker_info *vine_file_replica_table_find_worker(struct vine_manager *q, const char *cachename)
{
	struct set *workers = hash_table_lookup(q->file_worker_table, cachename);
	if (!workers) {
		return 0;
	}

	int total_count = set_size(workers);
	if (total_count < 1) {
		return 0;
	}

	int random_index = random() % total_count;

	struct vine_worker_info *peer = NULL;
	struct vine_worker_info *peer_selected = NULL;
	struct vine_file_replica *replica = NULL;

	int offset_bookkeep;
	SET_ITERATE_RANDOM_START(workers, offset_bookkeep, peer)
	{
		random_index--;
		if (!peer->transfer_port_active)
			continue;

		timestamp_t current_time = timestamp_get();
		if (current_time - peer->last_transfer_failure < q->transient_error_interval) {
			debug(D_VINE, "Skipping worker source after recent failure : %s", peer->transfer_host);
			continue;
		}

		if ((replica = hash_table_lookup(peer->current_files, cachename)) && replica->state == VINE_FILE_REPLICA_STATE_READY) {
			int current_transfers = peer->outgoing_xfer_counter;
			if (current_transfers < q->worker_source_max_transfers) {
				peer_selected = peer;
				if (random_index < 0) {
					return peer_selected;
				}
			}
		}
	}

	return peer_selected;
}

/*
Count number of replicas of a file in the system.
*/
int vine_file_replica_table_count_replicas(struct vine_manager *q, const char *cachename, vine_file_replica_state_t state)
{
	struct vine_worker_info *w;
	struct vine_file_replica *r;
	int count = 0;

	struct set *workers = hash_table_lookup(q->file_worker_table, cachename);
	if (workers) {
		SET_ITERATE(workers, w)
		{
			r = hash_table_lookup(w->current_files, cachename);
			if (r && r->state == state) {
				count++;
			}
		}
	}

	return count;
}

/*
Check if a file replica exists on a worker. We accept both CREATING and READY replicas,
since a CREATING replica may already exist physically but hasn't yet received the cache-update
message from the manager. However, we do not accept DELETING replicas, as they indicate
the source worker has already been sent an unlink request—any subsequent cache-update or
cache-invalid events will lead to deletion.
*/
int vine_file_replica_table_exists_somewhere(struct vine_manager *q, const char *cachename)
{
	struct set *workers = hash_table_lookup(q->file_worker_table, cachename);
	if (!workers || set_size(workers) < 1) {
		return 0;
	}

	struct vine_worker_info *w;
	SET_ITERATE(workers, w)
	{
		struct vine_file_replica *r = vine_file_replica_table_lookup(w, cachename);
		if (r && (r->state == VINE_FILE_REPLICA_STATE_CREATING || r->state == VINE_FILE_REPLICA_STATE_READY)) {
			return 1;
		}
	}

	return 0;
}

// get or create a replica for a worker and cachename
struct vine_file_replica *vine_file_replica_table_get_or_create(struct vine_manager *m, struct vine_worker_info *w, const char *cachename, vine_file_type_t type, vine_cache_level_t cache_level, int64_t size, time_t mtime)
{
	// First check if the replica already exists
	struct vine_file_replica *replica = vine_file_replica_table_lookup(w, cachename);
	if (replica) {
		// If the size is different, update accounting and fields
		if (replica->size != size) {
			w->inuse_cache -= replica->size;
			w->inuse_cache += size;
			replica->size = size;
		}
		// Always update these fields to match the latest info
		replica->mtime = mtime;
		replica->type = type;
		replica->cache_level = cache_level;

		struct set *temp_source_workers = hash_table_lookup(m->file_worker_table, cachename);
		if (!temp_source_workers || set_size(temp_source_workers) == 0) {
			debug(D_ERROR, "WHAT? The replica is found but the set is NULL! \n");
		}

		return replica;
	}

	// Create a new replica
	replica = vine_file_replica_create(type, cache_level, size, mtime);
	vine_file_replica_table_insert(m, w, cachename, replica);

	return replica;
}
