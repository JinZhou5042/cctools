/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_current_transfers.h"
#include "macros.h"
#include "vine_file.h"
#include "vine_file_replica.h"
#include "vine_file_replica_table.h"
#include "vine_blocklist.h"
#include "vine_manager.h"
#include "vine_datavine.h"
#include "xxmalloc.h"

#include "debug.h"

struct vine_transfer_pair {
	struct vine_worker_info *dest_worker;
	struct vine_worker_info *source_worker;
	char *source_url;
	int datavine_lease;
	int success;
	int completed;
};

static struct vine_transfer_pair *vine_transfer_pair_create(struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url)
{
	struct vine_transfer_pair *t = malloc(sizeof(struct vine_transfer_pair));
	t->dest_worker = dest_worker;
	t->source_worker = source_worker;
	t->source_url = source_url ? xxstrdup(source_url) : 0;
	t->datavine_lease = 0;
	t->success = 0;
	t->completed = 0;

	if (t->dest_worker) {
		t->dest_worker->incoming_xfer_counter++;
	}
	if (t->source_worker) {
		t->source_worker->outgoing_xfer_counter++;
	}

	return t;
}

static void vine_transfer_pair_delete(struct vine_transfer_pair *p)
{
	if (p) {
		if (p->dest_worker) {
			p->dest_worker->incoming_xfer_counter--;
		}
		if (p->source_worker) {
			p->source_worker->outgoing_xfer_counter--;
		}
		free(p->source_url);
		free(p);
	}
}

static void vine_transfer_pair_complete(struct vine_transfer_pair *p)
{
	if (!p || p->completed) {
		return;
	}
	p->completed = 1;
	if (p->dest_worker) {
		p->dest_worker->incoming_xfer_counter--;
		p->dest_worker = 0;
	}
	if (p->source_worker) {
		p->source_worker->outgoing_xfer_counter--;
		p->source_worker = 0;
	}
}

// add a current transaction to the transfer table
char *vine_current_transfers_add(struct vine_manager *q, struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url, struct vine_file *f)
{
	cctools_uuid_t uuid;
	cctools_uuid_create(&uuid);

	char *transfer_id = strdup(uuid.str);
	struct vine_transfer_pair *t = vine_transfer_pair_create(dest_worker, source_worker, source_url);
	if (f && f->datavine_data_id && source_worker) {
		if (!source_worker->workerid || !dest_worker || !dest_worker->workerid ||
				!vine_datavine_acquire_transfer(q, f->datavine_data_id, source_worker->workerid, dest_worker->workerid, transfer_id)) {
			debug(D_VINE, "DataVine rejected peer transfer lease for %s; using stable origin", f->datavine_data_id);
			vine_transfer_pair_delete(t);
			free(transfer_id);
			return 0;
		}
		t->datavine_lease = 1;
	}

	hash_table_insert(q->current_transfer_table, transfer_id, t);
	return transfer_id;
}

// remove a completed transaction from the transfer table - i.e. open the source to an additional transfer
int vine_current_transfers_remove(struct vine_manager *q, const char *id)
{
	struct vine_transfer_pair *p;
	p = hash_table_lookup(q->current_transfer_table, id);
	if (p) {
		if (p->datavine_lease && !vine_datavine_release_transfer(q, id, p->success)) {
			debug(D_ERROR, "DataVine transfer lease release failed for %s; retaining transfer record", id);
			return 0;
		}
		hash_table_remove(q->current_transfer_table, id);
		vine_transfer_pair_delete(p);
		return 1;
	} else {
		return 0;
	}
}

void set_throttle(struct vine_manager *m, struct vine_worker_info *w, int is_destination)
{
	if (!w) {
		return;
	}

	int good;
	int bad;
	int streak;

	int grace = 5; // XXX: make tunable parameter: q->consecutieve_max_xfer_errors;
	const char *dir;

	if (is_destination) {
		good = w->xfer_total_good_destination_counter;
		bad = w->xfer_total_bad_destination_counter;
		streak = w->xfer_streak_bad_destination_counter;
		dir = "destination";
		// since worker can talk to manager, probably the issue is with sources. Give more chances to
		// destinations.
		grace *= 2;
	} else {
		good = w->xfer_total_good_source_counter;
		bad = w->xfer_total_bad_source_counter;
		streak = w->xfer_streak_bad_source_counter;
		dir = "source";
	}

	debug(D_VINE, "Setting transfer failure (%d,%d/%d) timestamp on %s worker: %s:%d", streak, bad, good + bad, dir, w->hostname, w->transfer_port);

	w->last_transfer_failure = timestamp_get();

	/* first error treat as a normal error */
	if (streak < grace) {
		return;
	}

	if (good <= bad) {
		/* this worker has failed more often than not, release it. */
		notice(D_VINE, "Releasing worker %s because of repeated %s transfer failures: %d/%d", dir, w->addrport, bad, bad + good);
		vine_manager_remove_worker(m, w, VINE_WORKER_DISCONNECT_XFER_ERRORS);
	}
}

int vine_current_transfers_set_failure(struct vine_manager *q, char *id, const char *cachename)
{
	struct vine_transfer_pair *p = hash_table_lookup(q->current_transfer_table, id);

	/* If there is no matching transfer record, then it means a worker failed and the record was removed b/c of wipe_worker. */
	if (!p) {
		return 0;
	}

	struct vine_worker_info *source_worker = p->source_worker;
	struct vine_worker_info *dest_worker = p->dest_worker;
	p->success = 0;

	/* Stable URL transfers intentionally have no source worker.  A URL
	 * failure is not evidence that the destination worker is unhealthy. */
	if (!source_worker && p->source_url && dest_worker) {
		vine_transfer_pair_complete(p);
		return 0;
	}

	/* Peer transfers require both worker endpoints. */
	if (!source_worker || !dest_worker) {
		if (!source_worker) {
			debug(D_ERROR, "%s: peer transfer record for file %s with id %s is found, but source worker is null", __func__, cachename, id);
		}
		if (!dest_worker) {
			debug(D_ERROR, "%s: transfer record for file %s with id %s is found, but destination worker is null", __func__, cachename, id);
		}
		vine_transfer_pair_complete(p);
		return 0;
	}

	/* The transfer is considered a worker fault only if the replica exists on the source worker
	 * and its state is VINE_FILE_REPLICA_STATE_READY, meaning the source has the replica but, for
	 * some reason, failed to transfer it to the destination. */
	struct vine_file_replica *source_replica = vine_file_replica_table_lookup(source_worker, cachename);
	if (source_replica && source_replica->state == VINE_FILE_REPLICA_STATE_READY) {
		debug(D_VINE, "Unable to transfer a READY replica from %s (%s) to %s (%s) for file %s \n", source_worker->hostname, source_worker->addrport, dest_worker->hostname, dest_worker->addrport, cachename);

		/* Detach the transfer before throttling may remove either worker. */
		vine_transfer_pair_complete(p);

		source_worker->xfer_streak_bad_source_counter++;
		source_worker->xfer_total_bad_source_counter++;
		set_throttle(q, source_worker, 0);

		dest_worker->xfer_streak_bad_destination_counter++;
		dest_worker->xfer_total_bad_destination_counter++;
		set_throttle(q, dest_worker, 1);
		return 1;
	}

	vine_transfer_pair_complete(p);
	return 0;
}

void vine_current_transfers_set_success(struct vine_manager *q, char *id)
{
	struct vine_transfer_pair *p = hash_table_lookup(q->current_transfer_table, id);

	if (!p) {
		return;
	}
	p->success = 1;

	struct vine_worker_info *source = p->source_worker;
	if (source) {
		vine_blocklist_unblock(q, source->addrport);

		source->xfer_streak_bad_source_counter = 0;
		source->xfer_total_good_source_counter++;
	}

	struct vine_worker_info *dest_worker = p->dest_worker;
	if (dest_worker) {
		vine_blocklist_unblock(q, dest_worker->addrport);

		dest_worker->xfer_streak_bad_destination_counter = 0;
		dest_worker->xfer_total_good_destination_counter++;
	}
	vine_transfer_pair_complete(p);
}

// count the number transfers coming from a specific remote url (not a worker)
int vine_current_transfers_url_in_use(struct vine_manager *q, const char *source)
{
	char *id;
	struct vine_transfer_pair *t;
	int iteration;

	int c = 0;
	HASH_TABLE_ITERATE(q->current_transfer_table, iteration, id, t)
	{
		if (source == t->source_url)
			c++;
	}
	return c;
}

// remove all transactions involving a worker from the transfer table - if a worker failed or is being deleted
// intentionally
int vine_current_transfers_wipe_worker(struct vine_manager *q, struct vine_worker_info *w)
{
	debug(D_VINE, "Removing instances of worker from transfer table");

	int removed = 0;
	if (!w) {
		return removed;
	}

	struct list *ids_to_remove = list_create();

	char *id;
	struct vine_transfer_pair *t;
	int iteration;

	HASH_TABLE_ITERATE(q->current_transfer_table, iteration, id, t)
	{
		if (t->dest_worker == w || t->source_worker == w) {
			list_push_tail(ids_to_remove, xxstrdup(id));
		}
	}

	// BUG: Inefficient, implement safe remove in hash table
	list_first_item(ids_to_remove);
	char *transfer_id;
	while ((transfer_id = list_pop_head(ids_to_remove))) {
		struct vine_transfer_pair *t = hash_table_lookup(q->current_transfer_table, transfer_id);
		if (t) {
			t->success = 0;
			vine_transfer_pair_complete(t);
		}
		vine_current_transfers_remove(q, transfer_id);
		free(transfer_id);
		removed++;
	}

	list_delete(ids_to_remove);

	return removed;
}

void vine_current_transfers_print_table(struct vine_manager *q)
{
	char *id;
	struct vine_transfer_pair *t;
	struct vine_worker_info *w;
	int iteration;

	debug(D_VINE, "-----------------TRANSFER-TABLE--------------------");
	HASH_TABLE_ITERATE(q->current_transfer_table, iteration, id, t)
	{
		w = t->source_worker;
		if (w) {
			debug(D_VINE, "%s : source: %s:%d url: %s", id, w->transfer_host, w->transfer_port, t->source_url);
		} else {
			debug(D_VINE, "%s : source: remote url: %s", id, t->source_url);
		}
	}
	debug(D_VINE, "-----------------END-------------------------------");
}

void vine_current_transfers_clear(struct vine_manager *q)
{
	struct list *ids = list_create();
	char *id;
	struct vine_transfer_pair *t;
	int iteration;
	HASH_TABLE_ITERATE(q->current_transfer_table, iteration, id, t)
	{
		t->success = 0;
		vine_transfer_pair_complete(t);
		list_push_tail(ids, xxstrdup(id));
	}
	while ((id = list_pop_head(ids))) {
		if (!vine_current_transfers_remove(q, id)) {
			struct vine_transfer_pair *pending = hash_table_remove(q->current_transfer_table, id);
			vine_transfer_pair_delete(pending);
		}
		free(id);
	}
	list_delete(ids);
}

int vine_current_transfers_get_table_size(struct vine_manager *q)
{
	return hash_table_size(q->current_transfer_table);
}

int vine_current_transfers_is_datavine_peer(struct vine_manager *q, const char *id)
{
	struct vine_transfer_pair *p;
	if (!q || !id) {
		return 0;
	}
	p = hash_table_lookup(q->current_transfer_table, id);
	return p && p->datavine_lease && p->source_worker && p->dest_worker && !p->completed;
}

int vine_current_transfers_is_datavine_peer_destination(
		struct vine_manager *q,
		const char *id,
		struct vine_worker_info *destination)
{
	struct vine_transfer_pair *p;
	if (!q || !id || !destination) {
		return 0;
	}
	p = hash_table_lookup(q->current_transfer_table, id);
	return p && p->datavine_lease && p->source_worker &&
			p->dest_worker == destination && !p->completed;
}

int vine_current_transfers_abort_source(struct vine_manager *q, const char *id)
{
	struct vine_transfer_pair *p;
	struct vine_worker_info *source;
	if (!q || !id) {
		return 0;
	}
	p = hash_table_lookup(q->current_transfer_table, id);
	if (!p || !p->datavine_lease || p->completed || !p->source_worker) {
		return 0;
	}
	source = p->source_worker;
	vine_manager_send(q, source, "abort-worker\n");
	vine_manager_remove_worker(
			q, source, VINE_WORKER_DISCONNECT_FAILURE);
	return 1;
}

int vine_current_transfers_retry_releases(struct vine_manager *q, int limit)
{
	if (!q || limit < 1) {
		return 0;
	}
	struct list *ids = list_create();
	char *id;
	struct vine_transfer_pair *t;
	int iteration;
	HASH_TABLE_ITERATE(q->current_transfer_table, iteration, id, t)
	{
		if (t->datavine_lease && t->completed) {
			list_push_tail(ids, xxstrdup(id));
			if (list_size(ids) >= limit) {
				break;
			}
		}
	}
	int released = 0;
	while ((id = list_pop_head(ids))) {
		released += vine_current_transfers_remove(q, id);
		free(id);
	}
	list_delete(ids);
	return released;
}
