/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_current_transfers.h"
#include "macros.h"
#include "vine_file_replica.h"
#include "vine_file_replica_table.h"
#include "vine_blocklist.h"
#include "vine_manager.h"
#include "xxmalloc.h"

#include <string.h>

#include "debug.h"

/* One record of current_transfer_table. A record with a worker is also in that worker's list of incoming or outgoing
 * records, so the records of a removed worker are found without a pass over the table. */
struct vine_transfer_pair {
	struct vine_worker_info *dest_worker;
	struct vine_worker_info *source_worker;
	char *source_url;
	char *cachename;		      /* The file that the destination receives. */
	int checkpoint;			      /* Set for a checkpoint, counted in the source's outgoing_checkpoints. */
	timestamp_t start_time;		      /* When the transfer was requested, to tell whether its source spoke since. */
	char *id;			      /* A copy of the record's key in current_transfer_table. */
	struct vine_transfer_pair *dest_prev; /* Neighbors in dest_worker->incoming_transfers. */
	struct vine_transfer_pair *dest_next;
	struct vine_transfer_pair *source_prev; /* Neighbors in source_worker->outgoing_transfers. */
	struct vine_transfer_pair *source_next;
	struct vine_transfer_host *host;      /* The source worker's host, or null. */
	int64_t host_bytes;		      /* The size of the source's replica, counted in the host's bytes. */
	struct vine_transfer_pair *host_prev; /* Neighbors in the host's transfers to workers, in the order they started. */
	struct vine_transfer_pair *host_next;
};

/* The outgoing transfer records of the workers of one host, as their hostname names it, in q->transfer_hosts. */
struct vine_transfer_host {
	char *name;
	int refs;	   /* Workers attached to it plus its records, which keep it after its workers leave. */
	int sends;	   /* Its transfers to workers. */
	int64_t bytes;	   /* The sizes of the source replicas of those transfers. */
	int checkpoints;   /* Its checkpoints. */
	struct vine_transfer_pair *oldest; /* Its transfers to workers in the order they started. */
	struct vine_transfer_pair *newest;
};

static void host_unref(struct vine_manager *q, struct vine_transfer_host *h)
{
	if (--h->refs == 0) {
		hash_table_remove(q->transfer_hosts, h->name);
		free(h->name);
		free(h);
	}
}

void vine_current_transfers_attach_host(struct vine_manager *q, struct vine_worker_info *w)
{
	vine_current_transfers_detach_host(q, w);
	struct vine_transfer_host *h = hash_table_lookup(q->transfer_hosts, w->hostname);
	if (!h) {
		h = xxcalloc(1, sizeof(*h));
		h->name = xxstrdup(w->hostname);
		hash_table_insert(q->transfer_hosts, h->name, h);
	}
	h->refs++;
	w->transfer_host_load = h;
}

void vine_current_transfers_detach_host(struct vine_manager *q, struct vine_worker_info *w)
{
	if (w->transfer_host_load) {
		host_unref(q, w->transfer_host_load);
		w->transfer_host_load = NULL;
	}
}

timestamp_t vine_current_transfers_host_load(struct vine_worker_info *w, int *sends, int64_t *bytes, int *checkpoints)
{
	struct vine_transfer_host *h = w->transfer_host_load;
	*sends = h ? h->sends : 0;
	*bytes = h ? h->bytes : 0;
	*checkpoints = h ? h->checkpoints : 0;
	return h && h->oldest ? h->oldest->start_time : 0;
}

static struct vine_transfer_pair *vine_transfer_pair_create(struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url, const char *cachename, const char *id, int checkpoint)
{
	struct vine_transfer_pair *t = xxcalloc(1, sizeof(struct vine_transfer_pair));
	t->checkpoint = checkpoint;
	t->dest_worker = dest_worker;
	t->source_worker = source_worker;
	t->source_url = source_url ? xxstrdup(source_url) : 0;
	t->cachename = cachename ? xxstrdup(cachename) : 0;
	t->start_time = timestamp_get();
	t->id = xxstrdup(id);

	if (t->dest_worker) {
		t->dest_worker->incoming_xfer_counter++;
		t->dest_next = dest_worker->incoming_transfers;
		if (t->dest_next) {
			t->dest_next->dest_prev = t;
		}
		dest_worker->incoming_transfers = t;
	}
	if (t->source_worker && t->checkpoint) {
		t->source_worker->outgoing_checkpoints++;
	} else if (t->source_worker) {
		t->source_worker->outgoing_xfer_counter++;
	}
	if (t->source_worker) {
		t->source_next = source_worker->outgoing_transfers;
		if (t->source_next) {
			t->source_next->source_prev = t;
		}
		source_worker->outgoing_transfers = t;
	}
	if (t->source_worker && t->source_worker->transfer_host_load) {
		struct vine_transfer_host *h = t->source_worker->transfer_host_load;
		t->host = h;
		h->refs++;
		if (t->checkpoint) {
			h->checkpoints++;
		} else {
			struct vine_file_replica *replica = cachename ? vine_file_replica_table_lookup(source_worker, cachename) : 0;
			t->host_bytes = replica ? MAX(replica->size, 0) : 0;
			h->sends++;
			h->bytes += t->host_bytes;
			t->host_prev = h->newest;
			if (h->newest) {
				h->newest->host_next = t;
			} else {
				h->oldest = t;
			}
			h->newest = t;
		}
	}

	return t;
}

static void vine_transfer_pair_delete(struct vine_manager *q, struct vine_transfer_pair *p)
{
	if (p) {
		if (p->dest_worker) {
			p->dest_worker->incoming_xfer_counter--;
			if (p->dest_prev) {
				p->dest_prev->dest_next = p->dest_next;
			} else {
				p->dest_worker->incoming_transfers = p->dest_next;
			}
			if (p->dest_next) {
				p->dest_next->dest_prev = p->dest_prev;
			}
		}
		if (p->source_worker && p->checkpoint) {
			p->source_worker->outgoing_checkpoints--;
		} else if (p->source_worker) {
			p->source_worker->outgoing_xfer_counter--;
		}
		if (p->source_worker) {
			if (p->source_prev) {
				p->source_prev->source_next = p->source_next;
			} else {
				p->source_worker->outgoing_transfers = p->source_next;
			}
			if (p->source_next) {
				p->source_next->source_prev = p->source_prev;
			}
		}
		if (p->host && p->checkpoint) {
			p->host->checkpoints--;
		} else if (p->host) {
			struct vine_transfer_host *h = p->host;
			h->sends--;
			h->bytes -= p->host_bytes;
			if (p->host_prev) {
				p->host_prev->host_next = p->host_next;
			} else {
				h->oldest = p->host_next;
			}
			if (p->host_next) {
				p->host_next->host_prev = p->host_prev;
			} else {
				h->newest = p->host_prev;
			}
		}
		if (p->host) {
			host_unref(q, p->host);
		}
		free(p->source_url);
		free(p->cachename);
		free(p->id);
		free(p);
	}
}

// adjust the number of current transfers from a source url - the entry is removed when the count reaches zero
static void update_url_count(struct vine_manager *q, const char *source_url, int delta)
{
	if (!source_url) {
		return;
	}

	int *count = hash_table_lookup(q->current_transfer_url_table, source_url);
	if (!count) {
		count = xxmalloc(sizeof(*count));
		*count = 0;
		hash_table_insert(q->current_transfer_url_table, source_url, count);
	}

	*count += delta;
	if (*count <= 0) {
		hash_table_remove(q->current_transfer_url_table, source_url);
		free(count);
	}
}

static char *add_record(struct vine_manager *q, struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url, const char *cachename, int checkpoint)
{
	cctools_uuid_t uuid;
	cctools_uuid_create(&uuid);

	char *transfer_id = strdup(uuid.str);
	struct vine_transfer_pair *t = vine_transfer_pair_create(dest_worker, source_worker, source_url, cachename, transfer_id, checkpoint);

	hash_table_insert(q->current_transfer_table, transfer_id, t);
	update_url_count(q, t->source_url, 1);
	return transfer_id;
}

// add a current transaction to the transfer table
char *vine_current_transfers_add(struct vine_manager *q, struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url, const char *cachename)
{
	return add_record(q, dest_worker, source_worker, source_url, cachename, 0);
}

char *vine_current_transfers_add_checkpoint(struct vine_manager *q, struct vine_worker_info *source_worker, const char *source_url, const char *cachename)
{
	return add_record(q, NULL, source_worker, source_url, cachename, 1);
}

// remove a completed transaction from the transfer table - i.e. open the source to an additional transfer
int vine_current_transfers_remove(struct vine_manager *q, const char *id)
{
	struct vine_transfer_pair *p;
	p = hash_table_remove(q->current_transfer_table, id);
	if (p) {
		update_url_count(q, p->source_url, -1);
		vine_transfer_pair_delete(q, p);
		return 1;
	} else {
		return 0;
	}
}

/*
Count a failed transfer against a worker, and release the worker after repeated failures. A worker is released after
a streak of failures when it failed more often than not. A source is also released after a streak when it sent
nothing since the failed transfer was requested (silent), because its host then hangs with its connections open: it
does not answer keepalives either, and the tasks that it runs never return until it is removed.
*/

void set_throttle(struct vine_manager *m, struct vine_worker_info *w, int is_destination, int silent)
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

	/* The first errors of a streak are treated as normal errors. */
	if (streak >= grace && good <= bad) {
		/* this worker has failed more often than not, release it. */
		notice(D_VINE, "Releasing worker %s because of repeated %s transfer failures: %d/%d", dir, w->addrport, bad, bad + good);
		vine_manager_mark_worker_for_removal(m, w, VINE_WORKER_DISCONNECT_XFER_ERRORS);
		return;
	}

	if (streak >= grace && silent) {
		notice(D_VINE, "Releasing worker %s because %d transfers from it failed in a row and it sent nothing since the last one started", w->addrport, streak);
		vine_manager_mark_worker_for_removal(m, w, VINE_WORKER_DISCONNECT_XFER_ERRORS);
		return;
	}

	if (is_destination) {
		return;
	}

	/* The source is not chosen again until it sends a message. A check asks a live source for one now, unless a
	 * check is already outstanding. */
	w->last_source_failure = w->last_transfer_failure;
	if (w->last_msg_recv_time > w->last_update_msg_time) {
		if (vine_manager_send(m, w, "check\n") > 0) {
			w->last_update_msg_time = w->last_source_failure;
		}
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

	/* If p is valid, the elements of p should always be valid, because a failed worker causes the transfer record to be removed,
	 * not nulled out. This shouldn't happen, but we check and emit an error just in case. */
	if (!source_worker || !dest_worker) {
		if (!source_worker) {
			debug(D_ERROR, "%s: transfer record for file %s with id %s is found, but source worker is null", __func__, cachename, id);
		}
		if (!dest_worker) {
			debug(D_ERROR, "%s: transfer record for file %s with id %s is found, but destination worker is null", __func__, cachename, id);
		}
		return 0;
	}

	/* The transfer is considered a worker fault only if the replica exists on the source worker
	 * and its state is VINE_FILE_REPLICA_STATE_READY, meaning the source has the replica but, for
	 * some reason, failed to transfer it to the destination. */
	struct vine_file_replica *source_replica = vine_file_replica_table_lookup(source_worker, cachename);
	if (source_replica && source_replica->state == VINE_FILE_REPLICA_STATE_READY) {
		debug(D_VINE, "Unable to transfer a READY replica from %s (%s) to %s (%s) for file %s \n", source_worker->hostname, source_worker->addrport, dest_worker->hostname, dest_worker->addrport, cachename);

		/* A source that is being removed is already judged. */
		if (source_worker->pending_removal) {
			return 1;
		}

		/* A source that failed before and sent nothing since explains the failure, so it does not count against
		 * the destination. Otherwise a source whose host hangs fails every transfer to healthy destinations, which
		 * would be released in turn. */
		int source_suspect = vine_worker_source_is_suspect(source_worker);

		source_worker->xfer_streak_bad_source_counter++;
		source_worker->xfer_total_bad_source_counter++;
		set_throttle(q, source_worker, 0, source_worker->last_msg_recv_time < p->start_time);

		if (!source_suspect) {
			dest_worker->xfer_streak_bad_destination_counter++;
			dest_worker->xfer_total_bad_destination_counter++;
			set_throttle(q, dest_worker, 1, 0);
		}
		return 1;
	}

	return 0;
}

void vine_current_transfers_set_success(struct vine_manager *q, char *id)
{
	struct vine_transfer_pair *p = hash_table_lookup(q->current_transfer_table, id);

	if (!p) {
		return;
	}

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
}

// count the number transfers coming from a specific remote url (not a worker)
int vine_current_transfers_url_in_use(struct vine_manager *q, const char *source)
{
	if (!source) {
		return 0;
	}

	int *count = hash_table_lookup(q->current_transfer_url_table, source);
	return count ? *count : 0;
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

	/* Removing a record unlinks it from the worker's list, so the head is the next record each time. */
	while (w->incoming_transfers) {
		vine_current_transfers_remove(q, w->incoming_transfers->id);
		removed++;
	}
	while (w->outgoing_transfers) {
		vine_current_transfers_remove(q, w->outgoing_transfers->id);
		removed++;
	}

	return removed;
}

/*
A destination keeps waiting for data from a removed source until its peer transfer timeout, because a source whose host
hangs keeps its connections open. Unlinking the pending file kills the transfer process at the destination, which
reports cache-invalid, so the task that needs the file is set up again from another source at once. The records stay
until vine_current_transfers_wipe_worker(), so the failures that follow are not counted against either worker.
*/

int vine_current_transfers_abandon_source(struct vine_manager *q, struct vine_worker_info *w)
{
	int abandoned = 0;
	if (!w) {
		return abandoned;
	}

	struct vine_transfer_pair *p;
	for (p = w->outgoing_transfers; p; p = p->source_next) {
		if (p->dest_worker && p->dest_worker != w && p->cachename) {
			debug(D_VINE, "abandoning the transfer of %s from removed worker %s to %s", p->cachename, w->addrport, p->dest_worker->addrport);
			delete_worker_file(q, p->dest_worker, p->cachename, VINE_CACHE_LEVEL_TASK, VINE_CACHE_LEVEL_FOREVER);
			abandoned++;
		}
	}

	return abandoned;
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
	char *id;
	struct vine_transfer_pair *p;
	int iteration;
	/* A record's deletion also releases its host, so it needs the manager. */
	HASH_TABLE_ITERATE(q->current_transfer_table, iteration, id, p)
	{
		vine_transfer_pair_delete(q, p);
	}
	hash_table_clear(q->current_transfer_table, 0);
	/* Hosts that workers still attached are freed with the manager. */
	struct vine_transfer_host *h;
	HASH_TABLE_ITERATE(q->transfer_hosts, iteration, id, h)
	{
		free(h->name);
		free(h);
	}
	hash_table_clear(q->transfer_hosts, 0);
	hash_table_clear(q->current_transfer_url_table, free);
}

int vine_current_transfers_get_table_size(struct vine_manager *q)
{
	return hash_table_size(q->current_transfer_table);
}
