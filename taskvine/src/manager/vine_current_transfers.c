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
#include "vine_schedule.h"
#include "stringtools.h"
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
	int64_t bytes; /* The size of the source worker's replica, counted in its outgoing_xfer_bytes. */
	timestamp_t start_time;		      /* When the transfer was requested, to tell whether its source spoke since. */
	char *id;			      /* A copy of the record's key in current_transfer_table. */
	struct vine_transfer_pair *dest_prev; /* Neighbors in dest_worker->incoming_transfers. */
	struct vine_transfer_pair *dest_next;
	struct vine_transfer_pair *source_prev; /* Neighbors in source_worker->outgoing_transfers. */
	struct vine_transfer_pair *source_next;
};

static struct vine_transfer_pair *vine_transfer_pair_create(struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url, const char *cachename, const char *id)
{
	struct vine_transfer_pair *t = xxcalloc(1, sizeof(struct vine_transfer_pair));
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
	if (t->source_worker) {
		struct vine_file_replica *replica = cachename ? vine_file_replica_table_lookup(source_worker, cachename) : 0;
		t->bytes = replica ? MAX(replica->size, 0) : 0;
		t->source_worker->outgoing_xfer_bytes += t->bytes;
		t->source_worker->outgoing_xfer_counter++;
		t->source_next = source_worker->outgoing_transfers;
		if (t->source_next) {
			t->source_next->source_prev = t;
		}
		source_worker->outgoing_transfers = t;
	}

	return t;
}

static void vine_transfer_pair_delete(struct vine_transfer_pair *p)
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
		if (p->source_worker) {
			p->source_worker->outgoing_xfer_bytes -= p->bytes;
			p->source_worker->outgoing_xfer_counter--;
			if (p->source_prev) {
				p->source_prev->source_next = p->source_next;
			} else {
				p->source_worker->outgoing_transfers = p->source_next;
			}
			if (p->source_next) {
				p->source_next->source_prev = p->source_prev;
			}
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

// add a current transaction to the transfer table
char *vine_current_transfers_add(struct vine_manager *q, struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url, const char *cachename)
{
	/* The id only has to be unique among the manager's records and differ from "X", which a worker sends for a file
	 * without a record. Sixteen hex digits of a counter cost less than formatting a uuid. */
	static const char digits[] = "0123456789abcdef";
	uint64_t number = q->next_transfer_id++;
	char *transfer_id = xxmalloc(17);
	for (int i = 15; i >= 0; i--) {
		transfer_id[i] = digits[number & 0xf];
		number >>= 4;
	}
	transfer_id[16] = 0;
	struct vine_transfer_pair *t = vine_transfer_pair_create(dest_worker, source_worker, source_url, cachename, transfer_id);

	hash_table_insert(q->current_transfer_table, transfer_id, t);
	update_url_count(q, t->source_url, 1);
	return transfer_id;
}

static void remove_pair(struct vine_manager *q, struct vine_transfer_pair *p);
static void count_success(struct vine_manager *q, struct vine_transfer_pair *p);

// remove a completed transaction from the transfer table - i.e. open the source to an additional transfer
int vine_current_transfers_remove(struct vine_manager *q, const char *id)
{
	struct vine_transfer_pair *p = hash_table_remove(q->current_transfer_table, id);
	if (!p) {
		return 0;
	}
	remove_pair(q, p);
	return 1;
}

/* Remove the record of a transfer that succeeded, and clear the failure streaks of its source and destination. The
 * record is looked up once. */
int vine_current_transfers_remove_success(struct vine_manager *q, const char *id)
{
	struct vine_transfer_pair *p = hash_table_remove(q->current_transfer_table, id);
	if (!p) {
		return 0;
	}
	count_success(q, p);
	remove_pair(q, p);
	return 1;
}

/* Release a record taken out of the transfer table. */
static void remove_pair(struct vine_manager *q, struct vine_transfer_pair *p)
{
	update_url_count(q, p->source_url, -1);
	/* A source that leaves its limit of transfers, or now has room for the smallest file it refused for its bytes in
	 * flight, can serve another one, so ready tasks that wait for one of its files may run. Other ends of transfers
	 * change nothing for them. A transfer from the data service vault names its file after the manager:// prefix. */
	if (p->source_worker) {
		struct vine_worker_info *w = p->source_worker;
		int64_t remaining = w->outgoing_xfer_bytes - p->bytes;
		int64_t refused = w->outgoing_refused_bytes;
		if (w->outgoing_xfer_counter == q->worker_source_max_transfers || (refused >= 0 && (remaining == 0 || remaining + refused <= VINE_WORKER_SOURCE_MAX_BYTES))) {
			w->outgoing_refused_bytes = -1;
			vine_schedule_wake_worker(q, w);
		}
	} else if (p->source_url && vine_current_transfers_url_in_use(q, p->source_url) == q->file_source_max_transfers - 1) {
		vine_schedule_wake_file(q, string_prefix_is(p->source_url, "manager://") ? p->source_url + strlen("manager://") : p->source_url);
	}
	vine_transfer_pair_delete(p);
}

int vine_current_transfers_source_has_room(struct vine_manager *q, struct vine_worker_info *w, int64_t size)
{
	if (w->outgoing_xfer_counter >= q->worker_source_max_transfers) {
		return 0;
	}
	/* A size that is not known counts as none, so the count alone limits such a file. */
	size = MAX(size, 0);
	if (w->outgoing_xfer_bytes == 0 || w->outgoing_xfer_bytes + size <= VINE_WORKER_SOURCE_MAX_BYTES) {
		return 1;
	}
	if (w->outgoing_refused_bytes < 0 || size < w->outgoing_refused_bytes) {
		w->outgoing_refused_bytes = size;
	}
	return 0;
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

	/* A transfer from a URL has no source worker, so no worker is at fault. */
	if (!source_worker) {
		return 0;
	}

	/* If p is valid, the destination should always be valid, because a failed worker causes the transfer record to be removed,
	 * not nulled out. This shouldn't happen, but we check and emit an error just in case. */
	if (!dest_worker) {
		debug(D_ERROR, "%s: transfer record for file %s with id %s is found, but destination worker is null", __func__, cachename, id);
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

/* Clear the failure streaks of the source and destination of a successful transfer. */
static void count_success(struct vine_manager *q, struct vine_transfer_pair *p)
{
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
	hash_table_clear(q->current_transfer_table, (void *)vine_transfer_pair_delete);
	hash_table_clear(q->current_transfer_url_table, free);
}

int vine_current_transfers_get_table_size(struct vine_manager *q)
{
	return hash_table_size(q->current_transfer_table);
}
