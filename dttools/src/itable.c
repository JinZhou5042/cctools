/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"
#include "xxmalloc.h"
#include "itable.h"

#include <stdlib.h>
#include <string.h>

#define DEFAULT_SIZE 127
#define DEFAULT_MAX_LOAD 0.75
#define DEFAULT_MIN_LOAD 0.125
#define REMOVED_KEEP_MIN 1024 /* removed array capacity that is always kept across compactions */
#define MIGRATE_INSERT 16      /* old buckets that each insert moves while a resize is in progress */
#define MIGRATE_COMPACT 16384  /* old buckets that each compaction moves while a resize is in progress */

#define ITERATION_MAX 1000000 /* very conservative overflow protection */

/* One key and its value. A removed entry leaves its bucket at once, so lookups walk only live entries, but it stays
 * allocated in the table's removed array, still marked deleted and with its next pointer intact, until the table frees
 * removed entries at a compaction, a resize, or a clear. An iterator that stands on it therefore stays valid exactly
 * as long as when removed entries stayed in their buckets. */
struct entry {
	UINT64_T key;
	void *value;
	struct entry *next; /* Next entry of the bucket, or of the bucket it left when it was removed. */
	int deleted;
};

/* A resize moves the entries a few buckets at a time, so no single operation costs O(entries): while it is in
 * progress, old_buckets holds the buckets not moved yet, every insert moves MIGRATE_INSERT of them and every compaction
 * MIGRATE_COMPACT, and lookups and removals look in both arrays. Iterations walk the buckets and then the old buckets,
 * as one sequence of bucket_count + old_count buckets, and only inserts, compactions, and clears move entries, each of
 * which ends any iteration in progress. */
struct itable {
	int size;
	int bucket_count;
	struct entry **buckets;
	int ibucket;
	struct entry *ientry;
	int iteration_index;
	int need_compact;	/* entries removed since the last compaction, which the removed array holds */
	struct entry **removed; /* removed entries, freed at the next compaction, resize, or clear */
	int removed_capacity;	/* length of the removed array */
	struct entry **old_buckets; /* buckets that a resize in progress has not moved yet, or NULL */
	int old_count;		    /* length of old_buckets, or 0 */
	int old_next;		    /* index of the first old bucket not moved yet */
	int shrinking;		    /* set while compactions halve the buckets of a table whose load is low */
};

/* The bucket at index i of the sequence that iterations walk: the buckets, then the old buckets of a resize. */
static struct entry *bucket_at(struct itable *h, int i)
{
	return i < h->bucket_count ? h->buckets[i] : h->old_buckets[i - h->bucket_count];
}

static int bucket_total(struct itable *h)
{
	return h->bucket_count + h->old_count;
}

/* Stand the iterator on the first entry of the first bucket at index i or later of the sequence that iterations walk,
 * or on none. Bucket heads are live entries, because a removed entry leaves its bucket at once. */
static void seek_bucket(struct itable *h, int i)
{
	h->ientry = 0;
	for (; i < h->bucket_count; i++) {
		if (h->buckets[i]) {
			h->ientry = h->buckets[i];
			h->ibucket = i;
			return;
		}
	}
	for (; i < h->bucket_count + h->old_count; i++) {
		if (h->old_buckets[i - h->bucket_count]) {
			h->ientry = h->old_buckets[i - h->bucket_count];
			h->ibucket = i;
			return;
		}
	}
	h->ibucket = i;
}

struct itable *itable_create(int bucket_count)
{
	struct itable *h;

	h = (struct itable *)malloc(sizeof(struct itable));
	if (!h)
		return 0;

	if (bucket_count == 0)
		bucket_count = DEFAULT_SIZE;

	h->bucket_count = bucket_count;
	h->buckets = (struct entry **)calloc(bucket_count, sizeof(struct entry *));
	if (!h->buckets) {
		free(h);
		return 0;
	}

	h->size = 0;
	h->ientry = 0;
	h->iteration_index = 0;
	h->need_compact = 0;
	h->removed = 0;
	h->removed_capacity = 0;
	h->old_buckets = 0;
	h->old_count = 0;
	h->old_next = 0;
	h->shrinking = 0;
	return h;
}

/* Keep a removed entry until the next compaction, resize, or clear. */
static void hold_removed(struct itable *h, struct entry *e)
{
	if (h->need_compact == h->removed_capacity) {
		h->removed_capacity = h->removed_capacity ? 2 * h->removed_capacity : 8;
		h->removed = xxrealloc(h->removed, sizeof(*h->removed) * h->removed_capacity);
	}
	h->removed[h->need_compact++] = e;
}

/* Free the entries removed since the last compaction. The removed array keeps its capacity for the next removals,
 * unless it holds more than REMOVED_KEEP_MIN entries and eight times the live entries plus buckets, so it never holds
 * more than a constant times the most removals between two compactions, and a table that is filled and emptied again
 * and again does not reallocate it each time. The removed entries stay in an array rather than in a list linked
 * through them: freeing walks the array in removal order without a cache miss per entry, and a list link would move
 * the inline key of a hash_table entry and slow its lookups. */
static void free_removed(struct itable *h)
{
	if (h->need_compact) {
		/* The iterator may stand on a removed entry. A pop resumes from it, so it starts again instead. */
		h->ientry = 0;
	}
	for (int i = 0; i < h->need_compact; i++) {
		free(h->removed[i]);
	}
	h->need_compact = 0;
	if (h->removed_capacity > REMOVED_KEEP_MIN && h->removed_capacity > 8 * (h->size + h->bucket_count)) {
		free(h->removed);
		h->removed = 0;
		h->removed_capacity = 0;
	}
}

void itable_clear(struct itable *h, void (*delete_func)(void *))
{
	struct entry *e, *f;
	int i;

	for (i = 0; i < bucket_total(h); i++) {
		e = bucket_at(h, i);
		while (e) {
			if (delete_func) {
				delete_func(e->value);
			}
			f = e->next;
			free(e);
			e = f;
		}
	}

	for (i = 0; i < h->bucket_count; i++) {
		h->buckets[i] = 0;
	}

	free(h->old_buckets);
	h->old_buckets = 0;
	h->old_count = 0;
	h->old_next = 0;
	h->shrinking = 0;

	free_removed(h);
	h->size = 0;
	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;
}

/* Move the entries of up to count old buckets of a resize in progress into the buckets, in the order of the old
 * buckets, and end the resize once every old bucket was moved. Moving entries ends any iteration in progress, and a pop
 * starts again, because the iterator's bucket index no longer describes where its entry is. */
static void itable_migrate(struct itable *h, int count)
{
	if (!h->old_buckets) {
		return;
	}

	struct entry *e, *f;
	for (; count > 0 && h->old_next < h->old_count; count--, h->old_next++) {
		e = h->old_buckets[h->old_next];
		h->old_buckets[h->old_next] = 0;
		while (e) {
			f = e->next;
			UINT64_T index = e->key % h->bucket_count;
			e->next = h->buckets[index];
			h->buckets[index] = e;
			e = f;
		}
	}

	if (h->old_next == h->old_count) {
		free(h->old_buckets);
		h->old_buckets = 0;
		h->old_count = 0;
		h->old_next = 0;
	}

	h->ientry = 0;
	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;
}

/* Start moving every entry to a new array of new_count buckets, and free the removed entries. Inserts and compactions
 * then move the old buckets a few at a time (itable_migrate()), so a resize costs O(1) per operation instead of
 * O(entries) in one. The new array comes zeroed from calloc(), which maps a large one without touching its pages. */
static int itable_resize(struct itable *h, int new_count)
{
	if (h->old_buckets) {
		return 0;
	}

	struct entry **new_buckets = (struct entry **)calloc(new_count, sizeof(struct entry *));
	if (!new_buckets) {
		return 0;
	}

	free_removed(h);
	h->old_buckets = h->buckets;
	h->old_count = h->bucket_count;
	h->old_next = 0;
	h->buckets = new_buckets;
	h->bucket_count = new_count;

	h->ientry = 0;
	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;

	return 1;
}

/* Halve the buckets of a table whose load fell below DEFAULT_MIN_LOAD, as (n+1)/2-1 undoes the doubling to 2n+1,
 * unless the load would then exceed half of DEFAULT_MAX_LOAD, so the next inserts do not double it again at once. One
 * halving at a time keeps the old buckets of the resize at most twice the new ones, so the inserts before the next
 * doubling, at least a quarter of the old buckets, move them all. Return whether a resize started, which also frees
 * the removed entries. */
static int itable_reduce_buckets(struct itable *h)
{
	if (((float)h->size / h->bucket_count) >= DEFAULT_MIN_LOAD) {
		return 0;
	}

	int half = ((h->bucket_count + 1) / 2) - 1;
	if (half < DEFAULT_SIZE || ((float)h->size / half) > DEFAULT_MAX_LOAD / 2) {
		return 0;
	}

	return itable_resize(h, half);
}

/* Free the entries removed since the last compaction, move up to MIGRATE_COMPACT old buckets of a resize in
 * progress, and start halving the buckets when the load is low. A table whose removals started a shrink keeps
 * halving at later compactions until its load is no longer low. The cost is O(removed entries), amortized O(1) per
 * removal however often it is called, plus O(MIGRATE_COMPACT). A compaction that frees or moves entries ends any
 * iteration in progress, as an insert does: an iteration continued afterwards aborts instead of reading a freed
 * entry. */
int itable_compact(struct itable *h)
{
	int freed = h->need_compact;
	if (!freed && !h->old_buckets && !h->shrinking) {
		return 0;
	}

	if (!h->old_buckets) {
		h->shrinking = itable_reduce_buckets(h);
	}
	itable_migrate(h, MIGRATE_COMPACT);
	free_removed(h);
	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;

	return freed;
}

void itable_delete(struct itable *h)
{
	itable_clear(h, 0);
	free(h->removed);
	free(h->buckets);
	free(h);
}

int itable_size(struct itable *h)
{
	return h->size;
}

/* Return the live entry of key, whose bucket index is given, in the buckets or, while a resize is in progress, in the
 * old buckets, where a bucket already moved is empty. */
static struct entry *find_entry(struct itable *h, UINT64_T key, UINT64_T index)
{
	struct entry *e;
	for (e = h->buckets[index]; e; e = e->next) {
		if (key == e->key) {
			return e;
		}
	}
	if (h->old_buckets) {
		for (e = h->old_buckets[key % h->old_count]; e; e = e->next) {
			if (key == e->key) {
				return e;
			}
		}
	}
	return 0;
}

void *itable_lookup(struct itable *h, UINT64_T key)
{
	struct entry *e = find_entry(h, key, key % h->bucket_count);
	return e ? e->value : 0;
}

static int itable_double_buckets(struct itable *h)
{
	return itable_resize(h, (2 * (h->bucket_count + 1)) - 1);
}

int itable_insert(struct itable *h, UINT64_T key, const void *value)
{
	struct entry *e;
	UINT64_T index;

	/* A table above its load while a resize is in progress doubles once the resize ended, which takes fewer inserts
	 * than it took to reach that load. */
	if (h->old_buckets) {
		itable_migrate(h, MIGRATE_INSERT);
	} else if (((float)h->size / h->bucket_count) > DEFAULT_MAX_LOAD && itable_double_buckets(h)) {
		itable_migrate(h, MIGRATE_INSERT);
	}

	index = key % h->bucket_count;
	e = find_entry(h, key, index);
	if (e) {
		e->value = (void *)value;
		h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;
		return 1;
	}

	/* A table that is never iterated is never compacted, so compact once removed entries outnumber the rest. A new
	 * entry ends any iteration in progress, so no iterator is left on a freed entry. */
	if (h->need_compact > h->size + h->bucket_count) {
		itable_compact(h);
		index = key % h->bucket_count;
	}

	e = (struct entry *)malloc(sizeof(struct entry));
	if (!e)
		return 0;

	e->key = key;
	e->value = (void *)value;
	e->deleted = 0;
	e->next = h->buckets[index];
	h->buckets[index] = e;

	h->size++;
	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;

	return 1;
}

/* A removed entry leaves its bucket at once and is freed later, see struct entry. */
void *itable_remove(struct itable *h, UINT64_T key)
{
	struct entry *e, **link;
	UINT64_T index;

	index = key % h->bucket_count;
	link = &h->buckets[index];

	for (int old = 0; old < 2; old++) {
		while ((e = *link)) {
			if (key == e->key) {
				*link = e->next;
				e->deleted = 1;
				hold_removed(h, e);

				h->size--;
				return e->value;
			}

			link = &e->next;
		}
		if (!h->old_buckets) {
			break;
		}
		link = &h->old_buckets[key % h->old_count];
	}

	return 0;
}

void *itable_pop(struct itable *t)
{
	UINT64_T key;
	void *value;

	if (!t) {
		return NULL;
	}

	if (itable_nextkey(t, t->iteration_index, &key, (void **)&value)) {
		t->iteration_index++;
		return itable_remove(t, key);
	}

	itable_firstkey(t);

	if (itable_nextkey(t, t->iteration_index, &key, (void **)&value)) {
		return itable_remove(t, key);
	}

	return NULL;
}

int itable_firstkey(struct itable *h)
{
	itable_compact(h);
	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;

	seek_bucket(h, 0);

	return h->iteration_index;
}

int itable_nextkey(struct itable *h, int iteration, UINT64_T *key, void **value)
{
	if (!h) {
		return 0;
	}

	if (iteration != h->iteration_index) {
		fatal("cctools bug: the itable iteration has not been reset since last modification");
	}

	if (h->ientry) {
		*key = h->ientry->key;
		if (value)
			*value = h->ientry->value;

		h->ientry = h->ientry->next;
		while (h->ientry && h->ientry->deleted) {
			h->ientry = h->ientry->next;
		}
		if (!h->ientry) {
			seek_bucket(h, h->ibucket + 1);
		}

		return 1;
	} else {
		return 0;
	}
}

void itable_foreach_ro(struct itable *h, void (*func)(UINT64_T key, void *value, void *arg), void *arg)
{
	struct entry *e;
	int i;

	int prev_iteration = h->iteration_index;

	for (i = 0; i < bucket_total(h); i++) {
		e = bucket_at(h, i);
		while (e) {
			if (!e->deleted) {
				func(e->key, e->value, arg);
			}
			e = e->next;

			if (h->iteration_index != prev_iteration) {
				fatal("cctools bug: the itable iteration has been modified since last call to itable_foreach_ro");
			}
		}
	}
}

/* vim: set noexpandtab tabstop=8: */
