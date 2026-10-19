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
};

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

	for (i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
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

	free_removed(h);
	h->size = 0;
	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;
}

/* Move every entry to a new array of buckets, keeping the order in which the old buckets list them, and free the
 * removed entries. */
static int itable_resize(struct itable *h, int new_count)
{
	struct entry **new_buckets = (struct entry **)calloc(new_count, sizeof(struct entry *));
	if (!new_buckets) {
		return 0;
	}

	struct entry *e, *f;
	for (int i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			f = e->next;
			UINT64_T index = e->key % new_count;
			e->next = new_buckets[index];
			new_buckets[index] = e;
			e = f;
		}
	}

	/* Make the old point to the new */
	free(h->buckets);
	h->buckets = new_buckets;
	h->bucket_count = new_count;
	free_removed(h);

	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;

	return 1;
}

/* Halve the buckets of a table whose load fell below DEFAULT_MIN_LOAD, as (n+1)/2-1 undoes the doubling to 2n+1.
 * Return whether the buckets were resized, which also frees the removed entries. */
static int itable_reduce_buckets(struct itable *h)
{
	if (((float)h->size / h->bucket_count) >= DEFAULT_MIN_LOAD) {
		return 0;
	}

	int new_count = ((h->bucket_count + 1) / 2) - 1;

	/* DEFAULT_SIZE is the minimum size, and the table cannot be reduced above DEFAULT_MAX_LOAD */
	if (new_count < DEFAULT_SIZE || ((float)h->size / new_count) > DEFAULT_MAX_LOAD) {
		return 0;
	}

	return itable_resize(h, new_count);
}

/* Free the entries removed since the last compaction and shrink the buckets when the load is low. The cost is O(removed
 * entries), plus O(buckets) for a resize, which only follows removals of most entries. A compaction that frees entries
 * ends any iteration in progress, as an insert does, so an iteration continued afterwards aborts instead of reading a
 * freed entry. */
void itable_compact(struct itable *h)
{
	if (!h->need_compact) {
		return;
	}

	if (!itable_reduce_buckets(h)) {
		free_removed(h);
	}
	h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;
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

void *itable_lookup(struct itable *h, UINT64_T key)
{
	struct entry *e;
	UINT64_T index;

	index = key % h->bucket_count;
	e = h->buckets[index];

	while (e) {
		if (key == e->key) {
			return e->value;
		}
		e = e->next;
	}

	return 0;
}

static int itable_double_buckets(struct itable *h)
{
	return itable_resize(h, (2 * (h->bucket_count + 1)) - 1);
}

int itable_insert(struct itable *h, UINT64_T key, const void *value)
{
	struct entry *e;
	UINT64_T index;

	if (((float)h->size / h->bucket_count) > DEFAULT_MAX_LOAD)
		itable_double_buckets(h);

	index = key % h->bucket_count;
	e = h->buckets[index];

	while (e) {
		if (key == e->key) {
			e->value = (void *)value;
			h->iteration_index = (h->iteration_index + 1) % ITERATION_MAX;
			return 1;
		}
		e = e->next;
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

	h->ientry = 0;
	for (h->ibucket = 0; h->ibucket < h->bucket_count; h->ibucket++) {
		h->ientry = h->buckets[h->ibucket];
		if (h->ientry) {
			/* we don't need to check for deleted entries here because itable_compact removes them */
			break;
		}
	}

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
			h->ibucket++;
			for (; h->ibucket < h->bucket_count; h->ibucket++) {
				h->ientry = h->buckets[h->ibucket];
				if (h->ientry && !h->ientry->deleted)
					break;
			}
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

	for (i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
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
