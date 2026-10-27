/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "debug.h"
#include "xxmalloc.h"
#include "set.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>

#define DEFAULT_SIZE 127
#define DEFAULT_LOAD 0.75
#define DEFAULT_MIN_LOAD 0.125
#define REMOVED_KEEP_MIN 1024 /* removed array capacity that is always kept across compactions */
#define MIGRATE_INSERT 16      /* old buckets that each insert moves while a resize is in progress */
#define MIGRATE_COMPACT 16384  /* old buckets that each compaction moves while a resize is in progress */

#define ITERATION_MAX 1000000 /* very conservative overflow protection */

/* One element. A removed entry leaves its bucket at once, so lookups walk only live entries, but it stays allocated in
 * the set's removed array, still marked deleted and with its next pointer intact, until the set frees removed entries
 * at a compaction, a resize, or a clear. An iterator that stands on it therefore stays valid exactly as long as when
 * removed entries stayed in their buckets. */
struct entry {
	uintptr_t element;
	struct entry *next; /* Next entry of the bucket, or of the bucket it left when it was removed. */
	int deleted;
};

/* A resize moves the entries a few buckets at a time, so no single operation costs O(entries): while it is in
 * progress, old_buckets holds the buckets not moved yet, every insert moves MIGRATE_INSERT of them and every compaction
 * MIGRATE_COMPACT, and lookups and removals look in both arrays. Iterations walk the buckets and then the old buckets,
 * as one sequence of bucket_count + old_count buckets, and only inserts, compactions, and clears move entries, each of
 * which ends any iteration in progress. */
struct set {
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
	int shrinking;		    /* set while compactions halve the buckets of a set whose load is low */
};

/* The bucket at index i of the sequence that iterations walk: the buckets, then the old buckets of a resize. */
static struct entry *bucket_at(struct set *s, int i)
{
	return i < s->bucket_count ? s->buckets[i] : s->old_buckets[i - s->bucket_count];
}

static int bucket_total(struct set *s)
{
	return s->bucket_count + s->old_count;
}

/* Stand the iterator on the first entry of the first bucket at index i or later of the sequence that iterations walk,
 * or on none. Bucket heads are live entries, because a removed entry leaves its bucket at once. */
static void seek_bucket(struct set *s, int i)
{
	s->ientry = 0;
	for (; i < s->bucket_count; i++) {
		if (s->buckets[i]) {
			s->ientry = s->buckets[i];
			s->ibucket = i;
			return;
		}
	}
	for (; i < s->bucket_count + s->old_count; i++) {
		if (s->old_buckets[i - s->bucket_count]) {
			s->ientry = s->old_buckets[i - s->bucket_count];
			s->ibucket = i;
			return;
		}
	}
	s->ibucket = i;
}

/* Elements are mostly pointers, which are multiples of 16, so the bucket of an element comes from a mix of all of its
 * bits rather than from the element itself, which would fill only every 16th bucket of an even bucket count. */
static uint64_t set_bucket(uintptr_t element, int bucket_count)
{
	uint64_t x = (uint64_t)element * 0x9E3779B97F4A7C15ull;
	x ^= x >> 32;
	return x % bucket_count;
}

struct set *set_create(int bucket_count)
{
	struct set *s;

	s = (struct set *)malloc(sizeof(struct set));
	if (!s)
		return 0;

	if (bucket_count == 0)
		bucket_count = DEFAULT_SIZE;

	s->bucket_count = bucket_count;
	s->buckets = (struct entry **)calloc(bucket_count, sizeof(struct entry *));
	if (!s->buckets) {
		free(s);
		return 0;
	}

	s->size = 0;
	s->need_compact = 0;
	s->removed = 0;
	s->removed_capacity = 0;
	s->ientry = 0;
	s->iteration_index = 0;
	s->old_buckets = 0;
	s->old_count = 0;
	s->old_next = 0;
	s->shrinking = 0;
	return s;
}

struct set *set_duplicate(struct set *s)
{
	struct set *s2;

	if (!s) {
		return NULL;
	}

	s2 = set_create(0);
	const void *element;
	int iteration;
	SET_ITERATE(s, iteration, element)
	{
		set_insert(s2, element);
	}

	return s2;
}

struct set *set_union(struct set *s1, struct set *s2)
{

	struct set *s = set_duplicate(s1);

	void *element;
	int iteration;
	SET_ITERATE(s2, iteration, element)
	{
		set_insert(s, element);
	}

	return s;
}

/* Keep a removed entry until the next compaction, resize, or clear. */
static void hold_removed(struct set *s, struct entry *e)
{
	if (s->need_compact == s->removed_capacity) {
		s->removed_capacity = s->removed_capacity ? 2 * s->removed_capacity : 8;
		s->removed = xxrealloc(s->removed, sizeof(*s->removed) * s->removed_capacity);
	}
	s->removed[s->need_compact++] = e;
}

/* Free the entries removed since the last compaction. The removed array keeps its capacity for the next removals,
 * unless it holds more than REMOVED_KEEP_MIN entries and eight times the live entries plus buckets, so it never holds
 * more than a constant times the most removals between two compactions, and a set that is filled and emptied again
 * and again does not reallocate it each time. The removed entries stay in an array rather than in a list linked
 * through them: freeing walks the array in removal order without a cache miss per entry, and a list link would move
 * the inline key of a hash_table entry and slow its lookups. */
static void free_removed(struct set *s)
{
	if (s->need_compact) {
		/* The iterator may stand on a removed entry. A pop resumes from it, so it starts again instead. */
		s->ientry = 0;
	}
	for (int i = 0; i < s->need_compact; i++) {
		free(s->removed[i]);
	}
	s->need_compact = 0;
	if (s->removed_capacity > REMOVED_KEEP_MIN && s->removed_capacity > 8 * (s->size + s->bucket_count)) {
		free(s->removed);
		s->removed = 0;
		s->removed_capacity = 0;
	}
}

/* Move the entries of up to count old buckets of a resize in progress into the buckets, in the order of the old
 * buckets, and end the resize once every old bucket was moved. Moving entries ends any iteration in progress, and a pop
 * starts again, because the iterator's bucket index no longer describes where its entry is. */
static void set_migrate(struct set *s, int count)
{
	if (!s->old_buckets) {
		return;
	}

	struct entry *e, *f;
	for (; count > 0 && s->old_next < s->old_count; count--, s->old_next++) {
		e = s->old_buckets[s->old_next];
		s->old_buckets[s->old_next] = 0;
		while (e) {
			f = e->next;
			uint64_t index = set_bucket(e->element, s->bucket_count);
			e->next = s->buckets[index];
			s->buckets[index] = e;
			e = f;
		}
	}

	if (s->old_next == s->old_count) {
		free(s->old_buckets);
		s->old_buckets = 0;
		s->old_count = 0;
		s->old_next = 0;
	}

	s->ientry = 0;
	s->iteration_index = (s->iteration_index + 1) % ITERATION_MAX;
}

/* Start moving every entry to a new array of new_count buckets, and free the removed entries. Inserts and compactions
 * then move the old buckets a few at a time (set_migrate()), so a resize costs O(1) per operation instead of
 * O(entries) in one. The new array comes zeroed from calloc(), which maps a large one without touching its pages. */
static int set_resize(struct set *s, int new_count)
{
	if (s->old_buckets) {
		return 0;
	}

	struct entry **new_buckets = (struct entry **)calloc(new_count, sizeof(struct entry *));
	if (!new_buckets) {
		return 0;
	}

	free_removed(s);
	s->old_buckets = s->buckets;
	s->old_count = s->bucket_count;
	s->old_next = 0;
	s->buckets = new_buckets;
	s->bucket_count = new_count;

	s->ientry = 0;
	s->iteration_index = (s->iteration_index + 1) % ITERATION_MAX;

	return 1;
}

/* Halve the buckets of a set whose load fell below DEFAULT_MIN_LOAD, as (n+1)/2-1 undoes the doubling to 2n+1,
 * unless the load would then exceed half of DEFAULT_LOAD, so the next inserts do not double it again at once. One
 * halving at a time keeps the old buckets of the resize at most twice the new ones, so the inserts before the next
 * doubling, at least a quarter of the old buckets, move them all. Return whether a resize started, which also frees
 * the removed entries. */
static int set_reduce_buckets(struct set *s)
{
	if (((float)s->size / s->bucket_count) >= DEFAULT_MIN_LOAD) {
		return 0;
	}

	int half = ((s->bucket_count + 1) / 2) - 1;
	if (half < DEFAULT_SIZE || ((float)s->size / half) > DEFAULT_LOAD / 2) {
		return 0;
	}

	return set_resize(s, half);
}

/* Free the entries removed since the last compaction, move up to MIGRATE_COMPACT old buckets of a resize in
 * progress, and start halving the buckets when the load is low. A set whose removals started a shrink keeps halving at
 * later compactions until its load is no longer low. The cost is O(removed entries), amortized O(1) per removal however
 * often it is called, plus O(MIGRATE_COMPACT). A compaction that frees or moves entries ends any iteration in
 * progress, as an insert does: an iteration continued afterwards aborts instead of reading a freed entry. */
int set_compact(struct set *s)
{
	int freed = s->need_compact;
	if (!freed && !s->old_buckets && !s->shrinking) {
		return 0;
	}

	if (!s->old_buckets) {
		s->shrinking = set_reduce_buckets(s);
	}
	set_migrate(s, MIGRATE_COMPACT);
	free_removed(s);
	s->iteration_index = (s->iteration_index + 1) % ITERATION_MAX;

	return freed;
}

void set_clear(struct set *s)
{
	struct entry *e, *f;
	int i;

	for (i = 0; i < bucket_total(s); i++) {
		e = bucket_at(s, i);
		while (e) {
			f = e->next;
			free(e);
			e = f;
		}
	}

	for (i = 0; i < s->bucket_count; i++) {
		s->buckets[i] = 0;
	}

	free(s->old_buckets);
	s->old_buckets = 0;
	s->old_count = 0;
	s->old_next = 0;
	s->shrinking = 0;

	free_removed(s);
	s->size = 0;
	s->iteration_index = (s->iteration_index + 1) % ITERATION_MAX;
}

void set_delete(struct set *s)
{
	set_clear(s);
	free(s->removed);
	free(s->buckets);
	free(s);
}

int set_size(struct set *s)
{
	return s->size;
}

/* Return whether key, whose bucket index is given, is in the buckets or, while a resize is in progress, in the old
 * buckets, where a bucket already moved is empty. */
static int find_element(struct set *s, uintptr_t key, uint64_t index)
{
	struct entry *e;
	for (e = s->buckets[index]; e; e = e->next) {
		if (key == e->element) {
			return 1;
		}
	}
	if (s->old_buckets) {
		for (e = s->old_buckets[set_bucket(key, s->old_count)]; e; e = e->next) {
			if (key == e->element) {
				return 1;
			}
		}
	}
	return 0;
}

int set_lookup(struct set *s, void *element)
{
	return find_element(s, (uintptr_t)element, set_bucket((uintptr_t)element, s->bucket_count));
}

static int set_double_buckets(struct set *s)
{
	return set_resize(s, (2 * (s->bucket_count + 1)) - 1);
}

int set_insert(struct set *s, const void *element)
{
	struct entry *e;
	uint64_t index;

	uintptr_t key = (uintptr_t)element;

	/* A set above its load while a resize is in progress doubles once the resize ended, which takes fewer inserts
	 * than it took to reach that load. */
	if (s->old_buckets) {
		set_migrate(s, MIGRATE_INSERT);
	} else if (((float)s->size / s->bucket_count) > DEFAULT_LOAD && set_double_buckets(s)) {
		set_migrate(s, MIGRATE_INSERT);
	}

	index = set_bucket(key, s->bucket_count);
	if (find_element(s, key, index)) {
		return 1;
	}

	/* A set that is never iterated is never compacted, so compact once removed entries outnumber the rest. A new
	 * entry ends any iteration in progress, so no iterator is left on a freed entry. */
	if (s->need_compact > s->size + s->bucket_count) {
		set_compact(s);
		index = set_bucket(key, s->bucket_count);
	}

	e = (struct entry *)malloc(sizeof(struct entry));
	if (!e)
		return 0;

	e->element = key;
	e->deleted = 0;
	e->next = s->buckets[index];
	s->buckets[index] = e;
	s->size++;
	s->iteration_index = (s->iteration_index + 1) % ITERATION_MAX;

	return 1;
}

int set_insert_set(struct set *s, struct set *s2)
{
	int iteration;
	int additions = 0;
	const void *element;

	SET_ITERATE(s2, iteration, element)
	{
		additions += set_insert(s, element);
	}

	return additions;
}

int set_insert_list(struct set *s, struct list *l)
{
	list_first_item(l);
	int additions = 0;
	const void *element;
	while ((element = list_next_item(l))) {
		additions += set_insert(s, element);
	}

	return additions;
}

int set_push(struct set *s, const void *element)
{
	return set_insert(s, element);
}

int set_remove(struct set *s, const void *element)
{
	struct entry *e, **link;

	uintptr_t key = (uintptr_t)element;

	link = &s->buckets[set_bucket(key, s->bucket_count)];

	for (int old = 0; old < 2; old++) {
		while ((e = *link)) {
			if (key == e->element) {
				*link = e->next;
				e->deleted = 1;
				hold_removed(s, e);
				s->size--;
				return 1;
			}
			link = &e->next;
		}
		if (!s->old_buckets) {
			break;
		}
		link = &s->old_buckets[set_bucket(key, s->old_count)];
	}

	return 0;
}

void *set_pop(struct set *s)
{
	void *element;

	if (!s) {
		return NULL;
	}

	element = set_next_element(s, s->iteration_index);
	if (element) {
		s->iteration_index++;
		if (set_remove(s, element))
			return element;
		return NULL;
	}

	set_first_element(s);

	element = set_next_element(s, s->iteration_index);
	if (element) {
		if (set_remove(s, element))
			return element;
	}

	return NULL;
}

int set_first_element(struct set *s)
{
	set_compact(s);
	s->iteration_index = (s->iteration_index + 1) % ITERATION_MAX;

	seek_bucket(s, 0);

	return s->iteration_index;
}

void *set_next_element(struct set *s, int iteration)
{
	void *element;

	if (!s) {
		return 0;
	}

	if (iteration != s->iteration_index) {
		fatal("cctools bug: the set iteration has not been reset since last modification");
	}

	if (s->ientry) {
		element = (void *)s->ientry->element;

		s->ientry = s->ientry->next;
		while (s->ientry && s->ientry->deleted) {
			s->ientry = s->ientry->next;
		}
		if (!s->ientry) {
			seek_bucket(s, s->ibucket + 1);
		}

		return element;
	} else {
		return 0;
	}
}

void set_foreach_ro(struct set *s, void (*func)(void *element, void *arg), void *arg)
{
	struct entry *e;
	int i;

	int prev_iteration = s->iteration_index;

	for (i = 0; i < bucket_total(s); i++) {
		e = bucket_at(s, i);
		while (e) {
			if (!e->deleted) {
				func((void *)e->element, arg);
			}
			e = e->next;

			if (s->iteration_index != prev_iteration) {
				fatal("cctools bug: the set iteration has been modified since last call to set_foreach_ro");
			}
		}
	}
}

int set_random_element(struct set *s, int *offset_bookkeep)
{
	s->iteration_index = (s->iteration_index + 1) % ITERATION_MAX;

	s->ientry = 0;
	if (s->bucket_count < 1) {
		return s->iteration_index;
	}

	int ibucket_start = random() % bucket_total(s);

	for (s->ibucket = ibucket_start; s->ibucket < bucket_total(s); s->ibucket++) {
		s->ientry = bucket_at(s, s->ibucket);
		while (s->ientry && s->ientry->deleted) {
			s->ientry = s->ientry->next;
		}
		if (s->ientry) {
			*offset_bookkeep = s->ibucket;
			return s->iteration_index;
		}
	}

	for (s->ibucket = 0; s->ibucket < ibucket_start; s->ibucket++) {
		s->ientry = bucket_at(s, s->ibucket);
		while (s->ientry && s->ientry->deleted) {
			s->ientry = s->ientry->next;
		}
		if (s->ientry) {
			*offset_bookkeep = s->ibucket;
			return s->iteration_index;
		}
	}

	return s->iteration_index;
}

void *set_next_element_with_offset(struct set *s, int offset_bookkeep, int iteration)
{
	if (!s) {
		return 0;
	}

	if (iteration != s->iteration_index) {
		fatal("cctools bug: the set iteration has not been reset since last modification");
	}

	if (s->bucket_count < 1) {
		return 0;
	}

	void *element = NULL;

	offset_bookkeep = offset_bookkeep % bucket_total(s);

	if (s->ientry) {
		element = (void *)s->ientry->element;

		s->ientry = s->ientry->next;
		while (s->ientry && s->ientry->deleted) {
			s->ientry = s->ientry->next;
		}
		if (!s->ientry) {
			s->ibucket = (s->ibucket + 1) % bucket_total(s);
			for (; s->ibucket != offset_bookkeep; s->ibucket = (s->ibucket + 1) % bucket_total(s)) {
				s->ientry = bucket_at(s, s->ibucket);
				while (s->ientry && s->ientry->deleted) {
					s->ientry = s->ientry->next;
				}
				if (s->ientry) {
					break;
				}
			}
		}

		return element;
	}

	return 0;
}

void **set_values(struct set *s)
{
	if (s->size < 1) {
		return NULL;
	}

	void **elements = malloc(sizeof(void *) * s->size);

	int offset_bookkeep;
	int iteration;
	void *element;
	int i = 0;
	SET_ITERATE_RANDOM_START(s, offset_bookkeep, iteration, element)
	{
		elements[i] = element;
		i++;
	}

	return elements;
}

void **set_values_array(struct set *s)
{
	if (s->size < 1) {
		return NULL;
	}

	void **elements = malloc(sizeof(void *) * (s->size + 1));
	int iteration;
	void *element;
	int i = 0;
	SET_ITERATE(s, iteration, element)
	{
		elements[i] = element;
		i++;
	}

	elements[s->size] = NULL;

	return elements;
}

void set_free_values_array(void **values)
{
	free(values);
}

/* vim: set noexpandtab tabstop=8: */
